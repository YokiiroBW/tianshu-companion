import asyncio
import copy
import json
import unittest

from support import Harness
from tianshu_companion.context import scope_check
from tianshu_companion.contracts import Fault, PROFILE_DOMAIN


class ContextCheckpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness(silence_ms=0)

    async def asyncTearDown(self):
        await self.h.core.close()

    async def test_peer_failure_cancels_and_drains_slow_independent_reads(self):
        from tianshu_companion.context_checks import read_parallel

        entered, stopped = asyncio.Event(), asyncio.Event()

        async def slow():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        async def failed():
            await entered.wait()
            raise Fault("scope_changed")

        with self.assertRaises(Fault) as error:
            await asyncio.wait_for(read_parallel([slow, failed]), 1)
        self.assertEqual(error.exception.code, "scope_changed")
        self.assertTrue(stopped.is_set())

    async def test_preflight_overlaps_independent_reads_and_rechecks_revocation(self):
        previous = (await self.turns())[0]
        entered, release = set(), asyncio.Event()
        together = asyncio.Event()
        original_resolve = self.h.origins.resolve
        original_select = self.h.memory.select
        original_book = self.h.core.book_reading.verify

        async def wait(name):
            entered.add(name)
            if len(entered) == 3:
                together.set()
            await release.wait()

        async def resolve(*args, **kwargs):
            await wait("authority")
            return await original_resolve(*args, **kwargs)

        async def select(*args, **kwargs):
            await wait("memory")
            return await original_select(*args, **kwargs)

        async def book(*args, **kwargs):
            await wait("reading")
            return await original_book(*args, **kwargs)

        self.h.origins.resolve = resolve
        self.h.memory.select = select
        self.h.core.book_reading.verify = book
        job = asyncio.create_task(self.h.core._preflight(previous))
        try:
            await asyncio.wait_for(together.wait(), 1)
        finally:
            release.set()
            await job
        self.assertEqual(entered, {"authority", "memory", "reading"})
        self.h.origins.values[previous["origin"]["assertion_ref"]]["revoked"] = True
        with self.assertRaises(Fault):
            await self.h.core._preflight(previous)

    async def test_same_snapshot_with_distinct_local_turn_refs_shares_only_the_peer_read(self):
        previous = (await self.turns())[0]
        first = scope_check(previous)
        # Both local rows still receive local provenance validation. Their exact
        # remote request can coincide after a previous valid snapshot is reused.
        second = dict(first, source_turn_id=None)
        calls = 0
        original = self.h.memory.history

        async def history(*args, **kwargs):
            nonlocal calls
            calls += 1
            return await original(*args, **kwargs)

        self.h.memory.history = history
        await self.h.core._verify_checks(previous, [first, second])
        self.assertEqual(calls, 1)
        await self.h.core._verify_checks(previous, [first, second])
        self.assertEqual(calls, 2)

    async def turns(self, count=1):
        for i in range(count):
            await self.h.ingest(text=f"第{i}本书放在桌上", group=True, channel="group:checks")
            await self.h.cycles()
        return self.h.turns()

    async def test_profile_preparation_overlaps_reads_and_cancel_drains_all(self):
        for account in ("a", "b", "c"):
            await self.h.ingest(
                text="周末想看书", group=True, channel="group:checks", account=account
            )
            await self.h.cycles()
        entered, release = asyncio.Event(), asyncio.Event()
        active, peak = 0, 0
        original = self.h.memory.profiles

        async def profiles(*args, **kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active == 4:
                entered.set()
            try:
                await release.wait()
                return await original(*args, **kwargs)
            finally:
                active -= 1

        self.h.memory.profiles = profiles
        previous = self.h.turns()[-1]
        job = asyncio.create_task(self.h.core._prepare_context(previous, []))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            self.assertEqual(peak, 4)
            job.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await job
            self.assertEqual(active, 0)
        finally:
            release.set()
            if not job.done():
                job.cancel()
                await asyncio.gather(job, return_exceptions=True)

    async def test_independent_history_reads_overlap_with_bounded_admission(self):
        turns = await self.turns(6)
        entered, release = asyncio.Event(), asyncio.Event()
        calls, active, peak = 0, 0, 0
        original = self.h.memory.history

        async def history(*args, **kwargs):
            nonlocal calls, active, peak
            calls += 1
            active += 1
            peak = max(peak, active)
            if active == 4:
                entered.set()
            try:
                await release.wait()
                return await original(*args, **kwargs)
            finally:
                active -= 1

        self.h.memory.history = history
        job = asyncio.create_task(
            self.h.core._verify_checks(turns[-1], [scope_check(t) for t in turns])
        )
        try:
            await asyncio.wait_for(entered.wait(), 1)
            self.assertEqual(calls, 4)
        finally:
            release.set()
            await job
        self.assertEqual(calls, 6)
        self.assertEqual(peak, 4)
        self.assertEqual(active, 0)

    async def test_same_authority_is_shared_across_domains_only_in_this_checkpoint(self):
        previous = (await self.turns())[0]
        text = scope_check(previous)
        profile = dict(
            text,
            version_domain=PROFILE_DOMAIN,
            target=dict(kind="group", conversation_id=previous["conversation_id"]),
            text="阅读",
            selection=["topic"],
            scope_version=1,
        )
        calls = []
        original = self.h.origins.resolve

        async def resolve(*args):
            calls.append(args)
            return await original(*args)

        self.h.origins.resolve = resolve
        await self.h.core._verify_checks(previous, [text, profile])
        self.assertEqual(len(calls), 1)
        self.h.origins.values[previous["origin"]["assertion_ref"]]["revoked"] = True
        with self.assertRaises(Fault):
            await self.h.core._verify_checks(previous, [text, profile])
        self.assertEqual(len(calls), 2)

    async def test_distinct_origin_leases_are_not_merged_by_person(self):
        turns = await self.turns(2)
        calls = []
        original = self.h.origins.resolve

        async def resolve(service, envelope, now):
            calls.append(envelope["origin"]["assertion_ref"])
            return await original(service, envelope, now)

        self.h.origins.resolve = resolve
        await self.h.core._verify_checks(turns[-1], [scope_check(t) for t in turns])
        self.assertEqual(set(calls), {t["origin"]["assertion_ref"] for t in turns})

    async def test_one_source_lease_renews_once_even_with_two_parallel_domains(self):
        previous = (await self.turns())[0]
        previous["source_deadline"] = self.h.clock() + 1
        self.h.core.store.put("turns", previous)
        text = scope_check(previous)
        profile = dict(
            text,
            version_domain=PROFILE_DOMAIN,
            target=dict(kind="group", conversation_id=previous["conversation_id"]),
            text="阅读",
            selection=["topic"],
            scope_version=1,
        )
        calls = []
        original = self.h.core.continue_delivery_origin

        async def renew(turn):
            calls.append(turn["id"])
            await asyncio.sleep(0)
            return await original(turn)

        self.h.core.continue_delivery_origin = renew
        await self.h.core._verify_checks(previous, [text, profile])
        self.assertEqual(calls, [previous["id"]])

    async def test_cancellation_drains_shared_reads_and_does_not_cache_success(self):
        previous = (await self.turns())[0]
        entered, stopped, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = self.h.origins.resolve

        async def resolve(*args):
            entered.set()
            try:
                await release.wait()
                return await original(*args)
            finally:
                stopped.set()

        self.h.origins.resolve = resolve
        memo = {}
        job = asyncio.create_task(
            self.h.core._verify_checks(previous, [scope_check(previous)], verified=memo)
        )
        await asyncio.wait_for(entered.wait(), 1)
        job.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await job
        self.assertTrue(stopped.is_set())
        self.assertFalse(memo)
        self.h.origins.resolve = original
        self.h.origins.values[previous["origin"]["assertion_ref"]]["revoked"] = True
        with self.assertRaises(Fault):
            await self.h.core._verify_checks(previous, [scope_check(previous)], verified=memo)

    async def test_a_foreign_scope_cannot_trigger_a_foreign_authority_read(self):
        previous = (await self.turns())[0]
        check = copy.deepcopy(scope_check(previous))
        check["scope"]["conversation_id"] = "conversation:other"
        calls = []
        original = self.h.origins.resolve

        async def resolve(*args):
            calls.append(args)
            return await original(*args)

        self.h.origins.resolve = resolve
        with self.assertRaises(Fault):
            await self.h.core._verify_checks(previous, [check])
        self.assertEqual(calls, [])

    async def test_fact_revision_preserves_authorized_input_but_not_old_answer_or_withdrawal(self):
        source_epoch = 1
        original = self.h.memory.history

        async def input_history(origin, scope, *, expected=None, known=None):
            snapshot = await original(origin, scope, expected=expected)
            for check in snapshot["history_checks"]:
                check["history_version"] = source_epoch
            if known is not None and known != snapshot:
                raise Fault("scope_changed")
            return snapshot

        self.h.memory.input_history = input_history
        self.h.memory.history_version = 1
        self.h.gateway.segments = ["助手原来的建议。"]
        await self.h.ingest(
            text="周二要看牙，培训改到周三。", group=True, channel="group:checks", account="a"
        )
        await self.h.cycles()
        previous = self.h.turns()[0]
        self.h.memory.scope_version = self.h.memory.history_version = 2
        await self.h.ingest(
            text="之前培训从周二挪到周三是为啥？", group=True, channel="group:checks", account="b"
        )
        await self.h.cycles()
        groups = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        raw = next(g for g in groups if g["turn_id"] == previous["id"])
        self.assertTrue(raw["input_only"])
        self.assertEqual(raw["replies"], [])
        self.assertIn("看牙", json.dumps(raw, ensure_ascii=False))
        source_epoch = 3
        self.h.memory.scope_version = self.h.memory.history_version = 3
        await self.h.ingest(
            text="刚才培训为什么改日子？", group=True, channel="group:checks", account="b"
        )
        await self.h.cycles()
        self.assertEqual(
            json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"], []
        )
