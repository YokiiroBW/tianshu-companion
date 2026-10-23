import asyncio
import copy
import json
import unittest
from unittest.mock import patch

from support import Harness
from tianshu_companion.clients import epoch, utc
from tianshu_companion.context import profile_check, scope_check
from tianshu_companion.context_checks import VerificationCheckpoint
from tianshu_companion.contracts import Fault, PROFILE_DOMAIN, digest


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

    async def test_prepare_shares_successful_authority_across_candidate_groups_then_closes(self):
        first, second, current = await self.turns(3)
        for previous, text in ((first, "阅读"), (second, "周末")):
            previous["profile_checks"] = [
                profile_check(
                    first,
                    dict(kind="group", conversation_id=first["conversation_id"]),
                    text,
                    ["topic"],
                    dict(scope_version=self.h.memory.profile_version),
                )
            ]
            self.h.core.store.put("turns", previous)
        calls, checkpoints = [], []
        resolve = self.h.origins.resolve

        async def counted(service, envelope, now):
            calls.append(envelope["origin"])
            return await resolve(service, envelope, now)

        class Tracked(VerificationCheckpoint):
            def __init__(self):
                super().__init__()
                self.closed = False
                checkpoints.append(self)

            async def close(self):
                await super().close()
                self.closed = True

        self.h.origins.resolve = counted
        with patch("tianshu_companion.context_preparation.VerificationCheckpoint", Tracked):
            context, _, checks, profiles, _, _ = await self.h.core._prepare_context(current, [])
        self.assertEqual(len(context.data["recent_dialogue"]), 2)
        self.assertEqual(calls.count(first["origin"]), 1)
        self.assertEqual(len(checkpoints), 1)
        self.assertTrue(checkpoints[0].closed)
        current.update(context_checks=checks, profile_checks=profiles)
        await self.h.core._preflight(current)
        self.assertEqual(calls.count(first["origin"]), 2)

    async def test_preparation_overlaps_independent_recent_groups_and_preserves_order(self):
        first, second, current = await self.turns(3)
        entered, release = asyncio.Event(), asyncio.Event()
        source_refs = {first["origin"]["assertion_ref"], second["origin"]["assertion_ref"]}
        observed = set()
        active, peak = 0, 0
        resolve = self.h.origins.resolve

        async def gated(service, envelope, now):
            nonlocal active, peak
            source_ref = envelope["origin"]["assertion_ref"]
            if source_ref not in source_refs:
                return await resolve(service, envelope, now)
            observed.add(source_ref)
            active += 1
            peak = max(peak, active)
            if observed == source_refs:
                entered.set()
            try:
                await release.wait()
                return await resolve(service, envelope, now)
            finally:
                active -= 1

        self.h.origins.resolve = gated
        job = asyncio.create_task(self.h.core._prepare_context(current, []))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            self.assertEqual(peak, 2)
            release.set()
            context, *_ = await job
            self.assertEqual(
                [group["turn_id"] for group in context.data["recent_dialogue"]],
                [first["id"], second["id"]],
            )
            self.assertEqual(active, 0)
        finally:
            release.set()
            if not job.done():
                job.cancel()
                await asyncio.gather(job, return_exceptions=True)

    async def test_prepare_transient_full_candidate_failure_reopens_for_raw_input(self):
        self.h.memory.input_history = self.h.memory.history
        previous, current = await self.turns(2)
        calls, checkpoints = [], []
        resolve = self.h.origins.resolve

        async def transient(service, envelope, now):
            if envelope["origin"] == previous["origin"]:
                calls.append(envelope["origin"])
                if len(calls) == 1:
                    raise Fault("dependency_unavailable")
            return await resolve(service, envelope, now)

        class Tracked(VerificationCheckpoint):
            async def close(self):
                await super().close()
                checkpoints.append(self)

        self.h.origins.resolve = transient
        with patch("tianshu_companion.context_preparation.VerificationCheckpoint", Tracked):
            context, *_ = await self.h.core._prepare_context(current, [])
        self.assertEqual(len(calls), 2)
        self.assertEqual(len({id(item) for item in checkpoints}), 2)
        self.assertTrue(context.data["recent_dialogue"][0]["input_only"])
        self.assertEqual(context.data["recent_dialogue"][0]["replies"], [])

    async def test_cancelled_preparation_closes_the_shared_authority_graph(self):
        await self.turns(2)
        entered, stopped = asyncio.Event(), asyncio.Event()
        checkpoints = []

        async def blocked(*_):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        class Tracked(VerificationCheckpoint):
            async def close(self):
                await super().close()
                checkpoints.append(self)

        self.h.origins.resolve = blocked
        with patch("tianshu_companion.context_preparation.VerificationCheckpoint", Tracked):
            job = asyncio.create_task(self.h.core._prepare_context(self.h.turns()[-1], []))
            await asyncio.wait_for(entered.wait(), 1)
            job.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await job
        self.assertTrue(stopped.is_set())
        self.assertEqual(len(checkpoints), 1)
        self.assertTrue(all(task.done() for task in checkpoints[0].reads.values()))

    def lease_authority(self, turn, on_renew=None):
        from test_delivery_v2 import NativeQueue

        calls = []
        issuer = self.h.origins.values[turn["origin"]["assertion_ref"]]
        issuer["expires_at"] = utc(self.h.clock() + 60)
        turn["source_deadline"] = epoch(issuer["expires_at"])
        self.h.core.store.put("turns", turn)
        queue = NativeQueue(self.h.clock)

        async def context(source, scope, channel):
            self.assertEqual(source["origin"], turn["origin"])
            self.assertEqual(scope, turn["scope"])
            if issuer["revoked"]:
                raise Fault("forbidden")
            calls.append(self.h.clock())
            issuer["expires_at"] = utc(self.h.clock() + 60)
            if on_renew:
                on_renew()
                await asyncio.sleep(0)
            return dict(
                origin=turn["origin"], scope=scope, channel=channel, expires_at=issuer["expires_at"]
            )

        queue.expression_context = context
        self.h.core.sender = queue
        return calls

    async def test_expired_requester_is_authoritatively_renewed_before_late_profiles(self):
        current = (await self.turns())[0]
        renewals = self.lease_authority(current)
        history, profiles = self.h.memory.history, self.h.memory.profiles
        profile_calls = []

        async def slow_history(*args, **kwargs):
            result = await history(*args, **kwargs)
            self.h.clock.advance(61)
            return result

        async def guarded_profiles(*args, **kwargs):
            issuer = self.h.origins.values[current["origin"]["assertion_ref"]]
            self.assertGreater(epoch(issuer["expires_at"]), self.h.clock())
            profile_calls.append(args[2])
            return await profiles(*args, **kwargs)

        self.h.memory.history, self.h.memory.profiles = slow_history, guarded_profiles
        await self.h.core._prepare_context(current, [])
        self.assertEqual(len(renewals), 1)
        self.assertEqual(len(profile_calls), 2)
        self.assertGreater(
            self.h.core.store.get("turns", current["id"])["source_deadline"], self.h.clock()
        )

    async def test_renewal_wait_reloads_cancellation_source_and_role(self):
        current = (await self.turns())[0]
        original = self.h.core.store.get("turns", current["id"])
        for change in ("cancel", "source", "role"):
            with self.subTest(change=change):
                self.h.core.store.put("turns", copy.deepcopy(original))
                current = self.h.core.store.get("turns", original["id"])
                actor = current["scope"]["actor_id"]
                role = self.h.core.roles.get(actor)
                physical = self.h.core.store.get(
                    "physicals", digest(current["bundle"]["messages"][0]["message_key"])
                )

                def revoke():
                    fresh = self.h.core.store.get("turns", current["id"])
                    if change == "cancel":
                        fresh["cancelled"] = True
                        self.h.core.store.put("turns", fresh)
                    elif change == "source":
                        withdrawn = copy.deepcopy(physical)
                        withdrawn["fact"]["state"] = "retracted"
                        self.h.core.store.put("physicals", withdrawn)
                    else:
                        self.h.core.roles.pop(actor)

                self.lease_authority(current, revoke)
                self.h.clock.advance(61)
                try:
                    with self.assertRaises(Fault):
                        await self.h.core._refresh_preparation_origin(current)
                finally:
                    self.h.core.roles[actor] = role
                    self.h.core.store.put("physicals", physical)

    async def test_long_shared_checkpoint_does_not_reuse_a_previous_lease(self):
        previous = (await self.turns())[0]
        renewals = self.lease_authority(previous)
        calls = []
        original = self.h.origins.resolve

        async def counted(*args):
            calls.append(True)
            return await original(*args)

        self.h.origins.resolve = counted
        text = scope_check(previous)
        profile = profile_check(
            previous,
            dict(kind="group", conversation_id=previous["conversation_id"]),
            "阅读",
            ["topic"],
            dict(scope_version=self.h.memory.profile_version),
        )
        async with VerificationCheckpoint() as checkpoint:
            await self.h.core._verify_checks(previous, [text], checkpoint=checkpoint)
            self.h.clock.advance(61)
            await self.h.core._verify_checks(previous, [profile], checkpoint=checkpoint)
        self.assertEqual(len(renewals), 1)
        self.assertEqual(len(calls), 2)

    async def test_profile_crossing_expiry_fails_closed_until_authority_continues_lease(self):
        current = (await self.turns())[0]
        renewals = self.lease_authority(current)
        profiles = self.h.memory.profiles
        crossing = True

        async def expires_during_read(*args, **kwargs):
            nonlocal crossing
            if crossing:
                crossing = False
                self.h.clock.advance(61)
            issuer = self.h.origins.values[current["origin"]["assertion_ref"]]
            if epoch(issuer["expires_at"]) <= self.h.clock():
                raise Fault("forbidden")
            return await profiles(*args, **kwargs)

        self.h.memory.profiles = expires_during_read
        with self.assertRaises(Fault) as error:
            await self.h.core._prepare_context(current, [])
        self.assertEqual(error.exception.code, "forbidden")
        self.assertEqual(renewals, [])
        await self.h.core._prepare_context(current, [])
        self.assertEqual(len(renewals), 1)

    async def test_revoked_authority_cannot_be_replaced_with_a_local_extension(self):
        current = (await self.turns())[0]
        renewals = self.lease_authority(current)
        deadline = current["source_deadline"]
        self.h.origins.values[current["origin"]["assertion_ref"]]["revoked"] = True
        self.h.clock.advance(61)
        with self.assertRaises(Fault) as error:
            await self.h.core._prepare_context(current, [])
        self.assertEqual(error.exception.code, "forbidden")
        self.assertEqual(renewals, [])
        self.assertEqual(self.h.core.store.get("turns", current["id"])["source_deadline"], deadline)
