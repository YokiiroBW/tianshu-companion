"""Admission capacity prechecks retain the final transaction's authority."""

import asyncio
import copy
import unittest
from dataclasses import replace

import httpx

from test_source_sync import RecordingModelSelector, SourceHarness
from tianshu_companion.app import create_app
from tianshu_companion.clients import uid
from tianshu_companion.contracts import Fault


class SourceCapacityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = SourceHarness()
        await self.h.core.close()
        self.selected = RecordingModelSelector(self.h)
        self.h.core = self.h.new_core(default_model_selector=self.selected)

    async def asyncTearDown(self):
        await self.h.core.close()

    def message(self, text="Capacity input", *, account="a", actors=("actor:a",), **kwargs):
        return self.h.fanout(
            text=text,
            account=account,
            group=True,
            channel="group:capacity",
            targets=list(actors),
            **kwargs,
        )

    async def fill(self, *, queued=1, collectors=32, seal=False):
        h = self.h
        h.core.policy = replace(
            h.core.policy, max_queued_turns=queued, max_collectors_per_conversation=collectors
        )
        request = self.message()
        result = await h.submit(request)
        if seal:
            h.clock.advance(5)
            with h.core.store.transaction():
                h.core._seal_due(h.clock())
        return request, result

    async def assert_read_only_rejection(self):
        h = self.h
        self.selected.fail = True
        request = self.message(account="b")
        before = h.core.store.db.total_changes
        head = h.facts(head=True)["head"]
        with self.assertRaises(Fault) as caught:
            await h.submit(request)
        self.assertEqual("queue_full", caught.exception.code)
        self.assertEqual(1, len(self.selected.requests))
        self.assertEqual(before, h.core.store.db.total_changes)
        self.assertEqual(head, h.facts(head=True)["head"])
        self.assertEqual(1, len(h.core.store.list("inbox")))
        # Identity is still authorized before capacity is exposed.
        self.assertIn(tuple(sorted(request["input"]["author"].items())), h.memory.accounts)

    async def test_full_collector_limit_rejects_before_model_selector_without_writes(self):
        await self.fill(queued=8, collectors=1)
        await self.assert_read_only_rejection()

    async def test_full_queued_limit_rejects_before_model_selector_without_writes(self):
        await self.fill(seal=True)
        await self.assert_read_only_rejection()

    async def test_full_queue_http_error_is_retryable_not_started(self):
        await self.fill()
        self.selected.fail = True
        request = self.message(account="b")
        app = create_app(core=self.h.core, tokens={"nonebot": "bridge"})
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="https://fixture"
        ) as client:
            response = await client.post(
                "/internal/v1/conversation/ingest-actors",
                json=request,
                headers={"Authorization": "Bearer bridge"},
            )
        self.assertEqual(429, response.status_code)
        error = response.json()
        self.assertEqual("queue_full", error["code"])
        self.assertEqual("not_started", error["execution_state"])
        self.assertTrue(error["retryable"])
        self.assertEqual(request["command"]["request_id"], error["request_id"])
        self.assertEqual(1, len(self.selected.requests))

    async def test_full_queue_replays_and_open_author_append_keep_frozen_selection(self):
        request, first = await self.fill()
        self.selected.fail = True
        self.selected.version = 20
        original = first["outcomes"][0]["receipt"]
        for same_key in (True, False):
            with self.subTest(same_key=same_key):
                replay = copy.deepcopy(request)
                replay["command"]["request_id"] = uid("retry")
                if not same_key:
                    replay["command"]["idempotency_key"] = uid("new-command")
                result = await self.h.submit(replay)
                self.assertEqual("duplicate", result["outcomes"][0]["state"])
                self.assertEqual(
                    original["receipt_id"], result["outcomes"][0]["receipt"]["receipt_id"]
                )
        appended = await self.h.submit(self.message(text="Append to the open author"))
        self.assertEqual("accepted", appended["outcomes"][0]["state"])
        self.assertEqual(
            original["collection_id"], appended["outcomes"][0]["receipt"]["collection_id"]
        )
        self.assertEqual(1, len(self.selected.requests))
        collections = self.h.core.store.list("collections", states=["collecting"])
        self.assertEqual([2], [len(c["messages"]) for c in collections])
        self.assertEqual([10], [c["model_selection"]["config_version"] for c in collections])

    async def test_over_capacity_fanout_selects_no_models_and_accepts_no_actor(self):
        h = self.h
        h.core.policy = replace(h.core.policy, max_collectors_per_conversation=1)
        request = self.message(actors=("actor:a", "actor:b"))
        head = h.facts(head=True)["head"]
        before = h.core.store.db.total_changes
        with self.assertRaises(Fault) as caught:
            await h.submit(request)
        self.assertEqual("queue_full", caught.exception.code)
        self.assertFalse(self.selected.requests)
        self.assertEqual(before, h.core.store.db.total_changes)
        self.assertEqual(head, h.facts(head=True)["head"])
        for table in (
            "conversations",
            "physicals",
            "inbox",
            "admissions",
            "collections",
            "commands",
        ):
            self.assertEqual([], h.core.store.list(table), table)

    async def test_final_transaction_rechecks_capacity_after_model_selector_wait(self):
        h = self.h
        h.core.policy = replace(h.core.policy, max_queued_turns=1)
        gate = self.selected.gate_first = asyncio.Event()
        slow = asyncio.create_task(h.submit(self.message(account="b")))
        try:
            async with asyncio.timeout(1):
                while not self.selected.requests:
                    await asyncio.sleep(0)
            self.selected.version = 20
            first = await h.submit(self.message(account="a"))
            self.assertEqual("accepted", first["outcomes"][0]["state"])
            head = h.facts(head=True)["head"]
        finally:
            gate.set()
        with self.assertRaises(Fault) as caught:
            await slow
        self.assertEqual("queue_full", caught.exception.code)
        self.assertEqual(2, len(self.selected.requests))
        self.assertEqual(head, h.facts(head=True)["head"])
        for table in ("physicals", "inbox", "admissions", "collections", "commands"):
            self.assertEqual(1, len(h.core.store.list(table)), table)
        self.assertEqual(
            20, h.core.store.list("collections")[0]["model_selection"]["config_version"]
        )

    async def test_same_command_admitted_during_identity_is_duplicate_when_full(self):
        h = self.h
        h.core.policy = replace(h.core.policy, max_queued_turns=1)
        identity = h.memory.identity
        entered, gate = asyncio.Event(), asyncio.Event()
        calls = 0

        async def delayed(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                await gate.wait()
            return await identity(*args)

        h.memory.identity = delayed
        request = self.message()
        slow = asyncio.create_task(h.submit(request))
        try:
            async with asyncio.timeout(1):
                await entered.wait()
            replay = copy.deepcopy(request)
            replay["command"]["request_id"] = uid("request")
            first = await h.submit(replay)
        finally:
            gate.set()
        duplicate = await slow
        self.assertEqual("duplicate", duplicate["outcomes"][0]["state"])
        self.assertEqual(
            first["outcomes"][0]["receipt"]["receipt_id"],
            duplicate["outcomes"][0]["receipt"]["receipt_id"],
        )
        self.assertEqual(1, len(self.selected.requests))
        self.assertEqual(1, len(h.core.store.list("inbox")))

    async def test_late_capacity_failure_rolls_back_every_actor_in_the_fanout(self):
        h = self.h
        h.core.policy = replace(h.core.policy, max_queued_turns=2)
        gate = self.selected.actor_gates["actor:b"] = asyncio.Event()
        pending = asyncio.create_task(
            h.submit(self.message(account="b", actors=("actor:a", "actor:b")))
        )
        try:
            async with asyncio.timeout(1):
                while len(self.selected.requests) < 2:
                    await asyncio.sleep(0)
            self.selected.version = 20
            admitted = await h.submit(self.message(account="a"))
            head = h.facts(head=True)["head"]
        finally:
            gate.set()
        with self.assertRaises(Fault) as caught:
            await pending
        self.assertEqual("queue_full", caught.exception.code)
        self.assertEqual(3, len(self.selected.requests))
        self.assertEqual(head, h.facts(head=True)["head"])
        for table in ("physicals", "inbox", "admissions", "collections", "commands"):
            self.assertEqual(1, len(h.core.store.list(table)), table)
        self.assertEqual(
            admitted["outcomes"][0]["receipt"]["receipt_id"],
            h.core.store.list("inbox")[0]["receipt"]["receipt_id"],
        )

    async def test_expired_collector_still_uses_the_future_queued_slot(self):
        await self.fill()
        self.h.clock.advance(5)
        await self.assert_read_only_rejection()
        self.assertEqual(
            ["collecting"], [c["state"] for c in self.h.core.store.list("collections")]
        )
        self.assertEqual([], self.h.turns())

    async def test_expired_collector_no_longer_uses_the_live_collector_limit(self):
        await self.fill(queued=2, collectors=1)
        self.h.clock.advance(5)
        self.selected.version = 20
        admitted = await self.h.submit(self.message(account="b"))
        self.assertEqual("accepted", admitted["outcomes"][0]["state"])
        self.assertEqual(2, len(self.selected.requests))
        self.assertEqual([10], [t["config_version"] for t in self.h.turns()])
        self.assertEqual(
            [20],
            [
                c["model_selection"]["config_version"]
                for c in self.h.core.store.list("collections", states=["collecting"])
            ],
        )

    async def test_edit_and_retract_can_release_a_full_queue(self):
        request, _ = await self.fill(seal=True)
        native_id = request["input"]["message_key"]["message_id"]
        self.selected.version = 20
        edited = await self.h.submit(self.message(message=native_id, revision=2, kind="edit"))
        self.assertEqual("accepted", edited["outcomes"][0]["state"])
        self.assertEqual(["cancelled"], [t["phase"] for t in self.h.turns()])
        self.assertEqual(2, len(self.selected.requests))
        retracted = await self.h.submit(
            self.message(message=native_id, revision=3, kind="retract", actors=(), grants=[])
        )
        self.assertEqual([], retracted["outcomes"])
        self.assertEqual([], self.h.core.store.list("collections", states=["collecting"]))
        self.selected.version = 30
        admitted = await self.h.submit(self.message(account="b"))
        self.assertEqual("accepted", admitted["outcomes"][0]["state"])
        self.assertEqual(3, len(self.selected.requests))

    async def test_full_queue_does_not_turn_missing_authority_into_admission(self):
        await self.fill()
        self.selected.fail = True
        request = self.message(account="b", grants=[])
        result = await self.h.submit(request)
        self.assertEqual("forbidden", result["outcomes"][0]["state"])
        self.assertIsNone(result["person_id"])
        self.assertIsNone(result["outcomes"][0]["receipt"])
        self.assertEqual(1, len(self.h.core.store.list("inbox")))
        self.assertNotIn(tuple(sorted(request["input"]["author"].items())), self.h.memory.accounts)
        self.assertEqual(1, len(self.selected.requests))

    async def test_identity_rejection_precedes_capacity(self):
        await self.fill()

        async def revoked(*args):
            raise Fault("forbidden")

        self.h.memory.identity = revoked
        with self.assertRaises(Fault) as caught:
            await self.h.submit(self.message(account="b"))
        self.assertEqual("forbidden", caught.exception.code)
        self.assertEqual(1, len(self.selected.requests))
        self.assertEqual(1, len(self.h.core.store.list("inbox")))

    async def test_role_revoked_during_identity_is_rechecked_before_capacity(self):
        await self.fill()
        identity = self.h.memory.identity

        async def revoked(*args):
            current = await identity(*args)
            self.h.core.roles.pop("actor:a")
            return current

        self.h.memory.identity = revoked
        result = await self.h.submit(self.message(account="b"))
        self.assertEqual("forbidden", result["outcomes"][0]["state"])
        self.assertIsNone(result["outcomes"][0]["admission"])
        self.assertEqual(1, len(self.h.core.store.list("inbox")))
        self.assertEqual(1, len(self.selected.requests))
