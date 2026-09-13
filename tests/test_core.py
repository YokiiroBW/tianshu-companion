import asyncio
import copy
import tempfile
import unittest
from pathlib import Path

from support import Harness
from tianshu_companion.contracts import Fault
from tianshu_companion.core import ACTIVE


class CoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness()

    async def asyncTearDown(self):
        await self.h.core.close()

    async def replace(self, **policy):
        await self.h.core.close()
        self.h = Harness(**policy)

    async def test_sliding_window_duplicates_and_old_timers(self):
        h = self.h
        r = h.request(text="第一句")
        first = await h.core.ingest("nonebot", r)
        old = h.core.store.get("collections", first["collection_id"])
        h.clock.advance(2)
        await h.ingest(text="第二句")
        h.clock.advance(4)
        await h.ingest(text="等等，第三句")
        copy_r = copy.deepcopy(r)
        copy_r["command"]["idempotency_key"] = "new:retry"
        duplicate = await h.core.ingest("nonebot", copy_r)
        self.assertTrue(duplicate["deduplicated"])
        h.core.timer(old["id"], old["revision"], old["deadline"])
        self.assertFalse(h.turns())
        h.clock.advance(5)
        await h.cycles()
        self.assertEqual(1, len(h.turns()))
        self.assertEqual(3, len(h.turns()[0]["bundle"]["messages"]))
        self.assertEqual(1, len(h.gateway.calls))
        self.assertEqual("sent", h.turns()[0]["phase"])

    async def test_deadline_equality_and_late_timer_do_not_extend_old_group(self):
        h = self.h
        first = await h.ingest(text="old")
        h.clock.advance(5)
        second = await h.ingest(text="boundary")
        self.assertNotEqual(first["collection_id"], second["collection_id"])
        self.assertEqual(
            ["old"], [m["parts"][0]["text"] for m in h.turns()[0]["bundle"]["messages"]]
        )
        h.clock.advance(50)
        third = await h.ingest(text="late")
        self.assertNotEqual(second["collection_id"], third["collection_id"])
        self.assertEqual(2, len(h.turns()))

    async def test_group_authors_and_other_conversations_are_separate(self):
        h = self.h
        a = await h.ingest(account="a", group=True, channel="group:1")
        h.clock.advance(1)
        b = await h.ingest(account="b", group=True, channel="group:1")
        h.clock.advance(1)
        a2 = await h.ingest(account="a", group=True, channel="group:1")
        private = await h.ingest(account="a")
        self.assertEqual(a["collection_id"], a2["collection_id"])
        self.assertNotEqual(a["collection_id"], b["collection_id"])
        self.assertNotEqual(a["conversation_id"], private["conversation_id"])
        self.assertEqual(a["person_id"], private["person_id"])
        h.clock.advance(5)
        await h.cycles()
        bundle = next(
            t["bundle"] for t in h.turns() if t["bundle"]["collection_id"] == a["collection_id"]
        )
        self.assertEqual(
            ["a", "a"], [m["author"]["immutable_account_id"] for m in bundle["messages"]]
        )
        self.assertEqual(b["receipt_id"], bundle["context_refs"][0]["receipt_id"])

    async def test_continuous_short_sentences_have_no_implicit_max_wait(self):
        h = self.h
        for _ in range(30):
            await h.ingest(text="还有")
            h.clock.advance(4)
            await h.cycles(1)
        self.assertFalse(h.turns())
        h.clock.advance(1)
        await h.cycles()
        self.assertEqual(30, len(h.turns()[0]["bundle"]["messages"]))

    async def test_two_overlapping_turns_no_overtaking_or_third_active(self):
        await self.replace(silence_ms=0)
        h = self.h
        h.gateway.gates[1] = asyncio.Event()
        await h.ingest(text="T1")
        await h.ingest(text="T2", actor="actor:b")
        await h.ingest(text="T3")
        await h.cycles()
        self.assertEqual([1, 2], [n for n, _ in h.gateway.calls])
        self.assertEqual(["generating", "ready_to_send", "queued"], [t["phase"] for t in h.turns()])
        self.assertFalse(h.sender.calls)
        self.assertEqual(2, sum(t["phase"] in ACTIVE for t in h.turns()))
        h.gateway.gates[1].set()
        await h.cycles(70)
        self.assertEqual([1, 1, 2, 2, 3, 3], [r["turn_sequence"] for r in h.sender.calls])
        self.assertEqual([1, 2, 3], [n for n, _ in h.gateway.calls])
        self.assertTrue(all(t["phase"] == "sent" for t in h.turns()))

    async def test_dependency_prepares_early_but_waits_for_delivered_result(self):
        await self.replace(silence_ms=0)
        h = self.h
        h.gateway.gates[1] = asyncio.Event()
        await h.ingest(text="制定方案")
        await h.ingest(text="按你刚才的方案办")
        await h.cycles()
        self.assertEqual(2, sum(s["known_version"] is None for s in h.memory.selections))
        self.assertEqual("waiting_dependency", h.turns()[1]["phase"])
        self.assertEqual([1], [n for n, _ in h.gateway.calls])
        h.gateway.gates[1].set()
        await h.cycles(60)
        self.assertEqual([1, 2], [n for n, _ in h.gateway.calls])
        self.assertIn('"delivery_state":"sent"', h.gateway.calls[1][1][1]["content"])

    async def test_reservation_capacity_and_resource_limit_preserve_input(self):
        await self.replace(max_queued_turns=1, max_collection_messages=2)
        h = self.h
        first = await h.ingest(text="one")
        await h.ingest(text="two")
        with self.assertRaises(Fault) as error:
            await h.ingest(text="three")
        self.assertEqual("queue_full", error.exception.code)
        self.assertEqual(2, len(h.core.store.list("inbox")))
        turn = h.turns()[0]
        self.assertTrue(turn["bundle"]["possibly_incomplete"])
        with self.assertRaises(Fault):
            await h.ingest(account="b")
        await h.cycles()
        self.assertEqual("observed", h.turns()[0]["phase"])
        self.assertFalse(h.gateway.calls)
        continuation = await h.ingest(text="three complete")
        collection = h.core.store.get("collections", continuation["collection_id"])
        self.assertEqual(first["collection_id"], collection["continuation_of"])
        h.clock.advance(5)
        await h.cycles()
        self.assertEqual("sent", h.turns()[1]["phase"])

    async def test_partial_send_unknown_closes_without_replay_and_late_receipt(self):
        await self.replace(silence_ms=0, delivery_reconcile_timeout_ms=1000)
        h = self.h
        h.sender.states = ["sent", "unknown"]
        await h.ingest()
        await h.ingest(text="next")
        await h.cycles()
        self.assertEqual("reconciling", h.turns()[0]["phase"])
        self.assertEqual(2, len(h.sender.calls))
        h.clock.advance(1)
        await h.cycles(50)
        self.assertEqual("closed_unknown", h.turns()[0]["phase"])
        self.assertEqual("sent", h.turns()[1]["phase"])
        self.assertEqual(4, len(h.sender.calls))
        unknown_request = h.sender.calls[1]
        h.core.record_receipt(unknown_request["reply_id"], h.sender.receipt(unknown_request))
        await h.core.flush_outbox()
        self.assertEqual(2, len(h.memory.commits))
        projections = h.core.store.list("outbox", states=["local_projection"])
        self.assertEqual(1, len(projections))
        h.contracts.check("web#projection_event", projections[0]["event"])
        self.assertEqual("closed_unknown", h.turns()[0]["phase"])

    async def test_cancel_while_sending_retains_real_sent_fact(self):
        await self.replace(silence_ms=0)
        h = self.h
        h.sender.gate = asyncio.Event()
        await h.ingest()
        await h.cycles()
        result = await h.cancel(h.turns()[0])
        self.assertEqual("unknown", result["state"])
        self.assertFalse(result["external_actions_rolled_back"])
        h.sender.gate.set()
        await h.cycles()
        self.assertEqual(1, len(h.sender.calls))
        self.assertEqual("cancelled", h.turns()[0]["phase"])
        self.assertEqual("partial", h.turns()[0]["delivery_state"])
        self.assertEqual("sent", h.core.store.list("replies")[0]["state"])

    async def test_cancel_generation_and_ordinary_wait_words(self):
        await self.replace(silence_ms=0)
        h = self.h
        h.gateway.gates[1] = asyncio.Event()
        await h.ingest()
        await h.ingest(text="等等，普通续句")
        await h.cycles()
        self.assertEqual("generating", h.turns()[0]["phase"])
        result = await h.cancel(h.turns()[0])
        self.assertEqual("cancelled", result["state"])
        await h.cycles()
        self.assertEqual([2, 2], [r["turn_sequence"] for r in h.sender.calls])

    async def test_scope_revision_before_send_invalidates_old_candidate(self):
        await self.replace(silence_ms=0)
        h = self.h
        h.gateway.gates[1] = asyncio.Event()
        await h.ingest(text="昨天那个安排")
        await h.cycles()
        h.memory.scope_version = 2
        h.gateway.gates[1].set()
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertEqual("scope_changed", h.turns()[0]["failure"])
        self.assertFalse(h.sender.calls)

    async def test_edits_retracts_and_stale_revisions(self):
        h = self.h
        r = h.request(text="old", message="message:edit")
        original = await h.core.ingest("nonebot", r)
        h.clock.advance(1)
        await h.ingest(text="new", message="message:edit", revision=2, kind="edit")
        await h.ingest(text="other")
        stale = copy.deepcopy(r)
        stale["command"]["idempotency_key"] = "retry:old"
        await h.core.ingest("nonebot", stale)
        c = h.core.store.get("collections", original["collection_id"])
        self.assertEqual(["new", "other"], [m["parts"][0]["text"] for m in c["messages"]])
        await h.ingest(message="message:edit", revision=3, kind="retract")
        h.clock.advance(5)
        await h.cycles()
        self.assertEqual("other", h.turns()[0]["bundle"]["messages"][0]["parts"][0]["text"])

    async def test_restart_remaining_window_and_unknown_send(self):
        await self.h.core.close()
        with tempfile.TemporaryDirectory() as directory:
            h = self.h = Harness(Path(directory) / "core.db", delivery_reconcile_timeout_ms=1000)
            await h.ingest()
            h.clock.advance(2)
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            self.assertFalse(h.turns())
            h.clock.advance(3)
            h.sender.gate = asyncio.Event()
            await h.cycles()
            self.assertEqual(1, len(h.sender.calls))
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            self.assertEqual("reconciling", h.turns()[0]["phase"])
            h.clock.advance(2)
            await h.cycles()
            self.assertEqual("closed_unknown", h.turns()[0]["phase"])
            self.assertEqual(1, len(h.sender.calls))
            await h.core.close()
            h.core = Harness().core

    async def test_outbox_retry_schema_and_no_duplicate_state_commit(self):
        await self.replace(silence_ms=0)
        h = self.h
        await h.ingest()
        await h.cycles()
        for turn in h.turns():
            h.contracts.check("conversation#turn", h.core.turn_wire(turn))
        h.memory.fail_commit = True
        await h.core.flush_outbox()
        outbox = h.core.store.list("outbox")[0]
        self.assertEqual("pending", outbox["state"])
        self.assertEqual(1, outbox["attempts"])
        h.memory.fail_commit = False
        h.clock.advance(2)
        await h.core.flush_outbox()
        await h.core.flush_outbox()
        self.assertEqual(1, len(h.memory.commits))
        event = h.memory.commits[0]
        h.contracts.check("conversation#committed_event", event)
        self.assertTrue(all(s["archive_state"] == "pending" for s in event["sources"]))

    async def test_source_spoof_idempotency_conflict_and_unavailable(self):
        h = self.h
        request = h.request()
        spoof = copy.deepcopy(request)
        spoof["author"]["immutable_account_id"] = "other"
        with self.assertRaises(Fault):
            await h.core.ingest("nonebot", spoof)
        await h.core.ingest("nonebot", request)
        conflict = copy.deepcopy(request)
        conflict["parts"][0]["text"] = "changed"
        with self.assertRaises(Fault) as error:
            await h.core.ingest("nonebot", conflict)
        self.assertEqual("idempotency_conflict", error.exception.code)
        h.memory.unavailable = True
        with self.assertRaises(Fault) as error:
            await h.ingest()
        self.assertEqual("dependency_unavailable", error.exception.code)
        self.assertEqual(1, len(h.core.store.list("inbox")))

    async def test_model_failure_and_empty_result_release_slot(self):
        await self.replace(silence_ms=0)
        h = self.h
        h.gateway.fail = True
        await h.ingest()
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        h.gateway.fail = False
        h.gateway.segments = []
        await h.ingest()
        await h.cycles()
        self.assertEqual("observed", h.turns()[1]["phase"])
        self.assertFalse(h.sender.calls)
