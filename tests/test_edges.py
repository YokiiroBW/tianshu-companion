import asyncio
import copy
import unittest

from support import Harness
from tianshu_companion.contracts import Fault


class EdgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness()

    async def asyncTearDown(self):
        await self.h.core.close()

    async def test_group_role_targets_have_independent_collectors(self):
        h = self.h
        await h.ingest(text="A你好", group=True, channel="group:1", actor="actor:a")
        await h.ingest(text="B也听听", group=True, channel="group:1", actor="actor:b")
        h.clock.advance(5)
        await h.cycles()
        self.assertEqual("sent", h.turns()[0]["phase"])
        self.assertEqual(
            [["actor:a"], ["actor:b"]],
            [t["bundle"]["messages"][0]["target_actor_ids"] for t in h.turns()],
        )
        self.assertEqual(
            ["actor:a", "actor:a", "actor:b", "actor:b"], [r["actor_id"] for r in h.sender.calls]
        )

    async def test_max_wait_marks_fragment_and_no_target_group_is_observed(self):
        h = self.h
        from dataclasses import replace

        h.core.policy = replace(h.core.policy, max_wait_ms=6000)
        await h.ingest()
        h.clock.advance(4)
        await h.ingest()
        h.clock.advance(2)
        await h.cycles()
        self.assertEqual("max_wait", h.turns()[0]["bundle"]["close_reason"])
        self.assertTrue(h.turns()[0]["bundle"]["possibly_incomplete"])
        self.assertEqual("observed", h.turns()[0]["phase"])
        await h.ingest(group=True, channel="group:untargeted", targets=[])
        h.clock.advance(5)
        await h.cycles()
        self.assertTrue(all(t["phase"] == "observed" for t in h.turns()))
        self.assertFalse(h.gateway.calls)

    async def test_sealed_edit_cancels_old_generation_and_partial_cancel_lists_sent(self):
        h = self.h
        h.gateway.gates[1] = asyncio.Event()
        await h.ingest(text="old", message="source:edit")
        h.clock.advance(5)
        await h.cycles()
        await h.ingest(text="replacement", message="source:edit", revision=2, kind="edit")
        self.assertEqual("cancelled", h.turns()[0]["phase"])
        h.clock.advance(5)
        for _ in range(20):
            await h.cycles(1)
            if h.sender.calls:
                break
        # Let the first real receipt commit, then stop before the next submission.
        await asyncio.sleep(0)
        turn = h.turns()[1]
        result = await h.cancel(turn)
        self.assertIn(result["state"], {"partially_cancelled", "unknown"})
        await h.cycles()
        self.assertEqual("partial", h.turns()[1]["delivery_state"])
        self.assertTrue(result["already_sent_reply_ids"])

    async def test_cancel_expected_version_and_expired_source_fail_closed(self):
        h = self.h
        await h.ingest()
        h.clock.advance(5)
        h.gateway.gates[1] = asyncio.Event()
        await h.cycles()
        turn = h.turns()[0]
        stale = copy.deepcopy(turn)
        stale["version"] = 1
        with self.assertRaises(Fault) as error:
            await h.cancel(stale)
        self.assertEqual("version_conflict", error.exception.code)
        h.clock.advance(3600)
        h.gateway.gates[1].set()
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertFalse(h.sender.calls)

    async def test_memory_unavailable_after_acceptance_does_not_hold_slot(self):
        h = self.h
        await h.ingest()
        h.memory.unavailable = True
        h.clock.advance(5)
        await h.cycles()
        h.clock.advance(5)
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertEqual("blocked_scope", h.core.store.list("outbox")[0]["state"])
        self.assertFalse(h.gateway.calls)
