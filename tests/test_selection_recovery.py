"""A changed v2 selection is rebuilt by the existing durable preparation owner."""

import copy
import unittest

import test_memory_v2 as evidence
from tianshu_companion.contracts import Fault, canonical
from tianshu_companion.preparation_retry import retry_preparation, verify_prepared_memory


class SelectionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await evidence.CoreEvidenceTests.asyncSetUp(self)
        self.queries = []
        self.version = 1
        self.denied = False
        original = self.h.memory.query_context

        async def query(*args):
            self.queries.append(args)
            if self.denied:
                raise Fault("forbidden")
            result = await original(*args)
            result["bundle"]["test_candidate_revision"] = self.version
            result["evidence"][0]["text"] = f"第{self.version}版可见原话"
            return result

        self.h.memory.query_context = query

    def change_on_verify(self, *, repeatedly=False, revoke=False):
        calls = []

        async def verify(origin, scope, bundle, checks=()):
            calls.append(copy.deepcopy(checks))
            if len(calls) == 1 or repeatedly:
                self.version += 1
                self.denied = revoke
            if bundle["test_candidate_revision"] != self.version:
                raise Fault("scope_changed")

        self.h.memory.verify_context = verify
        return calls

    async def start(self):
        await self.h.ingest(text="请回忆刚才提到的计划。")
        await self.h.cycles()
        return self.h.turns()[-1]

    async def test_new_episode_reselects_all_material_before_one_model_call(self):
        calls = self.change_on_verify()
        first = await self.start()
        self.assertEqual("preparing", first["phase"])
        self.assertIsNone(first["preparation"])
        self.assertIsNone(first["memory_context"])
        self.assertEqual([], first["context_checks"])
        self.assertEqual([], self.h.gateway.calls)
        self.h.clock.advance(0.3)
        await self.h.cycles(80)
        fresh = self.h.turns()[-1]
        self.assertEqual("sent", fresh["phase"])
        self.assertEqual((2, 1), (len(self.queries), len(self.h.gateway.calls)))
        self.assertEqual(2, fresh["memory_context"]["bundle"]["test_candidate_revision"])
        prompt = canonical(self.h.gateway.calls[-1][1])
        self.assertIn("第2版可见原话", prompt)
        self.assertNotIn("第1版可见原话", prompt)
        self.assertGreaterEqual(len(calls), 3)
        self.assertEqual(1, fresh["preparation_retries"])

    async def test_grant_revocation_reselection_cannot_use_old_material(self):
        self.change_on_verify(revoke=True)
        await self.start()
        self.h.clock.advance(0.3)
        await self.h.cycles()
        fresh = self.h.turns()[-1]
        self.assertEqual(("failed", "forbidden"), (fresh["phase"], fresh["failure"]))
        self.assertEqual(2, len(self.queries))
        self.assertIsNone(fresh["preparation"])
        self.assertIsNone(fresh["memory_context"])
        self.assertEqual([], self.h.gateway.calls)

    async def test_continuous_change_keeps_one_deadline_and_bounded_attempts(self):
        self.change_on_verify(repeatedly=True)
        first = await self.start()
        started = first["preparation_retry_started_at"]
        for _ in range(6):
            self.h.clock.advance(2.1)
            await self.h.cycles()
        fresh = self.h.turns()[-1]
        self.assertEqual("failed", fresh["phase"])
        self.assertEqual(5, fresh["preparation_retries"])
        self.assertEqual(started, fresh["preparation_retry_started_at"])
        self.assertEqual(6, len(self.queries))
        self.assertEqual([], self.h.gateway.calls)

    async def test_expired_recovery_deadline_does_not_query_again(self):
        self.change_on_verify(repeatedly=True)
        first = await self.start()
        self.h.clock.advance(30.1)
        await self.h.cycles()
        fresh = self.h.turns()[-1]
        self.assertEqual(("failed", "timeout"), (fresh["phase"], fresh["failure"]))
        self.assertEqual(
            first["preparation_retry_started_at"], fresh["preparation_retry_started_at"]
        )
        self.assertEqual(1, len(self.queries))
        self.assertEqual([], self.h.gateway.calls)

    async def test_cancelled_preparation_never_reselects(self):
        self.change_on_verify()
        first = await self.start()
        await self.h.cancel(first)
        self.h.clock.advance(1)
        await self.h.cycles()
        self.assertEqual("cancelled", self.h.turns()[-1]["phase"])
        self.assertEqual(1, len(self.queries))
        self.assertEqual([], self.h.gateway.calls)

    async def test_combined_old_checks_do_not_masquerade_as_invalid_current_selection(self):
        turn = dict(origin={}, scope={}, memory_context=dict(bundle={}), model_calls=0)
        calls = []

        async def verify(*args):
            calls.append(args[-1])
            if args[-1]:
                raise Fault("scope_changed")

        self.h.memory.verify_context = verify
        with self.assertRaises(Fault) as caught:
            await verify_prepared_memory(self.h.memory, turn, ["old-check"])
        self.assertEqual([["old-check"], []], calls)
        self.assertFalse(getattr(caught.exception, "memory_selection_invalid", False))

    async def test_model_tool_and_expression_boundaries_never_mark_or_rebuild_selection(self):
        calls = []

        async def verify(*args):
            calls.append(args[-1])
            raise Fault("scope_changed")

        self.h.memory.verify_context = verify
        for boundary in (
            dict(model_calls=1),
            dict(native_execution={"id": "tool:started"}),
            dict(expression_id="expression:started"),
        ):
            with self.subTest(boundary=boundary):
                turn = dict(
                    origin={},
                    scope={},
                    memory_context=dict(bundle={}),
                    model_calls=0,
                    source_deadline=self.h.clock() + 60,
                    preparation={"old": True},
                )
                turn.update(boundary)
                with self.assertRaises(Fault) as caught:
                    await verify_prepared_memory(self.h.memory, turn, ["old-check"])
                self.assertFalse(getattr(caught.exception, "memory_selection_invalid", False))
                caught.exception.memory_selection_invalid = True
                self.assertFalse(retry_preparation(turn, caught.exception, self.h.clock()))
                self.assertEqual({"old": True}, turn["preparation"])
        self.assertEqual(3, len(calls))

    async def test_existing_reply_intent_blocks_owner_rebuild(self):
        async def verify(*args):
            turn = self.h.turns()[-1]
            self.h.core.store.put(
                "replies",
                dict(
                    id="reply:intent",
                    turn_id=turn["id"],
                    conversation_id=turn["conversation_id"],
                    sequence=1,
                    state="pending",
                    text="未发送",
                    receipt=None,
                    request=None,
                    unknown_since=None,
                    attempted_at=None,
                ),
            )
            raise Fault("scope_changed")

        self.h.memory.verify_context = verify
        fresh = await self.start()
        self.assertEqual("failed", fresh["phase"])
        self.assertNotIn("preparation_retries", fresh)
        self.assertEqual(1, len(self.queries))
        self.assertEqual([], self.h.gateway.calls)
