"""Durable automatic learning, model selection and unknown-result reconciliation."""

import asyncio
import copy
import json
import tempfile
import unittest
from pathlib import Path

from support import Harness
from tianshu_companion.contracts import Fault
from tianshu_companion.model_selection import ModelSelection

TEXT = "晚上不喝咖啡，白天偶尔可以"


class MemoryLearningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.h = Harness(Path(self.temp.name) / "learning.sqlite", silence_ms=0)
        self.calls, self.generated, self.selections = [], [], []
        self.completed = None
        self.lose_response = False
        self.reject = False
        self.model_fail = False
        self.h.memory.process_candidate = self.process
        self.original_generate = self.h.gateway.generate
        self.h.gateway.generate = self.generate

    async def asyncTearDown(self):
        await self.h.core.close()

    async def process(self, job, output):
        self.calls.append((job, copy.deepcopy(output)))
        if self.completed:
            return self.completed
        if output is None:
            return dict(
                state="pending",
                result=None,
                content=[dict(index=0, text=TEXT, sent_at="2026-10-11T00:00:00Z")],
            )
        if self.reject:
            self.reject = False
            return dict(state="rejected", result={"error_code": "invalid_input"}, content=[])
        self.completed = dict(
            state="completed",
            content=[],
            result=dict(
                state="committed", record_ids=["record:learned"], group_ids=["group:learned"]
            ),
        )
        if self.lose_response:
            self.lose_response = False
            raise Fault("result_unknown", unknown=True)
        return self.completed

    async def generate(self, turn, messages):
        if not turn["id"].startswith("memory-generation:"):
            return await self.original_generate(turn, messages)
        self.generated.append(copy.deepcopy(turn))
        if self.model_fail:
            raise Fault("dependency_unavailable")
        return [
            json.dumps(
                {
                    "memories": [
                        dict(
                            statement=TEXT,
                            conditions=["白天偶尔可以"],
                            negations=["晚上不喝咖啡"],
                            valid_time="current",
                            evidence=[dict(source_index=0, quote=TEXT)],
                        )
                    ]
                }
            )
        ], {}

    async def flush(self):
        await self.h.core.flush_outbox()
        await self.h.core.memory_learning.work()

    async def chat(self):
        await self.h.ingest(text=TEXT)
        await self.h.cycles()
        self.assertEqual(self.h.turns()[-1]["phase"], "sent")

    def task(self):
        return self.h.core.store.list("metadata", "memory:learning")[0]

    async def restart(self):
        await self.h.core.close()
        self.h.core = self.h.new_core()
        self.h.core.recover()

    async def test_automatic_processing_uses_memory_model_and_separate_provider_session(self):
        await self.chat()
        outer = self

        class Selector:
            async def select(self, request):
                outer.selections.append(request)
                return ModelSelection(42, outer.h.clock() + 3600)

        self.h.core.default_model_selector = Selector()
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["receipt"]["state"], "committed")
        self.assertEqual(self.selections[0].function_id, "memory")
        self.assertEqual(self.generated[0]["config_version"], 42)
        self.assertNotEqual(
            self.generated[0]["conversation_id"], self.h.turns()[0]["conversation_id"]
        )
        self.assertIsNone(self.task()["output"])
        from tianshu_companion.runtime_capabilities import read_capabilities

        status = read_capabilities(self.h.core)["automatic_memory_candidates"]
        self.assertTrue(status["retained_learning"]["completed"])
        self.assertFalse(status["retained_learning"]["pending"])

    async def test_lost_commit_ack_reconciles_after_restart_without_model_or_commit_replay(self):
        await self.chat()
        self.lose_response = True
        await self.flush()
        self.assertEqual(self.task()["state"], "pending")
        self.assertIsNotNone(self.task()["output"])
        await self.restart()
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(len(self.generated), 1)
        self.assertEqual(sum(output is not None for _, output in self.calls), 1)

    async def test_model_failure_keeps_pending_and_recovers(self):
        await self.chat()
        self.model_fail = True
        await self.flush()
        self.assertEqual(self.task()["state"], "pending")
        self.assertFalse(any(output is not None for _, output in self.calls))
        await self.restart()
        self.model_fail = False
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")

    async def test_malformed_receipt_does_not_discard_uncertain_proposal(self):
        await self.chat()
        self.lose_response = True
        await self.flush()
        proposal = self.task()["output"]

        async def malformed(job, output):
            raise Fault("invalid_input")

        self.h.memory.process_candidate = malformed
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.task()["output"], proposal)
        self.assertEqual(len(self.generated), 1)
        self.h.memory.process_candidate = self.process
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")

    async def test_cancelled_commit_retains_proposal_and_backoff_for_reconciliation(self):
        await self.chat()
        started = asyncio.Event()

        async def slow_commit(job, output):
            if output is None:
                return await self.process(job, output)
            started.set()
            await asyncio.Event().wait()

        self.h.memory.process_candidate = slow_commit
        worker = asyncio.create_task(self.flush())
        await asyncio.wait_for(started.wait(), 3)
        worker.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await worker
        self.assertIsNotNone(self.task()["output"])
        self.assertGreater(self.task()["deadline"], self.h.clock())
        self.h.memory.process_candidate = self.process
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(len(self.generated), 1)

    async def test_definitively_invalid_output_is_regenerated(self):
        await self.chat()
        self.reject = True
        await self.flush()
        self.assertIsNone(self.task()["output"])
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(len(self.generated), 2)
        self.assertEqual(self.task()["state"], "completed")

    async def test_disabled_candidates_do_not_run_model_or_write(self):
        await self.chat()
        await self.h.core.close()
        self.h.core = self.h.new_core(automatic_memory_candidates=False)
        self.h.core.recover()
        await self.flush()
        self.assertEqual(self.generated, [])
        self.assertEqual(self.calls, [])

    async def test_preexisting_delivered_candidate_is_backfilled(self):
        await self.chat()
        await self.h.core._flush_outbox()
        self.h.core.store.delete("metadata", self.task()["id"])
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
