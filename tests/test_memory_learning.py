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
        self.extraction_messages = []
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
        self.extraction_messages.append(copy.deepcopy(messages))
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
        await self.restart()
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(len(self.generated), 2)
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(len(self.extraction_messages[0]), 2)
        self.assertIn("rejected without committing", self.extraction_messages[1][1]["content"])

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

    async def test_failures_are_durably_bounded_across_restarts(self):
        await self.chat()
        self.model_fail = True
        for attempt in range(6):
            await self.flush()
            self.assertEqual(self.task()["attempts"], attempt + 1)
            await self.restart()
            self.h.clock.advance(301)
        self.assertEqual(self.task()["state"], "blocked")
        self.assertEqual(self.task()["last_error"], "dependency_unavailable")
        await self.flush()
        self.assertEqual(len(self.generated), 6)
        self.assertEqual(self.task()["attempts"], 6)
        from tianshu_companion.runtime_capabilities import read_capabilities

        self.assertTrue(
            read_capabilities(self.h.core)["automatic_memory_candidates"]["retained_learning"][
                "blocked"
            ]
        )

    async def test_uncertain_output_survives_terminal_retry_state(self):
        await self.chat()
        self.lose_response = True
        await self.flush()
        proposal = copy.deepcopy(self.task()["output"])

        async def unavailable(job, output):
            raise Fault("dependency_unavailable")

        self.h.memory.process_candidate = unavailable
        for _ in range(2):
            self.h.clock.advance(30)
            await self.flush()
        self.assertEqual(self.task()["state"], "pending")
        self.assertEqual(self.task()["output"], proposal)
        self.assertEqual(len(self.generated), 1)

        self.h.memory.process_candidate = self.process
        response = await self.h.core.memory_learning.reconcile(self.task()["job_id"])
        self.assertEqual(response["state"], "completed")
        self.assertEqual(self.task()["attempts"], 3)
        self.assertEqual(len(self.generated), 1)
        self.assertEqual(sum(output is not None for _, output in self.calls), 1)

    async def test_three_timeouts_cool_down_then_recover_after_restart_without_chat_replay(self):
        await self.chat()
        chat_calls = len(self.h.gateway.calls)
        self.model_fail = True
        for _ in range(3):
            await self.flush()
            self.h.clock.advance(10)
        self.assertEqual(self.task()["state"], "pending")
        self.assertGreater(self.task()["deadline"], self.h.clock())
        self.assertEqual(self.task()["attempts"], 3)
        await self.restart()
        self.model_fail = False
        await self.flush()
        self.assertEqual(len(self.generated), 3)
        self.h.clock.advance(60)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["attempts"], 4)
        self.assertEqual(len(self.h.gateway.calls), chat_calls)
        self.assertEqual(sum(output is not None for _, output in self.calls), 1)

    async def test_revoked_source_during_cooldown_never_regenerates(self):
        await self.chat()
        self.model_fail = True
        for _ in range(3):
            await self.flush()
            self.h.clock.advance(10)

        async def revoked(job, output):
            self.assertIsNone(output)
            return dict(state="invalidated", result={"state": "invalidated"}, content=[])

        self.h.memory.process_candidate = revoked
        self.h.clock.advance(60)
        await self.flush()
        self.assertEqual(self.task()["state"], "invalidated")
        self.assertEqual(len(self.generated), 3)

    async def test_legacy_blocked_transient_job_keeps_attempts_and_original_proposal(self):
        await self.chat()
        self.lose_response = True
        await self.flush()
        item = self.task()
        proposal = item["output"]
        item.update(state="blocked", attempts=3, last_error="dependency_unavailable")
        self.h.core.store.put("metadata", item)
        # The older three-attempt worker had no upgrade scan cursor.
        self.h.core.store.delete("metadata", "memory-learning-recovery")
        await self.restart()
        await self.flush()
        self.assertEqual(self.task()["attempts"], 3)
        self.assertEqual(self.task()["output"], proposal)
        self.h.clock.advance(61)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(len(self.generated), 1)
        self.assertEqual(sum(output is not None for _, output in self.calls), 1)

    async def test_memory_tool_publishes_complete_sourced_unit_schema(self):
        await self.chat()
        turn = self.h.turns()[-1]
        tool = next(
            t
            for t in self.h.core.role_actions.domain_tools(turn)
            if t["function"]["name"] == "memory_propose"
        )
        parameters = tool["function"]["parameters"]["properties"]
        target = parameters["target"]
        self.assertEqual(
            set(target["required"]),
            {"category", "field_key", "item_key", "record_id", "expected_version"},
        )
        unit = parameters["units"]["items"]
        self.assertEqual(
            set(unit["required"]),
            {"statement", "conditions", "negations", "valid_time", "uncertainty", "reality"},
        )
        self.assertNotIn("sources", unit["properties"])
        self.assertFalse(unit["additionalProperties"])

    def dialogue_memory_status(self):
        system = self.h.gateway.calls[-1][1][0]["content"]
        return json.loads(system.split("Trusted memory processing: ", 1)[1].split("\n", 1)[0])

    async def test_dialogue_reports_automatic_learning_when_explicit_skill_is_disabled(self):
        from test_skills import manage

        await manage(self.h, "skill.disable", dict(skill_id="memory.propose"))
        await self.chat()
        self.assertEqual(
            self.dialogue_memory_status()["automatic_extraction_after_delivery"], "enabled"
        )
        offered = self.h.core.role_actions.tools(self.h.turns()[-1])
        self.assertNotIn("memory_propose", {t["function"]["name"] for t in offered})
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")

    async def test_dialogue_does_not_report_disabled_background_learning_as_enabled(self):
        await self.h.core.close()
        self.h.core = self.h.new_core(automatic_memory_candidates=False)
        await self.chat()
        self.assertEqual(
            self.dialogue_memory_status()["automatic_extraction_after_delivery"], "disabled"
        )
        await self.flush()
        self.assertEqual(self.generated, [])
        self.assertEqual(self.calls, [])

    async def test_native_catalog_keeps_completed_empty_recall_separate_from_disabled_skill(self):
        from test_skills import manage

        await manage(self.h, "skill.disable", dict(skill_id="memory.propose"))
        captured = []

        async def complete(turn, messages, **kwargs):
            captured.append(copy.deepcopy(messages))
            text, receipt = await self.original_generate(turn, messages)
            return {"role": "assistant", "content": "".join(text)}, receipt

        self.h.gateway.complete = complete
        await self.chat()
        status = self.dialogue_memory_status()
        self.assertEqual(status["automatic_recall_before_reply"], "completed")
        self.assertEqual(status["recalled_units"], 0)
        self.assertEqual(status["automatic_extraction_after_delivery"], "enabled")
        system = captured[0][0]["content"]
        self.assertGreater(
            system.index("Trusted memory processing: "), system.index("memory.propose")
        )
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
