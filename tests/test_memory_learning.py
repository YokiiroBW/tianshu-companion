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

    async def test_commit_waits_for_reply_and_then_unblocks_next_snapshot(self):
        def phase(text):
            return next(t["phase"] for t in self.h.turns() if self.h.core._input_text(t) == text)

        await self.chat()
        generated = asyncio.Event()
        self.h.gateway.gates[2] = generated
        await self.h.ingest(text="下午泡了茉莉绿茶")
        await self.h.cycles()
        self.assertEqual(self.h.turns()[-1]["phase"], "generating")

        async def changing_commit(job, output):
            response = await self.process(job, output)
            if output is not None:
                self.h.memory.scope_version += 1
            return response

        self.h.memory.process_candidate = changing_commit
        await self.flush()
        item = self.task()
        self.assertEqual(item["stage"], "commit_wait")
        self.assertEqual(item["attempts"], 1)
        self.assertFalse(any(output is not None for _, output in self.calls))
        await self.h.ingest(text="还准备看会儿书")
        await self.h.ingest(text="另一个角色的对话", actor="actor:b", channel="private:b")
        await self.h.cycles()
        self.assertEqual(phase("还准备看会儿书"), "queued")
        self.assertEqual(phase("另一个角色的对话"), "sent")

        self.h.sender.gate = asyncio.Event()
        generated.set()
        await self.h.cycles()
        self.assertEqual(phase("下午泡了茉莉绿茶"), "sending")
        await self.h.core.memory_learning.work()  # Catch up the other actor's new outbox row.
        before = self.h.core.store.db.total_changes
        await self.h.core.memory_learning.work()
        self.assertEqual(self.h.core.store.db.total_changes, before)
        self.assertEqual(self.task()["attempts"], 1)

        self.h.sender.gate.set()
        await self.h.cycles()
        self.assertEqual(phase("下午泡了茉莉绿茶"), "sent")
        await self.h.core.memory_learning.work()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["attempts"], 1)
        self.assertEqual(len(self.generated), 1)
        self.assertEqual(sum(output is not None for _, output in self.calls), 1)
        await self.h.cycles()
        self.assertEqual(phase("还准备看会儿书"), "sent")

    async def test_commit_wait_survives_restart_without_regeneration_or_retry_charge(self):
        await self.chat()
        self.h.gateway.gates[2] = asyncio.Event()
        await self.h.ingest(text="下一条消息还在生成")
        await self.h.cycles()
        await self.flush()
        proposal = copy.deepcopy(self.task()["output"])
        self.assertEqual(self.task()["stage"], "commit_wait")
        await self.restart()
        self.assertEqual(self.task()["output"], proposal)
        await self.h.core.memory_learning.work()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["attempts"], 1)
        self.assertEqual(len(self.generated), 1)
        self.assertFalse(self.h.core.memory_learning.admission_blocked("actor:a"))

    async def test_empty_proposal_finishes_while_dialogue_is_generating(self):
        await self.chat()
        gate = asyncio.Event()
        self.h.gateway.gates[2] = gate
        await self.h.ingest(text="还在等这一条回复")
        await self.h.cycles()
        self.assertEqual(self.h.turns()[-1]["phase"], "generating")

        async def empty(turn, messages):
            if turn["id"].startswith("memory-generation:"):
                return ['{"memories":[]}'], {}
            return await self.original_generate(turn, messages)

        self.h.gateway.generate = empty
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["attempts"], 1)
        self.assertFalse(self.h.core.memory_learning.admission_blocked("actor:a"))
        self.assertEqual(self.calls[-1][1], {"memories": []})
        self.assertEqual(self.h.turns()[-1]["phase"], "generating")
        gate.set()
        await self.h.cycles()
        self.assertEqual(self.h.turns()[-1]["phase"], "sent")

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

    async def test_correction_waits_for_original_retry_but_other_person_can_progress(self):
        self.unique_jobs()
        await self.chat()
        self.model_fail = True
        await self.flush()
        original = self.task()
        self.model_fail = False
        await self.h.ingest(text="入口还是改成东门吧。")
        await self.h.cycles()
        correction_turn = self.h.turns()[-1]["id"]
        await self.h.ingest(text="我每天早上喝红茶。", account="b", channel="private:b")
        await self.h.cycles()
        await self.h.core.flush_outbox()
        tasks = self.h.core.store.list("metadata", "memory:learning")
        correction = next(x for x in tasks if x["turn_id"] == correction_turn)
        unrelated = next(x for x in tasks if x["id"] not in {original["id"], correction["id"]})
        await self.h.core.memory_learning.work()
        self.assertEqual(self.calls[-1][0], unrelated["job_id"])
        self.assertNotIn(correction["job_id"], [job for job, _ in self.calls])
        self.completed = None
        await self.restart()
        await self.h.core.memory_learning.work()
        self.assertNotIn(correction["job_id"], [job for job, _ in self.calls])
        self.h.clock.advance(10)
        await self.h.core.memory_learning.work()
        self.assertEqual(self.calls[-1][0], original["job_id"])
        await self.h.core.memory_learning.work()
        self.assertEqual(self.calls[-1][0], correction["job_id"])

    async def test_later_candidate_waits_for_older_unknown_outbox_acceptance(self):
        self.unique_jobs()
        await self.chat()
        original = self.h.core.store.list("outbox")[0]
        original.update(state="unknown", deadline=self.h.clock() + 60)
        self.h.core.store.put("outbox", original)
        await self.h.ingest(text="现在改到周日了。")
        await self.h.cycles()
        await self.flush()
        self.assertEqual(self.generated, [])
        self.h.clock.advance(61)
        await self.flush()
        self.assertEqual(self.generated[0]["sequence"], 1)

    def unique_jobs(self):
        commit = self.h.memory.commit

        async def unique(event):
            receipt = await commit(event)
            receipt["candidate_job_ref"] = "candidate:" + event["event_id"]
            return receipt

        self.h.memory.commit = unique

    async def test_many_waiting_followers_do_not_starve_an_independent_scope(self):
        from tianshu_companion.contracts import digest

        await self.chat()
        self.model_fail = True
        await self.flush()
        original = self.task()
        original["deadline"] = self.h.clock() + 600
        self.h.core.store.put("metadata", original)
        for index in range(2, 140):
            follower = dict(original, id=f"follower:{index}", sequence=index, deadline=0)
            self.h.core.store.put("metadata", follower)
        scope = dict(original["scope"], person_id="person:independent")
        independent = dict(
            original,
            id="independent",
            sequence=200,
            deadline=0,
            scope=scope,
            scope_key=digest(scope),
        )
        self.h.core.store.put("metadata", independent)
        worker = self.h.core.memory_learning
        worker._due_cursor = (-1, -1, "")
        self.assertIsNone(worker._next())
        self.assertIsNone(worker._next())
        self.assertEqual(worker._next()["id"], "independent")

    async def test_existing_pending_jobs_gain_order_without_resetting_attempts(self):
        await self.chat()
        self.model_fail = True
        await self.flush()
        old = self.task()
        for key in ("scope_key", "sequence", "scope"):
            old.pop(key, None)
        self.h.core.store.put("metadata", old)
        await self.restart()
        await self.h.core.memory_learning.work()
        migrated = self.task()
        self.assertEqual(migrated["attempts"], old["attempts"])
        self.assertEqual(migrated["deadline"], old["deadline"])
        self.assertEqual(migrated["sequence"], 1)
        self.assertTrue(migrated["scope_key"])

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
        self.assertFalse(self.h.core.memory_learning.admission_blocked("actor:a"))
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
