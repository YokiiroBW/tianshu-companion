"""Durable automatic learning, model selection and unknown-result reconciliation."""

import asyncio
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from support import Harness
from tianshu_companion.clients import Gateway, JsonService, Memory
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
        self.source_text = TEXT
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
                content=[dict(index=0, text=self.source_text, sent_at="2026-10-11T00:00:00Z")],
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

    async def test_empty_mixed_extraction_reviews_once_and_reconciles_uncertain_write(self):
        await self.chat()
        self.source_text = "钥匙一直放在门边抽屉，我现在去倒垃圾。"
        original = self.generate

        async def first_empty(turn, messages):
            result = await original(turn, messages)
            return (['{"memories":[]}'], {}) if len(self.generated) == 1 else result

        self.h.gateway.generate = first_empty
        await self.flush()
        self.assertEqual(self.task()["stage"], "empty_review")
        self.assertGreater(self.h.core.pass_count("memory.work"), 0)
        self.assertEqual(self.task()["empty_review"][0]["quote"], "钥匙一直放在门边抽屉")
        self.assertFalse(any(output is not None for _, output in self.calls))
        await self.restart()
        self.lose_response = True
        await self.flush()
        self.assertEqual(len(self.generated), 2)
        self.assertEqual(self.generated[1]["generation_budget"]["reasoning_effort"], "low")
        self.assertIn(
            "review hints, not accepted facts", self.extraction_messages[1][-2]["content"]
        )
        self.assertIsNotNone(self.task()["output"])
        await self.restart()
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(len(self.generated), 2)
        self.assertEqual(sum(output is not None for _, output in self.calls), 1)

    async def test_mixed_review_can_legitimately_remain_empty_without_looping(self):
        await self.chat()
        self.source_text = "我平时坐609路上班，先去洗手。"

        async def empty(turn, messages):
            self.generated.append(turn)
            return ['{"memories":[]}'], {}

        self.h.gateway.generate = empty
        await self.flush()
        await self.flush()
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(len(self.generated), 2)
        self.assertEqual(self.calls[-1][1], {"memories": []})

    async def test_identifiable_plan_update_is_hinted_initially_and_reviews_at_most_once(self):
        await self.chat()
        self.source_text = "我还是想去，几点出发我也没想好。"
        process = self.process

        async def with_plan(job, output):
            response = await process(job, output)
            if response["state"] == "pending":
                response["known_facts"] = [
                    dict(
                        key="event.visit.plan",
                        value="周日下午去书屋",
                        state="current",
                        superseded=False,
                        entity=None,
                    )
                ]
            return response

        async def empty(turn, messages):
            self.generated.append(copy.deepcopy(turn))
            self.extraction_messages.append(copy.deepcopy(messages))
            return ['{"memories":[]}'], {}

        self.h.memory.process_candidate = with_plan
        self.h.gateway.generate = empty
        await self.flush()
        payload = json.loads(self.extraction_messages[0][-1]["content"])
        self.assertEqual(
            payload["source_hints"],
            [dict(source_index=0, quote=self.source_text, known_plan_key="event.visit.plan")],
        )
        self.assertEqual(self.task()["stage"], "empty_review")
        self.assertFalse(any(output is not None for _, output in self.calls))
        await self.flush()
        await self.flush()
        self.assertEqual(len(self.generated), 2)
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.calls[-1][1], {"memories": []})
        self.assertEqual(self.generated[1]["generation_budget"]["reasoning_effort"], "low")

    async def test_group_commit_does_not_wait_for_unrelated_group_or_private_generation(self):
        await self.h.ingest(text=TEXT, group=True, channel="group:a")
        await self.h.cycles()
        gate = asyncio.Event()
        original = self.generate

        async def slow_chat(turn, messages):
            if not turn["id"].startswith("memory-generation:"):
                await gate.wait()
            return await original(turn, messages)

        self.h.gateway.generate = slow_chat
        await self.h.ingest(text="另一群慢回复", group=True, channel="group:b")
        await self.h.ingest(text="私聊慢回复", channel="private:c", account="c")
        await self.h.cycles()
        self.assertEqual(sum(t["phase"] == "generating" for t in self.h.turns()), 2)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        gate.set()
        await self.h.cycles(80)
        self.assertTrue(all(t["phase"] == "sent" for t in self.h.turns()))

    async def test_group_commit_wait_allows_new_unrelated_room_to_start(self):
        await self.h.ingest(text=TEXT, group=True, channel="group:a")
        await self.h.cycles()
        gate = asyncio.Event()
        original = self.generate

        async def slow(turn, messages):
            if self.h.core._input_text(turn) == "慢回复":
                await gate.wait()
            return await original(turn, messages)

        self.h.gateway.generate = slow
        await self.h.ingest(text="慢回复", group=True, channel="group:a")
        await self.h.cycles()
        await self.flush()
        self.assertEqual(self.task()["stage"], "commit_wait")
        await self.h.ingest(text="同群排队", group=True, channel="group:a")
        await self.h.ingest(text="另一群继续", group=True, channel="group:b")
        await self.h.ingest(text="私聊继续", channel="private:c", account="c")
        await self.h.cycles(100)
        phases = {self.h.core._input_text(t): t["phase"] for t in self.h.turns()}
        self.assertEqual(phases["同群排队"], "queued")
        self.assertEqual(phases["另一群继续"], "sent")
        self.assertEqual(phases["私聊继续"], "sent")
        gate.set()
        await self.h.cycles()
        await self.h.core.memory_learning.work()
        self.assertEqual(self.task()["state"], "completed")

    async def test_private_commit_still_waits_for_potentially_associated_private_scope(self):
        await self.chat()
        gate = asyncio.Event()
        original = self.generate

        async def slow(turn, messages):
            if not turn["id"].startswith("memory-generation:"):
                await gate.wait()
            return await original(turn, messages)

        self.h.gateway.generate = slow
        await self.h.ingest(text="另一个私聊", channel="private:c", account="c")
        await self.h.cycles()
        await self.flush()
        self.assertEqual(self.task()["stage"], "commit_wait")
        self.assertTrue(self.h.core.memory_learning.admission_blocked(self.h.turns()[-1]["scope"]))
        self.assertFalse(any(output is not None for _, output in self.calls))
        gate.set()
        await self.h.cycles()
        await self.h.core.memory_learning.work()
        self.assertEqual(self.task()["state"], "completed")

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
        self.assertFalse(self.h.core.memory_learning.admission_blocked(self.h.turns()[0]["scope"]))

    async def test_commit_wait_barrier_survives_slow_receipt_cancel_and_restart(self):
        await self.chat()
        gate = asyncio.Event()
        self.h.gateway.gates[2] = gate
        await self.h.ingest(text="同范围正在回复")
        await self.h.cycles()
        await self.flush()
        waiting = self.task()
        self.assertEqual(waiting["stage"], "commit_wait")
        proposal = copy.deepcopy(waiting["output"])
        gate.set()
        await self.h.cycles()
        entered, read_gate = asyncio.Event(), asyncio.Event()

        async def slow_receipt(job, output):
            self.assertIsNone(output)
            entered.set()
            await read_gate.wait()
            return await self.process(job, output)

        self.h.memory.process_candidate = slow_receipt
        worker = self.h.core.memory_learning
        await worker.work(wait=False)
        await asyncio.wait_for(entered.wait(), 2)
        self.assertEqual(self.task()["stage"], "commit_wait")
        self.assertTrue(worker.admission_blocked(waiting["scope"]))
        await self.h.ingest(text="同范围应当排队")
        await self.h.ingest(text="另一群正常回复", group=True, channel="group:other", account="b")
        await self.h.cycles()
        phases = {self.h.core._input_text(turn): turn["phase"] for turn in self.h.turns()}
        self.assertEqual(phases["同范围应当排队"], "queued")
        self.assertEqual(phases["另一群正常回复"], "sent")
        await worker.close()
        self.assertEqual(self.task()["stage"], "receipt")
        self.assertFalse(worker.admission_blocked(waiting["scope"]))
        self.assertEqual(self.task()["recovery_failures"], {"receipt": 1})
        self.assertEqual(self.task()["output"], proposal)
        await self.h.cycles()
        queued = next(t for t in self.h.turns() if self.h.core._input_text(t) == "同范围应当排队")
        self.assertEqual(queued["phase"], "sent")
        await self.restart()
        self.assertFalse(self.h.core.memory_learning.admission_blocked(waiting["scope"]))
        self.h.memory.process_candidate = self.process
        self.h.clock.advance(10)
        await self.h.core.memory_learning.work()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["attempts"], 2)
        self.assertEqual(len(self.generated), 1)

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
        self.assertFalse(self.h.core.memory_learning.admission_blocked(self.h.turns()[0]["scope"]))
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

    async def test_model_timeout_confirms_owned_cancellation_and_keeps_request_after_recovery(self):
        from test_completion_recovery import cancel_response, owned_receipt

        await self.chat()
        requests, terminal, streams = [], [], []

        class Waiting(httpx.AsyncByteStream):
            closed = False

            async def __aiter__(self):
                await asyncio.Event().wait()
                yield  # pragma: no cover

            async def aclose(self):
                self.closed = True

        def handler(request):
            requests.append((request.method, request.url.path))
            if request.url.path == "/v1/chat/completions":
                stream = Waiting()
                streams.append(stream)
                return httpx.Response(200, stream=stream)
            request_id = request.url.path.split("/")[-2 if request.method == "POST" else -1]
            if request.method == "POST":
                return httpx.Response(200, json=cancel_response(request_id))
            terminal.append(request_id)
            return httpx.Response(
                200, json=owned_receipt(request_id, "cancelled", self.h.core.config_version)
            )

        client = JsonService(
            "https://fixture.invalid", "secret", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(client.close)
        gateway = Gateway(self.h.contracts, client)
        self.h.gateway.generate = gateway.generate
        with patch("tianshu_companion.memory_learning.MODEL_TIMEOUT_SECONDS", 0.03):
            await self.flush()
        item = self.task()
        self.assertEqual(item["state"], "pending")
        self.assertEqual(item["recovery_failures"], {"temporary": 1})
        diagnostic = item["first_failure"]
        self.assertEqual(diagnostic["category"], "timeout")
        self.assertEqual(diagnostic["exception_type"], "TimeoutError")
        self.assertEqual(diagnostic["stage"], "model")
        self.assertTrue(diagnostic["unknown"])
        self.assertEqual(terminal, [diagnostic["model_request_id"]])
        self.assertEqual(requests[-2][1], requests[-1][1] + "/cancel")
        self.assertTrue(streams[0].closed)
        self.assertFalse(gateway.active_requests)
        self.assertFalse(any(output is not None for _, output in self.calls))
        self.h.gateway.generate = self.generate
        await self.restart()
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["first_failure"], diagnostic)
        self.assertEqual(self.task()["last_failure"], diagnostic)
        self.assertEqual(self.task()["failure_count"], 1)
        self.assertEqual(sum(path == "/v1/chat/completions" for _, path in requests), 1)

    async def test_typed_receipt_failure_survives_restart_and_success_without_model_replay(self):
        await self.chat()

        async def broken(request):
            raise httpx.ReadTimeout(
                "private URL, credential and response text must not be retained"
            )

        client = JsonService(
            "https://fixture.invalid",
            "fixture-secret",
            transport=httpx.MockTransport(broken),
            label="memory",
        )
        self.addAsyncCleanup(client.close)
        memory = Memory(self.h.contracts, client)
        self.h.memory.process_candidate = memory.process_candidate
        await self.flush()
        failed = self.task()
        diagnostic = failed["first_failure"]
        self.assertEqual(diagnostic["stage"], "receipt")
        self.assertEqual(diagnostic["attempt"], 1)
        self.assertEqual(diagnostic["category"], "read_error")
        self.assertEqual(diagnostic["exception_type"], "ReadTimeout")
        self.assertEqual(diagnostic["peer"], "memory")
        self.assertEqual(diagnostic["operation"], "query")
        self.assertNotIn("http_status", diagnostic)
        self.assertNotIn("private", json.dumps(diagnostic))
        self.assertNotIn("fixture", json.dumps(diagnostic))
        self.assertEqual(self.generated, [])
        await self.restart()
        self.h.memory.process_candidate = self.process
        self.h.clock.advance(10)
        await self.flush()
        completed = self.task()
        self.assertEqual(completed["state"], "completed")
        self.assertIsNone(completed["last_error"])
        self.assertEqual(completed["first_failure"], diagnostic)
        self.assertEqual(completed["last_failure"], diagnostic)
        self.assertEqual(completed["failure_count"], 1)
        self.assertEqual(completed["recovery_failures"], {"receipt": 1})
        self.assertEqual(len(self.generated), 1)

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
        self.assertFalse(self.h.core.memory_learning.admission_blocked(self.h.turns()[0]["scope"]))
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
        self.assertEqual(self.generated[0]["generation_budget"]["reasoning_effort"], "none")
        self.assertEqual(self.generated[1]["generation_budget"]["reasoning_effort"], "low")

    async def test_disabled_candidates_do_not_run_model_or_write(self):
        await self.chat()
        await self.h.core.close()
        self.h.core = self.h.new_core(automatic_memory_candidates=False)
        self.h.core.recover()
        await self.flush()
        self.assertEqual(self.generated, [])
        self.assertEqual(self.calls, [])

    async def test_pure_query_skips_model_but_gets_memory_owned_receipt(self):
        await self.chat()
        original = self.process

        async def query(job, output):
            value = await original(job, output)
            if output is None:
                value["content"] = [
                    dict(index=0, text="狗叫什么来着？", sent_at="2026-10-11T00:00:00Z")
                ]
            return value

        self.h.memory.process_candidate = query
        await self.flush()
        self.assertEqual(self.generated, [])
        self.assertEqual([v for _, v in self.calls], [None, {"memories": []}])
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["admission_reason"], "question_only")

    async def test_due_retry_is_not_starved_by_continuous_new_independent_jobs(self):
        from tianshu_companion.contracts import digest

        await self.chat()
        self.model_fail = True
        await self.flush()
        retry = self.task()
        self.h.clock.advance(3)
        worker = self.h.core.memory_learning
        worker._fresh_grants = 0
        for index in range(1, 6):
            scope = dict(retry["scope"], person_id=f"person:new:{index}")
            job = dict(
                retry,
                id=f"new:{index}",
                scope=scope,
                scope_key=digest(scope),
                sequence=index + 1,
                deadline=0,
            )
            self.h.core.store.put("metadata", job)
            chosen = worker._next()
            if index == 4:
                self.assertEqual(chosen["id"], retry["id"])
                break
            self.assertNotEqual(chosen["id"], retry["id"])
            chosen["state"] = "completed"
            self.h.core.store.put("metadata", chosen)

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

    async def test_five_rate_limits_then_unknown_receipt_read_recovers_same_job(self):
        await self.chat()
        generate, process = self.generate, self.process
        read_failed = False

        async def pressure(turn, messages):
            result = await generate(turn, messages)
            if len(self.generated) <= 5:
                error = Fault("dependency_unavailable", unknown=True)
                error.rate_limited = True
                raise error
            return result

        async def read_timeout(job, output):
            nonlocal read_failed
            if output is None and len(self.generated) == 5 and not read_failed:
                read_failed = True
                raise Fault("timeout", unknown=True)
            return await process(job, output)

        self.h.gateway.generate = pressure
        self.h.memory.process_candidate = read_timeout
        for _ in range(6):
            await self.flush()
            item = self.task()
            self.assertEqual(item["state"], "pending")
            self.assertIsNone(item["output"])
            self.h.clock.advance(item["deadline"] - self.h.clock() + 1)
        job_id = self.task()["job_id"]
        self.assertEqual(self.task()["attempts"], 6)
        self.assertEqual(self.task()["stage"], "receipt")
        self.assertEqual(self.task()["recovery_failures"], {"rate_limit": 5, "receipt": 1})
        self.assertEqual(self.task()["proposal_failures"], 0)
        self.assertFalse(any(output is not None for _, output in self.calls))
        await self.restart()
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["job_id"], job_id)
        self.assertEqual(self.task()["attempts"], 7)
        self.assertEqual(len(self.generated), 6)
        self.assertEqual(sum(output is not None for _, output in self.calls), 1)

    async def test_rate_limit_retry_after_is_persisted_without_holding_other_scopes(self):
        self.unique_jobs()
        await self.h.ingest(text=TEXT, group=True, channel="group:a")
        await self.h.ingest(text=TEXT, group=True, channel="group:b", account="b")
        await self.h.cycles()
        generate = self.generate
        receipts = {}

        async def scoped_process(job, output):
            # process() is an inline fixture. Its single receipt must be scoped
            # when two independent real queue jobs run concurrently here.
            saved, self.completed = self.completed, receipts.get(job)
            try:
                response = await self.process(job, output)
                if self.completed:
                    receipts[job] = self.completed
                return response
            finally:
                self.completed = saved

        async def pressure(turn, messages):
            if turn["bundle"]["collection_key"]["channel"]["channel_conversation_id"] == "group:a":
                self.generated.append(copy.deepcopy(turn))
                failure = Fault("dependency_unavailable", unknown=True)
                failure.rate_limited = True
                failure.retry_after_seconds = 45
                raise failure
            return await generate(turn, messages)

        self.h.gateway.generate = pressure
        self.h.memory.process_candidate = scoped_process
        await self.flush()
        await self.h.core.memory_learning.work()
        jobs = self.h.core.store.list("metadata", "memory:learning")
        pending = next(item for item in jobs if item["state"] == "pending")
        self.assertEqual(sum(item["state"] == "completed" for item in jobs), 1)
        self.assertEqual(pending["recovery_reason"], "rate_limit_cooldown")
        self.assertEqual(pending["deadline"] - self.h.clock(), 45)
        calls = len(self.generated)
        self.h.clock.advance(44)
        await self.flush()
        self.assertEqual(len(self.generated), calls)
        await self.restart()
        self.h.clock.advance(2)
        self.h.gateway.generate = generate
        await self.flush()
        self.assertEqual(self.h.core.store.get("metadata", pending["id"])["state"], "completed")

    async def test_saved_output_receipt_timeout_then_restart_never_regenerates(self):
        await self.chat()
        self.lose_response = True
        await self.flush()
        item = self.task()
        proposal = copy.deepcopy(item["output"])

        async def timeout(job, output):
            self.assertEqual(job, item["job_id"])
            self.assertIsNone(output)
            raise Fault("timeout", unknown=True)

        self.h.memory.process_candidate = timeout
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.task()["output"], proposal)
        self.assertEqual(self.task()["recovery_failures"], {"temporary": 1, "receipt": 1})
        self.assertEqual(self.task()["proposal_failures"], 0)
        await self.restart()
        self.h.memory.process_candidate = self.process
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(len(self.generated), 1)
        self.assertEqual(sum(output is not None for _, output in self.calls), 1)

    async def test_unknown_commit_keeps_retry_after_and_retries_exact_output_after_query(self):
        await self.chat()
        requests, failures = [], []
        submissions = 0

        def handler(request):
            nonlocal submissions
            body = json.loads(request.content)
            requests.append(body)
            if body["output"] is None:
                return httpx.Response(
                    200,
                    json=dict(
                        state="pending",
                        result=None,
                        content=[dict(index=0, text=TEXT, sent_at="2026-10-11T00:00:00Z")],
                    ),
                )
            submissions += 1
            if submissions == 1:
                return httpx.Response(
                    503,
                    json=dict(
                        code="dependency_unavailable", execution_state="unknown", retryable=True
                    ),
                    headers={"Retry-After": "45"},
                )
            return httpx.Response(
                200,
                json=dict(
                    state="completed",
                    result=dict(
                        state="committed",
                        record_ids=["record:learned"],
                        group_ids=["group:learned"],
                    ),
                    content=[],
                ),
            )

        client = JsonService(
            "https://fixture.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(client.close)
        memory = Memory(self.h.contracts, client)

        async def process(job, output):
            try:
                return await memory.process_candidate(job, output)
            except Fault as error:
                failures.append((error.unknown, error.retry_after_seconds))
                raise

        self.h.memory.process_candidate = process
        await self.flush()
        item = self.task()
        proposal = copy.deepcopy(item["output"])
        self.assertEqual(failures, [(True, 45)])
        self.assertEqual(item["deadline"] - self.h.clock(), 45)
        self.assertEqual(item["proposal_failures"], 0)
        self.assertEqual(len(requests), 2)
        diagnostic = item["first_failure"]
        self.assertEqual(diagnostic["stage"], "commit")
        self.assertEqual(diagnostic["attempt"], 1)
        self.assertEqual(diagnostic["http_status"], 503)
        self.assertEqual(diagnostic["exception_type"], "Fault")
        self.assertEqual(diagnostic["operation"], "command")
        self.assertTrue(diagnostic["unknown"])
        self.h.clock.advance(44)
        await self.flush()
        self.assertEqual(len(requests), 2)
        await self.restart()
        self.h.clock.advance(2)
        await self.flush()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(len(self.generated), 1)
        self.assertEqual([body["output"] for body in requests], [None, proposal, None, proposal])
        self.assertTrue(all(body["job_id"] == item["job_id"] for body in requests))
        self.assertEqual(self.task()["first_failure"], diagnostic)
        self.assertEqual(self.task()["last_failure"], diagnostic)

    async def test_rate_limit_failures_have_a_durable_bound(self):
        await self.chat()
        generate = self.generate

        async def pressure(turn, messages):
            await generate(turn, messages)
            failure = Fault("dependency_unavailable", unknown=True)
            failure.rate_limited = True
            raise failure

        self.h.gateway.generate = pressure
        for _ in range(6):
            await self.flush()
            await self.restart()
            self.h.clock.advance(301)
        self.assertEqual(self.task()["state"], "blocked")
        self.assertEqual(self.task()["attempts"], 6)
        self.assertEqual(self.task()["recovery_failures"], {"rate_limit": 6})
        before = len(self.generated)
        await self.flush()
        self.assertEqual(len(self.generated), before)

    async def test_null_output_receipt_failures_have_a_durable_bound(self):
        await self.chat()

        async def timeout(job, output):
            self.assertIsNone(output)
            raise Fault("timeout", unknown=True)

        self.h.memory.process_candidate = timeout
        for _ in range(6):
            await self.flush()
            await self.restart()
            self.h.clock.advance(301)
        self.assertEqual(self.task()["state"], "blocked")
        self.assertEqual(self.task()["attempts"], 6)
        self.assertEqual(self.task()["recovery_failures"], {"receipt": 6})
        self.assertEqual(self.generated, [])

    async def test_global_recovery_bound_stops_all_io_without_discarding_output(self):
        from tianshu_companion.memory_learning import MAX_TOTAL_ATTEMPTS

        await self.chat()
        self.lose_response = True
        await self.flush()
        item = self.task()
        proposal = copy.deepcopy(item["output"])
        item.update(attempts=MAX_TOTAL_ATTEMPTS, deadline=0)
        self.h.core.store.put("metadata", item)
        calls = len(self.calls)
        await self.restart()
        await self.flush()
        self.assertEqual(self.task()["state"], "blocked")
        self.assertEqual(self.task()["output"], proposal)
        self.assertEqual(len(self.calls), calls)
        self.assertEqual(len(self.generated), 1)

    async def test_withdrawn_source_during_receipt_wait_stops_generation(self):
        from tianshu_companion.contracts import digest

        await self.chat()
        turn = self.h.turns()[0]
        physical = self.h.core.store.get(
            "physicals", digest(turn["bundle"]["messages"][0]["message_key"])
        )
        process = self.process

        async def withdrawal(job, output):
            response = await process(job, output)
            physical["fact"]["state"] = "retracted"
            self.h.core.store.put("physicals", physical)
            return response

        self.h.memory.process_candidate = withdrawal
        await self.flush()
        self.assertEqual(self.task()["state"], "blocked")
        self.assertEqual(self.task()["last_error"], "scope_changed")
        self.assertEqual(self.generated, [])
        self.assertFalse(any(output is not None for _, output in self.calls))

    async def test_cancellation_during_generation_retains_output_without_submitting(self):
        await self.chat()
        generate = self.generate

        async def cancelled(turn, messages):
            result = await generate(turn, messages)
            fresh = self.h.turns()[0]
            fresh["cancelled"] = True
            self.h.core.store.put("turns", fresh)
            return result

        self.h.gateway.generate = cancelled
        await self.flush()
        self.assertEqual(self.task()["state"], "blocked")
        self.assertEqual(self.task()["last_error"], "scope_changed")
        self.assertIsNotNone(self.task()["output"])
        self.assertEqual(len(self.generated), 1)
        self.assertFalse(any(output is not None for _, output in self.calls))

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
        for index in range(3):
            await self.flush()
            if index < 2:
                self.h.clock.advance(10)
        self.assertEqual(self.task()["state"], "pending")
        self.assertGreater(self.task()["deadline"], self.h.clock())
        self.assertEqual(self.task()["deadline"] - self.h.clock(), 8)
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

    async def test_slow_receipt_does_not_hold_other_rooms_or_get_claimed_twice(self):
        self.unique_jobs()
        await self.chat()
        first_turn = self.h.turns()[0]["id"]
        await self.h.ingest(text=TEXT, group=True, channel="group:independent", account="b")
        await self.h.cycles()
        await self.h.core.flush_outbox()
        worker = self.h.core.memory_learning
        tasks = self.h.core.store.list("metadata", "memory:learning")
        first = next(job for job in tasks if job["turn_id"] == first_turn)
        second = next(job for job in tasks if job["turn_id"] != first_turn)
        gate = asyncio.Event()
        entered = asyncio.Event()
        original = self.process
        reads = []

        async def slow(job, output):
            if job == first["job_id"] and output is None:
                reads.append(job)
                entered.set()
                await gate.wait()
            return await original(job, output)

        self.h.memory.process_candidate = slow
        await worker.work(wait=False)
        await asyncio.wait_for(entered.wait(), 2)
        for _ in range(60):
            await asyncio.sleep(0)
        self.assertEqual(self.h.core.store.get("metadata", second["id"])["state"], "completed")
        self.assertEqual(self.h.core.store.get("metadata", first["id"])["state"], "pending")
        await worker.work(wait=False)
        self.assertEqual(len(reads), 1)
        gate.set()
        await worker.work()
        self.assertEqual(self.h.core.store.get("metadata", first["id"])["state"], "completed")

    async def test_background_models_overlap_but_same_scope_correction_keeps_order(self):
        from tianshu_companion.model_slots import ModelSlots

        self.unique_jobs()
        self.h.core.models = ModelSlots(4)
        turn_ids = []
        for index, (account, channel) in enumerate(
            [("a", "private:a"), ("a", "private:a"), ("b", "private:b")]
        ):
            text = TEXT + f"，来源{index}"
            await self.h.ingest(text=text, account=account, channel=channel)
            await self.h.cycles()
            turn_ids.append(
                next(turn["id"] for turn in self.h.turns() if self.h.core._input_text(turn) == text)
            )
        await self.h.core.flush_outbox()
        tasks = self.h.core.store.list("metadata", "memory:learning")
        first, correction, unrelated = [
            next(job for job in tasks if job["turn_id"] == turn_id) for turn_id in turn_ids
        ]
        gate = asyncio.Event()
        entered = []
        original = self.generate

        async def slow(turn, messages):
            if turn["id"].startswith("memory-generation:"):
                entered.append(turn["sequence"])
                await gate.wait()
            return await original(turn, messages)

        self.h.gateway.generate = slow
        worker = self.h.core.memory_learning
        await worker.work(wait=False)
        for _ in range(60):
            await asyncio.sleep(0)
        self.assertEqual(len(entered), 2)
        self.assertEqual(self.h.core.store.get("metadata", correction["id"])["attempts"], 0)
        self.assertEqual(self.h.core.store.get("metadata", first["id"])["attempts"], 1)
        self.assertEqual(self.h.core.store.get("metadata", unrelated["id"])["attempts"], 1)
        gate.set()
        await asyncio.gather(*worker.jobs.values())
        self.completed = None
        await worker.work()
        self.assertEqual(self.h.core.store.get("metadata", correction["id"])["state"], "completed")

    async def test_waiting_commit_does_not_hold_independent_extraction(self):
        self.unique_jobs()
        await self.h.ingest(text=TEXT, group=True, channel="group:a")
        await self.h.cycles()
        gate = asyncio.Event()
        original = self.generate

        async def slow(turn, messages):
            if self.h.core._input_text(turn) == "慢回复":
                await gate.wait()
            return await original(turn, messages)

        self.h.gateway.generate = slow
        await self.h.ingest(text="慢回复", group=True, channel="group:a")
        await self.h.cycles()
        await self.flush()
        first = self.task()
        self.assertEqual(first["stage"], "commit_wait")
        await self.h.ingest(text=TEXT, group=True, channel="group:b", account="b")
        await self.h.cycles()
        await self.h.core.flush_outbox()
        await self.h.core.memory_learning.work()
        jobs = self.h.core.store.list("metadata", "memory:learning")
        self.assertEqual(self.h.core.store.get("metadata", first["id"])["stage"], "commit_wait")
        self.assertEqual(sum(job["state"] == "completed" for job in jobs), 1)
        gate.set()
        await self.h.cycles()

    async def test_close_cancels_owned_generation_and_restart_keeps_attempt(self):
        await self.chat()
        gate = asyncio.Event()
        original = self.generate

        async def slow(turn, messages):
            await gate.wait()
            return await original(turn, messages)

        self.h.gateway.generate = slow
        await self.h.core.flush_outbox()
        worker = self.h.core.memory_learning
        await worker.work(wait=False)
        for _ in range(30):
            await asyncio.sleep(0)
        self.assertEqual(self.task()["stage"], "model")
        await worker.close()
        self.assertFalse(worker.jobs)
        self.assertEqual(self.task()["last_error"], "cancelled")
        self.assertIsNone(self.task()["output"])
        await self.restart()
        self.h.clock.advance(10)
        gate.set()
        await self.h.core.memory_learning.work()
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(self.task()["attempts"], 2)

    async def test_reconcile_inflight_commit_checks_receipt_without_a_second_submit(self):
        await self.chat()
        started = asyncio.Event()

        async def slow(job, output):
            result = await self.process(job, output)
            if output is not None:
                started.set()
                await asyncio.Event().wait()
            return result

        self.h.memory.process_candidate = slow
        await self.h.core.flush_outbox()
        worker = self.h.core.memory_learning
        await worker.work(wait=False)
        await asyncio.wait_for(started.wait(), 2)
        result = await worker.reconcile(self.task()["job_id"])
        self.assertEqual(result["state"], "completed")
        self.assertEqual(self.task()["state"], "completed")
        self.assertEqual(sum(output is not None for _, output in self.calls), 1)
        self.assertEqual(len(self.generated), 1)

    async def test_role_failure_while_waiting_for_model_slot_prevents_generation(self):
        from tianshu_companion.model_slots import ModelSlots

        await self.chat()
        self.h.core.models = ModelSlots(1)
        await self.h.core.models.acquire()
        await self.h.core.flush_outbox()
        worker = self.h.core.memory_learning
        await worker.work(wait=False)
        for _ in range(30):
            await asyncio.sleep(0)
        self.assertEqual(self.task()["stage"], "model")

        def revoked(turn):
            raise Fault("forbidden")

        self.h.core._check_role = revoked
        self.h.core.models.release()
        await asyncio.gather(*worker.jobs.values())
        self.assertEqual(self.generated, [])
        self.assertEqual(self.task()["state"], "blocked")
        self.assertEqual(self.task()["last_error"], "forbidden")

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
