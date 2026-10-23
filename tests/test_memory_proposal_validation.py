"""Local proposal failures never become uncertain Memory commit retries."""

import copy
import json
import unittest

import httpx
import test_memory_learning as learning
from test_memory_v2 import example

from tianshu_companion.clients import JsonService, Memory
from tianshu_companion.contracts import canonical
from tianshu_companion.memory_extraction import ProposalStructureError, parse_proposal


class ProposalValidationTests(unittest.IsolatedAsyncioTestCase):
    # Reuse the existing durable learning harness without collecting its tests twice.
    chat = learning.MemoryLearningTests.chat
    flush = learning.MemoryLearningTests.flush
    task = learning.MemoryLearningTests.task
    restart = learning.MemoryLearningTests.restart
    process = learning.MemoryLearningTests.process
    asyncTearDown = learning.MemoryLearningTests.asyncTearDown

    async def asyncSetUp(self):
        await learning.MemoryLearningTests.asyncSetUp(self)
        self.fixture = example("memory-extraction/v1", "extraction-positive")
        self.valid = self.fixture["submit_request"]["output"]
        self.invalid = copy.deepcopy(self.valid)
        self.invalid["memories"][0]["fact"].update(operation="add", target_refs=[])
        self.outputs = [canonical(self.invalid), canonical(self.valid)]
        self.remote_mode = None

        async def handler(request):
            body = json.loads(request.content)
            self.calls.append(body)
            if self.completed:
                return httpx.Response(200, json=self.completed)
            if body["output"] is None:
                return httpx.Response(200, json=self.fixture["pending_response"])
            if self.remote_mode == "forbidden":
                return httpx.Response(
                    403, json=dict(code="forbidden", execution_state="not_started")
                )
            if self.remote_mode == "unavailable":
                return httpx.Response(503, json=dict(code="dependency_unavailable"))
            if self.remote_mode == "malformed_response":
                return httpx.Response(200, json={})
            self.completed = self.fixture["completed_response"]
            if self.lose_response:
                raise httpx.ReadError("synthetic lost committed acknowledgement")
            return httpx.Response(200, json=self.completed)

        wire = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        self.h.memory.process_candidate = Memory(self.h.core.contracts, wire).process_candidate
        self.h.memory.extraction_schema = 4

    async def generate(self, turn, messages):
        if not turn["id"].startswith("memory-generation:"):
            return await self.original_generate(turn, messages)
        self.generated.append(copy.deepcopy(turn))
        self.extraction_messages.append(copy.deepcopy(messages))
        return [self.outputs[min(len(self.generated) - 1, len(self.outputs) - 1)]], {}

    async def assert_local_failure_recovers(self):
        await self.chat()
        await self.flush()
        rejected = self.task()
        self.assertEqual(
            ("pending", "proposal", 1),
            (rejected["state"], rejected["stage"], rejected["proposal_failures"]),
        )
        self.assertIsNone(rejected["output"])
        self.assertEqual({}, rejected["recovery_failures"])
        self.assertEqual([None], [row["output"] for row in self.calls])
        self.assertTrue(rejected["proposal_rejected"])
        self.assertTrue(rejected["proposal_diagnostics"])
        await self.restart()
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual("completed", self.task()["state"])
        self.assertEqual(2, len(self.generated))
        self.assertEqual([None, None, self.valid], [row["output"] for row in self.calls])
        feedback = json.loads(self.extraction_messages[1][-1]["content"])
        self.assertEqual(rejected["proposal_diagnostics"], feedback["previous_local_validation"])
        self.assertEqual("low", self.generated[1]["generation_budget"]["reasoning_effort"])
        self.assertNotIn("proposal_diagnostics", self.task())

    async def test_add_with_single_cardinality_repairs_after_restart_without_invalid_submit(self):
        await self.assert_local_failure_recovers()

    async def test_malformed_json_uses_same_bounded_correction(self):
        self.outputs[0] = '{"memories":'
        await self.assert_local_failure_recovers()

    async def test_three_local_failures_block_without_six_temporary_commit_attempts(self):
        self.outputs = [canonical(self.invalid)]
        await self.chat()
        for _ in range(3):
            await self.flush()
            self.h.clock.advance(10)
        item = self.task()
        self.assertEqual(
            ("blocked", 3, 3), (item["state"], item["attempts"], item["proposal_failures"])
        )
        self.assertEqual({}, item["recovery_failures"])
        self.assertIsNone(item["output"])
        self.assertEqual([None] * 3, [row["output"] for row in self.calls])
        await self.restart()
        await self.flush()
        self.assertEqual(3, len(self.generated))

    async def test_lost_ack_retains_exact_output_and_only_reads_receipt_after_restart(self):
        self.outputs = [canonical(self.valid)]
        self.lose_response = True
        await self.chat()
        await self.flush()
        self.assertEqual(self.valid, self.task()["output"])
        self.assertEqual(0, self.task()["proposal_failures"])
        self.assertTrue(self.task()["last_failure"]["unknown"])
        await self.restart()
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual("completed", self.task()["state"])
        self.assertEqual([None, self.valid, None], [row["output"] for row in self.calls])
        self.assertEqual(1, len(self.generated))

    async def assert_remote_failure_retains_output(self, mode):
        self.outputs = [canonical(self.valid)]
        self.remote_mode = mode
        await self.chat()
        await self.flush()
        item = self.task()
        self.assertEqual(self.valid, item["output"])
        self.assertEqual(0, item["proposal_failures"])
        self.assertNotIn("proposal_diagnostics", item)
        self.assertNotIn("proposal_rejected", item)
        await self.restart()
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(1, len(self.generated))
        self.assertEqual(self.valid, self.task()["output"])

    async def test_authorization_error_never_becomes_local_proposal_repair(self):
        await self.assert_remote_failure_retains_output("forbidden")
        self.assertEqual("blocked", self.task()["state"])

    async def test_remote_dependency_failure_never_becomes_local_proposal_repair(self):
        await self.assert_remote_failure_retains_output("unavailable")

    async def test_remote_schema_failure_never_becomes_local_proposal_repair(self):
        await self.assert_remote_failure_retains_output("malformed_response")

    async def test_old_saved_output_is_not_cleared_by_current_schema_without_noncommit_proof(self):
        self.outputs = [canonical(self.valid)]
        self.remote_mode = "unavailable"
        await self.chat()
        await self.flush()
        retained = self.task()
        retained["output"] = self.invalid
        self.h.core.store.put("metadata", retained)
        await self.restart()
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(self.invalid, self.task()["output"])
        self.assertEqual(1, len(self.generated))
        self.assertEqual(0, self.task()["proposal_failures"])

    def test_diagnostics_are_bounded_contract_paths_without_untrusted_values(self):
        output = dict(self.invalid, **{"PRIVATE_INSTRUCTION_DO_NOT_COPY": "private value"})
        output["memories"] = [copy.deepcopy(self.invalid["memories"][0]) for _ in range(32)]
        with self.assertRaises(ProposalStructureError) as caught:
            parse_proposal(self.h.core.contracts, canonical(output), schema4=True)
        diagnostics = caught.exception.diagnostics
        self.assertLessEqual(len(diagnostics), 3)
        self.assertTrue(all(len(row["path"]) <= 16 for row in diagnostics))
        self.assertNotIn("PRIVATE_INSTRUCTION", canonical(diagnostics))
        self.assertNotIn("private value", canonical(diagnostics))
        self.assertNotIn("餐桌", canonical(diagnostics))
