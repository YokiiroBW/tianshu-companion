import json
import unittest

import httpx
from support import contracts
from test_memory_segments import member, output_for, window_for

from tianshu_companion.clients import JsonService, Memory
from tianshu_companion.contracts import Fault
from tianshu_companion.memory_extraction import ProposalStructureError
from tianshu_companion.memory_summaries import parse_segment


class CandidateDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_peer_failure_retains_machine_location_and_unknown_state(self):
        async def handler(request):
            return httpx.Response(
                503,
                json=dict(
                    code="dependency_unavailable",
                    execution_state="unknown",
                    diagnostic=dict(
                        site="tianshu_memory.source_transport:_post:126",
                        source_owner="core",
                        source_operation="segment_frame",
                        source_http_status=403,
                        text="private source or token",
                    ),
                ),
            )

        peer = JsonService(
            "https://memory.invalid",
            "synthetic",
            transport=httpx.MockTransport(handler),
            label="memory",
        )
        self.addAsyncCleanup(peer.close)
        memory = Memory(contracts(), peer)
        with self.assertRaises(Fault) as caught:
            await memory.process_candidate("job:original", None)
        details = caught.exception.cause["candidate_diagnostic"]
        self.assertEqual(403, details["source_http_status"])
        self.assertNotIn("text", details)
        self.assertFalse(caught.exception.unknown)

    async def test_schema_and_owner_reference_failures_retain_only_field_paths(self):
        current = contracts()
        frame = dict(actor_id="actor:a", audience="group", conversation_id="conv:diagnostics")
        window = window_for(
            "segment:diagnostics", frame, [member("member:one", "person:a", frame=frame)]
        )
        output = output_for(window)
        output["results"][0]["covered_member_ids"] = ["member:unknown"]
        with self.assertRaises(ProposalStructureError) as caught:
            parse_segment(current, json.dumps(output), window)
        self.assertEqual(
            ["results", 0, "covered_member_ids"], caught.exception.diagnostics[0]["path"]
        )
        output = output_for(window)
        output["results"][0]["memories"] = "private source or token"
        with self.assertRaises(ProposalStructureError) as caught:
            parse_segment(current, json.dumps(output), window)
        diagnostic = json.dumps(caught.exception.diagnostics)
        self.assertIn("memories", diagnostic)
        self.assertNotIn("private source or token", diagnostic)
