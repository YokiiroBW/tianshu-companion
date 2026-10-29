"""Real event encoding/collection with synthetic peers; no production messages."""

import asyncio
import json
from pathlib import Path
import tempfile
import unittest

import httpx

from tianshu_companion import observability as obs
from tianshu_companion.clients import JsonService
from tianshu_companion.contracts import Fault


class PeerDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def test_100_calls_have_safe_route_target_status_and_credential_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            port = obs.LogAdapter(directory)
            client = JsonService(
                "https://synthetic.invalid",
                "secret-canary",
                label="platform",
                log_adapter=port,
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(
                        403, json={"code": "forbidden", "body": "secret-canary"}
                    )
                ),
            )
            try:
                for _ in range(100):
                    with self.assertRaises(Fault) as raised:
                        await client.call(
                            "/internal/v1/actor-self-memory/authority", {"private": "secret-canary"}
                        )
                self.assertTrue(port.flush())
                raw = b"".join(p.read_bytes() for p in Path(directory).glob("*.jsonl*"))
                records = [json.loads(line) for line in raw.splitlines()]
                finished = [event for event in records if event["event"] == "peer.call.finished"]
                self.assertEqual(len(finished), 100)
                self.assertNotIn(b"secret-canary", raw)
                for event in finished:
                    self.assertEqual(event["attributes"]["target"], "platform")
                    self.assertEqual(
                        event["attributes"]["route"], "/internal/v1/actor-self-memory/authority"
                    )
                    self.assertEqual(event["attributes"]["http_status"], 403)
                    self.assertEqual(event["attributes"]["attempt"], 1)
                    self.assertEqual(event["attributes"]["error_code"], "forbidden")
                diagnostic = obs.failure_diagnostic(
                    raised.exception,
                    code="forbidden",
                    attempt=3,
                    stage="preparation",
                    observed_at=1,
                )
                self.assertEqual(diagnostic["peer"], "platform")
                self.assertEqual(diagnostic["route"], finished[0]["attributes"]["route"])
                self.assertEqual(diagnostic["attempt"], 3)
                self.assertEqual(diagnostic["transport_attempt"], 1)
            finally:
                await client.close()
                port.close()

    async def test_stream_binary_and_cancellation_report_one_end_without_replay(self):
        class Port:
            def __init__(self):
                self.events = []

            def emit(self, event, outcome, **fields):
                self.events.append((event, outcome, fields))

        port = Port()
        calls = []

        async def handler(request):
            calls.append(request.url.path)
            if request.url.path == "/v1/chat/completions":
                raise asyncio.CancelledError()
            return httpx.Response(200, content=b"original")

        client = JsonService(
            "https://synthetic.invalid",
            "synthetic",
            label="gateway",
            log_adapter=port,
            transport=httpx.MockTransport(handler),
        )
        try:
            with self.assertRaises(asyncio.CancelledError):
                async for _ in client.stream("/v1/chat/completions", {}):
                    pass
            payload, _ = await client.binary("/internal/v1/knowledge/content/original", {})
            self.assertEqual(payload, b"original")
            self.assertEqual(len(calls), 2)
            finished = [row for row in port.events if row[0] == "peer.call.finished"]
            self.assertEqual(len(finished), 2)
            self.assertEqual(finished[0][2]["attributes"]["operation"], "stream")
            self.assertEqual(finished[1][2]["attributes"]["http_status"], 200)
        finally:
            await client.close()

    def test_unregistered_routes_and_dynamic_ids_never_enter_events(self):
        self.assertEqual(obs.peer_route("/secret-canary?token=secret-canary"), "unknown")
        self.assertEqual(
            obs.peer_route("/internal/v1/model-requests/model:" + "a" * 32),
            "/internal/v1/model-requests/{request_id}",
        )
        attributes = dict(
            target="platform",
            route="unknown",
            principal="sha256:" + "a" * 64,
            operation="query",
            http_status=None,
            error_code=None,
            attempt=0,
        )
        obs.validate_peer_attributes(attributes)
        for key, value in [
            ("principal", "secret-canary"),
            ("route", "/secret-canary"),
            ("attempt", True),
            ("http_status", 0),
        ]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                obs.validate_peer_attributes({**attributes, key: value})
        with self.assertRaises(ValueError):
            obs.validate_peer_attributes({**attributes, "body": "secret-canary"})

    async def test_consumer_closing_a_stream_is_cancellation_not_peer_failure(self):
        events = []

        class Port:
            def emit(self, event, outcome, **fields):
                events.append((event, outcome, fields))

        client = JsonService(
            "https://synthetic.invalid",
            "synthetic",
            label="gateway",
            log_adapter=Port(),
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, text='data: {"tool":1}\n\ndata: [DONE]\n\n')
            ),
        )
        try:
            stream = client.stream("/v1/chat/completions", {})
            self.assertEqual(await anext(stream), {"tool": 1})
            await stream.aclose()
            finished = [row for row in events if row[0] == "peer.call.finished"]
            self.assertEqual(len(finished), 1)
            self.assertEqual(finished[0][1], "cancelled")
            self.assertEqual(finished[0][2]["error_code"], "cancelled")
        finally:
            await client.close()
