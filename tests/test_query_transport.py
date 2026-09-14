"""Query-only recovery; synthetic contracts and real loopback TLS, never L0."""

import asyncio
import json
import os
import socket
import ssl
import subprocess
import tempfile
import unittest
from pathlib import Path

import httpx
import uvicorn

from support import contracts, Harness
from test_source_https import CERT_PROGRAM
from tianshu_companion.clients import JsonService, Memory, Sender
from tianshu_companion.contracts import Fault


SELECT = "/internal/v1/memory/select"
QUERIES = [
    ("/internal/v1/origins/resolve", {}),
    ("/internal/v1/identity/resolve", {}),
    (SELECT, {}),
    ("/internal/v1/memory/profiles/select", {}),
    ("/internal/v1/memory/source-sync/check", {}),
    ("/internal/v1/model-requests/model:" + "a" * 32, None),
]
WRITES = [
    "/internal/v1/source-access/read",
    "/internal/v1/identity/register",
    "/internal/v1/memory/turn-commits",
    "/internal/v1/memory/revise",
    "/internal/v1/conversation/ingest",
    "/internal/v1/conversation/ingest-actors",
    "/internal/v1/conversation/send",
    "/v1/chat/completions",
]


class QueryTransportTests(unittest.IsolatedAsyncioTestCase):
    def service(self, handler):
        service = JsonService(
            "https://synthetic.invalid", "synthetic-only", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(service.close)
        return service

    async def test_exact_queries_retry_once_with_identical_wire_request(self):
        for path, body in QUERIES:
            for error in (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError):
                with self.subTest(path=path, error=error):
                    seen = []

                    async def handler(request):
                        seen.append((request.method, request.url, request.content, request.headers))
                        if len(seen) == 1:
                            raise error("synthetic connection loss")
                        return httpx.Response(200, json={"ok": True})

                    service = self.service(handler)
                    self.assertEqual(
                        {"ok": True}, await service.call(path, body, {"X-Request-ID": "fixed"})
                    )
                    self.assertEqual(2, len(seen))
                    self.assertEqual(seen[0], seen[1])

    async def test_failure_limit_and_nonquery_paths_never_replay(self):
        cases = [(path, {}, 1) for path in WRITES] + [
            (SELECT, {}, 2),
            (SELECT, None, 1),
            (SELECT + "/", {}, 1),
            (SELECT + "?write=true", {}, 1),
            ("/internal/v1/model-requests/model:" + "a" * 32 + "/consume", None, 1),
            ("/internal/v1/model-requests/model:" + "a" * 32, {}, 1),
        ]
        for path, body, expected in cases:
            with self.subTest(path=path, body=body):
                seen = []

                async def handler(request):
                    seen.append(request)
                    raise httpx.RemoteProtocolError("received command but lost response")

                with self.assertRaises(Fault):
                    await self.service(handler).call(path, body)
                self.assertEqual(expected, len(seen))

    async def test_http_json_transport_configuration_and_body_failures_do_not_retry(self):
        class BrokenBody(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'{"partial":'
                raise httpx.ReadError("lost body after response headers")

        outcomes = [
            httpx.Response(status, json={"code": code})
            for status, code in [
                (401, "unauthorized"),
                (403, "forbidden"),
                (409, "scope_changed"),
                (409, "version_conflict"),
                (503, "dependency_unavailable"),
                (302, "redirect"),
            ]
        ] + [
            httpx.Response(200, content=b"not-json"),
            httpx.Response(200, stream=BrokenBody()),
            httpx.ConnectError("certificate verify failed"),
            httpx.ConnectTimeout("connect timeout"),
            httpx.ReadTimeout("read timeout"),
            httpx.PoolTimeout("busy pool"),
            httpx.LocalProtocolError("invalid request"),
        ]
        for outcome in outcomes:
            with self.subTest(outcome=outcome):
                seen = []

                async def handler(request):
                    seen.append(request)
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome

                with self.assertRaises(Fault):
                    await self.service(handler).call(SELECT, {})
                self.assertEqual(1, len(seen))

    async def test_schema_failure_is_not_retried(self):
        seen = []

        async def handler(request):
            seen.append(request)
            return httpx.Response(200, json={"invalid_schema": True})

        memory = Memory(contracts(), self.service(handler))
        with self.assertRaises(Fault):
            await memory.identity({}, {}, 0)
        self.assertEqual(1, len(seen))

    async def test_total_budget_covers_both_attempts_and_response_body(self):
        for delay_first, body_delay, expected in [(0.04, False, 2), (0.2, False, 1), (0, True, 2)]:
            with self.subTest(delay_first=delay_first, body_delay=body_delay):
                seen = []
                cancelled = asyncio.Event()

                class SlowBody(httpx.AsyncByteStream):
                    async def __aiter__(self):
                        try:
                            await asyncio.sleep(1)
                            yield b"{}"
                        finally:
                            cancelled.set()

                async def handler(request):
                    seen.append(request)
                    if len(seen) == 1:
                        await asyncio.sleep(delay_first)
                        raise httpx.ReadError("first failure")
                    if body_delay:
                        return httpx.Response(200, stream=SlowBody())
                    try:
                        await asyncio.sleep(0.08)
                        return httpx.Response(200, json={"ok": True})
                    finally:
                        cancelled.set()

                service = self.service(handler)
                service.QUERY_BUDGET_SECONDS = 0.1
                start = asyncio.get_running_loop().time()
                with self.assertRaises(Fault):
                    await service.call(SELECT, {})
                elapsed = asyncio.get_running_loop().time() - start
                self.assertLess(elapsed, 0.18)  # A reset budget or no body budget fails.
                self.assertEqual(expected, len(seen))
                if expected == 2:
                    self.assertTrue(cancelled.is_set())

    async def test_cancellation_on_either_attempt_propagates_without_another_request(self):
        for cancel_attempt in (1, 2):
            seen, entered = [], asyncio.Event()

            async def handler(request):
                seen.append(request)
                if len(seen) < cancel_attempt:
                    raise httpx.ReadError("first failure")
                entered.set()
                await asyncio.Event().wait()

            service = self.service(handler)
            task = asyncio.create_task(service.call(SELECT, {}))
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel("caller cancelled")
            with self.assertRaises(asyncio.CancelledError) as raised:
                await task
            self.assertEqual(("caller cancelled",), raised.exception.args)
            self.assertEqual(cancel_attempt, len(seen))


@unittest.skipUnless(os.environ.get("TIANSHU_TLS_PYTHON"), "requires TLS certificate interpreter")
class QueryTLS(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        subprocess.run(
            [os.environ["TIANSHU_TLS_PYTHON"], "-c", CERT_PROGRAM, str(self.root)],
            check=True,
            capture_output=True,
        )

    def service(self, port):
        service = JsonService(
            f"https://127.0.0.1:{port}", "synthetic-only", ca_file=str(self.root / "cert.pem")
        )
        self.addAsyncCleanup(service.close)
        return service

    async def test_uvicorn_idle_close_recovers_on_new_tls_connection_without_interrupting_t2(self):
        seen, traces = [], []
        t2_entered, release_t2, stalled = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def app(scope, receive, send):
            body = b""
            while True:
                message = await receive()
                body += message.get("body", b"")
                if not message.get("more_body"):
                    break
            value = json.loads(body)
            seen.append((value, scope["client"], dict(scope["headers"])))
            if value["round"] == "t2":
                t2_entered.set()
                await release_t2.wait()
            result = json.dumps(value).encode()
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-length", str(len(result)).encode())],
                }
            )
            await send({"type": "http.response.body", "body": result})

        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        config = uvicorn.Config(
            app,
            lifespan="off",
            log_level="critical",
            ssl_certfile=str(self.root / "cert.pem"),
            ssl_keyfile=str(self.root / "key.pem"),
        )
        self.assertEqual(5, config.timeout_keep_alive)
        server = uvicorn.Server(config)
        serving = asyncio.create_task(server.serve(sockets=[listener]))
        service = self.service(listener.getsockname()[1])
        t1 = t2 = None
        try:
            async with asyncio.timeout(5):
                while not server.started:
                    await asyncio.sleep(0.01)
            await service.call(SELECT, {"round": "warm"})
            armed = True

            async def trace(name, info):
                nonlocal armed
                traces.append((name, info))
                if name == "http11.send_request_headers.started" and armed:
                    armed = False
                    stalled.set()
                    await asyncio.sleep(5.2)

            async def attach_trace(request):
                if json.loads(request.content)["round"] == "t1":
                    request.extensions["trace"] = trace

            service.client.event_hooks["request"].append(attach_trace)
            t1 = asyncio.create_task(service.call(SELECT, {"round": "t1", "request_id": "fixed"}))
            await asyncio.wait_for(stalled.wait(), 1)
            t2 = asyncio.create_task(service.call(SELECT, {"round": "t2"}))
            await asyncio.wait_for(t2_entered.wait(), 2)
            self.assertEqual({"round": "t1", "request_id": "fixed"}, await asyncio.wait_for(t1, 8))
            self.assertFalse(t2.done())
            release_t2.set()
            self.assertEqual({"round": "t2"}, await asyncio.wait_for(t2, 2))
            names = [name for name, _ in traces]
            failures = [(name, info) for name, info in traces if name.endswith(".failed")]
            self.assertTrue(
                any(
                    type(info["exception"]).__name__ == "RemoteProtocolError"
                    for _, info in failures
                ),
                failures,
            )
            self.assertEqual(2, names.count("http11.send_request_headers.started"))
            self.assertEqual(1, names.count("http11.receive_response_headers.complete"))
            self.assertEqual(1, names.count("connection.start_tls.complete"))
            self.assertLess(
                names.index("http11.receive_response_headers.failed"),
                names.index("connection.start_tls.complete"),
            )
            by_round = {item[0]["round"]: item for item in seen}
            self.assertEqual(3, len(seen))
            self.assertNotEqual(by_round["warm"][1], by_round["t1"][1])
            self.assertEqual(b"Bearer synthetic-only", by_round["t1"][2][b"authorization"])
            self.assertFalse(service.client.is_closed)
            untrusted = JsonService(service.url, "synthetic-only")
            try:
                with self.assertRaises(Fault):
                    await untrusted.call(SELECT, {"round": "untrusted"})
                self.assertEqual(3, len(seen))
            finally:
                await untrusted.close()
            print(
                "TLS idle-close: RemoteProtocolError before headers; one new TLS connection; "
                "t1 received once; concurrent t2 completed on original connection"
            )
        finally:
            release_t2.set()
            for task in (t1, t2):
                if task and not task.done():
                    task.cancel()
            await asyncio.gather(*(task for task in (t1, t2) if task), return_exceptions=True)
            await service.close()
            server.should_exit = True
            await asyncio.wait_for(serving, 5)
            listener.close()

    async def test_received_writes_drop_response_without_replay_and_sender_stays_unknown(self):
        seen = []

        async def drop(reader, writer):
            try:
                headers = await reader.readuntil(b"\r\n\r\n")
                length = next(
                    int(line.split(b":", 1)[1])
                    for line in headers.split(b"\r\n")
                    if line.lower().startswith(b"content-length:")
                )
                body = await reader.readexactly(length)
                seen.append((headers.split(b" ")[1].decode(), json.loads(body)))
            finally:
                writer.close()
                await writer.wait_closed()

        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(self.root / "cert.pem", self.root / "key.pem")
        server = await asyncio.start_server(drop, "127.0.0.1", 0, ssl=tls)
        async with server:
            service = self.service(server.sockets[0].getsockname()[1])
            for path in WRITES:
                before = len(seen)
                with self.assertRaises(Fault):
                    await service.call(path, {"operation": "input", "idempotency_key": "fixed"})
                self.assertEqual(before + 1, len(seen))
                self.assertEqual(path, seen[-1][0])
            h = Harness(silence_ms=0)
            try:
                h.core.sender = Sender(h.contracts, service)
                await h.core.ingest("nonebot", h.request())
                await h.cycles(40)
                self.assertEqual("reconciling", h.turns()[0]["phase"])
                self.assertEqual("unknown", h.turns()[0]["delivery_state"])
                sends = len([path for path, _ in seen if path.endswith("/send")])
                self.assertEqual(2, sends)  # One raw probe and one Core send.
                await h.cycles(40)
                h.clock.advance(h.core.policy.delivery_reconcile_timeout_ms / 1000 + 1)
                await h.cycles(40)
                self.assertEqual("closed_unknown", h.turns()[0]["phase"])
                self.assertTrue(h.turns()[0]["unresolved_delivery"])
                self.assertEqual(sends, len([path for path, _ in seen if path.endswith("/send")]))
            finally:
                await h.core.close()
