"""Isolated AstrBot SDK shapes and Platform HTTP contract tests (no real account)."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import ssl
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError
from unittest.mock import patch


PLUGIN_PARENT = Path(__file__).resolve().parents[1] / "integrations" / "astrbot"
sys.path.insert(0, str(PLUGIN_PARENT))

CERT_PROGRAM = """
import datetime, ipaddress, pathlib, sys
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

root = pathlib.Path(sys.argv[1])
now = datetime.datetime.now(datetime.timezone.utc)

def name(value):
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, value)])

def save_key(path, key):
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))

def make_ca(label):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = name(label)
    cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(key, hashes.SHA256()))
    (root / (label + '.pem')).write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return key, cert

good_key, good_ca = make_ca('good-ca')
make_ca('wrong-ca')
server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
server_cert = (x509.CertificateBuilder().subject_name(name('synthetic-platform'))
    .issuer_name(good_ca.subject).public_key(server_key.public_key())
    .serial_number(x509.random_serial_number())
    .not_valid_before(now - datetime.timedelta(minutes=5))
    .not_valid_after(now + datetime.timedelta(days=1))
    .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
    .add_extension(x509.SubjectAlternativeName(
        [x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
    .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
    .sign(good_key, hashes.SHA256()))
(root / 'server.pem').write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
save_key(root / 'server-key.pem', server_key)
"""

from astrbot_plugin_tianshu.runtime import (  # noqa: E402
    ACK_ROUTE,
    CLAIM_ROUTE,
    EVENT_ROUTE,
    EVENT_STATUS_ROUTE,
    BoundaryError,
    Journal,
    Runner,
    Settings,
    normalize_event,
)
from astrbot_plugin_tianshu.http_port import PlatformHTTP  # noqa: E402


def settings(**overrides):
    value = {
        "base_url": "https://platform.example",
        "connection_id": "conn-1",
        "token": "fixture-token",
        "platform_id": "qq-adapter-1",
        "self_id": "9000",
        "allowed_conversations": ["private:1234", "group:5678"],
        "trigger_prefix": "天枢 ",
    }
    value.update(overrides)
    return Settings.from_config(value)


class Event:
    def __init__(self, *, text="天枢 你好", group="", message_id="m-1", segments=None):
        conversation_type = "group" if group else "private"
        self.message_obj = types.SimpleNamespace(
            message_id=message_id,
            raw_message={
                "post_type": "message",
                "message_type": conversation_type,
                "message_id": message_id,
                "time": 1770000000,
                "self_id": "9000",
                "user_id": "1234",
                "group_id": group or None,
                "message": segments
                if segments is not None
                else [{"type": "text", "data": {"text": text}}],
            },
        )
        self.group = group
        self.stopped = False
        self.platform_id = "qq-adapter-1"
        self.platform_name = "aiocqhttp"

    def get_platform_name(self):
        return self.platform_name

    def get_platform_id(self):
        return self.platform_id

    def get_self_id(self):
        return "9000"

    def get_sender_id(self):
        return "1234"

    def get_group_id(self):
        return self.group

    def stop_event(self):
        self.stopped = True


class NormalizationTests(unittest.TestCase):
    def test_exact_scope_and_sdk_origin(self):
        config = settings()
        result = normalize_event(Event(), config)
        self.assertEqual(result["conversation_id"], "private:1234")
        self.assertEqual(result["event_id"], "m-1")
        self.assertEqual(result["sent_at"], "2026-02-02T02:40:00Z")
        self.assertEqual(result["text"], "你好")
        group = Event(
            group="5678",
            segments=[
                {"type": "at", "data": {"qq": "9000"}},
                {"type": "text", "data": {"text": "天枢 群聊"}},
            ],
        )
        self.assertEqual(normalize_event(group, config)["conversation_id"], "group:5678")
        group.message_obj.raw_message["message"] += [{"type": "image", "data": {"file": "fixture"}}]
        self.assertIsNone(normalize_event(group, config))
        self.assertEqual(
            len(normalize_event(Event(text="天枢 " + "x" * 8000), config)["text"]), 8000
        )
        self.assertIsNone(normalize_event(Event(text="天枢 " + "x" * 8001), config))

    def test_other_plugin_messages_and_unstable_ids_pass_through(self):
        config = settings()
        event = Event(text="/help")
        self.assertIsNone(normalize_event(event, config))
        event = Event(text="你好")
        self.assertIsNone(normalize_event(event, config))
        event = Event()
        event.platform_id = "different-adapter"
        self.assertIsNone(normalize_event(event, config))
        named = Event()
        named.platform_id = "小月 QQ"
        self.assertIsNotNone(normalize_event(named, settings(platform_id="小月 QQ")))
        event = Event()
        event.message_obj.raw_message["message_id"] = "different"
        self.assertIsNone(normalize_event(event, config))
        event = Event()
        event.message_obj.raw_message["post_type"] = "notice"
        self.assertIsNone(normalize_event(event, config))
        event = Event()
        event.message_obj.raw_message["time"] = None
        self.assertIsNone(normalize_event(event, config))
        event = Event()
        event.message_obj.raw_message["message"] += [{"type": "reply", "data": {"id": "123"}}]
        self.assertIsNone(normalize_event(event, config))

    def test_disabled_scope_and_tls_constraints(self):
        with self.assertRaises(BoundaryError):
            settings(base_url="http://platform.example")
        with self.assertRaises(BoundaryError):
            settings(allowed_conversations=[])
        with self.assertRaises(BoundaryError):
            settings(allowed_conversations=["group:*"])
        with self.assertRaises(BoundaryError):
            settings(ca_file="relative/ca.pem")
        self.assertEqual(
            settings(base_url="http://127.0.0.1:8000", allow_http_loopback=True).base_url,
            "http://127.0.0.1:8000",
        )


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "connector.db"
        self.journal = Journal(self.path)

    async def asyncTearDown(self):
        self.journal.close()
        self.temp.cleanup()

    async def test_event_stops_only_owned_message_and_never_reposts_unknown(self):
        calls = []

        async def post(path, payload):
            calls.append((path, payload))
            if path == EVENT_ROUTE:
                raise TimeoutError("fixture")
            return {}

        async def send(_destination, _text):
            raise AssertionError("not outbound")

        runner = Runner(settings(), self.journal, post, send)
        other = Event(text="别的插件")
        self.assertFalse(await runner.on_event(other))
        self.assertFalse(other.stopped)
        owned = Event()
        self.assertTrue(await runner.on_event(owned))
        self.assertTrue(owned.stopped)
        duplicate = Event()
        self.assertTrue(await runner.on_event(duplicate))
        self.assertEqual([path for path, _ in calls], [EVENT_ROUTE])
        self.journal.close()
        self.journal = Journal(self.path)
        runner = Runner(settings(), self.journal, post, send)
        self.assertTrue(await runner.on_event(Event()))
        self.assertEqual([path for path, _ in calls], [EVENT_ROUTE])

    async def test_unknown_event_only_reconciles_status_after_restart(self):
        payloads = []

        async def post(path, payload):
            payloads.append((path, payload))
            if path == EVENT_ROUTE:
                raise TimeoutError("fixture")
            if path == EVENT_STATUS_ROUTE:
                return {"found": True, "state": "accepted", "message_id": "core-1", "outcomes": []}
            if path == CLAIM_ROUTE:
                return {"deliveries": []}
            return {}

        async def send(_conversation, _text):
            raise AssertionError("no reply")

        runner = Runner(settings(), self.journal, post, send)
        self.assertTrue(await runner.on_event(Event()))
        self.journal.close()
        self.journal = Journal(self.path)
        runner = Runner(settings(), self.journal, post, send)
        await runner.poll_once()
        self.assertEqual(
            [path for path, _ in payloads],
            [EVENT_ROUTE, EVENT_STATUS_ROUTE, CLAIM_ROUTE],
        )
        self.assertEqual(runner.journal.events_to_check(), [])

    async def test_real_id_ack_replayed_after_ack_loss_without_second_send(self):
        delivery = {
            "reply_id": "reply-1",
            "attempt_id": "attempt-1",
            "namespace": "qq",
            "conversation_id": "group:5678",
            "thread_id": None,
            "text": "真实模型输出的合成测试文案",
            "turn_id": "turn-1",
            "segment_sequence": 1,
        }
        claims, sends, acks = 0, [], []

        async def post(path, payload):
            nonlocal claims
            if path == CLAIM_ROUTE:
                claims += 1
                return {"deliveries": [delivery]}
            if path == ACK_ROUTE:
                acks.append(payload)
                if len(acks) == 1:
                    raise TimeoutError("ACK response lost")
                return {"reply_id": payload["reply_id"], "state": payload["state"]}
            return {}

        async def send(destination, text):
            sends.append((destination, text))
            return "778899"

        runner = Runner(settings(), self.journal, post, send)
        await runner.poll_once()
        self.assertEqual(len(sends), 1)
        self.assertEqual(acks[0]["channel_message_ids"], ["778899"])
        self.journal.close()
        self.journal = Journal(self.path)
        runner = Runner(settings(), self.journal, post, send)
        await runner.poll_once()
        self.assertEqual(len(sends), 1)
        self.assertEqual(acks[1], acks[0])
        self.assertEqual(acks[1]["state"], "sent")
        self.assertEqual(claims, 2)

    async def test_crash_at_native_boundary_recovers_unknown(self):
        delivery = {"reply_id": "reply-2", "attempt_id": "attempt-2", "text": "fixture"}
        self.assertEqual(self.journal.record_delivery(delivery, "conn-1")[0], "new")
        self.journal.close()
        self.journal = Journal(self.path)
        pending = self.journal.pending_acks("conn-1")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["state"], "unknown")
        self.assertEqual(pending[0]["channel_message_ids"], [])
        self.assertEqual(self.journal.record_delivery(delivery, "conn-1")[0], "duplicate")

    async def test_wrong_destination_and_missing_native_id_never_sent(self):
        deliveries = [
            {
                "reply_id": "r-out",
                "attempt_id": "a-out",
                "namespace": "qq",
                "conversation_id": "private:9999",
                "thread_id": None,
                "text": "x",
            },
            {
                "reply_id": "r-id",
                "attempt_id": "a-id",
                "namespace": "qq",
                "conversation_id": "private:1234",
                "thread_id": None,
                "text": "x",
            },
        ]
        calls, acks = [], []

        async def post(path, payload):
            if path == CLAIM_ROUTE:
                return {"deliveries": [deliveries.pop(0)] if deliveries else []}
            if path == ACK_ROUTE:
                acks.append(payload)
                return {"reply_id": payload["reply_id"], "state": payload["state"]}
            return {}

        async def send(conversation, text):
            calls.append((conversation, text))
            return ""

        runner = Runner(settings(), self.journal, post, send)
        await runner.poll_once()
        await runner.poll_once()
        self.assertEqual(calls, [("private:1234", "x")])
        self.assertEqual([x["state"] for x in acks], ["unknown", "unknown"])

    async def test_outbound_core_byte_boundary_and_restart_do_not_resend(self):
        texts = ["x" * 8001, "x" * 32768, "中" * 10922, "x" * 32769, "中" * 10923]
        deliveries = [
            {
                "reply_id": f"reply-long-{index}",
                "attempt_id": f"attempt-long-{index}",
                "namespace": "qq",
                "conversation_id": "private:1234",
                "thread_id": None,
                "text": value,
            }
            for index, value in enumerate(texts)
        ]
        claims, sends, acks = list(deliveries), [], []

        async def post(path, payload):
            if path == CLAIM_ROUTE:
                return {"deliveries": [claims.pop(0)] if claims else []}
            if path == ACK_ROUTE:
                acks.append(payload)
                return {"reply_id": payload["reply_id"], "state": payload["state"]}
            return {}

        async def send(conversation, text):
            sends.append((conversation, text))
            return str(8000 + len(sends))

        runner = Runner(settings(), self.journal, post, send)
        for _ in deliveries:
            await runner.poll_once()
        self.assertEqual([text for _, text in sends], texts[:3])
        self.assertEqual([ack["state"] for ack in acks], ["sent"] * 3 + ["failed"] * 2)
        self.assertEqual([ack["channel_message_ids"] for ack in acks][-2:], [[], []])

        # A duplicate claim after restart replays its receipt, never its SDK send.
        self.journal.close()
        self.journal = Journal(self.path)
        claims.extend([deliveries[0], deliveries[-1]])
        runner = Runner(settings(), self.journal, post, send)
        await runner.poll_once()
        await runner.poll_once()
        self.assertEqual(len(sends), 3)
        self.assertEqual(acks[-2:], [acks[0], acks[4]])


class NativeSDKTests(unittest.IsolatedAsyncioTestCase):
    async def test_plugin_calls_pinned_aiocqhttp_client_and_requires_native_id(self):
        api = types.ModuleType("astrbot.api")
        api.logger = types.SimpleNamespace(info=lambda *x: None, warning=lambda *x: None)
        event_api = types.ModuleType("astrbot.api.event")
        event_api.AstrMessageEvent = object
        event_api.filter = types.SimpleNamespace(
            EventMessageType=types.SimpleNamespace(ALL="all"),
            event_message_type=lambda *_a, **_k: lambda f: f,
            on_astrbot_loaded=lambda: lambda f: f,
        )
        star_api = types.ModuleType("astrbot.api.star")
        star_api.Context = object
        star_api.Star = object
        star_api.StarTools = types.SimpleNamespace(get_data_dir=lambda _: Path("."))
        root = types.ModuleType("astrbot")
        modules = {
            "astrbot": root,
            "astrbot.api": api,
            "astrbot.api.event": event_api,
            "astrbot.api.star": star_api,
        }
        with patch.dict(sys.modules, modules):
            main = importlib.import_module("astrbot_plugin_tianshu.main")
            try:
                sent = []

                class Client:
                    async def send_private_msg(self, **kwargs):
                        sent.append(kwargs)
                        return {"message_id": 42}

                    async def send_group_msg(self, **kwargs):
                        sent.append(kwargs)
                        return {}

                platform = types.SimpleNamespace(
                    meta=lambda: types.SimpleNamespace(name="aiocqhttp", id="qq-adapter-1"),
                    get_client=lambda: Client(),
                )
                plugin = object.__new__(main.TianshuPlugin)
                plugin.context = types.SimpleNamespace(get_platform_inst=lambda _: platform)
                plugin._runner = types.SimpleNamespace(settings=settings())
                self.assertEqual(await plugin._send_native("private:1234", "hi"), "42")
                self.assertEqual(sent[0]["self_id"], "9000")
                self.assertEqual(sent[0]["message"], [{"type": "text", "data": {"text": "hi"}}])
                with self.assertRaises(BoundaryError):
                    await plugin._send_native("group:5678", "hi")

                polled = asyncio.Event()

                class DisabledHeartbeat:
                    settings = types.SimpleNamespace(poll_seconds=0.01, heartbeat_seconds=30)

                    async def flush_acks(self):
                        return True

                    async def heartbeat(self):
                        raise PermissionError("synthetic disable")

                    async def poll_once(self):
                        polled.set()

                plugin._runner = DisabledHeartbeat()
                plugin._client = lambda: object()
                plugin._report = lambda _code: None
                task = asyncio.create_task(plugin._background())
                try:
                    await asyncio.wait_for(polled.wait(), timeout=1)
                finally:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
            finally:
                sys.modules.pop("astrbot_plugin_tianshu.main", None)


class HTTPPortTests(unittest.IsolatedAsyncioTestCase):
    async def test_loopback_json_and_bearer_without_redirect(self):
        seen = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                seen.append((self.path, self.headers.get("Authorization"), json.loads(raw)))
                if self.path == "/internal/v1/bot/replies/claim":
                    self.send_response(307)
                    self.send_header("Location", "/redirected")
                    self.end_headers()
                    return
                content = json.dumps({"state": "online"}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            config = settings(
                base_url=f"http://127.0.0.1:{server.server_port}", allow_http_loopback=True
            )
            port = PlatformHTTP(config)
            self.assertEqual(
                await port("/internal/v1/bot/heartbeat", {"connection_id": "conn-1"}),
                {"state": "online"},
            )
            self.assertEqual(seen[0][1], "Bearer fixture-token")
            with self.assertRaises(Exception):
                await port("/internal/v1/bot/replies/claim", {})
            self.assertEqual(len(seen), 2)
            with self.assertRaises(BoundaryError):
                await port("/unregistered", {})
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


@unittest.skipUnless(
    os.environ.get("TIANSHU_TLS_PYTHON"), "ephemeral TLS certificate runtime required"
)
class HTTPSCATests(unittest.IsolatedAsyncioTestCase):
    async def test_private_ca_succeeds_wrong_ca_and_hostname_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(
                [os.environ["TIANSHU_TLS_PYTHON"], "-c", CERT_PROGRAM, str(root)],
                check=True,
                capture_output=True,
                timeout=20,
            )
            seen = []

            class Handler(BaseHTTPRequestHandler):
                def do_POST(self):
                    seen.append(self.path)
                    content = b'{"state":"online"}'
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)

                def log_message(self, *_args):
                    pass

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(str(root / "server.pem"), str(root / "server-key.pem"))
            server.socket = context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                address = f"https://127.0.0.1:{server.server_port}"
                good = PlatformHTTP(settings(base_url=address, ca_file=str(root / "good-ca.pem")))
                self.assertEqual(
                    await good("/internal/v1/bot/heartbeat", {"connection_id": "conn-1"}),
                    {"state": "online"},
                )
                wrong = PlatformHTTP(settings(base_url=address, ca_file=str(root / "wrong-ca.pem")))
                with self.assertRaises(URLError) as wrong_ca:
                    await wrong("/internal/v1/bot/heartbeat", {})
                self.assertIsInstance(wrong_ca.exception.reason, ssl.SSLCertVerificationError)
                mismatch = PlatformHTTP(
                    settings(
                        base_url=f"https://localhost:{server.server_port}",
                        ca_file=str(root / "good-ca.pem"),
                    )
                )
                with self.assertRaises(URLError) as wrong_host:
                    await mismatch("/internal/v1/bot/heartbeat", {})
                self.assertIsInstance(wrong_host.exception.reason, ssl.SSLCertVerificationError)
                self.assertEqual(seen, ["/internal/v1/bot/heartbeat"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
