"""Optional real loopback HTTP joint test against BOT-P's isolated fixture."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import sys
import tempfile
import time
import types
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import AsyncMock, patch


PLUGIN_ROOT = Path(__file__).resolve().parents[1] / "integrations" / "astrbot"
sys.path.insert(0, str(PLUGIN_ROOT))

from astrbot_plugin_tianshu.http_port import PlatformHTTP  # noqa: E402
from astrbot_plugin_tianshu.runtime import ACK_ROUTE, CLAIM_ROUTE, Journal, Runner, Settings  # noqa: E402


class SDKEvent:
    """Only the AstrBot 4.27.3 getters and aiocqhttp event fields used by the plugin."""

    def __init__(self, timestamp: int, message_id: str, text: str = "天枢 你好"):
        self.message_obj = types.SimpleNamespace(
            message_id=message_id,
            raw_message={
                "post_type": "message",
                "message_type": "group",
                "message_id": message_id,
                "time": timestamp,
                "self_id": 9000,
                "user_id": 1234,
                "group_id": 123,
                "message": [{"type": "text", "data": {"text": text}}],
            },
        )
        self.stopped = False

    def get_platform_name(self):
        return "aiocqhttp"

    def get_platform_id(self):
        return "qq-adapter-1"

    def get_self_id(self):
        return "9000"

    def get_sender_id(self):
        return "1234"

    def get_group_id(self):
        return "123"

    def stop_event(self):
        self.stopped = True


@unittest.skipUnless(
    os.environ.get("TIANSHU_PLATFORM_REPO"), "set TIANSHU_PLATFORM_REPO for BOT-P joint test"
)
class AstrBotPlatformJointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        platform_root = Path(os.environ["TIANSHU_PLATFORM_REPO"])
        sys.path.insert(0, str(platform_root))
        sys.path.insert(0, str(platform_root / "tests" / "backend"))
        self.contract_env = patch.dict(
            os.environ,
            {
                "TS012_CONTRACT_DIR": str(
                    platform_root.parents[2] / "contracts" / "text-dialogue" / "v1"
                )
            },
        )
        self.contract_env.start()
        self.addCleanup(self.contract_env.stop)
        from fixtures import ENV, bearer, start_http
        from services.platform.server import create_app
        from services.platform.service import Platform
        from test_bots import BotTests, bot_settings

        self.bearer = bearer
        self.BotTests = BotTests
        self.temp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, ENV)
        self.env.start()
        self.now = [time.time()]
        self.config = bot_settings(self.temp.name)
        slot = self.config["bot_connections"]["slots"]["qq-onebot-main"]
        slot.update(adapter="astrbot", platform_id="qq-adapter-1", self_id="9000")
        for number, account in (("1", "1234"), ("2", "5678")):
            self.config["input_entries"]["bot-input-" + number]["account"][
                "immutable_account_id"
            ] = account
            self.config["entries"]["bot-actor-" + number]["account"]["immutable_account_id"] = (
                account
            )
        self.platform = Platform(self.config, clock=lambda: self.now[0])
        created = self.platform.bots.create("qq-onebot-main", ["actor:a"])
        self.connection_id = created["connection_id"]
        self.token = created["token"]
        self.platform.bots.change(self.connection_id, "enable")
        self.platform.sources.dispatch = AsyncMock(
            return_value={"outcomes": [{"actor_id": "actor:a", "state": "accepted"}]}
        )
        self.server, url = await start_http(create_app(self.platform))
        self.settings = Settings.from_config(
            {
                "base_url": url,
                "allow_http_loopback": True,
                "connection_id": self.connection_id,
                "token": self.token,
                "platform_id": "qq-adapter-1",
                "self_id": "9000",
                "allowed_conversations": ["group:123"],
            }
        )
        self.port = PlatformHTTP(self.settings)
        self.journal = Journal(Path(self.temp.name) / "astrbot-journal.sqlite3")
        self.sent = []

    async def asyncTearDown(self):
        self.journal.close()
        await self.server.cleanup()
        self.platform.close()
        self.env.stop()
        self.temp.cleanup()

    async def _native(self, destination, text):
        self.sent.append((destination, text))
        return str(7000 + len(self.sent))

    def _reply_request(self, index: int):
        fixture = types.SimpleNamespace(
            platform=self.platform, settings=self.config, now=self.now[0]
        )
        request = self.BotTests.send_request(fixture)
        request = copy.deepcopy(request)
        request["command"]["request_id"] = f"request:bot-reply-{index}"
        request["command"]["idempotency_key"] = f"command:bot-reply-{index}"
        request["reply_id"] = f"reply:bot-{index}"
        request["turn_id"] = f"turn:bot-{index}"
        request["turn_sequence"] = index
        return request

    async def _core_send_http(self, request):
        def sync_post():
            payload = json.dumps(request).encode()
            req = Request(
                self.settings.base_url + "/internal/v1/conversation/send",
                data=payload,
                headers={
                    "Authorization": self.bearer("COMPANION"),
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urlopen(req, timeout=10) as response:
                return json.load(response)

        return await asyncio.to_thread(sync_post)

    async def test_real_http_admission_claim_ack_competition_and_revocation(self):
        runner = Runner(self.settings, self.journal, self.port, self._native)
        await runner.heartbeat()
        event = SDKEvent(int(self.now[0]), "101")
        self.assertTrue(await runner.on_event(event))
        self.assertTrue(event.stopped)
        self.assertEqual(self.platform.sources.dispatch.await_count, 1)
        self.assertTrue(await runner.on_event(SDKEvent(int(self.now[0]), "101")))
        self.assertEqual(self.platform.sources.dispatch.await_count, 1)
        other = SDKEvent(int(self.now[0]), "102", "其它插件内容")
        self.assertFalse(await runner.on_event(other))
        self.assertFalse(other.stopped)

        request1 = self._reply_request(1)
        self.assertEqual((await self._core_send_http(request1))["state"], "unknown")
        await runner.poll_once()
        self.assertEqual(self.sent, [("group:123", "合成模型结果")])
        receipt = self.platform.bots.reply_status(self.bearer("COMPANION"), request1)["receipt"]
        self.assertEqual(receipt["channel_message_ids"], ["7001"])

        # Two persistent instances compete for one Platform claim. The first ACK
        # response is lost after Platform saved it; only the same ACK is replayed.
        request2 = self._reply_request(2)
        await self._core_send_http(request2)
        second_journal = Journal(Path(self.temp.name) / "astrbot-journal-2.sqlite3")
        first_ack_lost = [False]

        async def ack_lost_once(path, payload):
            result = await self.port(path, payload)
            if path == ACK_ROUTE and not first_ack_lost[0]:
                first_ack_lost[0] = True
                raise TimeoutError("synthetic ACK response loss")
            return result

        other_runner = Runner(self.settings, second_journal, ack_lost_once, self._native)
        first_runner = Runner(self.settings, self.journal, ack_lost_once, self._native)
        try:
            await asyncio.gather(first_runner.poll_once(), other_runner.poll_once())
            self.assertEqual(len(self.sent), 2)
            self.assertTrue(first_ack_lost[0])
            await asyncio.gather(first_runner.poll_once(), other_runner.poll_once())
            self.assertEqual(len(self.sent), 2)
            self.assertEqual(
                self.platform.bots.reply_status(self.bearer("COMPANION"), request2)["receipt"][
                    "state"
                ],
                "sent",
            )
        finally:
            second_journal.close()

        # A lease may have become unknown while the SDK call was in flight. A
        # real late SDK ID can still settle the same attempt without resending.
        request3 = self._reply_request(3)
        await self._core_send_http(request3)
        late_delivery = []

        async def late_post(path, payload):
            result = await self.port(path, payload)
            if path == CLAIM_ROUTE and result["deliveries"]:
                delivery = result["deliveries"][0]
                late_delivery.append(delivery)
                self.now[0] += 61
                status = await self.port(
                    "/internal/v1/bot/replies/status",
                    {
                        "connection_id": self.connection_id,
                        "reply_id": delivery["reply_id"],
                        "attempt_id": delivery["attempt_id"],
                    },
                )
                self.assertEqual(status["state"], "unknown")
            return result

        late_runner = Runner(self.settings, self.journal, late_post, self._native)
        await late_runner.poll_once()
        self.assertEqual(len(self.sent), 3)
        self.assertEqual(len(late_delivery), 1)
        self.assertEqual(
            (
                await self.port(
                    "/internal/v1/bot/replies/status",
                    {
                        "connection_id": self.connection_id,
                        "reply_id": late_delivery[0]["reply_id"],
                        "attempt_id": late_delivery[0]["attempt_id"],
                    },
                )
            )["state"],
            "sent",
        )

        # Disable races with a reply that was already claimed. The original
        # attempt may still produce a real SDK ID, which must remain reportable.
        request4 = self._reply_request(4)
        await self._core_send_http(request4)

        async def disable_after_claim(path, payload):
            result = await self.port(path, payload)
            if path == CLAIM_ROUTE and result["deliveries"]:
                self.platform.bots.change(self.connection_id, "disable")
            return result

        in_flight = Runner(self.settings, self.journal, disable_after_claim, self._native)
        await in_flight.poll_once()
        self.platform.bots.change(self.connection_id, "enable")
        self.assertEqual(
            self.platform.bots.reply_status(self.bearer("COMPANION"), request4)["receipt"]["state"],
            "sent",
        )
        self.assertEqual(len(self.sent), 4)

        # Token rotation keeps an old claim's identity but revokes the old
        # credential. A new credential may settle only its exact attempt.
        request5 = self._reply_request(5)
        await self._core_send_http(request5)
        rotated_delivery = (
            await self.port(
                CLAIM_ROUTE,
                {
                    "connection_id": self.connection_id,
                    "instance_id": self.journal.instance_id,
                    "limit": 1,
                },
            )
        )["deliveries"][0]
        rotated = self.platform.bots.change(self.connection_id, "rotate")
        original_ack = {
            "connection_id": self.connection_id,
            "reply_id": rotated_delivery["reply_id"],
            "attempt_id": rotated_delivery["attempt_id"],
            "state": "unknown",
            "channel_message_ids": [],
        }
        with self.assertRaises(HTTPError) as old_credential:
            await self.port(ACK_ROUTE, original_ack)
        self.assertEqual(old_credential.exception.code, 401)
        self.settings = replace(self.settings, token=rotated["token"])
        self.port = PlatformHTTP(self.settings)
        with self.assertRaises(HTTPError) as wrong_attempt:
            await self.port(ACK_ROUTE, {**original_ack, "attempt_id": "attempt:other"})
        self.assertEqual(wrong_attempt.exception.code, 403)
        self.assertEqual((await self.port(ACK_ROUTE, original_ack))["state"], "unknown")

        request6 = self._reply_request(6)
        request6["text"] = "x" * 8001
        await self._core_send_http(request6)
        runner = Runner(self.settings, self.journal, self.port, self._native)
        await runner.poll_once()
        self.assertEqual(self.sent[-1], ("group:123", request6["text"]))
        self.assertEqual(
            self.platform.bots.reply_status(self.bearer("COMPANION"), request6)["receipt"]["state"],
            "sent",
        )

        request7 = self._reply_request(7)
        request7["text"] = "x" * 32769
        await self._core_send_http(request7)
        await runner.poll_once()
        self.assertEqual(len(self.sent), 5)
        self.assertEqual(
            self.platform.bots.reply_status(self.bearer("COMPANION"), request7)["receipt"]["state"],
            "failed",
        )

        request8 = self._reply_request(8)
        await self._core_send_http(request8)
        self.platform.bots.change(self.connection_id, "disable")
        revoked = SDKEvent(int(self.now[0]), "103")
        self.assertTrue(await runner.on_event(revoked))
        self.assertTrue(revoked.stopped)
        self.assertEqual(self.platform.sources.dispatch.await_count, 1)
        with self.assertRaises(Exception):
            await runner.poll_once()
        self.assertEqual(len(self.sent), 5)


if __name__ == "__main__":
    unittest.main()
