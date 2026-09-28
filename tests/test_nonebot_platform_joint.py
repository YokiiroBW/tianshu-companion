"""Optional real loopback HTTP joint test against BOT-P; only the Core dispatch is synthetic."""

import asyncio
import os
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from support import contracts
from test_nonebot_sdk import FakeBot, group_event
from tianshu_companion.clients import PlatformBotSender
from tianshu_nonebot.runtime import BotRuntime


class LoopbackCoreClient:
    """Test-only client; production Core uses JsonService with HTTPS and a service token."""

    def __init__(self, url, token):
        self.url, self.token = url, token
        self.http = httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False)

    async def call(self, path, body):
        response = await self.http.post(
            self.url + path, json=body, headers={"Authorization": "Bearer " + self.token}
        )
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.http.aclose()


@pytest.mark.skipif(not os.environ.get("TIANSHU_PLATFORM_REPO"), reason="BOT-P path not set")
def test_typed_sdk_to_platform_http_to_native_ack_and_core_status(tmp_path, monkeypatch):
    async def run():
        root = Path(os.environ["TIANSHU_PLATFORM_REPO"])
        monkeypatch.syspath_prepend(str(root))
        monkeypatch.syspath_prepend(str(root / "tests/backend"))
        monkeypatch.setenv("TS012_CONTRACT_DIR", str(Path(os.environ["TIANSHU_CONTRACTS"])))
        # Fixture imports only after its published-contract path is provided.
        from fixtures import ENV, start_http  # noqa: PLC0415
        from services.platform.server import create_app  # noqa: PLC0415
        from services.platform.service import Platform  # noqa: PLC0415
        from test_bots import BotTests, bot_settings  # noqa: PLC0415

        settings = bot_settings(str(tmp_path))
        slot = settings["bot_connections"]["slots"]["qq-onebot-main"]
        slot["self_id"] = "42"
        for number in ("1", "2"):
            settings["input_entries"]["bot-input-" + number]["account"]["immutable_account_id"] = (
                "7" if number == "1" else "8"
            )
            settings["entries"]["bot-actor-" + number]["account"]["immutable_account_id"] = (
                "7" if number == "1" else "8"
            )
            for entry in (
                settings["input_entries"]["bot-input-" + number],
                settings["entries"]["bot-actor-" + number],
            ):
                entry["channel"]["channel_conversation_id"] = "group:99"
        now = time.time()
        with patch.dict(os.environ, ENV):
            platform = Platform(settings, clock=lambda: now)
            runner = None
            runtime = None
            core_client = None
            try:
                created = platform.bots.create("qq-onebot-main", ["actor:a"])
                platform.bots.change(created["connection_id"], "enable")
                platform.sources.dispatch = AsyncMock(
                    return_value={"outcomes": [{"actor_id": "actor:a", "state": "accepted"}]}
                )
                runner, url = await start_http(create_app(platform))
                bot = FakeBot()
                runtime = BotRuntime(
                    connection_id=created["connection_id"],
                    platform_id="onebot11-main",
                    bot_self_id="42",
                    allowed_conversations=["group:99"],
                    journal_path=tmp_path / "bot.db",
                    platform_url=url,
                    token=created["token"],
                    get_bot=lambda _: bot,
                )
                assert await runtime.capture(bot, group_event())
                await runtime.flush_events()
                assert await runtime.capture(bot, group_event())
                await runtime.flush_events()
                assert platform.sources.dispatch.await_count == 1
                assert runtime.journal.pending_events() == []

                fixture = SimpleNamespace(platform=platform, settings=settings, now=now)
                request = BotTests.send_request(fixture)
                core_client = LoopbackCoreClient(url, ENV["TS012_COMPANION"])
                sender = PlatformBotSender(contracts(), core_client)
                assert (await sender.send(request))["state"] == "unknown"
                assert await sender.reconcile(request) is None
                claims = await runtime.port.claim(runtime.connection_id, runtime.instance_id)
                assert len(claims) == 1
                await runtime.deliver(bot, claims[0])
                await runtime.flush_acks()
                receipt = await sender.reconcile(request)
                assert receipt["state"] == "sent"
                assert receipt["channel_message_ids"] == ["888"]
                assert len(bot.calls) == 1
                assert await runtime.port.claim(runtime.connection_id, "another-instance") == []
            finally:
                if core_client:
                    await core_client.close()
                if runtime:
                    await runtime.close()
                if runner:
                    await runner.cleanup()
                platform.close()

    asyncio.run(run())
