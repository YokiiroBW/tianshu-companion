"""Optional real Platform Sources and Companion Core loopback chain; outside peers are synthetic."""

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from support import FakeGateway, FakeMemory, contracts
from test_nonebot_sdk import FakeBot, group_event
from tianshu_companion.clients import (
    BotSenderRouter,
    JsonService,
    Origins,
    PlatformBotSender,
    Sender,
)
from tianshu_companion.core import Core, Policy
from tianshu_companion.store import Store
from tianshu_nonebot.platform import PlatformError, PlatformPort
from tianshu_nonebot.runtime import BotRuntime


@pytest.mark.skipif(not os.environ.get("TIANSHU_PLATFORM_REPO"), reason="BOT-P path not set")
def test_platform_sources_real_core_generation_and_sdk_delivery(tmp_path, monkeypatch):
    async def run():
        root = Path(os.environ["TIANSHU_PLATFORM_REPO"])
        monkeypatch.syspath_prepend(str(root))
        monkeypatch.syspath_prepend(str(root / "tests/backend"))
        monkeypatch.setenv("TS012_CONTRACT_DIR", os.environ["TIANSHU_CONTRACTS"])
        from aiohttp import web  # noqa: PLC0415
        from fixtures import ENV  # noqa: PLC0415
        from services.platform.server import create_app  # noqa: PLC0415
        from services.platform.service import Platform  # noqa: PLC0415
        from services.platform.transport import server_tls  # noqa: PLC0415
        from test_bots import bot_settings  # noqa: PLC0415

        subprocess.run(
            [sys.executable, str(root / "tests/backend/make_tls_fixture.py"), str(tmp_path)],
            check=True,
            capture_output=True,
            timeout=20,
        )
        cert = str(tmp_path / "localhost.pem")
        tls = {"certificate_file": cert, "private_key_file": str(tmp_path / "localhost-key.pem")}

        async def listen(app):
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_tls(tls))
            await site.start()
            return runner, "https://127.0.0.1:" + str(runner.addresses[0][1])

        settings = bot_settings(str(tmp_path))
        settings["principals"]["companion"]["actions"] += ["source.input", "origin.resolve"]
        settings["principals"]["companion"]["resolver"] = {
            "caller": "platform",
            "purpose": "dialogue",
        }
        settings["bot_connections"]["slots"]["qq-onebot-main"]["self_id"] = "42"
        for number, author in (("1", "7"), ("2", "8")):
            for entry in (
                settings["input_entries"]["bot-input-" + number],
                settings["entries"]["bot-actor-" + number],
            ):
                entry["account"]["immutable_account_id"] = author
                entry["channel"]["channel_conversation_id"] = "group:99"
        now = int(time.time())
        with patch.dict(
            os.environ, {**ENV, "TS013_CORE_DISPATCH": "synthetic-core-dispatch-credential"}
        ):
            platform = Platform(settings, clock=lambda: now)
            platform_runner = core_runner = None
            core = runtime = None
            clients = []
            try:
                created = platform.bots.create("qq-onebot-main", ["actor:a"])
                platform.bots.change(created["connection_id"], "enable")
                platform_runner, platform_url = await listen(create_app(platform))
                wrong_dir = tmp_path / "wrong-ca"
                wrong_dir.mkdir()
                subprocess.run(
                    [
                        sys.executable,
                        str(root / "tests/backend/make_tls_fixture.py"),
                        str(wrong_dir),
                    ],
                    check=True,
                    capture_output=True,
                    timeout=20,
                )
                wrong_port = PlatformPort(
                    platform_url, created["token"], ca_file=wrong_dir / "localhost.pem"
                )
                try:
                    with pytest.raises(PlatformError):
                        await wrong_port.heartbeat(created["connection_id"], "wrong-ca-instance")
                finally:
                    await wrong_port.close()
                release = contracts()
                source_client = JsonService(platform_url, ENV["TS012_COMPANION"], ca_file=cert)
                sender_client = JsonService(platform_url, ENV["TS012_COMPANION"], ca_file=cert)
                clients += [source_client, sender_client]
                bot_sender = PlatformBotSender(release, sender_client)
                legacy_client = JsonService()
                clients.append(legacy_client)
                binding_id = "binding:bot-qq"
                bindings = {
                    binding_id: {
                        "service": "platform",
                        "namespace": "qq",
                        "audience": "group",
                        "actor_ids": ["actor:a"],
                        "classification": {
                            "value": "real",
                            "basis": "registered_input_mode",
                            "policy_ref": "fixture:bot",
                            "policy_version": 1,
                        },
                    }
                }
                sender = BotSenderRouter(
                    Sender(release, legacy_client), bot_sender, [binding_id], bindings
                )
                memory = FakeMemory(lambda: now)
                gateway = FakeGateway()
                gateway.segments = ["synthetic model reply"]
                core = Core(
                    Store(str(tmp_path / "core.db")),
                    release,
                    Origins(release, {"platform": ("platform", source_client)}),
                    memory,
                    gateway,
                    sender,
                    bindings=bindings,
                    roles={"actor:a": {"version": 1, "persona": "synthetic role"}},
                    config_version=1,
                    policy=Policy(silence_ms=0),
                    clock=lambda: now,
                    automatic_memory_candidates=False,
                )

                async def core_ingest(request):
                    if (
                        request.headers.get("Authorization")
                        != "Bearer synthetic-core-dispatch-credential"
                    ):
                        return web.json_response({"code": "unauthorized"}, status=401)
                    body = await request.json()
                    response = await core.ingest_actors("platform", body)
                    return web.json_response(response)

                core_app = web.Application()
                core_app.router.add_post("/internal/v1/conversation/ingest-actors", core_ingest)
                core_runner, core_url = await listen(core_app)
                platform.sources.core = {
                    "base_url": core_url,
                    "token_env": "TS013_CORE_DISPATCH",
                    "ca_file": cert,
                    "timeout_seconds": 10,
                }
                bot = FakeBot()
                runtime = BotRuntime(
                    connection_id=created["connection_id"],
                    platform_id="onebot11-main",
                    bot_self_id="42",
                    allowed_conversations=["group:99"],
                    journal_path=tmp_path / "bot.db",
                    platform_url=platform_url,
                    token=created["token"],
                    ca_file=cert,
                    get_bot=lambda _: bot,
                )
                assert await runtime.capture(bot, group_event())
                await runtime.flush_events()
                assert not runtime.journal.pending_events()
                event = runtime.journal.db.execute("SELECT state FROM inbound").fetchone()
                assert event["state"] == "accepted"
                assert len(core.store.list("inbox")) == 1
                assert await runtime.capture(bot, group_event())
                await runtime.flush_events()
                assert len(core.store.list("inbox")) == 1
                for _ in range(30):
                    await core.tick()
                    await asyncio.sleep(0.01)
                    if platform.bots.view()["connections"][0]["delivery"]["pending"]:
                        break
                assert len(gateway.calls) == 1
                claims = await runtime.port.claim(runtime.connection_id, runtime.instance_id)
                assert len(claims) == 1
                await runtime.deliver(bot, claims[0])
                await runtime.flush_acks()
                for _ in range(15):
                    await core.tick()
                    await asyncio.sleep(0.01)
                    if core.store.list("turns")[0]["delivery_state"] == "sent":
                        break
                assert core.store.list("turns")[0]["delivery_state"] == "sent"
                assert len(bot.calls) == 1
                assert platform.bots.view()["connections"][0]["delivery"]["sent"] == 1
            finally:
                if runtime:
                    await runtime.close()
                for client in clients:
                    await client.close()
                if core_runner:
                    await core_runner.cleanup()
                if platform_runner:
                    await platform_runner.cleanup()
                if core:
                    core.store.close()
                platform.close()

    asyncio.run(run())
