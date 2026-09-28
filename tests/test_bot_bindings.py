"""Dynamic bot bindings cannot forge roles or route a disabled bot to the legacy sender."""

import tempfile
import os
import copy
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

from tianshu_companion.app import build_runtime, create_app
from tianshu_companion.bot_bindings import BotBindings
from tianshu_companion.clients import BotSenderRouter
from tianshu_companion.contracts import Fault
from tianshu_companion.store import Store


class Sender:
    available = True

    async def send(self, request):
        return None

    async def reconcile(self, request):
        return None


def request(revision=1, enabled=False):
    return {
        "request_id": f"request:{revision}",
        "connection_id": "bot:12345678",
        "revision": revision,
        "binding_id": "binding:bot:12345678",
        "actor_id": "actor:a",
        "conversation": {"kind": "group", "id": "group:123"},
        "enabled": enabled,
    }


def test_persisted_binding_lifecycle_and_static_isolation():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "core.sqlite"
        for restart in range(2):
            store = Store(path)
            bindings = {"binding:static": {"service": "nonebot", "namespace": "qq"}}
            legacy, platform = Sender(), Sender()
            router = BotSenderRouter(legacy, platform, [], bindings)
            manager = BotBindings(
                SimpleNamespace(store=store, bindings=bindings, roles={"actor:a": {}}),
                router,
                bindings,
            )
            if restart == 0:
                first = request()
                with pytest.raises(Fault) as error:
                    manager.apply("nonebot", first)
                assert error.value.code == "forbidden"
                with pytest.raises(Fault) as error:
                    manager.apply("platform", {**first, "actor_id": "actor:unapproved"})
                assert error.value.code == "invalid_input"
                with pytest.raises(Fault) as error:
                    manager.apply("platform", {**first, "binding_id": "binding:static"})
                assert error.value.code == "invalid_input"
                assert manager.apply("platform", first)["enabled"] is False
                assert (
                    router._sender({"destination": {"binding_id": first["binding_id"]}}) is platform
                )
                assert first["binding_id"] not in bindings
                with pytest.raises(Fault) as error:
                    manager.apply("platform", {**first, "revision": 1, "enabled": True})
                assert error.value.code == "idempotency_conflict"
                assert manager.apply("platform", request(2, True))["enabled"] is True
                assert first["binding_id"] in bindings
                assert manager.apply("platform", request(3, False))["enabled"] is False
                assert first["binding_id"] not in bindings
            else:
                assert manager.status("platform", {"connection_id": "bot:12345678"})["binding"] == {
                    "connection_id": "bot:12345678",
                    "revision": 3,
                    "enabled": False,
                }
                assert (
                    router._sender({"destination": {"binding_id": "binding:bot:12345678"}})
                    is platform
                )
                assert "binding:static" in bindings
            store.close()


def test_removed_role_or_new_static_binding_closes_dynamic_without_blocking_core():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "core.sqlite"
        store = Store(path)
        bindings = {}
        router = BotSenderRouter(Sender(), Sender(), [], bindings)
        manager = BotBindings(
            SimpleNamespace(store=store, bindings=bindings, roles={"actor:a": {}}), router, bindings
        )
        manager.apply("platform", request())
        manager.apply("platform", request(2, True))
        store.close()
        store = Store(path)
        bindings = {}
        router = BotSenderRouter(Sender(), Sender(), [], bindings)
        manager = BotBindings(
            SimpleNamespace(store=store, bindings=bindings, roles={}), router, bindings
        )
        assert (
            manager.status("platform", {"connection_id": "bot:12345678"})["binding"]["enabled"]
            is False
        )
        assert "binding:bot:12345678" not in bindings
        store.close()
        store = Store(path)
        static = {"binding:bot:12345678": {"service": "nonebot", "namespace": "qq"}}
        legacy, platform = Sender(), Sender()
        router = BotSenderRouter(legacy, platform, [], static)
        manager = BotBindings(
            SimpleNamespace(store=store, bindings=static, roles={"actor:a": {}}), router, static
        )
        assert (
            manager.status("platform", {"connection_id": "bot:12345678"})["binding"]["enabled"]
            is False
        )
        assert static["binding:bot:12345678"]["service"] == "nonebot"
        assert router._sender({"destination": {"binding_id": "binding:bot:12345678"}}) is legacy
        store.close()


def test_dynamic_selection_makes_platform_only_sender_available():
    legacy, platform = Sender(), Sender()
    legacy.available = False
    router = BotSenderRouter(legacy, platform, [], {})
    assert router.available is False
    router.select_dynamic("binding:bot:12345678")
    assert router.available is True


def test_management_gate_and_authenticated_platform_endpoint():
    base = {
        "contracts_path": os.environ["TIANSHU_CONTRACTS"],
        "database_path": ":memory:",
        "roles": {"actor:a": {}},
        "callers": {
            "platform": {
                "token_env": "TEST_BOT_PLATFORM_CALLER",
                "issuer": "platform",
                "origin_service": "platform_origin",
            }
        },
        "services": {
            "platform_sender": {
                "url": "https://platform.test",
                "token_env": "TEST_BOT_PLATFORM_SENDER",
            }
        },
    }

    async def run():
        with patch.dict(
            os.environ,
            {
                "TEST_BOT_PLATFORM_CALLER": "platform-only",
                "TEST_BOT_PLATFORM_SENDER": "sender-only",
            },
        ):
            core, _, clients, _ = build_runtime(base)
            app = create_app(core, {"platform": "platform-only", "nonebot": "nonebot-only"})
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post(
                    "/internal/v1/bot-bindings/apply",
                    json=request(),
                    headers={"Authorization": "Bearer platform-only"},
                )
                assert response.status_code == 503
            await core.close()
            for client in clients:
                await client.close()
            for invalid in (
                {
                    "callers": {
                        "platform": {
                            "token_env": "TEST_BOT_PLATFORM_CALLER",
                            "issuer": "nonebot",
                            "origin_service": "platform_origin",
                        }
                    }
                },
                {"services": {}},
            ):
                config = copy.deepcopy(base)
                config.update(invalid)
                config["bot_binding_management_enabled"] = True
                with pytest.raises(ValueError, match="Bot binding management requires"):
                    build_runtime(config)
            config = {**base, "bot_binding_management_enabled": True}
            core, _, clients, _ = build_runtime(config)
            app = create_app(core, {"platform": "platform-only", "nonebot": "nonebot-only"})
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post(
                    "/internal/v1/bot-bindings/apply",
                    json=request(),
                    headers={"Authorization": "Bearer nonebot-only"},
                )
                assert response.status_code == 403
                response = await client.post(
                    "/internal/v1/bot-bindings/apply",
                    json=request(),
                    headers={"Authorization": "Bearer platform-only"},
                )
                assert response.status_code == 200
                assert response.json()["enabled"] is False
            await core.close()
            for client in clients:
                await client.close()

    asyncio.run(run())
