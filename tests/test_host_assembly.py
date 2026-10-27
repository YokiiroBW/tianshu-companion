"""Explicit host assembly over isolated stores and the original HTTP boundaries."""

import asyncio
import copy
import json
import os
from pathlib import Path
import sqlite3

import httpx
import pytest

from support import Harness
from tianshu_companion.app import build_runtime, create_app
from tianshu_companion import observability as obs
from tianshu_companion.image_comfy import ComfyUI


def configuration(tmp_path):
    return dict(
        database_path=str(tmp_path / "companion.sqlite"),
        contracts_path=os.environ["TIANSHU_CONTRACTS"],
        bindings={},
        roles={},
        services={},
    )


class RecordedClients:
    def __init__(self):
        self.targets = []

    def async_transport(self, target):
        self.targets.append(target)
        return httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json=dict(target=target, authorization=request.headers.get("Authorization")),
            )
        )


class Log(obs.NullLogAdapter):
    def __init__(self):
        self.events = []

    def emit(self, *args, **kwargs):
        self.events.append((args, kwargs))


@pytest.mark.parametrize("bridge_origin", ["bridge_origin", "gateway"])
def test_runtime_maps_service_roles_without_guessing_their_url(tmp_path, bridge_origin):
    async def run():
        config = configuration(tmp_path)
        expected = dict(
            platform_sender="platform",
            provider_selector="platform",
            provider_selector_background="platform",
            qq_admin="platform",
            actor_memory_authority="platform",
            image_credentials="platform",
            memory="memory",
            knowledge="knowledge",
            gateway="gateway",
            gateway_background="gateway",
        )
        names = [
            *expected,
            "platform_origin",
            "bridge_origin",
            "nonebot",
            "chat_history",
        ]
        config["services"] = {
            name: dict(url="https://same.fixture.invalid", token_env=name.upper()) for name in names
        }
        config["callers"] = {
            "platform": dict(
                token_env="INCOMING", issuer="platform", origin_service="platform_origin"
            ),
            "nonebot": dict(token_env="BRIDGE", issuer="nonebot", origin_service=bridge_origin),
        }
        config["provider_self_service"] = True
        environ = {name.upper(): "fixture:" + name for name in names}
        environ.update(INCOMING="fixture:incoming", BRIDGE="fixture:bridge")
        internal, log = RecordedClients(), Log()
        original = copy.deepcopy(config)
        core, tokens, clients, _ = build_runtime(
            config, internal_clients=internal, environ=environ, log_adapter=log
        )
        try:
            assert config == original
            assert tokens == dict(platform="fixture:incoming", nonebot="fixture:bridge")
            assert core.events is log
            used = []
            for client in clients:
                if isinstance(client.client._transport, httpx.MockTransport):
                    result = await client.call("/fixture/read", {})
                    name = client.token.removeprefix("fixture:")
                    target = "platform" if name == "platform_origin" else expected[name]
                    assert result == dict(target=target, authorization="Bearer fixture:" + name)
                    used.append(name)
                else:
                    assert client.token.removeprefix("fixture:") in {
                        "bridge_origin",
                        "nonebot",
                        "chat_history",
                        bridge_origin,
                    }
            assert set(used) == set(expected) | {"platform_origin"}
            assert core.memory.client is not core.knowledge_client
            assert (
                core.memory.client.memory_query_slots
                is not core.knowledge_client.memory_query_slots
            )
            assert log.events and all(client.log_adapter is log for client in clients)
        finally:
            await core.close()
            for client in clients:
                await client.close()

    asyncio.run(run())


def test_explicit_factories_keep_environment_paths_credentials_and_logs_separate(tmp_path):
    async def run():
        first = configuration(tmp_path)
        first["services"]["memory"] = dict(url="https://memory.fixture.invalid", token_env="TOKEN")
        first["callers"] = dict(memory=dict(token_env="INCOMING"))
        second = copy.deepcopy(first)
        second["database_path"] = str(tmp_path / "second.sqlite")
        path = tmp_path / "second.json"
        path.write_text(json.dumps(second), encoding="utf-8")
        before = dict(os.environ)
        adapters = [Log(), Log()]
        apps = []
        try:
            for index, supplied in enumerate((dict(config=first), dict(config_path=path))):
                apps.append(
                    create_app(
                        **supplied,
                        internal_clients=RecordedClients(),
                        environ=dict(
                            TOKEN=f"memory:{index}",
                            INCOMING=f"incoming:{index}",
                            TIANSHU_STORAGE_NAMESPACE=f"host-{index}",
                            TIANSHU_DIAGNOSTICS_TOKEN=f"diagnostic:{index}",
                        ),
                        log_adapter=adapters[index],
                    )
                )
            assert dict(os.environ) == before
            assert first["database_path"] == str(tmp_path / "companion.sqlite")
            for index, app in enumerate(apps):
                assert app.state.core.memory.client.token == f"memory:{index}"
                assert app.state.core.storage_runtime.options.namespace == f"host-{index}"
                assert app.state.health.authorized(f"Bearer diagnostic:{index}")
                assert app.state.log is adapters[index] and app.state.core.events is adapters[index]
                assert app.state.core.knowledge_client is not app.state.core.memory.client
                # Legacy knowledge still shares Memory's destination and credential,
                # with its original independent query admission gates.
                assert app.state.core.knowledge_client.token == f"memory:{index}"
                await app.state.core.memory.client.call("/fixture/read", {})
            assert all(adapter.events for adapter in adapters)
        finally:
            for app in apps:
                async with app.router.lifespan_context(app):
                    pass

    asyncio.run(run())


def test_external_comfy_credentials_use_explicit_environment_without_local_transport():
    async def run():
        client = ComfyUI(
            "http://127.0.0.1:8188", token_env="TOKEN", environ=dict(TOKEN="synthetic-comfy")
        )
        try:
            assert client.client.headers["Authorization"] == "Bearer synthetic-comfy"
            assert isinstance(client.client._transport, httpx.AsyncHTTPTransport)
        finally:
            await client.close()

    asyncio.run(run())


def test_quiesce_waits_for_admitted_root_request_but_keeps_callbacks_reachable():
    async def run():
        h = Harness()
        entered, release = asyncio.Event(), asyncio.Event()

        async def command(service, body):
            entered.set()
            await release.wait()
            h.core.store.put("metadata", dict(id="admitted:done"))
            return dict(done=True)

        h.core.direct_command = command
        h.core.source_facts = lambda service, body: dict(callback=True)
        app = create_app(
            h.core, dict(platform="incoming", memory="callback"), environ={}, log_adapter=Log()
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://fixture.invalid"
        ) as client:
            request = asyncio.create_task(
                client.post(
                    "/internal/v1/conversation/direct-command",
                    json={},
                    headers=dict(Authorization="Bearer incoming"),
                )
            )
            try:
                await asyncio.wait_for(entered.wait(), 1)
                await app.state.lifecycle.quiesce()
                draining = asyncio.create_task(app.state.lifecycle.drain())
                await asyncio.sleep(0)
                assert not draining.done()
                refused = await client.post(
                    "/internal/v1/conversation/ingest",
                    json={},
                    headers=dict(Authorization="Bearer incoming"),
                )
                assert refused.status_code == 503
                unauthenticated = await client.post("/internal/v1/conversation/ingest", json={})
                assert unauthenticated.status_code == 401
                callback = await client.post(
                    "/internal/v1/source-facts/read",
                    json={},
                    headers=dict(Authorization="Bearer callback"),
                )
                assert callback.status_code == 200 and callback.json() == dict(callback=True)
                release.set()
                assert (await request).status_code == 200
                assert await draining
                assert h.core.store.get("metadata", "admitted:done") is not None
            finally:
                release.set()
                await asyncio.gather(request, return_exceptions=True)
                await h.core.close()

    asyncio.run(run())


def test_drain_deadline_cancels_producer_and_all_domain_tasks_before_store_close():
    async def run():
        h = Harness()
        app = create_app(h.core, {}, environ={}, log_adapter=Log())
        started, settled = set(), set()

        async def held(name):
            started.add(name)
            try:
                await asyncio.Event().wait()
            finally:
                h.core.store.put("metadata", dict(id="settled:" + name))
                settled.add(name)

        app.state.lifecycle.producers.append(asyncio.create_task(held("producer")))
        h.core.jobs["fixture"] = asyncio.create_task(held("turn"))
        h.core.memory_learning.jobs["fixture"] = asyncio.create_task(held("memory"))
        h.core.life._jobs["fixture"] = asyncio.create_task(held("life"))
        await asyncio.sleep(0)
        try:
            assert len(started) == 4
            assert not await app.state.lifecycle.drain(timeout=0.01)
            assert settled == started
            assert all(
                h.core.store.get("metadata", "settled:" + name) is not None for name in started
            )
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.fixture
def exchange_type(monkeypatch):
    repository = os.environ.get("TIANSHU_UNIFIED_RUNTIME_REPO")
    if not repository or not (Path(repository) / "deploy/single_container/exchange.py").is_file():
        pytest.skip("explicit TIANSHU_UNIFIED_RUNTIME_REPO required for the actual exchange")
    monkeypatch.syspath_prepend(repository)
    from deploy.single_container.exchange import LocalExchange

    return LocalExchange


def test_real_exchange_drains_last_pass_and_sync_source_callback_without_sockets(
    tmp_path, exchange_type
):
    async def run():
        from starlette.applications import Starlette
        from starlette.responses import JSONResponse
        from starlette.routing import Route

        exchange = exchange_type()
        config = configuration(tmp_path)
        config["services"]["memory"] = dict(
            url="https://memory.fixture.invalid", token_env="MEMORY"
        )
        config["callers"] = dict(
            memory=dict(token_env="CALLBACK"),
            platform=dict(
                token_env="INCOMING", issuer="platform", origin_service="platform_origin"
            ),
        )
        app = create_app(
            config=config,
            internal_clients=exchange,
            environ=dict(MEMORY="to-memory", CALLBACK="to-core", INCOMING="from-platform"),
            log_adapter=Log(),
        )
        core = app.state.core
        entered, release, called = asyncio.Event(), asyncio.Event(), asyncio.Event()

        @app.middleware("http")
        async def local_scope(request, call_next):
            assert exchange.owns(request) and request.scope["scheme"] == "http"
            assert request.client is None
            return await call_next(request)

        def facts(service, body):
            assert service == "memory"
            core.store.put("metadata", dict(id="callback:alive"))
            return dict(source="alive")

        core.source_facts = facts

        async def memory(request):
            assert exchange.owns(request) and request.scope["scheme"] == "http"
            assert request.headers["Authorization"] == "Bearer to-memory"

            def callback():
                with httpx.Client(transport=exchange.sync_transport("companion")) as client:
                    response = client.post(
                        "https://companion.fixture.invalid/internal/v1/source-facts/read",
                        json={},
                        headers=dict(Authorization="Bearer to-core"),
                    )
                    assert response.status_code == 200
                    return response.json()

            return JSONResponse(await asyncio.to_thread(callback))

        async def domain():
            answer = await core.memory.client.call("/fixture", {})
            assert answer == dict(source="alive")
            core.store.put("metadata", dict(id="domain:done"))
            called.set()

        async def last_pass():
            entered.set()
            await release.wait()
            core.jobs["fixture"] = asyncio.create_task(domain())

        core.tick = last_pass
        exchange.register(
            "memory",
            Starlette(routes=[Route("/fixture", memory, methods=["POST"])]),
            "asgi",
            "https://memory.fixture.invalid",
        )
        exchange.register("companion", app, "asgi", "https://companion.fixture.invalid")
        await exchange.open()
        try:
            await asyncio.wait_for(entered.wait(), 1)
            await app.state.lifecycle.quiesce()
            draining = asyncio.create_task(app.state.lifecycle.drain(timeout=2))
            await asyncio.sleep(0)
            assert not draining.done()
            release.set()
            assert await draining and called.is_set()
            assert core.store.get("metadata", "callback:alive") is not None
            assert core.store.get("metadata", "domain:done") is not None
            assert not core.memory.client.client.is_closed
        finally:
            release.set()
            await exchange.close()
        assert core.memory.client.client.is_closed
        with pytest.raises(sqlite3.ProgrammingError):
            core.store.db.execute("SELECT 1")

    asyncio.run(run())
