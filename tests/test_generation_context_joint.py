"""Opt-in joint routing-grant checks against the actual Platform owner.

The real ProviderAuthority/ProviderCatalog use isolated SQLite files. Transport,
registered service authentication and the model response are explicit fixtures;
this is not real TLS, provider execution, PG or production configuration.
"""

import asyncio
from contextlib import closing
from dataclasses import asdict
import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from support import Harness, contracts
from test_generation_context import Model
from tianshu_companion.clients import JsonService
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.model_selection import HttpDefaultModelSelector, SelectionRequest


@pytest.fixture
def owner(tmp_path, monkeypatch):
    repository = os.environ.get("TIANSHU_PLATFORM_REPO")
    if (
        not repository
        or not (Path(repository) / "services/platform/provider_authority.py").is_file()
    ):
        pytest.skip("explicit TIANSHU_PLATFORM_REPO required for the actual grant owner")
    monkeypatch.syspath_prepend(repository)
    from services.platform.contracts import Fault as PlatformFault
    from services.platform.provider_authority import ProviderAuthority
    from services.platform.provider_catalog import ProviderCatalog

    def clock():
        return 1_900_000_000

    catalog = ProviderCatalog(tmp_path / "provider", create=True, clock=clock)
    provider = catalog.save(
        client_id="fixture:create",
        name="Fixture provider",
        base_url="https://provider.fixture.invalid/v1",
        model_id="fixture-model",
        api_key="synthetic-key-never-real",
    )
    catalog.record_test(
        client_id="fixture:test",
        provider_id=provider["provider_id"],
        expected_revision=1,
        outcome="succeeded",
    )
    catalog.set_default(
        client_id="fixture:default",
        provider_id=provider["provider_id"],
        expected_revision=1,
        expected_default_revision=0,
    )
    database = tmp_path / "platform.sqlite"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE configs(version INTEGER PRIMARY KEY)")

    class RegisteredServiceAuth:
        def authenticate(self, header, db, action):
            assert action == "config.select"
            return "fixture", dict(
                kind="service",
                service={
                    "Bearer front-fixture": "companion",
                    "Bearer back-fixture": "companion_background",
                }[header],
            )

    platform = SimpleNamespace(
        provider_catalog=catalog,
        contracts=contracts(),
        auth=RegisteredServiceAuth(),
        store=SimpleNamespace(connect=lambda: closing(sqlite3.connect(database))),
        models=SimpleNamespace(max_lifetime=3600),
    )
    return SimpleNamespace(
        authority=ProviderAuthority(platform, clock),
        clock=clock,
        fault=PlatformFault,
        catalog=catalog,
        wire=[],
    )


def selector(owner, caller):
    token = "back-fixture" if caller == "companion_background" else "front-fixture"

    def request(message):
        body = json.loads(message.content)
        owner.wire.append(body)
        result = owner.authority.select(message.headers["Authorization"], body)
        return httpx.Response(200, json=result)

    client = JsonService(
        "https://platform.fixture.invalid", token, transport=httpx.MockTransport(request)
    )
    return HttpDefaultModelSelector(client, caller_service=caller), client


def test_new_background_learning_uses_distinct_real_grant_and_keeps_route_on_restart(
    owner, tmp_path
):
    async def run():
        h = Harness(tmp_path / "companion.sqlite")
        back, wire = selector(owner, "companion_background")
        try:
            await h.core.close()
            h.clock.now = owner.clock()
            h.core = h.new_core(background_gateway=Model(1), background_model_selector=back)
            scope = dict(
                actor_id="actor:a",
                person_id="person:a",
                audience="self_private",
                conversation_id="conv:a",
            )
            original_id = "memory-generation:" + digest("existing-job")
            old = SelectionRequest(original_id, **scope, function_id="memory")
            owner.authority.select("Bearer front-fixture", asdict(old))
            with pytest.raises(owner.fault, match="version_conflict"):
                owner.authority.select(
                    "Bearer back-fixture", dict(asdict(old), caller_service="companion_background")
                )
            item = dict(id="learning:new", conversation_id="memory:learning", state="pending")
            h.core.store.put("metadata", item)
            generation = dict(id=original_id, config_version=1)
            await h.core.memory_learning._pin_model(item, generation, scope)
            frozen = dict(item["model_route"])
            assert (
                frozen["caller_service"] == "companion_background"
                and frozen["turn_id"] != original_id
            )
            assert owner.wire[-1]["turn_id"] == frozen["turn_id"]
            assert owner.wire[-1]["function_id"] == "memory"
            # Persisted caller/ID survive removal of that identity's configuration.
            # It fails rather than borrowing a front grant or minting another ID.
            await h.core.close()
            h.core = h.new_core()
            retained = h.core.store.get("metadata", item["id"])
            with pytest.raises(Fault, match="dependency_unavailable"):
                await h.core.memory_learning._pin_model(
                    retained, dict(id=original_id, config_version=1), scope
                )
            assert retained["model_route"] == frozen
        finally:
            await wire.close()
            await h.core.close()

    asyncio.run(run())


def test_image_front_and_background_do_not_reuse_each_others_real_grants(owner):
    async def run():
        h = Harness()
        front, front_wire = selector(owner, "companion")
        back, back_wire = selector(owner, "companion_background")
        try:
            await h.core.close()
            h.clock.now = owner.clock()
            h.core = h.new_core(
                background_gateway=Model(1),
                background_model_selector=back,
                default_model_selector=front,
            )
            operation, actor = "image-translate:existing-request", "actor:a"
            old = SelectionRequest(
                operation,
                actor,
                "person:life:" + digest(actor),
                "self_private",
                "life:" + digest(actor),
                caller_service="companion_background",
                function_id="writing",
            )
            original = owner.authority.select("Bearer back-fixture", asdict(old))
            version = original["config_version"]
            h.core.gateway = Model(version)
            h.core.life.gateway = Model(version)
            catalog = h.core.image_backend.catalog
            await catalog.model(actor, operation, "system", {})
            await catalog.model(actor, operation, "system", {}, background=True)
            front_id, back_id = [body["turn_id"] for body in owner.wire]
            assert len({front_id, back_id, operation}) == 3
            assert [body["caller_service"] for body in owner.wire] == [
                "companion",
                "companion_background",
            ]
            assert len(h.core.gateway.calls) == len(h.core.life.gateway.calls) == 1
        finally:
            await front_wire.close()
            await back_wire.close()
            await h.core.close()

    asyncio.run(run())
