"""Registry/source execution boundaries with isolated actors and explicit adapters."""

import asyncio
import copy
import json

import pytest

from support import Harness
from test_comfy_deep import configured, image_value, SemanticModel
from test_delivery_v2 import source_turn
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.skills.native import definition
from tianshu_companion.skills.registry import EMPTY_CONFIG


async def manage(h, operation, value, actor="actor:a", request_id=None, expected=None):
    if not h.core.store.get("life_actors", actor):
        h.core.recover()
    return await h.core.skills.call(
        "platform",
        "manage",
        dict(
            schema_version=1,
            request_id=request_id or operation + ":" + digest(value),
            actor_id=actor,
            expected_version=expected or h.core.skills.actor(actor)["version"],
            operation=operation,
            value=value,
        ),
    )


def tool(name="life_image_request"):
    return dict(
        id="selected",
        function=dict(
            name=name, arguments=json.dumps(dict(value=dict(intent=dict(outfit="opaque pajamas"))))
        ),
    )


class EchoAdapter:
    domain = "test"
    operations = {"echo.call": "skill_echo_call"}

    def validate_config(self, config):
        if set(config["options"]) - {"label"}:
            raise Fault("invalid_input")

    def public_options(self, config):
        return {key: config["options"][key] for key in ("label",) if key in config["options"]}

    def availability(self, registry, actor, config):
        return dict(state="available", can_execute=True, reason_code=None)

    def tools(self, actions, turn, definition):
        return [
            dict(
                type="function",
                function=dict(
                    name="skill_echo_call",
                    description="Echo fixture",
                    parameters=dict(type="object", properties={}, additionalProperties=False),
                ),
            )
        ]

    def context(self, actions, turn):
        return None

    def instructions(self):
        return "Synthetic adapter only."

    async def execute(self, actions, turn_id, tool, *, model_slot_held=False):
        return dict(state="completed", adapter="explicit_fixture"), []


def source(source_id="source:one"):
    return dict(
        source_id=source_id,
        name=source_id,
        manifest_url="http://fixture.invalid/catalog.json",
        credential_ref=None,
        enabled=True,
        expected_sha256=None,
    )


def manifest(definitions):
    return json.dumps(dict(schema_version=1, source_version="1.0.0", skills=definitions)).encode()


def test_actor_cas_replay_and_disabled_model_tool(tmp_path):
    async def run():
        h = Harness(silence_ms=0)
        try:
            turn = await source_turn(h)
            await configured(h, tmp_path)
            original = h.core.skills.result("actor:a")
            assert original["actor_version"] == 1
            unsupported = [skill for skill in original["skills"] if not skill["installed"]]
            assert all(skill["availability"]["state"] == "unsupported" for skill in unsupported)
            names = {item["function"]["name"] for item in h.core.role_actions.tools(turn)}
            assert {
                "life_image_request",
                "life_read",
                "memory_propose",
                "life_concern_save",
            } <= names
            response = await manage(
                h, "skill.disable", dict(skill_id="image.generate"), request_id="same", expected=1
            )
            assert response["result"]["actor_version"] == 2
            assert (
                await manage(
                    h,
                    "skill.disable",
                    dict(skill_id="image.generate"),
                    request_id="same",
                    expected=1,
                )
            ) == response
            with pytest.raises(Fault, match="version_conflict"):
                await manage(h, "skill.enable", dict(skill_id="image.generate"), expected=1)
            names = {item["function"]["name"] for item in h.core.role_actions.tools(turn)}
            assert "life_image_request" not in names and "life_read" in names
            with pytest.raises(Fault):
                await h.core.role_actions.execute(turn["id"], tool())
            assert h.core.skills.project(
                "actor:b", h.core.skills.definitions("actor:b")["image.generate"]
            )["enabled"]
            with pytest.raises(Fault, match="forbidden"):
                await h.core.skills.call(
                    "stranger",
                    "read",
                    dict(schema_version=1, request_id="x", actor_id="actor:a", resource="list"),
                )
            with pytest.raises(Fault, match="invalid_input"):
                await h.core.skills.call(
                    "platform",
                    "read",
                    dict(schema_version=1, request_id="x", actor_id="actor:a", resource="detail"),
                )
        finally:
            await h.core.close()

    asyncio.run(run())


def test_definition_update_new_turn_and_selected_revision_only(tmp_path):
    async def run():
        h = Harness(silence_ms=0)
        try:
            turn = await source_turn(h)
            await configured(h, tmp_path)
            h.core.role_actions.tools(turn)
            before = h.core.skills.result("actor:a", "detail", "image.generate")["skills"][0]
            other = copy.deepcopy(h.core.skills.definitions("actor:a")["game.guides"])
            other.pop("source_id")
            other["version"] = "2.0.0"
            await manage(
                h, "skill.update", dict(definition=other, enabled=False, config=EMPTY_CONFIG)
            )
            assert (
                before["revision"]
                == h.core.skills.result("actor:a", "detail", "image.generate")["skills"][0][
                    "revision"
                ]
            )
            changed = copy.deepcopy(before["definition"])
            changed.update(version="2.0.0", description="updated clothing capability")
            await manage(
                h, "skill.update", dict(definition=changed, enabled=True, config=EMPTY_CONFIG)
            )
            with pytest.raises(Fault, match="scope_changed"):
                await h.core.role_actions.execute(turn["id"], tool())
            fresh = copy.deepcopy(turn)
            fresh.update(id="turn:new", skill_pins={}, skill_tool_owners={})
            h.core.store.put("turns", fresh)
            offered = h.core.role_actions.tools(fresh)
            image = next(
                item for item in offered if item["function"]["name"] == "life_image_request"
            )
            assert "updated clothing capability" in image["function"]["description"]
            assert "including modest sleepwear" in h.core.skills.instructions(fresh)
        finally:
            await h.core.close()

    asyncio.run(run())


def test_source_atomic_ownership_removal_and_stable_refresh(monkeypatch):
    async def run():
        h = Harness(silence_ms=0)
        try:
            turn = await source_turn(h)
            catalog = h.core.skills
            echo = definition("test.echo", "Echo", "fixture", "test", ["echo.call"], "test.echo")
            # Local adapter registration is executable; remote metadata cannot install one.
            catalog.handlers["test.echo"] = EchoAdapter()
            current = manifest([echo])

            async def fetch(*args):
                return current

            monkeypatch.setattr("tianshu_companion.skills.sources.fetch", fetch)
            await manage(h, "source.configure", source())
            first = (
                await manage(h, "source.refresh", dict(source_id="source:one"), request_id="first")
            )["result"]
            entry = next(item for item in first["skills"] if item["id"] == "test.echo")
            assert entry["installed"] and not entry["enabled"]
            await manage(h, "skill.enable", dict(skill_id="test.echo"))
            catalog.tools(turn)
            revision = catalog.result("actor:a", "detail", "test.echo")["skills"][0]["revision"]
            h.clock.advance(1)
            await manage(h, "source.refresh", dict(source_id="source:one"), request_id="again")
            assert (
                catalog.result("actor:a", "detail", "test.echo")["skills"][0]["revision"]
                == revision
            )
            result, _ = await h.core.role_actions.execute(
                turn["id"], dict(id="echo", function=dict(name="skill_echo_call", arguments="{}"))
            )
            assert result["adapter"] == "explicit_fixture"
            stable = catalog.result("actor:a")["catalog_version"]
            current = manifest([echo, catalog.builtins["image.generate"]])
            failure = (
                await manage(
                    h, "source.refresh", dict(source_id="source:one"), request_id="collision"
                )
            )["result"]
            assert (
                failure["sources"][0]["state"] == "invalid" and failure["catalog_version"] == stable
            )
            current = manifest([])
            await manage(h, "source.refresh", dict(source_id="source:one"), request_id="remove")
            assert "test.echo" not in catalog.definitions("actor:a")
            with pytest.raises(Fault):
                await h.core.role_actions.execute(
                    turn["id"],
                    dict(id="echo", function=dict(name="skill_echo_call", arguments="{}")),
                )
        finally:
            await h.core.close()

    asyncio.run(run())


def test_same_tool_has_explicit_owner_not_disabled_alias(tmp_path):
    async def run():
        h = Harness(silence_ms=0)
        try:
            turn = await source_turn(h)
            await configured(h, tmp_path)
            alias = definition(
                "image.alias", "Alternate", "fixture", "image", ["image.request"], "image.generate"
            )
            await manage(
                h, "skill.update", dict(definition=alias, enabled=False, config=EMPTY_CONFIG)
            )
            offered = h.core.role_actions.tools(turn)
            assert (
                len([item for item in offered if item["function"]["name"] == "life_image_request"])
                == 1
            )
            saved = h.core.store.get("turns", turn["id"])
            assert saved["skill_tool_owners"]["life_image_request"] == "image.generate"
            await manage(h, "skill.enable", dict(skill_id="image.alias"))
            assert (
                h.core.store.get("turns", turn["id"])["skill_tool_owners"]
                == saved["skill_tool_owners"]
            )
        finally:
            await h.core.close()

    asyncio.run(run())


def test_aggregate_contract_limit_rolls_back_source_catalog(monkeypatch):
    async def run():
        h = Harness(silence_ms=0)
        try:
            catalog = h.core.skills
            await manage(h, "source.configure", source())
            entries = [
                definition(
                    "external:" + str(n),
                    "Example",
                    "no adapter",
                    "extension",
                    ["query"],
                    "uninstalled",
                )
                for n in range(90)
            ]
            current = manifest(entries)

            async def fetch(*args):
                return current

            monkeypatch.setattr("tianshu_companion.skills.sources.fetch", fetch)
            await manage(h, "source.refresh", dict(source_id="source:one"))
            await manage(h, "source.configure", source("source:two"))
            before = catalog.actor("actor:a")
            current = manifest(
                [
                    definition(
                        "extra:" + str(n),
                        "Example",
                        "no adapter",
                        "extension",
                        ["query"],
                        "uninstalled",
                    )
                    for n in range(2)
                ]
            )
            with pytest.raises(Fault, match="invalid_input"):
                await manage(h, "source.refresh", dict(source_id="source:two"))
            assert catalog.actor("actor:a") == before
            assert len(catalog.result("actor:a")["skills"]) == 99
        finally:
            await h.core.close()

    asyncio.run(run())


def test_skill_disable_leaves_existing_domain_job(tmp_path):
    async def run():
        h = Harness(silence_ms=0)
        try:
            await configured(h, tmp_path)
            value = image_value(assist_model=False)
            prepared = await h.core.life_runtime.prepare_action(
                "actor:a", "image.request", value, 0
            )
            job = h.core.life_runtime._execute(
                "actor:a", "image.request", value, 0, "queued", "platform", prepared=prepared
            )
            await manage(h, "skill.disable", dict(skill_id="image.generate"))
            assert h.core.images.get(job["id"])["state"] == "queued"
            # Administrative image management remains in the original domain interface.
            await h.core.life_runtime.prepare_action(
                "actor:a", "image.request", image_value(id="admin:second", assist_model=False), 0
            )
        finally:
            await h.core.close()

    asyncio.run(run())


def test_native_disable_during_translation_does_not_create_job(tmp_path):
    async def run():
        h = Harness(silence_ms=0)
        try:
            turn = await source_turn(h)
            await configured(h, tmp_path)

            class ChangingModel(SemanticModel):
                async def generate(self, turn, messages):
                    await manage(h, "skill.disable", dict(skill_id="image.generate"))
                    return await super().generate(turn, messages)

            model = ChangingModel()
            h.core.gateway = h.core.life.gateway = model
            h.core.role_actions.tools(turn)
            with pytest.raises(Fault, match="dependency_unavailable"):
                await h.core.role_actions.execute(turn["id"], tool())
            assert len(model.calls) == 1 and h.core.store.list("image_jobs") == []
        finally:
            await h.core.close()

    asyncio.run(run())
