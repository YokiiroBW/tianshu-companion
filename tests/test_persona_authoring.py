"""Authoring stays in the existing Persona revision and operation ledger."""

import asyncio
from contextlib import closing

import pytest

from tianshu_companion.personas import PersonaError, Personas
from tianshu_companion.store import Store
from support import Harness, persona_config


def operation(op_name, request_id, **fields):
    return {
        "operation": op_name,
        "request_id": request_id,
        "operator": "synthetic-admin",
        "reason": "explicit editor action",
        **fields,
    }


def test_create_edit_apply_replay_conflict_and_restart(tmp_path):
    path = tmp_path / "authoring.sqlite"
    seed = {
        "config_version": 1,
        "roles": {"actor:a": {"version": 1, "persona": "Before", "custom": "keep-me"}},
    }
    with closing(Store(path)) as store:
        personas = Personas(store, lambda: 1000.0)
        personas.import_config(seed)
        old_pin = personas.pin("actor:a")
        create = operation(
            "create_profile",
            "create-1",
            name="温和伙伴",
            description="合成档案",
            content={"persona": "Hello", "tone": "Warm", "style": "Short", "address": "Friend"},
        )
        created = personas.manage(create)["item"]
        assert created["kind"] == "profile"
        assert personas.manage(create)["item"] == created
        assert personas.pin("actor:a")["revision_id"] == old_pin["revision_id"]
        with pytest.raises(PersonaError) as conflict:
            personas.manage({**create, "name": "changed"})
        assert conflict.value.code == "invalid_input"

        saved = personas.manage(
            operation(
                "save_profile",
                "save-1",
                subject=created["id"],
                expected=created["version"],
                name="温和伙伴",
                description="合成档案",
                content={
                    "persona": "Hello revised",
                    "tone": "Warm",
                    "style": "Short",
                    "address": "Friend",
                },
            )
        )["item"]
        assert personas.pin("actor:a")["revision_id"] == old_pin["revision_id"]
        with pytest.raises(PersonaError) as stale:
            personas.manage(
                operation(
                    "save_profile",
                    "save-2",
                    subject=created["id"],
                    expected=created["version"],
                    name="温和伙伴",
                    description="合成档案",
                    content={"persona": "stale"},
                )
            )
        assert stale.value.code == "version_conflict"

        apply = operation(
            "apply_profile",
            "apply-1",
            subject=created["id"],
            expected=saved["version"],
            target="actor:a",
            target_expected=personas.get("actor:a")["version"],
            name="温和伙伴",
            description="合成档案",
            content={
                "persona": "Hello revised",
                "tone": "Warm",
                "style": "Short",
                "address": "Friend",
            },
        )
        first = personas.manage(apply)["item"]
        assert personas.manage(apply)["item"] == first
        assert (
            first["profile"]["last_applied_target_revision"]
            == first["target"]["published_revision"]
        )
        assert len(personas.publications("actor:a")) == 2
        assert len(personas.approvals("actor:a")) == 1
        assert personas.pin("actor:a")["content"]["custom"] == "keep-me"
        assert personas.pin("actor:a")["persona"] == "Hello revised"
        assert old_pin["persona"] == "Before"
        assert personas.verify(old_pin) == old_pin["revision_id"]
        before_failed_apply = personas.author_view(created["id"])
        with pytest.raises(PersonaError) as stale_target:
            personas.manage(
                operation(
                    "apply_profile",
                    "apply-stale-target",
                    subject=created["id"],
                    expected=first["profile"]["version"],
                    target="actor:a",
                    target_expected=1,
                    name="温和伙伴",
                    description="合成档案",
                    content={"persona": "Must roll back"},
                )
            )
        assert stale_target.value.code == "version_conflict"
        assert personas.author_view(created["id"]) == before_failed_apply
        assert personas.pin("actor:a")["persona"] == "Hello revised"
        with pytest.raises(PersonaError) as wrong_target:
            personas.manage(
                operation(
                    "apply_profile",
                    "apply-2",
                    subject=created["id"],
                    expected=first["profile"]["version"],
                    target="actor:unknown",
                    target_expected=1,
                )
            )
        assert wrong_target.value.code == "not_found"

    with closing(Store(path)) as store:
        personas = Personas(store, lambda: 1001.0)
        assert personas.manage(create)["item"] == created
        assert personas.manage(apply)["item"] == first
        assert personas.pin("actor:a")["persona"] == "Hello revised"
        personas.import_config(seed)
        assert personas.pin("actor:a")["persona"] == "Hello revised"
        assert created["id"] not in personas.subjects()


def test_existing_role_can_save_then_apply_without_losing_extensions(tmp_path):
    with closing(Store(tmp_path / "role.sqlite")) as store:
        personas = Personas(store, lambda: 1000.0)
        personas.import_config(
            {
                "config_version": 1,
                "roles": {"actor:a": {"version": 1, "persona": "Before", "custom": "retained"}},
            }
        )
        version = personas.get("actor:a")["version"]
        saved = personas.manage(
            operation(
                "save_role",
                "draft-role",
                subject="actor:a",
                expected=version,
                name="角色甲",
                description="简介",
                content={"persona": "Draft", "tone": "Gentle"},
            )
        )["item"]
        assert personas.pin("actor:a")["persona"] == "Before"
        applied = personas.manage(
            operation(
                "apply_role",
                "apply-role",
                subject="actor:a",
                expected=saved["version"],
                name="角色甲",
                description="简介",
                content={"persona": "Applied", "tone": "Gentle"},
            )
        )["item"]
        assert applied["draft_revision"] is None
        assert personas.pin("actor:a")["content"]["custom"] == "retained"
        assert personas.pin("actor:a")["persona"] == "Applied"
        assert len(personas.publications("actor:a")) == 2
        copied = personas.manage(
            operation(
                "create_profile",
                "copy-role",
                name="角色甲副本",
                description="",
                source="actor:a",
                source_expected=applied["version"],
                content={"persona": "Copied", "tone": "Gentle"},
            )
        )["item"]
        assert personas.author_view(copied["id"])["content"]["custom"] == "retained"


def test_applying_profile_clears_omitted_optional_text_but_keeps_role_extensions(tmp_path):
    with closing(Store(tmp_path / "clear-optional.sqlite")) as store:
        personas = Personas(store, lambda: 1000.0)
        personas.import_config(
            {
                "config_version": 1,
                "roles": {
                    "actor:a": {
                        "version": 1,
                        "persona": "Old",
                        "tone": "Old tone",
                        "style": "Old style",
                        "address": "Old address",
                        "custom": "retained",
                    }
                },
            }
        )
        profile = personas.manage(
            operation(
                "create_profile",
                "clear-create",
                name="清空可选字段",
                description="",
                content={"persona": "New", "tone": "", "style": "", "address": ""},
            )
        )["item"]
        personas.manage(
            operation(
                "apply_profile",
                "clear-apply",
                subject=profile["id"],
                expected=profile["version"],
                target="actor:a",
                target_expected=personas.get("actor:a")["version"],
                name="清空可选字段",
                description="",
                content={"persona": "New", "tone": "", "style": "", "address": ""},
            )
        )
        pinned = personas.pin("actor:a")["content"]
        assert pinned["persona"] == "New"
        assert pinned["custom"] == "retained"
        assert all(field not in pinned for field in ("tone", "style", "address"))


def test_core_preparation_pins_only_the_explicitly_applied_revision():
    async def scenario():
        harness = Harness(personas=persona_config())
        core = harness.core
        prior = core._pin_role("actor:a")
        created = core.manage_persona(
            "persona_admin",
            operation(
                "create_profile",
                "core-create",
                name="合成档案",
                description="",
                content={"persona": "Later", "tone": "Soft"},
            ),
        )["item"]
        assert core._pin_role("actor:a")["revision_id"] == prior["revision_id"]
        core.manage_persona(
            "persona_admin",
            operation(
                "apply_profile",
                "core-apply",
                subject=created["id"],
                expected=created["version"],
                target="actor:a",
                target_expected=core.personas.get("actor:a")["version"],
                name="合成档案",
                description="",
                content={"persona": "Later", "tone": "Soft"},
            ),
        )
        assert core._pin_role("actor:a")["persona"] == "Later"
        assert prior["persona"] == "Role A"
        assert core.personas.verify(prior) == prior["revision_id"]
        await core.close()

    asyncio.run(scenario())


def test_core_without_personas_keeps_legacy_model_request():
    async def scenario():
        harness = Harness()
        try:
            await harness.ingest(text="Legacy role conversation")
            harness.clock.advance(15)
            await harness.cycles(80)
            assert len(harness.gateway.calls) == 1
            prompt = harness.gateway.calls[0][1][0]["content"]
            assert prompt.startswith("Role A\nInput messages")
            assert "content" not in harness.turns()[0]["role"]
            assert harness.turns()[0]["phase"] != "failed"
        finally:
            await harness.core.close()

    asyncio.run(scenario())


def test_core_model_request_uses_all_four_fields_from_each_pinned_revision():
    async def scenario():
        harness = Harness(
            personas=persona_config(
                **{
                    "actor:a": {
                        "version": 1,
                        "persona": "Old persona",
                        "tone": "Old tone",
                        "style": "Old style",
                        "address": "Old address",
                    },
                    "actor:b": {"version": 1, "persona": "Role B"},
                }
            )
        )
        core = harness.core
        gate = asyncio.Event()
        harness.gateway.gates[1] = gate
        await harness.ingest(text="在途旧轮次")
        harness.clock.advance(6)
        await harness.cycles(10)
        assert len(harness.gateway.calls) == 1
        first_turn = harness.turns()[0]
        old_revision = first_turn["role"]["revision_id"]

        created = core.manage_persona(
            "persona_admin",
            operation(
                "create_profile",
                "prompt-create",
                name="Catalog metadata only",
                description="Description stays outside prompt",
                content={
                    "persona": "New persona",
                    "tone": "New tone",
                    "style": "New style",
                    "address": "New address",
                },
            ),
        )["item"]
        applied = core.manage_persona(
            "persona_admin",
            operation(
                "apply_profile",
                "prompt-apply",
                subject=created["id"],
                expected=created["version"],
                target="actor:a",
                target_expected=core.personas.get("actor:a")["version"],
                name="Catalog metadata only",
                description="Description stays outside prompt",
                content={
                    "persona": "New persona",
                    "tone": "New tone",
                    "style": "New style",
                    "address": "New address",
                },
            ),
        )["item"]
        gate.set()
        await harness.cycles(80)
        await harness.ingest(text="新版轮次")
        harness.clock.advance(6)
        await harness.cycles(80)
        prompts = [messages[0]["content"] for _, messages in harness.gateway.calls]
        assert prompts[0].startswith(
            "Old persona\nTone: Old tone\nStyle: Old style\nAddress: Old address\n"
        )
        assert prompts[1].startswith(
            "New persona\nTone: New tone\nStyle: New style\nAddress: New address\n"
        )
        assert core.personas.verify(first_turn["role"]) == old_revision
        assert all("Catalog metadata only" not in prompt for prompt in prompts)
        assert all("Description stays outside prompt" not in prompt for prompt in prompts)

        cleared = core.manage_persona(
            "persona_admin",
            operation(
                "apply_profile",
                "prompt-clear",
                subject=created["id"],
                expected=applied["profile"]["version"],
                target="actor:a",
                target_expected=applied["target"]["version"],
                name="Catalog metadata only",
                description="Description stays outside prompt",
                content={"persona": "Cleared persona", "tone": "", "style": "", "address": ""},
            ),
        )["item"]
        assert cleared["target"]["published_revision"] != applied["target"]["published_revision"]
        await harness.ingest(text="清空后轮次")
        harness.clock.advance(6)
        await harness.cycles(80)
        latest_prompt = harness.gateway.calls[-1][1][0]["content"]
        assert latest_prompt.startswith("Cleared persona\nInput messages")
        assert all(
            value not in latest_prompt
            for value in ("Old tone", "New tone", "New style", "New address")
        )
        assert len(harness.gateway.calls) == 3
        await core.close()

    asyncio.run(scenario())
