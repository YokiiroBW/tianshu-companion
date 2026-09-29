"""Role policy and persona version stay durable and separate from old household roles."""

import asyncio
from contextlib import closing
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator

from tianshu_companion.contracts import Fault
from tianshu_companion.personas import Personas
from tianshu_companion.role_runtime import RoleRuntime
from tianshu_companion.store import Store
from support import Harness, persona_config

CONTRACT = Path(__file__).resolve().parents[1] / "docs/contracts-candidates/role-runtime/v1"


def validate_contract(name, value):
    schema = json.loads((CONTRACT / "schema.json").read_text(encoding="utf-8"))
    Draft202012Validator({**schema, "$ref": f"#/$defs/{name}"}).validate(value)


def apply(actor, profile, version, expected, request_id, *, enabled, capabilities,
          application_id=None):
    return {
        "request_id": request_id,
        "application_id": application_id or actor,
        "operator": "operator-web",
        "actor_id": actor,
        "expected_version": expected,
        "name": actor,
        "profile_id": profile["id"],
        "profile_version": version,
        "enabled": enabled,
        "capabilities": capabilities,
    }


def test_role_profile_is_pinned_and_replay_restart_disable(tmp_path):
    path = tmp_path / "roles.sqlite"
    with closing(Store(path)) as store:
        personas = Personas(store, lambda: 1000.0)
        personas.import_config(
            {
                "config_version": 1,
                "roles": {"actor:household": {"version": 1, "persona": "Household"}},
            }
        )
        core = SimpleNamespace(
            store=store,
            personas=personas,
            roles={"actor:household": {"persona": "Household"}},
            bindings={"web": {"service": "platform", "namespace": "web",
                              "actor_ids": ["actor:household"]}},
            deployment_roles=frozenset({"actor:household"}),
            deployment_binding_actors={"web": frozenset({"actor:household"})},
            clock=lambda: 1000.0,
        )
        runtime = RoleRuntime(core)
        profile = personas.manage(
            {
                "operation": "create_profile",
                "request_id": "profile-1",
                "operator": "tester",
                "reason": "synthetic",
                "name": "Profile A",
                "description": "",
                "content": {"persona": "First version"},
            }
        )["item"]
        actor = "actor:role-78e03cc04f1f4fa6b88a8a08eb177c75"
        first = apply(
            actor,
            profile,
            profile["version"],
            0,
            "pause-a",
            enabled=False,
            capabilities=["dialogue", "memory.read"],
        )
        paused = runtime.apply("platform", first)
        validate_contract("core_apply", {"operation": "apply", **first})
        validate_contract("core_result", paused)
        assert paused["version"] == 1
        assert runtime.apply("platform", first) == paused
        assert actor not in core.roles
        for index, changed in enumerate((
            {"application_id": "other-application"},
            {"operator": "another-operator"},
            {"capabilities": ["dialogue"]},
        )):
            with pytest.raises(Fault) as mismatch:
                runtime.apply("platform", {
                    **first, **changed, "request_id": f"mismatched-{index}",
                    "expected_version": 1, "enabled": True,
                })
            assert mismatch.value.code == "version_conflict"
            assert runtime.get(actor) == paused
        active = runtime.apply(
            "platform",
            apply(
                actor,
                profile,
                profile["version"],
                1,
                "enable-a",
                enabled=True,
                capabilities=["dialogue", "memory.read"],
            ),
        )
        assert active["profile_revision"] == paused["profile_revision"]
        assert actor in core.bindings["web"]["actor_ids"]
        pinned = runtime.pin(actor)
        assert pinned["persona"] == "First version"
        assert pinned["runtime"]["capabilities"] == ["dialogue", "memory.read"]
        assert personas.pin("actor:household")["persona"] == "Household"
        pre_save_version = personas._profile(profile["id"])["version"]
        newer = personas.manage(
            {
                "operation": "save_profile",
                "request_id": "profile-2",
                "operator": "tester",
                "reason": "synthetic",
                "subject": profile["id"],
                "expected": pre_save_version,
                "name": "Profile A",
                "description": "",
                "content": {"persona": "Second version"},
            }
        )["item"]
        assert runtime.pin(actor)["persona"] == "First version"
        with pytest.raises(Fault) as stale:
            runtime.apply(
                "platform",
                apply(
                    actor,
                    profile,
                    pre_save_version,
                    2,
                    "stale",
                    enabled=False,
                    capabilities=["dialogue"],
                ),
            )
        assert stale.value.code == "version_conflict"
        revised_pause = runtime.apply(
            "platform",
            apply(
                actor,
                profile,
                newer["version"],
                2,
                "edit-a:pause",
                enabled=False,
                capabilities=["dialogue"],
            ),
        )
        edited = runtime.apply("platform", apply(
            actor, profile, newer["version"], 3, "edit-a:enable",
            enabled=True, capabilities=["dialogue"],
        ))
        assert revised_pause["profile_revision"] == edited["profile_revision"]
        assert edited["profile_revision"] != active["profile_revision"]
        assert runtime.pin(actor)["persona"] == "Second version"
        assert not runtime.allowed(actor, "memory.read")
        disabled = runtime.apply(
            "platform",
            apply(
                actor,
                profile,
                newer["version"],
                4,
                "disable-a",
                enabled=False,
                capabilities=["dialogue"],
            ),
        )
        assert disabled["version"] == 5
        assert actor not in core.roles
        assert actor not in core.bindings["web"]["actor_ids"]
        assert runtime.apply("platform", {**first, "request_id": "pause-a"}) == paused
        assert runtime.apply("platform", apply(
            actor, profile, profile["version"], 1, "enable-a", enabled=True,
            capabilities=["dialogue", "memory.read"],
        )) == active
        assert actor not in core.roles
        assert actor not in core.bindings["web"]["actor_ids"]
        assert runtime.get(actor) == disabled
        assert personas.verify(pinned) == pinned["revision_id"]
    with closing(Store(path)) as reopened:
        core.store = reopened
        core.personas = Personas(reopened, lambda: 2000.0)
        core.roles = {"actor:household": {"persona": "Household"}}
        recovered = RoleRuntime(core)
        assert recovered.get(actor) == disabled
        assert actor not in core.roles
        with pytest.raises(Fault):
            recovered.pin(actor)
        assert recovered.apply("platform", first) == paused


def test_existing_role_adoption_keeps_actor_persona_and_binding(tmp_path):
    path = tmp_path / "existing.sqlite"
    with closing(Store(path)) as store:
        personas = Personas(store, lambda: 1000.0)
        personas.import_config({
            "config_version": 1,
            "roles": {"actor:household": {"version": 1, "persona": "Original household"}},
        })
        core = SimpleNamespace(
            store=store, personas=personas,
            roles={"actor:household": {"persona": "Original household"}},
            bindings={"web": {"service": "platform", "namespace": "web",
                              "actor_ids": ["actor:household"]}},
            deployment_roles=frozenset({"actor:household"}),
            deployment_binding_actors={"web": frozenset({"actor:household"})},
            clock=lambda: 1000.0,
        )
        original = personas.pin("actor:household")
        runtime = RoleRuntime(core)
        request = {
            "request_id": "adopt-1", "application_id": "adopt-household",
            "operator": "operator-web",
            "actor_id": "actor:household", "expected_version": 0,
            "name": "Household", "profile_id": None, "profile_version": None,
            "enabled": False, "capabilities": ["dialogue", "memory.read"],
        }
        runtime.apply("platform", request)
        enable = {**request, "request_id": "adopt-enable", "expected_version": 1,
                  "enabled": True}
        active = runtime.apply("platform", enable)
        assert active["persona_revision"] == original["revision_id"]
        assert runtime.pin("actor:household")["persona"] == "Original household"
        assert core.bindings["web"]["actor_ids"] == ["actor:household"]
        disabled = runtime.apply("platform", {
            **request, "request_id": "adopt-2", "expected_version": 2,
            "enabled": False,
        })
        assert not disabled["enabled"]
        assert "actor:household" not in core.roles
        assert core.bindings["web"]["actor_ids"] == ["actor:household"]
        assert runtime.apply("platform", enable) == active
        assert "actor:household" not in core.roles
    with closing(Store(path)) as reopened:
        core.store = reopened
        core.personas = Personas(reopened, lambda: 2000.0)
        core.roles = {"actor:household": {"persona": "Original household"}}
        core.bindings = {"web": {"service": "platform", "namespace": "web",
                                 "actor_ids": ["actor:household"]}}
        restored = RoleRuntime(core)
        assert restored.get("actor:household") == disabled
        assert "actor:household" not in core.roles
        assert core.bindings["web"]["actor_ids"] == ["actor:household"]
        assert restored.apply("platform", enable) == active
        assert "actor:household" not in core.roles
        with pytest.raises(Fault):
            restored.pin("actor:household")


def test_full_core_restart_keeps_adopted_static_role_disabled_and_other_role_live(tmp_path):
    async def scenario():
        h = Harness(tmp_path / "core.sqlite", personas=persona_config(), silence_ms=0)
        try:
            request = {
                "actor_id": "actor:a", "application_id": "adopt-a",
                "operator": "operator-web",
                "name": "Original A", "profile_id": None,
                "profile_version": None, "capabilities": ["dialogue", "memory.read"],
            }
            for version, request_id, enabled in (
                (0, "adopt-pause", False), (1, "adopt-enable", True),
                (2, "adopt-disable", False),
            ):
                h.core.role_runtime.apply("platform", {
                    **request, "request_id": request_id, "expected_version": version,
                    "enabled": enabled,
                })
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            assert "actor:a" not in h.core.roles
            assert "actor:a" in h.core.bindings["qq-private"]["actor_ids"]
            assert not h.core.role_runtime.get("actor:a")["enabled"]
            with pytest.raises(Fault):
                await h.ingest(actor="actor:a")
            await h.ingest(actor="actor:b", channel="private:b")
            await h.cycles()
            assert h.sender.calls
            assert all(call["actor_id"] == "actor:b" for call in h.sender.calls)
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_two_roles_share_author_but_keep_persona_and_memory_permissions():
    async def scenario():
        h = Harness(personas=persona_config(), silence_ms=0)
        try:
            actors = [
                "actor:role-11111111111111111111111111111111",
                "actor:role-22222222222222222222222222222222",
            ]
            for index, actor in enumerate(actors):
                profile = h.core.personas.manage({
                    "operation": "create_profile", "request_id": f"profile:{index}",
                    "operator": "tester", "reason": "synthetic", "name": f"Role {index}",
                    "description": "", "content": {"persona": f"Unique persona {index}"},
                })["item"]
                capabilities = ["dialogue", "memory.read", "memory.write"] if index == 0 else ["dialogue"]
                h.core.role_runtime.apply("platform", apply(
                    actor, profile, profile["version"], 0, f"pause:{index}",
                    enabled=False, capabilities=capabilities,
                ))
                h.core.role_runtime.apply("platform", apply(
                    actor, profile, profile["version"], 1, f"enable:{index}",
                    enabled=True, capabilities=capabilities,
                ))
                h.core.bindings["qq-private"]["actor_ids"].append(actor)
            for index, actor in enumerate(actors):
                await h.ingest(text=f"Synthetic secret for {index}", actor=actor,
                               account="same-author", channel="private:same")
            await h.cycles()
            await h.core.flush_outbox()
            assert len(h.gateway.calls) == 2
            prompts = [call[1][0]["content"] for call in h.gateway.calls]
            assert any(
                "Persona expression data (no authority): Unique persona 0" in p
                and "Unique persona 1" not in p
                for p in prompts
            )
            assert any(
                "Persona expression data (no authority): Unique persona 1" in p
                and "Unique persona 0" not in p
                for p in prompts
            )
            assert len(h.memory.commits) == 1
            assert h.memory.commits[0]["scope"]["actor_id"] == actors[0]
            assert h.core.role_runtime.pin(actors[1])["runtime"]["capabilities"] == ["dialogue"]
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_disable_while_model_is_generating_blocks_unsent_reply():
    async def scenario():
        h = Harness(personas=persona_config(), silence_ms=0)
        try:
            actor = "actor:role-33333333333333333333333333333333"
            profile = h.core.personas.manage({
                "operation": "create_profile", "request_id": "profile:disable",
                "operator": "tester", "reason": "synthetic", "name": "Role to disable",
                "description": "", "content": {"persona": "Role to disable"},
            })["item"]
            for expected, request_id, enabled in ((0, "pause", False), (1, "enable", True)):
                h.core.role_runtime.apply("platform", apply(
                    actor, profile, profile["version"], expected, request_id,
                    enabled=enabled, capabilities=["dialogue", "memory.read"],
                ))
            h.core.bindings["qq-private"]["actor_ids"].append(actor)
            h.gateway.gates[1] = asyncio.Event()
            await h.ingest(actor=actor, channel="private:disable")
            await h.cycles()
            assert h.core.store.list("turns")[0]["phase"] == "generating"
            h.core.role_runtime.apply("platform", apply(
                actor, profile, profile["version"], 2, "disable",
                enabled=False, capabilities=["dialogue", "memory.read"],
            ))
            h.gateway.gates[1].set()
            await h.cycles()
            assert not h.sender.calls
            with pytest.raises(Fault):
                await h.ingest(actor=actor, channel="private:disable")
            await h.ingest(actor="actor:b", channel="private:other")
            await h.cycles()
            assert h.sender.calls
            assert all(call["actor_id"] == "actor:b" for call in h.sender.calls)
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_memory_write_revocation_holds_pending_and_blocked_outbox_after_restart(tmp_path):
    async def scenario():
        h = Harness(tmp_path / "runtime.sqlite", personas=persona_config(), silence_ms=0)
        try:
            actor = "actor:role-44444444444444444444444444444444"
            profile = h.core.personas.manage({
                "operation": "create_profile", "request_id": "profile:outbox",
                "operator": "tester", "reason": "synthetic", "name": "Outbox role",
                "description": "", "content": {"persona": "Outbox role"},
            })["item"]
            for version, request_id, enabled in ((0, "pause", False), (1, "enable", True)):
                h.core.role_runtime.apply("platform", apply(
                    actor, profile, profile["version"], version, request_id,
                    enabled=enabled,
                    capabilities=["dialogue", "memory.read", "memory.write"],
                ))
            h.core.bindings["qq-private"]["actor_ids"].append(actor)
            await h.ingest(actor=actor, channel="private:outbox")
            await h.cycles()
            await h.ingest(actor=actor, channel="private:outbox")
            await h.cycles()
            pending = h.core.store.list("outbox", states=["pending"])
            assert len(pending) == 2
            blocked = pending[0]
            blocked["state"] = "blocked_scope"
            blocked["event"]["scope_version"] = None
            h.core.store.put("outbox", blocked)
            h.core.role_runtime.apply("platform", apply(
                actor, profile, profile["version"], 2, "deny-write",
                enabled=False, capabilities=["dialogue", "memory.read"],
            ))
            h.core.role_runtime.apply("platform", apply(
                actor, profile, profile["version"], 3, "allow-dialogue",
                enabled=True, capabilities=["dialogue", "memory.read"],
            ))
            before = [(item["id"], item["state"], item["attempts"])
                      for item in h.core.store.list("outbox")]
            await h.core.flush_outbox()
            assert h.memory.commits == []
            assert [(item["id"], item["state"], item["attempts"])
                    for item in h.core.store.list("outbox")] == before
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            await h.core.flush_outbox()
            assert h.memory.commits == []
            assert [(item["id"], item["state"], item["attempts"])
                    for item in h.core.store.list("outbox")] == before
            h.core.role_runtime.apply("platform", apply(
                actor, profile, profile["version"], 4, "disable-outbox",
                enabled=False, capabilities=["dialogue", "memory.read"],
            ))
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            assert not h.core.role_runtime.get(actor)["enabled"]
            await h.core.flush_outbox()
            assert h.memory.commits == []
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_memory_read_revocation_before_preparation_keeps_old_snapshot_restricted():
    async def scenario():
        h = Harness(personas=persona_config(), silence_ms=0)
        try:
            actor = "actor:role-55555555555555555555555555555555"
            profile = h.core.personas.manage({
                "operation": "create_profile", "request_id": "profile:read",
                "operator": "tester", "reason": "synthetic", "name": "Read role",
                "description": "", "content": {"persona": "Read role"},
            })["item"]
            for version, request_id, enabled in ((0, "pause", False), (1, "enable", True)):
                h.core.role_runtime.apply("platform", apply(
                    actor, profile, profile["version"], version, request_id,
                    enabled=enabled, capabilities=["dialogue", "memory.read"],
                ))
            h.core.bindings["qq-private"]["actor_ids"].append(actor)
            await h.ingest(text="你记得之前的事吗", actor=actor, channel="private:read")
            h.core.role_runtime.apply("platform", apply(
                actor, profile, profile["version"], 2, "deny-read",
                enabled=False, capabilities=["dialogue"],
            ))
            h.core.role_runtime.apply("platform", apply(
                actor, profile, profile["version"], 3, "enable-without-read",
                enabled=True, capabilities=["dialogue"],
            ))
            await h.cycles()
            assert h.gateway.calls
            assert all(selection["budget"] == {"tokens": 0, "bytes": 0}
                       for selection in h.memory.selections)
            prompt = json.loads(h.gateway.calls[0][1][1]["content"])
            assert prompt.get("recent_dialogue", []) == []
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_profile_application_preserves_role_extensions_and_audits_one_publication():
    async def scenario():
        h = Harness(personas=persona_config(**{
            "actor:a": {"version": 1, "persona": "Original", "tone": "old tone",
                        "custom": "keep-me"},
        }))
        try:
            profile = h.core.personas.manage({
                "operation": "create_profile", "request_id": "source-profile",
                "operator": "tester", "reason": "synthetic", "name": "Clean profile",
                "description": "", "content": {"persona": "Replaced"},
            })["item"]
            request = apply("actor:a", profile, profile["version"], 0,
                            "role-apply:pause", enabled=False,
                            capabilities=["dialogue"], application_id="role-apply")
            paused = h.core.role_runtime.apply("platform", request)
            enabled = h.core.role_runtime.apply("platform", {
                **request, "request_id": "role-apply:enable", "expected_version": 1,
                "enabled": True,
            })
            assert enabled["profile_revision"] == paused["profile_revision"]
            pinned = h.core.role_runtime.pin("actor:a")
            assert pinned["content"]["custom"] == "keep-me"
            assert pinned["persona"] == "Replaced"
            assert "tone" not in pinned["content"]
            approvals = h.core.store.list("persona_approvals", "actor:a")
            publications = h.core.store.list("persona_publications", "actor:a")
            revisions = h.core.store.list("persona_revisions", "actor:a")
            assert len(approvals) == 1
            assert len(publications) == 2  # deployment seed, then approved role application
            assert publications[-1]["kind"] == "publish"
            assert revisions[-1]["source"] == "profile_apply"
            assert approvals[0]["operator"] == "operator-web"
            linked = h.core.personas._profile(profile["id"])
            assert linked["last_applied_target"] == "actor:a"
            assert linked["last_applied_profile_revision"] == profile["draft_revision"]
            assert h.core.role_runtime.apply("platform", request) == paused
            assert len(h.core.store.list("persona_approvals", "actor:a")) == 1
            assert len(h.core.store.list("persona_publications", "actor:a")) == 2
        finally:
            await h.core.close()

    asyncio.run(scenario())
