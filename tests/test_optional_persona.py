from contextlib import closing
from types import SimpleNamespace

import pytest

from tianshu_companion.contracts import Fault
from tianshu_companion.personas import Personas
from tianshu_companion.role_runtime import RoleRuntime
from tianshu_companion.store import Store
from test_role_runtime import validate_contract


def test_optional_profile_lifecycle(tmp_path):
    path = tmp_path / "optional.sqlite"
    with closing(Store(path)) as store:
        personas = Personas(store, lambda: 1000.0)
        personas.import_config({"config_version": 1, "roles": {
            "actor:household": {"version": 1, "persona": "Private household persona"}}})
        core = SimpleNamespace(store=store, personas=personas,
            roles={"actor:household": {}}, bindings={"web": {
                "service": "platform", "namespace": "web", "actor_ids": ["actor:household"]}},
            deployment_roles=frozenset({"actor:household"}),
            deployment_binding_actors={"web": frozenset({"actor:household"})})
        runtime = RoleRuntime(core)
        request = {"request_id": "blank-create", "application_id": "blank-create",
            "operator": "operator-web", "actor_id": "actor:role-blank", "expected_version": 0,
            "name": "Blank", "profile_id": None, "profile_version": None,
            "enabled": False, "capabilities": ["dialogue"]}
        first = runtime.apply("platform", request)
        validate_contract("core_apply", {"operation": "apply", **request})
        validate_contract("core_result", first)
        assert runtime.apply("platform", request) == first
        assert "actor:role-blank" not in core.roles
        assert core.bindings["web"]["actor_ids"] == ["actor:household"]
        baseline = personas.pin("actor:role-blank")
        assert baseline["persona"] == "Respond to the user's request clearly and accurately."
        assert personas.pin("actor:household")["persona"] == "Private household persona"
        assert len(store.list("persona_publications", "actor:role-blank")) == 1
        for change in ({"name": " "}, {"profile_version": 1}, {"enabled": True}):
            with pytest.raises(Fault):
                runtime.apply("platform", {**request, **change, "request_id": "bad", "actor_id": "actor:new"})
        with pytest.raises(Fault) as conflict:
            runtime.apply("platform", {**request, "name": "Different"})
        assert conflict.value.code == "idempotency_conflict"
        profile = personas.create_profile(name="Later profile", description="", content={
            "persona": "Separate authored persona", "tone": "warm"}, operator="operator-web", reason="test")
        attached = runtime.apply("platform", {**request, "request_id": "attach",
            "application_id": "attach", "expected_version": 1,
            "profile_id": profile["id"], "profile_version": profile["version"]})
        assert personas.pin("actor:role-blank")["persona"] == "Separate authored persona"
        cleared = runtime.apply("platform", {**request, "request_id": "clear",
            "application_id": "clear", "expected_version": 2})
        assert personas.pin("actor:role-blank")["persona"] == baseline["persona"]
        assert set(personas._revision(cleared["persona_revision"])["content"]) == {"persona"}
        assert cleared["profile_id"] is None
        assert attached["persona_revision"] != cleared["persona_revision"]
    with closing(Store(path)) as store:
        core.store = store
        core.personas = Personas(store, lambda: 2000.0)
        restarted = RoleRuntime(core)
        assert restarted.get("actor:role-blank") == cleared
        assert "actor:role-blank" not in core.roles
        assert restarted.apply("platform", request) == first
        assert restarted.get("actor:role-blank") == cleared
