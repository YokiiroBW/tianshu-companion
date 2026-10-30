"""The one-shot migration preserves uncertain identities, progress and history."""

import importlib.util
from pathlib import Path

from test_runtime_v2 import activity


def test_convergence_is_idempotent_retains_history_and_removes_only_the_explicit_placeholder():
    path = Path(__file__).resolve().parents[1] / "scripts/converge_life_activities.py"
    spec = importlib.util.spec_from_file_location("pc05_convergence", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    item = dict(
        activity(),
        actor_id="actor:a",
        state="paused",
        next_due_at=None,
        runtime_epoch=1,
        version=9,
        step_operation="activity-step:original",
    )
    actor = dict(id="actor:a", activity_id=item["id"], life_runtime_epoch=1, version=2)
    placeholder = dict(id="actor:household", activity_id=None, version=3)
    actors = {row["id"]: row for row in (actor, placeholder)}
    changes = module.convergence([item], actors, 1900000000, remove_placeholder=True)
    changed = next(row["after"] for row in changes if row["table"] == "life_activities")
    assert changed["state"] == "dormant" and changed["step_operation"] == item["step_operation"]
    assert (
        changed["checkpoint"] == item["checkpoint"]
        and changed["result_refs"] == item["result_refs"]
    )
    assert len([row for row in changes if row["after"] is None]) == 1
    actor = next(row["after"] for row in changes if row["id"] == actor["id"])
    assert (
        module.convergence([changed], {actor["id"]: actor}, 1900000001, remove_placeholder=True)
        == []
    )
