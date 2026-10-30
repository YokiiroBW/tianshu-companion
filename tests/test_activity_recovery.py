"""PC-04: recovery deadlines, explicit dormancy, and retained uncertain operations."""

import asyncio
import copy
import os

import httpx
import pytest

from support import Harness
from test_life_background import ActivityModel, activity_core
from test_runtime_v2 import activity, platform_query
from tianshu_companion.app import create_app
from tianshu_companion.contracts import Fault, digest


def test_third_failure_retains_a_future_recovery_point_without_claiming_progress():
    async def run():
        h = Harness()
        try:
            model = ActivityModel()
            saved = activity_core(h, model)
            owner = h.core.life.activities
            for _ in range(3):
                owner._interrupt(saved, "dependency_unavailable")
                saved = owner.get("actor:a", saved["id"])
            assert saved["state"] == "paused"
            assert h.clock() < saved["next_due_at"] <= h.clock() + 21600
            assert saved["checkpoint"]["step"] == 2 and saved["result_refs"] == []
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.skipif(
    not os.environ.get("TIANSHU_PLATFORM_REPO"), reason="Platform joint checkout not selected"
)
def test_dormant_http_projection_is_accepted_by_the_platform_consumer(monkeypatch):
    monkeypatch.syspath_prepend(os.environ["TIANSHU_PLATFORM_REPO"])

    async def run():
        from services.platform.contracts import Contracts as PlatformContracts

        h = Harness()
        try:
            saved = activity_core(h, ActivityModel())
            dormant = h.core.life.activities.transition(
                "actor:a", saved["id"], "dormant", expected=saved["version"], reason="等待显式选择"
            )
            h.core.life.set_diary_access("actor:a", readers=["reader:operator"])
            app = create_app(
                h.core,
                {"platform": "test-token"},
                life_readers={"platform": dict(reader_id="reader:operator", actor_ids=["actor:a"])},
            )
            request = dict(
                schema_version=2,
                query=platform_query(h),
                actor_id="actor:a",
                resource="activities",
                object_id=dormant["id"],
                expected_version=None,
                limit=20,
                after=None,
                scope=None,
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://fixture.invalid"
            ) as client:
                response = await client.post(
                    "/internal/v2/life/read",
                    json=request,
                    headers={"Authorization": "Bearer test-token"},
                )
                assert response.status_code == 200
                result = response.json()
            consumer = PlatformContracts(os.environ["TIANSHU_CONTRACTS"])
            consumer.check("life-runtime#read_response", result)
            assert result["items"][0]["dormant_since"] == h.clock()
            assert result["items"][0]["state"] == "dormant"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_dormant_releases_the_actor_and_is_visible_to_goal_selection_but_never_auto_restarts():
    async def run():
        h = Harness()
        try:
            model = ActivityModel()
            saved = activity_core(h, model)
            owner = h.core.life.activities
            dormant = owner.transition(
                "actor:a",
                saved["id"],
                "dormant",
                expected=saved["version"],
                reason="需要新的探索决定",
            )
            assert dormant["dormant_since"] == h.clock()
            assert dormant["result_refs"] == saved["result_refs"]
            assert owner.current("actor:a") is None
            await owner.work(actor_id="actor:a")
            assert not model.calls
            actor = h.core.store.get("life_actors", "actor:a")
            captured = h.core.life.autonomy.snapshot(actor, {"available_until": h.clock() + 3600})
            assert captured["dormant"][0]["resume_condition"] == "需要新的探索决定"
            value = activity(saved["id"])
            with pytest.raises(Fault):
                owner.save("actor:a", value, expected=dormant["version"])
            resumed = owner.transition(
                "actor:a", dormant["id"], "running", expected=dormant["version"]
            )
            assert resumed["state"] == "running" and "dormant_since" not in resumed
        finally:
            await h.core.close()

    asyncio.run(run())


def test_unknown_operation_stays_guarded_after_dormancy_and_direct_save_cannot_bypass_states():
    async def run():
        h = Harness()
        try:
            saved = activity_core(h, ActivityModel())
            owner = h.core.life.activities
            operation = "activity-step:" + digest([saved["id"], saved["version"]])
            h.core.store.put("metadata", dict(id=operation, state="unknown"))
            owner._interrupt(saved, "result_unknown")
            dormant = owner.get("actor:a", saved["id"])
            assert dormant["state"] == "dormant" and dormant["step_operation"] == operation
            captured = owner.dormant("actor:a")[0]
            assert captured["step_operation"] == operation and captured["step_state"] == "unknown"
            assert owner.current("actor:a") is None
            with pytest.raises(Fault):
                owner.save("actor:a", activity("activity:other"), expected=0)
            assert h.core.store.get("metadata", operation)["state"] == "unknown"
            clean = copy.deepcopy(dormant)
            clean.update(state="completed")
            with pytest.raises(Fault):
                owner.save("actor:a", clean, expected=dormant["version"])
            # Explicit abandonment seals this activity against replay. It does not
            # claim an external outcome or prevent unrelated future work forever.
            owner.transition("actor:a", dormant["id"], "cancelled", expected=dormant["version"])
            other = owner.save("actor:a", activity("activity:other"), expected=0)
            assert other["state"] == "running"
            assert h.core.store.get("metadata", operation)["state"] == "unknown"
        finally:
            await h.core.close()

    asyncio.run(run())
