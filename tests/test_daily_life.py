"""Independent role life, durable generation and read-only authorized projections.

All clocks, actors, data and model replies are isolated synthetic fixtures.
"""

import asyncio
import copy
import json
import sqlite3
from datetime import datetime, timezone
from unittest.mock import patch

import httpx
import pytest

from support import Clock, Harness, contracts, persona_config
from tianshu_companion.app import create_app
from tianshu_companion.bot_bindings import BotBindings
from tianshu_companion.clients import BotSenderRouter
from tianshu_companion.clients import Gateway
from tianshu_companion.contracts import Fault
from tianshu_companion.life import Life
from tianshu_companion.life_daily import TASK_SCOPE
from tianshu_companion.life_read import LifeRead, readers
from tianshu_companion.life_read_index import TIMELINE_INDEX
from tianshu_companion.life_read_queries import LifeReadQueries
from tianshu_companion.model_selection import ModelSelection
from tianshu_companion.store import TABLES, Store


class Model:
    available = True

    def __init__(self):
        self.calls = []
        self.plan_failure = False
        self.stage_failure = False
        self.gate = None
        self.timeout_stage = False
        self.influence_gate = None

    async def generate(self, turn, messages):
        material = json.loads(messages[1]["content"])
        kind = (
            "influence"
            if "Extract role-life intentions" in messages[0]["content"]
            else "plan"
            if "today's fictional intentions" in messages[0]["content"]
            else "stage"
        )
        self.calls.append((kind, copy.deepcopy(turn), material))
        if kind == "influence" and self.influence_gate:
            await self.influence_gate.wait()
        if kind == "stage" and self.timeout_stage:
            raise asyncio.TimeoutError("Synthetic timeout")
        if self.gate and kind == "stage":
            await self.gate.wait()
        if (kind == "plan" and self.plan_failure) or (kind == "stage" and self.stage_failure):
            raise RuntimeError("Synthetic model failure")
        if kind == "influence":
            output = json.dumps(
                {
                    "intentions": ["研究天体物理纪录片中的恒星演化"]
                    if "天体物理" in material["dialogue"]
                    else []
                }
            )
        elif kind == "plan":
            expression = material.get("persona_expression") or ""
            output = json.dumps(
                {
                    "entries": [
                        {
                            "minute": e["minute"],
                            "activity": (
                                "painting a fictional landscape"
                                if expression == "Role B"
                                else "studying fictional constellations"
                                if expression == "Role A"
                                else e["activity"]
                            )
                            if e["activity"] not in {"sleeping", "睡眠"}
                            else e["activity"],
                            "detail": "A fictional intention for " + e["activity"],
                        }
                        for e in material["schedule"]
                    ]
                }
            )
        elif "stage" in material:
            output = (
                "I explored "
                + (", ".join(material["interests"]) or "a quiet fictional moment")
                + "."
            )
        else:
            output = "A fictional diary from the supplied experiences."
        return [output], {"fixture": True, "config_version": turn["config_version"]}


def make_life(path=":memory:", *, writing=True, count=1):
    clock = Clock()
    clock.now = datetime(2026, 10, 3, 9, 0, tzinfo=timezone.utc).timestamp()
    model = Model()
    life = Life(
        Store(path), clock, model, 19, asyncio.Semaphore(4), writing=writing, timezone_name="UTC"
    )
    for index in range(count):
        life.synchronize_role(f"actor:{index}", enabled=True, personality_version=1)
    life.recover()
    return life, clock, model


def plan(life, actor="actor:0"):
    return life.store.get("metadata", life.store.get("life_actors", actor)["daily_plan_id"])


def generation_tasks(life, kind):
    return life.store.list("metadata", TASK_SCOPE + ":" + kind)


def test_new_managed_role_is_readable_without_a_static_actor_entry():
    async def scenario():
        h = Harness(personas=persona_config())
        try:
            h.core.recover()
            app = create_app(
                h.core,
                {"platform": "fixture-management", "platform_life": "fixture-life"},
                {
                    "platform_life": {
                        "reader_id": "reader:platform-life",
                        "actor_ids": [],
                        "runtime_roles": True,
                    }
                },
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://core"
            ) as client:
                role = {
                    "actor_id": "actor:new-life",
                    "application_id": "role:new-life",
                    "operator": "operator:test",
                    "name": "新生活角色",
                    "profile_id": None,
                    "profile_version": None,
                    "capabilities": [],
                }
                for version, enabled in ((0, False), (1, True)):
                    response = await client.post(
                        "/internal/v1/role-runtime/manage",
                        headers={"Authorization": "Bearer fixture-management"},
                        json={
                            "operation": "apply",
                            **role,
                            "request_id": f"new-life:{version}",
                            "expected_version": version,
                            "enabled": enabled,
                        },
                    )
                    assert response.status_code == 200, response.text
                await h.core.life.work()
                headers = {"Authorization": "Bearer fixture-life"}
                before = facts(h.core.store)
                listed = await client.post(
                    "/internal/v1/life-read/actors", headers=headers, json={"schema_version": 1}
                )
                assert [row["actor_id"] for row in listed.json()["items"]] == [role["actor_id"]]
                today = await client.post(
                    "/internal/v1/life-read/today",
                    headers=headers,
                    json={"schema_version": 1, "actor_id": role["actor_id"]},
                )
                assert today.status_code == 200 and today.json()["enabled"]
                assert before == facts(h.core.store)
                access = h.core.store.get("life_access", role["actor_id"])
                h.core.life.set_diary_access(
                    role["actor_id"], readers=[], expected=access["version"]
                )
                # Exact role replay and the read path never re-grant an explicit revocation.
                h.core.role_runtime._sync_live(h.core.role_runtime.get(role["actor_id"]))
                denied = await client.post(
                    "/internal/v1/life-read/today",
                    headers=headers,
                    json={"schema_version": 1, "actor_id": role["actor_id"]},
                )
                assert denied.status_code == 404
                missing = await client.post(
                    "/internal/v1/life-read/today",
                    headers=headers,
                    json={"schema_version": 1, "actor_id": "actor:a"},
                )
                assert missing.status_code == 404  # Static roles remain explicitly scoped.
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_autonomous_generation_uses_each_actor_selected_model_and_explicit_false():
    async def scenario():
        h = Harness()
        requests = []

        class Selector:
            async def select(self, request):
                assert not h.core.store.db.in_transaction
                requests.append(request)
                return ModelSelection(31 if request.actor_id == "actor:a" else 47, h.clock() + 3600)

        try:
            await h.core.close()
            h.gateway = Model()
            h.core = h.new_core(default_model_selector=Selector())
            h.core.recover()
            assert not h.core.life.writing_available()  # Legacy diary/long-form switch unchanged.
            for _ in range(4):
                await h.core.life.work()
            assert {request.actor_id for request in requests} == {"actor:a", "actor:b"}
            for request in requests:
                assert request.function_id == "writing"
                assert request.audience == "self_private" and request.workload == "companion.text"
                assert request.conversation_id.startswith("life:")
                assert request.person_id.startswith("person:life:")
            for _, turn, material in h.gateway.calls:
                actor = material.get("actor_id")
                if actor is None:
                    actor = next(
                        t["actor_id"]
                        for kind in ("plan", "stage")
                        for t in generation_tasks(h.core.life, kind)
                        if turn["id"].startswith(t["id"] + ":")
                    )
                assert turn["config_version"] == (31 if actor == "actor:a" else 47)
            await h.core.close()
            h.core = h.new_core(default_model_selector=Selector(), life_writing=False)
            h.core.recover()
            before = len(h.gateway.calls), len(requests)
            await h.core.life.work()
            assert before == (len(h.gateway.calls), len(requests))
            assert plan(h.core.life, "actor:a")["generation_state"] == "unavailable"
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_expired_selection_lease_cannot_commit_late_generated_experience():
    async def scenario():
        life, clock, model = make_life()

        class Selector:
            async def select(self, request):
                return ModelSelection(29, clock() + 70)

        original = model.generate

        async def expire(turn, messages):
            result = await original(turn, messages)
            if "current stage" in messages[0]["content"]:
                clock.advance(71)
            return result

        try:
            life.model_selector = Selector()
            model.generate = expire
            await life.work()
            stage = generation_tasks(life, "stage")[0]
            assert stage["state"] == "unavailable"
            assert not life.store.get("life_actors", "actor:0").get("experience")
            assert not any(e["kind"] == "stage_experience" for e in life.store.list("life_events"))
        finally:
            life.store.close()

    asyncio.run(scenario())


def test_source_withdrawn_during_day_plan_cannot_return_via_planned_detail():
    async def scenario():
        h = Harness()
        gate = asyncio.Event()
        started = asyncio.Event()
        try:
            h.core.recover()
            life, model = h.core.life, Model()
            life.gateway, life.config_version = model, 19
            await h.core.ingest(
                "nonebot", h.request(text="今天研究天体物理纪录片", message="plan:source")
            )
            await life.influences.work()
            life.tick(force=True)
            original = model.generate

            async def blocked(turn, messages):
                material = json.loads(messages[1]["content"])
                if "today's fictional intentions" in messages[0]["content"] and any(
                    "天体物理" in value for value in material["interests"]
                ):
                    started.set()
                    await gate.wait()
                    return [
                        json.dumps(
                            {
                                "entries": [
                                    {
                                        "minute": e["minute"],
                                        "activity": "研究天体物理纪录片",
                                        "detail": "继续研究天体物理纪录片",
                                    }
                                    for e in material["schedule"]
                                ]
                            }
                        )
                    ], {"fixture": True}
                return await original(turn, messages)

            model.generate = blocked

            async def work_until_plan():
                for _ in range(8):
                    await life.work()
                    if started.is_set():
                        break

            worker = asyncio.create_task(work_until_plan())
            async with asyncio.timeout(2):
                await started.wait()
            await h.core.ingest(
                "nonebot", h.request(message="plan:source", revision=2, kind="retract")
            )
            gate.set()
            await worker
            assert all(
                "天体物理" not in (e["detail"] or "") for e in plan(life, "actor:a")["entries"]
            )
            old = [
                t
                for t in generation_tasks(life, "plan")
                if any("天体物理" in v for v in t["captured"]["interests"])
            ]
            assert old and old[0]["state"] == "superseded"
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_selected_attempt_identity_matches_real_gateway_header_and_retry():
    async def scenario():
        from test_life_read import receipt

        life, clock, model = make_life()
        grants = {}
        headers_seen = []

        class Selector:
            async def select(self, request):
                grants[request.turn_id] = 43
                return ModelSelection(43, clock() + 3600)

        class Transport:
            url, token = "https://synthetic.invalid", "synthetic-local-only"

            async def call(self, path, body=None, headers=None):
                if path == "/v1/chat/completions":
                    version = grants[headers["X-Tianshu-Turn-ID"]]
                    assert version == int(headers["X-Tianshu-Config-Version"])
                    headers_seen.append(dict(headers))
                    output, _ = await model.generate(
                        {"id": headers["X-Tianshu-Turn-ID"], "config_version": version},
                        body["messages"],
                    )
                    return {
                        "id": "synthetic",
                        "object": "chat.completion",
                        "created": 1,
                        "model": "synthetic",
                        "choices": [
                            {
                                "index": 0,
                                "finish_reason": "stop",
                                "message": {"role": "assistant", "content": "\n".join(output)},
                            }
                        ],
                    }
                previous = headers_seen[-1]
                return receipt(43, clock(), previous["X-Request-ID"])

        try:
            life.model_selector = Selector()
            life.gateway = Gateway(contracts(), Transport())
            model.timeout_stage = True
            await life.work()
            stage = generation_tasks(life, "stage")[0]
            assert stage["state"] == "interrupted"
            first = stage["generation_turn_id"]
            model.timeout_stage = False
            life.retry_generation(stage["id"], expected=stage["version"])
            await life.work()
            current = life.store.get("metadata", stage["id"])
            assert current["state"] == "completed"
            assert current["generation_turn_id"] != first
            assert current["generation_turn_id"] in grants
            assert {h["X-Tianshu-Turn-ID"] for h in headers_seen} == set(grants)
        finally:
            life.store.close()

    asyncio.run(scenario())


def test_disable_and_enable_same_persona_cannot_revive_inflight_stage():
    async def scenario():
        life, clock, model = make_life()
        try:
            await life.work()
            before = len(life.store.list("life_events"))
            clock.advance(3 * 3600)
            model.gate = asyncio.Event()
            pending = asyncio.create_task(life.work())
            for _ in range(20):
                await asyncio.sleep(0)
                if any(t["state"] == "generating" for t in generation_tasks(life, "stage")):
                    break
            task = next(t for t in generation_tasks(life, "stage") if t["state"] == "generating")
            life.synchronize_role("actor:0", enabled=False, personality_version=1)
            life.synchronize_role("actor:0", enabled=True, personality_version=1)
            life.tick(force=True)
            model.gate.set()
            await pending
            assert life.store.get("metadata", task["id"])["state"] == "superseded"
            assert not life.store.get("life_actors", "actor:0").get("experience")
            assert len(life.store.list("life_events")) == before + 1  # Clock transition only.
        finally:
            life.store.close()

    asyncio.run(scenario())


def test_intention_lease_is_checked_after_awaited_source_authorization():
    async def scenario():
        h = Harness()
        checks = 0

        class Selector:
            async def select(self, request):
                return ModelSelection(43, h.clock() + 70)

        async def guard(source):
            nonlocal checks
            checks += 1
            if checks == 2:
                h.clock.advance(71)
            return True

        try:
            h.core.recover()
            life = h.core.life
            life.gateway, life.model_selector, life.dialogue_guard = Model(), Selector(), guard
            await h.core.ingest(
                "nonebot", h.request(text="研究天体物理纪录片", message="lease:intentions")
            )
            await life.influences.work()
            task = life.store.list("metadata", "life:influence-generation")[0]
            assert task["state"] == "unavailable"
            assert life.store.get("life_actors", "actor:a")["life_interests"] == []
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_generation_retry_http_is_management_only_and_versioned():
    async def scenario():
        h = Harness()
        try:
            h.core.recover()
            life, model = h.core.life, Model()
            life.gateway, life.config_version = model, 19
            model.timeout_stage = True
            await life.work()
            current = plan(life, "actor:a")
            app = create_app(h.core, {"platform": "synthetic-admin", "reader": "synthetic-read"})
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://core"
            ) as client:
                request = {
                    "actor_id": "actor:a",
                    "plan_id": current["id"],
                    "phase_id": current["current_phase_id"],
                    "expected_version": current["version"],
                }

                async def send(token, document):
                    return await client.post(
                        "/internal/v1/life-generation/retry",
                        json=document,
                        headers={"Authorization": "Bearer " + token},
                    )

                assert (await send("synthetic-read", request)).status_code == 403
                stale = await send(
                    "synthetic-admin", {**request, "expected_version": current["version"] + 1}
                )
                assert stale.status_code == 409
                result = await send("synthetic-admin", request)
                assert result.status_code == 200 and result.json()["state"] == "queued"
                assert result.json()["plan_version"] > request["expected_version"]
                assert (await send("synthetic-admin", request)).status_code == 409
                model.timeout_stage = False
                for _ in range(4):
                    await life.work()
                assert life.store.get("life_actors", "actor:a").get("experience")
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_life_is_registered_on_runtime_start_without_a_message_or_model():
    async def scenario():
        h = Harness()
        try:
            h.clock.now = datetime(2026, 10, 3, 1, 0, tzinfo=timezone.utc).timestamp()
            h.core.recover()
            assert h.core.store.list("inbox") == []
            assert h.core.life.summary("actor:a")["activity"]
            initial = h.core.life.summary("actor:a")
            h.clock.advance(4 * 3600)
            await h.core.tick()
            h.clock.advance(4 * 3600)
            await h.core.tick()
            latest = h.core.life.summary("actor:a")
            assert latest["changed_at"] > initial["changed_at"]
            assert len(h.core.store.list("life_events")) >= 6
            await h.core.life.work()
            assert h.gateway.calls == []
            for actor in h.core.store.list("life_actors"):
                row = plan(h.core.life, actor["id"])
                assert row["generated_by"] == "baseline"
                assert row["generation_state"] == "unavailable"
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_register_does_not_replace_an_existing_manual_routine_or_hold():
    life, _, _ = make_life()
    try:
        actor = life.store.get("life_actors", "actor:0")
        life.configure_actor(
            "actor:0",
            actor["room_id"],
            schedule=[{"minute": 0, "activity": "painting"}],
            personality_version=1,
            expected=actor["version"],
        )
        actor = life.snapshot("actor:0")["actor"]
        life.set_activity("actor:0", "drawing", expected=actor["version"])
        before = life.store.get("life_actors", "actor:0")
        life.synchronize_role("actor:0", enabled=True, personality_version=1)
        life.recover()
        after = life.store.get("life_actors", "actor:0")
        assert after["schedule"] == before["schedule"]
        assert after["manual"] == before["manual"] and after["activity"] == "drawing"
    finally:
        life.store.close()


def test_daily_plan_and_due_experience_generate_without_chat_and_enter_diary_material():
    async def scenario():
        life, clock, model = make_life()
        try:
            head = life.store.source_head()
            await life.work()
            assert [c[0] for c in model.calls] == ["plan", "stage"]
            today = plan(life)
            assert today["generation_state"] == "completed" and today["generated_by"] == "gateway"
            assert any(e["detail"] for e in today["entries"] if e["state"] == "planned")
            material = life.materials("actor:0", "2026-10-03")
            assert any(m["summary"] == "I explored a quiet fictional moment." for m in material)
            old_events = life.store.list("life_events")
            await life.work()
            life.tick(force=True)
            assert life.store.list("life_events") == old_events
            clock.advance(3 * 3600)
            life.tick()
            await life.work()
            assert (
                len([e for e in life.store.list("life_events") if e["kind"] == "stage_experience"])
                == 2
            )
            # Previous actual experience is in the next stage's immutable capture.
            stages = [c for c in model.calls if c[0] == "stage"]
            assert any(
                e["summary"] == "I explored a quiet fictional moment."
                for e in stages[-1][2]["recent_experiences"]
            )
            assert life.store.source_head() == head
            clock.advance(24 * 3600)
            life.tick()
            await life.work()
            assert plan(life)["day"] == "2026-10-04"
            assert life.store.get("metadata", today["id"])["state"] == "completed"
            assert len([c for c in model.calls if c[0] == "plan"]) == 2
            assert any(c[0] == "stage" for c in model.calls)
        finally:
            life.store.close()

    asyncio.run(scenario())


def test_plan_failure_does_not_block_stage_and_known_failures_retry_boundedly():
    async def scenario():
        life, clock, model = make_life()
        model.plan_failure = True
        try:
            await life.work()
            assert plan(life)["generation_state"] == "failed"
            assert any(e["kind"] == "stage_experience" for e in life.store.list("life_events"))
            for _ in range(5):
                clock.advance(301)
                await life.work()
            tasks = generation_tasks(life, "plan")
            assert tasks[0]["attempt"] == 3
            model.plan_failure = False
            life.retry_generation(tasks[0]["id"], expected=tasks[0]["version"])
            await life.work()
            assert plan(life)["generation_state"] == "completed"
        finally:
            life.store.close()

    asyncio.run(scenario())


def test_late_stage_result_and_changed_schedule_do_not_write_old_experience():
    async def scenario():
        life, clock, model = make_life()
        model.gate = asyncio.Event()
        try:
            work = asyncio.create_task(life.work())
            for _ in range(20):
                await asyncio.sleep(0)
                if any(c[0] == "stage" for c in model.calls):
                    break
            clock.advance(3 * 3600)
            life.tick()
            model.gate.set()
            await work
            assert not [
                e for e in life.store.list("life_events") if e["kind"] == "stage_experience"
            ]
            old = generation_tasks(life, "stage")[0]
            assert old["state"] == "superseded" and old["outcome"] == "late_result"
            await life.work()
            assert (
                len([e for e in life.store.list("life_events") if e["kind"] == "stage_experience"])
                == 1
            )
            previous_plan = plan(life)
            actor = life.store.get("life_actors", "actor:0")
            life.configure_actor(
                "actor:0",
                actor["room_id"],
                schedule=[{"minute": 0, "activity": "drawing"}],
                personality_version=1,
                expected=actor["version"],
            )
            life.tick()
            assert plan(life)["id"] != previous_plan["id"]
            assert life.store.get("metadata", previous_plan["id"])["state"] == "superseded"
        finally:
            life.store.close()

    asyncio.run(scenario())


def test_restart_marks_unknown_generation_and_skips_gap_without_fabricating(tmp_path):
    life, clock, model = make_life(tmp_path / "life.sqlite")
    asyncio.run(life.work())
    before = len([e for e in life.store.list("life_events") if e["kind"] == "stage_experience"])
    current = generation_tasks(life, "stage")[0]
    current["state"] = "generating"
    life.store.put("metadata", current)
    life.store.close()
    clock.advance(7 * 86400 + 5 * 3600)
    restarted = Life(
        Store(tmp_path / "life.sqlite"), clock, model, 19, asyncio.Semaphore(4), writing=True
    )
    try:
        restarted.recover()
        assert restarted.store.get("metadata", current["id"])["state"] == "interrupted"
        assert (
            len([e for e in restarted.store.list("life_events") if e["kind"] == "stage_experience"])
            == before
        )
        today = plan(restarted)
        assert today["day"] == "2026-10-10"
        assert any(e["state"] == "skipped" for e in today["entries"])
        assert today["reconciled_after_gap_at"] == clock()
        existing = restarted.store.list("life_events")
        restarted.tick(force=True)
        assert restarted.store.list("life_events") == existing
    finally:
        restarted.store.close()


def test_cancellation_is_visible_and_never_automatically_repeats_unknown_call():
    async def scenario():
        life, _, model = make_life()
        model.gate = asyncio.Event()
        try:
            worker = asyncio.create_task(life.work())
            for _ in range(20):
                await asyncio.sleep(0)
                if any(c[0] == "stage" for c in model.calls):
                    break
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker
            task = generation_tasks(life, "stage")[0]
            assert task["state"] == "interrupted"
            calls = len(model.calls)
            await life.work()
            assert len(model.calls) == calls
            model.gate.set()
            life.retry_generation(task["id"], expected=task["version"])
            await life.work()
            assert generation_tasks(life, "stage")[0]["state"] == "completed"
        finally:
            life.store.close()

    asyncio.run(scenario())


def test_dialogue_influence_is_deduplicated_impersonal_and_actor_isolated():
    async def scenario():
        h = Harness(silence_ms=0)
        try:
            h.core.recover()
            model = Model()
            life = h.core.life
            life.gateway, life.writing, life.config_version = model, True, 19
            await life.work()
            old_events = copy.deepcopy(life.store.list("life_events"))
            request = h.request(text="Secret person 123456 wants to play music and read a book")
            await h.core.ingest("nonebot", request)
            actor = life.store.get("life_actors", "actor:a")
            version = actor["life_content_version"]
            assert actor["life_interests"] == ["music", "reading"]
            await h.core.ingest("nonebot", request)
            assert life.store.get("life_actors", "actor:a")["life_content_version"] == version
            assert life.store.get("life_actors", "actor:b").get("life_interests", []) == []
            life.tick(force=True)
            for _ in range(4):
                await life.work()
            calls = [c for c in model.calls if c[0] == "stage"]
            assert any(c[2]["interests"] == ["music", "reading"] for c in calls)
            assert "123456" not in json.dumps(calls)
            assert "Secret person" not in json.dumps(life.store.list("life_events"))
            assert all(life.store.get("life_events", e["id"]) == e for e in old_events)
            retract = h.request(
                message=request["message_key"]["message_id"], revision=2, kind="retract"
            )
            await h.core.ingest("nonebot", retract)
            assert life.store.get("life_actors", "actor:a")["life_interests"] == []
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_work_synchronizes_once_and_sql_growth_is_linear():
    counts = []
    for count in (5, 25):
        life, clock, _ = make_life(writing=False, count=count)
        clock.advance(2)
        statements = []
        life.store.db.set_trace_callback(statements.append)
        try:
            with patch.object(life, "tick", wraps=life.tick) as tick:
                asyncio.run(life.work())
                assert tick.call_count == 1
            counts.append(len(statements))
        finally:
            life.store.close()
    assert counts[1] < counts[0] * 6


def facts(store):
    return {
        table: [tuple(r) for r in store.db.execute(f"SELECT * FROM {table} ORDER BY id")]
        for table in TABLES
    }


def test_today_timeline_authorization_paging_and_read_only_http():
    async def scenario():
        h = Harness()
        try:
            h.core.recover()
            life = h.core.life
            life.set_diary_access("actor:a", readers=["reader:story"])
            clock_day = plan(life, "actor:a")["day"]
            actor = life.store.get("life_actors", "actor:a")
            for index in range(27):
                life.record_event(
                    f"synthetic:{index}",
                    actor["world_id"],
                    f"Fictional observation {index}",
                    participants=["actor:a"],
                )
            app = create_app(
                h.core,
                {"platform": "fixture-token", "other": "fixture-other"},
                life_readers={
                    "platform": {"reader_id": "reader:story", "actor_ids": ["actor:a", "actor:b"]}
                },
            )
            before = facts(life.store)
            request = {"schema_version": 1, "actor_id": "actor:a"}
            headers = {"Authorization": "Bearer fixture-token"}
            with patch.object(life, "tick", side_effect=AssertionError("read ticked")):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test"
                ) as client:
                    today = await client.post(
                        "/internal/v1/life-read/today", json=request, headers=headers
                    )
                    assert (
                        today.status_code == 200
                        and today.json()["plan"]["generated_by"] == "baseline"
                    )
                    body = {**request, "day": clock_day, "limit": 10}
                    seen = []
                    while True:
                        response = await client.post(
                            "/internal/v1/life-read/timeline", json=body, headers=headers
                        )
                        assert response.status_code == 200
                        page = response.json()
                        seen.extend(e["event_id"] for e in page["items"])
                        if page["next_after"] is None:
                            break
                        body["after"] = page["next_after"]
                    assert len(seen) == len(set(seen)) == 28
                    denied = await client.post(
                        "/internal/v1/life-read/today",
                        json={**request, "actor_id": "actor:b"},
                        headers=headers,
                    )
                    assert denied.status_code == 404
                    bad = await client.post(
                        "/internal/v1/life-read/timeline",
                        json={
                            **request,
                            "day": clock_day,
                            "after": {"position": True, "known_id": "any"},
                        },
                        headers=headers,
                    )
                    assert bad.status_code == 400
                    access = life.store.get("life_access", "actor:a")
                    life.set_diary_access("actor:a", readers=[], expected=access["version"])
                    after_revoke = facts(life.store)
                    denied = await client.post(
                        "/internal/v1/life-read/timeline",
                        json={**request, "day": clock_day},
                        headers=headers,
                    )
                    assert denied.status_code == 404 and facts(life.store) == after_revoke
            # The read operations themselves wrote zero rows; only the explicit revoke did.
            for table in TABLES - {"life_access"}:
                assert before[table] == facts(life.store)[table]
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_timeline_deep_same_second_page_uses_bounded_index_seeks():
    life, clock, _ = make_life(writing=False)
    try:
        actor = life.store.get("life_actors", "actor:0")
        with life.store.transaction():
            for index in range(1500):
                life.record_event(
                    f"event:{index:05}",
                    actor["world_id"],
                    "A fictional moment",
                    participants=["actor:0"],
                )
        query = LifeReadQueries(life.store.db)
        first = query.timeline_page("actor:0", "2026-10-03", 10)
        after = (first[-1]["sequence"], first[-1]["id"])
        steps = [0]
        life.store.db.set_progress_handler(lambda: steps.__setitem__(0, steps[0] + 1) or 0, 1)
        rows = query.timeline_page("actor:0", "2026-10-03", 10, after)
        life.store.db.set_progress_handler(None, 0)
        assert len(rows) == 10 and steps[0] < 400
    finally:
        life.store.close()


def test_role_disable_restart_enable_reinstalls_unchanged_bot_binding(tmp_path):
    async def scenario():
        h = Harness(tmp_path / "role.sqlite", personas=persona_config())
        try:
            h.sender.available = True
            role = {
                "actor_id": "actor:a",
                "application_id": "role:a",
                "operator": "operator:test",
                "name": "Role A",
                "profile_id": None,
                "profile_version": None,
                "capabilities": ["dialogue"],
            }
            router = BotSenderRouter(h.sender, h.sender, [], h.core.bindings)
            h.core.bot_bindings = BotBindings(h.core, router, h.core.bindings)
            binding = {
                "connection_id": "bot:test",
                "binding_id": "binding:bot:test",
                "actor_id": "actor:a",
                "conversation": {"kind": "group", "id": "synthetic:g"},
            }
            h.core.bot_bindings.apply(
                "platform", {**binding, "revision": 1, "request_id": "binding:1", "enabled": False}
            )
            h.core.bot_bindings.apply(
                "platform", {**binding, "revision": 2, "request_id": "binding:2", "enabled": True}
            )
            for version, enabled in ((0, False), (1, True), (2, False)):
                h.core.role_runtime.apply(
                    "platform",
                    {
                        **role,
                        "expected_version": version,
                        "request_id": f"role:{version}",
                        "enabled": enabled,
                    },
                )
            assert binding["binding_id"] not in h.core.bindings
            await h.core.close()
            h.core = h.new_core()
            router = BotSenderRouter(h.sender, h.sender, [], h.core.bindings)
            h.core.bot_bindings = BotBindings(h.core, router, h.core.bindings)
            h.core.recover()
            assert binding["binding_id"] not in h.core.bindings
            enabled = h.core.role_runtime.apply(
                "platform",
                {**role, "expected_version": 3, "request_id": "role:enable", "enabled": True},
            )
            assert enabled["enabled"] and binding["binding_id"] in h.core.bindings
            assert h.core.life.store.get("life_actors", "actor:a")["life_enabled"]
            same = {**binding, "revision": 2, "request_id": "binding:2", "enabled": True}
            assert h.core.bot_bindings.apply("platform", same)["enabled"]
            assert h.core.bindings[binding["binding_id"]]["actor_ids"] == ["actor:a"]
            h.core.role_runtime.apply(
                "platform",
                {**role, "expected_version": 4, "request_id": "role:disable", "enabled": False},
            )
            assert binding["binding_id"] not in h.core.bindings
            assert not h.core.bot_bindings.apply("platform", same)["enabled"]
            assert not h.core.life.store.get("life_actors", "actor:a")["life_enabled"]
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_enabled_role_without_dialogue_keeps_life_running():
    async def scenario():
        h = Harness(personas=persona_config())
        try:
            h.clock.now = datetime(2026, 10, 3, 1, tzinfo=timezone.utc).timestamp()
            h.core.recover()
            role = {
                "actor_id": "actor:a",
                "application_id": "life-only",
                "operator": "operator:test",
                "name": "Life only",
                "profile_id": None,
                "profile_version": None,
                "capabilities": [],
            }
            for version, enabled in ((0, False), (1, True)):
                h.core.role_runtime.apply(
                    "platform",
                    {
                        **role,
                        "request_id": f"life-only:{version}",
                        "expected_version": version,
                        "enabled": enabled,
                    },
                )
            life = h.core.life
            first = life.summary("actor:a")
            assert not h.core._role_allows("actor:a", "dialogue")
            with pytest.raises(Fault) as denied:
                await h.ingest(actor="actor:a")
            assert denied.value.code == "forbidden"
            h.clock.advance(3 * 3600)
            await h.core.tick()
            assert life.summary("actor:a")["changed_at"] > first["changed_at"]
            assert plan(life, "actor:a")["state"] == "active"
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_timeout_is_unknown_and_generated_projections_match_published_contract():
    async def scenario():
        life, clock, model = make_life()
        try:
            life.set_diary_access("actor:0", readers=["reader:story"])
            port = LifeRead(
                LifeReadQueries(life.store.db),
                readers=readers(
                    {"platform": {"reader_id": "reader:story", "actor_ids": ["actor:0"]}}
                ),
                contracts=contracts(),
                clock=clock,
            )
            model.timeout_stage = True
            await life.work()
            stage = generation_tasks(life, "stage")[0]
            assert stage["state"] == "interrupted"
            calls = len(model.calls)
            clock.advance(61)
            await life.work()
            assert len(model.calls) == calls
            model.timeout_stage = False
            life.retry_generation(stage["id"], expected=stage["version"])
            await life.work()
            before = facts(life.store)
            today = port.handle("platform", "today", {"schema_version": 1, "actor_id": "actor:0"})
            timeline = port.handle(
                "platform",
                "timeline",
                {"schema_version": 1, "actor_id": "actor:0", "day": "2026-10-03"},
            )
            assert today["plan"]["generated_by"] == "gateway"
            assert any(e["generated_by"] == "gateway" for e in timeline["items"])
            contracts().check("life-read#today_response", today)
            contracts().check("life-read#timeline_response", timeline)
            assert before == facts(life.store)
        finally:
            life.store.close()

    asyncio.run(scenario())


def test_timeline_index_missing_is_backed_up_once_before_initialization(tmp_path):
    path = tmp_path / "life.sqlite"
    life, _, _ = make_life(path, writing=False)
    original = facts(life.store)
    life.store.db.execute("DROP INDEX " + TIMELINE_INDEX)
    life.store.close()
    reopened = Store(path)
    try:
        assert facts(reopened) == original
        assert reopened.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name=?", (TIMELINE_INDEX,)
        ).fetchone()
        backups = list(tmp_path.glob("life.sqlite.pre-life-read-index-*.bak"))
        assert len(backups) == 1
        with sqlite3.connect(backups[0]) as backup:
            assert (
                backup.execute(
                    "SELECT 1 FROM sqlite_master WHERE name=?", (TIMELINE_INDEX,)
                ).fetchone()
                is None
            )
    finally:
        reopened.close()
    final = Store(path)
    final.close()
    assert len(list(tmp_path.glob("life.sqlite.pre-life-read-index-*.bak"))) == 1


def test_open_specific_dialogue_changes_later_content_and_withdrawal_removes_it():
    async def scenario():
        h = Harness(silence_ms=0)
        try:
            h.clock.now = datetime(2026, 10, 3, 1, tzinfo=timezone.utc).timestamp()
            h.core.recover()
            life, model = h.core.life, Model()
            life.gateway, life.writing, life.config_version = model, True, 19
            request = h.request(text="今天想让你研究天体物理纪录片", message="source:astrophysics")
            await h.core.ingest("nonebot", request)
            initial = life.store.get("life_actors", "actor:a")
            assert (
                initial["life_interests"] == []
            )  # No keyword match; only real extraction can affect it.
            for _ in range(5):
                await life.work()
            influenced = life.store.get("life_actors", "actor:a")
            assert "研究天体物理纪录片中的恒星演化" in influenced["life_interests"]
            assert life.store.get("life_actors", "actor:b")["life_interests"] == []
            version = influenced["life_content_version"]
            await h.core.ingest("nonebot", request)
            assert life.store.get("life_actors", "actor:a")["life_content_version"] == version
            h.clock.advance(3 * 3600)
            await h.core.tick()
            for _ in range(3):
                await life.work()
            assert any(
                "天体物理" in e["summary"]
                for e in life.store.list("life_events")
                if e["participants"] == ["actor:a"]
            )
            assert not any(
                "天体物理" in e["summary"]
                for e in life.store.list("life_events")
                if e["participants"] == ["actor:b"]
            )
            old = copy.deepcopy(life.store.list("life_events"))
            retract = h.request(message="source:astrophysics", revision=2, kind="retract")
            await h.core.ingest("nonebot", retract)
            assert life.store.get("life_actors", "actor:a")["life_interests"] == []
            assert life.store.list("life_events") == old
        finally:
            await h.core.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("regeneration", ["failed", "unavailable"])
def test_withdrawal_clears_generated_future_details_even_without_a_new_plan(regeneration):
    async def scenario():
        h = Harness()
        try:
            h.clock.now = datetime(2026, 10, 3, 9, tzinfo=timezone.utc).timestamp()
            life, model = h.core.life, Model()
            life.create_world("world:custom", timezone_name="UTC")
            life.create_room("room:custom", "world:custom")
            schedule = [
                {"minute": 0, "activity": "用户设置的休息"},
                {"minute": 540, "activity": "用户设置的上午安排"},
                {"minute": 720, "activity": "用户设置的午后安排"},
                {"minute": 900, "activity": "用户设置的傍晚安排"},
            ]
            life.configure_actor("actor:a", "room:custom", schedule=schedule, personality_version=1)
            h.core.recover()
            life.gateway, life.config_version = model, 19
            original = model.generate

            async def influenced_plan(turn, messages):
                material = json.loads(messages[1]["content"])
                if "today's fictional intentions" in messages[0]["content"] and any(
                    "天体物理" in intent for intent in material["interests"]
                ):
                    result, receipt = await original(turn, messages)
                    generated = json.loads(result[0])
                    for entry in generated["entries"]:
                        entry.update(
                            activity="研究天体物理纪录片", detail="后续继续研究天体物理纪录片"
                        )
                    return [json.dumps(generated)], receipt
                return await original(turn, messages)

            model.generate = influenced_plan
            await h.core.ingest(
                "nonebot",
                h.request(text="今天想让你研究天体物理纪录片", message="plan:committed-source"),
            )
            for _ in range(6):
                await life.work()
            before = plan(life, "actor:a")
            assert before["generation_state"] == "completed"
            assert any(
                "天体物理" in (e["detail"] or "") for e in before["entries"] if e["minute"] > 540
            )
            occurred = copy.deepcopy(life.store.list("life_events"))
            current_phase = copy.deepcopy(next(e for e in before["entries"] if e["minute"] == 540))
            await h.core.ingest(
                "nonebot", h.request(message="plan:committed-source", revision=2, kind="retract")
            )
            life.tick(force=True)
            cleared = plan(life, "actor:a")
            assert (
                next(e for e in cleared["entries"] if e["minute"] == 540)["detail"]
                == current_phase["detail"]
            )
            for entry in cleared["entries"]:
                if entry["minute"] > 540:
                    assert entry["detail"] is None
                    assert entry["activity"] == next(
                        e["activity"] for e in schedule if e["minute"] == entry["minute"]
                    )
            assert life.store.list("life_events") == occurred
            assert life.store.get("life_actors", "actor:a")["schedule"] == [
                dict(e, controls={}) for e in schedule
            ]
            if regeneration == "failed":
                model.plan_failure = True
            else:
                model.available = False
            for _ in range(4):
                await life.work()
            h.clock.advance(3 * 3600)
            life.tick(force=True)
            model.available = True
            model.plan_failure = True
            for _ in range(4):
                await life.work()
            later = [
                material
                for kind, _, material in model.calls
                if kind == "stage"
                and material["day"] == "2026-10-03"
                and material["stage"]["minute"] == 720
                and material["schedule"] == [dict(e, controls={}) for e in schedule]
            ]
            assert later
            assert later[-1]["planned_detail"] is None
            assert later[-1]["stage"]["activity"] == "用户设置的午后安排"
            assert all(
                "天体物理" not in (e["detail"] or "")
                for e in plan(life, "actor:a")["entries"]
                if e["minute"] > 540
            )
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_role_personas_and_ai_activity_names_reach_current_and_future_projections():
    async def scenario():
        h = Harness(personas=persona_config())
        try:
            h.clock.now = datetime(2026, 10, 3, 1, tzinfo=timezone.utc).timestamp()
            h.core.recover()
            life, model = h.core.life, Model()
            life.gateway, life.writing, life.config_version = model, True, 19
            with patch.object(h.core.personas, "verify", wraps=h.core.personas.verify) as verify:
                life.persona_verifier = verify
                for _ in range(4):
                    await life.work()
                assert verify.call_count >= 4
            plans = [call for call in model.calls if call[0] == "plan"]
            assert {call[2]["persona_expression"] for call in plans} == {"Role A", "Role B"}
            assert life.summary("actor:a")["activity"] == "studying fictional constellations"
            assert life.summary("actor:b")["activity"] == "painting a fictional landscape"
            schedules = {a["id"]: a["schedule"] for a in life.store.list("life_actors")}
            h.clock.advance(3 * 3600)
            await h.core.tick()
            assert life.summary("actor:a")["activity"] == "studying fictional constellations"
            assert life.summary("actor:b")["activity"] == "painting a fictional landscape"
            assert {a["id"]: a["schedule"] for a in life.store.list("life_actors")} == schedules
            for actor_id in ("actor:a", "actor:b"):
                daily = plan(life, actor_id)
                current = next(
                    e for e in daily["entries"] if e["phase_id"] == daily["current_phase_id"]
                )
                assert life.summary(actor_id)["activity"] == current["activity"]
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_source_withdrawn_during_intention_extraction_cannot_apply_late_result():
    async def scenario():
        h = Harness(silence_ms=0)
        try:
            h.core.recover()
            life, model = h.core.life, Model()
            life.gateway, life.writing, life.config_version = model, True, 19
            model.influence_gate = asyncio.Event()
            request = h.request(text="研究天体物理纪录片", message="source:late")
            await h.core.ingest("nonebot", request)
            worker = asyncio.create_task(life.work())
            for _ in range(20):
                await asyncio.sleep(0)
                if any(call[0] == "influence" for call in model.calls):
                    break
            await h.core.ingest(
                "nonebot", h.request(message="source:late", revision=2, kind="retract")
            )
            model.influence_gate.set()
            await worker
            assert life.store.get("life_actors", "actor:a")["life_interests"] == []
            assert not any("天体物理" in e["summary"] for e in life.store.list("life_events"))
            tasks = life.store.list("metadata", "life:influence-generation")
            assert tasks[0]["state"] == "superseded"
        finally:
            await h.core.close()

    asyncio.run(scenario())
