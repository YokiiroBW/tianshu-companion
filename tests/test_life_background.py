"""AL05: actual background admission, bounded actor phases and durable recovery."""

import asyncio
import copy
import json
import os
import hashlib

import httpx
import pytest

from support import Harness, native_sse
from test_daily_life import Model
from test_runtime_v2 import activity
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.app import build_runtime
from tianshu_companion import app as app_module
from tianshu_companion.clients import JsonService, utc
from tianshu_companion.life import Life
from tianshu_companion.model_slots import ModelSlots
from tianshu_companion.store import Store
from tianshu_companion.model_selection import HttpDefaultModelSelector, SelectionRequest


class ActivityModel:
    available = True

    def __init__(self):
        self.calls = []
        self.gate = None
        self.entered = asyncio.Event()
        self.error = None
        self.tool = True
        self.arguments = None
        self.tool_name = "life_activity_save"

    async def complete(self, turn, messages, *, tools):
        self.calls.append((copy.deepcopy(turn), copy.deepcopy(messages)))
        self.entered.set()
        if self.gate:
            await self.gate.wait()
        if self.error:
            raise self.error
        calls = []
        if self.tool:
            calls = [
                dict(
                    id="call:one",
                    type="function",
                    function=dict(name=self.tool_name, arguments=json.dumps(self.arguments)),
                )
            ]
        return dict(
            role="assistant", content="处理了本步骤；没有宣称阅读完成。", tool_calls=calls
        ), {"fixture": True}


def test_background_dynamic_selection_uses_its_own_authenticated_caller():
    async def run():
        seen = []

        def handler(request):
            body = json.loads(request.content)
            seen.append(body)
            assert request.headers["authorization"] == "Bearer background-selector"
            return httpx.Response(
                200,
                json=dict(
                    config_version=37,
                    expires_at=1_900_000_000,
                    revoked=False,
                    caller_service="companion_background",
                    workload="companion.text",
                ),
            )

        wire = JsonService(
            "https://platform.fixture.invalid",
            "background-selector",
            transport=httpx.MockTransport(handler),
        )
        selector = HttpDefaultModelSelector(wire, caller_service="companion_background")
        h = Harness()
        await h.core.close()
        h.core = h.new_core(background_gateway=ActivityModel(), background_model_selector=selector)
        try:
            task = dict(
                actor_id="actor:a",
                generation_turn_id="actual:background",
                config_version=99,
                model_selection=dict(
                    turn_id="actual:background",
                    expires_at=1_900_000_000,
                    caller_service="companion",
                ),
            )
            await h.core.life.select_generation(task)
            assert task["config_version"] == 37
            assert seen[0]["caller_service"] == "companion_background"
            assert seen[0]["function_id"] == "writing"
            with pytest.raises(Fault, match="forbidden"):
                await selector.select(
                    SelectionRequest("front", "actor:a", "person:a", "self_private", "conv:a")
                )
            assert len(seen) == 1
        finally:
            await h.core.close()
            await wire.close()

    asyncio.run(run())


def test_background_gateway_cannot_borrow_front_dynamic_or_default_version():
    async def run():
        class ForbiddenFrontSelector:
            async def select(self, request):
                raise AssertionError("background must not borrow a front grant")

        h = Harness()
        await h.core.close()
        h.core = h.new_core(
            background_gateway=ActivityModel(), default_model_selector=ForbiddenFrontSelector()
        )
        try:
            assert not h.core.life.generation_available()
            with pytest.raises(Fault, match="dependency_unavailable"):
                await h.core.life.select_generation(
                    dict(
                        actor_id="actor:a",
                        generation_turn_id="background:no-binding",
                        config_version=None,
                    )
                )
        finally:
            await h.core.close()
        h.core = h.new_core(
            background_gateway=ActivityModel(),
            life_config_version=41,
            default_model_selector=ForbiddenFrontSelector(),
        )
        try:
            task = dict(
                actor_id="actor:a", generation_turn_id="background:published", config_version=None
            )
            await h.core.life.select_generation(task)
            assert task["config_version"] == 41 and h.core.life.generation_available()
            assert "model_selection" not in task
        finally:
            await h.core.close()

    asyncio.run(run())


def activity_core(h, model):
    h.core.life.gateway = model
    h.core.recover()
    current = h.core.life.activities.current("actor:a")
    if current and current["state"] == "running":
        h.core.life.activities.transition(
            "actor:a", current["id"], "cancelled", expected=current["version"]
        )
    value = activity()
    value["next_due_at"] = h.clock()
    saved = h.core.life.activities.save("actor:a", value, expected=0)
    continued = copy.deepcopy(value)
    continued["checkpoint"]["step"] += 1
    model.arguments = dict(expected_version=saved["version"], value=continued)
    return saved


def test_activity_uses_background_reserve_and_separate_gateway():
    async def run():
        h = Harness()
        await h.core.close()
        model = ActivityModel()
        h.core = h.new_core(model_slots=2, background_gateway=model, life_config_version=1)
        model.gate = asyncio.Event()
        saved = activity_core(h, model)
        step = asyncio.create_task(h.core.life.activities.work(actor_id="actor:a"))
        try:
            await model.entered.wait()
            # A running activity must leave the foreground reserve available.
            async with asyncio.timeout(1):
                async with h.core.models:
                    assert len(model.calls) == 1
            assert not h.gateway.calls
            model.gate.set()
            assert await step
            marker = h.core.store.get(
                "metadata", "activity-step:" + digest([saved["id"], saved["version"]])
            )
            assert marker["state"] == "pending" and marker["round"] == 1
            assert marker["messages"][-1]["role"] == "tool"
        finally:
            model.gate.set()
            await step
            await h.core.close()

    asyncio.run(run())


def test_optional_background_service_is_a_distinct_authenticated_client(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setenv("AL05_FRONT_TOKEN", "fixture-foreground")
        monkeypatch.setenv("AL05_BACK_TOKEN", "fixture-background")
        config = dict(
            database_path=str(tmp_path / "services.db"),
            contracts_path=os.environ["TIANSHU_CONTRACTS"],
            config_version=1,
            services=dict(
                gateway=dict(url="https://gateway.fixture.invalid", token_env="AL05_FRONT_TOKEN"),
                gateway_background=dict(
                    url="https://gateway.fixture.invalid", token_env="AL05_BACK_TOKEN"
                ),
            ),
        )
        observed = []
        receipt_service = [None]

        # A protected service binding stand-in, not a body-declared workload class.
        def handler(request):
            service = {
                "Bearer fixture-foreground": "companion",
                "Bearer fixture-background": "companion_background",
            }[request.headers["Authorization"]]
            observed.append((request.url.path, service))
            if request.url.path == "/v1/chat/completions":
                assert "class" not in json.loads(request.content)
                return httpx.Response(
                    200,
                    json=dict(
                        choices=[
                            dict(
                                message=dict(role="assistant", content="本地替身"),
                                finish_reason="stop",
                            )
                        ]
                    ),
                )
            return httpx.Response(
                200,
                json=dict(
                    schema_version=1,
                    request_id=request.url.path.rsplit("/", 1)[-1],
                    config_version=1,
                    provider_id="provider:fixture",
                    requested_model=None,
                    resolved_model="fixture-model",
                    protocol="openai-chat-completions",
                    outcome="succeeded",
                    upstream_request_id="upstream:fixture",
                    usage=None,
                    fallback_used=False,
                    observed_at=utc(1_790_000_000),
                    credential_namespace="fixture",
                    caller_service=receipt_service[0] or service,
                    requested_reasoning={},
                    effective_reasoning={},
                    applied_policies=[
                        dict(field="model", mode="workload_binding", config_version=1)
                    ],
                    usage_complete=False,
                    native_usage=None,
                ),
            )

        monkeypatch.setattr(
            app_module,
            "JsonService",
            lambda *args, **kwargs: JsonService(
                *args, **kwargs, transport=httpx.MockTransport(native_sse(handler))
            ),
        )
        core, _, clients, _ = build_runtime(config)
        try:
            assert core.gateway.client.token == "fixture-foreground"
            assert core.life.gateway.client.token == "fixture-background"
            assert core.gateway.client is not core.life.gateway.client
            turn = dict(
                id="background:fixture",
                actor_id="actor:fixture",
                conversation_id="life:fixture",
                config_version=1,
            )
            _, receipt = await core.life.gateway.generate(turn, [dict(role="user", content="本地")])
            assert receipt["caller_service"] == "companion_background"
            assert {service for _, service in observed} == {"companion_background"}
            receipt_service[0] = "companion"
            with pytest.raises(Fault):
                await core.life.gateway.generate(turn, [dict(role="user", content="错误身份回执")])
        finally:
            await core.close()
            await asyncio.gather(*(client.close() for client in clients))
        del config["services"]["gateway_background"]
        core, _, clients, _ = build_runtime(config)
        try:
            assert core.life.gateway is core.gateway
        finally:
            await core.close()
            await asyncio.gather(*(client.close() for client in clients))

    asyncio.run(run())


def test_cancelled_before_background_slot_keeps_unsubmitted_phase_retryable():
    async def run():
        h = Harness()
        model = ActivityModel()
        saved = activity_core(h, model)
        operation = "activity-step:" + digest([saved["id"], saved["version"]])
        try:
            async with h.core.life.model_slots:
                # Occupy the other background slot too on the four-slot harness.
                async with h.core.life.model_slots:
                    task = asyncio.create_task(h.core.life.activities.work(actor_id="actor:a"))
                    await asyncio.sleep(0)
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            assert not model.calls
            assert h.core.store.get("metadata", operation)["state"] == "pending"
            assert h.core.life.activities.get("actor:a", saved["id"])["state"] == "running"
            await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.calls) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_proactive_expression_waits_in_background_and_rechecks_current_authority():
    async def run():
        h = Harness()
        h.core.recover()
        calls, checks = [], []

        class Model:
            available = True

            async def generate(self, turn, messages):
                calls.append(copy.deepcopy(messages))
                return ['{"send":false,"text":""}'], {"fixture": True}

        h.core.life.gateway = Model()
        scope = dict(
            actor_id="actor:a",
            person_id="person:fixture",
            audience="self_private",
            conversation_id="conversation:fixture",
        )
        candidate = dict(
            id="candidate:fixture",
            actor_id="actor:a",
            subject_version=1,
            summary="有真实来源的分享动机",
            sources=[],
            content_refs=[],
        )

        async def current(value):
            checks.append("current")
            return dict(scope=scope, origin={})

        async def validate(value, origin):
            checks.append("sources")

        async def select(*args, **kwargs):
            return {"selected_units": []}

        execution = h.core.runtime_execution
        execution.current_context, execution.validate_sources = current, validate
        h.memory.select = select
        try:
            async with h.core.life.model_slots:
                async with h.core.life.model_slots:
                    task = asyncio.create_task(execution.proactive_expression(candidate))
                    await asyncio.sleep(0)
                    assert checks == ["current", "sources"] and not calls
                    async with h.core.models:
                        assert not calls  # foreground capacity remains available
            result, _ = await task
            assert result is None and len(calls) == 1
            assert checks == ["current", "sources", "current", "sources", "sources"]
            assert not h.gateway.calls
        finally:
            await h.core.close()

    asyncio.run(run())


def test_activity_round_yields_and_restart_resumes_exact_known_messages(tmp_path):
    async def run():
        h = Harness(tmp_path / "background.db")
        model = ActivityModel()
        saved = activity_core(h, model)
        operation = "activity-step:" + digest([saved["id"], saved["version"]])
        try:
            await h.core.life.activities.work(actor_id="actor:a")
            frozen = copy.deepcopy(h.core.store.get("metadata", operation))
            assert len(model.calls) == 1 and frozen["state"] == "pending"
            await h.core.close()
            h.core = h.new_core(background_gateway=model, life_config_version=1)
            h.core.recover()
            model.tool = False
            await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.calls) == 2
            assert model.calls[1][1] == frozen["messages"]
            assert h.core.store.get("metadata", operation)["state"] == "completed"
            current = h.core.life.activities.get("actor:a", saved["id"])
            assert current["state"] == "paused"
            assert current["checkpoint"]["step"] == saved["checkpoint"]["step"] + 1
            result = h.core.store.get("metadata", "activity-result:" + digest(operation))
            assert len(result["receipts"]) == 2
        finally:
            await h.core.close()

    asyncio.run(run())


def test_unknown_activity_model_is_not_replayed_on_restart(tmp_path):
    async def run():
        h = Harness(tmp_path / "unknown.db")
        model = ActivityModel()
        saved = activity_core(h, model)
        model.error = Fault("dependency_unavailable", unknown=True)
        try:
            await h.core.life.activities.work(actor_id="actor:a")
            await h.core.close()
            h.core = h.new_core(background_gateway=model, life_config_version=1)
            h.core.recover()
            for _ in range(3):
                await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.calls) == 1
            current = h.core.life.activities.get("actor:a", saved["id"])
            assert current["state"] == "dormant" and current["next_due_at"] is None
            assert current["checkpoint"] == saved["checkpoint"]
            assert (
                h.core.store.get(
                    "metadata",
                    "activity-result:"
                    + digest("activity-step:" + digest([saved["id"], saved["version"]])),
                )
                is None
            )
        finally:
            await h.core.close()

    asyncio.run(run())


def test_unknown_activity_tool_freezes_original_step_instead_of_requesting_a_new_call():
    async def run():
        h = Harness()
        model = ActivityModel()
        saved = activity_core(h, model)
        model.tool_name = "life_content_acquire"
        model.arguments = dict(
            expected_version=0,
            value=dict(
                source=dict(kind="url", url="https://fixture.invalid/original"), purpose="read"
            ),
        )
        owner_calls = []

        class Owner:
            async def call(self, path, request, **kwargs):
                owner_calls.append(copy.deepcopy(request))
                raise Fault("dependency_unavailable", unknown=True)

        h.core.knowledge_client = Owner()
        operation = "activity-step:" + digest([saved["id"], saved["version"]])
        try:
            await h.core.life.activities.work(actor_id="actor:a")
            marker = copy.deepcopy(h.core.store.get("metadata", operation))
            assert marker["state"] == "unknown" and marker["unknown_call_id"] == "call:one"
            assert json.loads(marker["messages"][-1]["content"])["state"] == "unknown"
            h.clock.advance(600)
            for _ in range(3):
                await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.calls) == len(owner_calls) == 1
            assert h.core.store.get("metadata", operation) == marker
            assert h.core.life.activities.get("actor:a", saved["id"])["state"] == "paused"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_due_paused_known_round_keeps_operation_and_complete_tool_trajectory():
    async def run():
        h = Harness()
        model = ActivityModel()
        saved = activity_core(h, model)
        model.arguments["value"].update(state="paused", next_due_at=h.clock() + 60)
        operation = "activity-step:" + digest([saved["id"], saved["version"]])
        try:
            await h.core.life.activities.work(actor_id="actor:a")
            marker = copy.deepcopy(h.core.store.get("metadata", operation))
            await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.calls) == 1
            h.clock.advance(60)
            model.tool = False
            await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.calls) == 2 and model.calls[1][1] == marker["messages"]
            assert h.core.store.get("metadata", operation)["state"] == "completed"
            assert (
                len(
                    [
                        row
                        for row in h.core.store.list("metadata")
                        if row["id"].startswith("activity-step:")
                    ]
                )
                == 1
            )
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("withdraw", [False, True])
def test_activity_rechecks_actual_read_before_next_model_without_repeating_progress(withdraw):
    async def run():
        h = Harness()
        model = ActivityModel()
        saved = activity_core(h, model)
        prose = "已实际阅读的原话星空。"
        reference = dict(
            owner="memory",
            object_id="document:fixture",
            version=1,
            kind="text",
            sha256=hashlib.sha256(prose.encode()).hexdigest(),
            sources=[],
            coverage=dict(unit="characters", start=0, end=len(prose), total=len(prose)),
        )
        epoch_ = h.core.store.get("life_actors", "actor:a").get("life_runtime_epoch", 0)
        h.core.store.put(
            "life_content_refs",
            dict(
                id="content:" + digest(reference),
                actor_id="actor:a",
                scope=None,
                content_ref=reference,
                query=None,
            ),
        )
        h.core.store.put(
            "life_reading",
            dict(
                id="reading:fixture",
                actor_id="actor:a",
                conversation_id="actor:a",
                scope=None,
                version=1,
                state="open",
                runtime_epoch=epoch_,
                content_ref=reference,
                coverage=[],
                position=dict(unit="characters", start=0, end=0),
            ),
        )
        calls, revoked = [], [False]

        class Owner:
            async def call(self, path, request, **kwargs):
                calls.append(copy.deepcopy(request))
                if revoked[0]:
                    raise Fault("forbidden")
                return dict(
                    schema_version=1,
                    request_id=request["principal"]["request_id"],
                    content_ref=reference,
                    text=prose,
                    media_url=None,
                    coverage=dict(unit="characters", start=0, end=len(prose)),
                    complete=True,
                    representations=[],
                    gaps=[],
                    available_until=utc(h.clock() + 60),
                    current=True,
                    access_version=len(calls),
                    source=dict(
                        kind="upload",
                        locator="fixture:upload",
                        resolved_url=None,
                        filename="fixture.txt",
                        acquired_at=utc(h.clock()),
                        source_time=None,
                        media_type="text/plain",
                        size=len(prose.encode()),
                    ),
                )

        h.core.knowledge_client = Owner()
        model.tool_name = "life_reading_read"
        model.arguments = dict(
            expected_version=1,
            value=dict(
                id="reading:fixture", range=dict(unit="characters", start=0, end=len(prose))
            ),
        )
        operation = "activity-step:" + digest([saved["id"], saved["version"]])
        try:
            await h.core.life.activities.work(actor_id="actor:a")
            marker = copy.deepcopy(h.core.store.get("metadata", operation))
            assert marker["reads"][0]["content_ref"] == reference
            events = copy.deepcopy(h.core.store.list("life_events"))
            reading = copy.deepcopy(h.core.store.get("life_reading", "reading:fixture"))
            revoked[0], model.tool = withdraw, False
            await h.core.life.activities.work(actor_id="actor:a")
            assert len(calls) == 2
            assert len(model.calls) == (1 if withdraw else 2)
            assert h.core.store.get("life_reading", "reading:fixture") == reading
            assert (
                len(
                    [e for e in h.core.store.list("life_events") if e["kind"] == "reading_progress"]
                )
                == len([e for e in events if e["kind"] == "reading_progress"])
                == 1
            )
            if withdraw:
                assert h.core.store.get("metadata", operation)["messages"] == marker["messages"]
                assert h.core.life.activities.get("actor:a", saved["id"])["state"] == "paused"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_paused_activity_only_reevaluates_its_explicit_due_and_current_epoch():
    async def run():
        h = Harness()
        model = ActivityModel()
        model.tool = False
        saved = activity_core(h, model)
        value = {key: saved[key] for key in activity()}
        value.update(state="paused", next_due_at=h.clock() + 60)
        paused = h.core.life.activities.save("actor:a", value, expected=saved["version"])
        try:
            await h.core.life.activities.work(actor_id="actor:a")
            assert not model.calls
            h.clock.advance(60)
            await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.calls) == 1
            assert h.core.life.activities.get("actor:a", saved["id"])["version"] > paused["version"]
            # An old checkpoint cannot revive a disabled/re-enabled runtime epoch.
            current = h.core.life.activities.get("actor:a", saved["id"])
            value.update(state="running", next_due_at=h.clock())
            h.core.life.activities.save("actor:a", value, expected=current["version"])
            actor = h.core.store.get("life_actors", "actor:a")
            actor["life_runtime_epoch"] = actor.get("life_runtime_epoch", 0) + 1
            h.core.store.put("life_actors", actor)
            await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.calls) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_life_dispatcher_does_not_hold_other_actors_behind_a_slow_generation():
    async def run():
        h = Harness()
        slots = ModelSlots(4)
        gate = asyncio.Event()
        entered = asyncio.Event()
        model = Model()
        original = model.generate

        async def generate(turn, messages):
            if turn["actor_id"] == "actor:a":
                entered.set()
                await gate.wait()
            return await original(turn, messages)

        model.generate = generate
        life = Life(
            Store(":memory:"),
            h.clock,
            model,
            19,
            slots.background,
            writing=True,
            timezone_name="UTC",
            background_limit=slots.background_limit,
        )
        for actor in ("actor:a", "actor:b", "actor:c"):
            life.synchronize_role(actor, enabled=True, personality_version=1)
        life.recover()
        try:
            await life.work(wait=False)
            await entered.wait()
            for _ in range(20):
                await asyncio.sleep(0)
                await life.work(wait=False)
                if any(t[1]["actor_id"] == "actor:c" for t in model.calls):
                    break
            assert any(t[1]["actor_id"] == "actor:b" for t in model.calls)
            assert any(t[1]["actor_id"] == "actor:c" for t in model.calls)
            assert not any(t[1]["actor_id"] == "actor:a" for t in model.calls)
            assert not life.store.db.in_transaction
        finally:
            gate.set()
            await life.close()
            life.store.close()
            await h.core.close()

    asyncio.run(run())
