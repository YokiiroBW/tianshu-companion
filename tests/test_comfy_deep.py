"""Synthetic schemas and owners; no real model, account or GPU submission."""

import asyncio
import copy
import json
from pathlib import Path

import httpx
import pytest

from support import Harness
from test_delivery_v2 import source_turn
from test_images import png
from test_proactive import member, START
from tianshu_companion.contracts import Fault
from tianshu_companion.image_comfy import ComfyUI
from tianshu_companion.image_provider import ImageResult
from tianshu_companion.image_workflow import Workflow
from tianshu_companion.image_workflow_conversion import inspect
from test_delivery_v2 import NativeQueue


def fixtures():
    root = Path(__file__).parent / "fixtures"
    return tuple(
        json.loads((root / name).read_text(encoding="utf-8"))
        for name in ("comfy_semantic_ui.json", "comfy_object_info.json")
    )


class ComfyFixture:
    def __init__(self):
        self.source, self.info = fixtures()
        self.calls = []
        self.complete = False
        self.reject = False

    def response(self, request):
        self.calls.append((request.method, request.url.path))
        if request.url.path == "/system_stats":
            return httpx.Response(200, json={"system": {"fixture": True}})
        if request.url.path == "/object_info":
            return httpx.Response(200, json=self.info)
        if request.url.path == "/userdata":
            return httpx.Response(200, json=["synthetic.json", "_backups/old.json", "old.json.bak"])
        if request.url.path == "/userdata/workflows/synthetic.json":
            return httpx.Response(200, json=self.source)
        if request.url.path == "/prompt":
            if self.reject:
                return httpx.Response(400, json={"error": "fixture"})
            body = json.loads(request.content)
            self.prompt_id = body["prompt_id"]
            return httpx.Response(200, json={"prompt_id": self.prompt_id})
        if request.url.path.startswith("/history/"):
            if not self.complete:
                return httpx.Response(200, json={})
            return httpx.Response(
                200,
                json={
                    self.prompt_id: dict(
                        status=dict(completed=True),
                        outputs={
                            "20": dict(
                                images=[dict(filename="fixture.png", subfolder="", type="output")]
                            )
                        },
                    )
                },
            )
        if request.url.path == "/queue":
            return httpx.Response(
                200, json=dict(queue_pending=[], queue_running=[[0, self.prompt_id]])
            )
        if request.url.path == "/view":
            return httpx.Response(200, content=png())
        return httpx.Response(404, json={})

    async def resolve(self, value):
        transport = ComfyUI(value["base_url"], credential_ref=value["credential_ref"])
        await transport.client.aclose()
        transport.client = httpx.AsyncClient(
            base_url=value["base_url"], transport=httpx.MockTransport(self.response)
        )
        return transport


class SemanticModel:
    available = True

    def __init__(self):
        self.calls = []

    async def generate(self, turn, messages):
        self.calls.append(copy.deepcopy(messages))
        material = json.loads(messages[-1]["content"])
        if "workflows" in material:
            value = dict(
                workflow_id="synthetic.json", reason="fixture compatible character template"
            )
        elif "candidates" in material:
            value = dict(bindings=material["inferred"])
        else:
            value = {key: "translated " + key if value else "" for key, value in material.items()}
        return [json.dumps(value)], dict(config_version=1, fixture_only=True)


async def configured(h, directory):
    if not h.core.store.get("life_actors", "actor:a"):
        h.core.recover()
    fixture = ComfyFixture()
    h.core.image_backend.resolve = fixture.resolve
    h.core.images.staging = directory / "staging"
    catalog = h.core.image_backend.catalog
    base = dict(schema_version=1, actor_id="actor:a")
    await catalog.call(
        "platform",
        "manage",
        dict(
            base,
            request_id="connect",
            operation="connection.configure",
            expected_version=1,
            value=dict(base_url="http://127.0.0.1:8188", credential_ref=None, enabled=True),
        ),
    )
    await catalog.call(
        "platform",
        "manage",
        dict(
            base,
            request_id="select",
            operation="workflow.select",
            expected_version=1,
            value=dict(workflow_id="synthetic.json", bindings=None),
        ),
    )
    return fixture, catalog


def image_value(id="image:fixture", **extra):
    return dict(
        id=id,
        outfit_id=None,
        activity_id=None,
        scene="走到窗边",
        parameters=dict(width=1536, height=1024),
        edit_source_id=None,
        edit_source_ref=None,
        scope=None,
        query=None,
        **extra,
    )


def test_ui_conversion_routes_semantics_and_preserves_identity_style():
    source, info = fixtures()
    result, graph = inspect(source, info, "synthetic.json")
    assert result["state"] == "ready"
    assert graph["12"]["inputs"]["auto_detect_source"] == "backend"
    assert graph["12"]["inputs"]["auto_detect_width"] == 0
    assert (
        graph["12"]["inputs"]["rescale_value"]
        == source["nodes"][next(i for i, node in enumerate(source["nodes"]) if node["id"] == 12)][
            "properties"
        ]["rescaleValue"]
    )
    workflow = Workflow(graph, result["bindings"], result["outputs"])
    rendered = workflow.render(
        dict(
            outfit="blue dress",
            pose="reading",
            background="garden",
            camera="landscape",
            positive="fictional scene",
            width=1536,
            height=1024,
        )
    )
    assert "[Reference and wardrobe ruling]\nblue dress" in rendered["35"]["inputs"]["text"]
    assert "[Composition and continuity]\nreading" in rendered["35"]["inputs"]["text"]
    for node in ("11", "27", "32", "36"):
        assert rendered[node] == graph[node]
    graph["35"]["inputs"]["text"] = (
        "[Reference and wardrobe ruling]\nfixed silver dress\n\n"
        "[User image request]\nfixed template context"
    )
    inherited = Workflow(graph, result["bindings"], result["outputs"]).render(
        dict(positive="daily scene", pose="reading")
    )
    assert "fixed silver dress" in inherited["35"]["inputs"]["text"]
    assert "fixed template context" in inherited["35"]["inputs"]["text"]
    graph["35"]["inputs"]["text"] = "plain template with fixed silver dress"
    inherited = Workflow(graph, result["bindings"], result["outputs"]).render(
        dict(positive="daily scene", pose="reading")
    )
    assert "plain template with fixed silver dress" in inherited["35"]["inputs"]["text"]


@pytest.mark.parametrize("change", ["unknown", "layout", "dimensions", "bypass"])
def test_unsafe_ui_conversion_is_explicitly_unsupported(change):
    source, info = fixtures()
    node = next(node for node in source["nodes"] if node["id"] == 12)
    if change == "unknown":
        node["type"] = "UnknownCustomNode"
    elif change == "layout":
        node["properties"].pop("autoDetectSource")
    elif change == "dimensions":
        node["widgets_values"][2] += 64
    else:
        node["mode"] = 4
    result, graph = inspect(source, info, "synthetic.json")
    assert graph is None and result["state"] == "unsupported" and result["unresolved"]


def test_catalog_discovery_actor_isolation_cas_and_dry_run(tmp_path):
    async def run():
        h = Harness()
        try:
            fixture, catalog = await configured(h, tmp_path)
            found = await catalog.discover()
            assert [item["id"] for item in found["items"]] == ["synthetic.json"]
            assert catalog.actor("actor:b")["workflow_id"] is None
            request = dict(
                schema_version=1,
                request_id="compile",
                actor_id="actor:a",
                intent=dict(outfit="blue robe", camera="wide scene"),
                parameters=dict(width=1536, height=1024),
                assist_model=False,
            )
            result = (await catalog.call("platform", "compile", request))["result"]
            assert result["dimensions"] == dict(width=1536, height=1024)
            assert "horizontal canvas" in result["prompts"]["camera"]
            assert "character" not in result["prompts"]
            assert "27" in result["preserved_nodes"]
            assert not h.core.store.list("image_jobs")
            assert not any(method == "POST" for method, _ in fixture.calls)
            with pytest.raises(Fault) as denied:
                await catalog.call("nonebot", "compile", request)
            assert denied.value.code == "forbidden"
            with pytest.raises(Fault) as stale:
                await catalog.call(
                    "platform",
                    "manage",
                    dict(
                        schema_version=1,
                        request_id="stale",
                        actor_id="actor:a",
                        operation="actor.configure",
                        expected_version=1,
                        value=dict(workflow_id="synthetic.json", character_prompt="", defaults={}),
                    ),
                )
            assert stale.value.code == "version_conflict"
            request["parameters"] = dict(width=4096, height=4096)
            with pytest.raises(Fault) as large:
                await catalog.call("platform", "compile", request)
            assert large.value.code == "budget_exceeded"
            await catalog.call(
                "platform",
                "manage",
                dict(
                    schema_version=1,
                    request_id="disable",
                    actor_id="actor:a",
                    operation="connection.configure",
                    expected_version=2,
                    value=dict(
                        base_url="http://127.0.0.1:8188", credential_ref=None, enabled=False
                    ),
                ),
            )
            h.core.images.transport = fixture
            await catalog.restore()
            assert h.core.images.transport is None
        finally:
            await h.core.close()

    asyncio.run(run())


def test_model_workflow_selection_and_typed_adaptation(tmp_path):
    async def run():
        h = Harness()
        try:
            _, catalog = await configured(h, tmp_path)
            model = SemanticModel()
            h.core.gateway = h.core.life.gateway = model
            result = await catalog.call(
                "platform",
                "manage",
                dict(
                    schema_version=1,
                    request_id="analyze",
                    actor_id="actor:a",
                    operation="workflow.analyze",
                    expected_version=2,
                    value=dict(workflow_id=None, goal="角色日常穿搭", assist_model=True),
                ),
            )
            assert result["result"]["workflow_id"] == "synthetic.json"
            assert result["result"]["state"] == "ready"
            assert len(model.calls) == 2 and catalog.actor("actor:a")["version"] == 2
            assert "object_info" not in model.calls[0][-1]["content"]
        finally:
            await h.core.close()

    asyncio.run(run())


def test_native_image_tool_translation_single_slot_replay_and_no_outfit(tmp_path):
    async def run():
        h = Harness(silence_ms=0)
        try:
            turn = await source_turn(h)
            fixture, catalog = await configured(h, tmp_path)

            class Native(SemanticModel):
                def __init__(self):
                    super().__init__()
                    self.completions = 0

                async def complete(self, turn, messages, **kwargs):
                    self.completions += 1
                    if self.completions == 1:
                        return dict(
                            role="assistant",
                            content=None,
                            tool_calls=[
                                dict(
                                    id="photo-tool",
                                    type="function",
                                    function=dict(
                                        name="life_image_request",
                                        arguments=json.dumps(
                                            dict(
                                                expected_version=0,
                                                value={
                                                    key: value
                                                    for key, value in image_value().items()
                                                    if key not in {"id", "scope", "query"}
                                                },
                                            )
                                        ),
                                    ),
                                )
                            ],
                        ), dict(config_version=1, fixture_only=True)
                    return dict(role="assistant", content="图像请求已受理。"), dict(
                        config_version=1, fixture_only=True
                    )

            model = Native()
            h.core.gateway = h.core.life.gateway = model
            h.core.models = asyncio.Semaphore(1)
            async with h.core.models:
                output, _ = await asyncio.wait_for(h.core._respond_native(turn, []), timeout=2)
            job = h.core.store.list("image_jobs")[0]
            assert job["outfit"] is None and job["state"] == "queued"
            assert len(model.calls) == 1
            assert "translated pose" in job["graph"]["35"]["inputs"]["text"]
            assert job["graph"]["32"]["inputs"]["character_tags"] == "fixed character"
            await h.core.life_runtime.prepare_action(
                "actor:a", "image.request", image_value(id=job["id"]), 0
            )
            assert len(model.calls) == 1
            await h.core.images.work()
            fixture.complete = True
            await h.core.images.work()
            assert h.core.images.get(job["id"])["state"] == "completed"
            assert len([call for call in fixture.calls if call == ("POST", "/prompt")]) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_waiting_preparation_does_not_block_tool_owning_single_model_slot(tmp_path):
    async def run():
        h = Harness()
        try:
            _, catalog = await configured(h, tmp_path)
            model = SemanticModel()
            h.core.gateway = h.core.life.gateway = model
            h.core.models = asyncio.Semaphore(1)
            async with h.core.models:
                waiting = asyncio.create_task(
                    catalog.prepare_request("actor:a", image_value(id="image:waiting"))
                )
                await asyncio.sleep(0)
                with pytest.raises(Fault) as duplicate:
                    await catalog.prepare_request("actor:a", image_value(id="image:waiting"))
                assert duplicate.value.code == "result_unknown"
                await asyncio.wait_for(
                    catalog.prepare_request("actor:a", image_value(), model_slot_held=True),
                    timeout=2,
                )
            await asyncio.wait_for(waiting, timeout=2)
            assert len(model.calls) == 2
        finally:
            await h.core.close()

    asyncio.run(run())


def test_sync_provider_uses_opaque_plan_and_same_original_ledger(tmp_path):
    async def run():
        h = Harness()
        h.core.recover()

        class SyncProvider:
            identity = "fixture-provider"
            provider = "fixture-sync"

            def __init__(self):
                self.calls = []

            async def submit(self, local_id, plan):
                self.calls.append((local_id, plan))
                return ImageResult(
                    "completed", [dict(result_key="opaque")], handle=dict(receipt="server-assigned")
                )

            async def image(self, descriptor, maximum):
                persisted = h.core.images.get("sync")
                assert persisted["state"] == "unknown"
                assert persisted["provider_handle"] == dict(receipt="server-assigned")
                return png(), 1, 1

            async def close(self):
                pass

        provider = SyncProvider()
        h.core.images.transport = provider
        h.core.images.staging = tmp_path
        try:
            job = h.core.images.request(
                "sync",
                "actor:a",
                prepared_plan=dict(
                    provider=provider.provider, submission=dict(prompt="semantic request")
                ),
            )
            await h.core.images.work()
            result = h.core.images.get(job["id"])
            assert result["state"] == "completed" and result["provider_handle"] == dict(
                receipt="server-assigned"
            )
            assert len(h.core.store.list("image_media")) == 1
            assert len(provider.calls) == 1
            assert result["graph"] is None and result["outputs"] == []
        finally:
            await h.core.close()

    asyncio.run(run())


def test_async_provider_persists_server_handle_across_restart(tmp_path):
    async def run():
        h = Harness(tmp_path / "core.db")
        h.core.recover()

        class Provider:
            identity = "fixture-server-handles"
            provider = "fixture-async"

            def __init__(self):
                self.submits, self.polls = [], []

            async def submit(self, local_id, plan):
                self.submits.append(local_id)
                return ImageResult("queued", handle=dict(server_job="different-id"))

            async def poll(self, handle, plan):
                self.polls.append(handle)
                return ImageResult("completed", [dict(download="opaque")])

            async def image(self, descriptor, maximum):
                return png(), 1, 1

        provider = Provider()
        try:
            h.core.images.transport, h.core.images.staging = provider, tmp_path / "staging"
            job = h.core.images.request(
                "async",
                "actor:a",
                prepared_plan=dict(
                    provider=provider.provider, submission=dict(prompt="semantic request")
                ),
            )
            await h.core.images.work()
            failed = h.core.images.get("async")
            failed["failure"] = "ReadTimeout"
            h.core.images._save_job(failed)
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            h.core.images.transport, h.core.images.staging = provider, tmp_path / "staging"
            await h.core.images.work()
            assert h.core.images.get("async")["state"] == "completed"
            assert h.core.images.get("async")["failure"] is None
            assert provider.submits == [job["prompt_id"]] and provider.polls == [
                dict(server_job="different-id")
            ]
        finally:
            await h.core.close()

    asyncio.run(run())


def test_http_image_backend_reuses_platform_auth_and_never_submits(tmp_path):
    async def run():
        from tianshu_companion.app import create_app

        h = Harness()
        try:
            fixture, _ = await configured(h, tmp_path)
            app = create_app(h.core, {"platform": "test-token", "nonebot": "channel-token"})
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                request = dict(
                    schema_version=1,
                    request_id="status",
                    actor_id="actor:a",
                    resource="status",
                    workflow_id=None,
                )
                path = "/internal/v1/image-backend/read"
                assert (await client.post(path, json=request)).status_code == 401
                assert (
                    await client.post(
                        path, json=request, headers={"Authorization": "Bearer channel-token"}
                    )
                ).status_code == 403
                response = await client.post(
                    path, json=request, headers={"Authorization": "Bearer test-token"}
                )
                assert response.status_code == 200
                assert response.json()["result"]["workflow_id"] == "synthetic.json"
                assert response.headers["cache-control"] == "no-store"
                malformed = await client.post(
                    path,
                    content=json.dumps(request),
                    headers={"Authorization": "Bearer test-token", "Content-Type": "text/plain"},
                )
                assert malformed.status_code == 400
                assert not any(method == "POST" for method, _ in fixture.calls)
        finally:
            await h.core.close()

    asyncio.run(run())


async def photo_candidate(h, tmp_path):
    h.clock.now = START
    h.core.recover()
    target = await member(h, cooldown_seconds=0)
    fixture, catalog = await configured(h, tmp_path)
    scope = {
        key: target.subscription[key]
        for key in ("actor_id", "person_id", "audience", "conversation_id")
    }
    event = dict(
        id="event:outfit",
        world_id=h.core.life._get("actors", "actor:a")["world_id"],
        participants=["actor:a"],
        visible_to=[],
        summary="在花园阅读，想分享今天穿搭",
        occurred_at=h.clock(),
        fictional=True,
        kind="reading_progress",
        scope=scope,
        content_refs=[],
    )
    h.core.life._event(event)
    h.core.runtime_execution.offer_pending_events()
    candidate = h.core.store.list("proactive_candidates")[0]
    origin = h.request()["command"]["origin"]

    class Queue(NativeQueue):
        async def expression_context(self, requested_origin, requested_scope, channel):
            return dict(scope=requested_scope, origin=origin, channel=channel)

    queue = Queue(h.clock)
    queue.states, queue.final = ["sent"], True
    h.core.sender = queue
    model = SemanticModel()
    h.core.gateway = h.core.life.gateway = model
    candidate.update(
        photo=dict(
            intent=dict(outfit="blue robe", pose="reading in garden"),
            parameters=dict(width=1024, height=1536),
        ),
        photo_state="decided",
        expression_state="completed",
        content="想给你看看今天的穿搭。",
        content_version="fixture-photo",
    )
    h.core.proactive._save("candidates", candidate)
    return candidate, fixture, queue, model


def test_proactive_photo_waits_single_job_restores_and_sends_real_original(tmp_path):
    async def run():
        h = Harness(tmp_path / "core.db")
        try:
            candidate, fixture, queue, model = await photo_candidate(h, tmp_path)
            await h.core.proactive.work()
            waiting = h.core.store.get("proactive_candidates", candidate["id"])
            assert waiting["photo_state"] == "waiting"
            assert queue.requests == []
            job = h.core.images.get(waiting["photo_job_id"])
            assert job["proactive_candidate_id"] == candidate["id"]
            assert len(model.calls) == 1
            await h.core.proactive.dispatch(candidate["id"])
            assert queue.requests == []
            # Repeated passes and a reconstructed photo owner reuse the original id/plan.
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            h.core.image_backend.resolve = fixture.resolve
            h.core.images.staging = tmp_path / "staging"
            h.core.sender = queue
            h.core.gateway = h.core.life.gateway = model
            await h.core.image_backend.catalog.restore()
            await h.core.proactive.work()
            assert len(model.calls) == 1 and len(h.core.store.list("image_jobs")) == 1
            await h.core.images.work()
            fixture.complete = True
            await h.core.images.work()
            await h.core.proactive.work()
            assert len(queue.requests) == 1
            assert queue.requests[0]["segments"][0]["media"][0]["data"]
            assert queue.requests[0]["segments"][0]["content_refs"][0]["owner"] == "companion"
            assert len([call for call in fixture.calls if call == ("POST", "/prompt")]) == 1
            h.core.runtime_execution.offer_pending_events()
            motives = h.core.store.list("proactive_motives")
            assert len(motives) == 1  # This subscription already owns the photo delivery.
            completed_event = next(
                item
                for item in h.core.store.list("life_events")
                if item.get("kind") == "image_completed"
            )
            assert h.core.runtime_execution.photos.from_image_event(
                dict(sources=[dict(owner="companion", object_id=completed_event["id"], version=1)])
            )
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["generation", "revoked_before_submit", "revoked_before_send"])
def test_proactive_photo_failures_never_claim_sent_or_resubmit(tmp_path, failure):
    async def run():
        h = Harness()
        try:
            candidate, fixture, queue, model = await photo_candidate(h, tmp_path)
            await h.core.proactive.work()
            current = h.core.store.get("proactive_candidates", candidate["id"])
            job_id = current["photo_job_id"]
            subscription = h.core.store.get("proactive_subscriptions", candidate["subscription_id"])
            if failure == "revoked_before_submit":
                h.core.proactive.revoke_subscription(
                    subscription["id"],
                    expected=subscription["version"],
                    reason="fixture revocation",
                )
            elif failure == "generation":
                fixture.reject = True
            await h.core.images.work()
            if failure == "revoked_before_send":
                fixture.complete = True
                await h.core.images.work()
                h.core.proactive.revoke_subscription(
                    subscription["id"],
                    expected=subscription["version"],
                    reason="fixture revocation",
                )
            await h.core.proactive.work()
            await h.core.proactive.work()
            assert queue.requests == [] and len(model.calls) == 1
            assert len(h.core.store.list("image_jobs")) == 1
            assert h.core.store.get("proactive_candidates", candidate["id"])["state"] == "cancelled"
            if failure == "revoked_before_submit":
                assert h.core.images.get(job_id)["state"] == "cancelled"
                assert not any(call == ("POST", "/prompt") for call in fixture.calls)
        finally:
            await h.core.close()

    asyncio.run(run())
