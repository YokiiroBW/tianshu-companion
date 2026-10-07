"""Ordinary dialogue assembly and automatic original delivery; synthetic model/transport."""

import asyncio
import copy
import json

import pytest

from support import Harness
from test_comfy_deep import configured, SemanticModel
from test_delivery_v2 import source_turn
from tianshu_companion.clients import utc
from tianshu_companion.contracts import Fault


class RecordingQueue:
    available = True

    def __init__(self, clock):
        self.clock, self.requests, self.receipts = clock, [], {}

    def expression_available(self, channel):
        return True

    async def expression_context(self, origin, scope, channel):
        if getattr(self, "unavailable", False):
            raise Fault("dependency_unavailable")
        self.valid_until = self.clock() + 30
        return dict(origin=origin["origin"], scope=scope, channel=channel)

    async def send_expression(self, request):
        assert self.valid_until > self.clock()

        self.requests.append(copy.deepcopy(request))
        receipt = dict(
            schema_version=2,
            request_id=request["request_id"],
            expression_id=request["expression_id"],
            final=request["final"],
            state="sent",
            observed_at=utc(self.clock()),
            segments=[
                dict(
                    segment_id=segment["segment_id"],
                    reply_id=segment["reply_id"],
                    segment_sequence=segment["segment_sequence"],
                    state="sent",
                    receipt_id="fixture:" + segment["segment_id"],
                    retry_safe=False,
                    channel_message_ids=["fixture:" + segment["segment_id"]],
                )
                for segment in request["segments"]
            ],
        )
        self.receipts[request["expression_id"]] = receipt
        return receipt

    async def query_expression(self, expression_id):
        return self.receipts.get(expression_id)

    async def finalize_expression(self, expression_id, origin):
        receipt = self.receipts[expression_id]
        receipt["final"] = True
        return receipt


@pytest.mark.parametrize("state", ["configured", "disabled", "unconfigured"])
def test_image_context_exposes_truthful_configuration_and_actor_isolation(tmp_path, state):
    async def run():
        h = Harness(silence_ms=0)
        try:
            turn = await source_turn(h)
            if state != "unconfigured":
                _, catalog = await configured(h, tmp_path)
                if state == "disabled":
                    await catalog.call(
                        "platform",
                        "manage",
                        dict(
                            schema_version=1,
                            request_id="disabled",
                            actor_id="actor:a",
                            operation="connection.configure",
                            expected_version=2,
                            value=dict(
                                base_url="http://127.0.0.1:8188", enabled=False, credential_ref=None
                            ),
                        ),
                    )
            context = h.core.skills.handlers["image.generate"].context(h.core.role_actions, turn)
            facts = context["capability"]
            assert facts["can_request"] == (state == "configured")
            assert facts["state"] == ("not_configured" if state == "unconfigured" else state)
            assert context["current_outfit"] is None
            assert "127.0.0.1" not in json.dumps(context) and "credential" not in json.dumps(
                context
            )
            other = copy.deepcopy(turn)
            other["scope"]["actor_id"] = "actor:b"
            assert (
                h.core.skills.handlers["image.generate"].context(h.core.role_actions, other)[
                    "capability"
                ]["configured"]
                is False
            )
            tools = h.core.role_actions.tools(turn)
            if state != "configured":
                assert not any(tool["function"]["name"] == "life_image_request" for tool in tools)
                return
            schema = next(
                tool["function"]["parameters"]
                for tool in h.core.role_actions.tools(turn)
                if tool["function"]["name"] == "life_image_request"
            )
            assert schema["required"] == ["value"]
            fields = schema["properties"]["value"]["properties"]
            assert "id" not in fields and "character" not in fields["intent"]["properties"]
            assert not schema["properties"]["value"].get("required")
        finally:
            await h.core.close()

    asyncio.run(run())


def test_current_outfit_is_real_data_and_per_image_choice_does_not_change_it(tmp_path):
    async def run():
        h = Harness(silence_ms=0)
        try:
            turn = await source_turn(h)
            await configured(h, tmp_path)
            h.core.gateway = h.core.life.gateway = SemanticModel()
            outfit = h.core.images.put_outfit(
                "outfit:actual", description="实际记录", prompt="blue daily dress"
            )
            h.core.life.configure_actor(
                "actor:a",
                h.core.store.get("life_actors", "actor:a")["room_id"],
                personality_version=1,
                schedule=[dict(minute=0, activity="resting", controls={})],
                outfit_ref=outfit["id"],
                expected=h.core.store.get("life_actors", "actor:a")["version"],
            )
            context = h.core.skills.handlers["image.generate"].context(h.core.role_actions, turn)
            assert context["current_outfit"]["prompt"] == "blue daily dress"
            tool = dict(
                id="new-photo",
                function=dict(
                    name="life_image_request",
                    arguments=json.dumps(
                        dict(value=dict(intent=dict(pose="sitting", camera="full body")))
                    ),
                ),
            )
            result, _ = await h.core.role_actions.execute(turn["id"], tool)
            assert result["state"] == "queued"
            assert h.core.images.get(result["id"])["outfit"]["id"] == outfit["id"]
            assert h.core.store.get("life_actors", "actor:a")["outfit_ref"] == outfit["id"]
            assert (
                "translated outfit"
                in h.core.images.get(result["id"])["graph"]["35"]["inputs"]["text"]
            )
        finally:
            await h.core.close()

    asyncio.run(run())


def test_ordinary_dialogue_compact_image_tool_delivers_original_once(tmp_path):
    async def run():
        h = Harness(tmp_path / "core.db", silence_ms=0)
        try:
            fixture, _ = await configured(h, tmp_path)
            queue = RecordingQueue(h.clock)
            h.core.sender = queue

            class Model(SemanticModel):
                def __init__(self):
                    super().__init__()
                    self.completions = []

                async def complete(self, turn, messages, *, tools=None, on_delta=None):
                    self.completions.append(copy.deepcopy(messages))
                    if len(self.completions) == 1:
                        assert "看看小汐今天的睡衣" in messages[1]["content"]
                        assert '"can_request":true' in messages[0]["content"]
                        assert '"current_outfit":null' in messages[0]["content"]
                        assert "modest sleepwear" in messages[0]["content"]
                        assert (
                            sum(t["function"]["name"] == "life_image_request" for t in tools) == 1
                        )
                        return dict(
                            role="assistant",
                            content=None,
                            tool_calls=[
                                dict(
                                    id="pajamas",
                                    type="function",
                                    function=dict(
                                        name="life_image_request",
                                        arguments=json.dumps(
                                            dict(
                                                value=dict(
                                                    intent=dict(
                                                        outfit="opaque cotton pajamas",
                                                        pose="standing by a window",
                                                        camera="full body",
                                                    )
                                                )
                                            )
                                        ),
                                    ),
                                )
                            ],
                        ), dict(fixture_only=True)
                    answer = "我给你看看这次选的睡衣。"
                    if on_delta:
                        await on_delta(answer)
                    return dict(role="assistant", content=answer), dict(fixture_only=True)

            model = Model()
            h.core.gateway = h.core.life.gateway = model
            await h.ingest(text="看看小汐今天的睡衣")
            for _ in range(150):
                await h.cycles(1)
                if h.core.store.list("image_jobs") and not h.core.jobs:
                    break
            jobs = h.core.store.list("image_jobs")
            assert len(jobs) == 1 and jobs[0]["state"] == "queued"
            assert len(model.completions) == 2 and len(model.calls) == 1
            turn = h.turns()[0]
            assert h.core.store.get("life_actors", "actor:a")["outfit_ref"] is None
            assert not any(
                segment["content_refs"]
                for request in queue.requests
                for segment in request["segments"]
            )
            await h.core.images.work()
            h.clock.advance(65)
            fixture.complete = True
            await h.core.images.work()
            queue.unavailable = True
            await h.core.images.work()
            blocked = next(
                row for row in h.core.store.list("metadata") if row.get("job_id") == jobs[0]["id"]
            )
            assert blocked["last_error"] == "dependency_unavailable" and not blocked.get("envelope")
            queue.unavailable = False
            h.clock.advance(31)
            await h.core.images.work()
            await h.core.images.work()
            media_requests = [
                request for request in queue.requests if request["segments"][0]["content_refs"]
            ]
            assert len(media_requests) == 1
            notice = [item for item in h.core.store.list("metadata") if item.get("state") == "sent"]
            assert any(row.get("turn_id") == turn["id"] for row in notice)
            assert media_requests[0]["scope"] == turn["scope"]
            assert len([call for call in fixture.calls if call == ("POST", "/prompt")]) == 1
            with pytest.raises(Fault):
                await h.core.role_actions.execute(
                    turn["id"],
                    dict(
                        id="bad-id",
                        function=dict(
                            name="life_image_request",
                            arguments=json.dumps(dict(value=dict(id="model-supplied"))),
                        ),
                    ),
                )
        finally:
            await h.core.close()

    asyncio.run(run())


def test_natural_acceptance_script_uses_auto_tools_and_does_not_submit_or_repeat(
    tmp_path, monkeypatch
):
    async def run():
        import argparse
        import importlib.util
        from pathlib import Path
        import os
        from tianshu_companion.image_backend import ImageBackend
        from test_comfy_deep import ComfyFixture

        script = Path(__file__).parents[1] / "scripts" / "comfy_dialogue_acceptance.py"
        spec = importlib.util.spec_from_file_location("dialogue_acceptance", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        fixture = ComfyFixture()

        async def resolve(self, value):
            return await fixture.resolve(value)

        class Model(SemanticModel):
            native_calls = 0

            def __init__(self, *args):
                super().__init__()

            async def complete(self, turn, messages, *, tools=None, on_delta=None):
                type(self).native_calls += 1
                if type(self).native_calls == 1:
                    assert "original persona exact text" in messages[0]["content"]
                    assert '"stage":"acquaintance"' in messages[1]["content"]
                    assert '"relationship_type":"unspecified"' in messages[1]["content"]
                    return dict(
                        role="assistant",
                        content=None,
                        tool_calls=[
                            dict(
                                id="actual-model-choice",
                                type="function",
                                function=dict(
                                    name="life_image_request",
                                    arguments=json.dumps(
                                        dict(
                                            value=dict(
                                                intent=dict(
                                                    outfit="opaque pajamas",
                                                    pose="standing naturally",
                                                )
                                            )
                                        )
                                    ),
                                ),
                            )
                        ],
                    ), dict(fixture_only=True)
                if on_delta:
                    await on_delta("这次选这件睡衣。")
                return dict(role="assistant", content="这次选这件睡衣。"), dict(fixture_only=True)

        private = tmp_path / "private-input.json"
        private.write_text(
            json.dumps(
                dict(
                    actor_id="actor:a",
                    role=dict(persona="original persona exact text", version=1),
                    relationship_background=dict(
                        view="private",
                        relationship_type="unspecified",
                        display_label="",
                        stage="acquaintance",
                        version=2,
                    ),
                    config_version=22,
                    request_text="看看小汐今天的睡衣",
                )
            ),
            encoding="utf-8",
        )
        settings = tmp_path / "settings.json"
        settings.write_text(
            json.dumps(
                dict(
                    services=dict(
                        gateway=dict(
                            url="https://registered-fixture", token_env="DIALOGUE_FIXTURE_TOKEN"
                        )
                    )
                )
            ),
            encoding="utf-8",
        )
        monkeypatch.setenv("DIALOGUE_FIXTURE_TOKEN", "synthetic-not-a-real-secret")
        monkeypatch.setattr(ImageBackend, "resolve", resolve)
        monkeypatch.setattr(module, "Gateway", Model)
        h = Harness()
        args = argparse.Namespace(
            contracts=os.environ["TIANSHU_CONTRACTS"],
            settings=str(settings),
            private_input=str(private),
            output=str(tmp_path / "output"),
            base_url="http://127.0.0.1:8188",
            workflow="synthetic.json",
            run_dialogue=True,
            generate=False,
            dialogue_timeout=5,
            generation_timeout=5,
        )
        await h.core.close()
        assert await module.run(args) == 0
        report = json.loads((tmp_path / "output" / "receipt.json").read_text(encoding="utf-8"))
        assert report["scenario_state"] == "image_requested"
        assert report["tool_calls"] == ["life_image_request"]
        assert report["relationship_snapshot_version"] == 2
        assert report["image_jobs"][0]["submitted"] is False
        assert not any(method == "POST" for method, _ in fixture.calls)
        assert await module.run(args) == 0
        assert Model.native_calls == 2
        assert not any(method == "POST" for method, _ in fixture.calls)

    asyncio.run(run())
