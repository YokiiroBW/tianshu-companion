"""GSCore wire fixtures, not the currently disabled/incomplete resident HTTP server."""

import asyncio
import base64
import copy
import json

import httpx
import pytest

from support import Harness
from test_delivery_v2 import source_turn
from test_image_dialogue import RecordingQueue
from test_images import png
from test_skills import manage
from tianshu_companion.contracts import Fault
from tianshu_companion.core import TERMINAL
from tianshu_companion.skills.gscore import GSCoreSkill


async def configured_game(h):
    h.core.recover()
    definition = copy.deepcopy(h.core.skills.definitions("actor:a")["game.guides"])
    definition.pop("source_id")
    await manage(
        h,
        "skill.update",
        dict(
            definition=definition,
            enabled=True,
            config=dict(
                provider="gscore",
                base_url="http://fixture.invalid:28765",
                credential_ref="fixture:gscore",
                options={},
            ),
        ),
    )

    class Credential:
        async def call(self, path, request):
            assert request["purpose"] == "companion.skills"
            assert request["audience"] == "http://fixture.invalid:28765"
            return dict(
                schema_version=1,
                request_id=request["request_id"],
                credential_ref=request["credential_ref"],
                token="synthetic-only",
            )

    h.core.image_backend.credentials = Credential()


def test_semantic_whitelist_and_no_raw_or_fake_solver():
    skill = GSCoreSkill()
    assert (
        skill.command(dict(game="wuthering_waves", operation="guide", character="长离"))
        == "ww长离攻略"
    )
    assert (
        skill.command(dict(game="wuthering_waves", operation="team_statistics")) == "ww矩阵高分队"
    )
    assert skill.command(dict(game="nte", operation="team", character="角色甲")) == "nte角色甲配队"
    with pytest.raises(Fault):
        skill.command(dict(game="wuthering_waves", operation="team", character="长离"))
    with pytest.raises(Fault):
        skill.command(dict(game="nte", operation="guide", character="角色\n管理操作"))
    with pytest.raises(Fault):
        skill.command(dict(raw_command="任意指令"))


def test_natural_game_query_received_png_same_delivery_no_generated_job(tmp_path, monkeypatch):
    async def run():
        h = Harness(tmp_path / "core.db", silence_ms=0)
        try:
            await configured_game(h)
            h.core.images.staging = tmp_path / "originals"
            queue = RecordingQueue(h.clock)
            h.core.sender = queue
            requests = []

            async def request(config, token, body):
                requests.append(copy.deepcopy(body))
                assert token == "synthetic-only"
                assert body["content"] == [dict(type="text", data="ww长离攻略")]
                assert body["user_type"] == "direct" and body["group_id"] is None
                return dict(
                    status_code=200,
                    data=dict(
                        bot_id=body["bot_id"],
                        bot_self_id="",
                        msg_id=body["msg_id"],
                        target_type="direct",
                        target_id=body["user_id"],
                        echo=None,
                        content=[
                            dict(type="image", data="base64://" + base64.b64encode(png()).decode())
                        ],
                    ),
                )

            monkeypatch.setattr("tianshu_companion.skills.gscore.request", request)

            class Model:
                calls = 0

                async def complete(self, turn, messages, *, tools=None, on_delta=None):
                    self.calls += 1
                    if self.calls == 1:
                        assert "长离攻略" in messages[1]["content"]
                        offered = next(
                            tool for tool in tools if tool["function"]["name"] == "game_query"
                        )
                        assert "raw_command" not in json.dumps(offered)
                        assert "fixture.invalid" not in messages[0]["content"]
                        return dict(
                            role="assistant",
                            content=None,
                            tool_calls=[
                                dict(
                                    id="guide",
                                    type="function",
                                    function=dict(
                                        name="game_query",
                                        arguments=json.dumps(
                                            dict(
                                                game="wuthering_waves",
                                                operation="guide",
                                                character="长离",
                                            )
                                        ),
                                    ),
                                )
                            ],
                        ), dict(fixture_only=True)
                    result = json.loads(
                        next(
                            message["content"] for message in messages if message["role"] == "tool"
                        )
                    )
                    assert result["state"] == "unknown" and result["result"]["complete"] is False
                    answer = "收到一张攻略图，完整性还不能确认。"
                    if on_delta:
                        await on_delta(answer)
                    return dict(role="assistant", content=answer), dict(fixture_only=True)

            h.core.gateway = Model()
            await h.ingest(text="看看长离攻略")
            for _ in range(100):
                await h.cycles(1)
                if h.turns() and h.turns()[0]["phase"] in TERMINAL and not h.core.jobs:
                    break
            turn = h.turns()[0]
            assert turn["phase"] == "sent"
            media = h.core.store.list("image_media")
            assert len(media) == 1 and media[0]["origin_kind"] == "external_query"
            assert media[0]["scope"] == turn["scope"] and media[0]["source"]["provider"] == "gscore"
            assert h.core.store.list("image_jobs") == [] and h.core.store.list("life_album") == []
            outbound = [
                request for request in queue.requests if request["segments"][0]["content_refs"]
            ]
            assert len(outbound) == 1 and outbound[0]["scope"] == turn["scope"]
            data, original = h.core.life.album.read("actor:a", media[0]["id"], turn["scope"])
            assert data == png() and original["content_ref"] == media[0]["content_ref"]
            other = dict(turn["scope"], conversation_id="other")
            with pytest.raises(Fault):
                h.core.life.album.read("actor:a", media[0]["id"], other)
            # Even a new model tool-call ID cannot repeat the same uncertain query.
            result, _ = await h.core.role_actions.execute(
                turn["id"],
                dict(
                    id="another",
                    function=dict(
                        name="game_query",
                        arguments=json.dumps(
                            dict(game="wuthering_waves", operation="guide", character="长离")
                        ),
                    ),
                ),
            )
            assert result["state"] == "unknown" and len(requests) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "mode", ["timeout", "no_result", "remote_file", "mismatched_target", "invalid_fragment"]
)
def test_unknown_and_unsupported_outputs_are_not_retried(mode, monkeypatch):
    async def run():
        h = Harness(silence_ms=0)
        try:
            turn = await source_turn(h)
            await configured_game(h)
            calls = []

            async def request(config, token, body):
                calls.append(body)
                if mode == "timeout":
                    raise httpx.ReadTimeout("synthetic")
                if mode == "no_result":
                    return dict(status_code=-100, data=None)
                return dict(
                    status_code=200,
                    data=dict(
                        bot_id=body["bot_id"],
                        bot_self_id="",
                        msg_id=body["msg_id"],
                        target_type="direct",
                        target_id="foreign" if mode == "mismatched_target" else body["user_id"],
                        content=[None]
                        if mode == "invalid_fragment"
                        else [dict(type="image", data="file:///private/server/path.png")],
                        echo=None,
                    ),
                )

            monkeypatch.setattr("tianshu_companion.skills.gscore.request", request)
            query = dict(
                id="one",
                function=dict(
                    name="game_query",
                    arguments=json.dumps(dict(game="nte", operation="guide", character="角色甲")),
                ),
            )
            result, _ = await h.core.role_actions.execute(turn["id"], query)
            assert result["state"] == "unknown" and not result["complete"]
            assert (
                result["error_code"]
                == {
                    "timeout": "timeout",
                    "no_result": "no_confirmed_output",
                    "remote_file": "unsupported_content",
                    "mismatched_target": "scope_changed",
                    "invalid_fragment": "invalid_input",
                }[mode]
            )
            assert "/private/server" not in json.dumps(result)
            assert not result["content_refs"] and not h.core.store.list("image_media")
            assert (await h.core.role_actions.execute(turn["id"], dict(query, id="two")))[
                0
            ] == result
            assert len(calls) == 1
        finally:
            await h.core.close()

    asyncio.run(run())
