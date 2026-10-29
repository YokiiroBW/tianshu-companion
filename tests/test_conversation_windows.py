"""Synthetic model/source ports exercise the real chat, persistence and runtime wiring."""

import asyncio
import copy
import json
from dataclasses import replace

import httpx
import pytest

from support import Harness
from tianshu_companion.app import create_app
from tianshu_companion.contracts import Fault
from tianshu_companion.conversation_windows import WindowPolicy
from tianshu_companion.conversation_window_compaction import compact
from tianshu_companion.model_slots import ModelSlots


class SummaryModel:
    def __init__(self, h):
        self.h = h
        self.summaries, self.chats = [], []
        self.output = "小林把周五改为周六，时间为14:30。先听他说完，暂不建议。"
        self.entered = asyncio.Event()
        self.release = None
        self.unknown = False

    async def complete(self, turn, messages, *, request_id=None, **kwargs):
        if "sequence" in turn:
            self.chats.append(copy.deepcopy(messages))
            return dict(role="assistant", content="收到，继续说。"), dict(
                fixture_only=True, usage=None
            )
        state = self.h.core.conversation_windows.state(self.h.turns()[-1])
        assert state["pending"]["request_id"] == request_id
        assert state["pending"]["state"] == "sent"
        self.summaries.append((copy.deepcopy(turn), copy.deepcopy(messages), request_id))
        self.entered.set()
        if self.release:
            await self.release.wait()
        if self.unknown:
            raise Fault("result_unknown", unknown=True)
        return dict(role="assistant", content=self.output), dict(
            request_id=request_id, outcome="succeeded"
        )

    async def receipt(self, turn, request_id):
        return dict(request_id=request_id, outcome="succeeded")


def setup(path=":memory:"):
    h = Harness(path, silence_ms=0)
    model = SummaryModel(h)
    h.gateway = h.core.gateway = h.core.life.gateway = model
    h.core.conversation_windows.policy = WindowPolicy(max_turns=128, max_messages=1024)
    return h, model


async def seed(h, count=7, **kwargs):
    for i in range(count):
        before = {t["id"] for t in h.turns()}
        await h.ingest(
            text=f"第{i}件事：小林约周六14:30，先听我说完，不要建议。" + "具体讨论材料。" * 12,
            **kwargs,
        )
        await h.cycles(60)
        latest = next(t for t in h.turns() if t["id"] not in before)
    assert all(t["phase"] == "sent" for t in h.turns())
    return latest


def small(owner, **kwargs):
    owner.policy = replace(owner.policy, max_turns=6, retain_turns=2, minimum_turns=2, **kwargs)


def test_real_chat_compacts_then_injects_summary_and_preserves_raw_and_counts():
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            before = [(t["id"], t["bundle"]) for t in h.turns()]
            small(owner)
            await h.ingest(text="接着刚才的话题")
            await h.cycles(100)
            assert len(model.summaries) == 1
            state = owner.state(h.turns()[-1])
            assert state["summary"] == model.output
            assert state["covered"] > 0
            prompt = json.loads(model.chats[-1][1]["content"])
            assert prompt["conversation_summary"][0]["text"] == model.output
            assert all(g["turn_sequence"] > state["covered"] for g in prompt["recent_dialogue"])
            assert [(t["id"], t["bundle"]) for t in h.turns()[:7]] == before
            old = copy.deepcopy(state)
            owner.observe(h.turns()[-1])
            assert owner.state(turn)["text_bytes"] == old["text_bytes"]
            # A fresh hard prefix bypasses the successful summary's 10-minute cooldown.
            await seed(h, 5)
            assert len(model.summaries) >= 2
            assert len(owner.state(turn)["summary"].encode()) <= owner.policy.summary_bytes
        finally:
            await h.core.close()

    asyncio.run(run())


def test_group_authors_share_window_but_other_groups_private_and_actor_do_not():
    async def run():
        h, _ = setup()
        try:
            a = await seed(h, 1, group=True, channel="g1", account="a")
            b = await seed(h, 1, group=True, channel="g1", account="b")
            c = await seed(h, 1, group=True, channel="g2", account="b")
            d = await seed(h, 1, group=False, channel="g1", account="b")
            e = await seed(h, 1, group=True, channel="g1", account="b", actor="actor:b")
            owner = h.core.conversation_windows
            assert owner.identity(a)[0] == owner.identity(b)[0]
            assert len({owner.identity(t)[0] for t in [a, c, d, e]}) == 4
        finally:
            await h.core.close()

    asyncio.run(run())


def test_idle_soft_age_minimum_and_collecting_input_yield():
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            owner.policy = replace(
                owner.policy, retain_turns=2, minimum_turns=2, soft_percent=10, hard_percent=99
            )
            state = owner.state(turn)
            assert owner.reason(state, idle=True) is None
            h.clock.advance(181)
            assert owner.reason(state, idle=True) == "idle_high_water"
            h.core.policy = replace(h.core.policy, silence_ms=5000)
            await h.ingest(text="尚未封盘的新消息")
            assert not owner.idle_ready(state)
            await owner.work(wait=True)
            assert not model.summaries
            h.clock.advance(6)
            await h.cycles(60)
            h.clock.advance(181)
            await owner.work(wait=True)
            assert len(model.summaries) == 1
            state = owner.state(turn)
            assert owner.reason(state, idle=True) is None
            # Age alone never requests a model without enough new old content.
            h.clock.advance(4000)
            assert owner.reason(state, idle=True) is None
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("output", ["", "x" * 4000])
def test_invalid_summary_keeps_old_range_and_does_not_repeat_each_tick(output):
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            model.output = output
            await owner.before_turn(turn)
            await owner.before_turn(turn)
            state = owner.state(turn)
            assert len(model.summaries) == 1
            assert state["covered"] == 0 and state["summary"] is None
            assert state["turns"] == 7
        finally:
            await h.core.close()

    asyncio.run(run())


def test_tail_append_survives_inflight_summary():
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            model.release = asyncio.Event()
            task = asyncio.create_task(owner.before_turn(turn))
            await model.entered.wait()
            await h.ingest(text="这是摘要期间正常追加的新尾部")
            await h.cycles(60)
            model.release.set()
            await task
            state = owner.state(turn)
            assert state["covered"] == 5 and state["turns"] == 3
            assert h.turns()[-1]["sequence"] > state["covered"]
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("change", ["clear", "revision"])
def test_prefix_change_rejects_late_result(change):
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            model.release = asyncio.Event()
            task = asyncio.create_task(owner.before_turn(turn))
            await asyncio.wait_for(model.entered.wait(), 5)
            state = owner.state(turn)
            if change == "clear":
                owner.manage(
                    "platform", dict(schema_version=1, operation="clear", scope=state["scope"])
                )
            else:
                from tianshu_companion.source_sync import invalidate_physical, physical_key
                from tianshu_companion.contracts import digest

                first = h.turns()[0]
                conv = h.core.store.conversation(first["conversation_id"])
                invalidate_physical(
                    h.core, conv, digest(physical_key(first["bundle"]["messages"][0])), "edit"
                )
            model.release.set()
            await task
            fresh = owner.state(turn)
            assert fresh["summary"] is None and fresh["pending"] is None
            assert fresh["generation"] > state["generation"]
        finally:
            await h.core.close()

    asyncio.run(run())


def test_received_result_survives_restart_without_second_model_call(tmp_path, monkeypatch):
    async def run():
        h, model = setup(str(tmp_path / "core.db"))
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            import tianshu_companion.conversation_window_compaction as module

            actual = module._commit

            async def interrupted(*args):
                raise RuntimeError("synthetic process exit after result persistence")

            monkeypatch.setattr(module, "_commit", interrupted)
            with pytest.raises(RuntimeError):
                await owner.before_turn(turn)
            assert owner.state(turn)["pending"]["state"] == "result"
            await h.core.close()
            h.core = h.new_core(conversation_window_policy=owner.policy)
            monkeypatch.setattr(module, "_commit", actual)
            h.core.recover()
            await h.core.conversation_windows.work(wait=True)
            assert h.core.conversation_windows.state(turn)["summary"] == model.output
            execution = h.core.conversation_windows.state(turn)["last_execution"]
            assert execution["request_id"] == model.summaries[0][2]
            assert execution["receipt"]["outcome"] == "succeeded"
            assert len(model.summaries) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_idle_waiting_for_slot_yields_when_collecting_input_arrives():
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            h.clock.advance(181)
            h.core.models = ModelSlots(1)
            await h.core.models.acquire()
            task = asyncio.create_task(compact(owner, turn, idle=True))
            for _ in range(60):
                await asyncio.sleep(0)
                if owner.state(turn)["pending"]:
                    break
            assert owner.state(turn)["pending"]["state"] == "prepared"
            h.core.policy = replace(h.core.policy, silence_ms=5000)
            await h.ingest(text="前台新消息尚在收件")
            h.core.models.release()
            await task
            assert not model.summaries
            assert owner.state(turn)["pending"] is None
        finally:
            await h.core.close()

    asyncio.run(run())


def test_history_forget_rejects_old_summary_before_next_model_request():
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            await owner.before_turn(turn)
            assert owner.state(turn)["summary"]
            h.memory.history_version = 2  # The actual Memory history contract's withdrawal epoch.
            await h.ingest(text="继续聊")
            await h.cycles(100)
            assert not json.loads(model.chats[-1][1]["content"]).get("conversation_summary")
            assert owner.state(turn)["summary"] is None
        finally:
            await h.core.close()

    asyncio.run(run())


def test_unknown_execution_recovery_never_claims_text_or_blindly_replays(tmp_path):
    async def run():
        h, model = setup(str(tmp_path / "core.db"))
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            model.unknown = True
            await owner.before_turn(turn)
            assert owner.state(turn)["pending"]["state"] == "unknown"
            await h.core.close()
            h.core = h.new_core(conversation_window_policy=owner.policy)
            h.core.recover()
            owner = h.core.conversation_windows
            await owner.before_turn(turn)
            assert len(model.summaries) == 1
            h.clock.advance(601)
            await owner.work(wait=True)
            state = owner.state(turn)
            assert state["summary"] is None and state["covered"] == 0
            assert state["last_error"] == "execution_finished_without_persisted_text"
            assert len(model.summaries) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_management_auth_scope_status_and_clear():
    async def run():
        h, _ = setup()
        try:
            turn = await seed(h, 1)
            scope = h.core.conversation_windows.state(turn)["scope"]
            body = dict(schema_version=1, operation="status", scope=scope)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(
                    app=create_app(h.core, {"platform": "p", "nonebot": "n"})
                ),
                base_url="http://fixture",
            ) as client:
                assert (
                    await client.post("/internal/v1/conversation/window", json=body)
                ).status_code == 401
                assert (
                    await client.post(
                        "/internal/v1/conversation/window",
                        json=body,
                        headers={"Authorization": "Bearer n"},
                    )
                ).status_code == 403
                response = await client.post(
                    "/internal/v1/conversation/window",
                    json=body,
                    headers={"Authorization": "Bearer p"},
                )
                assert response.status_code == 200
                assert "summary" not in response.json()
                response = await client.post(
                    "/internal/v1/conversation/window",
                    json={**body, "operation": "clear"},
                    headers={"Authorization": "Bearer p"},
                )
                assert response.json()["generation"] == 2
                bad = {**body, "scope": {**scope, "person_id": "person:other"}}
                assert (
                    await client.post(
                        "/internal/v1/conversation/window",
                        json=bad,
                        headers={"Authorization": "Bearer p"},
                    )
                ).status_code == 400
        finally:
            await h.core.close()

    asyncio.run(run())


def test_single_slot_hard_compaction_has_no_nested_acquisition():
    async def run():
        h, model = setup()
        try:
            await seed(h)
            h.core.models = ModelSlots(1)
            small(h.core.conversation_windows)
            await h.ingest(text="触发实际前台硬压缩")
            await asyncio.wait_for(h.cycles(100), 5)
            assert len(model.summaries) == 1
            assert h.turns()[-1]["phase"] == "sent"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_runtime_configuration_and_background_worker_are_actually_connected(tmp_path):
    async def run():
        from test_host_assembly import configuration
        from tianshu_companion.app import build_runtime

        config = configuration(tmp_path)
        config["conversation_window"] = dict(idle_seconds=77, summary_bytes=2048)
        core, tokens, clients, _ = build_runtime(config)
        called = asyncio.Event()

        async def work(*, wait=False):
            assert wait is False
            called.set()

        core.conversation_windows.work = work
        app = create_app(core, tokens)
        try:
            assert core.conversation_windows.policy.idle_seconds == 77
            assert core.conversation_windows.policy.summary_bytes == 2048
            async with app.router.lifespan_context(app):
                await asyncio.wait_for(called.wait(), 2)
        finally:
            for client in clients:
                await client.close()

    asyncio.run(run())


def test_gateway_uses_preallocated_id_and_validates_receipt_identity():
    async def run():
        from support import native_sse
        from tianshu_companion.clients import Gateway, JsonService, utc

        h, _ = setup()
        seen = []
        wrong = False

        def handler(request):
            seen.append(request)
            if request.url.path == "/v1/chat/completions":
                return httpx.Response(
                    200,
                    json=dict(
                        choices=[
                            dict(
                                message=dict(role="assistant", content="摘要"), finish_reason="stop"
                            )
                        ]
                    ),
                )
            return httpx.Response(
                200,
                json=dict(
                    schema_version=1,
                    request_id="model:wrong" if wrong else "model:durable",
                    config_version=1,
                    provider_id="provider:fixture",
                    requested_model=None,
                    resolved_model="fixture",
                    protocol="openai-chat-completions",
                    outcome="succeeded",
                    upstream_request_id=None,
                    usage=None,
                    fallback_used=False,
                    observed_at=utc(h.clock()),
                    credential_namespace="fixture",
                    caller_service="companion",
                    requested_reasoning={},
                    effective_reasoning={},
                    applied_policies=[],
                    usage_complete=False,
                    native_usage=None,
                ),
            )

        client = JsonService(
            "https://fixture.invalid", "fixture", transport=httpx.MockTransport(native_sse(handler))
        )
        try:
            gateway = Gateway(h.contracts, client)
            task = dict(id="window:fixture", config_version=1)
            message, receipt = await gateway.complete(
                task, [dict(role="user", content="hi")], request_id="model:durable"
            )
            assert message["content"] == "摘要"
            assert seen[0].headers["X-Request-ID"] == "model:durable"
            assert seen[1].url.path.endswith("model:durable")
            assert await gateway.receipt(task, "model:durable") == receipt
            wrong = True
            with pytest.raises(Fault):
                await gateway.receipt(task, "model:durable")
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


def test_generate_only_client_degrades_summary_without_failing_chat():
    async def run():
        h = Harness(silence_ms=0)
        try:
            await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            await h.ingest(text="旧嵌入式客户端仍可以继续聊天")
            await h.cycles(60)
            assert h.turns()[-1]["phase"] == "sent"
            assert owner.state(h.turns()[-1])["last_error"] == "dependency_unavailable"
            assert owner.state(h.turns()[-1])["covered"] == 0
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("phase", ["unknown", "result"])
def test_recovery_read_failure_retains_execution_or_text_and_backs_off(phase, monkeypatch):
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            import tianshu_companion.conversation_window_compaction as module

            actual_commit = module._commit

            async def unavailable(*args):
                raise Fault("dependency_unavailable")

            if phase == "unknown":
                model.unknown = True
            else:
                monkeypatch.setattr(module, "_commit", unavailable)
            await owner.before_turn(turn)
            pending = owner.state(turn)["pending"]
            identity = pending["request_id"]
            assert pending["state"] == phase
            h.clock.advance(601)
            real_receipt = model.receipt
            model.receipt = unavailable
            await compact(owner, turn, idle=False)
            fresh = owner.state(turn)["pending"]
            assert fresh["state"] == phase and fresh["request_id"] == identity
            assert fresh["retry_at"] > h.clock()
            await owner.before_turn(turn)
            assert len(model.summaries) == 1
            model.receipt = real_receipt
            monkeypatch.setattr(module, "_commit", actual_commit)
            h.clock.advance(601)
            await compact(owner, turn, idle=False)
            state = owner.state(turn)
            assert state["pending"] is None
            assert bool(state["summary"]) is (phase == "result")
            assert len(model.summaries) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("checkpoint", ["selection", "verification"])
def test_idle_yields_if_front_input_arrives_during_final_preflight(checkpoint, monkeypatch):
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            h.clock.advance(181)
            entered, release = asyncio.Event(), asyncio.Event()
            if checkpoint == "selection":
                from tianshu_companion.model_selection import ModelSelection

                class Selector:
                    async def select(self, request):
                        entered.set()
                        await release.wait()
                        return ModelSelection(1, h.clock() + 3600)

                h.core.life.model_selector = Selector()
            else:
                verify = owner.verify_materials

                async def delayed(*args, **kwargs):
                    pending = owner.state(turn)["pending"]
                    if pending and pending["state"] == "prepared":
                        entered.set()
                        await release.wait()
                    return await verify(*args, **kwargs)

                monkeypatch.setattr(owner, "verify_materials", delayed)
            task = asyncio.create_task(compact(owner, turn, idle=True))
            await asyncio.wait_for(entered.wait(), 5)
            h.core.policy = replace(h.core.policy, silence_ms=5000)
            await h.ingest(text="在摘要最终核验期间到达的前台消息")
            release.set()
            await task
            assert not model.summaries and owner.state(turn)["pending"] is None
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("change", ["clear", "append"])
def test_receipt_recovery_cannot_overwrite_clear_or_new_tail(change):
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            model.unknown = True
            await owner.before_turn(turn)
            h.clock.advance(601)
            entered, release = asyncio.Event(), asyncio.Event()
            receipt = model.receipt

            async def delayed(*args):
                entered.set()
                await release.wait()
                return await receipt(*args)

            model.receipt = delayed
            task = asyncio.create_task(compact(owner, turn, idle=False))
            await asyncio.wait_for(entered.wait(), 5)
            if change == "clear":
                owner.manage(
                    "platform",
                    dict(schema_version=1, operation="clear", scope=owner.state(turn)["scope"]),
                )
            else:
                await h.ingest(text="回执查询期间的新尾部")
                await h.cycles(60)
            before = owner.state(turn)
            release.set()
            await task
            after = owner.state(turn)
            assert after["generation"] == before["generation"]
            assert after["turns"] == before["turns"]
            assert after["text_bytes"] == before["text_bytes"]
            assert after["pending"] is None and after["summary"] is None
            assert len(model.summaries) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_bootstrap_pages_from_oldest_without_skipping_the_first_128_turns():
    async def run():
        h, _ = setup()
        try:
            turn = await seed(h, 1)
            owner = h.core.conversation_windows
            # Stored synthetic history; do not simulate 140 provider requests to test a cursor.
            with h.core.store.transaction():
                for sequence in range(2, 141):
                    old = copy.deepcopy(turn)
                    old.update(id=f"turn:fixture-{sequence}", sequence=sequence)
                    h.core.store.put("turns", old)
            owner.reset(owner.state(turn), reason="fixture_rebuild")
            for _ in range(5):
                owner.bootstrap(turn)
            state = owner.state(turn)
            assert state["bootstrapped"] and state["scan"] == 140 and state["turns"] == 140
            assert owner.members(state)[0]["sequence"] == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_complete_fragment_chain_can_move_but_never_a_half_chain():
    async def run():
        h, model = setup()
        try:
            h.core.policy = replace(h.core.policy, silence_ms=5000, max_wait_ms=1)
            await h.ingest(text="第一个未说完片段" * 30)
            h.clock.advance(0.01)
            await h.cycles(60)
            assert h.turns()[0]["phase"] == "observed"
            h.core.policy = replace(h.core.policy, silence_ms=0, max_wait_ms=None)
            for _ in range(5):
                await h.ingest(text="接续完整的内容" * 30)
                await h.cycles(60)
            owner = h.core.conversation_windows
            small(owner)
            await owner.before_turn(h.turns()[-1])
            assert len(model.summaries) == 1
            material = json.loads(model.summaries[0][1][1]["content"])["conversation"]
            assert [v["sequence"] for v in material][:2] == [1, 2]
            assert owner.state(h.turns()[-1])["covered"] >= 2
        finally:
            await h.core.close()

    asyncio.run(run())
