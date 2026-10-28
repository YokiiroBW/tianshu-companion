"""Expression ownership across cancellation/restart, with synthetic model/dispatch ports."""

import asyncio

import pytest

from test_proactive import Fixture, START, candidate, member
from tianshu_companion.contracts import Fault


async def motive(h):
    target = await member(h)
    h.core.proactive.register_motive(
        target.actor,
        dict(
            id="motive:fixture",
            subscription_id=target.subscription["id"],
            summary="合成活动完成",
            sources=[dict(owner="companion", object_id="fixture:event", version=1)],
            content_refs=[],
            due_at=START,
            expires_at=START + 3600,
            weight=1.0,
        ),
    )
    return h.core.store.get("proactive_candidates", candidate(h, "motive")["id"])


@pytest.mark.parametrize("cancel_candidate", [True, False])
def test_interrupted_expression_preserves_cancel_and_does_not_replay(cancel_candidate):
    async def run():
        h = Fixture()
        h.clock.now = START
        h.core.recover()
        task = None
        try:
            item = await motive(h)
            started, release = asyncio.Event(), asyncio.Event()
            calls = []

            async def expression(value):
                calls.append(value["id"])
                started.set()
                await release.wait()
                raise RuntimeError("synthetic lost model result")

            h.core.proactive.expression = expression
            task = asyncio.create_task(h.core.proactive.work())
            await started.wait()
            if cancel_candidate:
                h.core.proactive.cancel_candidate(item["id"], reason="operator_cancel")
                release.set()
                await task
            else:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            current = h.core.store.get("proactive_candidates", item["id"])
            assert current["state"] == ("cancelled" if cancel_candidate else "unknown")
            assert current["decision"] == (
                "operator_cancel" if cancel_candidate else "expression_unknown"
            )
            assert current.get("cancel_requested", False) == cancel_candidate
            assert current["expression_state"] == "unknown"
            await h.core.proactive.work()
            assert calls == [item["id"]]
            assert h.adapter.calls == []
        finally:
            if task and not task.done():
                task.cancel()
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("withdraw_at", ["generation", "dispatch", None])
def test_adopted_recipient_memory_is_rechecked_before_persist_and_send(withdraw_at):
    async def run():
        h = Fixture()
        h.clock.now = START
        h.core.recover()
        try:
            item = await motive(h)
            # The motive itself is owned locally; recipient memory is an independent input.
            item["sources"] = [dict(owner="companion", object_id=item["subject_id"], version=1)]
            h.core.proactive._save("candidates", item)
            scope = {
                key: item[key] for key in ("actor_id", "person_id", "audience", "conversation_id")
            }

            async def current(value):
                return dict(scope=scope, origin={})

            original_select = h.memory.select

            async def select(*args, **kwargs):
                result = await original_select(*args, **kwargs)
                if args[3]["bytes"]:
                    result["selected_units"] = [{"text": "synthetic recipient memory"}]
                return result

            class Model:
                available = True

                async def generate(self, turn, messages):
                    assert "synthetic recipient memory" in messages[1]["content"]
                    if withdraw_at == "generation":
                        h.memory.scope_version += 1
                    return ['{"send":true,"text":"synthetic expression"}'], {"fixture": True}

            execution = h.core.runtime_execution
            execution.current_context = current
            h.memory.select = select
            h.core.life.gateway = Model()
            if withdraw_at == "generation":
                with pytest.raises(Fault, match="scope_changed"):
                    await execution.proactive_expression(item)
                assert "memory_context_check" not in h.core.store.get(
                    "proactive_candidates", item["id"]
                )
            else:
                content, _ = await execution.proactive_expression(item)
                assert content == "synthetic expression"
                saved = h.core.store.get("proactive_candidates", item["id"])
                proof = saved["memory_context_check"]
                assert proof["scope"] == scope and proof["scope_version"] == 1
                assert "synthetic recipient memory" not in str(proof)
                if withdraw_at == "dispatch":
                    h.memory.scope_version += 1
                    with pytest.raises(Fault, match="scope_changed"):
                        await execution.dispatch({"candidate_id": item["id"]})
                else:
                    await execution.validate_sources(saved, {})
            assert h.adapter.calls == []
            assert all(row["budget"] == dict(tokens=0, bytes=0) for row in h.memory.selections[1:])
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("photo_pending", [False, True])
def test_persisted_expression_requires_explicit_retry_after_restart(tmp_path, photo_pending):
    async def run():
        h = Fixture(tmp_path / "core.db")
        h.clock.now = START
        h.core.recover()
        try:
            item = await motive(h)
            item.update(expression_state="generating", expression_attempts=1)
            if photo_pending:
                item.update(photo={"intent": {}}, photo_state="decided")
            h.core.proactive._save("candidates", item)
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            calls = []

            async def expression(value):
                calls.append(value["id"])
                return "合成表达", {"id": "synthetic:model"}

            h.core.proactive.expression = expression
            await h.core.proactive.work()
            current = h.core.store.get("proactive_candidates", item["id"])
            assert current["state"] == "unknown"
            assert current["expression_state"] == "unknown"
            assert calls == h.adapter.calls == []
            if photo_pending:
                return  # No photo job or expression was launched from the interrupted intent.
            h.core.proactive.retry_candidate(item["id"], reason="explicit_fixture_retry")
            await h.core.proactive.work()
            assert calls == [item["id"]]
            assert len(h.adapter.calls) == 1
        finally:
            await h.core.close()

    asyncio.run(run())
