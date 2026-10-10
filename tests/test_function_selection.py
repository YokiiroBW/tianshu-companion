"""Function selection uses the existing lease and never changes the gateway protocol."""

import asyncio
from types import SimpleNamespace

from test_life import setup
from tianshu_companion.model_selection import (
    HttpDefaultModelSelector,
    ModelSelection,
    SelectionRequest,
)


def test_chat_keeps_original_wire_shape_and_writing_identifies_its_function():
    async def scenario():
        sent = []

        async def call(path, body):
            sent.append(body)
            return dict(
                config_version=12,
                expires_at=9999999999,
                revoked=False,
                caller_service="companion",
                workload="companion.text",
            )

        selector = HttpDefaultModelSelector(
            SimpleNamespace(url="http://fixture", token="synthetic", call=call)
        )
        for function in ("chat", "writing"):
            await selector.select(
                SelectionRequest(
                    turn_id="turn:a",
                    actor_id="actor:a",
                    person_id="person:a",
                    audience="self_private",
                    conversation_id="conversation:a",
                    function_id=function,
                )
            )
        assert "function_id" not in sent[0]
        assert sent[1]["function_id"] == "writing"

    asyncio.run(scenario())


def test_diary_uses_writing_binding_without_static_config_and_retries_with_new_grant():
    async def scenario():
        life, clock, model = setup(version=None)
        requests = []

        class Selector:
            async def select(self, request):
                requests.append(request)
                return ModelSelection(42, clock() + 3600)

        life.model_selector = Selector()
        try:
            life.record_event("event", "world", "Fictional sun", participants=["a"])
            meta = life.request_diary("a", "2026-09-14")
            assert meta["state"] == "queued"
            model.fail = True
            await life.work()
            failed = life.diary_metadata(meta["id"])
            assert failed["state"] == "failed"
            life.retry_diary(meta["id"], expected=failed["version"])
            model.fail = False
            await life.work()
            assert life.diary_metadata(meta["id"])["state"] == "draft"
            assert len(requests) == 2
            assert all(request.function_id == "writing" for request in requests)
            assert requests[0].turn_id != requests[1].turn_id
            assert [turn["config_version"] for turn, _ in model.calls] == [42, 42]
            assert model.calls[1][0]["id"] == requests[1].turn_id
        finally:
            life.store.close()

    asyncio.run(scenario())
