import asyncio

import httpx
import pytest

from support import Harness
from test_rate_limit_recovery import receipt
from tianshu_companion.clients import Gateway, JsonService
from tianshu_companion.contracts import Fault


def closed_attempt(request):
    request_id = request.url.path.split("/")[-2 if request.method == "POST" else -1]
    if request.method == "POST":
        return httpx.Response(
            200,
            json=dict(
                schema_version=1, request_id=request_id, state="terminal", upstream_outcome="failed"
            ),
        )
    return httpx.Response(200, json=receipt(request_id))


@pytest.mark.parametrize(
    "status,retryable",
    [(502, True), (503, True), (504, True), (401, False), (403, False), (429, False), (400, False)],
)
def test_only_transient_status_before_stream_is_eligible(status, retryable):
    async def run():
        h = Harness()

        def handler(request):
            if request.url.path == "/v1/chat/completions" or status == 429:
                return httpx.Response(status)
            return closed_attempt(request)

        client = JsonService(
            "https://test.invalid",
            "fixture",
            transport=httpx.MockTransport(handler),
        )
        try:
            gateway = Gateway(h.contracts, client)
            with pytest.raises(Fault) as caught:
                await gateway.complete(
                    dict(id="turn:test", config_version=7), [dict(role="user", content="test")]
                )
            assert caught.value.retryable_model is retryable
            assert not gateway.active_requests
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "delta",
    [
        {"content": "部分回复"},
        {
            "tool_calls": [
                {"index": 0, "id": "tool:test", "function": {"name": "test", "arguments": "{"}}
            ]
        },
    ],
)
def test_observed_stream_never_retries_even_if_later_fault_is_flagged(delta):
    async def run():
        h = Harness()

        class Client:
            async def stream(self, *args):
                yield {"choices": [{"index": 0, "delta": delta, "finish_reason": None}]}
                raise Fault("dependency_unavailable", retryable_model=True)

        try:
            gateway = Gateway(h.contracts, Client())
            with pytest.raises(Fault) as caught:
                await gateway.complete(
                    dict(id="turn:test", config_version=7), [dict(role="user", content="test")]
                )
            assert not caught.value.retryable_model
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("suffix", [b"", b"data: [DONE]\n\n"])
def test_reasoning_only_interruption_can_recover_before_any_answer_or_tool(suffix):
    async def run():
        h = Harness()
        body = (
            b'data: {"choices":[{"index":0,"delta":{"reasoning_content":"synthetic test"},"finish_reason":null}]}\n\n'
            + suffix
        )

        def handler(request):
            if request.url.path == "/v1/chat/completions":
                return httpx.Response(200, content=body)
            return closed_attempt(request)

        client = JsonService(
            "https://test.invalid",
            "fixture",
            transport=httpx.MockTransport(handler),
        )
        visible = []

        async def delta(text):
            visible.append(text)

        try:
            gateway = Gateway(h.contracts, client)
            with pytest.raises(Fault) as caught:
                await gateway.complete(
                    dict(id="turn:test", config_version=7),
                    [dict(role="user", content="test")],
                    on_delta=delta,
                )
            assert caught.value.retryable_model
            assert not visible and not gateway.active_requests
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


def test_explicit_model_length_limit_does_not_trigger_connection_retry():
    async def run():
        h = Harness()
        body = b'data: {"choices":[{"index":0,"delta":{"reasoning_content":"synthetic test"},"finish_reason":"length"}]}\n\ndata: [DONE]\n\n'
        client = JsonService(
            "https://test.invalid",
            "fixture",
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=body)),
        )
        try:
            with pytest.raises(Fault) as caught:
                await Gateway(h.contracts, client).complete(
                    dict(id="turn:test", config_version=7), [dict(role="user", content="test")]
                )
            assert not caught.value.retryable_model
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("field,retryable", [("reasoning_content", True), ("content", False)])
def test_transport_cut_distinguishes_hidden_reasoning_from_answer_text(field, retryable):
    import json

    async def run():
        h = Harness()

        class BrokenStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                chunk = {
                    "choices": [{"index": 0, "delta": {field: "synthetic"}, "finish_reason": None}]
                }
                yield ("data: " + json.dumps(chunk) + "\n\n").encode()
                raise httpx.RemoteProtocolError("Synthetic truncated stream")

        def handler(request):
            if request.url.path == "/v1/chat/completions":
                return httpx.Response(200, stream=BrokenStream())
            return closed_attempt(request)

        client = JsonService(
            "https://test.invalid",
            "fixture",
            transport=httpx.MockTransport(handler),
        )
        try:
            with pytest.raises(Fault) as caught:
                await Gateway(h.contracts, client).complete(
                    dict(id="turn:test", config_version=7), [dict(role="user", content="test")]
                )
            assert caught.value.retryable_model is retryable
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "mode,expected_calls,expected_phase",
    [
        ("recover", 2, "sent"),
        ("persistent", 2, "failed"),
        ("revoked", 1, "failed"),
        ("unknown", 1, "failed"),
    ],
)
def test_foreground_recovery_is_bounded_and_rechecks_authority(
    mode, expected_calls, expected_phase
):
    async def run():
        h = Harness(silence_ms=0)
        calls = []

        async def complete(turn, messages, **kwargs):
            calls.append(turn)
            if len(calls) == 1 or mode == "persistent":
                if mode == "revoked":
                    h.origins.values[turn["origin"]["assertion_ref"]]["revoked"] = True
                raise Fault(
                    "dependency_unavailable", unknown=True, retryable_model=mode != "unknown"
                )
            return dict(role="assistant", content="恢复后的回复"), dict(fixture_only=True)

        h.gateway.complete = complete
        try:
            await h.ingest(text="钥匙一直放门边")
            for _ in range(100):
                await h.cycles(5)
                if h.turns()[0]["phase"] in {"sent", "failed"}:
                    break
                await asyncio.sleep(0.01)
            assert h.turns()[0]["phase"] == expected_phase
            assert len(calls) == expected_calls
            assert h.turns()[0]["model_calls"] == expected_calls
            assert len(h.sender.calls) == (1 if mode == "recover" else 0)
            if mode == "recover":
                assert calls[0]["config_version"] == calls[1]["config_version"]
        finally:
            await h.core.close()

    asyncio.run(run())


def test_model_failure_after_tool_execution_does_not_replay_the_tool():
    async def run():
        h = Harness(silence_ms=0)
        calls, executed = [], []

        async def complete(turn, messages, **kwargs):
            calls.append(turn)
            if len(calls) > 1:
                raise Fault("dependency_unavailable", retryable_model=True)
            return dict(
                role="assistant",
                content=None,
                tool_calls=[dict(id="tool:once", function=dict(name="life_read", arguments="{}"))],
            ), {}

        async def execute(turn, call, **kwargs):
            executed.append(call)
            return dict(state="completed"), []

        h.gateway.complete = complete
        h.core.role_actions.execute = execute
        try:
            await h.ingest(text="翻一下刚才读的内容")
            await h.cycles(100)
            assert len(calls) == 2 and len(executed) == 1
            assert h.turns()[0]["phase"] == "failed"
            assert not h.sender.calls
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "mode", ["recover", "persistent", "revoked", "changed", "unknown", "forbidden", "cancelled"]
)
def test_source_preflight_rechecks_without_regenerating_a_completed_model_result(mode):
    from test_delivery_v2 import NativeQueue

    async def run():
        h = Harness(silence_ms=0)

        class SentQueue(NativeQueue):
            async def finalize_expression(self, expression_id, origin):
                self.final = True
                return self.receipt()

        queue = SentQueue(h.clock)
        queue.states = ["sent"]
        h.sender = h.core.sender = queue
        generated = False
        calls = []
        rechecks = []
        original = h.memory.select

        async def complete(turn, messages, *, on_delta=None, **kwargs):
            nonlocal generated
            generated = True
            calls.append(turn["id"])
            text = "已经生成的固定答案。"
            if on_delta:
                await on_delta(text)
            return dict(role="assistant", content=text), dict(fixture_only=True)

        async def select(origin, scope, text, budget, *args, **kwargs):
            if generated and budget == dict(tokens=0, bytes=0):
                rechecks.append(scope)
                if len(rechecks) <= (2 if mode == "recover" else 1) or mode == "persistent":
                    turn = h.turns()[0]
                    assert turn.get("native_execution")
                    if mode == "revoked":
                        h.origins.values[origin["assertion_ref"]]["revoked"] = True
                    if mode == "changed":
                        h.memory.scope_version += 1
                    if mode == "cancelled":
                        fresh = h.core.store.get("turns", turn["id"])
                        h.core._cancel_turn(fresh, "user_requested")
                    raise Fault(
                        "forbidden" if mode == "forbidden" else "dependency_unavailable",
                        retryable_read=mode != "forbidden",
                        unknown=mode == "unknown",
                    )
            return await original(origin, scope, text, budget, *args, **kwargs)

        h.gateway.complete = complete
        h.memory.select = select
        try:
            await h.ingest(text="我上午一般喝无糖豆浆")
            for _ in range(900):
                await h.cycles(3)
                if h.turns()[0]["phase"] in {"sent", "failed", "cancelled"}:
                    break
                await asyncio.sleep(0.01)
            turn = h.turns()[0]
            assert len(calls) == turn["model_calls"] == 1
            assert turn["phase"] == (
                "sent" if mode == "recover" else "cancelled" if mode == "cancelled" else "failed"
            )
            assert len(queue.requests) == (1 if mode == "recover" else 0)
            if mode == "recover":
                assert len(rechecks) == 3
                assert h.core._replies(turn)[0]["text"] == "已经生成的固定答案。"
            elif mode == "persistent":
                assert len(rechecks) == 6
                assert turn["failure_count"] == 6
            elif mode in {"unknown", "forbidden", "cancelled"}:
                assert len(rechecks) == 1
        finally:
            await h.core.close()

    asyncio.run(run())
