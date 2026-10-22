"""Empty native replies and closing the old request, with no real model or channel."""

import asyncio
import json

import httpx
import pytest

from support import Harness
from test_rate_limit_recovery import FinishedQueue, chunk, event, receipt
from tianshu_companion import observability as obs
from tianshu_companion.clients import Gateway, JsonService
from tianshu_companion.contracts import Fault


def owned_receipt(request_id, state="completed", version=7):
    value = receipt(request_id)
    value["config_version"] = version
    value["outcome"] = {
        "completed": "succeeded",
        "failed": "failed",
    }.get(state, "unknown")
    value["execution"].update(
        state=state,
        cancel_requested=state == "cancelled",
        finish_reasons=["stop"] if state == "completed" else [],
        error_code=None,
    )
    return value


def cancel_response(request_id, state="requested"):
    return dict(schema_version=1, request_id=request_id, state=state, upstream_outcome="unknown")


@pytest.mark.parametrize(
    "mode,valid",
    [
        ("empty", True),
        ("none", True),
        ("whitespace", True),
        ("tool", True),
        ("wrong_id", False),
        ("wrong_version", False),
        ("wrong_caller", False),
    ],
)
def test_native_assembly_preserves_empty_or_tool_messages_and_owned_receipt_checks(mode, valid):
    async def run():
        h = Harness()
        requests = []

        def handler(request):
            requests.append(request.url.path)
            if request.url.path == "/v1/chat/completions":
                delta = {"content": "" if mode == "empty" else None}
                finish = "stop"
                if mode == "whitespace":
                    delta = {"content": " \n\t "}
                elif mode == "tool":
                    delta = {
                        "tool_calls": [
                            dict(
                                index=0,
                                id="tool:one",
                                function=dict(name="life_read", arguments="{}"),
                            )
                        ]
                    }
                    finish = "tool_calls"
                body = event(chunk(delta)) + event(chunk({}, finish)) + "data: [DONE]\n\n"
                return httpx.Response(200, text=body)
            assert request.method == "GET", "completed empty replies must not be cancelled"
            request_id = request.url.path.rsplit("/", 1)[-1]
            value = owned_receipt(request_id)
            if mode == "wrong_id":
                value["request_id"] = "model:" + "b" * 32
            elif mode == "wrong_version":
                value["config_version"] += 1
            elif mode == "wrong_caller":
                value["caller_service"] = "external"
            return httpx.Response(200, json=value)

        client = JsonService(
            "https://fixture.invalid", "secret", transport=httpx.MockTransport(handler)
        )
        gateway = Gateway(h.contracts, client)
        try:
            call = gateway.complete(
                dict(id="turn:test", config_version=7), [dict(role="user", content="test")]
            )
            if valid:
                message, _ = await call
                if mode == "tool":
                    assert message["content"] is None and len(message["tool_calls"]) == 1
                elif mode == "whitespace":
                    assert message["content"] == " \n\t "
                else:
                    assert message["content"] is None
            else:
                with pytest.raises(Fault) as caught:
                    await call
                assert not caught.value.retryable_model
                assert caught.value.model_request_id.startswith("model:")
            assert len(requests) == 2 and not gateway.active_requests
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "mode,can_retry",
    [
        ("cancelled", True),
        ("completed", True),
        ("failed", True),
        ("running", False),
        ("unknown", False),
        ("not_active", False),
        ("wrong_cancel_id", False),
        ("wrong_id", False),
        ("wrong_version", False),
        ("wrong_caller", False),
        ("missing_receipt", False),
        ("slow_cancel", False),
        ("slow_receipt", False),
    ],
)
def test_output_free_interruption_requires_owned_terminal_after_cancellation(mode, can_retry):
    async def run():
        h = Harness()
        calls, closed, reads = [], [], []

        class Cut(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield event(chunk({"reasoning_content": "synthetic"})).encode()
                raise httpx.ReadTimeout("secret body and URL must not be retained")

            async def aclose(self):
                closed.append(True)

        async def handler(request):
            calls.append((request.method, request.url.path))
            if request.url.path == "/v1/chat/completions":
                return httpx.Response(200, stream=Cut())
            assert closed, "transport must have closed before cancel/receipt"
            request_id = request.url.path.split("/")[-2 if request.method == "POST" else -1]
            if request.method == "POST":
                if mode == "slow_cancel":
                    await asyncio.sleep(5)
                response = cancel_response(
                    request_id, "not_active" if mode == "not_active" else "requested"
                )
                if mode == "wrong_cancel_id":
                    response["request_id"] = "model:" + "b" * 32
                return httpx.Response(200, json=response)
            reads.append(True)
            if mode == "slow_receipt":
                await asyncio.sleep(5)
            if mode == "missing_receipt":
                return httpx.Response(404, json={})
            state = (
                "running"
                if mode == "running" or (mode == "cancelled" and len(reads) == 1)
                else mode
            )
            if state not in {"running", "cancelled", "completed", "failed", "unknown"}:
                state = "cancelled"
            value = owned_receipt(request_id, state)
            if mode == "wrong_id":
                value["request_id"] = "model:" + "b" * 32
            elif mode == "wrong_version":
                value["config_version"] += 1
            elif mode == "wrong_caller":
                value["caller_service"] = "external"
            return httpx.Response(200, json=value)

        client = JsonService(
            "https://fixture.invalid",
            "secret",
            label="gateway",
            transport=httpx.MockTransport(handler),
        )
        gateway = Gateway(h.contracts, client)
        try:
            with pytest.raises(Fault) as caught:
                await asyncio.wait_for(
                    gateway.complete(
                        dict(id="turn:test", config_version=7), [dict(role="user", content="test")]
                    ),
                    2,
                )
            failure = caught.value
            assert failure.retryable_model is can_retry
            assert failure.unknown
            assert failure.cause["exception_type"] == "ReadTimeout"
            assert failure.cause["peer"] == "gateway"
            assert failure.model_request_id.startswith("model:")
            assert calls[1][0] == "POST" and calls[1][1].endswith("/cancel")
            assert sum(path == "/v1/chat/completions" for _, path in calls) == 1
            if mode == "cancelled":
                assert len(reads) == 2
            assert not gateway.active_requests
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


def test_external_cancel_during_old_request_settlement_stays_cancellation():
    async def run():
        h = Harness()
        entered = asyncio.Event()
        calls = []

        class Cut(httpx.AsyncByteStream):
            async def __aiter__(self):
                raise httpx.ReadTimeout("synthetic")
                yield  # pragma: no cover

        async def handler(request):
            calls.append(request.url.path)
            if request.url.path == "/v1/chat/completions":
                return httpx.Response(200, stream=Cut())
            if request.method == "POST":
                return httpx.Response(200, json=cancel_response(request.url.path.split("/")[-2]))
            entered.set()
            await asyncio.sleep(5)
            return httpx.Response(
                200, json=owned_receipt(request.url.path.rsplit("/", 1)[-1], "cancelled")
            )

        client = JsonService(
            "https://fixture.invalid", "secret", transport=httpx.MockTransport(handler)
        )
        gateway = Gateway(h.contracts, client)
        try:
            task = asyncio.create_task(
                gateway.complete(
                    dict(id="turn:test", config_version=7), [dict(role="user", content="test")]
                )
            )
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert calls.count("/v1/chat/completions") == 1
            assert not gateway.active_requests
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("cleanup_stage", ["cancel", "receipt"])
def test_second_cancellation_interrupts_cleanup_without_a_shielded_request_task(cleanup_stage):
    async def run():
        h = Harness()
        streaming, cleaning = asyncio.Event(), asyncio.Event()
        active_calls, handler_tasks, requests = set(), set(), []

        class Waiting(httpx.AsyncByteStream):
            async def __aiter__(self):
                streaming.set()
                await asyncio.Event().wait()
                yield  # pragma: no cover

        async def handler(request):
            requests.append(request.url.path)
            if request.url.path == "/v1/chat/completions":
                return httpx.Response(200, stream=Waiting())
            handler_tasks.add(asyncio.current_task())
            active_calls.add(request.url.path)
            try:
                if (request.method == "POST") == (cleanup_stage == "cancel"):
                    cleaning.set()
                    await asyncio.Event().wait()
                if request.method == "POST":
                    return httpx.Response(
                        200, json=cancel_response(request.url.path.split("/")[-2])
                    )
                return httpx.Response(
                    200, json=owned_receipt(request.url.path.rsplit("/", 1)[-1], "cancelled")
                )
            finally:
                active_calls.discard(request.url.path)

        client = JsonService(
            "https://fixture.invalid", "secret", transport=httpx.MockTransport(handler)
        )
        gateway = Gateway(h.contracts, client)
        try:
            task = asyncio.create_task(
                gateway.complete(
                    dict(id="turn:test", config_version=7), [dict(role="user", content="test")]
                )
            )
            await asyncio.wait_for(streaming.wait(), 1)
            task.cancel("first cancellation")
            await asyncio.wait_for(cleaning.wait(), 1)
            task.cancel("second cancellation")
            with pytest.raises(asyncio.CancelledError) as caught:
                await asyncio.wait_for(task, 1)
            assert caught.value.args == ("second cancellation",)
            assert not active_calls and not gateway.active_requests
            assert handler_tasks == {task}
            assert requests.count("/v1/chat/completions") == 1
            assert sum(path.endswith("/cancel") for path in requests) == 1
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "mode", ["empty", "persistent", "cut", "revoked", "unverified", "whitespace", "tool_then_empty"]
)
def test_native_recovery_is_single_output_and_diagnostics_survive_reopen(tmp_path, mode):
    async def run():
        path = tmp_path / "core.db"
        h = Harness(path, silence_ms=0)
        queue = FinishedQueue(h.clock)
        queue.states = ["sent"]
        h.sender = h.core.sender = queue
        attempts, cancelled, old_closed, tools = [], [], [], []

        class Cut(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield event(chunk({"reasoning_content": "synthetic"})).encode()
                raise httpx.ReadTimeout("secret URL/body")

        def handler(request):
            if request.url.path == "/v1/chat/completions":
                if attempts and mode in {"cut", "revoked"}:
                    assert old_closed, "new generation must wait for the old owned terminal"
                attempts.append(request.headers["X-Request-ID"])
                if len(attempts) == 1 and mode in {"cut", "revoked"}:
                    return httpx.Response(200, stream=Cut())
                delta, finish = {}, "stop"
                if mode == "whitespace":
                    delta = {"content": " \t\n "}
                elif mode == "tool_then_empty" and len(attempts) == 1:
                    delta = {
                        "tool_calls": [
                            dict(
                                index=0,
                                id="tool:one",
                                function=dict(name="life_read", arguments="{}"),
                            )
                        ]
                    }
                    finish = "tool_calls"
                elif len(attempts) == 2 and mode not in {"persistent", "tool_then_empty"}:
                    delta = {"content": "恢复后的完整回复。"}
                return httpx.Response(
                    200, text=event(chunk(delta)) + event(chunk({}, finish)) + "data: [DONE]\n\n"
                )
            request_id = request.url.path.split("/")[-2 if request.method == "POST" else -1]
            if request.method == "POST":
                cancelled.append(request_id)
                if mode == "revoked":
                    turn = h.turns()[0]
                    h.origins.values[turn["origin"]["assertion_ref"]]["revoked"] = True
                return httpx.Response(200, json=cancel_response(request_id))
            if request_id in cancelled:
                old_closed.append(request_id)
            value = owned_receipt(
                request_id,
                "cancelled" if request_id in cancelled else "completed",
                h.turns()[0]["config_version"],
            )
            if mode == "unverified":
                value.pop("execution")
            return httpx.Response(200, json=value)

        async def execute(*args, **kwargs):
            tools.append(True)
            return {"state": "completed"}, []

        client = JsonService(
            "https://fixture.invalid",
            "secret",
            label="gateway",
            transport=httpx.MockTransport(handler),
        )
        h.gateway = h.core.gateway = Gateway(h.contracts, client)
        h.core.role_actions.execute = execute
        try:
            await h.ingest(text="钥匙一直放门边")
            for _ in range(180):
                await h.cycles(4)
                if h.turns()[0]["phase"] in {"sent", "failed", "observed"}:
                    break
                await asyncio.sleep(0.01)
            turn = h.turns()[0]
            expected = (
                "observed"
                if mode in {"whitespace", "tool_then_empty"}
                else "failed"
                if mode in {"persistent", "revoked", "unverified"}
                else "sent"
            )
            assert turn["phase"] == expected
            assert len(attempts) == (1 if mode in {"revoked", "unverified", "whitespace"} else 2)
            assert len(queue.requests) == (1 if expected == "sent" else 0)
            assert len(tools) == (1 if mode == "tool_then_empty" else 0)
            if mode in {"whitespace", "tool_then_empty"}:
                assert "first_failure" not in turn
            else:
                assert turn["first_failure"]["stage"] == "model"
                assert turn["first_failure"]["model_request_id"] == attempts[0]
                assert turn["failure_count"] == (2 if mode == "persistent" else 1)
                assert (
                    turn["last_failure"]["model_request_id"]
                    == attempts[-1 if mode == "persistent" else 0]
                )
                assert "secret" not in json.dumps(turn["first_failure"])
                if mode == "cut":
                    assert turn["first_failure"]["exception_type"] == "ReadTimeout"
                if expected == "sent":
                    assert turn["failure"] is None
                retained = {k: turn[k] for k in ["first_failure", "last_failure", "failure_count"]}
                await h.core.close()
                reopened = Harness(path, silence_ms=0)
                try:
                    assert {k: reopened.turns()[0][k] for k in retained} == retained
                finally:
                    await reopened.core.close()
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


def test_shared_diagnostic_retains_only_registered_machine_fields():
    error = Fault(
        "dependency_unavailable",
        unknown=True,
        cause={
            "category": "read_error",
            "exception_type": "ReadTimeout",
            "peer": "gateway",
            "operation": "stream",
            "http_status": 200,
            "body": "secret",
            "url": "secret",
        },
    )
    error.model_request_id = "model:" + "a" * 32
    projected = obs.failure_diagnostic(
        error, code=error.code, attempt=1, stage="model", observed_at=12.5
    )
    assert projected == dict(
        attempt=1,
        stage="model",
        observed_at=12.5,
        code="dependency_unavailable",
        category="read_error",
        unknown=True,
        exception_type="ReadTimeout",
        peer="gateway",
        operation="stream",
        http_status=200,
        model_request_id=error.model_request_id,
    )

    class PrivateTimeout(httpx.ReadTimeout):
        pass

    untrusted = PrivateTimeout("secret")
    untrusted.cause = {
        "category": "secret",
        "exception_type": "PrivateTimeout",
        "peer": "secret",
        "operation": "secret",
        "http_status": True,
    }
    untrusted.model_request_id = "secret"
    fallback = obs.failure_diagnostic(
        untrusted, code="secret", attempt=2, stage="model", observed_at=13.5
    )
    assert fallback == dict(
        attempt=2,
        stage="model",
        observed_at=13.5,
        code="internal_error",
        category="internal_error",
        unknown=False,
        exception_type="ReadTimeout",
    )
    assert "secret" not in json.dumps(fallback)
