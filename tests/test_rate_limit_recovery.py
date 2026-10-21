"""Typed terminal rejection and the durable native send boundary, all synthetic."""

import asyncio
import json
import time
from unittest.mock import patch

import httpx
import pytest

from support import Harness
from test_delivery_v2 import NativeQueue
from tianshu_companion.clients import Gateway, JsonService, _retry_after_seconds
from tianshu_companion.contracts import Fault
from tianshu_companion.contracts import digest


def receipt(request_id):
    return dict(
        schema_version=1,
        request_id=request_id,
        config_version=7,
        provider_id="provider:synthetic",
        requested_model=None,
        resolved_model="fixture",
        protocol="openai-chat-completions",
        outcome="failed",
        upstream_request_id=None,
        usage=None,
        fallback_used=False,
        observed_at="2026-10-21T01:00:00Z",
        credential_namespace="fixture:namespace",
        caller_service="companion",
        requested_reasoning={},
        effective_reasoning={},
        applied_policies=[],
        usage_complete=False,
        native_usage=None,
        execution=dict(
            state="failed",
            upstream_started=True,
            received_bytes=10,
            forwarded_bytes=10,
            event_count=2,
            output_observed=True,
            finish_reasons=[],
            cancel_requested=False,
            error_code="upstream_http_error",
        ),
    )


def event(value):
    return "data: " + json.dumps(value, ensure_ascii=False) + "\n\n"


def chunk(delta, finish=None):
    return dict(choices=[dict(index=0, delta=delta, finish_reason=finish)])


@pytest.mark.parametrize(
    "value,expected",
    [
        ("45", 45),
        (" 90 ", 90),
        ("Thu, 01 Jan 1970 00:02:00 GMT", 60),
        ("Thu, 01 Jan 1970 00:00:30 GMT", 0),
        ("-1", None),
        ("NaN", None),
        ("1.5", None),
        ("unknown", None),
        (None, None),
    ],
)
def test_retry_after_accepts_only_valid_delay_or_http_date(value, expected):
    with patch("tianshu_companion.clients.time.time", return_value=60):
        assert _retry_after_seconds(value) == expected


@pytest.mark.parametrize("status", [429, 503])
def test_model_http_pressure_retains_retry_after_without_broadening_foreground_retry(status):
    async def run():
        h = Harness()
        calls = []

        def handler(request):
            calls.append(request.url.path)
            return httpx.Response(status, json={}, headers={"Retry-After": "90"})

        client = JsonService(
            "https://fixture.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        try:
            gateway = Gateway(h.contracts, client)
            with pytest.raises(Fault) as caught:
                await gateway.generate(
                    dict(id="turn:fixture", config_version=7),
                    [dict(role="user", content="fixture")],
                )
            assert caught.value.retry_after_seconds == 90
            assert caught.value.rate_limited is (status == 429)
            assert caught.value.retryable_model is (status == 503)
            assert calls[0] == "/v1/chat/completions"
            assert len(calls) == (2 if status == 429 else 1)
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


def test_receipt_pressure_retry_after_is_returned_to_durable_owner_without_inline_replay():
    async def run():
        calls = []

        def handler(request):
            calls.append(json.loads(request.content))
            return httpx.Response(
                503,
                json=dict(code="queue_full", execution_state="not_started", retryable=True),
                headers={"Retry-After": "45"},
            )

        client = JsonService(
            "https://fixture.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        try:
            with pytest.raises(Fault) as caught:
                await client.call(
                    "/internal/v1/memory/candidates/process",
                    dict(job_id="job:fixture", output=None),
                )
            assert caught.value.retryable_read
            assert not caught.value.unknown
            assert caught.value.retry_after_seconds == 45
            assert calls == [dict(job_id="job:fixture", output=None)]
        finally:
            await client.close()

    asyncio.run(run())


def rejected_stream(text="", *, kind="rate_limit_error", extra=""):
    prefix = event(chunk(dict(content=text))) if text else ""
    return prefix + event(dict(error=dict(type=kind, message="fixture only"))) + extra


@pytest.mark.parametrize(
    "mode,expected,reads",
    [
        ("terminal", True, 1),
        ("running_then_terminal", True, 2),
        ("running", False, 3),
        ("unknown", False, 1),
        ("wrong_id", False, 1),
        ("wrong_version", False, 1),
        ("wrong_caller", False, 1),
        ("cancelled", False, 1),
        ("not_started", False, 1),
        ("missing_execution", False, 1),
        ("wrong_error", False, 1),
        ("finish", False, 0),
        ("tool", False, 0),
        ("other_type", False, 0),
        ("malformed_after_signal", False, 0),
        ("message_only", False, 0),
        ("mixed_choices", False, 0),
        ("mixed_empty_choices", False, 0),
        ("mixed_tool", False, 0),
        ("mixed_content", False, 0),
        ("mixed_nested_tool", False, 0),
        ("not_found", False, 1),
        ("succeeded", False, 1),
    ],
)
def test_retry_flag_requires_typed_signal_and_owned_terminal_receipt(mode, expected, reads):
    retry_flag(mode, expected, reads)


@pytest.mark.parametrize(
    "mode,expected,reads",
    [
        ("terminal", True, 1),
        ("running_then_terminal", True, 2),
        ("running", False, 3),
        ("unknown", False, 1),
        ("wrong_id", False, 1),
        ("wrong_version", False, 1),
        ("wrong_caller", False, 1),
        ("cancelled", False, 1),
        ("not_started", False, 1),
        ("missing_execution", False, 1),
        ("wrong_error", False, 1),
        ("not_found", False, 1),
        ("succeeded", False, 1),
    ],
)
def test_http_429_requires_the_same_owned_terminal_receipt(mode, expected, reads):
    retry_flag(mode, expected, reads, http_429=True)


def retry_flag(mode, expected, reads, *, http_429=False):
    async def run():
        h = Harness()
        observed = []
        complete = []

        def handler(request):
            if request.url.path == "/v1/chat/completions":
                if http_429:
                    return httpx.Response(429, json=dict(error=dict(type="rate_limit_error")))
                value = rejected_stream("\u200b")
                if mode == "finish":
                    value = event(chunk({}, "length")) + value
                elif mode == "tool":
                    value = (
                        event(
                            chunk(
                                dict(
                                    tool_calls=[
                                        dict(
                                            index=0,
                                            id="tool:fragment",
                                            function=dict(name="life_read", arguments="{"),
                                        )
                                    ]
                                )
                            )
                        )
                        + value
                    )
                elif mode == "other_type":
                    value = rejected_stream(kind="Rate_Limit_Error")
                elif mode == "message_only":
                    value = event(dict(error=dict(type="other", message="rate_limit_error")))
                elif mode == "malformed_after_signal":
                    value += event(dict(choices=[dict(index=0, delta=None)]))
                elif mode.startswith("mixed_"):
                    mixed = dict(error=dict(type="rate_limit_error"))
                    if mode == "mixed_choices":
                        mixed.update(chunk(dict(content="cannot be ignored"), "stop"))
                    elif mode == "mixed_empty_choices":
                        mixed["choices"] = []
                    elif mode == "mixed_content":
                        mixed["content"] = "cannot be ignored"
                    elif mode == "mixed_tool":
                        mixed["tool_calls"] = [dict(index=0)]
                    else:
                        mixed["error"]["tool_calls"] = [dict(index=0)]
                    value = event(mixed)
                return httpx.Response(
                    200, text=value, headers={"Content-Type": "text/event-stream"}
                )
            observed.append(request.url.path)
            value = receipt(request.url.path.rsplit("/", 1)[-1])
            execution = value["execution"]
            if mode in {"running", "running_then_terminal"} and (
                mode == "running" or len(observed) == 1
            ):
                value["outcome"] = "unknown"
                execution.update(state="running", error_code=None)
            elif mode == "unknown":
                value["outcome"] = "unknown"
                execution.update(state="unknown", error_code="transport_error")
            elif mode == "wrong_id":
                value["request_id"] = "model:wrong"
            elif mode == "wrong_version":
                value["config_version"] = 8
            elif mode == "wrong_caller":
                value["caller_service"] = "another-service"
            elif mode == "cancelled":
                execution["cancel_requested"] = True
            elif mode == "not_started":
                execution["upstream_started"] = False
            elif mode == "missing_execution":
                value.pop("execution")
            elif mode == "wrong_error":
                execution["error_code"] = "stream_incomplete"
            elif mode == "not_found":
                return httpx.Response(404, json=dict(code="not_found"))
            elif mode == "succeeded":
                value["outcome"] = "succeeded"
            return httpx.Response(200, json=value)

        client = JsonService(
            "https://fixture.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        try:
            gateway = Gateway(h.contracts, client)
            with pytest.raises(Fault) as caught:
                await gateway.complete(
                    dict(id="turn:fixture", config_version=7),
                    [dict(role="user", content="fixture")],
                    on_delta=lambda text: collect(complete, text),
                )
            assert caught.value.retryable_model is expected
            assert caught.value.unknown
            assert len(observed) == reads
            assert not gateway.active_requests
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


async def collect(values, text):
    values.append(text)


@pytest.mark.parametrize("slow_receipt", [False, True])
def test_receipt_is_read_after_eof_and_close_with_one_short_wait_budget(slow_receipt):
    async def run():
        h = Harness()
        finished, closed, cancelled = [], [], []

        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield rejected_stream("\u200b").encode()
                await asyncio.sleep(0)
                finished.append(True)

            async def aclose(self):
                closed.append(True)

        async def handler(request):
            if request.url.path == "/v1/chat/completions":
                return httpx.Response(
                    200, stream=Stream(), headers={"Content-Type": "text/event-stream"}
                )
            assert finished and closed
            if slow_receipt:
                try:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    cancelled.append(True)
                    raise
            return httpx.Response(200, json=receipt(request.url.path.rsplit("/", 1)[-1]))

        client = JsonService(
            "https://fixture.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        try:
            gateway = Gateway(h.contracts, client)
            with pytest.raises(Fault) as caught:
                await asyncio.wait_for(
                    gateway.complete(
                        dict(id="turn:fixture", config_version=7),
                        [dict(role="user", content="fixture")],
                    ),
                    timeout=2,
                )
            assert caught.value.retryable_model is not slow_receipt
            assert cancelled == ([True] if slow_receipt else [])
            assert not gateway.active_requests
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


class FinishedQueue(NativeQueue):
    async def query_expression(self, expression_id):
        if not self.requests:
            raise Fault("result_unknown", unknown=True)
        return await super().query_expression(expression_id)

    async def finalize_expression(self, expression_id, origin):
        if not self.requests:
            raise Fault("result_unknown", unknown=True)
        self.final = True
        return self.receipt()


@pytest.mark.parametrize("text", ["", " \n\t", "\u200b", "尚未发送的短文本"])
def test_proved_rejection_discards_only_local_pending_text_and_regenerates_once(text):
    asyncio.run(native_recovery(text, "recover"))


def test_http_429_uses_the_existing_single_recovery_and_honors_retry_after():
    asyncio.run(native_recovery("", "recover", transport=429, retry_after=1))


@pytest.mark.parametrize(
    "mode",
    [
        "persistent",
        "revoked",
        "cancelled",
        "native_execution",
        "v1_sending",
        "v1_unknown",
        "v1_sent",
        "v1_pending",
        "expression",
    ],
)
def test_http_429_cannot_cross_a_durable_send_tool_or_authority_boundary(mode):
    asyncio.run(native_recovery("", mode, transport=429))


@pytest.mark.parametrize(
    "mode",
    [
        "persistent",
        "revoked",
        "cancelled",
        "native_execution",
        "v1_sending",
        "v1_unknown",
        "v1_sent",
        "v1_pending",
        "expression",
        "partial_sent",
        "partial_unknown",
    ],
)
def test_any_durable_send_or_tool_boundary_blocks_regeneration(mode):
    asyncio.run(native_recovery("\u200b", mode))


@pytest.mark.parametrize(
    "transport", [False, True, 429], ids=["typed_cf", "transport_503", "http_429"]
)
@pytest.mark.parametrize(
    "change",
    [
        "cancelled",
        "permission_revoked",
        "terminal",
        "role",
        "lease",
        "source",
        "sending",
        "expression",
        "native_execution",
    ],
)
def test_retry_reloads_durable_state_after_preflight_yields(change, transport):
    asyncio.run(
        native_recovery(
            "\u200b", "preflight_" + change, during_preflight=change, transport=transport
        )
    )


async def native_recovery(text, mode, *, during_preflight=None, transport=False, retry_after=None):
    h = Harness(silence_ms=0)
    queue = FinishedQueue(h.clock)
    queue.states = ["unknown" if mode == "partial_unknown" else "sent"]
    h.sender = h.core.sender = queue
    attempts = []
    attempt_times = []
    changed = []

    def handler(request):
        if request.url.path != "/v1/chat/completions":
            value = receipt(request.url.path.rsplit("/", 1)[-1])
            value["config_version"] = h.turns()[0]["config_version"]
            if len(attempts) == 2 and mode != "persistent":
                value["outcome"] = "succeeded"
                value["execution"].update(
                    state="completed", error_code=None, finish_reasons=["stop"]
                )
            return httpx.Response(200, json=value)
        attempts.append(request.headers["X-Request-ID"])
        attempt_times.append(time.monotonic())
        if len(attempts) == 1:
            turn = h.turns()[0]
            if mode == "revoked":
                h.origins.values[turn["origin"]["assertion_ref"]]["revoked"] = True
            elif mode == "cancelled":
                h.core._cancel_turn(turn, "user_requested")
            elif mode in {"native_execution", "expression"}:
                turn["native_execution" if mode == "native_execution" else "expression_id"] = (
                    dict(messages=[dict(role="tool", content="already completed")])
                    if mode == "native_execution"
                    else "expression:already"
                )
                h.core.store.put("turns", turn)
            elif mode.startswith("v1_"):
                h.core.store.put(
                    "replies",
                    dict(
                        id="reply:prior",
                        conversation_id=turn["conversation_id"],
                        turn_id=turn["id"],
                        sequence=1,
                        segment_sequence=1,
                        segment_count=1,
                        state=mode.removeprefix("v1_"),
                        text="already attempted",
                        receipt=None,
                        request=None,
                        unknown_since=None,
                        attempted_at=h.clock(),
                    ),
                )
        if transport and len(attempts) == 1:
            return httpx.Response(
                429 if transport == 429 else 503,
                headers={"Retry-After": str(retry_after)} if retry_after is not None else {},
            )
        if len(attempts) == 1 or mode == "persistent":
            partial = (
                "已经越过实际发送边界的完整段落。" * 4 if mode.startswith("partial_") else text
            )
            body = rejected_stream(partial)
        else:
            body = event(chunk(dict(content="全新的完整回复。"))) + event(chunk({}, "stop"))
            body += "data: [DONE]\n\n"
        return httpx.Response(200, text=body, headers={"Content-Type": "text/event-stream"})

    client = JsonService(
        "https://fixture.invalid", "fixture", transport=httpx.MockTransport(handler)
    )
    h.gateway = h.core.gateway = Gateway(h.contracts, client)
    preflight = h.core._preflight

    async def yielding_preflight(turn):
        await preflight(turn)
        if turn.get("model_connection_retries") != 1 or changed or not during_preflight:
            return
        assert not h.core.store.db.in_transaction
        await asyncio.sleep(0)
        fresh = h.core.store.get("turns", turn["id"])
        changed.append(during_preflight)
        if during_preflight in {"cancelled", "permission_revoked"}:
            h.core._cancel_turn(
                fresh, "user_requested" if during_preflight == "cancelled" else during_preflight
            )
        elif during_preflight == "terminal":
            h.core._finish(fresh, "failed", "failed")
        elif during_preflight == "source":
            row = h.core.store.get(
                "physicals", digest(fresh["bundle"]["messages"][0]["message_key"])
            )
            row["fact"]["state"] = "retracted"
            h.core.store.put("physicals", row)
        elif during_preflight == "sending":
            h.core.store.put(
                "replies",
                dict(
                    id="reply:during-preflight",
                    conversation_id=fresh["conversation_id"],
                    turn_id=fresh["id"],
                    sequence=1,
                    segment_sequence=1,
                    segment_count=1,
                    state="sending",
                    text="persisted send intent",
                    receipt=None,
                    request=None,
                    unknown_since=None,
                    attempted_at=h.clock(),
                ),
            )
        else:
            if during_preflight == "role":
                fresh["role"] = None
            elif during_preflight == "lease":
                fresh["model_selection"] = dict(expires_at=h.clock() - 1)
            elif during_preflight == "expression":
                fresh["expression_id"] = "expression:already"
            else:
                fresh["native_execution"] = dict(messages=[dict(role="tool", content="done")])
            h.core._save_turn(fresh)

    h.core._preflight = yielding_preflight
    try:
        await h.ingest(text="今天午后还是喝那杯茶。")
        for _ in range(200 if retry_after else 100):
            await h.cycles(4)
            if attempts and not h.core.jobs:
                break
            await asyncio.sleep(0.01)
        turn = h.turns()[0]
        expected = 2 if mode in {"recover", "persistent"} else 1
        assert len(attempts) == turn["model_calls"] == expected
        if retry_after is not None and expected == 2:
            assert attempt_times[1] - attempt_times[0] >= retry_after
        if during_preflight:
            assert changed == [during_preflight]
            assert turn["model_connection_retries"] == 1
            if during_preflight in {"cancelled", "permission_revoked"}:
                assert turn["cancelled"] and turn["phase"] == "cancelled"
            elif during_preflight == "role":
                assert turn["role"] is None
            elif during_preflight == "lease":
                assert turn["model_selection"]["expires_at"] < h.clock()
            elif during_preflight == "sending":
                assert h.core.store.get("replies", "reply:during-preflight")["state"] == "sending"
        if mode == "recover":
            assert turn["phase"] == "sent"
            assert len(queue.requests) == 1
            assert queue.requests[0]["segments"][0]["text"] == "全新的完整回复。"
            assert turn["model_connection_retries"] == 1
        elif mode.startswith("partial_"):
            assert len(queue.requests) == 1  # Confirmed or unknown is never replayed.
        else:
            assert not queue.requests
    finally:
        await client.close()
        await h.core.close()
