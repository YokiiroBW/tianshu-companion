"""Network stream budgets never cancel the consumer's delivery callback."""

import asyncio
import json

import httpx
import pytest

from support import Harness, contracts
from test_delivery_v2 import NativeQueue
from test_model_recovery import closed_attempt
from tianshu_companion.clients import Gateway, JsonService
from tianshu_companion.contracts import Fault


def event(delta, finish=None):
    value = dict(choices=[dict(index=0, delta=delta, finish_reason=finish)])
    return ("data: " + json.dumps(value, ensure_ascii=False) + "\n\n").encode()


def route(request, config_version=7):
    return dict(
        schema_version=1,
        request_id=request.url.path.rsplit("/", 1)[-1],
        config_version=config_version,
        provider_id="provider:synthetic",
        requested_model=None,
        resolved_model="synthetic",
        protocol="openai-chat-completions",
        outcome="succeeded",
        upstream_request_id=None,
        usage=None,
        fallback_used=False,
        observed_at="2026-10-22T00:00:00Z",
        credential_namespace="fixture:namespace",
        caller_service="companion",
        requested_reasoning={},
        effective_reasoning={},
        applied_policies=[],
        usage_complete=False,
        native_usage=None,
    )


class Stream(httpx.AsyncByteStream):
    def __init__(self, chunks, *, delay=0):
        self.chunks = chunks
        self.delay = delay
        self.reads = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            await asyncio.sleep(self.delay)
            self.reads += 1
            yield chunk

    async def aclose(self):
        self.closed = True


class SyntheticPrivateReadTimeout(httpx.ReadTimeout):
    pass


@pytest.mark.parametrize(
    "exception,category,registered_type",
    [
        (httpx.ReadTimeout, "read_error", "ReadTimeout"),
        (httpx.ReadError, "read_error", "ReadError"),
        (httpx.ConnectTimeout, "connect_error", "ConnectTimeout"),
        (httpx.ConnectError, "connect_error", "ConnectError"),
        (SyntheticPrivateReadTimeout, "read_error", "ReadTimeout"),
    ],
)
def test_native_transport_cause_keeps_machine_request_link_without_secret_text(
    exception, category, registered_type
):
    async def run():
        def handler(request):
            if request.url.path != "/v1/chat/completions":
                return closed_attempt(request)
            raise exception("secret URL, credential and upstream body")

        client = JsonService(
            "https://synthetic.invalid",
            "fixture",
            transport=httpx.MockTransport(handler),
            label="gateway",
        )
        try:
            gateway = Gateway(contracts(), client)
            with pytest.raises(Fault) as caught:
                await gateway.complete(
                    {"id": "turn:fixture", "config_version": 7},
                    [{"role": "user", "content": "合成"}],
                )
            error = caught.value
            assert error.cause == dict(
                category=category,
                exception_type=registered_type,
                peer="gateway",
                operation="stream",
            )
            assert error.model_request_id.startswith("model:")
            assert error.retryable_model
            assert "secret" not in json.dumps(error.cause)
            assert not gateway.active_requests
        finally:
            await client.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "failure",
    ["incomplete_choice", "empty_choice", "invalid_receipt", "receipt_mismatch", "invalid_request"],
)
def test_native_completion_postprocessing_keeps_one_request_identity_and_retry_rules(failure):
    async def run():
        requests = []
        chunks = [event({"content": "合成答复"})]
        if failure == "empty_choice":
            chunks = []
        elif failure != "incomplete_choice":
            chunks.append(event({}, "stop"))
        stream = Stream([*chunks, b"data: [DONE]\n\n"])

        def handler(request):
            requests.append(request)
            if request.url.path == "/v1/chat/completions":
                return httpx.Response(200, stream=stream)
            if failure == "empty_choice":
                return closed_attempt(request)
            receipt = route(request)
            if failure == "invalid_receipt":
                receipt.pop("caller_service")
            elif failure == "receipt_mismatch":
                receipt["config_version"] = 8
            return httpx.Response(200, json=receipt)

        client = JsonService(
            "https://synthetic.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        gateway = Gateway(contracts(), client)
        try:
            messages = [{"role": "user", "content": "合成"}]
            if failure == "invalid_request":
                messages = []
            with pytest.raises(Fault) as caught:
                await gateway.complete({"id": "turn:fixture", "config_version": 7}, messages)
            error = caught.value
            if failure == "invalid_request":
                assert requests == []
                assert not hasattr(error, "model_request_id")
            else:
                assert error.model_request_id == requests[0].headers["X-Request-ID"]
            assert error.retryable_model == (failure == "empty_choice")
            assert error.unknown == (failure in {"empty_choice", "incomplete_choice"})
            assert not gateway.active_requests
        finally:
            await client.close()

    asyncio.run(run())


def test_slow_native_delivery_does_not_expire_the_network_stream_budget():
    async def run():
        h = Harness(silence_ms=0)
        text = "这一段只用于合成回归，确认慢投递回调不会取消已经收到的模型流，也不会使会话一直停留在生成状态。"
        stream = Stream([event({"content": text}), event({}, "stop"), b"data: [DONE]\n\n"])
        requests = []

        def handler(request):
            requests.append(request.url.path)
            if request.url.path == "/v1/chat/completions":
                assert set(request.extensions["timeout"].values()) == {30}
                return httpx.Response(200, stream=stream)
            return httpx.Response(200, json=route(request, h.core.config_version))

        class SlowDelivery(NativeQueue):
            async def send_expression(self, request):
                await asyncio.sleep(0.65)
                return await super().send_expression(request)

            async def finalize_expression(self, expression_id, origin):
                self.final = True
                return self.receipt()

        client = JsonService(
            "https://synthetic.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        client.STREAM_BUDGET_SECONDS = 0.3
        queue = SlowDelivery(h.clock)
        queue.states = ["sent"]
        h.core.gateway = Gateway(h.contracts, client)
        h.sender = h.core.sender = queue
        try:
            await h.ingest(text="合成的普通问句")
            for _ in range(200):
                await h.cycles(2)
                if h.turns()[0]["phase"] in {"sent", "failed"}:
                    break
                await asyncio.sleep(0.01)
            turn = h.turns()[0]
            assert turn["phase"] == "sent" and not turn["stream_open"]
            assert turn["model_calls"] == 1 and turn["failure"] is None
            assert len(queue.requests) == 1 and stream.closed
            assert requests.count("/v1/chat/completions") == 1
            assert not h.core.gateway.active_requests
        finally:
            await client.close()
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "delta,retryable",
    [
        ({"reasoning_content": "synthetic"}, True),
        ({"content": "synthetic"}, False),
        (
            {
                "tool_calls": [
                    {"index": 0, "id": "tool:test", "function": {"name": "test", "arguments": "{"}}
                ]
            },
            False,
        ),
    ],
)
def test_connect_and_reads_share_one_network_budget_without_replaying_partial_output(
    delta, retryable
):
    async def run():
        stream = Stream([event(delta)] * 4 + [event({}, "stop"), b"data: [DONE]\n\n"], delay=0.15)
        requests = []

        async def handler(request):
            requests.append(request.url.path)
            if request.url.path != "/v1/chat/completions":
                return closed_attempt(request)
            await asyncio.sleep(0.05)
            return httpx.Response(200, stream=stream)

        client = JsonService(
            "https://synthetic.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        client.STREAM_BUDGET_SECONDS = 0.4
        gateway = Gateway(contracts(), client)
        try:
            with pytest.raises(Fault) as caught:
                await gateway.complete(
                    dict(id="turn:test", config_version=7), [{"role": "user", "content": "test"}]
                )
            assert caught.value.code == "dependency_unavailable"
            assert caught.value.retryable_model is retryable
            assert 1 <= stream.reads < 4 and stream.closed
            assert requests[0] == "/v1/chat/completions"
            assert len(requests) == (3 if retryable else 1)
            if retryable:
                assert requests[1] == requests[2] + "/cancel"
            assert not gateway.active_requests
        finally:
            await client.close()

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["headers", "connect", "read"])
def test_connection_and_response_headers_remain_bounded(failure):
    async def run():
        requests = []

        async def handler(request):
            requests.append(request.url.path)
            if request.url.path != "/v1/chat/completions":
                return closed_attempt(request)
            if failure == "connect":
                raise httpx.ConnectTimeout("synthetic connection timeout")
            if failure == "read":
                raise httpx.ReadTimeout("synthetic response header timeout")
            await asyncio.sleep(0.2)
            return httpx.Response(200, content=b"data: [DONE]\n\n")

        client = JsonService(
            "https://synthetic.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        client.STREAM_BUDGET_SECONDS = 0.04
        gateway = Gateway(contracts(), client)
        try:
            with pytest.raises(Fault) as caught:
                await gateway.complete(
                    dict(id="turn:test", config_version=7), [{"role": "user", "content": "test"}]
                )
            assert caught.value.code == "dependency_unavailable"
            assert caught.value.retryable_model
            assert requests[0] == "/v1/chat/completions" and len(requests) == 3
            assert requests[1] == requests[2] + "/cancel" and not gateway.active_requests
        finally:
            await client.close()

    asyncio.run(run())


@pytest.mark.parametrize("stage", ["headers", "body", "callback"])
def test_real_external_cancellation_is_not_reclassified_as_a_network_timeout(stage):
    async def run():
        entered = asyncio.Event()
        requests = []

        class WaitingBody(Stream):
            async def __aiter__(self):
                if stage == "body":
                    entered.set()
                    await asyncio.Event().wait()
                async for chunk in super().__aiter__():
                    yield chunk

        stream = WaitingBody([event({"content": "synthetic"})])

        async def handler(request):
            requests.append(request.url.path)
            if request.url.path.endswith("/cancel"):
                return httpx.Response(200, json={})
            if stage == "headers":
                entered.set()
                await asyncio.Event().wait()
            return httpx.Response(200, stream=stream)

        async def callback(text):
            if stage == "callback":
                entered.set()
                await asyncio.Event().wait()

        client = JsonService(
            "https://synthetic.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        gateway = Gateway(contracts(), client)
        try:
            task = asyncio.create_task(
                gateway.complete(
                    dict(id="turn:test", config_version=7),
                    [{"role": "user", "content": "test"}],
                    on_delta=callback,
                )
            )
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel("actual caller cancellation")
            with pytest.raises(asyncio.CancelledError) as caught:
                await task
            assert caught.value.args == ("actual caller cancellation",)
            assert requests[0] == "/v1/chat/completions" and len(requests) == 2
            assert requests[1].endswith("/cancel") and not gateway.active_requests
            assert stage == "headers" or stream.closed
        finally:
            await client.close()

    asyncio.run(run())


def test_response_size_failure_after_content_never_becomes_a_safe_generation_retry():
    async def run():
        first = event({"content": "synthetic answer"})
        stream = Stream([first, b"x" * 100])
        client = JsonService(
            "https://synthetic.invalid",
            "fixture",
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream)),
        )
        client.MAX_RESPONSE_BYTES = len(first) + 1
        gateway = Gateway(contracts(), client)
        try:
            with pytest.raises(Fault) as caught:
                await gateway.complete(
                    dict(id="turn:test", config_version=7), [{"role": "user", "content": "test"}]
                )
            assert caught.value.code == "budget_exceeded" and not caught.value.retryable_model
            assert stream.closed and not gateway.active_requests
        finally:
            await client.close()

    asyncio.run(run())
