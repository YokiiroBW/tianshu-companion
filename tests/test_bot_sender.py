"""Explicit bot binding selection and read-only Platform receipt reconciliation."""

import asyncio
import copy
import json

import httpx
import pytest

from support import FakeSender, Harness
from test_delivery_v2 import NativeQueue
from tianshu_companion.clients import BotSenderRouter, JsonService, PlatformBotSender, utc
from tianshu_companion.contracts import Fault


class AvailableSender(FakeSender):
    available = True


def context_transport(h, *, status=200, lose_send_ack=False):
    """Real sender/client/Core flow; only the remote Platform is a stand-in."""
    state, seen = {"status": status}, []
    queue = NativeQueue(h.clock)
    queue.states = ["sent", "sent"]

    async def handler(request):
        body = json.loads(request.content)
        path = request.url.path
        seen.append((path, copy.deepcopy(body)))
        if path == "/internal/v2/bot-delivery/context":
            if state["status"] != 200:
                return httpx.Response(
                    state["status"],
                    json=dict(
                        schema_version=1,
                        request_id=body["request_id"],
                        code="forbidden" if state["status"] == 403 else "dependency_unavailable",
                        execution_state="not_started",
                        retryable=state["status"] == 503,
                    ),
                )
            return httpx.Response(
                200,
                json=dict(
                    schema_version=2,
                    request_id=body["request_id"],
                    origin=body["origin"]["origin"],
                    scope=body["scope"],
                    channel=body["channel"],
                    expires_at=utc(h.clock() + 3600),
                ),
            )
        if path == "/internal/v2/bot-delivery/send":
            receipt = await queue.send_expression(body)
            if lose_send_ack:
                raise httpx.ReadError("Synthetic accepted send lost its ACK")
            return httpx.Response(200, json=receipt)
        assert path in {"/internal/v2/bot-delivery/query", "/internal/v2/bot-delivery/finalize"}
        if path.endswith("/finalize"):
            queue.final = True
        return httpx.Response(
            200,
            json=dict(
                schema_version=2,
                request_id=body["request_id"],
                expression_id=body["expression_id"],
                receipt=None if lose_send_ack else queue.receipt(),
            ),
        )

    client = JsonService(
        "https://platform.synthetic.invalid",
        "companion-only",
        label="channel",
        transport=httpx.MockTransport(handler),
    )
    h.core.sender = PlatformBotSender(h.contracts, client)
    generate = h.gateway.generate

    async def complete(turn, messages, *, tools=None, on_delta=None):
        segments, receipt = await generate(turn, messages)
        text = "".join(segments)
        if on_delta is not None:
            await on_delta(text)
        return dict(role="assistant", content=text), receipt

    h.gateway.complete = complete
    return client, state, seen, queue


def test_pending_delivery_context_uses_existing_preparation_recovery_before_one_model():
    async def run():
        h = Harness(silence_ms=0)
        client, state, seen, queue = context_transport(h, status=503)
        try:
            await h.ingest()
            await h.cycles()
            waiting = h.turns()[0]
            assert waiting["phase"] == "preparing"
            assert waiting["preparation_retries"] == 1
            assert waiting["first_failure"]["http_status"] == 503
            assert waiting["first_failure"]["operation"] == "query"
            assert waiting["first_failure"]["read_execution"] == "not_started"
            assert len(seen) == 1 and not h.gateway.calls and not queue.requests
            original = copy.deepcopy(seen[0][1])
            state["status"] = 200  # The original ingress confirmation is now available.
            h.clock.advance(0.3)
            await asyncio.sleep(0.21)  # Respect the client's actual peer cooldown.
            await h.cycles(80)
            fresh = h.turns()[0]
            assert fresh["phase"] == "sent"
            assert fresh["id"] == waiting["id"]
            assert fresh["preparation_retries"] == 1
            assert fresh["preparation_retry_started_at"] == waiting["preparation_retry_started_at"]
            assert len(h.gateway.calls) == len(queue.requests) == 1
            assert queue.requests[0]["segments"][0]["text"] == "".join(h.gateway.segments)
            contexts = [body for path, body in seen if path.endswith("/context")]
            assert len(contexts) > 1
            assert all(
                all(body[key] == original[key] for key in ("origin", "scope", "channel"))
                for body in contexts
            )
        finally:
            await h.core.close()
            await client.close()

    asyncio.run(run())


def test_forbidden_delivery_context_does_not_retry_or_generate():
    async def run():
        h = Harness(silence_ms=0)
        client, _, seen, queue = context_transport(h, status=403)
        try:
            await h.ingest()
            await h.cycles()
            h.clock.advance(1)
            await h.cycles()
            turn = h.turns()[0]
            assert (turn["phase"], turn["failure"]) == ("failed", "forbidden")
            assert "preparation_retries" not in turn
            assert len(seen) == 1 and not h.gateway.calls and not queue.requests
        finally:
            await h.core.close()
            await client.close()

    asyncio.run(run())


def test_delivery_context_read_does_not_make_accepted_send_replayable():
    async def run():
        h = Harness(silence_ms=0)
        h.gateway.segments = ["One accepted segment"]
        client, _, seen, queue = context_transport(h, lose_send_ack=True)
        try:
            await h.ingest()
            await h.cycles(80)
            turn = h.turns()[0]
            assert len(h.gateway.calls) == len(queue.requests) == 1
            assert turn["phase"] == "reconciling"
            assert turn["unresolved_delivery"]
            h.clock.advance(h.core.policy.delivery_reconcile_timeout_ms / 1000 + 1)
            await h.cycles(80)
            assert h.turns()[0]["phase"] == "closed_unknown"
            assert len([path for path, _ in seen if path.endswith("/send")]) == 1
            assert len(queue.requests) == 1
            assert "preparation_retries" not in h.turns()[0]
        finally:
            await h.core.close()
            await client.close()

    asyncio.run(run())


def test_selected_platform_binding_preserves_legacy_exit():
    async def run():
        h = Harness()
        legacy, platform = AvailableSender(h.clock), AvailableSender(h.clock)
        bindings = {
            "qq-legacy": {"service": "nonebot", "namespace": "qq"},
            "qq-platform": {"service": "platform", "namespace": "qq"},
        }
        router = BotSenderRouter(legacy, platform, ["qq-platform"], bindings)
        await router.send(
            {
                "destination": {"binding_id": "qq-legacy"},
                "reply_id": "one",
                "command": {"request_id": "r"},
                "segment_sequence": 1,
            }
        )
        await router.send(
            {
                "destination": {"binding_id": "qq-platform"},
                "reply_id": "two",
                "command": {"request_id": "r"},
                "segment_sequence": 1,
            }
        )
        assert [r["reply_id"] for r in legacy.calls] == ["one"]
        assert [r["reply_id"] for r in platform.calls] == ["two"]
        with pytest.raises(ValueError):
            BotSenderRouter(legacy, platform, ["qq-legacy"], bindings)
        h.core.store.close()

    asyncio.run(run())


def test_platform_status_returns_only_matching_published_receipt():
    async def run():
        h = Harness()
        await h.ingest()
        h.clock.advance(6)
        await h.cycles()
        request = h.sender.calls[0]
        receipt = h.sender.receipt(request, "sent")
        observed = []

        def handler(http_request):
            observed.append((http_request.url.path, http_request.headers["Authorization"]))
            return httpx.Response(200, json={"receipt": receipt})

        client = JsonService(
            "https://platform.synthetic.invalid",
            "companion-only",
            transport=httpx.MockTransport(handler),
        )
        sender = PlatformBotSender(h.contracts, client)
        assert await sender.reconcile(request) == receipt
        assert observed == [("/internal/v1/conversation/reply-status", "Bearer companion-only")]
        bad = copy.deepcopy(receipt)
        bad["reply_id"] = "reply:other"

        def wrong(_):
            return httpx.Response(200, json={"receipt": bad})

        client.client = httpx.AsyncClient(transport=httpx.MockTransport(wrong))
        with pytest.raises(Fault):
            await sender.reconcile(request)
        await client.close()
        h.core.store.close()

    asyncio.run(run())
