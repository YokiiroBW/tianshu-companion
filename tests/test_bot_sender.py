"""Explicit bot binding selection and read-only Platform receipt reconciliation."""

import asyncio
import copy

import httpx
import pytest

from support import FakeSender, Harness
from tianshu_companion.clients import BotSenderRouter, JsonService, PlatformBotSender
from tianshu_companion.contracts import Fault


class AvailableSender(FakeSender):
    available = True


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
