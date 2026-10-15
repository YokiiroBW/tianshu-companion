import asyncio
import unittest

import httpx
from support import Harness
from tianshu_companion.clients import JsonService
from tianshu_companion.contracts import Fault


class CongestionTests(unittest.IsolatedAsyncioTestCase):
    async def test_preparation_is_globally_bounded_and_queued_conversations_drain(self):
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        gate = asyncio.Event()
        h.gateway.gates[1] = gate
        for i in range(12):
            account = str(1000 + i)
            await h.ingest(account=account, channel="private:" + account, text="刚泡了一杯茶")
        await h.cycles()
        self.assertEqual(len(h.gateway.calls), h.core.preparation_slots)
        self.assertEqual(sum(t["phase"] == "queued" for t in h.turns()), 8)
        gate.set()
        await h.cycles(180)
        self.assertEqual(len(h.gateway.calls), 12)
        self.assertTrue(all(t["phase"] == "sent" for t in h.turns()))

    async def test_explicit_query_congestion_reuses_request_but_never_replays_a_command(self):
        seen = []

        async def handle(request):
            seen.append((request.content, request.headers))
            if len(seen) == 1:
                return httpx.Response(
                    429, json=dict(code="queue_full", execution_state="not_started", retryable=True)
                )
            return httpx.Response(200, json={"ok": True})

        client = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handle)
        )
        self.addAsyncCleanup(client.close)
        self.assertEqual(
            {"ok": True}, await client.call("/internal/v1/memory/select", {"request_id": "same"})
        )
        self.assertEqual(seen[0], seen[1])
        seen.clear()
        with self.assertRaises(Fault):
            await client.call("/internal/v1/identity/register", {"request_id": "command"})
        self.assertEqual(1, len(seen))

    async def test_preparation_recovers_without_replaying_model(self):
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        select = h.memory.select
        attempts = 0

        async def busy(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise Fault("queue_full")
            return await select(*args, **kwargs)

        h.memory.select = busy
        await h.ingest(text="我平时喝茶")
        await h.cycles()
        self.assertEqual([], h.gateway.calls)
        self.assertEqual("preparing", h.turns()[0]["phase"])
        h.clock.advance(1)
        await h.cycles()
        self.assertEqual("sent", h.turns()[0]["phase"])
        self.assertEqual(1, len(h.gateway.calls))

    async def test_model_failure_is_not_replayed_and_preparation_retries_are_bounded(self):
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        h.gateway.fail = True
        await h.ingest(text="早上好")
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertEqual(1, len(h.gateway.calls))
        h.gateway.fail = False

        async def busy(*args, **kwargs):
            raise Fault("queue_full")

        h.memory.select = busy
        await h.ingest(text="再来一句")
        for _ in range(8):
            await h.cycles()
            h.clock.advance(3)
        self.assertEqual("failed", h.turns()[-1]["phase"])
        self.assertGreater(h.turns()[-1]["preparation_retries"], 0)
        self.assertLessEqual(h.turns()[-1]["preparation_retries"], 5)
