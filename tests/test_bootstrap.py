import unittest

import httpx

from support import Harness
from tianshu_companion.app import create_app
from tianshu_companion.clients import JsonService, query, utc
from tianshu_companion.contracts import Fault
from tianshu_companion.store import Store
from tianshu_nonebot import Bridge


class BootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness(silence_ms=0)

    async def asyncTearDown(self):
        await self.h.core.close()

    async def test_unknown_channel_http_receipt_mapping_and_w_zero_race(self):
        h = self.h
        store = Store(":memory:")
        client = JsonService(
            "https://core.synthetic.invalid",
            "synthetic-only",
            transport=httpx.ASGITransport(app=create_app(h.core, {"nonebot": "synthetic-only"})),
        )

        class DelayedReceiptClient:
            async def call(self, path, request):
                result = await client.call(path, request)
                # The HTTP body is delivered but the bridge has not saved its map yet.
                await h.cycles()
                self.observed_phase = h.turns()[0]["phase"]
                self.model_calls = len(h.gateway.calls)
                return result

        delayed = DelayedReceiptClient()
        bridge = Bridge(store, h.contracts, delayed, destinations={}, clock=h.clock)
        original_resolve = h.origins.resolve
        original_select = h.memory.select

        async def resolve(service, envelope, now):
            context = await original_resolve(service, envelope, now)
            context["allowed_scope"]["conversation_id"] = bridge.channel_mapping(
                context["verified_channel"]
            )
            return context

        async def strict_select(origin, scope, text, budget, known_version=None, **kwargs):
            context = await resolve("nonebot", query(origin), h.clock())
            current = context["allowed_scope"]["conversation_id"]
            if current is None:
                raise Fault("dependency_unavailable")
            if current != scope["conversation_id"]:
                raise Fault("forbidden")
            return await original_select(origin, scope, text, budget, known_version)

        h.origins.resolve = resolve
        h.memory.select = strict_select
        request = h.request(account="new-account", channel="new-channel")
        try:
            self.assertIsNone(bridge.channel_mapping(request["message_key"]["channel"]))
            bridge.capture(request)
            await bridge.flush()
            self.assertEqual("preparing", delayed.observed_phase)
            self.assertEqual(0, delayed.model_calls)
            receipt = store.list("inbox")[0]["receipt"]
            self.assertEqual(
                receipt["conversation_id"],
                bridge.channel_mapping(request["message_key"]["channel"]),
            )
            self.assertEqual(1, len(h.memory.accounts))
            h.clock.advance(0.1)
            await h.cycles()
            self.assertEqual("sent", h.turns()[0]["phase"])
            self.assertEqual(1, len(h.gateway.calls))
        finally:
            store.close()
            await client.close()

    async def test_response_gate_and_cancel_during_bounded_mapping_wait(self):
        h = self.h
        request = h.request()
        receipt = await h.core.ingest("nonebot", request, defer_processing=True)
        await h.cycles()
        self.assertEqual("queued", h.turns()[0]["phase"])
        self.assertFalse(h.memory.selections)

        async def unavailable(*args, **kwargs):
            raise Fault("dependency_unavailable")

        h.memory.select = unavailable
        await h.core.acknowledge_ingest(receipt["receipt_id"])
        await h.cycles()
        self.assertEqual("preparing", h.turns()[0]["phase"])
        await h.cancel(h.turns()[0])
        h.clock.advance(10)
        await h.cycles()
        self.assertEqual("cancelled", h.turns()[0]["phase"])
        self.assertFalse(h.gateway.calls)

    async def test_bootstrap_deadline_and_explicit_rejection_are_not_blind_retries(self):
        h = self.h
        request = h.request()
        request["command"]["deadline_at"] = utc(h.clock() + 0.2)
        await h.core.ingest("nonebot", request)
        calls = []

        async def unavailable(*args, **kwargs):
            calls.append(1)
            raise Fault("dependency_unavailable")

        h.memory.select = unavailable
        await h.cycles()
        self.assertEqual(1, len(calls))
        h.clock.advance(0.2)
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertEqual("timeout", h.turns()[0]["failure"])
        self.assertEqual(1, len(calls))

        async def forbidden(*args, **kwargs):
            calls.append(2)
            raise Fault("forbidden")

        h.memory.select = forbidden
        await h.ingest(text="later input")
        await h.cycles()
        self.assertEqual("failed", h.turns()[1]["phase"])
        self.assertEqual([1, 2], calls)

    async def test_non_null_conflicting_source_mapping_is_rejected(self):
        h = self.h
        request = h.request()
        origin = h.origins.values[request["command"]["origin"]["assertion_ref"]]
        origin["allowed_scope"]["conversation_id"] = "conv:untrusted"
        with self.assertRaises(Fault):
            await h.core.ingest("nonebot", request)
        self.assertFalse(h.core.store.list("inbox"))

    async def test_life_readers_deployment_failures_stop_the_app_from_starting(self):
        """A deployment that means to grant reading must not start half-configured."""
        h = self.h
        tokens = {"story": "story-token", "other": "other-token"}
        entry = {"reader_id": "reader:story", "actor_ids": ["actor:a"]}
        self.assertTrue(create_app(h.core, tokens, {"story": entry}))
        for broken in (
            {"nobody": entry},  # a service that holds no credential
            {"story": {"reader_id": "reader:story"}},
            {"story": {"reader_id": "", "actor_ids": ["actor:a"]}},
            {"story": {"reader_id": "reader:story", "actor_ids": []}},
            {"story": {"reader_id": "reader:story", "actor_ids": ["actor:a"] * 65}},
            {"story": {"reader_id": "reader:story", "actor_ids": ["actor:a"] * 2}},
            {"story": dict(entry, extra=True)},
            {"story": entry, "other": entry},  # one reader identity per request, never two
            [entry],
        ):
            with self.assertRaises(ValueError):
                create_app(h.core, tokens, broken)

        async def answer(app, token):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://core"
            ) as client:
                return await client.post(
                    "/internal/v1/life-read/actors",
                    json={"schema_version": 1},
                    headers={"Authorization": "Bearer " + token},
                )

        # Without the section the port is not deployed and says so; with it, only the
        # registered caller is a reader and nobody is enumerated from an empty grant list.
        missing = await answer(create_app(h.core, tokens), "story-token")
        self.assertEqual(503, missing.status_code)
        self.assertEqual("dependency_unavailable", missing.json()["code"])
        deployed = create_app(h.core, tokens, {"story": entry})
        self.assertEqual(403, (await answer(deployed, "other-token")).status_code)
        listed = await answer(deployed, "story-token")
        self.assertEqual(200, listed.status_code)
        self.assertEqual([], listed.json()["items"])
