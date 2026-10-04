import copy
import unittest

import httpx

from test_source_sync import SourceHarness
from tianshu_companion.app import create_app
from tianshu_companion.clients import JsonService
from tianshu_companion.contracts import Fault
from tianshu_companion.store import Store
from tianshu_nonebot import Bridge


class SourceBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_inline_confirm_retry_and_actor_receipt_swap_rejected(self):
        h = SourceHarness(silence_ms=0)
        store = Store(":memory:")
        client = JsonService(
            "https://core.fixture.invalid",
            "fixture",
            transport=httpx.ASGITransport(app=create_app(h.core, {"nonebot": "fixture"})),
        )
        confirmations = []
        original_select = h.memory.select
        mapping = {}

        async def select(origin, scope, text, budget, known_version=None, **kwargs):
            if not mapping:
                raise Fault("dependency_unavailable")
            return await original_select(origin, scope, text, budget, known_version, **kwargs)

        h.memory.select = select

        async def confirm(request, result):
            confirmations.append(copy.deepcopy(result))
            if len(confirmations) == 1:
                await h.cycles()
                self.assertFalse(h.gateway.calls)
                raise Fault("dependency_unavailable")
            for outcome in result["outcomes"]:
                admission = outcome["admission"]
                self.assertEqual(outcome["actor_id"], admission["scope"]["actor_id"])
                mapping[outcome["actor_id"]] = admission["scope"]
                h.origins.values[admission["accepted_origin"]["assertion_ref"]]["allowed_scope"] = (
                    admission["scope"]
                )
            return True

        bridge = Bridge(
            store, h.contracts, client, destinations={}, clock=h.clock, confirm_admissions=confirm
        )
        try:
            request = h.fanout()
            bridge.capture(request)
            await bridge.flush()
            self.assertEqual("pending", store.list("inbox")[0]["state"])
            self.assertIsNotNone(store.list("inbox")[0]["routing_record"])
            h.clock.advance(1)
            await bridge.flush()
            self.assertEqual("accepted", store.list("inbox")[0]["state"])
            self.assertEqual(2, len(h.core.store.list("inbox")))
            self.assertEqual(
                ["duplicate", "duplicate"], [o["state"] for o in confirmations[-1]["outcomes"]]
            )
            await h.cycles(60)
            self.assertEqual(["sent", "sent"], [t["phase"] for t in h.turns()])
            result = copy.deepcopy(confirmations[-1])
            result["outcomes"][0]["receipt"], result["outcomes"][1]["receipt"] = (
                result["outcomes"][1]["receipt"],
                result["outcomes"][0]["receipt"],
            )
            with self.assertRaises(Fault):
                bridge._check_admissions(store.list("inbox")[0]["request"], result, None)
        finally:
            await h.core.close()
            store.close()
            await client.close()

    async def test_missing_confirm_adapter_leaves_request_pending_without_core_call(self):
        h = SourceHarness()
        store = Store(":memory:")

        class NoCall:
            async def call(self, *args):
                raise AssertionError("No accepted mapping adapter")

        bridge = Bridge(store, h.contracts, NoCall(), destinations={}, clock=h.clock)
        try:
            bridge.capture(h.fanout())
            await bridge.flush()
            self.assertEqual("dependency_unavailable", store.list("inbox")[0]["last_error"])
            self.assertFalse(h.core.store.list("inbox"))
        finally:
            await h.core.close()
            store.close()
