import asyncio
import json
import unittest
from dataclasses import replace

from support import Harness
from tianshu_companion.clients import command, uid
from tianshu_companion.contracts import canonical, digest


class ContinuationRevisionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness(silence_ms=5000, max_wait_ms=1)
        await self.h.ingest(text="权限内受理的旧片段")
        self.h.clock.advance(0.01)
        await self.h.cycles()
        self.first = self.h.turns()[0]
        self.collection_id = self.first["bundle"]["collection_id"]
        self.assertEqual("observed", self.first["phase"])
        self.assertEqual("max_wait", self.first["bundle"]["close_reason"])
        self.h.core.policy = replace(self.h.core.policy, silence_ms=0, max_wait_ms=None)

    async def asyncTearDown(self):
        await self.h.core.close()

    async def revoke(self):
        h = self.h
        first = h.core.store.get("turns", self.first["id"])
        response = await h.core.cancel(
            "nonebot",
            dict(
                command=command(first["origin"], uid("revoke"), h.clock()),
                conversation_id=first["conversation_id"],
                turn_id=first["id"],
                expected_version=first["version"],
                reason="permission_revoked",
            ),
        )
        self.assertEqual("too_late", response["state"])

    async def complete(self):
        await self.h.ingest(text="新的完整输入")
        await self.h.cycles()

    def prompt(self):
        return json.loads(self.h.gateway.calls[-1][1][1]["content"])

    def remove_old_marker(self):
        with self.h.core.store.transaction():
            collection = self.h.core.store.get("collections", self.collection_id)
            collection.pop("source_context_revision")
            self.h.core.store.put("collections", collection)

    async def test_observed_fragment_revocation_prevents_new_continuation_attachment(self):
        h = self.h
        await self.revoke()
        await self.complete()
        self.assertEqual("sent", h.turns()[1]["phase"])
        self.assertIsNone(h.turns()[1]["bundle"]["continuation_of"])
        self.assertEqual([], self.prompt()["earlier_fragment"])
        self.assertNotIn("权限内受理的旧片段", canonical(self.prompt()))
        original = h.core.store.get("collections", self.collection_id)
        self.assertEqual(1, original["source_context_revision"])
        conv = h.core.store.get("conversations", digest(original["collection_key"]["channel"]))
        self.assertEqual(2, conv["context_revision"])
        fresh = h.core.store.get("collections", h.turns()[1]["bundle"]["collection_id"])
        self.assertEqual(2, fresh["source_context_revision"])

    async def test_valid_observed_fragment_still_continues_as_a_complete_group(self):
        await self.complete()
        self.assertEqual("sent", self.h.turns()[1]["phase"])
        self.assertEqual(self.collection_id, self.h.turns()[1]["bundle"]["continuation_of"])
        self.assertEqual(
            "权限内受理的旧片段", self.prompt()["earlier_fragment"][0]["parts"][0]["text"]
        )
        self.assertEqual("新的完整输入", self.prompt()["messages"][0]["parts"][0]["text"])
        self.assertEqual(1, len(self.h.gateway.calls))

    async def test_already_associated_inflight_chain_cannot_send_after_revocation(self):
        h = self.h
        h.gateway.gates[2] = asyncio.Event()
        await self.complete()
        self.assertEqual("generating", h.turns()[1]["phase"])
        self.assertEqual(self.collection_id, h.turns()[1]["bundle"]["continuation_of"])
        await self.revoke()
        h.gateway.gates[2].set()
        await h.cycles()
        self.assertEqual("failed", h.turns()[1]["phase"])
        self.assertEqual("scope_changed", h.turns()[1]["failure"])
        self.assertFalse(h.sender.calls)

    async def test_legacy_fragment_without_marker_is_not_attached_or_promoted(self):
        self.remove_old_marker()
        await self.complete()
        self.assertEqual("sent", self.h.turns()[1]["phase"])
        self.assertEqual([], self.prompt()["earlier_fragment"])
        self.assertNotIn(
            "source_context_revision", self.h.core.store.get("collections", self.collection_id)
        )

    async def test_already_associated_chain_with_missing_marker_fails_closed(self):
        h = self.h
        h.gateway.gates[2] = asyncio.Event()
        await self.complete()
        self.remove_old_marker()
        h.gateway.gates[2].set()
        await h.cycles()
        self.assertEqual("scope_changed", h.turns()[1]["failure"])
        self.assertFalse(h.sender.calls)
