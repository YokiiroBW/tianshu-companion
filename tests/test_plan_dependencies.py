"""Plan references stay within the current scope despite other actors' turns."""

import asyncio
import json
import unittest
from contextlib import asynccontextmanager

import httpx

from test_profile_context import profile_unit, response_for
from test_source_sync import SourceHarness
from tianshu_companion.clients import JsonService, Memory


@asynccontextmanager
async def scenario():
    h = SourceHarness(silence_ms=0, delivery_reconcile_timeout_ms=1000)
    try:
        yield h
    finally:
        await h.core.close()


async def say(h, text, *, actor="actor:a", group=False, account="a", **kwargs):
    result = await h.submit(h.fanout(text, targets=[actor], group=group, account=account, **kwargs))
    collection = h.core.store.get("collections", result["outcomes"][0]["receipt"]["collection_id"])
    await h.cycles(60)
    return h.core.store.get("turns", collection["turn_id"])


def prompt(h, turn):
    return next(
        content
        for _, messages in h.gateway.calls
        if (content := json.loads(messages[1]["content"]))["messages"][0]["source"]
        == turn["bundle"]["messages"][0]["source"]
    )


class PlanDependencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_private_and_group_actor_interjections_select_own_plan(self):
        for group in (False, True):
            with self.subTest(group=group):
                async with scenario() as h:
                    h.gateway.segments = ["A的已送达方案"]
                    plan = await say(h, "给我一个方案", group=group)
                    h.gateway.segments = ["B的插话"]
                    # More interjections than the short-context lookback. The
                    # scope index selects one candidate without loading them.
                    for _ in range(10):
                        await say(h, "你好", actor="actor:b", group=group)
                    follow = await say(h, "按你刚才的方案继续", group=group)
                    self.assertEqual("sent", follow["phase"])
                    self.assertEqual(
                        [plan["id"]], [d["turn_id"] for d in follow["bundle"]["dependencies"]]
                    )
                    dependencies = prompt(h, follow)["delivered_dependencies"]
                    self.assertEqual(["A的已送达方案"], [d["text"] for d in dependencies])
                    self.assertTrue(all(d["turn_id"] == plan["id"] for d in dependencies))

    async def test_same_actor_other_person_is_not_the_plan_author(self):
        for group in (False, True):
            with self.subTest(group=group):
                async with scenario() as h:
                    plan = await say(h, "给我一个方案", group=group)
                    other = await say(h, "给另一位作者的方案", account="other-person", group=group)
                    self.assertNotEqual(plan["scope"]["person_id"], other["scope"]["person_id"])
                    follow = await say(h, "按你刚才的方案继续", group=group)
                    self.assertEqual("sent", follow["phase"])
                    self.assertEqual(plan["id"], follow["bundle"]["dependencies"][0]["turn_id"])
                    self.assertTrue(
                        all(
                            d["turn_id"] == plan["id"]
                            for d in prompt(h, follow)["delivered_dependencies"]
                        )
                    )

    async def test_unmatched_scope_never_uses_other_actor_audience_or_conversation(self):
        async with scenario() as h:
            await say(h, "群内方案", group=True)
            await say(h, "其他会话方案", channel="other-conversation")
            await say(h, "其他角色方案", actor="actor:b")
            follow = await say(h, "按你刚才的方案继续")
            self.assertEqual("sent", follow["phase"])
            self.assertEqual([], follow["bundle"]["dependencies"])
            self.assertEqual([], prompt(h, follow)["delivered_dependencies"])

    async def test_nearest_invalid_or_unsent_result_never_falls_back_to_older_plan(self):
        cases = (
            "failed",
            "partial",
            "observed",
            "unknown",
            "cancelled",
            "text_version",
            "retract",
            "edit",
            "revoked_origin",
        )
        for group in (False, True):
            for case in cases:
                with self.subTest(group=group, case=case):
                    async with scenario() as h:
                        older = await say(h, "更旧的已送达方案", group=group)
                        if case == "failed":
                            h.gateway.fail = True
                        elif case == "partial":
                            h.sender.states = ["sent", "failed"]
                        elif case == "observed":
                            h.gateway.segments = []
                        elif case == "unknown":
                            h.sender.states = ["unknown"]
                        if case == "cancelled":
                            receipt = await h.submit(
                                h.fanout("最近的方案", group=group, targets=["actor:a"])
                            )
                            collection = h.core.store.get(
                                "collections", receipt["outcomes"][0]["receipt"]["collection_id"]
                            )
                            nearest = h.core.store.get("turns", collection["turn_id"])
                            await h.cancel(nearest)
                        else:
                            nearest = await say(
                                h, "最近的方案", group=group, message="plan:nearest"
                            )
                        h.gateway.fail = False
                        h.gateway.segments = ["合成回复一", "合成回复二"]
                        if case == "unknown":
                            h.clock.advance(1)
                            await h.cycles()
                        elif case == "text_version":
                            h.memory.scope_version += 1
                        elif case in ("retract", "edit"):
                            await h.submit(
                                h.fanout(
                                    "修订后的输入",
                                    group=group,
                                    message="plan:nearest",
                                    revision=2,
                                    kind=case,
                                    targets=[],
                                    grants=[],
                                )
                            )
                        elif case == "revoked_origin":
                            h.origins.values[nearest["origin"]["assertion_ref"]]["revoked"] = True
                        await say(h, "其他角色插话", actor="actor:b", group=group)
                        calls_before = len(h.gateway.calls)
                        follow = await say(h, "按你刚才的方案继续", group=group)
                        self.assertEqual("sent", older["phase"])
                        self.assertEqual(
                            nearest["id"], follow["bundle"]["dependencies"][0]["turn_id"]
                        )
                        self.assertEqual("failed", follow["phase"])
                        self.assertIn(follow["failure"], {"scope_changed", "forbidden"})
                        self.assertEqual(calls_before, len(h.gateway.calls))

    async def test_scoped_predecessor_still_waits_for_inflight_delivery(self):
        for group in (False, True):
            with self.subTest(group=group):
                async with scenario() as h:
                    await say(h, "更旧方案", group=group)
                    h.gateway.gates[2] = asyncio.Event()
                    nearest = await say(h, "新的方案", group=group)
                    other = await say(h, "插话", actor="actor:b", group=group)
                    follow = await say(h, "按你刚才的方案继续", group=group)
                    self.assertEqual("queued", follow["phase"])
                    self.assertEqual("ready_to_send", other["phase"])
                    h.gateway.gates[2].set()
                    # As soon as the second slot opens the follow-up can prepare;
                    # all four turns must still send in the conversation order.
                    await h.cycles(100)
                    follow = h.core.store.get("turns", follow["id"])
                    self.assertEqual("sent", follow["phase"])
                    self.assertEqual(nearest["id"], follow["bundle"]["dependencies"][0]["turn_id"])
                    self.assertEqual(
                        [1, 1, 2, 2, 3, 3, 4, 4], [r["turn_sequence"] for r in h.sender.calls]
                    )

    async def test_inherited_profile_probe_still_stops_send_after_actor_interjection(self):
        async with scenario() as h:
            version = 1

            def handle(incoming):
                request = json.loads(incoming.content)
                if request["known_scope_version"] not in (None, version):
                    return httpx.Response(409, json=dict(code="scope_changed"))
                units = []
                if (
                    request["requester_scope"]["actor_id"] == "actor:a"
                    and request["target"]["kind"] == "group"
                    and request["budget"]["bytes"]
                ):
                    units = [profile_unit(request["target"], "style")]
                return httpx.Response(200, json=response_for(request, units, version))

            client = JsonService(
                "https://profile.fixture.invalid",
                "fixture-only",
                transport=httpx.MockTransport(handle),
            )
            h.memory.profiles = Memory(h.contracts, client).profiles
            try:
                plan = await say(h, "给我一个方案", group=True)
                self.assertTrue(plan["profile_checks"])
                await say(h, "插话", actor="actor:b", group=True)
                h.gateway.gates[3] = asyncio.Event()
                follow = await say(h, "按你刚才的方案继续", group=True)
                self.assertEqual("generating", follow["phase"])
                self.assertEqual(plan["id"], follow["bundle"]["dependencies"][0]["turn_id"])
                self.assertTrue(
                    any(
                        c["version_domain"] == "profile-memory/v1" for c in follow["context_checks"]
                    )
                )
                submitted = len(h.sender.calls)
                version = 2
                h.gateway.gates[3].set()
                await h.cycles(60)
                follow = h.core.store.get("turns", follow["id"])
                self.assertEqual("failed", follow["phase"])
                self.assertEqual("scope_changed", follow["failure"])
                self.assertEqual(submitted, len(h.sender.calls))
            finally:
                await client.close()
