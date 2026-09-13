import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from support import Harness
from tianshu_companion.clients import command, uid
from tianshu_companion.contracts import Fault, canonical
from tianshu_companion.short_context import ShortContextPolicy, select_recent


class ShortContextTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness(silence_ms=0)

    async def asyncTearDown(self):
        await self.h.core.close()

    def prompt(self, call=-1):
        return json.loads(self.h.gateway.calls[call][1][1]["content"])

    async def say(self, text, **kwargs):
        await self.h.ingest(text=text, **kwargs)
        await self.h.cycles()

    async def test_name_and_pronoun_followups_use_complete_recent_inputs_and_sent_replies(self):
        h = self.h
        h.gateway.segments = ["小明你好，收到你的名字。"]
        await self.say("我叫小明", message="source:name")
        first = h.turns()[0]
        self.assertEqual("sent", first["phase"])
        h.gateway.segments = ["你叫小明。"]
        await self.say("我叫什么名字")
        recent = self.prompt()["recent_dialogue"]
        self.assertEqual([first["id"]], [g["turn_id"] for g in recent])
        self.assertEqual("我叫小明", recent[0]["messages"][0]["parts"][0]["text"])
        self.assertEqual("a", recent[0]["messages"][0]["author"]["immutable_account_id"])
        self.assertEqual("source:name", recent[0]["messages"][0]["message_key"]["message_id"])
        self.assertEqual("小明你好，收到你的名字。", recent[0]["replies"][0]["text"])
        self.assertEqual("sent", recent[0]["replies"][0]["state"])
        await self.say("那你怎么称呼我呢")
        self.assertEqual(2, len(self.prompt()["recent_dialogue"]))
        self.assertIn("你叫小明。", canonical(self.prompt()["recent_dialogue"]))
        self.assertTrue(all(s["budget"] == dict(tokens=0, bytes=0) for s in h.memory.selections))
        self.assertEqual(3, len(h.gateway.calls))

    async def test_same_conversation_window_survives_database_reopen(self):
        await self.h.core.close()
        with tempfile.TemporaryDirectory() as directory:
            self.h = h = Harness(Path(directory) / "recent.db", silence_ms=0)
            h.gateway.segments = ["小明你好"]
            await self.say("我叫小明")
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            await self.say("我叫什么名字")
            self.assertIn("我叫小明", canonical(self.prompt()["recent_dialogue"]))
            self.assertIn("小明你好", canonical(self.prompt()["recent_dialogue"]))
            await h.core.close()
            self.h = Harness(silence_ms=0)

    async def test_scope_isolation_covers_other_private_group_thread_person_and_role(self):
        h = self.h
        h.gateway.segments = ["私聊回复"]
        await self.say("只在这里知道的名字", message="source:private")
        for kwargs in [
            dict(channel="private:other"),
            dict(group=True, channel="group:1"),
            dict(account="b"),
            dict(actor="actor:b"),
        ]:
            with self.subTest(kwargs=kwargs):
                await self.say("我是谁", **kwargs)
                self.assertNotIn("只在这里知道的名字", canonical(self.prompt()["recent_dialogue"]))
        request = h.request(text="不同线程")
        request["message_key"]["channel"]["thread_id"] = "thread:2"
        # The fixture issuer shares the channel object and independently verifies it.
        await h.core.ingest("nonebot", request)
        await h.cycles()
        self.assertEqual([], self.prompt()["recent_dialogue"])
        await self.say("群成员A资料", group=True, channel="group:shared", account="a")
        await self.say("我是群成员B", group=True, channel="group:shared", account="b")
        recent = self.prompt()["recent_dialogue"]
        self.assertEqual("a", recent[0]["author"]["immutable_account_id"])
        self.assertIn("群成员A资料", canonical(recent))
        self.assertNotIn("只在这里知道的名字", canonical(recent))

    async def test_t2_uses_t1_input_without_waiting_for_or_reading_its_draft(self):
        h = self.h
        h.gateway.segments = ["未发草稿中独有的短语"]
        h.gateway.gates[1] = asyncio.Event()
        await self.say("T1用户明确说的话")
        await self.say("普通代词续问，它是什么")
        self.assertEqual([1, 2], [seq for seq, _ in h.gateway.calls])
        recent = self.prompt()["recent_dialogue"]
        self.assertIn("T1用户明确说的话", canonical(recent))
        self.assertNotIn("未发草稿中独有的短语", canonical(recent))
        self.assertEqual([], recent[0]["replies"])
        self.assertEqual("ready_to_send", h.turns()[1]["phase"])
        self.assertFalse(h.sender.calls)
        h.gateway.gates[1].set()
        await h.cycles(60)
        self.assertEqual([1, 2], [r["turn_sequence"] for r in h.sender.calls])

    async def test_sending_unknown_and_pending_segments_are_excluded_but_sent_is_preserved(self):
        h = self.h
        h.gateway.segments = ["已确认第一段", "未知第二段", "从未发送第三段"]
        h.sender.states = ["sent", "unknown"]
        await self.say("这是一组已受理输入")
        self.assertEqual("reconciling", h.turns()[0]["phase"])
        await self.say("接着说")
        recent = self.prompt()["recent_dialogue"]
        self.assertIn("已确认第一段", canonical(recent))
        self.assertNotIn("未知第二段", canonical(recent))
        self.assertNotIn("从未发送第三段", canonical(recent))
        self.assertEqual("unknown", recent[0]["delivery_state"])
        self.assertEqual(1, len(recent[0]["replies"]))

    async def test_inflight_send_is_not_a_delivered_reply(self):
        h = self.h
        h.gateway.segments = ["发送中不可读的回复"]
        h.sender.gate = asyncio.Event()
        await self.say("已受理输入")
        self.assertEqual("sending", h.turns()[0]["phase"])
        await self.say("普通后续句")
        self.assertEqual([], self.prompt()["recent_dialogue"][0]["replies"])
        self.assertNotIn("发送中不可读的回复", canonical(self.prompt()["recent_dialogue"]))

    async def test_retraction_invalidates_derived_history_without_undoing_sent_facts(self):
        h = self.h
        h.gateway.segments = ["旧名小明"]
        await self.say("我的旧名是小明", message="source:name")
        await self.say("你记住了吗")
        self.assertIn("小明", canonical(self.prompt()["recent_dialogue"]))
        await h.ingest(message="source:name", revision=2, kind="retract")
        h.gateway.segments = ["新回答"]
        await self.say("我叫什么名字")
        self.assertEqual([], self.prompt()["recent_dialogue"])
        self.assertTrue(all(t["phase"] == "sent" for t in h.turns()))
        self.assertTrue(
            any(
                r["text"] == "旧名小明" and r["state"] == "sent"
                for r in h.core.store.list("replies")
            )
        )

    async def test_edit_replaces_history_and_invalidates_already_generated_dependent_reply(self):
        h = self.h
        h.gateway.segments = ["旧名字回复"]
        await self.say("旧名小明", message="source:name")
        h.gateway.gates[2] = asyncio.Event()
        await self.say("现在你记住我了吗")
        self.assertIn("旧名小明", canonical(self.prompt()["recent_dialogue"]))
        h.gateway.segments = ["新的回复"]
        await h.ingest(text="新名小红", message="source:name", revision=2, kind="edit")
        h.gateway.gates[2].set()
        await h.cycles(80)
        self.assertEqual("failed", h.turns()[1]["phase"])
        self.assertEqual("scope_changed", h.turns()[1]["failure"])
        await self.say("我现在叫什么")
        recent = canonical(self.prompt()["recent_dialogue"])
        self.assertIn("新名小红", recent)
        self.assertNotIn("旧名小明", recent)
        self.assertNotIn("旧名字回复", recent)

    async def test_memory_scope_revision_forget_and_dependency_failure_never_reuse_private_history(
        self,
    ):
        h = self.h
        h.gateway.segments = ["旧私人信息的衍生回复"]
        await self.say("只在旧范围有效的信息")
        h.memory.scope_version = 2  # Authoritative revision/forget increments the scope version.
        await self.say("你刚才说什么")
        self.assertEqual([], self.prompt()["recent_dialogue"])
        h.memory.unavailable = True
        count = len(h.gateway.calls)
        with self.assertRaises(Fault):
            await self.say("服务不可用时也不要恢复缓存")
        self.assertEqual(count, len(h.gateway.calls))

    async def test_permission_revocation_invalidates_history_and_keeps_delivery_ledger(self):
        h = self.h
        await self.say("旧权限内的消息")
        turn = h.turns()[0]
        request = dict(
            command=command(turn["origin"], uid("revoke"), h.clock()),
            conversation_id=turn["conversation_id"],
            turn_id=turn["id"],
            expected_version=turn["version"],
            reason="permission_revoked",
        )
        result = await h.core.cancel("nonebot", request)
        self.assertEqual("too_late", result["state"])
        await self.say("后来的消息")
        self.assertEqual([], self.prompt()["recent_dialogue"])

    async def test_explicit_plan_dependency_cannot_bypass_forgetting(self):
        h = self.h
        h.gateway.segments = ["旧权限内的具体方案"]
        await self.say("给我方案")
        h.memory.scope_version = 2
        await self.say("按你刚才的方案办")
        self.assertEqual(1, len(h.gateway.calls))
        self.assertEqual("failed", h.turns()[1]["phase"])
        self.assertEqual("scope_changed", h.turns()[1]["failure"])

    async def test_continuation_cannot_reintroduce_fragment_after_scope_revision(self):
        h = self.h
        from dataclasses import replace

        h.core.policy = replace(h.core.policy, silence_ms=5000, max_collection_messages=1)
        await h.ingest(text="旧范围片段")
        with self.assertRaises(Fault):
            await h.ingest(text="未受理的后半句")
        await h.cycles()
        self.assertEqual("observed", h.turns()[0]["phase"])
        h.memory.scope_version = 2
        await h.ingest(text="现在接着说")
        h.clock.advance(5)
        await h.cycles()
        self.assertFalse(h.gateway.calls)
        self.assertEqual("scope_changed", h.turns()[1]["failure"])

    async def test_whole_group_byte_boundary_turn_limit_age_and_indexed_window(self):
        h = self.h
        h.gateway.segments = ["reply"]
        # Two messages in the first group must remain intact at the budget boundary.
        from dataclasses import replace

        h.core.policy = replace(h.core.policy, silence_ms=5000)
        await h.ingest(text="第一句")
        await h.ingest(text="第二句")
        h.clock.advance(5)
        await h.cycles()
        await h.ingest(text="再问")
        h.clock.advance(5)
        await h.cycles()
        payload = self.prompt()["recent_dialogue"]
        self.assertEqual(2, len(payload[0]["messages"]))
        size = len(canonical(payload).encode("utf-8"))
        turn = h.turns()[1]
        whole, meta = select_recent(
            h.core.store, turn, ShortContextPolicy(max_bytes=size), h.clock()
        )
        self.assertEqual(payload, whole)
        self.assertEqual(size, meta["bytes_used"])
        empty, meta = select_recent(
            h.core.store, turn, ShortContextPolicy(max_bytes=size - 1), h.clock()
        )
        self.assertEqual([], empty)
        self.assertEqual(0, meta["bytes_used"])
        h.core.policy = replace(h.core.policy, silence_ms=0)
        h.core.short_context_policy = ShortContextPolicy(max_turns=2)
        for i in range(4):
            await self.say(f"后续完整组{i}")
        self.assertEqual(2, len(self.prompt()["recent_dialogue"]))
        queries = []
        latest_turn = h.turns()[-1]
        h.core.store.db.set_trace_callback(queries.append)
        select_recent(h.core.store, latest_turn, h.core.short_context_policy, h.clock())
        h.core.store.db.set_trace_callback(None)
        recent_query = next(q for q in queries if "FROM turns WHERE" in q)
        self.assertIn("ORDER BY position DESC LIMIT 4", recent_query)
        h.clock.advance(1800)
        await self.say("久以后")
        self.assertEqual([], self.prompt()["recent_dialogue"])
