import json
import unittest

from support import Harness
from tianshu_companion.contracts import canonical


class ContextRetrievalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness(silence_ms=0)

    async def asyncTearDown(self):
        await self.h.core.close()

    async def say(self, text, **kwargs):
        await self.h.ingest(text=text, **kwargs)
        await self.h.cycles()

    async def test_old_topic_returns_after_six_interleaved_turns_within_same_group(self):
        await self.say("约好的暗号是青苔雨伞", group=True, channel="group:test", account="a")
        for text in (
            "面包烤好了",
            "我要换键盘",
            "衣服该收了",
            "看完那部电影",
            "刚剪完头发",
            "快去给花浇水",
        ):
            await self.say(text, group=True, channel="group:test", account="b")
        await self.say("我们上次约好的暗号是什么？", group=True, channel="group:test", account="b")
        prompt = json.loads(self.h.gateway.calls[-1][1][1]["content"])
        self.assertIn("青苔雨伞", canonical(prompt["recent_dialogue"]))
        self.assertLessEqual(len(prompt["recent_dialogue"]), 4)
        await self.say("我们上次约好的暗号是什么？", group=True, channel="group:other", account="b")
        prompt = json.loads(self.h.gateway.calls[-1][1][1]["content"])
        self.assertNotIn("青苔雨伞", canonical(prompt["recent_dialogue"]))

    async def test_expired_history_is_not_reintroduced_by_topic_match(self):
        await self.say("约好的暗号是青苔雨伞")
        self.h.clock.advance(1801)
        await self.say("上次暗号是什么？")
        prompt = json.loads(self.h.gateway.calls[-1][1][1]["content"])
        self.assertNotIn("青苔雨伞", canonical(prompt["recent_dialogue"]))

    async def test_source_precedes_repeated_question_and_verbose_recent_answers(self):
        self.h.gateway.segments = ["这是一段较长的已发送回复。" * 30]
        await self.say("周六公园碰面就用青苔雨伞当暗号，别和上次那个混了。")
        await self.say("刚才碰面的暗号是什么呀？")
        for text in ("面包烤好了", "我要换键盘", "衣服该收了", "看完电影", "刚剪头发", "给花浇水"):
            await self.say(text)
        await self.say("咱们刚才约定的暗号是什么？")
        prompt = json.loads(self.h.gateway.calls[-1][1][1]["content"])
        selected = prompt["recent_dialogue"]
        self.assertIn("青苔雨伞", canonical(selected))
        self.assertLessEqual(len(canonical(selected).encode()), 8192)
        self.assertLessEqual(len(selected), 4)
