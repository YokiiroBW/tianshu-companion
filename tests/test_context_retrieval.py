import json
import unittest

from support import Harness
from tianshu_companion.context_relevance import changes_plan
from tianshu_companion.contracts import canonical


class ContextRetrievalTests(unittest.IsolatedAsyncioTestCase):
    def test_change_cues_distinguish_recall_from_new_proposals_and_statements(self):
        for text, expected in [
            ("之前为什么换到大厅？", False),
            ("最后改到哪儿来着？", False),
            ("是不是改到周四了？", False),
            ("Why did we change to the east entrance?", False),
            ("要不换到大厅，怎么样？", True),
            ("现在改到周四吧，可以吗？", True),
            ("Why don't we change to the east entrance?", True),
            ("后来时间又改了，最后定周六。", True),
            ("已经换到南门了，你知道了吗？", True),
            ("那次为什么换到大厅？现在改到二楼了。", True),
            ("我把窗台擦干净了。", False),
        ]:
            with self.subTest(text=text):
                self.assertEqual(changes_plan(text), expected)

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

    async def test_question_about_a_change_does_not_extend_the_required_change_span(self):
        await self.assert_change_reason_keeps_original("这是一段较长的已发送回复。" * 30)

    async def test_recent_correction_keeps_original_even_when_bot_replies_are_short(self):
        await self.assert_change_reason_keeps_original("收到。")

    async def assert_change_reason_keeps_original(self, reply):
        self.h.gateway.segments = [reply]
        rows = [
            ("a", "我的培训原来是周二，地点在南院。"),
            ("a", "周二要看牙，我和老师商量换成周三了。"),
            ("a", "后来日期又改了，最后定周五，地点还是南院。"),
            ("b", "窗台的小花今天开了，颜色挺亮。"),
            ("a", "培训最后哪天来着，改到那天的原因说过吗？"),
        ]
        for account, text in rows:
            await self.say(text, group=True, channel="group:changes", account=account)
        await self.say(
            "之前从周二挪到周三又是为啥？", group=True, channel="group:changes", account="b"
        )
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("我的培训原来是周二", canonical(recent))
        self.assertIn("周二要看牙", canonical(recent))
        self.assertIn("最后定周五", canonical(recent))
        self.assertLessEqual(len(recent), 4)
        self.assertLessEqual(len(canonical(recent).encode()), 8192)

    async def test_fact_question_uses_humans_but_question_about_bot_keeps_its_reply(self):
        invented = "你昨天告诉我它叫团子，所有人都同意了。"
        self.h.gateway.segments = [invented]
        await self.say("昨天傍晚我家的狗把我叫醒了，它叫团子。")
        await self.say("狗叫什么来着？")
        prompt_messages = self.h.gateway.calls[-1][1]
        payload = json.loads(prompt_messages[1]["content"])
        recent = payload["recent_dialogue"]
        self.assertIn("昨天傍晚我家的狗把我叫醒", canonical(recent))
        self.assertNotIn(invented, canonical(recent))
        self.assertTrue(all(g.get("input_only") and not g["replies"] for g in recent))
        self.assertNotIn("昨天傍晚我家的狗把我叫醒", prompt_messages[0]["content"])
        self.assertNotIn(invented, canonical(payload["human_source_view"]))
        self.assertEqual(1, len(payload["human_source_view"]["participants"]))
        self.assertEqual(
            ["昨天傍晚我家的狗把我叫醒了，它叫团子。", "狗叫什么来着？"],
            [m["text"] for m in payload["human_source_view"]["participants"][0]["messages"]],
        )
        await self.say("你刚才怎么回答的？")
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn(invented, canonical(recent))

    async def test_recent_independent_confirmations_survive_verbose_bot_history(self):
        self.h.gateway.segments = ["助手旧的判断不等于人的确认。" * 70]
        for account, text in [
            ("a", "周六槐安馆从北门进吧。"),
            ("b", "我在吃早饭，还没看路线。"),
            ("c", "先拉开窗帘。"),
            ("a", "北门大家都同意了吗？"),
            ("b", "北门没问题，我同意走这条路线。"),
            ("a", "现在是谁明确同意北门了？"),
            ("c", "我也同意从北门进，到时门口见。"),
        ]:
            await self.say(text, group=True, channel="group:consent", account=account)
        # A new confirmation also needs the other person's earlier confirmation;
        # it must not inherit an obsolete bot summary saying that person was silent.
        confirmation = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("北门没问题，我同意", canonical(confirmation))
        self.assertIn("周六槐安馆从北门进吧", canonical(confirmation))
        self.assertNotIn("助手旧的判断", canonical(confirmation))
        await self.say(
            "现在有哪些人明确同意北门？", group=True, channel="group:consent", account="a"
        )
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("北门没问题，我同意", canonical(recent))
        self.assertIn("我也同意从北门进", canonical(recent))
        self.assertIn("周六槐安馆从北门进吧", canonical(recent))
        self.assertNotIn("助手旧的判断", canonical(recent))
        self.assertLessEqual(len(recent), 4)

    async def test_factual_lookup_does_not_retain_another_persons_unrelated_pet(self):
        for account, text in [
            ("a", "昨天傍晚我家的狗把我叫醒了，它叫团子。"),
            ("b", "我下午去溪岸店取东西，只是个打算，还没去。"),
            ("a", "狗叫什么来着？"),
            ("b", "溪岸店那件事，已经办完了吗？"),
        ]:
            await self.say(text, group=True, channel="group:errands", account=account)
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("只是个打算，还没去", canonical(recent))
        self.assertNotIn("团子", canonical(recent))

    async def test_personal_withdrawal_keeps_proposal_and_other_persons_confirmation(self):
        self.h.gateway.segments = ["助手旧的判断不等于人的确认。" * 70]
        for account, text in [
            ("a", "周日下午去青石馆吧，我想从西侧入口进去。"),
            ("b", "我正吃面呢，得晚点看一下安排。"),
            ("c", "刚才楼下挺吵，我关下窗。"),
            ("b", "我看好了，周日下午和西侧入口都行，我同意。"),
            ("a", "现在都有哪些人已经明确同意了？"),
            ("c", "周日下午我有事，我就不去了，你们玩。"),
            ("a", "那现在谁同意去青石馆，谁说去不了了？"),
        ]:
            await self.say(text, group=True, channel="group:withdrawal", account=account)
            if text == "现在都有哪些人已经明确同意了？":
                generic = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
                self.assertIn("我想从西侧入口进去", canonical(generic))
                self.assertIn("西侧入口都行，我同意", canonical(generic))
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        for text in ("我想从西侧入口进去", "西侧入口都行，我同意", "我就不去了"):
            self.assertIn(text, canonical(recent))
        self.assertNotIn("助手旧的判断", canonical(recent))
        self.assertLessEqual(len(recent), 4)
        self.assertLessEqual(len(canonical(recent).encode()), 8192)

    async def test_consent_does_not_separate_a_real_venue_correction_from_its_origin(self):
        for account, text in [
            ("a", "周日下午去青石馆吧，我想从西侧入口进去。"),
            ("b", "我同意，不过西门关闭，改成东侧入口吧。"),
            ("c", "我刚吃完饭。"),
            ("a", "同意去青石馆的安排最后从哪个门进？"),
        ]:
            await self.say(text, group=True, channel="group:consent-change", account=account)
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("我想从西侧入口进去", canonical(recent))
        self.assertIn("西门关闭，改成东侧入口", canonical(recent))

    async def test_elliptical_followup_uses_original_changes_not_only_the_bots_summary(self):
        rows = [
            "周日河岸书屋门口见吧，我拿书过去。",
            "改成二楼楼梯口碰头吧，楼下怕挡着人。",
            "店家说二楼不能等人，那就回门口，我早到一点。",
            "我家猫又窝在纸箱里睡着了。",
            "毛巾刚洗完，晾上了。",
            "下午的风倒是凉快。",
            "周日河岸书屋最后在哪碰头？",
            "我们中间还考虑过哪儿，后来为什么又没用？",
        ]
        self.h.gateway.segments = ["助手遗漏了更正的原因。"]
        for text in rows:
            await self.say(text)
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        for expected in rows[:3]:
            self.assertIn(expected, canonical(recent))
        self.assertNotIn("助手遗漏", canonical(recent))
        await self.say("后来那只猫叫什么？")
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertNotIn(rows[0], canonical(recent))

    async def test_recall_does_not_promote_an_earlier_question_into_the_authors_knowledge(self):
        for text in [
            "我的培训原来是周二，地点在南院。",
            "周二要看牙，我和老师商量换成周三了。",
            "后来日期又改了，最后定周五，地点还是南院。",
            "窗台的小花今天开了，颜色挺亮。",
            "培训最后哪天来着，改到那天的原因说过吗？",
            "之前从周二挪到周三又是为啥？",
        ]:
            await self.say(text)
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("周二要看牙", canonical(recent))
        self.assertIn("最后定周五", canonical(recent))
        self.assertNotIn("改到那天的原因说过吗", canonical(recent))
