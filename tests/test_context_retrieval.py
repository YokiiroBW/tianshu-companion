import json
import unittest

from support import Harness
from tianshu_companion.clients import uid
from tianshu_companion.context_relevance import _is_statement, changes_plan
from tianshu_companion.contracts import Fault, canonical
from tianshu_companion.short_context import ShortContextPolicy


class ContextRetrievalTests(unittest.IsolatedAsyncioTestCase):
    def test_embedded_whether_keeps_uncertainty_but_not_a_question(self):
        for text, expected in [
            ("周六先看看排班，暂时不能确定是否参加。", True),
            ("我还不知道是否参加。", True),
            ("目前无法确认是否出席。", True),
            ("还没决定是否参加，先别算我。", True),
            ("是否参加？", False),
            ("周六是否参加", False),
            ("你知道是否参加？", False),
            ("你不能确定是否参加吗", False),
            ("还不知道是否参加，怎么安排？", False),
            ("我不确定是否参加，你知道吗", False),
        ]:
            with self.subTest(text=text):
                self.assertEqual(_is_statement(text), expected)

    async def test_explicit_self_comparison_keeps_both_original_human_sources(self):
        self.h.core.short_context_policy = ShortContextPolicy(max_turns=2)
        for author, text in [
            ("a", "我叫青芷，我家狗叫桑团，刚把球推到门边。"),
            ("b", "我叫棠舟，我家猫叫榆饼，现在趴在窗台上。"),
            ("a", "柏迟家那只猫叫什么来着？"),
            ("b", "我家的猫叫什么、刚才在哪里？"),
        ]:
            await self.say(text, group=True, channel="group:compare", account=author)
        await self.say(
            "我和棠舟养的动物分别叫什么？刚才球和猫在哪儿？",
            group=True,
            channel="group:compare",
            account="a",
        )
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertEqual(len(recent), 2)
        self.assertIn("狗叫桑团，刚把球推到门边", canonical(recent))
        self.assertIn("猫叫榆饼，现在趴在窗台", canonical(recent))
        self.assertNotIn("柏迟", canonical(recent))
        self.assertTrue(all(group["input_only"] for group in recent))

    async def test_unknown_comparison_or_bare_respectively_does_not_add_an_older_self_source(self):
        self.h.core.short_context_policy = ShortContextPolicy(max_turns=2)
        for text in ("我和未见过的人分别叫什么？", "这几样分别叫什么？"):
            channel = "group:" + uid("compare")
            for author, source in [
                ("a", "我的旅行箱放在门边。"),
                ("b", "我刚把窗帘拉开。"),
                ("a", "早餐吃什么？"),
                ("b", "下午怎么安排？"),
            ]:
                await self.say(source, group=True, channel=channel, account=author)
            await self.say(text, group=True, channel=channel, account="a")
            recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
            self.assertNotIn("旅行箱", canonical(recent))

    async def test_collective_confirmation_keeps_uncertainty_as_the_original_statement(self):
        for author, text in [
            ("a", "我叫温槐，计划周六下午去槐溪展馆，入口还没选。"),
            ("b", "我叫闻舟，周六先看看排班，暂时不能确定是否参加。"),
        ]:
            await self.say(text, group=True, channel="group:uncertain", account=author)
        await self.say(
            "所以周六我们俩都能去吧？", group=True, channel="group:uncertain", account="a"
        )
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertEqual(len(recent), 2)
        self.assertIn("计划周六下午去槐溪展馆，入口还没选", canonical(recent))
        self.assertIn("暂时不能确定是否参加", canonical(recent))
        self.assertTrue(all(group["input_only"] for group in recent))

    async def test_old_assent_requires_the_later_uncertain_source_to_fit(self):
        for author, text in [
            ("a", "周六去青石馆，我同意参加。"),
            ("b", "周六我也参加。"),
            ("a", "周六我还不能确定是否参加。" + "排班还没有出来。" * 500),
        ]:
            await self.say(text, group=True, channel="group:uncertain-budget", account=author)
        self.h.core.short_context_policy = ShortContextPolicy(max_bytes=3300)
        await self.say(
            "周六我们俩都能去吧？", group=True, channel="group:uncertain-budget", account="b"
        )
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("周六我也参加", canonical(recent))
        self.assertNotIn("我同意参加", canonical(recent))

    async def test_verified_same_persons_second_account_exit_still_protects_older_state(self):
        person = uid("person")
        for account in ("a", "alias"):
            self.h.memory.accounts[
                tuple(sorted(dict(namespace="qq", immutable_account_id=account).items()))
            ] = person
        for author, text in [
            ("a", "周日去青石馆，九点东门集合，算我一个。"),
            ("b", "我也同意九点东门这个安排。"),
            ("alias", "周日我去不了了，你们照常。" + "临时工作还没有处理完。" * 500),
        ]:
            await self.say(text, group=True, channel="group:linked-state", account=author)
        self.h.core.short_context_policy = ShortContextPolicy(max_bytes=3300)
        await self.say(
            "现在谁确定去青石馆？", group=True, channel="group:linked-state", account="b"
        )
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("我也同意九点东门", canonical(recent))
        self.assertNotIn("算我一个", canonical(recent))

    async def test_older_assent_cannot_outlive_its_oversized_later_exit(self):
        for author, text in [
            ("a", "周日去青石馆，九点东门集合，算我一个。"),
            ("b", "我也同意九点东门这个安排。"),
            ("a", "周日我去不了了，你们照常。" + "临时工作还没有处理完。" * 500),
        ]:
            await self.say(text, group=True, channel="group:state-budget", account=author)
        self.h.core.short_context_policy = ShortContextPolicy(max_bytes=3300)
        await self.say(
            "现在谁确定去青石馆？", group=True, channel="group:state-budget", account="b"
        )
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("我也同意九点东门", canonical(recent))
        self.assertNotIn("算我一个", canonical(recent))

    async def test_older_assent_is_not_reintroduced_when_latest_source_verification_fails(self):
        for author, text in [
            ("a", "周日去青石馆，九点东门集合，算我一个。"),
            ("b", "我也同意九点东门这个安排。"),
            ("a", "周日我去不了了，你们照常。"),
        ]:
            await self.say(text, group=True, channel="group:state-check", account=author)
        latest = self.h.turns()[-1]["id"]
        verify = self.h.core._verify_checks

        async def stale(turn, checks, **kwargs):
            if any(check.get("source_turn_id") == latest for check in checks):
                raise Fault("scope_changed")
            return await verify(turn, checks, **kwargs)

        self.h.core._verify_checks = stale
        await self.say("现在谁确定去青石馆？", group=True, channel="group:state-check", account="b")
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn("我也同意九点东门", canonical(recent))
        self.assertNotIn("算我一个", canonical(recent))

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
        self.assertNotIn("human_source_view", payload)
        self.assertEqual(
            payload["messages"][0]["speaker_ref"], recent[0]["messages"][0]["speaker_ref"]
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

    async def test_exit_after_status_questions_keeps_other_authors_and_original_plan(self):
        rows = [
            ("a", "下周日早上去槐月馆逛逛吧，先约北门。"),
            ("b", "北门那边施工，我觉得东门进去更顺。"),
            ("a", "可以，东门九点，就这么安排。"),
            ("c", "我得看排班，先别算上我。"),
            ("b", "我那边也没问题，东门九点。"),
            ("c", "已经都说好了吗？"),
            ("a", "现在确定的是哪几位，走哪里，几点？"),
            ("b", "那天临时要值班，我不参加这次了。"),
        ]
        self.h.gateway.segments = ["这是助手的旧总结，不能作为人类表态。"]
        for author, text in rows:
            await self.say(text, group=True, channel="group:source-links", account=author)
        await self.say(
            "现在还有谁确定参加，时间地点变了吗？",
            group=True,
            channel="group:source-links",
            account="c",
        )
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        for expected in (rows[0][1], rows[2][1], rows[3][1], rows[7][1]):
            self.assertIn(expected, canonical(recent))
        self.assertNotIn(rows[5][1], canonical(recent))
        self.assertNotIn("这是助手的旧总结", canonical(recent))
        self.assertLessEqual(len(recent), 4)

    async def test_link_hints_never_revive_retracted_or_other_channel_sources(self):
        await self.say(
            "周日去栖风馆吧，九点在东门见。",
            group=True,
            channel="group:links",
            account="a",
            message="old:proposal",
        )
        await self.say("行，九点东门，我这边定了。", group=True, channel="group:links", account="b")
        await self.say("我临时有事，先不参加了。", group=True, channel="group:links", account="b")
        await self.h.ingest(
            message="old:proposal",
            revision=2,
            kind="retract",
            group=True,
            channel="group:links",
            account="a",
        )
        await self.say("现在谁确定参加？", group=True, channel="group:links", account="b")
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertNotIn("栖风馆", canonical(recent))
        await self.say("现在谁确定参加？", group=True, channel="group:unrelated", account="b")
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertEqual([], recent)

    async def test_two_plans_keep_each_current_source_and_not_only_a_repeated_question(self):
        rows = [
            ("a", "周六上午去松桥馆，十点西门集合。"),
            ("b", "松桥馆的安排我可以。"),
            ("c", "周日下午去海边骑车，我从南堤出发。"),
            ("a", "骑车我先不确定，要看天气。"),
            ("b", "松桥馆那趟我不去了，临时要加班。"),
        ]
        for author, text in rows:
            await self.say(text, group=True, channel="group:two-plans", account=author)
        await self.say(
            "谁确定参加松桥馆那次，最后约在哪儿？",
            group=True,
            channel="group:two-plans",
            account="c",
        )
        recent = json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]
        self.assertIn(rows[0][1], canonical(recent))
        self.assertIn(rows[4][1], canonical(recent))
        self.assertLessEqual(len(recent), 4)
