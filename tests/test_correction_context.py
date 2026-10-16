import json
import unittest

from support import Harness
from tianshu_companion.contracts import canonical, Fault
from tianshu_companion.context import scope_check, TEXT_INPUT_DOMAIN
from tianshu_companion.short_context import ShortContextPolicy


class CorrectionContextTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness(silence_ms=0)

    async def asyncTearDown(self):
        await self.h.core.close()

    async def say(self, text, account="a", channel="group:plans"):
        await self.h.ingest(text=text, group=True, channel=channel, account=account)
        await self.h.cycles()
        return self.h.turns()[-1]

    def recent(self):
        return json.loads(self.h.gateway.calls[-1][1][1]["content"])["recent_dialogue"]

    async def history(self):
        self.h.gateway.segments = ["助手自作主张：碰面后去三楼窗边。"]
        anchor = await self.say("周六去榆树公园碰面，从南门进。")
        self.h.gateway.segments = ["普通回复。"]
        await self.say("我顺路带点果汁。", "b")
        correction = await self.say("看了下路线，咱们改成北门吧，离车站近。")
        for text in ("衣服该收了", "晚饭煮了面", "耳机线缠住了", "热水晾凉了"):
            await self.say(text, "b")
        return anchor, correction

    async def test_topic_return_keeps_change_and_intervening_speaker_without_bot_invention(self):
        anchor, correction = await self.history()
        await self.say("周六公园碰面的入口是哪边？", "b")
        groups = self.recent()
        ids = [g["turn_id"] for g in groups]
        self.assertIn(anchor["id"], ids)
        self.assertIn(correction["id"], ids)
        self.assertIn("带点果汁", canonical(groups))
        self.assertNotIn("三楼窗边", canonical(groups))
        self.assertLessEqual(len(groups), 4)
        self.assertLessEqual(len(canonical(groups).encode()), 8192)
        self.assertEqual(
            ids, sorted(ids, key=lambda key: self.h.core.store.get("turns", key)["sequence"])
        )
        self.assertTrue(all(g.get("input_only") and not g["replies"] for g in groups))
        self.assertNotEqual(groups[0]["person_id"], groups[1]["person_id"])
        await self.say("周六公园碰面的入口是哪边？", "b", "group:other")
        self.assertNotIn("榆树公园", canonical(self.recent()))
        self.assertNotIn("北门", canonical(self.recent()))

    async def test_unavailable_correction_cannot_leave_its_obsolete_anchor(self):
        anchor, correction = await self.history()
        self.h.origins.values[correction["origin"]["assertion_ref"]]["revoked"] = True
        await self.say("周六公园碰面的入口是哪边？", "b")
        ids = {g["turn_id"] for g in self.recent()}
        self.assertNotIn(anchor["id"], ids)
        self.assertNotIn(correction["id"], ids)

    async def test_tight_budget_omits_whole_anchor_change_batch(self):
        anchor, correction = await self.history()
        self.h.core.short_context_policy = ShortContextPolicy(max_bytes=1500)
        await self.say("周六公园碰面的入口是哪边？", "b")
        ids = {g["turn_id"] for g in self.recent()}
        self.assertNotIn(anchor["id"], ids)
        self.assertNotIn(correction["id"], ids)

    async def test_multiple_plans_and_speakers_stay_separate_in_the_returned_context(self):
        await self.say("周六我去榆树公园，先从南门进。", "a")
        await self.say("我周日去的是银杏公园，从西门进。", "b")
        await self.say("我周日那个改成东门，周六那场没改。", "b")
        for text in ("衣服该收了", "晚饭煮了面", "耳机线缠住了", "热水晾凉了"):
            await self.say(text, "b")
        await self.say("周六去榆树公园该从哪个门进？", "a")
        groups = self.recent()
        self.assertIn("周六那场没改", canonical(groups))
        self.assertIn("周日去的是银杏公园", canonical(groups))
        self.assertNotEqual(groups[0]["person_id"], groups[1]["person_id"])

    async def test_successive_changes_remain_chronological(self):
        await self.say("周六榆树公园南门碰头。")
        await self.say("改成东门吧。")
        await self.say("东门施工，换到北门。")
        for text in ("衣服该收了", "晚饭煮了面", "耳机线缠住了", "热水晾凉了"):
            await self.say(text, "b")
        await self.say("周六榆树公园最后从哪个门进？")
        texts = canonical(self.recent())
        self.assertLess(texts.index("南门碰头"), texts.index("改成东门"))
        self.assertLess(texts.index("改成东门"), texts.index("换到北门"))

    async def test_natural_followup_without_change_keyword_is_not_lost(self):
        await self.say("周三竹影馆北边入口碰头。")
        await self.say("我带上相机。", "b")
        await self.say("北边正在维修，我从西边进，你到了也往那边走。")
        for text in ("衣服该收了", "晚饭煮了面", "耳机线缠住了", "热水晾凉了"):
            await self.say(text, "b")
        await self.say("周三竹影馆我们在哪边碰头？", "b")
        self.assertIn("北边正在维修，我从西边进", canonical(self.recent()))
        self.assertIn("周三竹影馆", canonical(self.recent()))

    async def test_source_only_projection_does_not_reuse_unshown_answer_dependencies(self):
        anchor, correction = await self.history()
        anchor["context_checks"] = [{"unavailable_answer_dependency": True}]
        self.h.core.store.put("turns", anchor)
        await self.say("周六公园碰面的入口是哪边？", "b")
        ids = {g["turn_id"] for g in self.recent()}
        self.assertIn(anchor["id"], ids)
        self.assertIn(correction["id"], ids)
        self.assertNotIn("三楼窗边", canonical(self.recent()))

    async def test_overlapping_oversized_context_cannot_resurrect_a_partial_old_plan(self):
        anchor = await self.say("周六公园南门碰头。")
        await self.say("顺手带点果汁。", "b")
        await self.say("改成北门，走小路过去。")
        await self.say("我把相机装包里了。", "b")
        await self.say("周六公园见面地点和入口路线。" + "这段是旁人的长篇行程介绍。" * 700, "b")
        await self.say("最后改成西门吧。")
        for text in ("衣服该收了", "晚饭煮了面", "耳机线缠住了", "热水晾凉了"):
            await self.say(text, "b")
        await self.say("周六公园见面地点和入口路线再说一下？", "b")
        self.assertNotIn(anchor["id"], {g["turn_id"] for g in self.recent()})

    async def test_explicit_question_about_old_bot_steps_retains_confirmed_reply(self):
        self.h.gateway.segments = ["先经过白石桥，再沿青松路到入口。"]
        await self.say("周六去竹影馆的路线怎么走？")
        self.h.gateway.segments = ["普通回复。"]
        for text in (
            "衣服该收了",
            "晚饭煮了面",
            "耳机线缠住了",
            "热水晾凉了",
            "给花浇水",
            "书读完了",
        ):
            await self.say(text, "b")
        await self.say("你之前给我列的路线步骤是什么？")
        self.assertIn("先经过白石桥，再沿青松路到入口", canonical(self.recent()))

    async def test_repeated_question_cannot_displace_its_less_lexical_human_source(self):
        self.h.gateway.segments = ["普通回复。"]
        await self.say("周日去松林书店吧，到门口碰头时就说月光纸船。")
        for text in ("衣服该收了", "晚饭煮了面", "耳机线缠住了"):
            await self.say(text, "b")
        await self.say("刚才书店门口说哪句话来着？")
        for text in ("热水晾凉了", "给花浇水", "书读完了", "雨停了"):
            await self.say(text, "b")
        self.h.gateway.segments = ["助手此前误答：没查到那句话。"]
        await self.say("咱们书店碰头时要说什么？")
        self.h.gateway.segments = ["普通回复。"]
        await self.say("我家那只狗叫什么？", "b")
        await self.say("我的门卡平时在哪儿？", "c")
        await self.say("书店碰头的那句话再提醒一下。")
        text = canonical(self.recent())
        self.assertIn("月光纸船", text)
        self.assertNotIn("助手此前误答", text)

    async def test_long_replies_cannot_crowd_out_a_source_within_the_turn_window(self):
        self.h.gateway.segments = ["这是助手对旁枝话题的长篇补充。" * 90]
        anchor = await self.say("周日去松林书店，碰头时说月光纸船。")
        for text in ("我家狗叫糯米", "门卡放在抽屉", "面包烤焦了一点"):
            await self.say(text, "b")
        await self.say("刚才书店门口说哪句话来着？")
        groups = self.recent()
        self.assertIn(anchor["id"], {g["turn_id"] for g in groups})
        self.assertIn("月光纸船", canonical(groups))
        self.assertNotIn("长篇补充", canonical(groups))
        self.assertLessEqual(len(groups), 4)
        self.assertLessEqual(len(canonical(groups).encode()), 8192)

    async def test_correction_anchor_keeps_preceding_names_for_current_and_old_name_queries(self):
        self.h.gateway.segments = ["这是助手对旁枝话题的长篇补充。" * 90]
        original = await self.say("桌上红皮笔记本叫《山岚》，蓝皮那本叫《溪桥》。")
        await self.say("蓝皮这本后来改叫《纸鸢》了，红皮的名字没变。")
        await self.say("我家那只叼拖鞋的小家伙叫什么来着？")
        for question in ("红皮和蓝皮两本现在分别叫什么？", "蓝皮这本以前叫什么？"):
            await self.say(question)
            groups = self.recent()
            self.assertIn(original["id"], {g["turn_id"] for g in groups})
            for name in ("山岚", "溪桥", "纸鸢"):
                self.assertIn(name, canonical(groups))
            self.assertNotIn("长篇补充", canonical(groups))

    async def test_source_context_survives_fact_revision_but_answers_and_withdrawn_inputs_do_not(
        self,
    ):
        source_epoch = 1

        async def input_history(origin, scope, *, expected=None, known=None):
            if expected is not None and expected["scope_version"] != self.h.memory.scope_version:
                raise Fault("scope_changed")
            snapshot = dict(
                association_version=1,
                history_checks=[
                    dict(
                        scope=scope,
                        history_version=source_epoch,
                        association_id=None,
                        association_version=1,
                    )
                ],
            )
            if known is not None and known != snapshot:
                raise Fault("scope_changed")
            return snapshot

        self.h.memory.input_history = input_history
        self.h.memory.history_version = 1
        anchor, correction = await self.history()
        self.h.memory.scope_version = self.h.memory.history_version = 2
        await self.say("周六公园碰面的入口是哪边？", "b")
        current = self.h.turns()[-1]
        text = canonical(self.recent())
        self.assertIn("榆树公园", text)
        self.assertIn("改成北门", text)
        self.assertNotIn("三楼窗边", text)
        source_check = scope_check(anchor, input_only=True)
        self.assertEqual(source_check["version_domain"], TEXT_INPUT_DOMAIN)
        await self.h.core._verify_checks(current, [source_check])
        with self.assertRaises(Fault):
            await self.h.core._verify_checks(current, [scope_check(anchor)])
        source_epoch = 3
        with self.assertRaises(Fault):
            await self.h.core._verify_checks(current, [source_check])
        source_epoch = 1
        self.h.origins.values[anchor["origin"]["assertion_ref"]]["revoked"] = True
        with self.assertRaises(Fault):
            await self.h.core._verify_checks(current, [source_check])

    async def test_legacy_turn_without_input_snapshot_keeps_strict_history_validation(self):
        anchor, correction = await self.history()
        self.h.memory.scope_version = self.h.memory.history_version = 2
        await self.say("周六公园碰面的入口是哪边？", "b")
        ids = {g["turn_id"] for g in self.recent()}
        self.assertNotIn(anchor["id"], ids)
        self.assertNotIn(correction["id"], ids)
