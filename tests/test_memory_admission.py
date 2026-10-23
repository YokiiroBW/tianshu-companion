import pytest

from tianshu_companion.memory_admission import empty_extraction_reason, extraction_review_hints


@pytest.mark.parametrize(
    "text",
    [
        "狗叫什么来着？",
        "刚才书店门口说哪句话来着？",
        "谁明确同意了？",
        "周六是不是说好从东门进去？",
        "红皮和蓝皮两本现在分别叫什么？",
        "我的门卡平时收在哪儿？",
        "谢谢你！",
        "你好。谢谢！",
        "Where is my card?",
        "我家猫叫什么来着？",
        "我家那只总是叫醒我的狗叫什么？",
        "群里现在有哪些人明确同意北门？",
        "周日书屋最后在哪碰头？",
        "下午还是想喝平时那杯茶，叫什么来着？",
        "我还想翻以前那本书，叫什么来着？",
        "出门前想拿平时那串钥匙，收在哪儿？",
    ],
)
def test_unambiguous_questions_and_greetings_need_no_extraction(text):
    assert empty_extraction_reason([{"text": text}])


@pytest.mark.parametrize(
    "text",
    [
        "我什么都吃。",
        "我在哪都习惯喝红茶。",
        "我不知道哪家的茶好喝。",
        "我的书叫《为什么》。",
        "我的猫叫豆沙，你的呢？",
        "我同意。",
        "我不去了。",
        "周六从北门进吧。",
        "后来换成东门了。",
        "我把卡放在第二层抽屉。",
        "我家猫叫什么都不吃。",
        "My cat is called What.",
        "你好，我住在青岛。",
        "我家的那本书叫为什么。",
        "我的狗叫豆沙，你的狗叫什么？",
        "我叫林弦。",
        "我叫林弦，我现在去洗手。",
        "我的名字是“星‘河’”，我先去拿快递。",
        "下午还是想喝平时那杯茶。",
        "我每天都想喝红茶，杯子在哪儿？",
        "明天想去上次那家店，咱们在门口碰头吧，几点开门？",
        "我还想拿平时那串钥匙，现在收在玄关抽屉，知道了吗？",
        "下午还是想喝平时那杯茶，不过已经换成红茶了，你喝什么？",
    ],
)
def test_statements_names_mixed_messages_and_stances_keep_model_extraction(text):
    assert empty_extraction_reason([{"text": text}]) is None


def test_every_message_must_be_proven_empty():
    assert empty_extraction_reason([{"text": "谁同意了？"}, {"text": "我同意北门。"}]) is None


@pytest.mark.parametrize(
    "text,quote",
    [
        ("钥匙一直放在门边抽屉，我现在去倒垃圾。", "钥匙一直放在门边抽屉"),
        ("我平时坐609路去上班，先去接个电话。", "我平时坐609路去上班"),
        ("先去洗手，我不喜欢香菜。", "我不喜欢香菜"),
    ],
)
def test_mixed_stable_review_preserves_exact_source_clause(text, quote):
    assert extraction_review_hints([dict(index=7, text=text)]) == [
        dict(source_index=7, quote=quote)
    ]


@pytest.mark.parametrize(
    "text",
    [
        "我现在去倒垃圾，杯子先放这儿。",
        "钥匙一直放在门边抽屉吗？我现在去倒垃圾。",
        "如果钥匙一直放在门边抽屉，我现在去倒垃圾。",
        "他说钥匙一直放在门边抽屉，我现在去倒垃圾。",
        "朋友说他的钥匙一直放门边，我现在去洗手。",
        "“钥匙一直放在门边抽屉”，我现在去倒垃圾。",
        "我平时去哪里吃饭呢，我先去接电话。",
        "钥匙一直放在门边抽屉。",
    ],
)
def test_no_empty_review_for_transient_question_quotation_or_hypothesis(text):
    assert extraction_review_hints([dict(index=0, text=text)]) == []


def test_plan_update_hint_has_only_current_source_and_the_known_key():
    text = "我还是想去，几点出发我也没想好。"
    plan = dict(key="event.visit.plan", state="current", superseded=False, value="周日下午去书屋")
    assert extraction_review_hints([dict(index=3, text=text)], [plan]) == [
        dict(source_index=3, quote=text, known_plan_key="event.visit.plan")
    ]
    assert "书屋" not in str(extraction_review_hints([dict(index=3, text=text)], [plan]))


@pytest.mark.parametrize(
    "facts",
    [
        [],
        [dict(key="identity.name", state="current")],
        [dict(key="event.visit.plan", state="historical")],
        [dict(key="event.visit.plan", state="current", superseded=True)],
        [dict(key="event.visit.plan", state="current", valid_to="2026-01-01T00:00:00Z")],
        [dict(key="event.visit.plan", state="current", entity=dict(id="entity:x"))],
        [
            dict(key="event.visit.plan", state="current"),
            dict(key="event.meeting.plan", state="current"),
        ],
        [dict(key="event.visit.plan.departure", state="current")],
        [
            dict(key="event.visit.plan", state="current"),
            dict(key="event.visit.plan.departure", state="current"),
        ],
        [
            dict(key="event.visit.plan", state="current"),
            dict(key="event.meeting", state="current", entity=dict(id="entity:other")),
        ],
    ],
)
def test_unresolved_ended_multiple_or_implicit_entity_plans_do_not_trigger_review(facts):
    text = "我还是想去，几点出发我也没想好。"
    assert extraction_review_hints([dict(index=0, text=text)], facts) == []


@pytest.mark.parametrize(
    "text",
    [
        "你说我能去吗？",
        "如果我能去，几点出发还没定。",
        "朋友说我能去。",
        "我现在去倒垃圾，几点出发还没想好。",
        "谢谢，晚安。",
        "I can swim.",
    ],
)
def test_known_plan_does_not_turn_a_question_report_or_chore_into_update(text):
    plan = dict(key="event.visit.plan", state="current", superseded=False)
    assert extraction_review_hints([dict(index=0, text=text)], [plan]) == []
