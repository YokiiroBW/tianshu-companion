import pytest

from tianshu_companion.memory_admission import empty_extraction_reason


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
