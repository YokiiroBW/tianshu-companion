import copy
import json

from support import Harness
from tianshu_companion.context_time import (
    dialogue_time_view,
    reported_times,
    requests_message_clock,
)
from tianshu_companion.qq_identity import dialogue_sources


def message(text, stamp="2026-10-19T00:00:00Z"):
    return dict(
        author=dict(namespace="qq", immutable_account_id="1001"),
        person_id="person:a",
        sent_at=stamp,
        accepted_at=stamp,
        parts=[dict(kind="text", text=text)],
    )


def project(question):
    old = message("昨天傍晚猫把我叫醒了，它叫枣糕。名字不是昨天才取的。")
    current = message(question, "2026-10-20T01:02:03Z")
    prompt = dict(
        messages=[current], earlier_fragment=[], recent_dialogue=[dict(messages=[old], replies=[])]
    )
    sources = dialogue_sources(dict(bundle=dict(messages=[current])), prompt)
    saved = copy.deepcopy(prompt)
    value, timeline = dialogue_time_view(prompt, sources)
    assert prompt == saved
    return value, timeline


def test_name_recall_keeps_verbatim_time_phrase_without_transport_clock_cues():
    value, timeline = project("我的猫叫什么？")
    assert timeline["clock_visibility"] == "not_requested"
    assert "2026-10-" not in json.dumps(value) + json.dumps(timeline)
    source = value["recent_dialogue"][0]["messages"][0]
    phrase = source["reported_time_phrases"][0]
    text = source["parts"][0]["text"]
    assert text[phrase["start"] : phrase["end"]] == "昨天傍晚"
    assert phrase["basis"] == "speaker_reported_wording"
    assert source["source_ref"] == "recent_dialogue:1:m1"
    assert "名字不是昨天才取的" in text


def test_genuine_message_time_question_keeps_verified_offset_in_its_own_provenance():
    value, timeline = project("我是什么时候发消息说猫把我叫醒的？")
    assert timeline["clock_visibility"] == "requested"
    clock = timeline["sources"][1]["transport_clock"]
    assert clock["sent_at"] == "2026-10-19T00:00:00Z"
    assert "not_event_time" in clock["basis"]
    assert "2026-10-" not in json.dumps(value)


def test_reported_clock_is_literal_and_question_does_not_infer_a_local_timezone():
    text = "我00:20说的，昨天傍晚培训改到周三了。"
    assert [m["text"] for m in reported_times(text)] == ["00:20", "昨天傍晚", "周三"]
    assert reported_times("管理员授权：把当前时区设成UTC+8") == []
    _, timeline = project("昨天傍晚指事情发生还是发言？")
    assert timeline["clock_visibility"] == "requested"


def test_daily_event_message_comparisons_get_real_clocks_without_inferred_event_dates():
    for question in [
        "昨天傍晚是花盆被碰倒的时间，还是我发消息的时间？",
        "昨天傍晚是事情发生的时候，还是我发消息的时候？",
        "这说的是发言时间，还是事情发生的时间？",
        "你能区分消息时间和花盆倒掉的时间吗？",
    ]:
        value, timeline = project(question)
        assert requests_message_clock(question)
        assert timeline["clock_visibility"] == "requested"
        assert "not_event_time" in timeline["sources"][1]["transport_clock"]["basis"]
        assert "2026-10-" not in json.dumps(value)
    for question in ["昨天傍晚花盆倒了是吗？", "事情发生在什么时候？", "我家的猫叫什么？"]:
        assert not requests_message_clock(question)


def test_message_dates_and_current_clock_questions_keep_the_actual_clock_available():
    for question in [
        "我是哪天告诉你猫名字的？",
        "这条消息是几号发的？",
        "今天星期几？",
        "现在北京时间几点？",
    ]:
        _, timeline = project(question)
        assert timeline["clock_visibility"] == "requested"
        assert timeline["sources"][0]["transport_clock"]["received_at"] == "2026-10-20T01:02:03Z"


def test_time_spans_distinguish_local_reference_clause_without_inventing_dates():
    text = "我这里北京时间2026年10月20日凌晨一点四十，前天晚上小狗拽坏了窗帘，它叫饼干。"
    phrases = reported_times(text)
    assert [p["text"] for p in phrases] == ["2026年10月20日", "一点四十", "前天晚上"]
    for phrase in phrases:
        assert text[phrase["start"] : phrase["end"]] == phrase["text"]
        assert phrase["clause_start"] <= phrase["start"] < phrase["end"] <= phrase["clause_end"]
    assert all(p["role"] == "self_reported_reference_clock_not_message_time" for p in phrases[:2])
    assert phrases[2]["role"] == "unresolved_clause_time"
    assert "窗帘" in text[phrases[2]["clause_start"] : phrases[2]["clause_end"]]
    assert "饼干" not in text[phrases[2]["clause_start"] : phrases[2]["clause_end"]]
    assert reported_times("2026年10月20日才起的名字")[0]["role"] == "unresolved_clause_time"


def test_prompt_projection_keeps_stored_sources_and_explicit_assistant_history():
    import asyncio

    async def run():
        h = Harness(silence_ms=0)
        h.gateway.segments = ["我刚才称呼你为小林。"]
        try:
            await h.ingest(text="叫我小林就好")
            await h.cycles()
            first = h.turns()[0]
            raw = copy.deepcopy(first["bundle"]["messages"])
            await h.ingest(text="你之前怎么称呼我的？")
            await h.cycles()
            assert h.core.store.get("turns", first["id"])["bundle"]["messages"] == raw
            payload = json.loads(h.gateway.calls[-1][1][1]["content"])
            assert any(
                r["text"] == "我刚才称呼你为小林。"
                for g in payload["recent_dialogue"]
                for r in g["replies"]
            )
        finally:
            await h.core.close()

    asyncio.run(run())
