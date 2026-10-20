import copy
import json

from support import Harness
from tianshu_companion.context_time import (
    dialogue_time_view,
    reported_times,
    time_intent,
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
    assert timeline["time_request"]["kind"] == "event_attribution"
    assert timeline["clock_visibility"] == "not_requested"


def test_event_attribution_does_not_expand_an_unasked_transport_clock():
    for question in [
        "昨天傍晚是花盆被碰倒的时间，还是我发消息的时间？",
        "昨天傍晚是事情发生的时候，还是我发消息的时候？",
        "这说的是发言时间，还是事情发生的时间？",
        "你能区分消息时间和花盆倒掉的时间吗？",
    ]:
        value, timeline = project(question)
        assert time_intent(question) == "event_attribution"
        assert timeline["clock_visibility"] == "not_requested"
        assert all("transport_clock" not in source for source in timeline["sources"])
        assert "2026-10-" not in json.dumps(value)
    for question in ["昨天傍晚花盆倒了是吗？", "事情发生在什么时候？", "我家的猫叫什么？"]:
        assert time_intent(question) == "none"


def test_message_dates_and_current_clock_questions_keep_the_actual_clock_available():
    for question in [
        "我是哪天告诉你猫名字的？",
        "这条消息是几号发的？",
    ]:
        _, timeline = project(question)
        assert timeline["clock_visibility"] == "requested"
        assert timeline["sources"][0]["transport_clock"]["received_at"] == "2026-10-20T01:02:03Z"


def test_reported_clock_verification_exposes_real_provenance_without_promoting_a_report():
    for question in [
        "三点十分是你从消息系统核实的时间吗？",
        "那个零点二十分是你核实的，还是我自己说的？",
        "你核对过这个钟点吗？",
    ]:
        _, timeline = project(question)
        assert timeline["time_request"]["kind"] == "reported_clock_verification"
        assert timeline["sources"][1]["transport_clock"]["sent_at"] == "2026-10-19T00:00:00Z"
        assert "current_clock" not in timeline
    assert reported_times("我这边北京时间三点十分")[0]["role"] == (
        "self_reported_reference_clock_not_message_time"
    )
    for question in [
        "这个地点是你查到的，还是我自己说的？",
        "这件事是你核实的还是我自己说的？",
        "这三点意见是你核对过的，还是我自己说的？",
        "今天这件事是你查到的吗？",
    ]:
        _, timeline = project(question)
        assert time_intent(question) == "none"
        assert timeline["clock_visibility"] == "not_requested"


def test_current_clock_is_a_fresh_core_observation_not_a_message_timestamp():
    for question in ["今天星期几？", "现在北京时间几点？"]:
        current = message(question, "2026-10-20T23:59:50Z")
        prompt = dict(messages=[current])
        metadata = dialogue_sources(dict(bundle=dict(messages=[current])), prompt)
        _, timeline = dialogue_time_view(prompt, metadata, observed_at="2026-10-21T00:01:02Z")
        assert timeline["time_request"]["kind"] == "current_clock"
        assert timeline["current_clock"] == dict(
            observed_at="2026-10-21T00:01:02Z",
            basis="core_clock_at_prompt_projection_not_message_time",
        )
        assert all("transport_clock" not in source for source in timeline["sources"])
        assert "timezone" not in timeline["current_clock"]  # No guessed local date/timezone.
        _, missing = dialogue_time_view(prompt, metadata)
        assert missing["current_clock"] is None


def test_quantities_and_numbered_points_are_not_clock_annotations():
    for text in [
        "今天的面包稍微烤焦了一点。",
        "还有三点意见，我先说第一点。",
        "多吃一点水果，这里还差一点点。",
        "一点也不困，喝一点温水。",
    ]:
        assert all("点" not in phrase["text"] for phrase in reported_times(text))
    for text in ["下午三点", "凌晨一点", "三点钟", "三点见", "一点四十分", "03:10"]:
        assert len(reported_times(text)) == 1


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


def test_native_prompt_observes_current_clock_after_context_wait():
    import asyncio

    from tianshu_companion.clients import utc

    async def run():
        h = Harness(silence_ms=0)
        prepare = h.core._prepare_context

        async def delayed(*args, **kwargs):
            result = await prepare(*args, **kwargs)
            h.clock.advance(5)
            return result

        h.core._prepare_context = delayed
        try:
            await h.ingest(text="现在几点？")
            await h.cycles()
            messages = h.gateway.calls[0][1]
            timeline = json.loads(
                messages[0]["content"].split("Human source timeline: ", 1)[1].split("\n", 1)[0]
            )
            assert timeline["current_clock"]["observed_at"] == utc(h.clock())
            original = h.turns()[0]["bundle"]["messages"][0]
            assert timeline["current_clock"]["observed_at"] != original["accepted_at"]
            assert original["accepted_at"] == original["sent_at"]
        finally:
            await h.core.close()

    asyncio.run(run())
