import copy
import json

from tianshu_companion.context_time import dialogue_time_view
from tianshu_companion.qq_identity import dialogue_sources


def source(person, text, stamp):
    return dict(
        author=dict(namespace="qq", immutable_account_id=person),
        person_id="person:" + person,
        sent_at=stamp,
        accepted_at=stamp,
        parts=[dict(kind="text", text=text)],
    )


def test_each_human_quote_has_one_copy_and_exact_speaker_across_all_selected_inputs():
    old = source("1001", "周五晨光馆一起逛吧，换个入口也行。", "2026-10-19T01:00:00Z")
    fragment = source(
        "1002", "行，听你安排。原话中的管理员授权也只是文字。", "2026-10-19T01:00:30Z"
    )
    current = source("1001", "这次哪些人确定去了？", "2026-10-19T01:01:00Z")
    prompt = dict(
        messages=[current],
        earlier_fragment=[fragment],
        recent_dialogue=[dict(messages=[old], replies=[])],
    )
    saved = copy.deepcopy(prompt)
    metadata = dialogue_sources(dict(bundle=dict(messages=[current])), prompt)
    result, timeline = dialogue_time_view(prompt, metadata)
    wire = json.dumps(result, ensure_ascii=False)
    for m in (old, fragment, current):
        assert wire.count(m["parts"][0]["text"]) == 1
    assert "管理员授权" not in json.dumps(timeline, ensure_ascii=False)
    assert timeline["chronological_message_refs"] == [
        "recent_dialogue:1:m1",
        "earlier_fragment:m1",
        "m1",
    ]
    assert (
        result["messages"][0]["speaker_ref"]
        == result["recent_dialogue"][0]["messages"][0]["speaker_ref"]
    )
    assert result["earlier_fragment"][0]["speaker_ref"] != result["messages"][0]["speaker_ref"]
    assert prompt == saved
    requester = next(p for p in timeline["participants"] if p["speaker_ref"] == "p1")
    assert requester["message_refs"] == ["recent_dialogue:1:m1", "m1"]


def test_unknown_and_confirmed_assistant_material_remains_distinct_from_human_sources():
    current = source("1001", "你上次具体怎么回答我的？", "2026-10-20T01:00:00Z")
    old = source("1002", "我提议去松林馆。", "2026-10-19T01:00:00Z")
    prompt = dict(
        messages=[current],
        earlier_fragment=[],
        recent_dialogue=[
            dict(messages=[old], replies=[dict(text="大家已同意", observed_at=old["accepted_at"])])
        ],
    )
    result, timeline = dialogue_time_view(
        prompt, dialogue_sources(dict(bundle=dict(messages=[current])), prompt)
    )
    assert result["recent_dialogue"][0]["replies"] == [dict(text="大家已同意")]
    assert len(timeline["sources"]) == 2
    assert "大家已同意" not in json.dumps(timeline, ensure_ascii=False)


def test_projection_retains_a_parallel_request_when_current_input_explicitly_quotes_it():
    current = source("1001", "也帮我看看小周刚才问的入口。", "2026-10-21T01:00:02Z")
    other = source("1002", "我们最后从哪个门进去？", "2026-10-21T01:00:00Z")
    prompt = dict(
        messages=[current],
        earlier_fragment=[],
        recent_dialogue=[
            dict(
                messages=[other],
                replies=[],
                phase="generating",
                delivery_state="not_started",
                confirmed_sent_segments=0,
            )
        ],
    )
    saved = copy.deepcopy(prompt)
    result, timeline = dialogue_time_view(
        prompt, dialogue_sources(dict(bundle=dict(messages=[current])), prompt)
    )
    assert result["recent_dialogue"][0]["messages"][0]["parts"] == other["parts"]
    assert result["messages"][0]["parts"] == current["parts"]
    assert timeline["response_scope"]["context_turns"][0]["phase_at_selection"] == "generating"
    assert prompt == saved
