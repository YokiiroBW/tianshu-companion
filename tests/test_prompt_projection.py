import asyncio
import copy
import json

from support import Harness
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


def test_cold_recall_keeps_multiple_owners_without_inventing_human_messages_or_names():
    current = source("1001", "群里提到的几只猫分别叫什么？", "2026-10-21T01:00:02Z")
    evidence = [
        dict(subject_person_id="person:1002", statement="我家猫叫桃酥"),
        dict(subject_person_id="person:1003", statement="我家猫叫雪团"),
        dict(subject_person_id="person:1002", statement="我家猫周末去过体检"),
        dict(subject_person_id="person:1001", statement="我的杯子叫晚星"),
    ]
    prompt = dict(messages=[current], evidence=evidence, earlier_fragment=[], recent_dialogue=[])
    saved = copy.deepcopy(prompt)
    result, timeline = dialogue_time_view(
        prompt, dialogue_sources(dict(bundle=dict(messages=[current])), prompt)
    )
    assert result["evidence"] == evidence
    assert timeline["recalled_subjects"] == [
        dict(
            person_ref="person:1002",
            relation_to_requester="other_person",
            evidence_refs=["evidence:1", "evidence:3"],
            speaker_refs=[],
        ),
        dict(
            person_ref="person:1003",
            relation_to_requester="other_person",
            evidence_refs=["evidence:2"],
            speaker_refs=[],
        ),
        dict(
            person_ref="person:1001",
            relation_to_requester="current_requester",
            evidence_refs=["evidence:4"],
            speaker_refs=["p1"],
        ),
    ]
    assert len(timeline["participants"]) == len(timeline["sources"]) == 1
    assert not any(
        unit["statement"] in json.dumps(timeline, ensure_ascii=False) for unit in evidence
    )
    assert prompt == saved


def test_recalled_owner_links_only_to_existing_exact_person_speakers():
    current = source("1001", "刚才那只猫叫什么？", "2026-10-21T01:00:02Z")
    earlier = source("1002", "我家的那只叫桃酥。", "2026-10-21T01:00:01Z")
    prompt = dict(
        messages=[current],
        evidence=[dict(subject_person_id="person:1002", statement="我家猫叫桃酥")],
        earlier_fragment=[],
        recent_dialogue=[dict(messages=[earlier], replies=[])],
    )
    timeline = dialogue_sources(dict(bundle=dict(messages=[current])), prompt)
    assert timeline["recalled_subjects"][0]["speaker_refs"] == ["p2"]
    assert timeline["participants"][1]["person_ref"] == "person:1002"
    assert timeline["sources"][1]["message_ref"] == "recent_dialogue:1:m1"
    empty = dict(prompt, evidence=[])
    assert "recalled_subjects" not in dialogue_sources(dict(bundle=dict(messages=[current])), empty)


def test_native_memory_tool_rebinds_provenance_to_the_actual_refreshed_owner_and_record_set():
    async def run():
        h = Harness(silence_ms=0)
        calls, proposals = [], []
        committed = False
        original_select = h.memory.select

        def unit(owner, record, statement):
            return dict(
                subject_person_id=owner,
                record_id=record,
                record_version=1,
                semantic_group_id="group:" + record,
                statement=statement,
                conditions=[],
                negations=[],
                valid_time="current",
                uncertainty="confirmed",
                reality="real",
                visibility="shared_projection",
                sources=[
                    dict(
                        kind="shareable_projection",
                        owner="memory",
                        projection_ref="projection:" + record,
                        projection_version=1,
                    )
                ],
            )

        async def select(origin, scope, text, budget, *args, **kwargs):
            selected = await original_select(origin, scope, text, budget, *args, **kwargs)
            if all(budget.values()):
                selected["selected_units"] = (
                    [
                        unit("person:third", "new-third", "我的猫叫雪团"),
                        unit(scope["person_id"], "new-own", "我的杯子叫晚星"),
                    ]
                    if committed
                    else [
                        unit("person:second", "old-second", "我的猫叫桃酥"),
                        unit("person:third", "old-third", "我的猫叫雪团"),
                    ]
                )
                selected["dependency_groups"] = [
                    dict(
                        semantic_group_id=item["semantic_group_id"],
                        record_ids=[item["record_id"]],
                        complete=True,
                    )
                    for item in selected["selected_units"]
                ]
            return selected

        async def propose(request):
            nonlocal committed
            proposals.append(copy.deepcopy(request))
            committed = True
            h.memory.scope_version += 1
            return dict(state="committed", record_ids=["new-own"])

        async def complete(turn, messages, *, tools=None, on_delta=None):
            calls.append(copy.deepcopy(messages))
            if len(calls) == 1:
                assert "memory_propose" in {tool["function"]["name"] for tool in tools}
                value = dict(
                    kind="upsert",
                    target=dict(
                        category="evidence",
                        field_key="cup",
                        item_key=None,
                        record_id=None,
                        expected_version=None,
                    ),
                    units=[
                        dict(
                            statement="我的杯子叫晚星",
                            conditions=[],
                            negations=[],
                            valid_time="current",
                            uncertainty="confirmed",
                            reality="real",
                        )
                    ],
                    intent=dict(source_quote="记住我的杯子叫晚星", unambiguous_target=True),
                )
                message = dict(
                    role="assistant",
                    content=None,
                    tool_calls=[
                        dict(
                            id="tool:own-memory",
                            type="function",
                            function=dict(
                                name="memory_propose",
                                arguments=json.dumps(value, ensure_ascii=False),
                            ),
                        )
                    ],
                )
            else:
                message = dict(role="assistant", content="已按原请求记下。")
            return message, dict(config_version=1, fixture_only=True)

        h.memory.select = select
        h.memory.propose = propose
        h.gateway.complete = complete
        try:
            await h.ingest(text="记住我的杯子叫晚星", group=True, channel="group:owners")
            await h.cycles(80)
            assert len(calls) == 2
            turn = h.turns()[0]
            assert turn["phase"] == "sent"
            assert len(proposals) == 1 and proposals[0]["scope"] == turn["scope"]
            assert proposals[0]["scope"]["person_id"] not in {"person:second", "person:third"}
            assert (
                calls[0][0]["content"].split("\nTrusted identity projection: ", 1)[0]
                == (calls[1][0]["content"].split("\nTrusted identity projection: ", 1)[0])
            )
            for messages, owners, records in (
                (calls[0], ["person:second", "person:third"], ["old-second", "old-third"]),
                (calls[1], ["person:third", turn["scope"]["person_id"]], ["new-third", "new-own"]),
            ):
                prompt = json.loads(messages[1]["content"])
                assert list(prompt)[-1] == "messages"
                timeline = json.loads(
                    messages[0]["content"].split("Human source timeline: ", 1)[1].split("\n", 1)[0]
                )
                assert [item["record_id"] for item in prompt["evidence"]] == records
                assert [item["person_ref"] for item in timeline["recalled_subjects"]] == owners
                assert [item["evidence_refs"] for item in timeline["recalled_subjects"]] == [
                    ["evidence:1"],
                    ["evidence:2"],
                ]
                assert messages[0]["content"].count("Human source timeline: ") == 1
        finally:
            await h.core.close()

    asyncio.run(run())
