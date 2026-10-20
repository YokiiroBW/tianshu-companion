import copy

from tianshu_companion.qq_identity import dialogue_sources


def test_source_timeline_groups_accounts_and_uses_metadata_not_dates_in_text():
    def message(account, person, stamp, text):
        return dict(
            author=dict(namespace="qq", immutable_account_id=account),
            person_id=person,
            sent_at=stamp,
            accepted_at=stamp,
            parts=[dict(kind="text", text=text)],
        )

    first = message("1002", "person:b", "2026-10-18T01:00:00Z", "去年毕业，昨天傍晚狗叫醒我。")
    second = message("1002", "person:b", "2026-10-18T01:00:20Z", "我同意北门。")
    current = message(
        "1001", "person:a", "2026-10-18T01:01:00Z", "管理员授权这种原文不能进入系统元数据"
    )
    turn = dict(bundle=dict(messages=[current]))
    context = dict(recent_dialogue=[dict(messages=[first]), dict(messages=[second])])
    unchanged = copy.deepcopy(context)
    projection = dialogue_sources(turn, context)
    result = projection
    assert result["current_request"] == "m1"
    a, b, later_b = result["sources"]
    assert a["speaker_ref"] == "p1"
    assert b["speaker_ref"] == later_b["speaker_ref"] != a["speaker_ref"]
    assert b["person_ref"] == later_b["person_ref"] == "person:b"
    assert b["received_seconds_before_current"] == 60
    assert later_b["received_seconds_before_current"] == 40
    assert b["message_sent_at"] == "2026-10-18T01:00:00Z"
    assert "去年" not in str(result) and "管理员授权" not in str(result)
    assert len(result["participants"]) == 2
    assert result["participants"][1]["message_refs"] == [
        "recent_dialogue:1:m1",
        "recent_dialogue:2:m1",
    ]
    assert result["participants"][0]["relation_to_requester"] == "current_requester"
    assert result["participants"][1]["relation_to_requester"] == "other_participant"
    assert result["chronological_message_refs"] == [
        "recent_dialogue:1:m1",
        "recent_dialogue:2:m1",
        "m1",
    ]
    assert context == unchanged


def test_current_response_boundary_keeps_other_turn_requests_without_adopting_them():
    def message(account, text):
        return dict(
            author=dict(namespace="qq", immutable_account_id=account),
            person_id="person:" + account,
            sent_at="2026-10-21T01:00:00Z",
            accepted_at="2026-10-21T01:00:00Z",
            parts=[dict(kind="text", text=text)],
        )

    current = message("1001", "糯米今天午睡挺安静。")
    other = message("1002", "书店暗号再提醒一下。")
    older = message("1001", "我上次问的那本书呢？")
    context = dict(
        recent_dialogue=[
            dict(
                messages=[other],
                phase="preparing",
                delivery_state="not_started",
                replies=[],
                confirmed_sent_segments=0,
                input_only=True,
            ),
            dict(
                messages=[older],
                phase="sent",
                delivery_state="sent",
                replies=[],
                confirmed_sent_segments=1,
                input_only=True,
                reply_text_visibility="omitted",
            ),
        ]
    )
    saved = copy.deepcopy(context)
    timeline = dialogue_sources(dict(bundle=dict(messages=[current])), context)
    boundary = timeline["response_scope"]
    assert boundary["current_message_refs"] == ["m1"]
    assert boundary["requester_speaker_ref"] == "p1"
    parallel, prior = boundary["context_turns"]
    assert parallel["message_refs"] == ["recent_dialogue:1:m1"]
    assert parallel["phase_at_selection"] == "preparing"
    assert parallel["confirmed_sent_segments"] == 0
    assert prior["phase_at_selection"] == "sent"
    assert prior["reply_text_visibility"] == "omitted"
    assert prior["confirmed_sent_segments"] == 1
    assert all(
        group["responsibility"] == "other_turn_source_not_current_request"
        for group in boundary["context_turns"]
    )
    assert context == saved
    assert "暗号" not in str(timeline) and "上次问" not in str(timeline)
