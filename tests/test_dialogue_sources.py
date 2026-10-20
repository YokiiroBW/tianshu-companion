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
    result = projection["metadata"]
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
    view = projection["view"]
    assert len(view["participants"]) == 2
    assert [m["text"] for m in view["participants"][1]["messages"]] == [
        "去年毕业，昨天傍晚狗叫醒我。",
        "我同意北门。",
    ]
    assert "管理员授权" in view["participants"][0]["messages"][0]["text"]
    assert view["participants"][0]["relation_to_requester"] == "current_requester"
    assert view["participants"][1]["relation_to_requester"] == "other_participant"
    assert view["participants"][1]["messages"][0]["first_person_ref"] == b["speaker_ref"]
    assert (
        view["participants"][1]["messages"][0]["time_basis"]
        == "speaker_wording_not_verified_event_or_transport_time"
    )
    assert context == unchanged
