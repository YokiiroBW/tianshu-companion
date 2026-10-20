"""Render dialogue clocks separately from the speaker's event-time wording.

This changes only model input. Stored sources, timestamps and verification inputs
remain intact; no local timezone or event date is inferred.
"""

import re


REPORTED_TIME = re.compile(
    r"(?:大前天|前天|昨天|昨晚|今天|今晚|明天|明晚|后天|去年|今年|明年|"
    r"上周|这周|本周|下周|上个月|这个月|下个月|周[一二三四五六日天]|星期[一二三四五六日天]|"
    r"[一二三四五六七八九十两\d]+天[前后]|\d{4}年\d{1,2}月\d{1,2}日)"
    r"(?:清晨|早上|上午|中午|下午|傍晚|晚上|夜里)?|"
    r"(?<!\d)\d{1,2}[:：]\d{2}(?!\d)|"
    r"(?:\d{1,2}|[零〇一二三四五六七八九十两]{1,3})点"
    r"(?:半|[零〇一二三四五六七八九十\d]{1,3}分?)?"
)
MESSAGE_CLOCK_QUESTION = re.compile(
    r"(?:几[点时]|哪天|哪一天|几号|什么时候|何时|哪个时间|实际时间|具体时间|时间戳|隔了多久|多久前)"
    r".{0,16}(?:发|说|提到|告诉|回复)|"
    r"(?:发言|消息|发送|接收|说话|说过|提到|回复).{0,16}"
    r"(?:时间戳|具体时间|实际时间|几点|哪天|哪一天|几号|什么时候|何时|多久前)|"
    r"(?:现在|当前).{0,8}(?:几点|几号|时间|日期|星期)|今天(?:是)?(?:几号|星期几)|"
    r"\b(?:when|what time)\b.{0,40}\b(?:sent|received|said|tell|told|message|reply)\b",
    re.I,
)


def reported_times(text):
    """Exact spans and their clause boundary; no inferred event or validity date."""
    result = []
    for clause in re.finditer(r"[^，,。；;！!？?\n]+", text):
        # A literal local-clock report can anchor the speaker's relative words,
        # but is never a verified send timestamp. Do not silently promote it.
        reference = bool(
            re.search(r"我(?:这里|这边|当地)|现在|此刻", clause[0])
            and re.search(r"北京时间|当地时间|UTC|GMT|时区", clause[0], re.I)
        )
        for match in REPORTED_TIME.finditer(clause[0]):
            result.append(
                dict(
                    text=match[0],
                    start=clause.start() + match.start(),
                    end=clause.start() + match.end(),
                    clause_start=clause.start(),
                    clause_end=clause.end(),
                    basis="speaker_reported_wording",
                    role="self_reported_reference_clock_not_message_time"
                    if reference
                    else "unresolved_clause_time",
                )
            )
            if len(result) == 8:
                return result
    return result


def dialogue_time_view(prompt, metadata):
    """Keep raw clocks only in the labelled provenance section when requested."""
    current = prompt["messages"]
    question = "\n".join(p["text"] for m in current for p in m["parts"] if p["kind"] == "text")
    clocks = bool(MESSAGE_CLOCK_QUESTION.search(question))

    by_ref = {source["message_ref"]: source for source in metadata["sources"]}

    def message(value, ref):
        result = {k: v for k, v in value.items() if k not in {"sent_at", "accepted_at"}}
        result.update(source_ref=ref, speaker_ref=by_ref[ref]["speaker_ref"])
        phrases = reported_times(
            "\n".join(p["text"] for p in value["parts"] if p["kind"] == "text")
        )
        if phrases:
            result["reported_time_phrases"] = phrases
        return result

    result = dict(prompt, messages=[message(m, f"m{i}") for i, m in enumerate(current, 1)])
    result["earlier_fragment"] = [
        message(m, f"earlier_fragment:m{i}")
        for i, m in enumerate(prompt.get("earlier_fragment", []), 1)
    ]
    result["recent_dialogue"] = [
        dict(
            g,
            messages=[
                message(m, f"recent_dialogue:{i}:m{j}") for j, m in enumerate(g["messages"], 1)
            ],
            replies=[{k: v for k, v in r.items() if k != "observed_at"} for r in g["replies"]],
        )
        for i, g in enumerate(prompt.get("recent_dialogue", []), 1)
    ]
    # Ordering and exact author references are sufficient for ordinary factual
    # recall. Raw transport times elsewhere would duplicate this provenance.
    sources = []
    for original in metadata["sources"]:
        # Participant rows already own the exact person and ordered references.
        # Avoid three copies of those same IDs and relations in model input.
        source = {key: original[key] for key in ("message_ref", "speaker_ref")}
        if clocks:
            source["transport_clock"] = dict(
                sent_at=original["message_sent_at"],
                received_at=original["message_received_at"],
                basis="verified_transport_timestamp_with_explicit_offset_not_event_time",
            )
        sources.append(source)
    timeline = {k: v for k, v in metadata.items() if k != "current_request_received_at"}
    timeline.update(sources=sources, clock_visibility="requested" if clocks else "not_requested")
    return result, timeline
