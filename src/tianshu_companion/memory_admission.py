"""Conservative no-fact admission for automatic extraction, never for explicit tools."""

import re


SOCIAL = re.compile(
    r"(?:你好|早上好|晚上好|早安|晚安|谢谢(?:你)?|多谢|辛苦了|再见|拜拜|哈哈+|呵呵+|"
    r"hello|hi|thanks|thank you|good morning|good night|bye)",
    re.I,
)
QUESTION = re.compile(
    r"谁|什么|哪|多少|几(?:点|个|岁)|怎么|为何|为什么|为啥|"
    r"是否|是不是|有没有|来着|记不记得|还记得|"
    r"^(?:what|where|when|who|whose|which|how|do you remember)\b",
    re.I,
)
# Quoted values, proposals, corrections and multi-clause messages remain for the
# extractor. A question mark alone cannot distinguish a fact from a question.
AMBIGUOUS = re.compile(
    r"[《》「」『』\"“”‘’]|要不|不如|建议|改成|改到|改回|换成|换回|其实|"
    r"\b(?:actually|instead|let.s|shall we|my .+ is)\b",
    re.I,
)
AUTHOR_DECLARATION = re.compile(
    r"我(?:也|还|已经|现在|一直|平时|家|的).{0,20}(?:叫|住|喜欢|习惯|改|换)"
)
NAME_LOOKUP = re.compile(r"叫(?:什么|啥)(?:名字)?(?:来着)?[?？]?$")
RECALL_PREFACE = re.compile(
    r"(?:今天|下午|晚上|现在|出门前)?(?:我)?(?:还是|还|又)?(?:想要|想|要|准备)?"
    r"(?:喝|吃|拿|取|找|翻|看|用)(?:一下)?(?:平时|以前|之前|上次)(?:的)?那"
    r"[^，,。！？?\n]{1,12}"
)


STABLE_CLAUSE = re.compile(
    r"(?:一直|平时|通常|每天|每周|习惯|长期|固定).{0,18}(?:放|收|住|坐|喝|吃|用|戴|带|走|喜欢)|"
    r"(?:我|我家|我的).{0,12}(?:住在|叫(?!醒|我|你|他|她)|名字是|喜欢|不喜欢|过敏)|"
    r"\b(?:I (?:always|usually|live|prefer)|my .{1,20} (?:is|are))\b",
    re.I,
)
TEMPORARY_CLAUSE = re.compile(
    r"(?:我)?(?:现在|这会儿|先|马上|等会儿?).{0,8}(?:去|倒|拿|放|接|洗|扔|吃|喝)|"
    r"临时|待会.{0,8}(?:拿|收|放)|\b(?:right now|for a minute)\b",
    re.I,
)
REPORTED_OR_HYPOTHETICAL = re.compile(
    r"[?？《》「」『』\"“”‘’]|如果|假如|要是|比如|例如|据说|听说|他说|她说|朋友说|同事说|[说讲](?:他|她|它)|"
    r"\b(?:if|said|suppose)\b",
    re.I,
)
NON_ASSERTION = re.compile(
    REPORTED_OR_HYPOTHETICAL.pattern + r"|可能|也许|大概|不一定|是否|是不是|\b(?:might|perhaps)\b",
    re.I,
)
PLAN_UPDATE = re.compile(
    r"我(?:也|还|还是|现在|已经)?(?:(?:想|打算|准备|决定|能|可以|不能|不想).{0,12}(?:去|参加|到|来)|不去|不参加)|"
    r"(?:出发|到场|到达|碰面|集合|入口|几点|时间).{0,12}(?:未定|没定|没想好|未确定|还不确定)|"
    r"\bI (?:can|cannot|still want to) (?:go|come|attend|join)\b|"
    r"\bdeparture .{0,16}(?:undecided|not decided)\b",
    re.I,
)


def extraction_review_hints(content, known_facts=()):
    """Exact source hints for one bounded review, never accepted facts.

    A stable assertion alongside a temporary action, or an elliptical update to
    one current open-ended non-entity plan, may merit review. Attribution and
    validity stay with Memory; no hint proves an event link or becomes a fact.
    """
    events = [
        fact
        for fact in known_facts
        if fact.get("key", "").startswith("event.")
        and fact.get("state") == "current"
        and not fact.get("superseded")
    ]
    # Count every current event before narrowing: another entity's event or an
    # existing subproperty must not disappear and make a reference look unique.
    plan = events[0] if len(events) == 1 else None
    if plan is not None and (
        not plan["key"].endswith(".plan")
        or plan.get("valid_to") is not None
        or plan.get("entity") is not None
    ):
        plan = None
    hints = []
    for message in content:
        text = message["text"]
        if (
            plan is not None
            and PLAN_UPDATE.search(text)
            and not REPORTED_OR_HYPOTHETICAL.search(text)
            and not TEMPORARY_CLAUSE.search(text)
        ):
            hints.append(
                dict(
                    source_index=message["index"],
                    quote=text.strip(),
                    known_plan_key=plan["key"],
                )
            )
        if NON_ASSERTION.search(text):
            continue
        clauses = [c.strip() for c in re.split(r"[，,。.!！;；\n]", text) if c.strip()]
        if len(clauses) < 2 or not any(TEMPORARY_CLAUSE.search(c) for c in clauses):
            continue
        for clause in clauses:
            if (
                STABLE_CLAUSE.search(clause)
                and not TEMPORARY_CLAUSE.search(clause)
                and not QUESTION.search(clause)
            ):
                hints.append(dict(source_index=message["index"], quote=clause))
    return hints[:8]


def empty_extraction_reason(content):
    """Skip only inputs whose every clause is a greeting or an explicit query.

    Mixed statements, short assent/withdrawal and uncertain cases still reach
    the model. The caller must obtain Memory's current content and commit an
    empty proposal through its ordinary source-checked receipt path.
    """
    reasons = set()
    for message in content:
        text = message["text"].strip()
        if AMBIGUOUS.search(text):
            return None
        if AUTHOR_DECLARATION.search(text) and not NAME_LOOKUP.search(text):
            return None
        clauses = [c.strip() for c in re.split(r"(?<=[，,。.!！?？;；\n])", text) if c.strip()]
        for clause in clauses:
            plain = clause.rstrip("，,。.!！?？;；\n ")
            if not plain:
                continue
            if SOCIAL.fullmatch(plain):
                reasons.add("social_only")
            elif RECALL_PREFACE.fullmatch(plain) and not re.search(
                r"不|没|别|改|换|放|收|叫|名字", plain
            ):
                # A wish to revisit an already-mentioned drink/book/object is
                # only a question's preface, not a fresh lasting preference.
                # Require an actual query below; independent statements still
                # fail closed to model extraction, even in the same message.
                reasons.add("recall_preface")
            elif QUESTION.search(plain) and (
                clause.endswith(("?", "？"))
                or plain.endswith(("呢", "吗", "么", "来着", "什么", "哪里", "哪儿", "哪边"))
                or re.match(r"^(?:what|where|when|who|whose|which|how)\b", plain, re.I)
            ):
                reasons.add("question_only")
            else:
                return None
    if "recall_preface" in reasons and "question_only" not in reasons:
        return None
    return "+".join(sorted(reasons)) if reasons else "empty_source"
