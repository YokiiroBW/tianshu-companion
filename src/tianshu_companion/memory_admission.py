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
NON_ASSERTION = re.compile(
    r"[?？《》「」『』\"“”‘’]|如果|假如|要是|比如|例如|据说|听说|他说|她说|朋友说|同事说|[说讲](?:他|她|它)|"
    r"可能|也许|大概|不一定|是否|是不是|\b(?:if|might|perhaps|said)\b",
    re.I,
)


def mixed_stable_hints(content):
    """Exact source hints for a bounded empty-proposal review, never stored facts.

    Review only a stable assertion alongside a temporary action. Attribution and
    validity still belong to Memory, including after a successful model review.
    """
    hints = []
    for message in content:
        text = message["text"]
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
