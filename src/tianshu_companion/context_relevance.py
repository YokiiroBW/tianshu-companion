"""Small lexical selector for older inputs inside an already-authorized conversation."""

import re
import unicodedata

SCAFFOLD = re.compile(
    r"(?:刚才|之前|以前|上次|现在|我们|咱们|你们|那个|这个|什么|怎么|哪里|哪个|"
    r"是不是|来着|记得|记住|知道|告诉|一下|关于|继续|好像|可以|时候|的|了|呢|呀|吗|是)"
)
UPDATE = re.compile(
    r"改成|改到|改在|改为|改回|改叫|改名|换成|换到|换回|挪到|挪至|"
    r"(?:改|换)(?:了|过)|最后(?:定|决定)|调整(?:到|为)|取消|不去了|"
    r"不是.{0,40}(?:而是|是)|不在.{0,40}(?:在|到)|别.{0,30}(?:去|进)|"
    r"\b(?:instead|actually|change\w*.{0,40}\bto|no longer)\b",
    re.I,
)
QUESTION = re.compile(r"[?？]|什么|哪(?:个|里|儿)|多少|怎么|是否|是不是")
FACT_LOOKUP = re.compile(r"叫什么|叫啥|名字|姓什么|在哪里|在哪|放哪|收在哪|什么时候|哪天|同意|来着")


def changes_plan(text):
    """A recall question mentions a change without making another change.

    Check clauses separately: a statement followed by a question still carries
    its update. Tentative new proposals remain context, never confirmed facts.
    """
    for clause in re.split(r"[，,。；;！!？?\n]", text):
        if not UPDATE.search(clause):
            continue
        if re.search(
            r"要不|不如|能不能|可不可以|怎么样|好不好|好吗|吧|"
            r"\b(?:let.s|shall we|could we|why don.t we|what about)\b",
            clause,
            re.I,
        ):
            return True
        if re.search(
            r"为(?:什么|啥)|怎么|如何|哪(?:天|里|儿|个|次)|何时|什么时候|"
            r"是否|是不是|有没有|来着|记不记得|还记得|原因.{0,20}(?:说|提)|[吗么呢]\s*$|"
            r"^\s*(?:why|when|where|what|how|did|have|has|was|were)\b",
            clause,
            re.I,
        ):
            continue
        return True
    return False


def input_text(group):
    return " ".join(p["text"] for m in group["messages"] for p in m["parts"] if p["kind"] == "text")


def tokens(text):
    text = SCAFFOLD.sub(" ", unicodedata.normalize("NFKC", text).casefold())
    result = set()
    for word in re.findall(r"[\u3400-\u9fff]+|[a-z0-9]+", text):
        if word.isascii():
            if len(word) > 1 and word not in {"the", "what", "that", "this", "was", "our"}:
                result.add(word)
        else:
            result.update(word[i : i + 2] for i in range(len(word) - 1))
    return result


def relevance(turn, group):
    query = tokens(input_text(turn["bundle"]))
    # Assistant answers cannot turn a hallucinated detail into a history anchor.
    value = input_text(group)
    content = tokens(value)
    score = len(query.intersection(content)) / max(1, len(query))
    # A repeated question is a weaker anchor than the source assertion at the
    # same lexical overlap. This never promotes an assistant answer as evidence.
    question = bool(QUESTION.search(value))
    return score, not question


def retrieval_batches(turn, candidates, limit, *, compact=False):
    """Keep an older anchor with its nearby follow-ups, without resolving their meaning.

    A change often omits the original topic words. Carry the intervening human
    inputs too: their speakers and other plans are needed to interpret that change.
    The batch is indivisible; an oversized or revoked follow-up must not leave an
    isolated obsolete anchor. This is conversation context, never a fact update.
    """
    scores = {g["turn_id"]: relevance(turn, g) for g, _ in candidates}
    topic_return = any(scores[g["turn_id"]][0] > 0 for g, _ in candidates[limit:]) or (
        compact and any(score > 0 for score, _ in scores.values())
    )
    asks_assistant = bool(
        re.search(
            r"你(?:刚才|之前|上次|当时)?.{0,12}(?:说|建议|方案|列|写|回复|回答|推荐|解释|称呼|叫我)|"
            r"your (?:plan|reply|suggestion)|\byou\b.{0,40}\b(?:said|suggested|recommended|wrote|listed)\b",
            input_text(turn["bundle"]),
            re.I,
        )
    )
    query = input_text(turn["bundle"])
    asks_fact = (
        not asks_assistant
        and QUESTION.search(query)
        and (FACT_LOOKUP.search(query) or any(score > 0 for score, _ in scores.values()))
    )

    def project(batch):
        if asks_assistant:
            return batch
        # A bot's old guesses are not fresh human evidence, even when its
        # delivery and authority are still valid. Keep the original statements.
        return [(dict(g, replies=[], input_only=True), expiry) for g, expiry in batch]

    def priority(item):
        score, assertion = scores[item[0]["turn_id"]]
        # A repeated question can echo more query words than the actual source.
        # Prefer a relevant human assertion for factual recall. Explicit requests
        # for the bot's earlier answer still rank the original question normally.
        return (
            not asks_assistant and score > 0 and assertion,
            score,
            assertion,
            item[0]["turn_sequence"],
        )

    ranked = sorted(
        candidates,
        key=priority,
        reverse=True,
    )
    recent_ids = {g["turn_id"] for g, _ in candidates[:limit]}
    needs_origin = bool(
        asks_fact
        and ranked
        and scores[ranked[0][0]["turn_id"]][0] > 0
        and changes_plan(input_text(ranked[0][0]))
    )
    # A recent correction still needs the object from its original statement.
    # This applies before byte compaction too: short bot replies must not make
    # the ordinary recent window drop the thing that was changed.
    topic_return = topic_return or needs_origin
    recent_fact = bool(
        asks_fact
        and ranked
        and ranked[0][0]["turn_id"] in recent_ids
        and scores[ranked[0][0]["turn_id"]][1]
        and not needs_origin
    )
    if not topic_return or (
        (not compact or recent_fact)
        and ranked[0][0]["turn_id"] in recent_ids
        and scores[ranked[0][0]["turn_id"]][1]
        and not needs_origin
    ):
        # Continuing an already-recent asserted topic needs ordinary adjacency,
        # not overlapping historical spans that crowd out the preceding turn.
        # Raw recent confirmations can fit even when verbose bot replies cannot.
        # A correction anchor still needs its earlier description as one batch.
        return [project([item]) if asks_fact else [item] for item in candidates], False
    chronological = list(reversed(candidates))
    positions = {g["turn_id"]: i for i, (g, _) in enumerate(chronological)}

    def anchor_batch(item):
        position = positions[item[0]["turn_id"]]
        if changes_plan(input_text(item[0])):
            # A correction can itself be the best lexical match. Its original
            # description then precedes it, including another speaker in between.
            start = max(0, position - min(2, limit - 1))
            end = position
        else:
            start = position
            end = min(start + min(2, limit - 1), len(chronological) - 1)
        # Ordinary follow-ups need not say "change": e.g. an entrance is closed
        # and the speaker will use another one. Keep two adjacent human inputs
        # before using explicit continuation cues to extend the same small span.
        for i in range(end + 1, len(chronological)):
            if i - end >= limit:
                break
            if changes_plan(input_text(chronological[i][0])):
                end = i
        # Human input does not inherit the claims or provenance of the bot's
        # response. Keep the original input intact and label the projection.
        return project(chronological[start : end + 1])

    preferred = [anchor_batch(ranked[0])]
    preferred.extend(project([item]) for item in candidates[: limit // 2])
    preferred.extend(anchor_batch(item) for item in ranked[1:])
    batches, seen = [], set()
    for batch in preferred:
        ids = {item[0]["turn_id"] for item in batch}
        if not ids.intersection(seen):
            batches.append(batch)
            seen.update(ids)
        # Never trim a required follow-up merely because an overlapping batch
        # appeared earlier in the priority list: that earlier batch may not fit.
    return batches, True
