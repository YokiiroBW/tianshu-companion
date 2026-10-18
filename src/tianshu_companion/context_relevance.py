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
POSITION_STATEMENT = re.compile(
    r"同意|赞成|反对|不参加|参加不了|去不了|不去了|\b(?:agree|agreed|disagree|can't come)\b", re.I
)
FOLLOWUP_QUESTION = re.compile(
    r"^(?:那|我们|咱们)?(?:中间|后来|当时|之前)(?:还|又|为什么|为啥|怎么|考虑|是为|呢|[，,？?])|"
    r"^(?:那|我们|咱们)?(?:为什么|为啥)(?:又|还|没|不)"
)


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


def relevance(query, group):
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
    query_text = input_text(turn["bundle"])
    query_tokens = tokens(query_text)
    scores = {g["turn_id"]: relevance(query_tokens, g) for g, _ in candidates}
    if (
        candidates
        and QUESTION.search(query_text)
        and FOLLOWUP_QUESTION.search(query_text)
        and not any(score > 0 and assertion for score, assertion in scores.values())
    ):
        previous = candidates[0][0]
        author = turn["bundle"]["messages"][-1]
        last = previous["messages"][-1]
        if (last["author"], last["person_id"]) == (
            author["author"],
            author["person_id"],
        ) and QUESTION.search(input_text(previous)):
            # Resolve an elliptical follow-up through the same speaker's most
            # recent question. Its words are retrieval cues, never factual evidence.
            query_tokens |= tokens(input_text(previous))
            scores = {g["turn_id"]: relevance(query_tokens, g) for g, _ in candidates}
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
    asks_fact = not asks_assistant and (
        (
            QUESTION.search(query)
            and (FACT_LOOKUP.search(query) or any(score > 0 for score, _ in scores.values()))
        )
        or POSITION_STATEMENT.search(query)
    )

    def updates_plan(group):
        text = input_text(group)
        if asks_fact and POSITION_STATEMENT.search(query):
            # A person's acceptance/withdrawal is their stance, not a change to
            # everybody's plan. Keep those source statements independently so
            # an unrelated interlude cannot make the whole consent set too big.
            # Actual changes in the same message still require their origin.
            text = POSITION_STATEMENT.sub("", text)
        return changes_plan(text)

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
        and updates_plan(ranked[0][0])
    )
    # A recent correction still needs the object from its original statement.
    # This applies before byte compaction too: short bot replies must not make
    # the ordinary recent window drop the thing that was changed.
    topic_return = topic_return or needs_origin
    recent_fact = bool(
        asks_fact
        and ranked
        and any(
            key in recent_ids and score > 0 and assertion
            for key, (score, assertion) in scores.items()
        )
        and not needs_origin
    )
    if recent_fact and scores[ranked[0][0]["turn_id"]][0] > 0:
        # Repeated questions must not displace the original proposal or a
        # different person's confirmation. Keep related assertions, not every
        # recent topic. Preserve adjacency after the newest matching assertion
        # for implicit follow-ups; explicit changes still use atomic spans below.
        assertions = [item for item in ranked if all(scores[item[0]["turn_id"]])]
        first = min(g["turn_sequence"] for g, _ in assertions)
        has_changes = any(g["turn_sequence"] >= first and updates_plan(g) for g, _ in candidates)
        if not has_changes:
            newest = max(g["turn_sequence"] for g, _ in assertions)
            following = [
                item
                for item in candidates
                if item[0]["turn_sequence"] > newest and scores[item[0]["turn_id"]][1]
            ]
            return [project([item]) for item in following + assertions], True
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
        if updates_plan(item[0]):
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
            if updates_plan(chronological[i][0]):
                end = i
        # Human input does not inherit the claims or provenance of the bot's
        # response. Keep the original input intact and label the projection.
        return project(chronological[start : end + 1])

    preferred = [anchor_batch(ranked[0])]
    preferred.extend(
        project([item])
        for item in candidates[: limit // 2]
        if not asks_fact or scores[item[0]["turn_id"]][1]
    )
    preferred.extend(
        anchor_batch(item) for item in ranked[1:] if not asks_fact or scores[item[0]["turn_id"]][1]
    )
    batches, seen = [], set()
    for batch in preferred:
        ids = {item[0]["turn_id"] for item in batch}
        if not ids.intersection(seen):
            batches.append(batch)
            seen.update(ids)
        # Never trim a required follow-up merely because an overlapping batch
        # appeared earlier in the priority list: that earlier batch may not fit.
    return batches, True
