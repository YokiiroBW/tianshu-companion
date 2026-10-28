"""Bounded recent dialogue from core-owned inputs and confirmed delivery facts."""

from dataclasses import dataclass

from .clients import epoch
from .contracts import canonical, digest
from .source_sync import selector


@dataclass(frozen=True)
class ShortContextPolicy:
    max_turns: int = 4
    max_bytes: int = 8192
    max_age_seconds: int = 1800

    def __post_init__(self):
        for name, maximum in [("max_turns", 16), ("max_bytes", 65536), ("max_age_seconds", 86400)]:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
                raise ValueError(f"Invalid short context {name}")


def current_revision(store, turn):
    channel = turn["bundle"]["collection_key"]["channel"]
    return store.get("conversations", digest(channel)).get("context_revision", 1)


def sources_current(store, turn):
    for message in turn["bundle"]["messages"]:
        if not message_current(store, turn["scope"], message):
            return False
    return True


def message_current(store, scope, message):
    chosen = selector(message, scope["actor_id"])
    latest = store.latest_source(scope["conversation_id"], digest(chosen["key"]))
    row = store.get("admissions", digest(chosen))
    return _source_matches(scope, message, latest, row)


def _source_matches(scope, message, latest, row):
    return bool(
        latest
        and row
        and latest["revision"] == message["message_key"]["revision"]
        and latest["fact"]["state"] == "active"
        and row["fact"]["physical_receipt_id"] == latest["fact"]["physical_receipt_id"]
        and row["fact"]["source"] == message["source"]
        and row["fact"]["scope"] == scope
    )


def select_recent(store, turn, policy, now, *, after_sequence=0):
    """Keep adjacency and retrieve older relevant inputs within the same boundary.

    Ordinary adjacency includes confirmed replies. Retrieved human inputs keep nearby
    corrections together; bot answers need not accompany that source projection.
    Draft/sending/unknown text is not read. Oversized batches are omitted whole.
    """
    revision = current_revision(store, turn)
    metadata = dict(
        context_revision=revision,
        scope_version=turn["scope_version"],
        turn_ids=[],
        reply_ids=[],
        bytes_used=0,
        valid_until=None,
    )
    if not policy.max_turns or not policy.max_bytes or not policy.max_age_seconds:
        return [], metadata
    eligible = []
    for previous in store.recent_turns(turn["conversation_id"], turn["sequence"], 128):
        if previous["sequence"] <= after_sequence:
            break
        recover_input = (
            previous["phase"] == "failed"
            and previous.get("failure") == "dependency_unavailable"
            and (previous.get("model_calls") or 0) > 0
            and previous.get("input_history_snapshot") is not None
        )
        expiry = epoch(previous["bundle"]["sealed_at"]) + policy.max_age_seconds
        if expiry <= now:
            break
        same_person = previous["scope"] == turn["scope"]
        same_boundary = (
            all(
                previous["scope"][k] == turn["scope"][k]
                for k in ("actor_id", "audience", "conversation_id")
            )
            and previous["bundle"]["collection_key"]["channel"]
            == turn["bundle"]["collection_key"]["channel"]
        )
        if (
            not same_boundary
            or (turn["scope"]["audience"] != "group" and not same_person)
            or previous["scope_version"] is None
            or (
                same_person
                and previous["scope_version"] != turn["scope_version"]
                and not previous.get("history_snapshot")
            )
            or previous.get("context_revision") != revision
            or (same_person and previous["binding_version"] != turn["binding_version"])
            or (
                turn["scope"]["audience"] != "group"
                and previous["bundle"]["collection_key"] != turn["bundle"]["collection_key"]
            )
            or previous["bundle"]["possibly_incomplete"]
            or previous["bundle"]["continuation_of"]
            or previous["cancelled"]
            or previous["phase"] in {"queued", "cancelled"}
            or (previous["phase"] == "failed" and not recover_input)
        ):
            continue
        eligible.append((previous, expiry, recover_input))

    # This snapshot lives only during this synchronous selection. Network/model
    # and delivery boundaries still recheck their own current authority. Reading
    # 128 candidates need not execute three SQL statements for every message.
    lookups = {}
    for previous, _, _ in eligible:
        entries = []
        for message in previous["bundle"]["messages"]:
            chosen = selector(message, previous["scope"]["actor_id"])
            entries.append((message, digest(chosen["key"]), digest(chosen)))
        lookups[previous["id"]] = entries
    admissions = store.get_many(
        "admissions", [key for entries in lookups.values() for _, _, key in entries]
    )
    latest = store.latest_sources(
        turn["conversation_id"], [base for entries in lookups.values() for _, base, _ in entries]
    )
    candidates, links = [], {}
    replies_by_turn = store.replies_for_turns(
        [p["id"] for p, _, recover in eligible if not recover], "sent"
    )
    for previous, expiry, recover_input in eligible:
        if not all(
            _source_matches(
                previous["scope"],
                message,
                latest.get(base),
                admissions.get(key),
            )
            for message, base, key in lookups[previous["id"]]
        ):
            continue
        # A provider failure says nothing about the already accepted human
        # input. Retain only that input through its independent fresh checks;
        # never revive partial/unknown bot output or authorization failures.
        replies = replies_by_turn.get(previous["id"], [])
        replies = [
            r
            for r in replies
            if r["receipt"]
            and r["receipt"]["state"] == "sent"
            and r["receipt"]["channel_message_ids"]
        ]
        group = dict(
            turn_id=previous["id"],
            turn_sequence=previous["sequence"],
            collection_id=previous["bundle"]["collection_id"],
            input_revision=previous["bundle"]["collection_revision"],
            actor_id=previous["scope"]["actor_id"],
            person_id=previous["scope"]["person_id"],
            author=previous["bundle"]["collection_key"]["author"],
            phase=previous["phase"],
            delivery_state=previous["delivery_state"],
            confirmed_sent_segments=len(replies),
            messages=previous["bundle"]["messages"],
            replies=[
                dict(
                    reply_id=r["id"],
                    segment_sequence=r["segment_sequence"],
                    text=r["text"],
                    state="sent",
                    channel_message_ids=r["receipt"]["channel_message_ids"],
                    observed_at=r["receipt"]["observed_at"],
                )
                for r in replies
            ],
        )
        if recover_input:
            group["input_only"] = True
        candidates.append((group, expiry))
        # Earlier selections are retrieval hints, never inherited answers or
        # permissions. Only candidates passing today's source checks can enter.
        links[previous["id"]] = (previous.get("short_context") or {}).get("turn_ids", [])

    from .context_relevance import retrieval_batches

    ordinary = [group for group, _ in candidates[: policy.max_turns]]
    batches, topic_return, requires = retrieval_batches(
        turn,
        candidates,
        policy.max_turns,
        compact=len(canonical(ordinary).encode("utf-8")) > policy.max_bytes,
        links=links,
    )
    chosen = []
    linked = []
    oldest_expiry = None
    for batch in batches:
        available = {g["turn_id"] for g, _ in batch} | {g["turn_id"] for g in chosen}
        if any(requires.get(g["turn_id"], g["turn_id"]) not in available for g, _ in batch):
            continue
        candidate = sorted([g for g, _ in batch] + chosen, key=lambda item: item["turn_sequence"])
        size = len(canonical(candidate).encode("utf-8"))
        if size > policy.max_bytes or len(candidate) > policy.max_turns:
            if topic_return:
                continue
            break
        chosen = candidate
        if len(batch) > 1:
            linked.append([g["turn_id"] for g, _ in batch])
        metadata["bytes_used"] = size
        expiry = min(expiry for _, expiry in batch)
        oldest_expiry = expiry if oldest_expiry is None else min(oldest_expiry, expiry)
        if len(chosen) == policy.max_turns:
            break
    chosen_ids = {g["turn_id"] for g in chosen}
    by_head = {}
    for child, head in requires.items():
        if {child, head} <= chosen_ids:
            by_head.setdefault(head, {head}).add(child)
    positions = {g["turn_id"]: g["turn_sequence"] for g in chosen}
    linked.extend(sorted(ids, key=positions.__getitem__) for ids in by_head.values())
    metadata.update(
        turn_ids=[g["turn_id"] for g in chosen],
        reply_ids=[r["reply_id"] for g in chosen for r in g["replies"]],
        valid_until=oldest_expiry,
    )
    if linked:
        metadata["linked_turn_ids"] = linked
    return chosen, metadata
