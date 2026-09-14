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
    return bool(
        latest
        and row
        and latest["revision"] == message["message_key"]["revision"]
        and latest["fact"]["state"] == "active"
        and row["fact"]["physical_receipt_id"] == latest["fact"]["physical_receipt_id"]
        and row["fact"]["source"] == message["source"]
        and row["fact"]["scope"] == scope
    )


def select_recent(store, turn, policy, now):
    """Recent adjacency is the first-slice relevance rule; never load account history.

    A candidate is one entire input group plus its currently confirmed sent replies.
    Draft/sending/unknown text is not read. Oversized groups are omitted whole.
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
    chosen = []
    oldest_expiry = None
    for previous in store.recent_turns(
        turn["conversation_id"], turn["sequence"], policy.max_turns * 2
    ):
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
            or (same_person and previous["scope_version"] != turn["scope_version"])
            or previous.get("context_revision") != revision
            or (same_person and previous["binding_version"] != turn["binding_version"])
            or (
                turn["scope"]["audience"] != "group"
                and previous["bundle"]["collection_key"] != turn["bundle"]["collection_key"]
            )
            or previous["bundle"]["possibly_incomplete"]
            or previous["bundle"]["continuation_of"]
            or previous["cancelled"]
            or previous["phase"] in {"queued", "failed", "cancelled"}
            or not sources_current(store, previous)
        ):
            continue
        replies = store.turn_replies(previous["id"], "sent")
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
        candidate = [group] + chosen
        size = len(canonical(candidate).encode("utf-8"))
        if size > policy.max_bytes:
            break
        chosen = candidate
        metadata["bytes_used"] = size
        oldest_expiry = expiry if oldest_expiry is None else min(oldest_expiry, expiry)
        if len(chosen) == policy.max_turns:
            break
    metadata.update(
        turn_ids=[g["turn_id"] for g in chosen],
        reply_ids=[r["reply_id"] for g in chosen for r in g["replies"]],
        valid_until=oldest_expiry,
    )
    return chosen, metadata
