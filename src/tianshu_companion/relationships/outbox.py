"""Candidate delivery attached to the existing committed-turn outbox."""

import copy

from .. import observability as obs
from ..clients import command, uid
from ..contracts import Fault, digest
from ..short_context import sources_current
from ..source_sync import ensure_channel, selector
from .client import pair
from .contract import DOMAIN


def candidate(core, turn, item):
    event = item["event"]
    pins = [c for c in turn.get("context_checks", []) if c["version_domain"] == DOMAIN]
    replies = core._replies(turn)
    if (
        turn["cancelled"]
        or turn["phase"] != "sent"
        or event["delivery_state"] != "sent"
        or event["reality"] != "real"
        or event["scope"]["audience"] != "self_private"
        or not core._turn_allows(turn, "memory.read")
        or not core._turn_allows(turn, "memory.write")
        or not any(
            c["scope"] == turn["scope"] and c.get("relationship_view") == "private" for c in pins
        )
        or not replies
        or any(
            r["state"] != "sent" or not (r.get("receipt") or {}).get("channel_message_ids")
            for r in replies
        )
        or set(event["reply_ids"]) != {r["id"] for r in replies}
        or not sources_current(core.store, turn)
    ):
        return None
    # Only direct current user input; quoted/history/model/proactive material is never a source.
    messages = [
        m
        for m in turn["bundle"]["messages"]
        if m["person_id"] == turn["scope"]["person_id"]
        and not m["reply_refs"]
        and any(p["kind"] == "text" and p["text"].strip() for p in m["parts"])
    ]
    if not messages:
        return None
    message = messages[0]
    physical = core.store.get("physicals", digest(message["message_key"]))
    if physical is None or physical["fact"]["kind"] != "message":
        return None
    return {
        "event_id": "affinity:" + digest([turn["id"], "conversation_completed"]),
        "pair": pair(turn["scope"]),
        "kind": "conversation_completed",
        "turn_id": turn["id"],
        "source_ref": digest(selector(message, turn["scope"]["actor_id"])),
        "source_revision": message["source"]["message_key"]["revision"],
        "occurred_at": event["occurred_at"],
    }


def queue(runtime, core, item, turn):
    value = candidate(core, turn, item)
    if value is not None:
        runtime.client.contract.check("AffinityEventCandidate", value)
        item.update(
            state="relationship_pending",
            relationship_candidate=value,
            relationship_attempts=0,
            deadline=core.clock(),
        )


def recover(core):
    for item in core.store.list("outbox", states=["relationship_submitting"]):
        item.update(state="relationship_unknown", relationship_error="result_unknown")
        core.store.put("outbox", item)


async def flush(runtime, core):
    for item in core.store.due("outbox", ["relationship_pending"], core.clock()):
        turn = core.store.get("turns", item["event"]["aggregate_id"])
        attempts = item.get("relationship_attempts", 0) + 1
        outcome, code = "succeeded", None
        try:
            ensure_channel(core.store, turn["bundle"]["collection_key"]["channel"])
            core._check_role(turn)
            if candidate(core, turn, item) != item["relationship_candidate"]:
                raise Fault("scope_changed")
            _, _, binding = await core._authorize(
                turn["service"],
                command(turn["origin"], uid("check"), core.clock()),
                turn["bundle"]["collection_key"]["channel"],
                scope=turn["scope"],
            )
            version = await core.memory.check_sources(turn, item["event"]["sources"])
            fresh = core.store.get("turns", turn["id"])
            if (
                binding != turn["binding_version"]
                or version != item["event"]["scope_version"]
                or candidate(core, fresh, item) != item["relationship_candidate"]
            ):
                raise Fault("scope_changed")
            item["state"] = "relationship_submitting"
            core.store.put("outbox", item)
            try:
                result = await runtime.client.settle(
                    turn["origin"], copy.deepcopy(item["relationship_candidate"])
                )
            except BaseException:
                item.update(state="relationship_unknown", relationship_error="result_unknown")
                core.store.put("outbox", item)
                raise
            item.update(state="delivered", relationship_receipt=result, relationship_error=None)
        except (Fault, OSError, TimeoutError) as error:
            uncertain = item["state"] == "relationship_unknown" and (
                not isinstance(error, Fault) or error.unknown or error.code == "result_unknown"
            )
            code = (
                "result_unknown"
                if uncertain
                else error.code
                if isinstance(error, Fault)
                else "dependency_unavailable"
            )
            outcome = "unknown" if uncertain else "failed"
            if not uncertain:
                item["state"] = (
                    "relationship_pending"
                    if code in {"dependency_unavailable", "timeout", "queue_full"}
                    else "relationship_rejected"
                )
            item.update(
                relationship_error=code, deadline=core.clock() + min(60, 2 ** min(attempts - 1, 6))
            )
        item["relationship_attempts"] = attempts
        core.store.put("outbox", item)
        obs.emit(core.events, "outbox.flush", outcome, error_code=code)
        core.counted("core.outbox")
