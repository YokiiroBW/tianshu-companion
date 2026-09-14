"""Authorized, bounded web history projection; never a model-recall authorization path."""

import asyncio
import copy
import json

from .clients import epoch, utc
from .contracts import Fault, canonical, digest
from .short_context import message_current
from .source_sync import ensure_channel, physical_key

TERMINAL = {"sent", "failed", "cancelled", "observed", "closed_unknown"}
MAX_BYTES = 1_048_576


def deadline(core, request):
    if epoch(request["deadline_at"]) <= core.clock():
        raise Fault("timeout")


async def snapshot(core, service, request):
    if service != "platform":
        raise Fault("forbidden")
    core.contracts.check("web-conversation#snapshot_request", request)
    deadline(core, request)
    try:
        return await asyncio.wait_for(
            _read(core, service, request), min(30, epoch(request["deadline_at"]) - core.clock())
        )
    except TimeoutError:
        raise Fault("timeout") from None


async def authorize(core, service, request):
    ctx, person, version = await core._authorize(service, request["query"])
    deadline(core, request)
    channel, allowed = ctx["verified_channel"], ctx["allowed_scope"]
    if (
        channel["namespace"] != "web"
        or allowed["audience"] != "self_private"
        or allowed["actor_id"] != request["actor_id"]
        or allowed["conversation_id"] != request["conversation_id"]
    ):
        raise Fault("forbidden")
    conv = ensure_channel(core.store, channel)
    if conv is None or conv["conversation_id"] != request["conversation_id"]:
        raise Fault("forbidden")
    owned = core.store.db.execute(
        "SELECT 1 FROM collections WHERE conversation_id=? "
        "AND json_extract(body,'$.collection_key.author')=? "
        "AND json_extract(body,'$.scope.person_id')=? LIMIT 1",
        (request["conversation_id"], canonical(ctx["verified_account"]), person),
    ).fetchone()
    if owned is None:
        raise Fault("forbidden")
    scope = dict(
        actor_id=request["actor_id"],
        person_id=person,
        audience="self_private",
        conversation_id=request["conversation_id"],
    )
    return dict(
        scope=scope,
        binding_version=version,
        channel=channel,
        account=ctx["verified_account"],
        allowed_scope=allowed,
        binding=copy.deepcopy(core.bindings[channel["binding_id"]]),
        expires_at=ctx["expires_at"],
    )


def rows(core, table, authority, *, terminal=None, before=None, limit):
    """SQL filters exact actor/person/channel/account before loading bounded bodies."""
    assert table in {"collections", "turns"}
    prefix = "$.bundle.collection_key" if table == "turns" else "$.collection_key"
    scope = authority["scope"]
    sql = f"SELECT body FROM {table} WHERE conversation_id=?"
    args = [scope["conversation_id"]]
    for key in ("actor_id", "person_id", "audience", "conversation_id"):
        sql += f" AND json_extract(body,'$.scope.{key}')=?"
        args.append(scope[key])
    # Compare canonical JSON for exact channel/account, including thread and binding.
    sql += f" AND json_extract(body,'{prefix}.channel')=?"
    args.append(canonical(authority["channel"]))
    sql += f" AND json_extract(body,'{prefix}.author')=?"
    args.append(canonical(authority["account"]))
    if table == "collections":
        sql += " AND status='collecting'"
    else:
        marks = ",".join("?" for _ in TERMINAL)
        sql += f" AND status {'IN' if terminal else 'NOT IN'} ({marks})"
        args.extend(sorted(TERMINAL))
        if before is not None:
            sql += " AND position<?"
            args.append(before)
    sql += " ORDER BY position DESC,id LIMIT ?"
    args.append(limit)
    return [json.loads(row[0]) for row in core.store.db.execute(sql, args)]


def message_view(core, scope, message):
    latest = core.store.latest_source(scope["conversation_id"], digest(physical_key(message)))
    state = "active"
    if latest and latest["fact"]["state"] == "withdrawn":
        state = "retracted"
    elif latest and latest["revision"] != message["message_key"]["revision"]:
        state = "edited"
    elif not message_current(core.store, scope, message):
        state = "unavailable"
    if any(part["kind"] != "text" for part in message["parts"]):
        state = "unavailable"  # No raw media locator or unsupported attachment projection.
    return dict(
        message_id=message["message_key"]["message_id"],
        revision=message["message_key"]["revision"],
        sent_at=message["sent_at"],
        state=state,
        parts=message["parts"] if state == "active" else [],
    )


def sources_visible(core, turn, authority, visited=None):
    """Use durable per-source evidence; not old origin expiry or global Memory versions.

    Prior local dialogue and explicit plan dependencies are checked recursively. The
    current release has no viewer-authorized Memory provenance revalidation endpoint;
    remote profile/evidence revocations that have not reached Core remain a stated gap.
    """
    visited = {} if visited is None else visited
    if turn["id"] in visited:
        return visited[turn["id"]]
    if len(visited) >= 64:
        return False
    visited[turn["id"]] = False
    if (
        turn.get("display_invalidated")
        or turn["scope"] != authority["scope"]
        or turn["binding_version"] != authority["binding_version"]
        or turn["bundle"]["collection_key"]
        != dict(channel=authority["channel"], author=authority["account"])
    ):
        return False
    if not all(message_current(core.store, turn["scope"], m) for m in turn["bundle"]["messages"]):
        return False
    predecessors = {d["turn_id"] for d in turn["bundle"]["dependencies"]}
    predecessors.update((turn.get("short_context") or {}).get("turn_ids", []))
    continuation = turn["bundle"]["continuation_of"]
    if continuation:
        group = core.store.get("collections", continuation)
        if (
            group is None
            or group["scope"] != authority["scope"]
            or not group.get("turn_id")
            or group["collection_key"]
            != dict(channel=authority["channel"], author=authority["account"])
        ):
            return False
        predecessors.add(group["turn_id"])
    for key in predecessors:
        prior = core.store.get("turns", key)
        if prior is None or not sources_visible(core, prior, authority, visited):
            return False
    visited[turn["id"]] = True
    return True


def turn_view(core, turn, authority):
    visible = sources_visible(core, turn, authority)
    replies = []
    for reply in core._replies(turn):
        if reply["state"] == "cancelled":
            if (
                reply.get("attempted_at") is not None
                or reply.get("request")
                or reply.get("receipt")
            ):
                raise Fault("dependency_unavailable")
            continue  # Unattempted cancelled drafts are not delivery facts in this wire.
        receipt = reply.get("receipt")
        available = (
            visible
            and reply["state"] == "sent"
            and receipt
            and receipt["state"] == "sent"
            and receipt["channel_message_ids"]
        )
        replies.append(
            dict(
                reply_id=reply["id"],
                segment_sequence=reply["segment_sequence"],
                segment_count=reply["segment_count"],
                state=reply["state"],
                content_state="available" if available else "unavailable",
                text=reply["text"] if available else None,
            )
        )
    messages = [message_view(core, turn["scope"], m) for m in turn["bundle"]["messages"]]
    if turn.get("display_invalidated"):
        for message in messages:
            message.update(state="unavailable", parts=[])
    return dict(turn=core.turn_wire(turn), messages=messages, replies=replies)


async def _read(core, service, request):
    first = await authorize(core, service, request)
    current = await authorize(core, service, request)
    if any(first[key] != current[key] for key in first if key != "expires_at"):
        raise Fault("scope_changed")
    # No await after this point: current source facts and output form one owner-loop view.
    collectors = rows(core, "collections", current, limit=33)
    active = rows(core, "turns", current, terminal=False, limit=65)
    history = rows(
        core,
        "turns",
        current,
        terminal=True,
        before=request["before_turn_sequence"],
        limit=request["limit"] + 1,
    )
    if len(collectors) > 32 or len(active) > 64:
        raise Fault("budget_exceeded")
    more = len(history) > request["limit"]
    history = history[: request["limit"]]
    if any(
        item["binding_version"] != current["binding_version"]
        for item in collectors + active + history
    ):
        raise Fault("scope_changed")
    result = dict(
        schema_version=1,
        request_id=request["query"]["request_id"],
        conversation_id=request["conversation_id"],
        actor_id=request["actor_id"],
        observed_at=utc(core.clock()),
        collectors=[
            dict(
                collection_id=c["id"],
                revision=c["revision"],
                deadline_at=utc(c["deadline"]),
                messages=[message_view(core, c["scope"], m) for m in c["messages"]],
            )
            for c in collectors
        ],
        active_turns=[turn_view(core, t, current) for t in active],
        history=[turn_view(core, t, current) for t in history],
        next_before_turn_sequence=history[-1]["sequence"] if more else None,
    )
    deadline(core, request)
    if epoch(current["expires_at"]) <= core.clock():
        raise Fault("forbidden")
    if len(canonical(result).encode()) > MAX_BYTES:
        raise Fault("budget_exceeded")
    try:
        core.contracts.check("web-conversation#snapshot_response", result)
    except Fault:
        # An existing legitimate group cannot fit the projection's field/array bounds.
        raise Fault("budget_exceeded") from None
    for group in result["collectors"] + result["active_turns"] + result["history"]:
        keys = [(m["message_id"], m["revision"]) for m in group["messages"]]
        if len(set(keys)) != len(keys):
            raise Fault("dependency_unavailable")
        replies = group.get("replies", [])
        segments = [r["segment_sequence"] for r in replies]
        if len(set(segments)) != len(segments) or any(
            r["segment_sequence"] > r["segment_count"] for r in replies
        ):
            raise Fault("dependency_unavailable")
    deadline(core, request)
    if epoch(current["expires_at"]) <= core.clock():
        raise Fault("forbidden")
    return result
