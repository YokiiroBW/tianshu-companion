"""Core-owned physical/admission facts and conservative legacy recovery."""

import copy

from .clients import epoch, uid, utc
from .contracts import Fault, canonical, digest
from .model_selection import SelectionRequest, resolve_selection, verify_lease

INPUT_FIELDS = (
    "message_key",
    "author",
    "sent_at",
    "kind",
    "parts",
    "reply_refs",
    "mentioned_accounts",
)
UNCLASSIFIED = dict(
    value="unclassified", basis="unclassified", policy_ref=None, policy_version=None
)


def physical_key(data):
    return {k: v for k, v in data["message_key"].items() if k != "revision"}


def actor_key(data, actor):
    return digest([data["message_key"], actor])


def needs_model_reservation(core, data, channel, cid, actor, now):
    """Read-only plan; accept_actor repeats the decision inside its transaction."""
    if core.store.get("inbox", actor_key(data, actor)):
        return False
    if cid is None:
        return True
    collection_key = dict(channel=channel, author=data["author"])
    return not any(
        c["collection_key"] == collection_key
        and c["scope"]["actor_id"] == actor
        and c["deadline"] > now
        for c in core.store.list("collections", cid, ["collecting"])
    )


def selector(data, actor):
    return dict(key=physical_key(data), actor_id=actor)


def make_physical(data, cid, audience, classification):
    fact = dict(
        key=physical_key(data),
        revision=data["message_key"]["revision"],
        physical_receipt_id=uid("physical"),
        conversation_id=cid,
        author=data["author"],
        content_digest=digest(data),
        kind=data["kind"],
        state="withdrawn" if data["kind"] == "retract" else "active",
        classification=copy.deepcopy(classification),
        content=copy.deepcopy(data),
        audience=audience,
    )
    return dict(
        id=digest(data["message_key"]),
        conversation_id=cid,
        base=digest(fact["key"]),
        revision=fact["revision"],
        request=copy.deepcopy(data),
        fact=fact,
    )


def make_admission(data, actor, scope, source, physical, binding_version, origin, accepted_at):
    return dict(
        selector=selector(data, actor),
        scope=scope,
        source=source,
        physical_receipt_id=physical["fact"]["physical_receipt_id"],
        binding_version=binding_version,
        accepted_origin=origin,
        accepted_at=accepted_at,
    )


def ensure_channel(store, channel):
    conv = store.get("conversations", digest(channel))
    if conv and conv.get("source_quarantined"):
        raise Fault("dependency_unavailable")
    return conv


def migrate_legacy(store):
    """Never assign an old receipt using the current collector's actor alone.

    A singleton actor in the original immutable request is mandatory. A group's
    entire original receipt set must corroborate that actor and scope; otherwise
    quarantine the conversation, retaining every old record for offline review.
    Legacy reality was hardcoded, so its migration classification is unknown.
    """
    if store.get("metadata", "source_migration"):
        return
    with store.transaction():
        for conv in store.list("conversations"):
            cid = conv["conversation_id"]
            rows = store.list("inbox", cid)
            proofs = []
            try:
                withdrawn = set()
                for row in sorted(rows, key=lambda r: r["revision"]):
                    base = digest(physical_key(row["request"]))
                    if base in withdrawn:
                        raise ValueError("legacy tombstone followed by later input")
                    if row["request"]["kind"] == "retract":
                        withdrawn.add(base)
                for row in rows:
                    request, receipt = row["request"], row["receipt"]
                    targets = request["target_actor_ids"]
                    if len(targets) != 1 or request["kind"] == "retract":
                        # Retractions can be migrated physically but must never
                        # acquire an actor source/control receipt mapping.
                        if request["kind"] != "retract":
                            raise ValueError("ambiguous original actor")
                    group = store.get("collections", row["collection_id"])
                    actor = targets[0] if len(targets) == 1 else None
                    if (
                        not group
                        or (actor and group["scope"]["actor_id"] != actor)
                        or group["scope"]["person_id"] != receipt["person_id"]
                        or group["scope"]["conversation_id"] != cid
                        or group["collection_key"] != receipt["collection_key"]
                    ):
                        raise ValueError("conflicting receipt scope")
                    if any(
                        m["target_actor_ids"] != [group["scope"]["actor_id"]]
                        for m in group["messages"]
                    ):
                        raise ValueError("mixed group")
                    # The group is corroboration, not the original actor evidence.
                    proofs.append((row, actor, group))
                for turn in store.list("turns", cid):
                    for message in turn["bundle"]["messages"]:
                        match = next(
                            (
                                r
                                for r in rows
                                if r["receipt"]["receipt_id"] == message["source"]["receipt_id"]
                            ),
                            None,
                        )
                        if not match or match["request"]["target_actor_ids"] != [
                            turn["scope"]["actor_id"]
                        ]:
                            raise ValueError("mixed or unproven turn")
            except (KeyError, TypeError, ValueError):
                conv["source_quarantined"] = True
                store.put("conversations", conv)
                continue
            for row, actor, group in sorted(proofs, key=lambda item: item[0]["revision"]):
                request = row["request"]
                data = {k: request[k] for k in INPUT_FIELDS}
                physical = make_physical(data, cid, group["scope"]["audience"], UNCLASSIFIED)
                store.put("physicals", physical)
                if data["kind"] == "retract":
                    continue
                admission = make_admission(
                    data,
                    actor,
                    group["scope"],
                    row["source"],
                    physical,
                    group["binding_version"],
                    request["command"]["origin"],
                    row["receipt"]["accepted_at"],
                )
                row.update(id=actor_key(data, actor), actor_id=actor, admission=admission)
                row["authorization"] = dict(
                    kind="legacy_original_request",
                    ingress_service=group["service"],
                    actor_origin=request["command"]["origin"],
                    input_origin=None,
                )
                store.put("inbox", row)
                store.put(
                    "admissions",
                    dict(
                        id=digest(admission["selector"]),
                        conversation_id=cid,
                        fact=admission,
                        authorization=row["authorization"],
                    ),
                )
                # Remove only the proven old index row inside the same transaction.
                store.db.execute("DELETE FROM inbox WHERE id=?", (digest(data["message_key"]),))
        store.put("metadata", dict(id="source_migration", version=2))


def read_facts(store, contracts, service, request):
    if service != "memory":
        raise Fault("forbidden")
    contracts.check("sources#facts_request", request)
    with store.transaction():
        physicals, admissions, turns = [], [], []
        keys = {}
        for chosen in request["selectors"]:
            keys[digest(chosen["key"])] = chosen["key"]
            ensure_channel(store, chosen["key"]["channel"])
            row = store.get("admissions", digest(chosen))
            admissions.append(
                copy.deepcopy(row["fact"]) if row else dict(selector=chosen, state="missing")
            )
        for base, key in keys.items():
            conv = ensure_channel(store, key["channel"])
            row = store.latest_source(conv["conversation_id"], base) if conv else None
            if row:
                fact = copy.deepcopy(row["fact"])
                if not request["include_content"]:
                    fact["content"] = None
                physicals.append(fact)
            else:
                physicals.append(dict(key=key, state="missing"))
        for tid in request["turn_ids"]:
            turn = store.get("turns", tid)
            if not turn:
                turns.append(dict(turn_id=tid, state="missing"))
                continue
            ensure_channel(store, turn["bundle"]["collection_key"]["channel"])
            events = [
                r
                for r in store.list("outbox", turn["conversation_id"])
                if r["event"]["aggregate_id"] == tid
            ]
            saved = events[0]["event"] if events else None
            if saved and (saved["scope_version"] is None or saved["reality"] is None):
                saved = None
            turns.append(
                dict(
                    turn_id=tid,
                    scope=turn["scope"],
                    input_revision=turn["bundle"]["collection_revision"],
                    aggregate_version=turn["version"],
                    context_revision=turn.get("context_revision") or 1,
                    input_sources=[m["source"] for m in turn["bundle"]["messages"]],
                    phase=turn["phase"],
                    delivery_state=turn["delivery_state"]
                    if turn["delivery_state"] != "not_started"
                    else "not_required",
                    reply_ids=[
                        r["id"]
                        for r in store.turn_replies(tid)
                        if r["state"] in {"sent", "unknown", "sending"}
                    ],
                    committed_event=saved,
                )
            )
        response = dict(
            schema_version=1,
            request_id=request["request_id"],
            request_digest=digest(request),
            head=store.source_head(),
            physicals=physicals,
            admissions=admissions,
            turns=turns,
        )
        contracts.check("sources#facts_response", response)
        if len(canonical(response).encode()) > 1_048_576:
            raise Fault("dependency_unavailable")
        return response


def context_valid(core, service, data, context, actor, audience, cid, person=None, *, legacy=False):
    if not context:
        return False
    scope, channel = context["allowed_scope"], data["message_key"]["channel"]
    binding = core.bindings.get(channel["binding_id"], {})
    return (
        bool(binding)
        and binding.get("service") == service
        and binding.get("namespace") == channel["namespace"]
        and binding.get("audience") == audience
        and actor in binding.get("actor_ids", [])
        and actor in core.roles
        and (legacy or context["issuer"] == "platform")
        and context["authenticated_service"] == service
        and context["audience_service"] == "companion"
        and not context["revoked"]
        and context["verified_account"] == data["author"]
        and context["verified_channel"] == channel
        and data["author"]["namespace"] == channel["namespace"]
        and scope["actor_id"] == actor
        and scope["audience"] == audience
        and (person is None or scope["person_id"] in (None, person))
        and scope["conversation_id"] in (None, cid)
        and epoch(context["expires_at"]) > core.clock()
    )


def invalidate_physical(core, conv, base, kind):
    """Negative-only propagation reaches every registered actor in the channel."""
    cid = conv["conversation_id"]
    core.life.withdraw_dialogue(cid, base)
    conv["context_revision"] = conv.get("context_revision", 1) + 1
    core.store.put("conversations", conv)
    for collection in core.store.list("collections", cid):
        members = [m for m in collection["messages"] if digest(physical_key(m)) != base]
        if len(members) == len(collection["messages"]):
            continue
        if collection["state"] == "collecting":
            collection["messages"] = members
            collection["revision"] += 1
            if not members and kind != "edit":
                collection["state"] = "cancelled"
            core.store.put("collections", collection)
        elif collection.get("turn_id"):
            core.life.concerns.invalidate(
                "companion", collection["turn_id"], collection["revision"]
            )
            core._cancel_turn(core.store.get("turns", collection["turn_id"]), kind)


def accept_actor(
    core,
    service,
    request,
    data,
    physical,
    conv,
    actor,
    ctx,
    identity,
    defer,
    now,
    authorization,
    reservation=None,
):
    store = core.store
    person, binding_version = identity
    scope = dict(
        actor_id=actor,
        person_id=person,
        audience=physical["fact"]["audience"],
        conversation_id=conv["conversation_id"],
    )
    previous = store.get("inbox", actor_key(data, actor))
    if previous:
        if (
            previous["admission"]["scope"] != scope
            or previous["admission"]["binding_version"] != binding_version
        ):
            raise Fault("scope_changed")
        return dict(
            actor_id=actor,
            state="duplicate",
            admission=previous["admission"],
            receipt={
                **previous["receipt"],
                "request_id": request["command"]["request_id"],
                "deduplicated": True,
            },
        )
    cid = conv["conversation_id"]
    collection_key = dict(channel=data["message_key"]["channel"], author=data["author"])
    groups = store.list("collections", cid, ["collecting"])
    collection = next(
        (
            c
            for c in groups
            if c["collection_key"] == collection_key and c["scope"]["actor_id"] == actor
        ),
        None,
    )
    if collection and (
        collection["scope"] != scope or collection["binding_version"] != binding_version
    ):
        raise Fault("scope_changed")
    if collection and core.default_model_selector is not None:
        selected = collection.get("model_selection")
        if selected is None:
            raise Fault("dependency_unavailable")
        verify_lease(selected["expires_at"], now)
    if collection is None:
        if (
            len(groups) >= core.policy.max_collectors_per_conversation
            or len(groups) + len(store.list("turns", cid, ["queued"]))
            >= core.policy.max_queued_turns
        ):
            raise Fault("queue_full")
        if core.default_model_selector is not None:
            if reservation is None:
                # A collector closed after the read-only plan. Replan outside the
                # transaction; never select against a changed default here.
                error = Fault("dependency_unavailable")
                error.selection_race = True
                raise error
            verify_lease(reservation["expires_at"], now)
        continuations = [
            c
            for c in store.list("collections", cid)
            if c["collection_key"] == collection_key
            and c["scope"] == scope
            and c.get("possibly_incomplete")
            and not c.get("continued_by")
            and c.get("source_context_revision") == conv.get("context_revision", 1)
        ]
        collection = dict(
            id=uid("col"),
            conversation_id=cid,
            collection_key=collection_key,
            state="collecting",
            source_context_revision=conv.get("context_revision", 1),
            revision=0,
            started=now,
            deadline=now,
            sequence=conv["ingest_sequence"] + 1,
            messages=[],
            continuation_of=continuations[-1]["id"] if continuations else None,
            service=service,
            scope=scope,
            binding_version=binding_version,
            bootstrap_mapping=ctx["allowed_scope"]["conversation_id"] is None,
            correlation_id=core.ingress_correlation(),
            turn_id=reservation["turn_id"] if reservation else None,
            model_selection=reservation if reservation else None,
            role=(
                core.role_runtime.pin(actor)
                if hasattr(core, "role_runtime") and core.role_runtime.get(actor)
                else None
            ),
        )
    if (
        len(collection["messages"]) + 1 > core.policy.max_collection_messages
        or sum(len(canonical(m["parts"]).encode()) for m in collection["messages"])
        + len(canonical(data["parts"]).encode())
        > core.policy.max_collection_bytes
    ):
        error = Fault("queue_full")
        if collection["messages"]:
            error.collection_id = collection["id"]
        raise error
    conv["ingest_sequence"] += 1
    seq, receipt_id = conv["ingest_sequence"], uid("receipt")
    source = dict(
        message_key=data["message_key"],
        receipt_id=receipt_id,
        archive_state="pending",
        locator=None,
    )
    origin = dict(assertion_ref=ctx["assertion_ref"])
    message = {k: copy.deepcopy(v) for k, v in data.items() if k != "kind"}
    message.update(
        person_id=person,
        accepted_at=utc(now),
        ingest_sequence=seq,
        source=source,
        target_actor_ids=request["target_actor_ids"],
    )
    collection["messages"].append(message)
    collection["revision"] += 1
    collection.update(origin=origin, source_deadline=epoch(request["command"]["deadline_at"]))
    collection["deadline"] = core._deadline(collection, now)
    store.put("collections", collection)
    receipt = dict(
        schema_version=1,
        request_id=request["command"]["request_id"],
        receipt_id=receipt_id,
        deduplicated=False,
        conversation_id=cid,
        person_id=person,
        collection_key=collection_key,
        collection_id=collection["id"],
        collection_revision=collection["revision"],
        accepted_at=utc(now),
        ingest_sequence=seq,
        archive_state="pending",
    )
    admission = make_admission(
        data, actor, scope, source, physical, binding_version, origin, utc(now)
    )
    store.put(
        "inbox",
        dict(
            id=actor_key(data, actor),
            conversation_id=cid,
            sequence=seq,
            base=physical["base"],
            revision=physical["revision"],
            collection_id=collection["id"],
            correlation_id=core.ingress_correlation(),
            signature=digest(data),
            request={
                **data,
                "command": {**request["command"], "origin": origin},
                "target_actor_ids": request["target_actor_ids"],
            },
            receipt=receipt,
            source=source,
            stale=False,
            response_released=not defer,
            actor_id=actor,
            admission=admission,
            authorization={**authorization, "actor_origin": origin},
        ),
    )
    store.put(
        "admissions",
        dict(
            id=digest(admission["selector"]),
            conversation_id=cid,
            fact=admission,
            authorization={**authorization, "actor_origin": origin},
        ),
    )
    store.put("conversations", conv)
    core.life.influence_dialogue(actor, data, source, scope)
    return dict(actor_id=actor, state="accepted", receipt=receipt, admission=admission)


async def ingest(core, service, request, defer=False, *, legacy=False):
    for attempt in range(3):
        try:
            return await _ingest(core, service, request, defer, legacy=legacy)
        except Fault as error:
            if getattr(error, "selection_race", False) and attempt < 2:
                continue
            # The failed fanout has rolled back every new P/A. Close only an existing
            # full collector so the caller can retry its unaccepted input later.
            if error.code == "queue_full" and getattr(error, "collection_id", None):
                with core.store.transaction():
                    collection = core.store.get("collections", error.collection_id)
                    if collection and collection["state"] == "collecting":
                        core._seal(collection, core.clock(), "resource_limit")
            raise


async def _ingest(core, service, request, defer=False, *, legacy=False):
    store, contracts = core.store, core.contracts
    if service not in {"platform", "nonebot"}:
        raise Fault("forbidden")
    contracts.check("conversation#ingest_request" if legacy else "sources#fanout_request", request)
    original = request
    data = {k: request[k] for k in INPUT_FIELDS} if legacy else request["input"]
    channel = data["message_key"]["channel"]
    conv = ensure_channel(store, channel)
    cid = conv["conversation_id"] if conv else None
    operation = "ingest" if legacy else "ingest-actors"
    cmd_key, signature, prior = core._command_key(service, operation, original)
    if epoch(request["command"]["deadline_at"]) <= core.clock():
        raise Fault("timeout")
    legacy_identity = None
    if legacy:
        ctx, person, bv = await core._authorize(
            service, request["command"], channel, data["author"]
        )
        actor = ctx["allowed_scope"]["actor_id"]
        if any(a != actor for a in request["target_actor_ids"]):
            raise Fault("forbidden")
        # Empty legacy intent stays attached to the authorized actor, including
        # the established observe-only group behavior; never expand defaults.
        request = dict(
            schema_version=1,
            command=request["command"],
            input=data,
            target_actor_ids=request["target_actor_ids"],
        )
        contexts, effective = {actor: ctx}, [actor] if data["kind"] != "retract" else []
        authority = dict(
            expires_at=ctx["expires_at"],
            routing_version=1,
            audience=ctx["allowed_scope"]["audience"],
        )
        legacy_identity = person, bv
    else:
        authority = await core.origins.input_access(service, request)
        if epoch(authority["expires_at"]) <= core.clock():
            raise Fault("forbidden")
        contexts = {c["allowed_scope"]["actor_id"]: c for c in authority["actor_contexts"]}
        if len(contexts) != len(authority["actor_contexts"]):
            raise Fault("forbidden")
        effective = (
            []
            if data["kind"] == "retract"
            else sorted(
                prior["routing_record"]["effective_actor_ids"]
                if prior
                else request["target_actor_ids"] or authority["default_actor_ids"]
            )
        )
    binding = core.bindings.get(channel["binding_id"], {})
    if (
        binding.get("service") != service
        or binding.get("namespace") != channel["namespace"]
        or binding.get("audience") != authority["audience"]
        or data["author"]["namespace"] != channel["namespace"]
    ):
        raise Fault("forbidden")
    identity = legacy_identity
    grants = {}
    for actor in effective:
        ctx = contexts.get(actor)
        if not context_valid(
            core, service, data, ctx, actor, authority["audience"], cid, legacy=legacy
        ):
            continue
        current = legacy_identity or await core.memory.identity(
            dict(assertion_ref=ctx["assertion_ref"]), data["author"], core.clock()
        )
        if identity is not None and current != identity:
            raise Fault("scope_changed")
        identity = current
        if context_valid(
            core, service, data, ctx, actor, authority["audience"], cid, identity[0], legacy=legacy
        ):
            grants[actor] = ctx
    # Select before the accepting transaction. A queued turn then carries the exact version
    # even if the administrator changes the default before tick starts its background work.
    candidate_cid = cid or uid("conv")
    reservations = {}
    if core.default_model_selector is not None and data["kind"] != "retract" and prior is None:
        for actor in grants:
            # A new command key can still refer to an accepted physical input, or add
            # a message to an open collection. Both already own a pinned turn grant.
            if not needs_model_reservation(core, data, channel, cid, actor, core.clock()):
                continue
            turn_id = uid("turn")
            try:
                selected = await resolve_selection(
                    core.default_model_selector,
                    SelectionRequest(
                        turn_id, actor, identity[0], authority["audience"], candidate_cid
                    ),
                    core.clock,
                )
            except Fault as error:
                current = ensure_channel(store, channel)
                current_cid = current["conversation_id"] if current else None
                if not needs_model_reservation(
                    core, data, channel, current_cid, actor, core.clock()
                ):
                    error.selection_race = True
                raise
            reservations[actor] = dict(
                turn_id=turn_id,
                config_version=selected.config_version,
                expires_at=selected.expires_at,
            )
    with store.transaction():
        now = core.clock()
        if epoch(request["command"]["deadline_at"]) <= now:
            raise Fault("timeout")
        if epoch(authority["expires_at"]) <= now:
            raise Fault("forbidden")
        conv = ensure_channel(store, channel) or dict(
            id=digest(channel),
            conversation_id=candidate_cid,
            channel=channel,
            ingest_sequence=0,
            turn_sequence=0,
        )
        cid = conv["conversation_id"]
        if reservations and cid != candidate_cid:
            error = Fault("dependency_unavailable")
            error.selection_race = True
            raise error
        grants = {
            a: c
            for a, c in grants.items()
            if context_valid(
                core, service, data, c, a, authority["audience"], cid, identity[0], legacy=legacy
            )
        }
        _, _, fresh_prior = core._command_key(service, operation, original)
        if fresh_prior != prior:
            # Another request completed while identity was in flight. Re-enter
            # outside the transaction so frozen routing is reauthorized.
            error = Fault("dependency_unavailable")
            error.selection_race = True
            raise error
        if prior:
            if legacy:
                receipt = prior["response"]
                row = store.receipt_input(receipt["receipt_id"])
                if data["kind"] != "retract" and (not row or row.get("actor_id") not in grants):
                    raise Fault("forbidden")
                if (
                    row
                    and row.get("admission")
                    and (
                        row["admission"]["scope"]["person_id"] != identity[0]
                        or row["admission"]["binding_version"] != identity[1]
                    )
                ):
                    raise Fault("scope_changed")
                return {
                    **receipt,
                    "request_id": request["command"]["request_id"],
                    "deduplicated": True,
                }
            result = copy.deepcopy(prior["response"])
            result.update(
                request_id=request["command"]["request_id"],
                request_digest=digest(request),
                physical_deduplicated=True,
            )
            for outcome in result["outcomes"]:
                if outcome["state"] == "forbidden":
                    continue  # Original negative result is not promoted by a retry.
                if outcome["actor_id"] not in grants:
                    outcome.update(state="forbidden", receipt=None, admission=None)
                else:
                    if (
                        outcome["admission"]["scope"]["person_id"] != identity[0]
                        or outcome["admission"]["binding_version"] != identity[1]
                    ):
                        raise Fault("scope_changed")
                    outcome["state"] = "duplicate"
                    outcome["receipt"].update(
                        request_id=request["command"]["request_id"], deduplicated=True
                    )
            if not any(o["receipt"] for o in result["outcomes"]):
                result["person_id"] = None
            return result
        physical = store.get("physicals", digest(data["message_key"]))
        latest = store.latest_source(cid, digest(physical_key(data)))
        if latest and latest["request"]["author"] != data["author"]:
            raise Fault("forbidden")
        if physical and physical["fact"]["content_digest"] != digest(data):
            raise Fault("idempotency_conflict")
        if legacy and physical and data["kind"] != "retract":
            old = store.get("inbox", actor_key(data, actor))
            if old:
                if (
                    actor not in grants
                    or old["admission"]["scope"]["person_id"] != identity[0]
                    or old["admission"]["binding_version"] != identity[1]
                ):
                    raise Fault("forbidden")
                result = {
                    **old["receipt"],
                    "request_id": request["command"]["request_id"],
                    "deduplicated": True,
                }
                core._remember_command(cmd_key, signature, result)
                return result
        # Historical requests can recover their command result, but cannot grant
        # a new actor or resurrect a tombstone by using a different command key.
        if latest and (
            data["message_key"]["revision"] < latest["revision"]
            or (latest["fact"]["state"] == "withdrawn" and not physical)
        ):
            raise Fault("version_conflict")
        if not physical and (
            (latest is None and data["kind"] != "message")
            or (latest is not None and data["kind"] == "message")
        ):
            raise Fault("invalid_input")
        core._seal_due(now, cid)
        conv = store.get("conversations", conv["id"]) or conv
        deduplicated = physical is not None
        if not physical:
            classification = binding.get("classification", UNCLASSIFIED)
            contracts.check("shared#classification", classification)
            physical = make_physical(data, cid, authority["audience"], classification)
            store.put("conversations", conv)
            store.put("physicals", physical)
            if latest:
                invalidate_physical(core, conv, physical["base"], data["kind"])
                conv = store.get("conversations", conv["id"])
        outcomes = []
        for actor in sorted(effective):
            if actor not in grants:
                outcomes.append(
                    dict(actor_id=actor, state="forbidden", receipt=None, admission=None)
                )
                continue
            # New default routing has explicit actor intent for generation. Old
            # empty group targets retain observe-only semantics.
            actor_request = request if legacy else {**request, "target_actor_ids": [actor]}
            outcomes.append(
                accept_actor(
                    core,
                    service,
                    actor_request,
                    data,
                    physical,
                    conv,
                    actor,
                    grants[actor],
                    identity,
                    defer,
                    now,
                    dict(
                        kind="legacy_actor_origin" if legacy else "source_input_authority",
                        ingress_service=service,
                        input_origin=None if legacy else request["command"]["origin"],
                        authority_digest=digest(authority),
                    ),
                    reservations.get(actor),
                )
            )
        for collection in store.list("collections", cid, ["collecting"]):
            if not collection["messages"]:
                collection["state"] = "cancelled"
                store.put("collections", collection)
        if legacy and data["kind"] == "retract":
            # Old response shape requires a control receipt. It is deliberately
            # absent from admissions, inbox sources and future input bundles.
            old = next(
                (
                    r
                    for r in reversed(store.list("inbox", cid))
                    if r["base"] == physical["base"]
                    and r.get("actor_id") == ctx["allowed_scope"]["actor_id"]
                ),
                None,
            )
            if not old:
                raise Fault("forbidden")
            conv["ingest_sequence"] += 1
            store.put("conversations", conv)
            result = {
                **old["receipt"],
                "receipt_id": uid("control"),
                "request_id": request["command"]["request_id"],
                "ingest_sequence": conv["ingest_sequence"],
                "accepted_at": utc(now),
                "deduplicated": deduplicated,
            }
        elif legacy:
            if not outcomes or outcomes[0]["state"] == "forbidden":
                raise Fault("forbidden")
            result = outcomes[0]["receipt"]
        else:
            result = dict(
                schema_version=1,
                request_id=request["command"]["request_id"],
                request_digest=digest(request),
                physical_receipt_id=physical["fact"]["physical_receipt_id"],
                physical_deduplicated=deduplicated,
                conversation_id=cid,
                person_id=identity[0] if any(o["receipt"] for o in outcomes) else None,
                effective_actor_ids=effective,
                routing_version=authority["routing_version"],
                routing_state="routed" if effective else "unrouted",
                outcomes=outcomes,
            )
        contracts.check(
            "conversation#ingest_response" if legacy else "sources#fanout_response", result
        )
        store.put(
            "commands",
            dict(
                id=cmd_key,
                signature=signature,
                response=result,
                routing_record=dict(
                    semantic_digest=digest(
                        dict(input=data, target_actor_ids=request["target_actor_ids"])
                    ),
                    effective_actor_ids=effective,
                    routing_version=authority["routing_version"],
                ),
            ),
        )
        core._seal_due(now, cid)
        return result
