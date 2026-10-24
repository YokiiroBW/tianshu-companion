"""Whole-group preparation planning; Memory attests checks, Core chooses context."""

import copy
from dataclasses import dataclass, field, replace

from .clients import JsonService, query
from .context import (
    TEXT_DOMAIN,
    TEXT_INPUT_DOMAIN,
    bound_recent_provenance,
    merge_checks,
    profile_check,
    profile_targets,
    recent_checks,
)
from .context_checks import VerificationCheckpoint, dependency_changed, read_parallel, verify_checks
from .context_relevance import input_only
from .contracts import Fault, PROFILE_DOMAIN, canonical, digest
from .delivery import SOURCE_RENEWAL_MARGIN_SECONDS
from .short_context import select_recent
from .preparation_retry import verify_prepared_memory


def _lease(core, turn, check):
    identity = check.get("source_turn_id")
    if identity:
        previous = core.store.get("turns", identity)
        if previous is None:
            raise dependency_changed()
        return previous["source_deadline"]
    if check["origin"] == turn["origin"]:
        current = core.store.get("turns", turn["id"])
        if current is None:
            raise dependency_changed()
        return current["source_deadline"]
    return None


def _checkpoint_changed():
    # The adopted set stays intact. Only its pure-read checkpoint may restart
    # through the existing bounded preparation/preflight recovery policy.
    return Fault("dependency_unavailable", retryable_read=True)


@dataclass
class AdoptedChecks:
    """Local authority proofs awaiting one final, complete Memory barrier."""

    catalog: dict = field(default_factory=dict)
    checks: dict = field(default_factory=dict)
    leases: dict = field(default_factory=dict)

    async def collect(self, core, turn, checks, checkpoint, *, read_budget=0):
        async def attest(check, point):
            self.leases[digest(check)] = await core._verify_context_check(
                turn, check, point, memory_checks=self.checks, read_budget=read_budget
            )

        checks = merge_checks(checks)
        self.catalog.update((digest(check), check) for check in checks)
        await verify_checks(checks, attest, checkpoint=checkpoint)

    async def verify(self, core, turn, checkpoint):
        # Other main/QQ/book/local reads finish before this phase. Their waits
        # may have consumed an early source's renewal margin, or another caller
        # may have renewed it. Never attach an old authorization to a new lease.
        read_budget = JsonService.QUERY_BUDGET_SECONDS
        margin = SOURCE_RENEWAL_MARGIN_SECONDS + read_budget
        await core._refresh_preparation_origin(turn, read_budget=read_budget, checkpoint=checkpoint)
        main = dict(
            service=turn["service"],
            origin=turn["origin"],
            channel=turn["bundle"]["collection_key"]["channel"],
            scope=turn["scope"],
            binding_version=turn["binding_version"],
        )
        main_lease = _lease(core, turn, main)
        await core._authorize_context(main, checkpoint, main_lease)

        pending = [
            check
            for identity, check in self.catalog.items()
            if (lease := _lease(core, turn, check)) != self.leases[identity]
            or lease is not None
            and lease <= core.clock() + margin
        ]
        # One coordination pass, never a new retry loop. Changed deadlines use
        # new checkpoint keys, so each renewal gets its own authority proof.
        await self.collect(core, turn, pending, checkpoint, read_budget=read_budget)

        def current(*, margin):
            if _lease(core, turn, main) != main_lease or main_lease <= core.clock() + margin:
                raise _checkpoint_changed()
            for identity, check in self.catalog.items():
                lease = _lease(core, turn, check)
                if lease != self.leases[identity] or (
                    lease is not None and lease <= core.clock() + margin
                ):
                    raise _checkpoint_changed()

        current(margin=SOURCE_RENEWAL_MARGIN_SECONDS)
        await verify_prepared_memory(
            core.memory, turn, [self.checks[key] for key in sorted(self.checks)]
        )
        # The 20-second margin governs early renewal, not publication. A proof
        # returned before its unchanged durable deadline is still valid.
        current(margin=0)


async def _v2_variants(core, turn, checks, batches):
    plans = []
    catalog = {digest(check): copy.deepcopy(check) for check in merge_checks(checks)}
    required = set(catalog)
    for batch in batches:
        variants = [batch]
        if any(member["replies"] for member in batch):
            variants.append([input_only(member) for member in batch])
        planned = []
        for candidate in variants:
            inherited = recent_checks(core.store, candidate)
            members = []
            for check in inherited:
                identity = digest(check)
                catalog.setdefault(identity, copy.deepcopy(check))
                members.append(identity)
            planned.append((candidate, inherited, members))
        plans.append(planned)

    origins = {
        identity: canonical([check["service"], check["origin"]])
        for identity, check in catalog.items()
    }
    required_origins = {canonical([turn["service"], turn["origin"]])}
    required_origins.update(origins[key] for key in required)

    async with VerificationCheckpoint() as checkpoint:

        async def attest(identity, check):
            memory_checks = {}
            try:
                lease = await core._verify_context_check(
                    turn,
                    check,
                    checkpoint,
                    memory_checks=memory_checks,
                    read_budget=JsonService.QUERY_BUDGET_SECONDS,
                )
            except Fault as error:
                if (
                    identity not in required
                    and not error.unknown
                    and getattr(error, "context_dependency_invalid", False)
                ):
                    return identity, "rejected", {}, None
                if (
                    identity not in required
                    and origins[identity] not in required_origins
                    and check["version_domain"] in {TEXT_DOMAIN, TEXT_INPUT_DOMAIN, PROFILE_DOMAIN}
                    and not error.unknown
                    and not getattr(error, "context_renewal_failure", False)
                    and getattr(error, "origin_read_unavailable", False)
                ):
                    return identity, "unavailable", {}, None
                raise
            return identity, "valid", memory_checks, lease

        results = await read_parallel(
            [lambda identity=i, check=c: attest(identity, check) for i, c in catalog.items()]
        )
        unavailable_origins = {
            origins[identity] for identity, status, _, _ in results if status == "unavailable"
        }
        local, memory, links, required_memory, leases = {}, {}, {}, set(), {}
        for identity, valid, values, lease in results:
            if origins[identity] in unavailable_origins:
                valid, values, lease = "unavailable", {}, None
            local[identity], links[identity], leases[identity] = valid, tuple(values), lease
            for key, value in values.items():
                if key in memory and memory[key] != value:
                    raise Fault("invalid_input")
                memory[key] = value
                if identity in required:
                    required_memory.add(key)
        ordered = [memory[key] for key in sorted(memory)]
        outcomes = await core.memory.prepare_context(
            turn["origin"],
            turn["scope"],
            turn["memory_context"]["bundle"],
            ordered,
            sorted(required_memory),
        )
        if set(outcomes) != set(memory):
            raise Fault("invalid_input")
        for identity, check in catalog.items():
            if local[identity] != "valid" or any(
                outcomes[key] != "valid" for key in links[identity]
            ):
                # Rejected/unavailable Memory dependencies cannot be adopted;
                # their lease must neither extend nor invalidate valid material.
                continue
            lease = _lease(core, turn, check)
            if lease != leases[identity]:
                # A concurrent renewal invalidates this checkpoint, not selection.
                raise Fault("dependency_unavailable", retryable_read=True)
            if lease is not None and lease <= core.clock():
                raise Fault("forbidden")
        return [
            [
                (candidate, inherited)
                for candidate, inherited, members in variants
                if all(
                    local[identity] == "valid"
                    and all(outcomes[key] == "valid" for key in links[identity])
                    for identity in members
                )
            ]
            for variants in plans
        ]


async def recent(core, turn, context, checks):
    policy = replace(
        core.short_context_policy,
        max_bytes=min(core.short_context_policy.max_bytes, context.remaining),
    )
    selected, metadata = select_recent(core.store, turn, policy, core.clock())
    if not core._turn_allows(turn, "memory.read"):
        selected = []
    linked = {key: ids for ids in metadata.get("linked_turn_ids", []) for key in ids}
    by_id = {group["turn_id"]: group for group in selected}
    visited, batches = set(), []
    for group in selected:
        if group["turn_id"] not in visited:
            ids = linked.get(group["turn_id"], [group["turn_id"]])
            batches.append([by_id[key] for key in ids])
            visited.update(ids)
    if turn.get("memory_context") is not None:
        batches, metadata["reply_dependency_omissions"] = bound_recent_provenance(
            core.store, batches, checks
        )
        await core._refresh_preparation_origin(turn, read_budget=JsonService.QUERY_BUDGET_SECONDS)
        prepared = await _v2_variants(core, turn, checks, batches)
        accepted = []
        for variants in prepared:
            for candidate, inherited in variants:
                if context.extend("recent_dialogue", candidate):
                    checks = merge_checks(checks, inherited)
                    accepted.extend(candidate)
                break
    else:
        accepted, checks = await _legacy_recent(core, turn, context, checks, batches)
    accepted.sort(key=lambda group: group["turn_sequence"])
    context.data["recent_dialogue"] = accepted
    metadata.update(
        turn_ids=[g["turn_id"] for g in accepted],
        reply_ids=[r["reply_id"] for g in accepted for r in g["replies"]],
        bytes_used=len(canonical(accepted).encode()) if accepted else 0,
    )
    return accepted, metadata, checks


async def _legacy_recent(core, turn, context, checks, batches):
    # Persisted v1 turns keep the existing independent checks and fallback.
    verified, checkpoint = {}, VerificationCheckpoint()
    accepted = []
    try:
        if await core._refresh_preparation_origin(turn):
            await checkpoint.close()
            checkpoint = VerificationCheckpoint()
        await core._verify_checks(turn, checks, verified=verified, checkpoint=checkpoint)

        async def precheck(batch):
            try:
                inherited = recent_checks(core.store, batch)
                await core._verify_checks(turn, inherited, verified=verified, checkpoint=checkpoint)
                return inherited, None
            except Fault as error:
                return None, error

        if await core._refresh_preparation_origin(turn):
            await checkpoint.close()
            checkpoint = VerificationCheckpoint()
        prepared = await read_parallel([lambda b=b: precheck(b) for b in batches])
        reusable = True
        for batch, primary in zip(batches, prepared):
            variants = [batch]
            if any(member["replies"] for member in batch):
                variants.append([input_only(member) for member in batch])
            for candidate in variants:
                if await core._refresh_preparation_origin(turn):
                    await checkpoint.close()
                    checkpoint = VerificationCheckpoint()
                    reusable = False
                try:
                    if reusable and candidate is batch:
                        inherited, error = primary
                        if error is not None:
                            raise error
                    else:
                        inherited = recent_checks(core.store, candidate)
                        await core._verify_checks(
                            turn, inherited, verified=verified, checkpoint=checkpoint
                        )
                    proposed = merge_checks(checks, inherited)
                    if len(proposed) > 64:
                        raise Fault("budget_exceeded")
                except Fault:
                    await checkpoint.close()
                    checkpoint = VerificationCheckpoint()
                    reusable = False
                    continue
                if context.extend("recent_dialogue", candidate):
                    checks = proposed
                    accepted.extend(candidate)
                break
    finally:
        await checkpoint.close()
    return accepted, checks


async def profiles(core, turn, context, accepted):
    text = core._input_text(turn) or "media"
    targets = profile_targets(turn, accepted) if core._turn_allows(turn, "memory.read") else []
    checks = []
    for offset in range(0, len(targets), 4):
        allowance = min(4096, context.remaining)
        if allowance <= 0:
            break
        batch = targets[offset : offset + 4]
        await core._refresh_preparation_origin(turn, read_budget=JsonService.QUERY_BUDGET_SECONDS)
        budget = dict(tokens=allowance, bytes=allowance)
        if turn.get("memory_context") is not None:
            requests = [
                dict(
                    query=query(turn["origin"]),
                    requester_scope=turn["scope"],
                    target=target,
                    query_text=text,
                    selection=selection,
                    known_scope_version=None,
                    budget=budget,
                )
                for target, selection in batch
            ]
            responses = await core.memory.profiles_batch(turn["origin"], turn["scope"], requests)
        else:
            responses = await read_parallel(
                [
                    lambda target=target, selection=selection: core.memory.profiles(
                        turn["origin"], turn["scope"], target, text, selection, budget
                    )
                    for target, selection in batch
                ]
            )
        for (target, selection), response in zip(batch, responses, strict=True):
            if response["selected_units"] and context.append(
                "profiles",
                dict(
                    target=target,
                    selected_units=response["selected_units"],
                    dependency_groups=response["dependency_groups"],
                ),
            ):
                checks.append(profile_check(turn, target, text, selection, response))
    return checks
