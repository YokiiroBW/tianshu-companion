"""Whole-turn context accounting and bounded provenance; no identity inference."""

import copy

from .contracts import Fault, PROFILE_DOMAIN, canonical, digest

TEXT_DOMAIN = "text-dialogue/v1"
TEXT_INPUT_DOMAIN = "text-input/v1"
CONTEXT_LIMIT = 16384


class TurnContext:
    """UTF-8 bytes are also a conservative token bound, never measured model usage."""

    def __init__(self, selection, fragments, dependencies, *, memory=None):
        self.data = dict(
            evidence=selection["selected_units"],
            dependency_groups=selection["dependency_groups"],
            profiles=[],
            recent_dialogue=[],
            earlier_fragment=fragments,
            delivered_dependencies=dependencies,
        )
        if memory is not None:
            from .memory_context import prompt_material

            self.data.update(prompt_material(memory))
        if self.used > CONTEXT_LIMIT:
            # Explicit continuation/dependency inputs cannot be silently cut in half.
            raise Fault("budget_exceeded")

    @property
    def used(self):
        return len(canonical(self.data).encode("utf-8"))

    @property
    def remaining(self):
        return CONTEXT_LIMIT - self.used

    def append(self, name, value):
        return self.extend(name, [value])

    def extend(self, name, values):
        before = len(self.data[name])
        self.data[name].extend(values)
        if self.remaining < 0:
            del self.data[name][before:]
            return False
        return True


def scope_check(turn, *, input_only=False, raw_input=None):
    input_only = input_only and (
        raw_input is not None or turn.get("input_history_snapshot") is not None
    )
    check = dict(
        version_domain=TEXT_INPUT_DOMAIN if input_only else TEXT_DOMAIN,
        source_turn_id=turn["id"],
        origin=turn["origin"],
        scope=turn["scope"],
        channel=turn["bundle"]["collection_key"]["channel"],
        service=turn["service"],
        binding_version=turn["binding_version"],
        scope_version=turn["scope_version"],
        history_snapshot=turn.get("input_history_snapshot" if input_only else "history_snapshot"),
        memory_snapshot={
            key: turn["preparation"][key] for key in ("association_version", "scope_checks")
        }
        if turn.get("preparation") and "association_version" in turn["preparation"]
        else None,
    )
    if input_only and raw_input is not None:
        # This projection carries only these original human inputs. Its old
        # query's history/association set is not a version of their raw text.
        # Bind the actual selected material, not a later reread of the turn.
        check.update(
            scope=dict(
                turn["scope"], actor_id=raw_input["actor_id"], person_id=raw_input["person_id"]
            ),
            input_author=raw_input["author"],
            collection_id=raw_input["collection_id"],
            input_revision=raw_input["input_revision"],
            input_sources=[m["source"] for m in raw_input["messages"]],
        )
        for key in ("scope_version", "history_snapshot", "memory_snapshot"):
            del check[key]
        return copy.deepcopy(check)
    return check


def profile_check(turn, target, text, selection, response):
    return dict(
        scope_check(turn),
        version_domain=PROFILE_DOMAIN,
        target=target,
        text=text,
        selection=selection,
        scope_version=response["scope_version"],
    )


def merge_checks(*parts):
    checks = {digest(check): check for part in parts for check in part}
    # The v2 wire check limit applies to a batch, not the lifetime of a conversation.
    # Callers keep the complete deduplicated dependency set and validate every batch.
    return list(checks.values())


def inherited_checks(previous):
    # Include the checks behind confirmed replies, not merely their visible text.
    return merge_checks(
        [scope_check(previous)],
        previous.get("profile_checks", []),
        previous.get("context_checks", []),
    )


def recent_checks(store, candidate, *, exact_inputs=False):
    checks = []
    for member in candidate:
        previous = store.get("turns", member["turn_id"])
        input_projection = not member["replies"] or (member.get("input_only") and not exact_inputs)
        checks = merge_checks(
            checks,
            [scope_check(previous, input_only=True, raw_input=member if exact_inputs else None)]
            if input_projection
            else inherited_checks(previous),
        )
    return checks


def bound_recent_provenance(store, batches, mandatory):
    """Optional replies cannot implicitly expand a bounded history selection.

    A reply whose proof reaches outside selected groups is replaced by the
    existing whole-group human-input projection. Required plan/continuation
    dependencies remain complete and are never truncated here.
    """
    from .context_relevance import input_only

    selected = {member["turn_id"] for batch in batches for member in batch}
    selected.update(check.get("source_turn_id") for check in mandatory)
    result, omitted = [], []
    for batch in batches:
        dependencies = recent_checks(store, batch)
        outside = any(
            check.get("source_turn_id") and check["source_turn_id"] not in selected
            for check in dependencies
        )
        if outside:
            omitted.extend(member["turn_id"] for member in batch if member["replies"])
            batch = [input_only(member) for member in batch]
        result.append(batch)
    return result, omitted


def profile_targets(turn, recent):
    if turn["scope"]["audience"] != "group":
        return []
    people = []
    # Exact account mentions prioritize known authors. Never resolve a supplied nickname,
    # or look up another account using the requester's identity assertion.
    mentions = [a for m in turn["bundle"]["messages"] for a in m["mentioned_accounts"]]
    authors = [m for g in reversed(recent) for m in g["messages"]]
    authors.sort(key=lambda m: m["author"] not in mentions)
    for message in authors + turn["bundle"]["messages"]:
        if message["person_id"] not in people:
            people.append(message["person_id"])
    return [
        (dict(kind="group", conversation_id=turn["conversation_id"]), ["topic", "style"]),
        *((dict(kind="person", person_id=p), ["interest", "style"]) for p in people),
    ]
