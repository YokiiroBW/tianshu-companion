"""Whole-turn context accounting and bounded provenance; no identity inference."""

from .contracts import Fault, PROFILE_DOMAIN, canonical, digest

TEXT_DOMAIN = "text-dialogue/v1"
CONTEXT_LIMIT = 16384
MAX_CHECKS = 64


class TurnContext:
    """UTF-8 bytes are also a conservative token bound, never measured model usage."""

    def __init__(self, selection, fragments, dependencies):
        self.data = dict(
            evidence=selection["selected_units"],
            dependency_groups=selection["dependency_groups"],
            profiles=[],
            recent_dialogue=[],
            earlier_fragment=fragments,
            delivered_dependencies=dependencies,
        )
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
        self.data[name].append(value)
        if self.remaining < 0:
            self.data[name].pop()
            return False
        return True


def scope_check(turn):
    return dict(
        version_domain=TEXT_DOMAIN,
        origin=turn["origin"],
        scope=turn["scope"],
        channel=turn["bundle"]["collection_key"]["channel"],
        service=turn["service"],
        binding_version=turn["binding_version"],
        scope_version=turn["scope_version"],
        memory_snapshot={
            key: turn["preparation"][key] for key in ("association_version", "scope_checks")
        }
        if turn.get("preparation") and "association_version" in turn["preparation"]
        else None,
    )


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
    if len(checks) > MAX_CHECKS:
        raise Fault("budget_exceeded")
    return list(checks.values())


def inherited_checks(previous):
    # Include the checks behind confirmed replies, not merely their visible text.
    return merge_checks(
        [scope_check(previous)],
        previous.get("profile_checks", []),
        previous.get("context_checks", []),
    )


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
