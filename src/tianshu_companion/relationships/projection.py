"""Bounded expression data and version pins, never identity or a second score ledger."""

from ..clients import epoch
from ..context import scope_check
from ..contracts import Fault, canonical
from .contract import DOMAIN, SCHEMA_HASH


async def prepare(runtime, core, turn, context):
    if not core._turn_allows(turn, "memory.read"):
        return []
    pin = dict(
        scope_check(turn),
        version_domain=DOMAIN,
        schema_hash=SCHEMA_HASH,
        relationship_view="unavailable",
        relationship_version=None,
        checked_at=None,
    )
    try:
        value = await runtime.client.read(turn["origin"], turn["scope"], core.clock)
    except (Fault, OSError, TimeoutError):
        # No cached state and no invented score/type. Ordinary dialogue remains available.
        return [pin]
    if value["view"] == "private":
        expression = {
            name: value[name]
            for name in ("view", "pair", "relationship_type", "display_label", "stage")
        }
    else:
        expression = {name: value[name] for name in ("view", "pair", "expression_hint")}
    context.data["relationship_expression"] = [expression]
    if len(canonical(expression).encode()) > runtime.client.max_bytes or context.remaining < 0:
        del context.data["relationship_expression"]
        raise Fault("budget_exceeded")
    pin.update(
        relationship_view=value["view"],
        relationship_version=value["version"],
        checked_at=value["checked_at"],
    )
    return [pin]


async def verify(runtime, check, now):
    if check.get("schema_hash") != SCHEMA_HASH:
        raise Fault("dependency_unavailable")
    if check.get("relationship_view") == "unavailable":
        if check.get("relationship_version") is not None:
            raise Fault("dependency_unavailable")
        return
    scope = check["scope"]
    view = "private" if scope["audience"] == "self_private" else "public"
    if (
        check.get("relationship_view") != view
        or type(check.get("relationship_version")) is not int
        or check["relationship_version"] < 1
        or now - epoch(check["checked_at"]) < -1
        or runtime is None
    ):
        raise Fault("scope_changed")
    # The owner checks current authority and the exact version on every delivery.
    # Cache age alone must not prevent that live check for delayed work.
    await runtime.client.check(check["origin"], scope, check["relationship_version"])
