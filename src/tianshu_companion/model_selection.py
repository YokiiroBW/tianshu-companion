"""Trusted, opt-in selection port. No transport, secrets, or expanded model grants.

Adapters authenticate as companion and must resolve an authorized published binding.
Gateway authorization still independently checks the pinned version at execution time.
"""

import asyncio
import math
from dataclasses import dataclass
from dataclasses import asdict
from typing import Protocol

from .contracts import Fault

SELECTION_TIMEOUT = 5.0
MINIMUM_LEASE = 65.0  # Existing generation timeout (60s), with dispatch allowance.


@dataclass(frozen=True)
class SelectionRequest:
    turn_id: str
    actor_id: str
    person_id: str | None
    audience: str
    conversation_id: str
    caller_service: str = "companion"
    workload: str = "companion.text"
    function_id: str = "chat"

    @classmethod
    def from_turn(cls, turn):
        return cls(turn_id=turn["id"], **turn["scope"])


@dataclass(frozen=True)
class ModelSelection:
    config_version: int
    expires_at: float
    revoked: bool = False
    caller_service: str = "companion"
    workload: str = "companion.text"


class DefaultModelSelector(Protocol):
    async def select(self, request: SelectionRequest) -> ModelSelection: ...


class HttpDefaultModelSelector:
    """Dedicated companion identity; the platform proves the exact publication."""

    def __init__(self, client, *, caller_service="companion"):
        if not client.url or not client.token:
            raise ValueError("provider selector service is not configured")
        if caller_service not in {"companion", "companion_background"}:
            raise ValueError("Unsupported model selector caller")
        self.client = client
        self.caller_service = caller_service

    async def select(self, request: SelectionRequest) -> ModelSelection:
        if request.caller_service != self.caller_service:
            raise Fault("forbidden")
        payload = asdict(request)
        if request.function_id == "chat":
            payload.pop("function_id")  # Preserve the existing chat request and grant digest.
        value = await self.client.call("/internal/v1/provider-self-service/select", payload)
        if not isinstance(value, dict) or set(value) != {
            "config_version",
            "expires_at",
            "revoked",
            "caller_service",
            "workload",
        }:
            raise Fault("dependency_unavailable")
        return ModelSelection(**value)


def verify_lease(expires_at, now, *, minimum=MINIMUM_LEASE):
    if (
        type(expires_at) not in (int, float)
        or not math.isfinite(expires_at)
        or expires_at - now <= 0
        or expires_at - now < minimum
    ):
        raise Fault("dependency_unavailable")


async def resolve_selection(selector: DefaultModelSelector, request, clock):
    try:
        async with asyncio.timeout(SELECTION_TIMEOUT):
            result = await selector.select(request)
    except asyncio.CancelledError:
        raise
    except Exception:
        # A transport's exception can contain credentials or provider response text.
        raise Fault("dependency_unavailable") from None
    if (
        type(result) is not ModelSelection
        or type(result.config_version) is not int
        or result.config_version < 1
        or result.revoked is not False
        or result.caller_service != request.caller_service
        or result.workload != request.workload
    ):
        raise Fault("dependency_unavailable")
    verify_lease(result.expires_at, clock())
    return result


async def pin_generation(
    task, request, selector, clock, *, static_version=None, fallback_version=None, independent=False
):
    """Pin one generation to its actual caller; independent callers never borrow a grant."""
    if selector is not None:
        retained = task.get("model_selection") or {}
        renew = (
            retained.get("turn_id") != request.turn_id
            or retained.get("caller_service", "companion") != request.caller_service
        )
        if not renew:
            try:
                verify_lease(retained.get("expires_at"), clock())
            except Fault:
                renew = True
        if renew:
            # Only the original authorization owner may renew this exact turn.
            # Its selector preserves the pinned provider/revision or refuses;
            # the caller never manufactures a replacement turn to get a grant.
            chosen = await resolve_selection(selector, request, clock)
            task["config_version"] = chosen.config_version
            task["model_selection"] = {
                "expires_at": chosen.expires_at,
                "turn_id": request.turn_id,
                "caller_service": request.caller_service,
            }
        verify_lease(task["model_selection"]["expires_at"], clock())
    elif independent:
        if static_version is None:
            raise Fault("dependency_unavailable")
        task["config_version"] = static_version
        task.pop("model_selection", None)
    elif task.get("config_version") is None:
        task["config_version"] = static_version or fallback_version
    if task.get("config_version") is None:
        raise Fault("dependency_unavailable")
