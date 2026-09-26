"""Trusted, opt-in selection port. No transport, secrets, or expanded model grants.

Adapters authenticate as companion and must resolve an authorized published binding.
Gateway authorization still independently checks the pinned version at execution time.
"""

import asyncio
import math
from dataclasses import dataclass
from typing import Protocol

from .contracts import Fault

SELECTION_TIMEOUT = 5.0
MINIMUM_LEASE = 65.0  # Existing generation timeout (60s), with dispatch allowance.


@dataclass(frozen=True)
class SelectionRequest:
    turn_id: str
    actor_id: str
    person_id: str
    audience: str
    conversation_id: str
    caller_service: str = "companion"
    workload: str = "companion.text"

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


def verify_lease(expires_at, now):
    if (
        type(expires_at) not in (int, float)
        or not math.isfinite(expires_at)
        or expires_at - now < MINIMUM_LEASE
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
