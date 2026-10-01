"""Consumer of TS-114's existing Memory service ports."""

import asyncio
from datetime import datetime

from ..clients import query
from ..contracts import Fault, canonical


def pair(scope):
    return {name: scope[name] for name in ("actor_id", "person_id")}


class RelationshipClient:
    def __init__(self, service, contract, *, max_bytes=2048, timeout_seconds=5):
        self.service, self.contract = service, contract
        self.max_bytes, self.timeout_seconds = max_bytes, timeout_seconds

    async def _call(self, operation, origin, field, *, write=False, **fields):
        request = dict(query(origin), **fields)
        try:
            async with asyncio.timeout(self.timeout_seconds):
                response = await self.service.call(
                    "/internal/v1/relationships/" + operation,
                    request,
                    uncertain_write=write,
                )
        except TimeoutError:
            raise Fault("timeout", unknown=write) from None
        if (
            type(response) is not dict
            or set(response) != {"schema_version", "request_id", field}
            or type(response["schema_version"]) is not int
            or response["schema_version"] != 1
            or response["request_id"] != request["request_id"]
            or len(canonical(response).encode()) > 16384
        ):
            raise Fault("dependency_unavailable", unknown=write)
        return response[field]

    async def read(self, origin, scope, clock):
        target = pair(scope)
        self.contract.check("Pair", target)
        value = await self._call("read", origin, "projection", pair=target)
        view = "private" if scope["audience"] == "self_private" else "public"
        self.contract.check("PrivateProjection" if view == "private" else "PublicProjection", value)
        if value["pair"] != target or len(canonical(value).encode()) > self.max_bytes:
            raise Fault("dependency_unavailable")
        checked = datetime.fromisoformat(value["checked_at"]).timestamp()
        if not -1 <= clock() - checked <= 120:
            raise Fault("scope_changed")
        return value

    async def check(self, origin, scope, expected_version):
        value = await self._call(
            "check", origin, "check", pair=pair(scope), expected_version=expected_version
        )
        if (
            type(value) is not dict
            or set(value) != {"version", "current"}
            or type(value["version"]) is not int
            or value["version"] != expected_version
            or value["current"] is not True
        ):
            raise Fault("dependency_unavailable")

    async def settle(self, origin, candidate):
        self.contract.check("AffinityEventCandidate", candidate)
        value = await self._call("settle", origin, "settlement", write=True, candidate=candidate)
        try:
            self.contract.check("SettlementResult", value)
            if value["event_id"] != candidate["event_id"] or value["pair"] != candidate["pair"]:
                raise Fault("dependency_unavailable")
        except Fault:
            raise Fault("dependency_unavailable", unknown=True) from None
        return value
