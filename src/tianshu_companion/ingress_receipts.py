"""Read a caller's exact durable fanout receipt without admitting or replaying input."""

import re

from .contracts import Fault, digest


def read(core, service, request):
    if service not in {"platform", "nonebot"}:
        raise Fault("forbidden")
    if (
        not isinstance(request, dict)
        or set(request) != {"schema_version", "request_id", "idempotency_key", "semantic_digest"}
        or type(request["schema_version"]) is not int
        or request["schema_version"] != 1
        or not isinstance(request["semantic_digest"], str)
        or re.fullmatch(r"[0-9a-f]{64}", request["semantic_digest"]) is None
    ):
        raise Fault("invalid_input")
    for field in ("request_id", "idempotency_key"):
        core.contracts.check("common#id", request[field])
    key = digest([service, "ingest-actors", request["idempotency_key"]])
    receipt = core.store.get("commands", key)
    if receipt and receipt["signature"] != request["semantic_digest"]:
        raise Fault("idempotency_conflict")
    # Absence does not prove nonexecution: a request can still be in flight.
    return {"schema_version": 1, "request_id": request["request_id"], "found": receipt is not None}
