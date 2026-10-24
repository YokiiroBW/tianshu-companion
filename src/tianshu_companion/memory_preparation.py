"""Closed rev9 preparation reads; no authority survives a later boundary."""

import copy
import time

from .clients import epoch, query
from .contracts import Fault, canonical
from .memory_context import bundle_identity


BODY_LIMIT = 262144


def _identity(request, response):
    if (
        response["request_id"] != request["query"]["request_id"]
        or response["effective_scope"] != request["requested_scope"]
    ):
        raise Fault("invalid_input")


def _fresh(response):
    now = time.time()
    if (
        epoch(response["checked_at"]) > now + 1
        or epoch(response["valid_until"]) <= now
        or epoch(response["checked_at"]) >= epoch(response["valid_until"])
    ):
        raise Fault("scope_changed")


def _bundle(before, response):
    current = response["bundle"]
    if bundle_identity(current) != bundle_identity(before):
        raise Fault("invalid_input")
    if epoch(current["valid_until"]) < epoch(response["valid_until"]):
        raise Fault("invalid_input")
    _fresh(response)


def _invalid(request, response):
    failure = response["failure"]
    component, identity = response["component"], response["check_id"]
    if (
        failure["request_id"] != request["query"]["request_id"]
        or failure["execution_state"] != "not_started"
        or (
            component == "current_selection"
            and (identity is not None or failure["code"] != "scope_changed")
        )
        or (component == "required_check" and identity not in request["required_check_ids"])
    ):
        raise Fault("invalid_input")
    error = Fault(
        failure["code"],
        current_version=failure.get("current_version"),
        cause=dict(
            category="fault",
            peer="memory",
            operation="query",
            http_status=200,
            read_execution="not_started",
            component=component,
        ),
    )
    error.read_execution = "not_started"
    if component == "current_selection":
        error.memory_selection_invalid = True
    raise error


def _catalog(checks, required_ids):
    # Freeze before any I/O; one ID cannot quietly change meaning between packages.
    catalog = {}
    for check in copy.deepcopy(list(checks)):
        identity = check["check_id"]
        if identity in catalog and catalog[identity] != check:
            raise Fault("invalid_input")
        catalog[identity] = check
    required = set(required_ids)
    if not required <= catalog.keys():
        raise Fault("invalid_input")
    return list(catalog.values()), required


def packages(origin, scope, bundle, checks, required_ids):
    catalog, required = _catalog(checks, required_ids)
    result = []

    def request(items):
        return dict(
            schema_version=2,
            query=query(origin),
            requested_scope=scope,
            bundle=bundle,
            checks=items,
            required_check_ids=[c["check_id"] for c in items if c["check_id"] in required],
        )

    current = request([])
    for check in catalog:
        proposed = dict(current, checks=current["checks"] + [check])
        proposed["required_check_ids"] = [
            c["check_id"] for c in proposed["checks"] if c["check_id"] in required
        ]
        if len(proposed["checks"]) > 64 or len(canonical(proposed).encode()) > BODY_LIMIT:
            if not current["checks"]:
                raise Fault("budget_exceeded")
            result.append(current)
            current = request([check])
        else:
            current = proposed
        if len(canonical(current).encode()) > BODY_LIMIT:
            raise Fault("budget_exceeded")
    if len(canonical(current).encode()) > BODY_LIMIT:
        raise Fault("budget_exceeded")
    result.append(current)
    return copy.deepcopy(result)


async def prepare(memory, origin, scope, bundle, checks, required_ids):
    requests = packages(origin, scope, bundle, checks, required_ids)
    required_origins = {canonical(origin)}
    for request in requests:
        required_origins.update(
            canonical(check["request"]["query"]["origin"])
            for check in request["checks"]
            if check["check_id"] in request["required_check_ids"]
        )
    origins = {
        check["check_id"]: canonical(check["request"]["query"]["origin"])
        for request in requests
        for check in request["checks"]
    }
    outcomes, responses, unavailable_origins = {}, [], set()
    for request in requests:
        memory.contracts.check("memory-context-v2#prepare_request", request)
        response = await memory.client.call("/internal/v2/memory/context/prepare", request)
        memory.contracts.check("memory-context-v2#prepare_response", response)
        _identity(request, response)
        if response["state"] == "invalid":
            _invalid(request, response)
        _bundle(request["bundle"], response)
        if [(c["check_id"], c["kind"]) for c in response["checks"]] != [
            (c["check_id"], c["kind"]) for c in request["checks"]
        ]:
            raise Fault("invalid_input")
        for original, check in zip(request["checks"], response["checks"], strict=True):
            if check["check_id"] in request["required_check_ids"] and check["status"] != "valid":
                raise Fault("invalid_input")
            if (
                check["status"] == "unavailable"
                and canonical(original["request"]["query"]["origin"]) in required_origins
            ):
                local_origins = {canonical(origin)} | {
                    canonical(c["request"]["query"]["origin"])
                    for c in request["checks"]
                    if c["check_id"] in request["required_check_ids"]
                }
                if canonical(original["request"]["query"]["origin"]) in local_origins:
                    raise Fault("invalid_input")
                # Other packages cannot reveal their required-origin closure
                # to this producer request. Enforce the immutable global plan.
                raise Fault(
                    "dependency_unavailable",
                    retryable_read=True,
                    cause=dict(category="fault", peer="memory", operation="query", http_status=200),
                )
            outcomes[check["check_id"]] = check["status"]
            if check["status"] == "unavailable":
                unavailable_origins.add(origins[check["check_id"]])
        responses.append(response)
    # A later package cannot extend an earlier proof's lease.
    for response in responses:
        _fresh(response)
    return {
        identity: "unavailable" if origins[identity] in unavailable_origins else status
        for identity, status in outcomes.items()
    }


async def profiles(memory, origin, scope, requests):
    request = dict(
        schema_version=2,
        query=query(origin),
        requested_scope=scope,
        requests=copy.deepcopy(requests),
    )
    memory.contracts.check("memory-context-v2#profile_batch_request", request)
    if len(canonical(request).encode()) > BODY_LIMIT:
        raise Fault("budget_exceeded")
    for item in request["requests"]:
        memory._profile_request(item)
        if (
            item["query"]["origin"] != request["query"]["origin"]
            or item["requester_scope"] != scope
        ):
            raise Fault("invalid_input")
    response = await memory.client.call("/internal/v2/memory/context/profiles", request)
    memory.contracts.check("memory-context-v2#profile_batch_response", response)
    _identity(request, response)
    _fresh(response)
    if len(response["responses"]) != len(requests):
        raise Fault("invalid_input")
    for item, value in zip(request["requests"], response["responses"], strict=True):
        memory._profile_response(item, value)
        if epoch(response["valid_until"]) > epoch(value["valid_until"]):
            raise Fault("invalid_input")
    return response["responses"]
