"""Resolve registered secret references without persisting or exposing secret values."""

from .clients import uid
from .contracts import Fault


async def resolve(client, reference, audience, purpose, *, contracts=None):
    if reference is None:
        return None
    if client is None:
        raise Fault("dependency_unavailable")
    request = dict(
        schema_version=1,
        request_id=uid("credential"),
        credential_ref=reference,
        audience=audience,
        purpose=purpose,
    )
    document = "skills/v1/dependencies/platform-credential.json"
    if contracts is not None:
        contracts.check_document(document, "request", request)
    response = await client.call("/internal/v1/service-credentials/resolve", request)
    if contracts is not None:
        contracts.check_document(document, "response", response)
    if (
        not isinstance(response, dict)
        or set(response) != {"schema_version", "request_id", "credential_ref", "token"}
        or response["schema_version"] != 1
        or response["request_id"] != request["request_id"]
        or response["credential_ref"] != reference
        or not isinstance(response["token"], str)
        or not 1 <= len(response["token"]) <= 8192
    ):
        raise Fault("dependency_unavailable")
    return response["token"]
