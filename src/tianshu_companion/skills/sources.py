"""Bounded data-only plugin discovery; installed Python adapters own execution."""

import copy
import hashlib
import re
from urllib.parse import urlsplit

import httpx

from ..clients import utc
from ..contracts import Fault, canonical, strict_json
from ..service_credentials import resolve


def origin(url):
    parts = urlsplit(url)
    if (
        parts.scheme not in {"http", "https"}
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        raise Fault("invalid_input")
    # Parsing the port also validates malformed URLs; configured service paths remain data.
    try:
        parts.port
    except ValueError:
        raise Fault("invalid_input") from None
    return parts.scheme + "://" + parts.netloc


def validate_config(config):
    if config["base_url"]:
        origin(config["base_url"])
    if config["provider"] and not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", config["provider"]):
        raise Fault("invalid_input")
    if len(canonical(config["options"]).encode()) > 8192:
        raise Fault("invalid_input")


def source_projection(source):
    return {
        **{
            key: source[key]
            for key in (
                "source_id",
                "name",
                "manifest_url",
                "enabled",
                "expected_sha256",
                "version",
                "state",
                "last_refreshed_at",
                "content_sha256",
                "error_code",
            )
        },
        "credential_configured": bool(source["credential_ref"]),
    }


def configure_source(current, value):
    origin(value["manifest_url"])
    source_id = value["source_id"]
    if source_id in {"builtin", "local"}:
        raise Fault("invalid_input")
    previous = current["sources"].get(source_id, {})
    return dict(
        copy.deepcopy(previous),
        **value,
        version=previous.get("version", 0) + 1,
        state="configured" if value["enabled"] else "disabled",
        error_code=None,
        last_refreshed_at=previous.get("last_refreshed_at"),
        content_sha256=previous.get("content_sha256"),
    )


async def fetch(registry, source):
    token = await resolve(
        registry.core.image_backend.credentials,
        source["credential_ref"],
        origin(source["manifest_url"]),
        "companion.skills",
        contracts=registry.core.contracts,
    )
    headers = {"Authorization": "Bearer " + token} if token else {}
    async with httpx.AsyncClient(timeout=10, follow_redirects=False) as client:
        async with client.stream("GET", source["manifest_url"], headers=headers) as response:
            if response.status_code != 200:
                raise Fault("dependency_unavailable")
            data = bytearray()
            async for chunk in response.aiter_bytes():
                data.extend(chunk)
                if len(data) > 65536:
                    raise Fault("budget_exceeded")
    return bytes(data)


async def refresh_source(registry, current, source_id):
    if source_id not in current["sources"]:
        raise Fault("not_found")
    source = copy.deepcopy(current["sources"][source_id])
    if not source["enabled"]:
        raise Fault("invalid_input")
    try:
        data = await fetch(registry, source)
        checksum = hashlib.sha256(data).hexdigest()
        if source["expected_sha256"] and source["expected_sha256"] != checksum:
            raise Fault("invalid_input")
        manifest = strict_json(data)
        registry.core.contracts.check("skills#manifest", manifest)
        existing = registry.definitions(current["actor_id"])
        seen = set()
        for definition in manifest["skills"]:
            registry.validate_definition(definition)
            skill_id = definition["id"]
            if skill_id in seen or (
                skill_id in existing and existing[skill_id]["source_id"] != source_id
            ):
                raise Fault("invalid_input")
            seen.add(skill_id)
        # Validate everything before replacing exactly this source's entries. Deleted IDs
        # no longer resolve to a handler; compiled jobs remain owned by their domain.
        old_ids = {item["id"] for item in source.get("definitions", [])}
        for skill_id in seen - old_ids:
            current["skills"].setdefault(skill_id, {})["enabled"] = False
        source.update(
            definitions=manifest["skills"],
            source_version=manifest["source_version"],
            content_sha256=checksum,
            state="ready",
            error_code=None,
        )
    except (Fault, ValueError, UnicodeError, httpx.HTTPError, TimeoutError) as error:
        code = (
            error.code
            if isinstance(error, Fault)
            else "dependency_unavailable"
            if isinstance(error, (httpx.HTTPError, TimeoutError))
            else "invalid_input"
        )
        source.update(
            state="unreachable" if code == "dependency_unavailable" else "invalid", error_code=code
        )
    source["last_refreshed_at"] = utc(registry.core.clock())
    return source
