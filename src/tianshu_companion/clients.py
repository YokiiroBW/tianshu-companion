"""Fixed service endpoints, bounded calls, and published wire documents."""

import time
import uuid
import ssl
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx

from .contracts import Fault, canonical, digest, strict_json


def uid(prefix):
    return prefix + ":" + uuid.uuid4().hex


def utc(seconds=None):
    return (
        datetime.fromtimestamp(time.time() if seconds is None else seconds, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def epoch(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def query(origin):
    return dict(schema_version=1, request_id=uid("req"), origin=origin)


def command(origin, key, now, seconds=30):
    return dict(**query(origin), idempotency_key=key, deadline_at=utc(now + seconds))


class JsonService:
    def __init__(self, url=None, token=None, *, transport=None, ca_file=None):
        if url and (
            urlsplit(url).scheme != "https"
            or urlsplit(url).username
            or urlsplit(url).password
            or urlsplit(url).query
            or urlsplit(url).fragment
        ):
            raise ValueError("Internal service URL must be an explicitly configured HTTPS URL")
        self.url, self.token = url, token
        if ca_file is not None and (not Path(ca_file).is_absolute() or not Path(ca_file).is_file()):
            raise ValueError("CA file must be an existing absolute deployment path")
        verify = ssl.create_default_context(cafile=ca_file) if ca_file else True
        self.client = httpx.AsyncClient(
            timeout=15, follow_redirects=False, trust_env=False, transport=transport, verify=verify
        )

    async def call(self, path, body=None, headers=None):
        if not self.url or not self.token:
            raise Fault("dependency_unavailable")
        try:
            response = await self.client.request(
                "GET" if body is None else "POST",
                self.url.rstrip("/") + path,
                json=body,
                headers={"Authorization": "Bearer " + self.token, **(headers or {})},
            )
            if len(response.content) > 2_000_000:
                raise Fault("dependency_unavailable")
            value = strict_json(response.content)
            if response.status_code >= 300:
                code = value.get("code") if isinstance(value, dict) else None
                if code in {
                    "scope_changed",
                    "forbidden",
                    "unauthorized",
                    "version_conflict",
                    "budget_exceeded",
                    "queue_full",
                    "timeout",
                    "result_unknown",
                }:
                    raise Fault(code, unknown=value.get("execution_state") == "unknown")
                raise Fault("dependency_unavailable")
            return value
        except (httpx.HTTPError, ValueError):
            raise Fault("dependency_unavailable") from None

    async def close(self):
        await self.client.aclose()


class Origins:
    def __init__(self, contracts, issuers):
        self.contracts, self.issuers = contracts, issuers

    async def input_access(self, service, ingest):
        if service not in {"platform", "nonebot"} or service not in self.issuers:
            raise Fault("unauthorized")
        issuer, client = self.issuers[service]
        if issuer != "platform":
            raise Fault("forbidden")
        request = dict(schema_version=1, request_id=uid("req"), operation="input", ingest=ingest)
        value = await client.call("/internal/v1/source-access/read", request)
        self.contracts.check("sources#input_authority", value)
        if (
            value["request_id"] != request["request_id"]
            or value["request_digest"] != digest(request)
            or value["ingest_digest"] != digest(ingest)
            or value["input_digest"] != digest(ingest["input"])
            or value["verified_account"] != ingest["input"]["author"]
            or value["verified_channel"] != ingest["input"]["message_key"]["channel"]
            or value["origin_ref"] != ingest["command"]["origin"]["assertion_ref"]
        ):
            raise Fault("forbidden")
        return value

    async def resolve(self, service, envelope, now):
        if service not in self.issuers:
            raise Fault("unauthorized")
        issuer, client = self.issuers[service]
        request = dict(
            schema_version=1,
            request_id=envelope["request_id"],
            assertion_ref=envelope["origin"]["assertion_ref"],
        )
        value = await client.call("/internal/v1/origins/resolve", request)
        self.contracts.check("common#origin_resolve_response", value)
        context = value["context"]
        if (
            value["request_id"] != request["request_id"]
            or context["issuer"] != issuer
            or context["authenticated_service"] != service
            or context["audience_service"] != "companion"
            or context["assertion_ref"] != request["assertion_ref"]
            or epoch(context["expires_at"]) <= now
            or context["revoked"]
        ):
            raise Fault("forbidden")
        return context


class Memory:
    def __init__(self, contracts, client):
        self.contracts, self.client = contracts, client

    async def identity(self, origin, account, now):
        request = dict(query=query(origin), account=account)
        response = await self.client.call("/internal/v1/identity/resolve", request)
        self.contracts.check("identity-memory#resolve_response", response)
        if response["request_id"] != request["query"]["request_id"]:
            raise Fault("dependency_unavailable")
        if response["state"] == "unregistered":
            request = dict(
                command=command(origin, "register:" + digest(account), now), account=account
            )
            response = await self.client.call("/internal/v1/identity/register", request)
            self.contracts.check("identity-memory#identity_response", response)
            if response["request_id"] != request["command"]["request_id"]:
                raise Fault("dependency_unavailable")
        return response["person_id"], response["binding_version"]

    async def check_sources(self, turn, sources):
        request = dict(
            schema_version=1,
            request_id=uid("req"),
            turn_id=turn["id"],
            input_revision=turn["bundle"]["collection_revision"],
            scope=turn["scope"],
            sources=sources,
        )
        self.contracts.check("shared#check_request", request)
        response = await self.client.call("/internal/v1/memory/source-sync/check", request)
        self.contracts.check("shared#check_response", response)
        if (
            response["request_id"] != request["request_id"]
            or response["request_digest"] != digest(request)
            or response["scope"] != request["scope"]
            or response["version_domain"] != "text-dialogue/v1"
        ):
            raise Fault("dependency_unavailable")
        return response["scope_version"]

    async def select(self, origin, scope, text, budget, known_version=None):
        request = dict(
            query=query(origin),
            requested_scope=scope,
            query_text=text,
            selection=["evidence"],
            known_scope_version=known_version,
            budget=budget,
        )
        self.contracts.check("identity-memory#select_request", request)
        response = await self.client.call("/internal/v1/memory/select", request)
        self.contracts.check("identity-memory#select_response", response)
        if (
            response["request_id"] != request["query"]["request_id"]
            or response["effective_scope"] != scope
        ):
            raise Fault("forbidden")
        if known_version is not None and response["scope_version"] != known_version:
            raise Fault("scope_changed")
        if epoch(response["valid_until"]) <= time.time():
            raise Fault("scope_changed")
        if any(response["budget_used"][k] > budget[k] for k in budget):
            raise Fault("budget_exceeded")
        units = response["selected_units"]
        ids = {u["record_id"] for u in units}
        groups = response["dependency_groups"]
        if len(ids) != len(units) or len({g["semantic_group_id"] for g in groups}) != len(groups):
            raise Fault("invalid_input")
        grouped = set()
        for group in groups:
            members = {
                u["record_id"]
                for u in units
                if u["semantic_group_id"] == group["semantic_group_id"]
            }
            if members != set(group["record_ids"]) or len(members) != len(group["record_ids"]):
                raise Fault("invalid_input")
            grouped.update(members)
        if grouped != ids or (units and len(canonical(units).encode()) > budget["bytes"]):
            raise Fault("budget_exceeded")
        for unit in units:
            if unit["subject_person_id"] != scope["person_id"]:
                raise Fault("forbidden")
            if scope["audience"] == "group" and (
                unit["visibility"] != "shared_projection"
                or any(s["kind"] != "shareable_projection" for s in unit["sources"])
            ):
                raise Fault("forbidden")
        return response

    async def profiles(self, origin, scope, target, text, selection, budget, known_version=None):
        request = dict(
            query=query(origin),
            requester_scope=scope,
            target=target,
            query_text=text,
            selection=selection,
            known_scope_version=known_version,
            budget=budget,
        )
        self.contracts.check("profiles#select_request", request)
        if target["kind"] == "group" and (
            scope["audience"] != "group" or target["conversation_id"] != scope["conversation_id"]
        ):
            raise Fault("forbidden")
        response = await self.client.call("/internal/v1/memory/profiles/select", request)
        self.contracts.check("profiles#select_response", response)
        if (
            response["request_id"] != request["query"]["request_id"]
            or response["requester_scope"] != scope
            or response["target"] != target
        ):
            raise Fault("forbidden")
        if (known_version is not None and response["scope_version"] != known_version) or epoch(
            response["valid_until"]
        ) <= time.time():
            raise Fault("scope_changed")
        units, groups = response["selected_units"], response["dependency_groups"]
        ids = {u["record_id"] for u in units}
        if len(ids) != len(units) or len({g["semantic_group_id"] for g in groups}) != len(groups):
            raise Fault("invalid_input")
        grouped = set()
        for group in groups:
            members = {
                u["record_id"]
                for u in units
                if u["semantic_group_id"] == group["semantic_group_id"]
            }
            if (
                not members
                or members != set(group["record_ids"])
                or len(members) != len(group["record_ids"])
            ):
                raise Fault("invalid_input")
            grouped.update(members)
        if grouped != ids:
            raise Fault("invalid_input")
        for unit in units:
            if (
                unit["subject"] != target
                or unit["category"] not in selection
                or (unit["sharing"] == "group_only" and scope["audience"] != "group")
            ):
                raise Fault("forbidden")
        # Same conservative accounting as the producer, including dependency metadata.
        size = (
            len(canonical(dict(selected_units=units, dependency_groups=groups)).encode())
            if units
            else 0
        )
        if any(
            response["budget_used"][k] > budget[k] or response["budget_used"][k] < size
            for k in budget
        ):
            raise Fault("budget_exceeded")
        return response

    async def commit(self, event):
        value = await self.client.call("/internal/v1/memory/turn-commits", event)
        self.contracts.check("identity-memory#consume_receipt", value)
        if (
            value["event_id"] != event["event_id"]
            or value["turn_id"] != event["aggregate_id"]
            or value["input_revision"] != event["input_revision"]
        ):
            raise Fault("invalid_input")
        return value


class Gateway:
    def __init__(self, contracts, client):
        self.contracts, self.client = contracts, client

    async def generate(self, turn, messages):
        request_id = uid("model")
        headers = {
            "X-Request-ID": request_id,
            "X-Tianshu-Config-Version": str(turn["config_version"]),
            "X-Tianshu-Workload": "companion.text",
            "X-Tianshu-Turn-ID": turn["id"],
        }
        # Model is intentionally absent: only the gateway's published workload binding selects it.
        body = dict(messages=messages, stream=False)
        self.contracts.check("model#native_request", body)
        response = await self.client.call("/v1/chat/completions", body, headers)
        self.contracts.check("model#native_response", response)
        choices = response["choices"]
        if len(choices) != 1 or choices[0].get("finish_reason") != "stop":
            raise Fault("dependency_unavailable")
        message = choices[0].get("message", {})
        if message.get("tool_calls") or message.get("function_call"):
            raise Fault("invalid_input")
        text = message.get("content")
        if not isinstance(text, str) or len(text.encode()) > 32768:
            raise Fault("invalid_input")
        receipt = await self.client.call("/internal/v1/model-requests/" + request_id)
        self.contracts.check("model#route_receipt", receipt)
        if (
            receipt["request_id"] != request_id
            or receipt["config_version"] != turn["config_version"]
            or receipt["outcome"] != "succeeded"
            or receipt["caller_service"] != "companion"
        ):
            raise Fault("dependency_unavailable")
        return ([text] if text.strip() else []), receipt


class Sender:
    def __init__(self, contracts, client):
        self.contracts, self.client = contracts, client

    @property
    def available(self):
        return bool(self.client.url and self.client.token)

    async def send(self, request):
        self.contracts.check("conversation#send_request", request)
        result = await self.client.call("/internal/v1/conversation/send", request)
        self.contracts.check("conversation#send_receipt", result)
        return result

    async def reconcile(self, request):
        # No receipt lookup route is frozen in v1. Deployment can supply an audited
        # channel-specific lookup; lack of one must never trigger another send.
        return None
