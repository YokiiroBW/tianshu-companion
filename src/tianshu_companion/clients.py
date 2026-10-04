"""Fixed service endpoints, bounded calls, and published wire documents."""

import asyncio
import re
import time
import uuid
import ssl
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx

from .contracts import Fault, canonical, digest, strict_json
from . import observability as obs

# The process-wide runtime event port. The host assembles it once, at the same place the
# clients are built; nothing here creates a destination of its own, and an unassembled port
# is the no-op adapter so a client used outside the process (a maintenance script, a test)
# still behaves identically.
LOG_PORT = obs.NullLogAdapter()


def set_log_port(port):
    """Assemble the runtime event port. Called once by the application factory."""
    global LOG_PORT
    LOG_PORT = port if port is not None else obs.NullLogAdapter()
    return LOG_PORT


def _duration_ms(started):
    return round(max(0.0, (time.perf_counter() - started) * 1000), 3)


def _label(value, allowed):
    """A registered interface label, never a configured value from a deployment document."""
    return value if isinstance(value, str) and value in allowed else "unknown"


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
    # Published text/profile/source-sync queries only. Source barriers may persist
    # invalidations, but repeating these queries cannot issue a business command.
    QUERY_PATHS = frozenset(
        {
            "/internal/v1/origins/resolve",
            "/internal/v1/identity/resolve",
            "/internal/v1/qq-admin/check",
            "/internal/v1/memory/select",
            "/internal/v1/relationships/read",
            "/internal/v1/relationships/check",
            "/internal/v1/memory/profiles/select",
            "/internal/v1/memory/source-sync/check",
            "/internal/v1/conversation/reply-status",
            "/internal/v1/memory/context/query",
            "/internal/v1/memory/context/receipt",
            "/internal/v2/bot-delivery/query",
        }
    )
    QUERY_BUDGET_SECONDS = 15
    COMMAND_BUDGET_SECONDS = 15
    MAX_RESPONSE_BYTES = 2_000_000

    def __init__(self, url=None, token=None, *, transport=None, ca_file=None, label=None):
        if url and (
            urlsplit(url).scheme != "https"
            or urlsplit(url).username
            or urlsplit(url).password
            or urlsplit(url).query
            or urlsplit(url).fragment
        ):
            raise ValueError("Internal service URL must be an explicitly configured HTTPS URL")
        self.url, self.token = url, token
        # A registered interface label for the runtime event stream. It is never the
        # configured URL, host or any other deployment value.
        self.label = _label(label, obs.PEER_LABELS)
        if ca_file is not None and (not Path(ca_file).is_absolute() or not Path(ca_file).is_file()):
            raise ValueError("CA file must be an existing absolute deployment path")
        verify = ssl.create_default_context(cafile=ca_file) if ca_file else True
        self.client = httpx.AsyncClient(
            timeout=15, follow_redirects=False, trust_env=False, transport=transport, verify=verify
        )

    async def stream(self, path, body, headers=None):
        if not self.url or not self.token:
            raise Fault("dependency_unavailable")
        request_headers = {
            "Authorization": "Bearer " + self.token,
            "Accept": "text/event-stream",
            "Accept-Encoding": "identity",
            obs.CORRELATION_HEADER: obs.current_correlation_id() or obs.new_correlation_id(),
            **(headers or {}),
        }
        pending, event_data, total = bytearray(), [], 0
        try:
            async with asyncio.timeout(120):
                async with self.client.stream(
                    "POST",
                    self.url.rstrip("/") + path,
                    json=body,
                    headers=request_headers,
                    timeout=httpx.Timeout(30),
                ) as response:
                    if (
                        response.status_code != 200
                        or response.headers.get("content-encoding", "identity") != "identity"
                    ):
                        raise Fault("dependency_unavailable")
                    async for chunk in response.aiter_bytes():
                        total += len(chunk)
                        if total > self.MAX_RESPONSE_BYTES:
                            raise Fault("budget_exceeded", unknown=True)
                        pending.extend(chunk)
                        while b"\n" in pending:
                            line, _, rest = pending.partition(b"\n")
                            pending = bytearray(rest)
                            line = line.rstrip(b"\r")
                            if len(line) > 200000:
                                raise Fault("budget_exceeded", unknown=True)
                            if line.startswith(b"data:"):
                                event_data.append(line[5:].lstrip(b" "))
                            elif not line and event_data:
                                data = b"\n".join(event_data)
                                event_data.clear()
                                if data == b"[DONE]":
                                    return
                                yield strict_json(data)
                        if len(pending) > 200000:
                            raise Fault("budget_exceeded", unknown=True)
            raise Fault("result_unknown", unknown=True)
        except (httpx.HTTPError, TimeoutError, ValueError, UnicodeError):
            raise Fault("dependency_unavailable", unknown=True) from None

    async def call(self, path, body=None, headers=None, *, uncertain_write=False):
        if not self.url or not self.token:
            raise Fault("dependency_unavailable")
        retry_query = (body is not None and path in self.QUERY_PATHS) or (
            body is None
            and re.fullmatch(r"/internal/v1/model-requests/model:[0-9a-f]{32}", path) is not None
        )
        # One correlation ID for the whole logical call, taken unchanged from the operation
        # in flight so the caller's other events share it. It is not propagated to a peer
        # body or credential: only the dedicated header carries it.
        correlation = obs.current_correlation_id() or obs.new_correlation_id()
        request_headers = {
            "Authorization": "Bearer " + self.token,
            obs.CORRELATION_HEADER: correlation,
            "Accept-Encoding": "identity",
            **(headers or {}),
        }
        started, attempts = time.perf_counter(), 0
        sent, not_started = False, False
        LOG_PORT.emit(
            "peer.call.started",
            "started",
            correlation_id=correlation,
            duration_ms=None,
        )
        try:
            request = self.client.build_request(
                "GET" if body is None else "POST",
                self.url.rstrip("/") + path,
                json=body,
                headers=request_headers,
            )
            # One wall-clock budget covers both attempts and the response body.
            # Reuse serialized bytes/IDs/headers, never replace the shared client.
            async with asyncio.timeout(
                self.QUERY_BUDGET_SECONDS if retry_query else self.COMMAND_BUDGET_SECONDS
            ):
                for attempt in range(2 if retry_query else 1):
                    attempts = attempt + 1
                    try:
                        sent = True
                        response = await self.client.send(request, stream=True)
                        break
                    except (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError):
                        # HTTPX/httpcore releases the failed connection. Only retry
                        # before response headers; this does NOT prove nonexecution.
                        if not retry_query or attempt:
                            raise
                try:
                    # The internal JSON port negotiates identity. Reject an encoded body
                    # before HTTPX can inflate one compressed chunk without a byte bound.
                    if response.headers.get("content-encoding", "identity").lower() != "identity":
                        raise Fault("dependency_unavailable")
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(content) + len(chunk) > self.MAX_RESPONSE_BYTES:
                            raise Fault("dependency_unavailable")
                        content.extend(chunk)
                finally:
                    await response.aclose()
            value = strict_json(content)
            if response.status_code >= 300:
                not_started = (
                    isinstance(value, dict) and value.get("execution_state") == "not_started"
                )
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
            self._report("succeeded", correlation, started, attempts)
            return value
        except (httpx.HTTPError, ValueError, TimeoutError, Fault) as error:
            # Third-party exception text is never logged; only a fixed failure class.
            self._report("failed", correlation, started, attempts, error)
            if isinstance(error, Fault):
                if uncertain_write and sent and not not_started:
                    raise Fault(
                        error.code, unknown=True, current_version=error.current_version
                    ) from None
                raise
            raise Fault("dependency_unavailable", unknown=uncertain_write and sent) from None

    def _report(self, outcome, correlation, started, attempts, error=None):
        LOG_PORT.emit(
            "peer.call.finished",
            outcome,
            level="INFO" if outcome == "succeeded" else "WARNING",
            correlation_id=correlation,
            error_code=(
                error.code
                if isinstance(error, Fault)
                else obs.failure_class(error)
                if error is not None
                else None
            ),
            duration_ms=_duration_ms(started),
        )

    async def close(self):
        await self.client.aclose()

    async def binary(self, path, body, *, max_bytes=33554432):
        """Authenticated original POST. No redirects, retry or public URL conversion."""
        if not self.url or not self.token:
            raise Fault("dependency_unavailable")
        headers = {
            "Authorization": "Bearer " + self.token,
            "Accept-Encoding": "identity",
            obs.CORRELATION_HEADER: obs.current_correlation_id() or obs.new_correlation_id(),
        }
        try:
            async with asyncio.timeout(60):
                async with self.client.stream(
                    "POST", self.url.rstrip("/") + path, json=body, headers=headers
                ) as response:
                    if response.headers.get("content-encoding", "identity") != "identity":
                        raise Fault("dependency_unavailable")
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(data) + len(chunk) > max_bytes:
                            raise Fault("budget_exceeded")
                        data.extend(chunk)
                    if response.status_code >= 300:
                        error = strict_json(data)
                        raise Fault(error.get("code", "dependency_unavailable"))
                    return bytes(data), dict(response.headers)
        except (httpx.HTTPError, TimeoutError, ValueError):
            raise Fault("dependency_unavailable") from None


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

    async def resolve_identity(self, origin, account):
        request = dict(query=query(origin), account=account)
        response = await self.client.call("/internal/v1/identity/resolve", request)
        self.contracts.check("identity-memory#resolve_response", response)
        if response["request_id"] != request["query"]["request_id"]:
            raise Fault("dependency_unavailable")
        return response.get("person_id") if response["state"] != "unregistered" else None

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

    async def select(
        self, origin, scope, text, budget, known_version=None, *, known=None, time_range=None
    ):
        request = dict(
            query=query(origin),
            requested_scope=scope,
            query_text=text,
            selection=["identity", "style", "relationship", "current_items", "evidence"],
            known_scope_version=known_version,
            budget=budget,
            include_associated=scope["audience"] == "self_private",
            time_range=time_range,
            limit=32,
            known_association_version=known.get("association_version") if known else None,
            known_scope_checks=known.get("scope_checks") if known else None,
        )

        self.contracts.check("memory-context#query_request", request)
        response = await self.client.call("/internal/v1/memory/context/query", request)
        self.contracts.check("memory-context#query_response", response)
        if (
            response["request_id"] != request["query"]["request_id"]
            or response["effective_scope"] != scope
        ):
            raise Fault("forbidden")
        if known_version is not None and response["scope_version"] != known_version:
            raise Fault("scope_changed")
        if known and (
            response["association_version"] != known["association_version"]
            or response["scope_checks"] != known["scope_checks"]
        ):
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

    async def propose(self, request):
        self.contracts.check("memory-context#proposal_request", request)
        try:
            response = await self.client.call(
                "/internal/v1/memory/context/propose", request, uncertain_write=True
            )
        except Fault as error:
            if not error.unknown:
                raise
            response = await self.receipt(
                request["command"]["origin"],
                request["scope"],
                request["command"]["idempotency_key"],
            )
            if response is None:
                raise Fault("result_unknown", unknown=True) from None
        self.contracts.check("memory-context#receipt", response)
        if (
            response["operation_id"] != request["command"]["idempotency_key"]
            or response["item_id"] != request["item_id"]
        ):
            raise Fault("invalid_input", unknown=True)
        return response

    async def receipt(self, origin, scope, operation_id):
        request = dict(query=query(origin), scope=scope, operation_id=operation_id)
        self.contracts.check("memory-context#receipt_request", request)
        response = await self.client.call("/internal/v1/memory/context/receipt", request)
        self.contracts.check("memory-context#receipt_response", response)
        if response["request_id"] != request["query"]["request_id"] or (
            response["receipt"] and response["receipt"]["operation_id"] != operation_id
        ):
            raise Fault("invalid_input")
        return response["receipt"]

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
        value = await self.client.call(
            "/internal/v1/memory/turn-commits", event, uncertain_write=True
        )
        try:
            self.contracts.check("identity-memory#consume_receipt", value)
        except Fault as error:
            raise Fault(error.code, unknown=True) from None
        if (
            value["event_id"] != event["event_id"]
            or value["turn_id"] != event["aggregate_id"]
            or value["input_revision"] != event["input_revision"]
        ):
            raise Fault("invalid_input", unknown=True)
        return value


class Gateway:
    def __init__(self, contracts, client):
        self.contracts, self.client = contracts, client
        # Live transport handles only; execution evidence remains the gateway's receipt.
        self.active_requests = {}

    async def cancel(self, turn_id):
        request_id = self.active_requests.get(turn_id)
        if request_id:
            return await self.client.call(
                "/internal/v1/model-requests/" + request_id + "/cancel", {}
            )

    @property
    def available(self):
        return bool(self.client.url and self.client.token)

    async def complete(self, turn, messages, *, tools=None, on_delta=None):
        from .model_native import Completion

        request_id = uid("model")
        headers = {
            "X-Request-ID": request_id,
            "X-Tianshu-Config-Version": str(turn["config_version"]),
            "X-Tianshu-Workload": "companion.text",
            "X-Tianshu-Turn-ID": turn["id"],
        }
        scope = turn.get("scope") or {}
        role = turn.get("role") or {}
        actor = scope.get("actor_id") or role.get("actor_id") or turn.get("actor_id")
        conversation = turn.get("conversation_id") or scope.get("conversation_id")
        if actor and conversation:
            # Stable routing metadata across turns, independent of execution authorization.
            headers["X-Tianshu-Provider-Session"] = digest(
                ["provider-session", actor, conversation]
            )
        # Model is intentionally absent: only the gateway's published workload binding selects it.
        body = dict(messages=messages, stream=True)
        if tools:
            body.update(tools=tools, tool_choice="auto")
        self.contracts.check("model#native_request", body)
        result = Completion()
        self.active_requests[turn["id"]] = request_id
        try:
            async for chunk in self.client.stream("/v1/chat/completions", body, headers):
                text = result.consume(chunk)
                if text and on_delta:
                    await on_delta(text)
        except asyncio.CancelledError:
            try:
                await asyncio.shield(
                    self.client.call("/internal/v1/model-requests/" + request_id + "/cancel", {})
                )
            except (Fault, OSError):
                pass
            raise
        finally:
            if self.active_requests.get(turn["id"]) == request_id:
                self.active_requests.pop(turn["id"], None)
        message = result.message()
        receipt = await self.client.call("/internal/v1/model-requests/" + request_id)
        self.contracts.check("model#route_receipt", receipt)
        if (
            receipt["request_id"] != request_id
            or receipt["config_version"] != turn["config_version"]
            or receipt["outcome"] != "succeeded"
            or receipt["caller_service"] != "companion"
        ):
            raise Fault("dependency_unavailable")
        return message, receipt

    async def generate(self, turn, messages):
        message, receipt = await self.complete(turn, messages)
        if message.get("tool_calls"):
            raise Fault("invalid_input")
        text = message.get("content") or ""
        return ([text] if text.strip() else []), receipt

    async def capabilities(self, turn):
        headers = {
            "X-Request-ID": uid("model"),
            "X-Tianshu-Config-Version": str(turn["config_version"]),
            "X-Tianshu-Workload": "companion.text",
            "X-Tianshu-Turn-ID": turn["id"],
        }
        value = await self.client.call("/internal/v1/model-capabilities", headers=headers)
        self.contracts.check("model#capabilities_response", value)
        return value


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


class PlatformBotSender(Sender):
    """Platform bot queue: read the receipt without repeating a possibly sent command."""

    def expression_available(self, channel):
        return self.available

    async def send_expression(self, request):
        self.contracts.check("bot-delivery#send_request", request)
        value = await self.client.call(
            "/internal/v2/bot-delivery/send", request, uncertain_write=True
        )
        self.contracts.check("bot-delivery#send_receipt", value)
        if value["expression_id"] != request["expression_id"]:
            raise Fault("invalid_input", unknown=True)
        return value

    async def query_expression(self, expression_id):
        request = dict(
            schema_version=2, request_id=uid("delivery-query"), expression_id=expression_id
        )
        value = await self.client.call("/internal/v2/bot-delivery/query", request)
        self.contracts.check("bot-delivery#lookup_response", value)
        if value["expression_id"] != expression_id or value["request_id"] != request["request_id"]:
            raise Fault("invalid_input")
        return value["receipt"]

    async def expression_context(self, origin, scope, channel):
        request = dict(
            schema_version=2,
            request_id=uid("delivery-context"),
            origin=origin,
            scope=scope,
            channel=channel,
        )
        self.contracts.check("bot-delivery#context_request", request)
        value = await self.client.call("/internal/v2/bot-delivery/context", request)
        self.contracts.check("bot-delivery#context_response", value)
        if (
            value["request_id"] != request["request_id"]
            or value["scope"] != scope
            or value["channel"] != channel
            or epoch(value["expires_at"]) <= time.time()
        ):
            raise Fault("scope_changed")
        return value

    async def finalize_expression(self, expression_id, origin):
        request = dict(
            schema_version=2,
            request_id="finalize:" + digest(expression_id),
            expression_id=expression_id,
            origin=origin,
        )
        self.contracts.check("bot-delivery#finalize_request", request)
        value = await self.client.call(
            "/internal/v2/bot-delivery/finalize", request, uncertain_write=True
        )
        self.contracts.check("bot-delivery#lookup_response", value)
        if value["expression_id"] != expression_id or value["request_id"] != request["request_id"]:
            raise Fault("invalid_input", unknown=True)
        return value["receipt"]

    async def cancel_expression(self, expression_id):
        request = dict(
            schema_version=2,
            request_id="cancel:" + digest(expression_id),
            expression_id=expression_id,
        )
        value = await self.client.call(
            "/internal/v2/bot-delivery/cancel", request, uncertain_write=True
        )
        self.contracts.check("bot-delivery#lookup_response", value)
        if value["expression_id"] != expression_id or value["request_id"] != request["request_id"]:
            raise Fault("invalid_input", unknown=True)
        return value["receipt"]

    async def reconcile(self, request):
        self.contracts.check("conversation#send_request", request)
        value = await self.client.call("/internal/v1/conversation/reply-status", request)
        if not isinstance(value, dict) or set(value) != {"receipt"}:
            raise Fault("invalid_input")
        receipt = value["receipt"]
        if receipt is None:
            return None
        self.contracts.check("conversation#send_receipt", receipt)
        if (
            receipt["reply_id"] != request["reply_id"]
            or receipt["request_id"] != request["command"]["request_id"]
            or receipt["segment_sequence"] != request["segment_sequence"]
        ):
            raise Fault("invalid_input")
        return receipt


class BotSenderRouter:
    """Only deployment-selected platform-owned bindings use the bot polling queue."""

    def __init__(self, legacy, platform, selected_bindings, bindings):
        self.legacy, self.platform = legacy, platform
        if not isinstance(selected_bindings, (list, tuple)) or len(set(selected_bindings)) != len(
            selected_bindings
        ):
            raise ValueError("bot_platform_bindings must be a unique list")
        self.selected = frozenset(selected_bindings)
        for binding_id in self.selected:
            binding = bindings.get(binding_id)
            if (
                not binding
                or binding.get("service") != "platform"
                or binding.get("namespace") not in {"qq", "tg"}
            ):
                raise ValueError("A platform bot binding must be a registered qq/tg binding")
        if self.selected and not platform.available:
            raise ValueError("A selected platform bot binding requires platform_sender service")
        self.dynamic = set()

    def select_dynamic(self, binding_id):
        if not self.platform.available:
            raise ValueError("A dynamic platform bot binding requires platform_sender service")
        self.dynamic.add(binding_id)

    @property
    def available(self):
        return self.legacy.available or (
            bool(self.selected or self.dynamic) and self.platform.available
        )

    def _sender(self, request):
        binding_id = request["destination"]["binding_id"]
        return (
            self.platform
            if binding_id in self.selected or binding_id in self.dynamic
            else self.legacy
        )

    def expression_available(self, channel):
        return (
            channel["binding_id"] in self.selected or channel["binding_id"] in self.dynamic
        ) and self.platform.available

    async def send_expression(self, request):
        if not self.expression_available(request["channel"]):
            raise Fault("dependency_unavailable")
        return await self.platform.send_expression(request)

    async def query_expression(self, expression_id):
        return await self.platform.query_expression(expression_id)

    async def expression_context(self, origin, scope, channel):
        return await self.platform.expression_context(origin, scope, channel)

    async def finalize_expression(self, expression_id, origin):
        return await self.platform.finalize_expression(expression_id, origin)

    async def cancel_expression(self, expression_id):
        return await self.platform.cancel_expression(expression_id)

    async def send(self, request):
        return await self._sender(request).send(request)

    async def reconcile(self, request):
        return await self._sender(request).reconcile(request)
