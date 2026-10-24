"""Fixed service endpoints, bounded calls, and published wire documents."""

import asyncio
import re
import time
import uuid
import ssl
import math
from contextlib import AsyncExitStack, aclosing
from email.utils import parsedate_to_datetime
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx

from .contracts import Fault, canonical, digest, strict_json
from .memory_context import group_context_owners
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


def _retry_after_seconds(value):
    if not isinstance(value, str):
        return None
    value = value.strip()
    try:
        if re.fullmatch(r"[0-9]+", value):
            delay = float(value)
        else:
            moment = parsedate_to_datetime(value)
            if moment.tzinfo is None:
                return None
            delay = max(0, moment.timestamp() - time.time())
        return delay if math.isfinite(delay) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _retry_hint(error, response):
    # Internal metadata only; never reinterpret a pressure hint as authority or
    # evidence of whether a command executed.
    error.retry_after_seconds = _retry_after_seconds(response.headers.get("retry-after"))
    return error


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
            "/internal/v1/books/reading",
            "/internal/v1/memory/select",
            "/internal/v1/relationships/read",
            "/internal/v1/relationships/check",
            "/internal/v1/memory/profiles/select",
            "/internal/v1/memory/source-sync/check",
            "/internal/v1/conversation/reply-status",
            "/internal/v1/memory/context/query",
            "/internal/v1/memory/context/history",
            "/internal/v1/memory/context/input-history",
            "/internal/v1/memory/context/receipt",
            "/internal/v2/memory/context/query",
            "/internal/v2/memory/context/verify",
            "/internal/v2/memory/context/prepare",
            "/internal/v2/memory/context/profiles",
            "/internal/v2/bot-delivery/query",
        }
    )
    QUERY_BUDGET_SECONDS = 15
    COMMAND_BUDGET_SECONDS = 15
    STREAM_BUDGET_SECONDS = 120
    MAX_RESPONSE_BYTES = 2_000_000

    @staticmethod
    def _optional_transport_failure(error):
        # This is narrower than retryable_read. Trust/protocol failures and
        # unclassified ConnectError cannot become optional-history omissions.
        chain, current = [], error
        while current is not None and all(current is not item for item in chain):
            chain.append(current)
            current = current.__cause__ or current.__context__
        if any(isinstance(item, ssl.SSLError) for item in chain):
            return False
        if isinstance(error, (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout)):
            return True
        return isinstance(error, (httpx.ReadError, httpx.WriteError)) and any(
            isinstance(item, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError))
            for item in chain
        )

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
        self.query_slots = asyncio.Semaphore(4)
        self.query_not_before = 0.0
        # A registered interface label for the runtime event stream. It is never the
        # configured URL, host or any other deployment value.
        self.label = _label(label, obs.PEER_LABELS)
        if ca_file is not None and (not Path(ca_file).is_absolute() or not Path(ca_file).is_file()):
            raise ValueError("CA file must be an existing absolute deployment path")
        verify = ssl.create_default_context(cafile=ca_file) if ca_file else True
        self.client = httpx.AsyncClient(
            timeout=15, follow_redirects=False, trust_env=False, transport=transport, verify=verify
        )

    def _cause(self, error, operation, status=None):
        # Never copy exception text, URLs, headers or peer response fields.
        if isinstance(error, Fault) and error.cause is not None:
            return error.cause
        cause = dict(category=obs.failure_class(error), peer=self.label, operation=operation)
        exception_type = obs.failure_type(error)
        if exception_type is not None:
            cause["exception_type"] = exception_type
        if status is not None:
            cause["http_status"] = status
        return cause

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
        remaining = self.STREAM_BUDGET_SECONDS
        loop = asyncio.get_running_loop()
        response = None

        async def receive(operation, *args):
            nonlocal remaining
            if remaining <= 0:
                raise TimeoutError
            started = loop.time()
            try:
                async with asyncio.timeout(remaining):
                    return await operation(*args)
            finally:
                remaining -= loop.time() - started

        try:
            async with AsyncExitStack() as stack:
                response = await receive(
                    stack.enter_async_context,
                    self.client.stream(
                        "POST",
                        self.url.rstrip("/") + path,
                        json=body,
                        headers=request_headers,
                        timeout=httpx.Timeout(30),
                    ),
                )
                if response.status_code != 200:
                    # No completion stream has begun. Only transient service
                    # statuses permit the dialogue owner to retry generation;
                    # authorization, quota and invalid requests do not.
                    failure = _retry_hint(
                        Fault(
                            "dependency_unavailable",
                            unknown=True,
                            retryable_model=response.status_code in {502, 503, 504},
                        ),
                        response,
                    )
                    failure.rate_limited = response.status_code == 429
                    raise failure
                if response.headers.get("content-encoding", "identity") != "identity":
                    raise Fault("dependency_unavailable")
                chunks = response.aiter_bytes().__aiter__()
                while True:
                    try:
                        # No timeout context may survive the yield below: its
                        # consumer can be awaiting a local delivery callback.
                        chunk = await receive(anext, chunks)
                    except StopAsyncIteration:
                        break
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
            raise Fault("result_unknown", unknown=True, retryable_model=True)
        except Fault as error:
            error.cause = self._cause(
                error, "stream", response.status_code if response is not None else None
            )
            raise
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            raise Fault(
                "dependency_unavailable", retryable_model=True, cause=self._cause(error, "stream")
            ) from None
        except (
            httpx.ReadError,
            httpx.ReadTimeout,
            httpx.RemoteProtocolError,
            TimeoutError,
        ) as error:
            # The completion owner decides whether any answer/tool output was
            # observed. A truncated reasoning-only stream has no delivered result.
            raise Fault(
                "dependency_unavailable",
                unknown=True,
                retryable_model=True,
                cause=self._cause(
                    error, "stream", response.status_code if response is not None else None
                ),
            ) from None
        except (httpx.HTTPError, ValueError, UnicodeError) as error:
            raise Fault(
                "dependency_unavailable",
                unknown=True,
                cause=self._cause(
                    error, "stream", response.status_code if response is not None else None
                ),
            ) from None

    async def call(
        self, path, body=None, headers=None, *, uncertain_write=False, error_response=None
    ):
        retry_query = self._is_query(path, body)
        if not retry_query:
            return await self._call_once(
                path, body, headers, uncertain_write=uncertain_write, error_response=error_response
            )
        headers = dict(headers or {})
        headers.setdefault(
            obs.CORRELATION_HEADER, obs.current_correlation_id() or obs.new_correlation_id()
        )
        started = False
        try:
            # Share the peer's capacity fairly; commands never enter this replay path.
            async with asyncio.timeout(self.QUERY_BUDGET_SECONDS):
                async with self.query_slots:
                    # Backpressure belongs to this peer, not just the caller
                    # that happened to see it. Waiting consumes the same local
                    # budget and never starts a second semantic request.
                    while self.query_not_before > time.monotonic():
                        await asyncio.sleep(self.query_not_before - time.monotonic())
                    started = True
                    return await self._call_once(
                        path,
                        body,
                        headers,
                        uncertain_write=uncertain_write,
                        error_response=error_response,
                    )
        except TimeoutError as error:
            failure = Fault(
                "dependency_unavailable", retryable_read=True, cause=self._cause(error, "query")
            )
            failure.read_execution = "unconfirmed" if started else "waiting"
            self._cool_query(failure)
            raise failure from None
        except Fault as error:
            self._cool_query(error)
            raise

    def _cool_query(self, error):
        if not error.retryable_read:
            return
        # A timed-out HTTP owner does not stop the peer's worker. Avoid
        # immediately adding more work while that worker actually drains.
        if getattr(error, "read_execution", None) != "waiting":
            delay = 1.0 if getattr(error, "read_execution", None) == "unconfirmed" else 0.2
            delay = max(delay, getattr(error, "retry_after_seconds", None) or 0)
            self.query_not_before = max(self.query_not_before, time.monotonic() + delay)
        error.retry_after_seconds = max(
            getattr(error, "retry_after_seconds", None) or 0,
            self.query_not_before - time.monotonic(),
        )

    @classmethod
    def _is_query(cls, path, body):
        if path == "/internal/v1/memory/candidates/process":
            # This endpoint has both receipt lookup and proposal submission.
            # Only the exact null-output lookup can recover a stale connection
            # without spending another extraction attempt or replaying a write.
            return (
                type(body) is dict
                and (
                    set(body) == {"job_id", "output"}
                    or (
                        set(body) == {"job_id", "output", "supported_extraction_schemas"}
                        and body["supported_extraction_schemas"] in ([3], [4], [3, 4])
                    )
                    or (
                        set(body)
                        == {"job_id", "job_ids", "output", "supported_extraction_schemas", "window"}
                        and body["supported_extraction_schemas"] == [5]
                    )
                )
                and isinstance(body["job_id"], str)
                and bool(body["job_id"])
                and body["output"] is None
            )
        return (body is not None and path in cls.QUERY_PATHS) or (
            body is None
            and re.fullmatch(r"/internal/v1/model-requests/model:[0-9a-f]{32}", path) is not None
        )

    async def _call_once(
        self, path, body=None, headers=None, *, uncertain_write=False, error_response=None
    ):
        if not self.url or not self.token:
            raise Fault("dependency_unavailable")
        retry_query = self._is_query(path, body)
        # One correlation ID for the whole logical call, taken unchanged from the operation
        # in flight so the caller's other events share it. It is not propagated to a peer
        # body or credential: only the dedicated header carries it.
        correlation = (
            (headers or {}).get(obs.CORRELATION_HEADER)
            or obs.current_correlation_id()
            or obs.new_correlation_id()
        )
        request_headers = {
            "Authorization": "Bearer " + self.token,
            obs.CORRELATION_HEADER: correlation,
            "Accept-Encoding": "identity",
            **(headers or {}),
        }
        started, attempts = time.perf_counter(), 0
        sent, not_started = False, False
        response = None
        body_complete = False
        value = None
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
                body_complete = True
            value = strict_json(content)
            if response.status_code >= 300:
                not_started = (
                    isinstance(value, dict) and value.get("execution_state") == "not_started"
                )
                code = value.get("code") if isinstance(value, dict) else None
                if (
                    retry_query
                    and (
                        response.status_code in {429, 503}
                        and code in {"queue_full", "dependency_unavailable"}
                        or response.status_code == 408
                        and code == "timeout"
                    )
                    and not_started
                    and value.get("retryable") is True
                ):
                    failure = _retry_hint(Fault(code, retryable_read=True), response)
                    failure.read_execution = "not_started"
                    raise failure
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
                    failure = _retry_hint(
                        Fault(code, unknown=value.get("execution_state") == "unknown"), response
                    )
                    # Preserve the peer's explicit read outcome for its semantic
                    # owner. This alone does not make another HTTP error retryable.
                    if retry_query and not_started:
                        failure.read_execution = "not_started"
                    raise failure
                raise _retry_hint(Fault("dependency_unavailable"), response)
            self._report("succeeded", correlation, started, attempts)
            return value
        except (httpx.HTTPError, ValueError, TimeoutError, Fault) as error:
            # Third-party exception text is never logged; only a fixed failure class.
            self._report("failed", correlation, started, attempts, error)
            cause = self._cause(
                error,
                "query" if retry_query else "command",
                response.status_code if response is not None else None,
            )
            if isinstance(error, Fault):
                error.cause = cause
                if error_response is not None and body_complete and response.status_code >= 300:
                    # The semantic client owns schema/request binding. Inspect the
                    # bounded complete body here; never attach peer text to faults
                    # or logs, and never classify transport/partial responses.
                    error_response(response.status_code, value, error)
                if (
                    path == "/internal/v1/origins/resolve"
                    and body_complete
                    and response.status_code in {403, 404}
                    and not error.unknown
                ):
                    error.origin_response_unavailable = True
                if uncertain_write and sent and not not_started:
                    failure = Fault(
                        error.code, unknown=True, current_version=error.current_version, cause=cause
                    )
                    failure.retry_after_seconds = getattr(error, "retry_after_seconds", None)
                    raise failure from None
                raise
            recover_read = retry_query and isinstance(
                error,
                (
                    httpx.TimeoutException,
                    httpx.NetworkError,
                    httpx.RemoteProtocolError,
                    TimeoutError,
                ),
            )
            failure = Fault(
                "dependency_unavailable",
                unknown=uncertain_write and sent,
                retryable_read=recover_read,
                cause=cause,
            )
            if recover_read:
                failure.read_execution = "unconfirmed"
                failure.optional_read_unavailable = self._optional_transport_failure(error)
            if (
                path == "/internal/v1/origins/resolve"
                and body_complete
                and not isinstance(error, UnicodeError)
                and (
                    response.status_code in {403, 404}
                    or response.status_code == 200
                    and isinstance(error, ValueError)
                )
            ):
                # Only a complete bounded origin response can be discarded here.
                # TLS, HTTP framing, body encoding/size and shared auth stay faults.
                failure.origin_response_unavailable = True
            raise failure from None

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

    async def book_snapshot(self, service, origin, scope, *, grant_id=None):
        if service not in self.issuers or "reading-state" not in self.contracts.schemas:
            return None
        issuer, client = self.issuers[service]
        if issuer != "platform":
            return None
        request = dict(schema_version=1, query=query(origin), scope=scope, grant_id=grant_id)
        self.contracts.check("reading-state#request", request)
        response = await client.call("/internal/v1/books/reading", request)
        self.contracts.check("reading-state#response", response)
        if response["request_id"] != request["query"]["request_id"] or response["scope"] != scope:
            raise Fault("forbidden")
        return response

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
        if issuer not in ("nonebot", "platform"):
            raise Fault("dependency_unavailable")
        request = dict(
            schema_version=1,
            request_id=envelope["request_id"],
            assertion_ref=envelope["origin"]["assertion_ref"],
        )
        try:
            value = await client.call("/internal/v1/origins/resolve", request)
        except Fault as error:
            if not error.unknown and (
                getattr(error, "origin_response_unavailable", False)
                or getattr(error, "optional_read_unavailable", False)
            ):
                error.origin_read_unavailable = True
            raise
        try:
            self.contracts.check("common#origin_resolve_response", value)
        except Fault as error:
            if error.code == "invalid_input" and not error.unknown:
                error.origin_read_unavailable = True
            raise
        context = value["context"]
        if (
            value["request_id"] != request["request_id"]
            or context["issuer"] != issuer
            or context["authenticated_service"] != service
            or context["audience_service"] != "companion"
            or context["assertion_ref"] != request["assertion_ref"]
        ):
            error = Fault("forbidden")
            error.origin_read_unavailable = True
            raise error
        if epoch(context["expires_at"]) <= now or context["revoked"]:
            from .context_checks import dependency_changed

            raise dependency_changed("forbidden")
        return context


class Memory:
    extraction_schema = 5

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
            response = await self.client.call(
                "/internal/v1/identity/register", request, uncertain_write=True
            )
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

        def source_error(status, value, error):
            if status != 409 or error.unknown:
                return
            try:
                self.contracts.check("common#error", value)
            except Fault:
                return
            if (
                value["request_id"] == request["request_id"]
                and value["code"] == "version_conflict"
                and value["execution_state"] == "not_started"
                and value["retryable"] is False
                and error.code == "version_conflict"
            ):
                # This endpoint's sole409 is _source_rows' exact source
                # inactive/revision/payload mismatch. The original Fault is
                # unchanged for repair/outbox and all non-context callers.
                # not_started is an envelope label, not a claim that its
                # source barrier did no work before proving the negative.
                error.source_invalid = True

        response = await self.client.call(
            "/internal/v1/memory/source-sync/check", request, error_response=source_error
        )
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
            group_units = [u for u in units if u["semantic_group_id"] == group["semantic_group_id"]]
            members = {u["record_id"] for u in group_units}
            if (
                members != set(group["record_ids"])
                or len(members) != len(group["record_ids"])
                or len({u["subject_person_id"] for u in group_units}) > 1
            ):
                raise Fault("invalid_input")
            grouped.update(members)
        if grouped != ids or (units and len(canonical(units).encode()) > budget["bytes"]):
            raise Fault("budget_exceeded")
        owners = {scope["person_id"]}
        if scope["audience"] == "group":
            owners = group_context_owners(scope, response["scope_checks"])
            unit_owners = {unit["subject_person_id"] for unit in units}
            if response["scope_checks"][0]["scope_version"] != response["scope_version"]:
                raise Fault("scope_changed")
            # Content returns checks only for owners whose complete groups fit.
            # A zero-budget recheck instead retains the already pinned owner set.
            if (not known or all(budget.values())) and owners != unit_owners | {scope["person_id"]}:
                raise Fault("forbidden")
        for unit in units:
            if unit["subject_person_id"] not in owners:
                raise Fault("forbidden")
            if scope["audience"] == "group" and (
                unit["visibility"] != "shared_projection"
                or any(s["kind"] != "shareable_projection" for s in unit["sources"])
            ):
                raise Fault("forbidden")
        return response

    async def query_context(
        self,
        origin,
        scope,
        text,
        budget,
        *,
        queries=None,
        known_bundle=None,
        retrieval_mode="factual",
    ):
        from .memory_context import query_request, validate_response

        request = query_request(
            query(origin),
            scope,
            text,
            budget,
            queries=queries,
            known_bundle=known_bundle,
            retrieval_mode=retrieval_mode,
        )
        self.contracts.check("memory-context-v2#query_request", request)
        try:
            response = await self.client.call("/internal/v2/memory/context/query", request)
        except Fault as error:
            # The initial two-phase selection can lose its candidate domain
            # before producing any bundle. Only this explicit read conflict
            # belongs to the same preparation owner as an invalid current bundle.
            if (
                known_bundle is None
                and error.code == "scope_changed"
                and not error.unknown
                and (error.cause or {}).get("http_status") == 409
                and getattr(error, "read_execution", None) == "not_started"
            ):
                error.memory_selection_invalid = True
            raise
        self.contracts.check("memory-context-v2#query_response", response)
        return validate_response(request, response, time.time(), epoch)

    async def verify_context(self, origin, scope, bundle, checks=()):
        # The wire bound is per batch, never a new rejection of a long valid history.
        # Every batch is awaited and validated before the caller can generate/send.
        batches = [list(checks)[i : i + 64] for i in range(0, len(checks), 64)] or [[]]
        for batch in batches:
            request = dict(
                schema_version=2,
                query=query(origin),
                requested_scope=scope,
                bundle=bundle,
                checks=batch,
            )
            self.contracts.check("memory-context-v2#verify_request", request)
            response = await self.client.call("/internal/v2/memory/context/verify", request)
            self.contracts.check("memory-context-v2#verify_response", response)
            current = response["bundle"]
            if (
                response["request_id"] != request["query"]["request_id"]
                or response["effective_scope"] != scope
                or current["effective_scope"] != scope
            ):
                raise Fault("forbidden")
            if (
                epoch(response["valid_until"]) <= time.time()
                or epoch(current["valid_until"]) <= time.time()
            ):
                raise Fault("scope_changed")
            if current.get("retrieval_mode", "factual") != bundle.get("retrieval_mode", "factual"):
                raise Fault("invalid_input")
            if bundle.get("retrieval_mode") == "context":
                from .memory_context import bundle_identity

                if bundle_identity(current) != bundle_identity(bundle):
                    raise Fault("scope_changed")
            if any(
                current[key] != bundle[key]
                for key in (
                    "queries",
                    "selection",
                    "include_associated",
                    "budget",
                    "limits",
                    "evidence_ids",
                    "evidence_digest",
                    "group_dependencies",
                    "episode_dependencies",
                )
            ):
                raise Fault("scope_changed")
            expected = [(c["check_id"], c["kind"], "valid") for c in batch]
            if [(c["check_id"], c["kind"], c["status"]) for c in response["checks"]] != expected:
                raise Fault("invalid_input")
        return response

    async def prepare_context(self, origin, scope, bundle, checks, required_check_ids):
        from .memory_preparation import prepare

        return await prepare(self, origin, scope, bundle, checks, required_check_ids)

    async def profiles_batch(self, origin, scope, requests):
        from .memory_preparation import profiles

        return await profiles(self, origin, scope, requests)

    async def history(self, origin, scope, *, expected=None, known=None):
        return await self._history(origin, scope, expected=expected, known=known)

    async def input_history(self, origin, scope, *, expected=None, known=None):
        return await self._history(origin, scope, expected=expected, known=known, input_only=True)

    async def _history(self, origin, scope, *, expected=None, known=None, input_only=False):
        request = dict(
            query=query(origin),
            requested_scope=scope,
            include_associated=scope["audience"] == "self_private",
            expected_snapshot=expected,
            known_snapshot=known,
        )
        self.contracts.check("context-history#request", request)
        path = "input-history" if input_only else "history"
        response = await self.client.call("/internal/v1/memory/context/" + path, request)
        self.contracts.check("context-history#response", response)
        if (
            response["request_id"] != request["query"]["request_id"]
            or response["effective_scope"] != scope
        ):
            raise Fault("forbidden")
        snapshot = response["snapshot"]
        history_scopes = snapshot["history_checks"]
        if not history_scopes or history_scopes[0]["scope"] != scope:
            raise Fault("forbidden")
        if scope["audience"] == "group":
            group_context_owners(scope, history_scopes)
        if expected is not None and (
            response["scope_version"] != expected["scope_version"]
            or snapshot["association_version"] != expected["association_version"]
            or [
                {k: row[k] for k in ("scope", "association_id", "association_version")}
                for row in history_scopes
            ]
            != [
                {k: row[k] for k in ("scope", "association_id", "association_version")}
                for row in expected["scope_checks"]
            ]
        ):
            raise Fault("scope_changed")
        if epoch(response["valid_until"]) <= time.time() or (
            known is not None and response["snapshot"] != known
        ):
            raise Fault("scope_changed")
        return response["snapshot"]

    async def propose(self, request):
        self.contracts.check("memory-context#proposal_request", request)
        return await self._memory_write(request, "/internal/v1/memory/context/propose")

    async def forget_episode(self, request):
        self.contracts.check("memory-context-v2#forget_request", request)
        return await self._memory_write(request, "/internal/v2/memory/context/forget")

    async def _memory_write(self, request, path):
        try:
            response = await self.client.call(path, request, uncertain_write=True)
        except Fault as error:
            if not error.unknown:
                raise
            try:
                response = await self.receipt(
                    request["command"]["origin"],
                    request["scope"],
                    request["command"]["idempotency_key"],
                )
            except Fault as lookup_error:
                # Failure to read a receipt says nothing about the submitted
                # write. Keep its operation available for later recovery.
                raise Fault(lookup_error.code, unknown=True, cause=lookup_error.cause) from None
            if response is None:
                raise Fault("result_unknown", unknown=True) from None
        try:
            self.contracts.check("memory-context#receipt", response)
        except Fault:
            # A successful transport with an unusable receipt does not prove
            # the write failed. Preserve the submitted operation for recovery.
            raise Fault("invalid_input", unknown=True) from None
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
        self._profile_request(request)
        response = await self.client.call("/internal/v1/memory/profiles/select", request)
        return self._profile_response(request, response)

    def _profile_request(self, request):
        scope, target = request["requester_scope"], request["target"]
        self.contracts.check("profiles#select_request", request)
        if target["kind"] == "group" and (
            scope["audience"] != "group" or target["conversation_id"] != scope["conversation_id"]
        ):
            raise Fault("forbidden")

    def _profile_response(self, request, response):
        scope, target = request["requester_scope"], request["target"]
        selection, budget = request["selection"], request["budget"]
        known_version = request["known_scope_version"]
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

    async def process_candidate(
        self, job_id, output, *, extraction_schema=None, job_ids=None, window=None, batch_ref=None
    ):
        if extraction_schema is not None and (
            type(extraction_schema) is not int or extraction_schema not in (3, 4, 5)
        ):
            raise Fault("invalid_input")
        request = dict(job_id=job_id, output=output)
        if extraction_schema == 5:
            mutates = output is not None or window is not None
            request["job_ids"] = list(job_ids or [job_id])
            if output is None:
                request.update(supported_extraction_schemas=[5], window=window)
            else:
                request.update(extraction_schema=5, batch_ref=batch_ref)
            self.contracts.check("memory-extraction#process_request", request)
            if len(canonical(request).encode()) > 262144:
                raise Fault("budget_exceeded")
            response = await self.client.call(
                "/internal/v1/memory/candidates/process",
                request,
                # A new window can capture episodes/claim a batch even before
                # model output exists. Receipt-only lookups create no batch.
                uncertain_write=mutates,
            )
            try:
                self.contracts.check("memory-extraction#process_response", response)
            except Fault:
                raise Fault("invalid_input", unknown=mutates) from None
            if response["extraction_schema"] != 5 or job_id not in response["job_ids"]:
                raise Fault("invalid_input", unknown=mutates)
            if response["job_ids"] != request["job_ids"]:
                raise Fault("invalid_input", unknown=mutates)
            if batch_ref is not None and response.get("batch_ref", batch_ref) != batch_ref:
                raise Fault("invalid_input", unknown=mutates)
            if response["state"] == "completed":
                result = response["result"]
                if (
                    result["covered_job_ids"] != response["job_ids"]
                    or result["cursor"]["job_ids"] != response["job_ids"]
                ):
                    raise Fault("invalid_input", unknown=mutates)
                if (
                    window is not None
                    and result["cursor"]["window_id"] != window["manifest"]["window_id"]
                ):
                    raise Fault("invalid_input", unknown=mutates)
            if (
                response["state"] == "pending"
                and window is not None
                and response["window_id"] != window["manifest"]["window_id"]
            ):
                raise Fault("invalid_input", unknown=mutates)
            if response["state"] == "context_changed":
                if (
                    output is not None
                    or window is None
                    or response["window_id"] != window["manifest"]["window_id"]
                ):
                    raise Fault("invalid_input", unknown=mutates)
                members = {member["member_id"]: member for member in window["manifest"]["members"]}
                invalid = response["invalid_context_member_ids"]
                if len(set(invalid)) != len(invalid) or any(
                    identity not in members or members[identity]["purpose"] != "context"
                    for identity in invalid
                ):
                    raise Fault("invalid_input", unknown=mutates)
            if response["state"] == "unclaimed" and (
                output is not None or window is not None or batch_ref is not None
            ):
                raise Fault("invalid_input", unknown=mutates)
            return response
        if extraction_schema is not None:
            if output is None:
                request["supported_extraction_schemas"] = [3, 4] if extraction_schema == 4 else [3]
            else:
                request["extraction_schema"] = extraction_schema
            self.contracts.check("memory-extraction#process_request", request)
        value = await self.client.call(
            "/internal/v1/memory/candidates/process",
            request,
            uncertain_write=output is not None,
        )
        if extraction_schema is not None:
            try:
                self.contracts.check("memory-extraction#process_response", value)
            except Fault:
                raise Fault("invalid_input", unknown=output is not None) from None
            supported = request.get("supported_extraction_schemas", [extraction_schema])
            if (
                type(value["extraction_schema"]) is not int
                or value["extraction_schema"] not in supported
            ):
                raise Fault("invalid_input", unknown=output is not None)
            for index, source in enumerate(value["content"]):
                if source["index"] != index:
                    raise Fault("invalid_input", unknown=output is not None)
            return value
        if (
            not isinstance(value, dict)
            or set(value)
            not in (
                {"state", "result", "content"},
                {"state", "result", "content", "extraction_schema", "known_facts"},
            )
            or value["state"] not in {"pending", "completed", "invalidated", "rejected"}
            or not isinstance(value["content"], list)
        ):
            raise Fault("invalid_input", unknown=output is not None)
        if value["state"] == "rejected":
            if value["result"] == {"error_code": "invalid_input"} and not value["content"]:
                return value
            raise Fault("invalid_input", unknown=output is not None)
        if value["state"] == "completed":
            try:
                self.contracts.check("workflow#candidate_result", value["result"])
            except Fault:
                raise Fault("invalid_input", unknown=output is not None) from None
        elif value["result"] is not None:
            raise Fault("invalid_input", unknown=output is not None)
        if value["state"] != "pending" and value["content"]:
            raise Fault("invalid_input", unknown=output is not None)
        if "extraction_schema" in value:
            if (
                value["state"] != "pending"
                or type(value["extraction_schema"]) is not int
                or value["extraction_schema"] not in {2, 3}
                or type(value["known_facts"]) is not list
                or len(value["known_facts"]) > 128
                or len(canonical(value["known_facts"]).encode()) > 32768
            ):
                raise Fault("invalid_input", unknown=output is not None)
        for index, source in enumerate(value["content"]):
            if (
                type(source) is not dict
                or set(source) != {"index", "text", "sent_at"}
                or type(source["index"]) is not int
                or source["index"] != index
                or type(source["text"]) is not str
                or type(source["sent_at"]) is not str
            ):
                raise Fault("invalid_input", unknown=output is not None)
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
        if "generation_budget" in turn:
            budget = turn["generation_budget"]
            body.update(
                max_tokens=budget["max_tokens"], reasoning_effort=budget["reasoning_effort"]
            )
        if tools:
            body.update(tools=tools, tool_choice="auto")
        self.contracts.check("model#native_request", body)
        result = Completion()
        self.active_requests[turn["id"]] = request_id
        try:
            interrupted = None
            try:
                async with aclosing(
                    self.client.stream("/v1/chat/completions", body, headers)
                ) as stream:
                    async for chunk in stream:
                        text = result.consume(chunk)
                        if text and on_delta:
                            await on_delta(text)
            except Fault as error:
                interrupted = error
            if result.rate_limited or getattr(interrupted, "rate_limited", False):
                # Drain the stream first. A typed rejection is not an unknown
                # transport cut. HTTP 429 is also only a pressure signal: its
                # own Gateway must confirm a terminal failure before recovery.
                allowed = (
                    not result.tools
                    and result.finish_reason is None
                    and (
                        interrupted is None
                        or interrupted.code in {"dependency_unavailable", "result_unknown"}
                    )
                    and await self._rejected_rate_limit(turn, request_id)
                )
                failure = Fault(
                    "dependency_unavailable",
                    unknown=True,
                    retryable_model=allowed,
                    cause=interrupted.cause if interrupted is not None else None,
                )
                failure.rate_limited = True
                failure.retry_after_seconds = getattr(interrupted, "retry_after_seconds", None)
                raise failure
            if interrupted is not None:
                # Ordinary cuts and callback faults retain the original output
                # boundary. The explicit rejection case above never broadens it.
                if result.content or result.tools or result.finish_reason:
                    interrupted.retryable_model = False
                elif interrupted.retryable_model:
                    interrupted.retryable_model = await self._settle_request(turn, request_id)
                    if not interrupted.retryable_model:
                        interrupted.unknown = True
                raise interrupted
            try:
                message = result.message()
            except Fault as error:
                # [DONE] without a completed choice is also an interrupted result.
                # Hidden reasoning is never a user reply or a completed tool call.
                if error.code == "dependency_unavailable" and not (
                    result.content or result.tools or result.finish_reason
                ):
                    error.retryable_model = await self._settle_request(turn, request_id)
                raise
            receipt = await self._route_receipt(turn, request_id)
            if receipt["outcome"] != "succeeded":
                raise Fault("dependency_unavailable")
            return message, receipt
        except asyncio.CancelledError as error:
            # Once caught, cancellation allows bounded cleanup in this task.
            # A second cancel interrupts it directly; no shielded child request
            # may survive after this task and its model slot have been released.
            error.model_request_id = request_id
            error.unknown = True
            await self._settle_request(turn, request_id)
            raise
        except Fault as error:
            error.model_request_id = request_id
            raise
        finally:
            if self.active_requests.get(turn["id"]) == request_id:
                self.active_requests.pop(turn["id"], None)

    async def _route_receipt(self, turn, request_id):
        receipt = await self.client.call("/internal/v1/model-requests/" + request_id)
        self.contracts.check("model#route_receipt", receipt)
        if (
            receipt["request_id"] != request_id
            or receipt["config_version"] != turn["config_version"]
            or receipt["caller_service"] != "companion"
        ):
            raise Fault("dependency_unavailable")
        return receipt

    async def _settle_request(self, turn, request_id):
        """Request owned cancellation and briefly confirm local execution closure."""
        try:
            async with asyncio.timeout(1):
                cancelled = await self.client.call(
                    "/internal/v1/model-requests/" + request_id + "/cancel", {}
                )
                self.contracts.check("model#cancel_response", cancelled)
                if cancelled["request_id"] != request_id or cancelled["state"] not in {
                    "requested",
                    "terminal",
                }:
                    return False
                for attempt in range(3):
                    receipt = await self._route_receipt(turn, request_id)
                    execution = receipt.get("execution") or {}
                    state = execution.get("state")
                    # requested/unknown/not_active do not prove local execution
                    # has closed. Cancellation never proves upstream nonexecution.
                    if (
                        (
                            state == "cancelled"
                            and execution.get("cancel_requested") is True
                            and receipt["outcome"] == "unknown"
                        )
                        or (state == "completed" and receipt["outcome"] == "succeeded")
                        or (state == "failed" and receipt["outcome"] == "failed")
                    ):
                        return True
                    if state not in {"queued", "running"} or attempt == 2:
                        return False
                    await asyncio.sleep(0.05 * 2**attempt)
        except (Fault, TimeoutError, OSError):
            return False
        return False

    async def _rejected_rate_limit(self, turn, request_id):
        """One short read window for the stream owner's durable terminal receipt."""
        try:
            async with asyncio.timeout(1):
                for attempt in range(3):
                    receipt = await self._route_receipt(turn, request_id)
                    execution = receipt.get("execution") or {}
                    if receipt["outcome"] == "failed":
                        return (
                            execution.get("state") == "failed"
                            and execution.get("error_code") == "upstream_http_error"
                            and execution.get("upstream_started") is True
                            and execution.get("cancel_requested") is False
                            and execution.get("finish_reasons") == []
                        )
                    if (
                        receipt["outcome"] != "unknown"
                        or execution.get("state") not in {"queued", "running"}
                        or execution.get("cancel_requested")
                        or attempt == 2
                    ):
                        return False
                    await asyncio.sleep(0.05 * 2**attempt)
        except (Fault, TimeoutError):
            return False
        return False

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
        if request["origin"]["kind"] in {"response", "direct"}:
            current = await self.expression_context(
                request["origin"], request["scope"], request["channel"]
            )
            if current["origin"] != request["origin"]["origin"]:
                raise Fault("scope_changed")
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
