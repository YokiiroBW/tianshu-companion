"""Independent safe runtime-event log adapter for the diagnostics contract v1.

This module is the whole logging boundary of the companion service, and it owns exactly
four things:

1. **The closed record.** One UTF-8 JSON document plus one LF per event, at most 4096
   bytes, with the eleven fields `contracts/diagnostics/v1/event.schema.json` freezes.
   There is no place to put a message, an extra, a stack trace, a URL, a header, a token,
   an account or any request/response body, so a secret cannot be logged by accident: the
   only free-form values the port accepts are a registered event name, a registered error
   code, an outcome from the frozen enumeration and a correlation ID that is strictly 32
   lowercase hex characters.
2. **The registered event enum.** `EVENTS` is the complete static registry of runtime
   events this product emits. An event name that is not in it is refused, so a caller
   string can never become an event name.
3. **Durable output and honest degradation.** With `TIANSHU_LOG_DIR` set, records are
   appended to a per-instance file, segmented at 64 MiB, inside a directory budget
   (1 GiB by default, configurable from 32 MiB to 64 GiB). When the budget is reached the
   adapter refuses new records so the host can refuse new business instead of silently
   dropping the "all events" promise. A write failure never raises into business code: the
   event is dropped, `log_unavailable` is kept in memory, stderr carries one fixed safe
   warning, and recovery needs a genuinely successful write.
4. **Correlation.** A correlation ID is 32 random lowercase hex characters and is never
   derived from a user identity. HTTP input is validated before it is used; an invalid
   value is replaced and never echoed back.

Deliberate non-goals: this module imports nothing from the domain modules (`core`,
`direct`, `life`, `writing`, ...), opens no database, and decides no business question. It
is a port the host injects, never a second business ledger.
"""

import json
import os
import re
import secrets
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = "1.0.0"
SERVICE = "companion"

# One record is one line; the contract bounds the line, not the file.
MAX_RECORD_BYTES = 4096
LF = b"\n"

RECORD_FIELDS = (
    "schema_version",
    "timestamp",
    "service",
    "instance_id",
    "sequence",
    "event_id",
    "level",
    "event",
    "outcome",
    "correlation_id",
    "duration_ms",
    "error_code",
)

LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
OUTCOMES = (
    "started",
    "succeeded",
    "failed",
    "cancelled",
    "unknown",
    "rejected",
    "degraded",
)

CORRELATION_HEADER = "X-Tianshu-Correlation-Id"
# The one variable that turns file output on. It is explicit on purpose: a deployment that
# meant to keep full logs must not depend on a default directory being writable.
ENVIRONMENT_LOG_DIR = "TIANSHU_LOG_DIR"
ENVIRONMENT_LOG_SEGMENT_BYTES = "TIANSHU_LOG_SEGMENT_BYTES"
ENVIRONMENT_LOG_DIRECTORY_BYTES = "TIANSHU_LOG_DIRECTORY_BYTES"
CORRELATION_RE = re.compile(r"^[a-f0-9]{32}$")
EVENT_RE = re.compile(r"^[a-z][a-z0-9_.]{0,63}$")
ERROR_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

# The static registry: every runtime event this product emits. Full coverage means every
# one of these is emitted where it happens; an unregistered name is a programming error,
# and a user string can never take the place of one.
EVENTS = frozenset(
    {
        # process and background loops
        "runtime.started",
        "runtime.stopping",
        "runtime.stopped",
        "runtime.log_probe",
        # A loop reports what it did: real work, or a failure. It does not report merely
        # starting or idling - neither is an outcome, both would drown the real events, and a
        # per-loop startup record is written before the process can serve, which is exactly
        # where a hard limit on the channel (a full pipe) turns noise into a stalled start.
        "runtime.background_work",
        "runtime.background_failed",
        # inbound service requests (the two health probes are deliberately not here: a
        # probe never writes a log record, so the probe itself stays purely read-only)
        "service.request.started",
        "service.request.authenticated",
        "service.request.finished",
        # outbound calls to already-existing internal peers
        "peer.call.started",
        "peer.call.finished",
        # conversation scheduling
        "turn.prepared",
        "turn.queued",
        "turn.generation.started",
        "turn.generation.finished",
        "turn.cancelled",
        "turn.delivery.started",
        "turn.delivery.finished",
        "outbox.flush",
        # explicit functional commands
        "direct.request.queued",
        "direct.request.cancelled",
        "direct.attempt.started",
        "direct.attempt.finished",
        "direct.delivery.started",
        "direct.delivery.finished",
        "direct.delivery.deferred",
    }
)

# The static error-code registry. A value that is not registered is replaced by the fixed
# `internal_error`: an unregistered code must not reach the file, and no exception text
# (which could quote a secret) may be logged in its place.
ERROR_CODES = frozenset(
    {
        "internal_error",
        "log_capacity_exhausted",
        "dependency_unavailable",
        "unauthorized",
        "forbidden",
        "invalid_input",
        "not_found",
        "version_conflict",
        "idempotency_conflict",
        "scope_changed",
        "budget_exceeded",
        "queue_full",
        "timeout",
        "result_unknown",
        "unknown",
        # Delivery and routing reasons that already exist as durable state on a request or
        # a turn. The event repeats the stored reason; it never derives a new one.
        "conversation_mismatch",
        "conversation_unmapped",
        "plugin_unavailable",
        "delivery_port_unavailable",
        "invalid_parameters",
        "authorization_revoked",
        "outbound_band_busy",
        "interrupted_delivery",
        "interrupted_dependency_call",
        "explicit_user_cancel",
        "source_retracted",
        "source_edited",
        "permission_revoked",
        "classification_changed",
        "expired",
        "resource_limit",
    }
    # Every label the failure table can produce is itself a registered code: a reported
    # failure must never be rewritten into `internal_error` merely because the label was
    # missing from this registry.
    | {
        "cancelled",
        "connect_error",
        "read_error",
        "write_error",
        "protocol_error",
        "transport_error",
        "os_error",
        "fault",
    }
)

# Failure classes the host may attach to a peer call or a transport attempt. These are
# fixed labels derived from the exception *type* through an explicit table - never from
# `str(exception)`, which could quote a request body or a token.
FAILURE_CLASSES = {
    "TimeoutError": "timeout",
    "asyncio.TimeoutError": "timeout",
    "CancelledError": "cancelled",
    "ConnectError": "connect_error",
    "ConnectTimeout": "connect_error",
    "ReadTimeout": "read_error",
    "WriteTimeout": "write_error",
    "ReadError": "read_error",
    "WriteError": "write_error",
    "RemoteProtocolError": "protocol_error",
    "HTTPError": "transport_error",
    "OSError": "os_error",
    "Fault": "fault",
    "ValueError": "invalid_input",
    "UnicodeError": "invalid_input",
}

DEFAULT_MAX_SEGMENT_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_DIRECTORY_BYTES = 1024 * 1024 * 1024
MIN_DIRECTORY_BYTES = 32 * 1024 * 1024
MAX_DIRECTORY_BYTES = 64 * 1024 * 1024 * 1024

# The fixed labels an outbound peer call may carry. These are code-level interface names,
# never a configured URL, host, account or channel identifier.
PEER_LABELS = frozenset(
    {"origins", "identity", "memory", "profiles", "gateway", "channel", "images"}
)

# How many outbound transport attempts one logical peer call may report. A transport retry
# is still one call in business terms, so the count is reported as an attribute rather than
# duplicated as extra events.
MAX_TRANSPORT_ATTEMPTS = 8

_SEGMENT_SUFFIX = re.compile(r"^\.jsonl(?:\.(\d+))?$")

# The correlation ID of the work in flight. Set from a validated inbound header, or minted
# where an operation begins; every nested call in the same task inherits it unchanged.
_correlation: ContextVar = ContextVar("tianshu_correlation_id", default=None)


def new_correlation_id():
    """32 random lowercase hex characters; never derived from any user identity."""
    return secrets.token_hex(16)


def valid_correlation_id(value):
    return isinstance(value, str) and CORRELATION_RE.match(value) is not None


def current_correlation_id():
    return _correlation.get()


@contextmanager
def correlation_scope(value=None):
    """Run a block under one correlation ID, reusing a live one unless a value is given."""
    resolved = value if valid_correlation_id(value) else current_correlation_id()
    if not valid_correlation_id(resolved):
        resolved = new_correlation_id()
    token = _correlation.set(resolved)
    try:
        yield resolved
    finally:
        _correlation.reset(token)


def failure_class(error):
    """A fixed label for an exception, from its type only - never from its message."""
    for kind in type(error).__mro__:
        label = FAILURE_CLASSES.get(kind.__name__)
        if label is not None:
            return label
    return "internal_error"


def error_code(value):
    """Normalise a code to the registered static enum, or the fixed fallback."""
    return value if isinstance(value, str) and value in ERROR_CODES else "internal_error"


def timestamp(seconds=None):
    """Millisecond UTC ISO-8601, the shape the published contract sample uses."""
    moment = datetime.fromtimestamp(time.time() if seconds is None else seconds, timezone.utc)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def encode_record(record):
    """One closed record as bytes: UTF-8 JSON plus LF, or a refusal.

    A record that is not exactly the frozen field set, carries an unregistered event or
    outcome, or is longer than the contract's 4096-byte line is refused rather than
    truncated or quietly grown.
    """
    if tuple(sorted(record)) != tuple(sorted(RECORD_FIELDS)):
        raise ValueError("Runtime event must carry exactly the frozen field set")
    if record["schema_version"] != SCHEMA_VERSION:
        raise ValueError("Unsupported diagnostics schema version")
    if record["service"] != SERVICE:
        raise ValueError("Unsupported diagnostics service name")
    if record["level"] not in LEVELS:
        raise ValueError("Unsupported runtime event level")
    if record["outcome"] not in OUTCOMES:
        raise ValueError("Unsupported runtime event outcome")
    if not isinstance(record["event"], str) or record["event"] not in EVENTS:
        raise ValueError("Unregistered runtime event name")
    if record["error_code"] is not None and record["error_code"] not in ERROR_CODES:
        raise ValueError("Unregistered runtime error code")
    if record["correlation_id"] is not None and not valid_correlation_id(record["correlation_id"]):
        raise ValueError("Correlation ID must be 32 lowercase hex characters")
    if type(record["sequence"]) is not int or record["sequence"] < 1:
        raise ValueError("Runtime event sequence must be a positive integer")
    for field in ("instance_id", "event_id"):
        if not isinstance(record[field], str):
            raise ValueError("Runtime event identifiers must be strings")
        uuid.UUID(record[field])
    duration = record["duration_ms"]
    if duration is not None and (
        isinstance(duration, bool) or not isinstance(duration, (int, float)) or duration < 0
    ):
        raise ValueError("Runtime event duration must be a non-negative number or null")
    line = json.dumps(record, allow_nan=False, ensure_ascii=True).encode("utf-8") + LF
    if len(line) > MAX_RECORD_BYTES:
        raise ValueError("Runtime event exceeds the 4096-byte contract line")
    return line


class LogAdapter:
    """The runtime-event port: one bounded, durable, secret-free JSON line per event.

    `emit` never raises. Business code must not learn about a logging failure, must not
    retry because of one, and must not change any `unknown` verdict because of one. What a
    failure does change is honestly reported health: `log_unavailable` stays in memory and
    `/health/ready` cannot claim `ready` again until a real write succeeds.
    """

    def __init__(
        self,
        directory=None,
        *,
        max_segment_bytes=DEFAULT_MAX_SEGMENT_BYTES,
        max_directory_bytes=DEFAULT_MAX_DIRECTORY_BYTES,
        stderr=None,
    ):
        if not 1 <= max_segment_bytes <= MAX_DIRECTORY_BYTES:
            raise ValueError("Invalid log segment bound")
        if not MIN_DIRECTORY_BYTES <= max_directory_bytes <= MAX_DIRECTORY_BYTES:
            raise ValueError("Log directory budget must be between 32MiB and 64GiB")
        self.directory = Path(directory) if directory else None
        self.max_segment_bytes = max_segment_bytes
        self.max_directory_bytes = max_directory_bytes
        self.instance_id = str(uuid.uuid4())
        self.service = SERVICE
        self._sequence = 0
        self._lock = threading.RLock()
        self._stream = None
        self._segment_index = None
        self._segment_bytes = 0
        self._directory_bytes = 0
        self._stderr = stderr
        self.io_failed = False
        self.dropped = 0
        self.capacity_exhausted = False
        self.wrote_once = False
        self._warned = False
        self.probe_pending = False

    # ------------------------------------------------------------------ properties

    @property
    def durable(self):
        return self.directory is not None

    @property
    def degraded(self):
        return self.io_failed or self.capacity_exhausted

    @property
    def log_dir(self):
        return str(self.directory) if self.directory else None

    def health(self):
        """Closed status, no path and no environment value beyond the configured names."""
        if not self.durable:
            return "non_durable"
        if self.capacity_exhausted:
            return "capacity_exhausted"
        if self.io_failed:
            return "log_unavailable"
        return "ok"

    def accepts_business(self):
        """May the host admit new business work? Contract: no while the budget is full."""
        return not (self.durable and self.capacity_exhausted)

    # ---------------------------------------------------------------------- output

    def emit(
        self,
        event,
        outcome,
        *,
        level="INFO",
        correlation_id=None,
        error_code=None,
        duration_ms=None,
        fsync=False,
    ):
        """Write one event. Returns True when it is on disk (or on stderr), else False."""
        if event not in EVENTS:
            # A programming error, not a data error: refuse to invent an event name.
            raise ValueError("Unregistered runtime event name")
        with self._lock:
            record = {
                "schema_version": SCHEMA_VERSION,
                "timestamp": timestamp(),
                "service": SERVICE,
                "instance_id": self.instance_id,
                "sequence": self._sequence + 1,
                "event_id": str(uuid.uuid4()),
                "level": level,
                "event": event,
                "outcome": outcome,
                "correlation_id": correlation_id if valid_correlation_id(correlation_id) else None,
                "duration_ms": duration_ms,
                "error_code": error_code,
            }
            try:
                line = encode_record(record)
            except (ValueError, UnicodeError):
                # A malformed record is a defect, never a reason to disturb business flow
                # and never a reason to write the offending value somewhere else.
                self.dropped += 1
                return False
            if self.degraded:
                # Once the destination has failed or the budget is full the adapter stops
                # touching the file: an ordinary event is dropped, counted, and reported as
                # missing through health. Recovery is an explicit maintenance act - one
                # genuinely successful write - not the side effect of a lucky later event.
                self.dropped += 1
                return False
            if not self.durable:
                # Development fallback: an explicit, honest non-durable channel.
                self._to_stderr(line)
                self._sequence += 1
                self.wrote_once = True
                return True
            if self._write(line, fsync=fsync):
                self._sequence += 1
                self.wrote_once = True
                return True
            self.dropped += 1
            return False

    # ----------------------------------------------------------------------- files

    def _segment_path(self, index):
        name = "companion.jsonl" if index == 0 else f"companion.jsonl.{index}"
        return self.directory / name

    def _scan_directory(self):
        total, highest = 0, -1
        for entry in self.directory.iterdir():
            if not entry.is_file():
                continue
            match = _SEGMENT_SUFFIX.match(entry.name)
            if match is None or not entry.name.startswith("companion"):
                continue
            total += entry.stat().st_size
            index = int(match.group(1)) if match.group(1) else 0
            highest = max(highest, index)
        self._directory_bytes = total
        return highest

    def _open(self):
        if self._stream is not None:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        os.chmod(self.directory, 0o700)
        index = self._scan_directory()
        self._segment_index = index + 1
        self._open_segment()

    def _open_segment(self):
        path = self._segment_path(self._segment_index)
        self._stream = open(path, "ab", buffering=0)
        self._segment_bytes = path.stat().st_size

    def _write(self, line, *, fsync):
        try:
            self._open()
            size = len(line)
            if (
                self._segment_bytes + size > self.max_segment_bytes
                or self._directory_bytes + size > self.max_directory_bytes
            ):
                if self._directory_bytes + size > self.max_directory_bytes:
                    # The budget is full, so further segments are refused rather than
                    # deleting segments that central collection has not confirmed.
                    self.capacity_exhausted = True
                    self._warn_once("log capacity reached; new business is refused")
                    return False
                self._stream.close()
                self._segment_index += 1
                self._open_segment()
            self._stream.write(line)
            if fsync:
                self._stream.flush()
                os.fsync(self._stream.fileno())
            self._segment_bytes += size
            self._directory_bytes += size
            return True
        except (OSError, ValueError):
            self.io_failed = True
            self._warn_once("runtime event log is unavailable")
            return False

    def _warn_once(self, text):
        if self._warned:
            return
        self._warned = True
        self._to_stderr(("tianshu-companion runtime log: " + text + "\n").encode("utf-8"))

    def _to_stderr(self, line):
        stream = self._stderr
        if stream is None:
            import sys

            stream = sys.stderr
        try:
            stream.write(line.decode("utf-8", "replace"))
            if hasattr(stream, "flush"):
                stream.flush()
        except (OSError, ValueError, AttributeError):
            pass

    # ------------------------------------------------------------------ maintenance

    def probe(self):
        """Explicit maintenance action: prove a real write before trusting the log again.

        Never called by the health endpoints or by any request path, so a probe never writes a
        log record on its own account. Returns True only after a genuinely successful write;
        until then the degraded state - and the `not_ready` verdict it forces - stands.
        """
        with self._lock:
            if not self.durable or not self.degraded:
                return not self.degraded
            self.probe_pending = True
            was_io_failed, was_capacity = self.io_failed, self.capacity_exhausted
            self.io_failed = self.capacity_exhausted = False
            try:
                line = encode_record(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "timestamp": timestamp(),
                        "service": SERVICE,
                        "instance_id": self.instance_id,
                        "sequence": self._sequence + 1,
                        "event_id": str(uuid.uuid4()),
                        "level": "INFO",
                        "event": "runtime.log_probe",
                        "outcome": "succeeded",
                        "correlation_id": None,
                        "duration_ms": None,
                        "error_code": None,
                    }
                )
                if self._write(line, fsync=True):
                    self._sequence += 1
                    self.wrote_once = True
                    self._warned = False
                    return True
            except (ValueError, UnicodeError):
                pass
            finally:
                self.probe_pending = False
            self.io_failed, self.capacity_exhausted = was_io_failed, was_capacity
            return False

    def close(self):
        with self._lock:
            if self._stream is not None:
                try:
                    self._stream.close()
                except OSError:
                    pass
                self._stream = None


class NullLogAdapter:
    """The port with no destination, for tests and for callers that inject nothing."""

    instance_id = ""
    service = SERVICE
    durable = False

    def emit(self, *args, **kwargs):
        return False

    def health(self):
        return "non_durable"

    def accepts_business(self):
        return True

    def close(self):
        return None


def emit(port, event, outcome, **fields):
    """Report one event through an injected port, never disturbing business flow.

    A logging failure must not become a business failure: it must not produce a second
    model call, a second send, or a changed `unknown` verdict, so this closes over the
    port's own errors as well as a port that is missing entirely.
    """
    if port is None:
        return False
    if fields.get("correlation_id") is None:
        fields["correlation_id"] = current_correlation_id()
    try:
        return bool(port.emit(event, outcome, **fields))
    except Exception:
        return False


def build_log_adapter(directory=None, **options):
    """Assemble the port from an explicitly configured directory, or the stderr fallback."""
    if directory is None:
        return LogAdapter(None)
    return LogAdapter(directory, **options)
