"""Independent safe runtime-event log adapter for the diagnostics contract v1.

This module is the whole logging boundary of the companion service, and it owns exactly five
things:

1. **The closed record.** One UTF-8 JSON document plus one LF per event, at most 4096
   bytes, with the twelve fields `contracts/diagnostics/v1/event.schema.json` freezes.
   There is no place to put a message, an extra, a stack trace, a URL, a header, a token,
   an account or any request/response body, so a secret cannot be logged by accident: the
   only free-form values the port accepts are a registered event name, a registered error
   code, an outcome from the frozen enumeration and a correlation ID that is strictly 32
   lowercase hex characters.
2. **The registered event and error-code enums.** `EVENTS` is the complete static registry of
   runtime events this product emits; an event name that is not in it is refused, so a caller
   string can never become an event name. `ERROR_CODES` does the same for codes.
3. **The domain-to-runtime outcome mapping.** Business code names its own states (`sent`,
   `completed`, `too_late`, ...). Those are *not* runtime outcomes, and the frozen outcome
   enumeration must not grow to accommodate them. `runtime_outcome` is the one explicit table
   that maps a domain state to a registered outcome, so a successful send is recorded as
   `succeeded` instead of being silently dropped.
4. **One bounded writer, off the event loop.** Every filesystem call happens on a single
   writer thread that owns the file handle. Business code - including code holding the
   authoritative SQLite transaction - only enqueues a record; it never waits for a disk. A
   bounded queue means a saturated writer refuses new business (`503`) instead of piling up
   threads, growing without limit, or quietly pretending the event was written.
5. **Durable admission before side effects.** `admit` writes one record and waits for it to
   reach the disk (`flush` + `fsync`) *outside* any transaction. A request that cannot get
   its admission record written is refused before it does anything, so an IO failure can never
   let business run without the log that was supposed to account for it. A failure after the
   fact is reported honestly and never turns into a second send.

Deliberate non-goals: this module imports nothing from the domain modules (`core`, `direct`,
`life`, `writing`, ...), opens no database, and decides no business question. It is a port the
host injects, never a second business ledger.
"""

import asyncio
import io
import json
import os
import queue
import re
import secrets
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
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
        # inbound service requests (the health probes are deliberately not here: a probe never
        # writes a log record, so the probe itself stays purely read-only)
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

# The terminal events: the registered events that *are* an outcome - the end of a request,
# background pass, peer call, turn, attempt, delivery or of the process itself. The contract
# requires the end of a piece of work to be *persisted*, not merely handed to the kernel, so these
# are the records the writer fsyncs. They are still written on the writer's thread: the caller
# (often the event loop) only enqueues, and the acknowledgement it can wait for is
# `flush`/`await_flush`, which is why a terminal event never blocks the work it is describing.
#
# The classification is by *meaning*, not by convenience, and the registered events split cleanly:
#
#   * terminal - carries the result of work that has already happened: `...finished`,
#     `runtime.background_work`/`runtime.background_failed` (one pass really worked, or really
#     failed), `turn.cancelled`, `direct.request.cancelled`, `direct.delivery.deferred`,
#     `service.request.authenticated` (a credential was really accepted) and `outbox.flush`.
#   * not terminal - announces work that has not finished, or reports no result at all:
#     `runtime.started`/`runtime.stopping`, `service.request.started`, `turn.prepared`,
#     `turn.queued`, `turn.generation.started`, `turn.delivery.started`,
#     `direct.request.queued`, `direct.attempt.started`, `direct.delivery.started`.
#
# `runtime.log_probe` is deliberately absent: the maintenance probe already writes its own record
# with `fsync=True`, and it is the one record that is not emitted through this registry.
#
# The cost of a terminal record is one `fsync` on the writer's thread, so the event loop and any
# authoritative transaction never wait for a disk. It is bounded work on a thread that exists to
# do exactly this, which is why the classification follows the contract rather than the write
# rate: an unpersisted completion is the thing this registry exists to prevent.
TERMINAL_EVENTS = frozenset(
    {
        # process and background loops
        "runtime.stopped",
        "runtime.background_work",
        "runtime.background_failed",
        # inbound service requests
        "service.request.authenticated",
        "service.request.finished",
        # outbound calls to already-existing internal peers
        "peer.call.finished",
        # conversation scheduling
        "turn.generation.finished",
        "turn.cancelled",
        "turn.delivery.finished",
        "outbox.flush",
        # explicit functional commands
        "direct.request.cancelled",
        "direct.attempt.finished",
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
        # The log destination itself refused the write. It is its own code because "some
        # dependency is unavailable" would hide which one, and this one is the reason a request
        # was refused before it ran.
        "log_unavailable",
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

# The explicit domain-state table. Keys are the state names the published contracts and the
# product's own durable rows actually use; values are the frozen runtime outcomes. This table
# exists because "sent" is a *business* verdict and "succeeded" is a *runtime* outcome: the
# frozen enumeration is shared, so it is the mapping that adapts, never the enumeration.
DOMAIN_OUTCOMES = {
    # contracts/text-dialogue/v1 send_receipt#state
    "sent": "succeeded",
    "failed": "failed",
    "unknown": "unknown",
    # contracts/text-dialogue/v1 turn#delivery_state and source-sync committed_event
    "not_started": "started",
    "partial": "degraded",
    "not_required": "succeeded",
    # contracts/text-dialogue/v1 cancel_response#state
    "cancelled": "cancelled",
    "partially_cancelled": "degraded",
    "too_late": "rejected",
    # functional command attempts and requests (docs/routing.md)
    "completed": "succeeded",
    "accepted": "succeeded",
    "registered": "succeeded",
    "pending": "started",
    "dispatching": "started",
    "submitted": "started",
    "awaiting_result": "started",
    "awaiting_core": "started",
    "superseded": "rejected",
    "revoked": "rejected",
    "expired": "rejected",
    "ready_to_deliver": "succeeded",
    "deferred": "degraded",
    "not_configured": "rejected",
    "rejected": "rejected",
    "degraded": "degraded",
    "started": "started",
    "succeeded": "succeeded",
}

# The level a runtime outcome is reported at. Derived from the outcome, not from the domain
# state, so one place decides how a verdict is emphasised.
OUTCOME_LEVELS = {
    "started": "DEBUG",
    "succeeded": "INFO",
    "failed": "ERROR",
    "cancelled": "WARNING",
    "unknown": "WARNING",
    "rejected": "WARNING",
    "degraded": "WARNING",
}

DEFAULT_MAX_SEGMENT_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_DIRECTORY_BYTES = 1024 * 1024 * 1024
MIN_DIRECTORY_BYTES = 32 * 1024 * 1024
MAX_DIRECTORY_BYTES = 64 * 1024 * 1024 * 1024

# How many records may wait for the single writer. A bound, not a suggestion: past it the
# adapter refuses new business instead of accumulating unbounded memory or threads, and the
# refusal is reported rather than hidden.
DEFAULT_QUEUE_RECORDS = 4096

# How long shutdown waits for the writer to drain before giving up. Bounded on purpose: a
# process that cannot stop is worse than a process that stops with an incomplete log.
DEFAULT_DRAIN_SECONDS = 5.0

# How often the awaiting side re-checks whether its record reached the disk. Small enough to
# be invisible next to a disk write, large enough that the poll is not a busy loop.
ADMISSION_POLL_SECONDS = 0.001

# How long the owner waits on an empty queue before looking at the stop request again. The stop
# signal must not depend on finding a free queue slot - a closer that arrives while the queue is
# full would then have no way to tell the owner to finish, and the owner would sit in `get` for
# ever. So the owner treats "nothing to write" as a moment to re-read the stop state, and a stop
# request that cannot be delivered as a wake-up is still observed within this bound.
OWNER_POLL_SECONDS = 0.001

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


def runtime_outcome(state):
    """Map a domain state onto the frozen runtime outcome, or None when it is not a state.

    A value that is already a runtime outcome maps to itself, so a caller may pass either
    vocabulary without a second rule. An unknown state returns None, which the caller records
    as `internal_error` - never as a silently dropped success.
    """
    if not isinstance(state, str):
        return None
    return DOMAIN_OUTCOMES.get(state)


def outcome_level(outcome):
    return OUTCOME_LEVELS.get(outcome, "WARNING")


def _nonblocking(stream):
    """Ask a stream or descriptor not to wait for its reader, once, best effort.

    A process whose standard error is a pipe that nobody drains would otherwise block on the
    first write that fills the buffer. This is what the runtime server itself does to its own
    output for the same reason. A destination that cannot be switched - a console, a file, a
    test double - is left exactly as it was, and the caller handles a refusal instead.
    """
    try:
        descriptor = stream if isinstance(stream, int) else stream.fileno()
    except (OSError, ValueError, AttributeError, TypeError, io.UnsupportedOperation):
        return False
    try:
        os.set_blocking(descriptor, False)
    except (OSError, ValueError, TypeError):
        return False
    return True


def _write_line(stream, line):
    """Write one encoded line to a stream or a descriptor, without buffering it.

    Returns whether the whole line was written. A short write is a refusal: a half record in a
    stream that is read line by line would be worse than a missing one.
    """
    if isinstance(stream, int):
        return os.write(stream, line) == len(line)
    if _nonblocking(stream):
        # The raw descriptor, so the interpreter's own text buffer cannot turn a non-blocking
        # write back into a waiting one.
        return os.write(stream.fileno(), line) == len(line)
    stream.write(line.decode("utf-8", "replace"))
    if hasattr(stream, "flush"):
        stream.flush()
    return True


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


class _Queued:
    """One record waiting for the writer: its bytes, its sequence, and its completion flag.

    Completion is published as a plain flag rather than an `asyncio` future. A future would
    have to be resolved from the writer thread, and waking a running event loop from another
    thread is not dependable on every platform this runs on: when the wakeup is missed the
    waiting coroutine never resumes, which turns a slow disk into a hung request. A flag the
    waiting side polls cannot be missed, because the waiter is the one doing the checking.
    """

    __slots__ = ("line", "sequence", "size", "fsync", "durable", "settled", "ok", "stream")

    def __init__(self, line, sequence, *, fsync, durable=False, stream=False):
        self.line = line
        self.sequence = sequence
        self.size = len(line)
        self.fsync = fsync
        # Whether this record must be *persisted*, not merely written, before it counts as
        # settled. The contract requires the terminal event of a request, turn, attempt or
        # delivery to be durable, so those carry it; the writer honours it with one `fsync`
        # on its own thread, which is why the caller never waits for a disk.
        self.durable = durable
        # Which destination this record is for: the segment file, or the diagnostic stream the
        # process was started with when no directory is configured.
        self.stream = stream
        self.settled = False
        self.ok = False

    def settle(self, ok):
        self.ok = ok
        self.settled = True


class LogAdapter:
    """The runtime-event port: bounded, durable, secret-free JSON lines, written off the loop.

    Three verbs, three different promises:

    * `submit(...)` - enqueue and return immediately. Never blocks the event loop and never
      touches a disk. Returns False when the record could not be accepted (queue full, budget
      full, already degraded), and counts it.
    * `emit(...)` - `submit` with the domain-state mapping applied. This is what business code
      calls: it must not wait for a disk and must not raise.
    * `await admit(...)` - enqueue *and wait for a durable write* (`flush` + `fsync`). This is
      the admission gate: a request that cannot be admitted is refused before it acts. It is
      always awaited outside any transaction, so no authoritative transaction ever waits on IO.

    Failure is never silent: `dropped` counts what was not written, `health()` reports it, and
    `accepts_business()` refuses new work while the destination is unusable. Recovery is an
    explicit maintenance act (`probe`), never the side effect of a later lucky event.
    """

    def __init__(
        self,
        directory=None,
        *,
        max_segment_bytes=DEFAULT_MAX_SEGMENT_BYTES,
        max_directory_bytes=DEFAULT_MAX_DIRECTORY_BYTES,
        stderr=None,
        queue_records=DEFAULT_QUEUE_RECORDS,
        drain_seconds=DEFAULT_DRAIN_SECONDS,
        writer=True,
    ):
        if not 1 <= max_segment_bytes <= MAX_DIRECTORY_BYTES:
            raise ValueError("Invalid log segment bound")
        if not MIN_DIRECTORY_BYTES <= max_directory_bytes <= MAX_DIRECTORY_BYTES:
            raise ValueError("Log directory budget must be between 32MiB and 64GiB")
        if queue_records < 1:
            raise ValueError("The writer queue must hold at least one record")
        self.directory = Path(directory) if directory else None
        self.max_segment_bytes = max_segment_bytes
        self.max_directory_bytes = max_directory_bytes
        self.queue_records = queue_records
        self.drain_seconds = drain_seconds
        self.instance_id = str(uuid.uuid4())
        self.service = SERVICE
        self._sequence = 0
        self._state_lock = threading.RLock()
        self._stream = None
        self._segment_index = None
        self._segment_bytes = 0
        self._directory_bytes = 0
        self._pending_bytes = 0
        self._pending = 0
        # The sequences accepted but not yet written. This - not the pending counter - is what
        # `flush` and `admit` treat as the truth about a quiet point: a record that is still in
        # the writer's hands stays here, so it can never be reported as drained. A record the
        # writer could not write leaves the set and is counted as dropped, which `admit` surfaces
        # as a refusal - the caller of a durable admission is never told a lost record landed.
        self._outstanding = set()
        self._stderr = stderr
        self.io_failed = False
        self.dropped = 0
        self.capacity_exhausted = False
        self.queue_full = False
        self.wrote_once = False
        self._warned = False
        self.probe_pending = False
        self.closing = False
        self.closed = False
        self._shutdown_started = False
        self._shutdown_confirmed = False
        self._writer = None
        self._queue = None
        self._stop = None
        # The real capacity baseline, measured here - before anything can be admitted - rather
        # than discovered by the writer on its first append. Measuring late is not a small
        # ordering detail: `submit` decides capacity from the directory usage, so an unmeasured
        # directory reads as empty and the first admission of a *full* directory succeeds,
        # putting the file over the budget it declares. A restart must not be able to buy
        # capacity that the running process was refused.
        if self.durable:
            self._measure()
        if self.durable and writer:
            self._queue = queue.Queue(maxsize=queue_records)
            self._stop = threading.Event()
            self._writer = threading.Thread(
                target=self._drain, name="tianshu-log-writer", daemon=True
            )
            self._writer.start()
        elif writer:
            # The diagnostic channel is written by its own thread for the same reason the
            # segment file is: `emit` runs on the event loop, and a stream that is not being
            # read - a captured standard error whose pipe has filled - would otherwise park the
            # whole application inside a write. The queue is bounded here too, so a stream that
            # nobody reads costs records, never latency.
            self._queue = queue.Queue(maxsize=queue_records)
            self._stop = threading.Event()
            self._writer = threading.Thread(
                target=self._drain_stream, name="tianshu-log-stream", daemon=True
            )
            self._writer.start()

    # ------------------------------------------------------------------ properties

    @property
    def durable(self):
        return self.directory is not None

    @property
    def degraded(self):
        return self.io_failed or self.capacity_exhausted or self.queue_full

    @property
    def log_dir(self):
        return str(self.directory) if self.directory else None

    @property
    def pending(self):
        """Records accepted but not yet written. Bounded by `queue_records`."""
        return self._pending

    def health(self):
        """Closed status, no path and no environment value beyond the configured names."""
        if not self.durable:
            return "non_durable"
        if self.capacity_exhausted:
            return "capacity_exhausted"
        if self.io_failed:
            return "log_unavailable"
        if self.queue_full:
            return "writer_saturated"
        return "ok"

    def accepts_business(self):
        """May the host admit new business work?

        Not while the destination is unusable: a full budget, a failed writer, or a saturated
        queue all mean the event stream that is supposed to account for the work cannot be
        kept, so the work is refused rather than performed unlogged.
        """
        return not (self.durable and self.degraded)

    # ---------------------------------------------------------------------- output

    def submit(
        self,
        event,
        outcome,
        *,
        level=None,
        correlation_id=None,
        error_code=None,
        duration_ms=None,
        fsync=False,
    ):
        """Enqueue one record without touching a disk.

        Returns the queued record on acceptance and `None` on refusal, so a caller that needs to
        wait for the write (`admit`) can do so without a second lookup. A caller that only needs
        to know whether the event was accepted uses `emit`.
        """
        if event not in EVENTS:
            # A programming error, not a data error: refuse to invent an event name.
            raise ValueError("Unregistered runtime event name")
        resolved = runtime_outcome(outcome)
        with self._state_lock:
            if resolved is None:
                # A domain state with no mapping is a defect in the caller's vocabulary. It is
                # counted and reported as such - never written as a success, never ignored.
                self.dropped += 1
                return None
            record = {
                "schema_version": SCHEMA_VERSION,
                "timestamp": timestamp(),
                "service": SERVICE,
                "instance_id": self.instance_id,
                "sequence": self._sequence + 1,
                "event_id": str(uuid.uuid4()),
                "level": level or outcome_level(resolved),
                "event": event,
                "outcome": resolved,
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
                return None
            if not self.durable:
                # Development fallback: an explicit, honest non-durable channel. The record is
                # handed to the diagnostic writer thread, exactly like a segment write, so the
                # caller never waits for a stream that may not be read.
                #
                # A stream nobody reads is not a reason to refuse business. This channel is a
                # fallback for a process with no configured directory, its health is already
                # `non_durable` (so it can never make the process ready), and its guarantee was
                # never durability. A saturated stream therefore costs records - counted in
                # `dropped`, never hidden - instead of turning an unread pipe into a service
                # outage. The durable channel keeps the strict rule: there, a record that cannot
                # be kept refuses the work.
                if self.closing or self.closed:
                    self.dropped += 1
                    return None
                item = _Queued(line, self._sequence + 1, fsync=False, stream=True)
                # Counted before it is queued: the writer may consume the record the instant it
                # is enqueued, and a pending count that briefly reads zero would let `flush`
                # report a quiet point while this record is still unwritten. `_outstanding` is
                # the same promise kept exactly: a sequence leaves it only once the writer has
                # really written the record, so a failed append can never look like a quiet point.
                self._sequence += 1
                self._pending += 1
                self._pending_bytes += item.size
                self._outstanding.add(item.sequence)
                try:
                    self._queue.put_nowait(item)
                except queue.Full:
                    self._pending -= 1
                    self._pending_bytes -= item.size
                    self._outstanding.discard(item.sequence)
                    self.dropped += 1
                    return None
                return item
            if self.closing or self.closed or self.degraded:
                self.dropped += 1
                return None
            size = len(line)
            if self._directory_bytes + self._pending_bytes + size > self.max_directory_bytes:
                # The budget is checked against the *measured* directory usage plus records still
                # waiting for the writer, so the file can never grow past the bound it declares -
                # including the first admission after a restart into an already-full directory.
                self.capacity_exhausted = True
                self._warn_once("log capacity reached; new business is refused")
                self.dropped += 1
                return None
            item = _Queued(line, self._sequence + 1, fsync=fsync, durable=event in TERMINAL_EVENTS)
            self._sequence += 1
            self._pending += 1
            self._pending_bytes += size
            self._outstanding.add(item.sequence)
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                self._pending -= 1
                self._pending_bytes -= size
                self._outstanding.discard(item.sequence)
                self.queue_full = True
                self._warn_once("runtime event writer is saturated; new business is refused")
                self.dropped += 1
                return None
            return item

    def emit(self, event, outcome, **fields):
        """Report one event from synchronous or transaction-holding code.

        Returns whether the event was accepted. A refusal is counted, never raised: a log
        failure must not become a business failure. An *unregistered event name* is different
        in kind - it is a defect in the caller, not a condition of the destination - so it
        raises `ValueError` exactly as the record encoder does, and is never silently counted as
        a dropped event.
        """
        if event not in EVENTS:
            raise ValueError("Unregistered runtime event name")
        try:
            return self.submit(event, outcome, **fields) is not None
        except Exception:
            with self._state_lock:
                self.dropped += 1
            return False

    async def admit(self, event, outcome, **fields):
        """Enqueue one record and wait until it has really been written. The admission gate.

        On the durable channel this returns True only after the segment file has confirmed the
        record with a `flush` + `fsync`. A False answer means the caller must not proceed: the
        event that was supposed to account for the work does not exist.

        The non-durable channel has no disk to confirm anything with, and it is never a reason to
        refuse business: its records are written by the diagnostic writer thread, and a record
        that stream could not take is counted in `dropped`. So there, admission means the event
        was handed to the destination - not that it survived.

        The wait is a bounded poll of a plain flag the writer publishes, not an `asyncio`
        future resolved from the writer thread. Waking a running event loop from another thread
        is not dependable everywhere this runs; when that wakeup is missed the coroutine never
        resumes, so a slow disk would hang the request instead of failing it. Polling cannot
        miss the completion, and the poll yields to the loop between checks, so nothing else is
        blocked while the disk is busy.
        """
        if event not in EVENTS:
            raise ValueError("Unregistered runtime event name")
        if not self.durable:
            return bool(self.emit(event, outcome, **fields))
        try:
            item = self.submit(event, outcome, fsync=True, **fields)
        except Exception:
            with self._state_lock:
                self.dropped += 1
            return False
        if item is None:
            return False
        deadline = time.monotonic() + self.drain_seconds
        while not item.settled:
            if time.monotonic() > deadline:
                # A writer that has not answered within the bound is not a durable admission.
                with self._state_lock:
                    self.dropped += 1
                return False
            await asyncio.sleep(ADMISSION_POLL_SECONDS)
        if not item.ok:
            return False
        # A write that landed while the destination is already known to be unusable is still
        # not an admission.
        return self.accepts_business()

    def flush(self, timeout=None):
        """Wait until every record already accepted has really been written, within a bound.

        This is the synchronous sibling of `admit`'s wait: it never blocks the writer and never
        takes the state lock while waiting, so a caller may use it to reach a quiet point (a
        test asserting on the file, an operator draining before an action) without racing the
        writer. It applies to both destinations, because the diagnostic stream is written by a
        thread of its own too.

        A quiet point means *written*, not *attempted*: a record the writer could not persist -
        because the disk failed, the directory is out of budget, or the append raised - is
        counted as dropped and never reported as drained. Returns whether every accepted record
        landed inside the bound.
        """
        if self._writer is None:
            # No writer thread at all: nothing is ever queued, so there is nothing to drain.
            return True
        bound = self.drain_seconds if timeout is None else timeout
        deadline = time.monotonic() + bound
        while True:
            if self.settled_quietly():
                return True
            if time.monotonic() > deadline:
                return False
            time.sleep(ADMISSION_POLL_SECONDS)

    def settled_quietly(self):
        """Whether every record this adapter accepted has left the writer's hands.

        A quiet point is about the queue, not about success: a record whose append failed has been
        settled - and counted as dropped, and surfaced to the caller by `admit` - so it is not
        still queued. What must never be reported as quiet is a record that is *still* waiting,
        which is why the pending count is published before the record is enqueued and cleared only
        after the writer has finished with it.
        """
        with self._state_lock:
            return not self._outstanding

    async def await_flush(self, timeout=None):
        """`flush` without blocking the event loop while the writer drains."""
        if self._writer is None:
            return True
        bound = self.drain_seconds if timeout is None else timeout
        deadline = time.monotonic() + bound
        while True:
            if self.settled_quietly():
                return True
            if time.monotonic() > deadline:
                return False
            await asyncio.sleep(ADMISSION_POLL_SECONDS)

    # ----------------------------------------------------------------------- writer

    def _next_item(self):
        """The owner's one wait: the next record, or a reason to stop.

        Two independent ways out, because the stop signal must not depend on the queue having a
        free slot. A stop request that arrives while the queue is full cannot be enqueued as a
        sentinel at all, so the owner also re-reads the stop state whenever the queue looks empty.
        Without that, a shutdown that timed out against a full queue left the owner parked in
        `queue.get` for ever: the records it still held were written, `pending` reached zero, and
        the thread - the only thing that can release the handle - never left.

        Returns the next record, or None when this owner should finish.
        """
        while True:
            if self._stop.is_set():
                # Records accepted before the stop request are still drained: the stop state only
                # stops *new* admissions, so anything already in the queue is handled here first,
                # in order, before the owner leaves.
                try:
                    return self._queue.get_nowait()
                except queue.Empty:
                    return None
            try:
                return self._queue.get(timeout=OWNER_POLL_SECONDS)
            except queue.Empty:
                continue

    def _drain(self):
        """The only place that touches the file. One thread, one handle, no sharing.

        This thread also owns the *durability* of what it writes. A terminal record - the end of
        a request, background pass, peer call, turn, attempt, delivery or of the process - is
        followed by `flush` + `fsync` before it is acknowledged, because the contract asks for a
        persisted terminal event and a successful `write` only means the kernel took the bytes.
        The cost is paid here, on the writer, never by the event loop or by an authoritative
        transaction.
        """
        while True:
            item = self._next_item()
            try:
                if item is None:
                    return
                ok = self._write(item.line, fsync=item.fsync or item.durable)
                with self._state_lock:
                    self._pending -= 1
                    self._pending_bytes -= item.size
                    if ok:
                        self.wrote_once = True
                    else:
                        self.dropped += 1
                    self._outstanding.discard(item.sequence)
                item.settle(ok)
            except Exception:  # noqa: BLE001 - the writer must outlive any single bad record
                # One record that fails unexpectedly must not take the writer with it: a dead
                # writer turns every later admission into a silent drop, which is exactly the
                # failure mode this adapter exists to prevent. The record is counted and the
                # thread carries on, so the next append can still succeed.
                with self._state_lock:
                    self._pending = max(0, self._pending - 1)
                    self._pending_bytes = max(0, self._pending_bytes - item.size)
                    self.dropped += 1
                    self.io_failed = True
                    self._outstanding.discard(item.sequence)
                item.settle(False)
            finally:
                # Only a record that was actually taken from the queue is accounted for. The stop
                # path can also return "nothing to do", and calling `task_done` for a `get` that
                # never happened raises inside the owner thread - which would kill the one thread
                # allowed to release the handle.
                if item is not None:
                    self._queue.task_done()
                else:
                    # The owner releases its own handle on the way out, so a shutdown that timed
                    # out can never close a file this thread is still writing to.
                    self._release_stream()

    def _drain_stream(self):
        """The only place that touches the diagnostic stream, for the same reason as `_drain`.

        One thread owns the stream, so the event loop never waits for a reader that may have
        stopped reading, and the completion flag the admission gate polls is published here.
        """
        while True:
            item = self._next_item()
            try:
                if item is None:
                    return
                ok = self._to_stderr(item.line)
                with self._state_lock:
                    self._pending -= 1
                    self._pending_bytes -= item.size
                    self._outstanding.discard(item.sequence)
                    if ok:
                        self.wrote_once = True
                    else:
                        self.dropped += 1
                item.settle(ok)
            finally:
                if item is not None:
                    self._queue.task_done()

    def _write(self, line, *, fsync):
        """Append one line. Runs on the writer thread only; never on the event loop."""
        try:
            self._open()
            size = len(line)
            # The same budget `submit` enforces, re-checked here against what the file really
            # holds. `submit` cannot be the last word on capacity: between the measurement and
            # this append another instance, a rotation or a recovered segment can have taken the
            # space, and a declared bound that the file can step over is not a bound. The
            # reserved bytes of everything still queued are already counted, so a healthy
            # directory never fails this check.
            if self._directory_bytes + size > self.max_directory_bytes:
                self.capacity_exhausted = True
                self._warn_once("log capacity reached; new business is refused")
                return False
            if self._segment_bytes + size > self.max_segment_bytes:
                # Sealing a segment costs no extra bytes, so rotation is always allowed; the
                # directory bound was checked immediately above.
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

    def _segment_path(self, index):
        name = "companion.jsonl" if index == 0 else f"companion.jsonl.{index}"
        return self.directory / name

    def _scan_directory(self):
        """Total bytes of this instance's segments, and the highest segment index present.

        `_SEGMENT_SUFFIX` describes the part *after* the `companion` prefix, so it is matched
        against that part: matching it against the whole name would classify every segment as
        foreign and report an occupied directory as empty - the exact mistake that let the first
        admission of a full directory through.
        """
        total, highest = 0, -1
        for entry in self.directory.iterdir():
            if not entry.is_file() or not entry.name.startswith("companion"):
                continue
            match = _SEGMENT_SUFFIX.match(entry.name[len("companion") :])
            if match is None:
                continue
            total += entry.stat().st_size
            index = int(match.group(1)) if match.group(1) else 0
            highest = max(highest, index)
        return total, highest

    def _measure(self):
        """Establish the real capacity baseline for an existing directory, once, off the loop.

        Called from the constructor (startup, never a request path) and from the writer when it
        has to open the destination. A directory that cannot be read is not assumed empty: the
        adapter refuses business until an explicit `probe` proves a real write, because "I could
        not measure the budget" must never be answered with "then write anyway".

        It deliberately does not *create* the directory. A directory that does not exist yet holds
        nothing, so there is nothing to measure and nothing to report - and creating it here would
        make every construction of the adapter, including the one a read-only readiness probe
        performs, leave a directory behind on a filesystem the probe was told not to touch.
        """
        if not self.directory.exists():
            with self._state_lock:
                self._directory_bytes = 0
                self._segment_index = 0
            return
        try:
            total, highest = self._scan_directory()
        except OSError:
            self.io_failed = True
            self._warn_once("runtime event log directory is unavailable")
            return
        with self._state_lock:
            self._directory_bytes = total
            self._segment_index = highest + 1

    def _open(self):
        if self._stream is not None:
            return
        self._measure()
        self.directory.mkdir(parents=True, exist_ok=True)
        self._open_segment()

    def _open_segment(self):
        path = self._segment_path(self._segment_index)
        self._stream = open(path, "ab", buffering=0)
        self._segment_bytes = path.stat().st_size

    def _release_stream(self):
        """Give up the segment handle, unless another thread may still be using it.

        The writer owns the handle, so this is the writer's own exit path - either the writer
        calling it as it leaves, or a closer that has already watched it stop. A close that timed
        out while the writer was still inside a write must not reach in and close the handle under
        it: that is how a shutdown becomes a write into a closed file, or a second writer. The
        handle then stays open and the owner releases it when it finishes - the loss is already
        counted, and a leaked descriptor at process exit is the smaller problem by far.

        The close itself happens *outside* the state lock. Closing a file is IO: a close that a
        slow filesystem parks (a full disk, a network-backed mount, an antivirus filter) would
        otherwise hold the lock that every reader of `health()`, `pending` and `settled_quietly()`
        needs - including the closer measuring its own deadline. That is how a 30 ms shutdown
        budget turned into 250 ms: the deadline was fine, but the thread checking it could not
        reach the state it needed to check. The lock here only ever swaps a reference.

        The handle is dropped from the adapter *before* the close is attempted, so a second caller
        can never close the same descriptor twice. The owner thread reference is dropped only after
        the close returns: while that close is still in progress the owner is very much alive, and
        a closer that saw otherwise would report a shutdown that has not happened - and could try
        to release the same handle a second time.
        """
        with self._state_lock:
            if self._stream is None:
                return
            owner = self._writer
            if owner is not None and owner is not threading.current_thread() and owner.is_alive():
                return
            stream = self._stream
            self._stream = None
        try:
            stream.close()
        except OSError:
            pass
        with self._state_lock:
            # This path is only reached once no writer can still be inside the handle - either
            # the owner is calling it on its way out, or the owner has already stopped - so the
            # reference to that finished thread is cleared here rather than left dangling.
            self._writer = None

    def _warn_once(self, text):
        if self._warned:
            return
        self._warned = True
        self._to_stderr(("tianshu-companion runtime log: " + text + "\n").encode("utf-8"))

    def _to_stderr(self, line):
        """Write one diagnostic line without ever waiting for the reader.

        A deployment that captures the process's standard error through a pipe stops reading it
        as soon as the pipe is full. A blocking write there would park whoever called `emit`,
        which is usually the event loop - no request, no health probe, no background pass would
        run again until someone drained that pipe. The write is therefore made non-blocking and
        a line that cannot be written right now is reported as not written: a diagnostic channel
        is not a reason to stop the process it is describing.

        Returns whether the line reached the stream.
        """
        stream = self._stderr
        if stream is None:
            import sys

            stream = sys.stderr
        try:
            return _write_line(stream, line)
        except (OSError, ValueError, AttributeError):
            # Includes the full-pipe case and a destination that refuses non-blocking mode.
            return False

    # ------------------------------------------------------------------ maintenance

    def probe(self):
        """Explicit maintenance action: prove a real write before trusting the log again.

        Never called by the health endpoints or by any request path, so a probe never writes a
        log record on its own account. Returns True only after a genuinely successful write;
        until then the degraded state - and the `not_ready` verdict it forces - stands.
        """
        with self._state_lock:
            if not self.durable or not self.degraded:
                return not self.degraded
            self.probe_pending = True
            was_io_failed, was_capacity, was_full = (
                self.io_failed,
                self.capacity_exhausted,
                self.queue_full,
            )
            self.io_failed = self.capacity_exhausted = self.queue_full = False
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
            except (ValueError, UnicodeError):
                self.io_failed, self.capacity_exhausted, self.queue_full = (
                    was_io_failed,
                    was_capacity,
                    was_full,
                )
                self.probe_pending = False
                return False
            item = _Queued(line, self._sequence + 1, fsync=True)
            # Counted and marked outstanding *before* it is queued, for the same reason as
            # `submit`: the writer can consume the record the moment it appears.
            self._sequence += 1
            self._pending += 1
            self._pending_bytes += item.size
            self._outstanding.add(item.sequence)
            try:
                self._queue.put(item, timeout=self.drain_seconds)
            except queue.Full:
                self._pending -= 1
                self._pending_bytes -= item.size
                self._outstanding.discard(item.sequence)
                self.io_failed, self.capacity_exhausted, self.queue_full = (
                    was_io_failed,
                    was_capacity,
                    was_full,
                )
                self.probe_pending = False
                return False
        # Waiting for the writer outside the state lock, so a slow disk never blocks a reader
        # of `health()` - the maintenance action may wait, the health endpoint may not. The
        # completion flag is polled rather than awaited, for the same reason `admit` polls it.
        deadline = time.monotonic() + self.drain_seconds
        while not item.settled and time.monotonic() < deadline:
            time.sleep(ADMISSION_POLL_SECONDS)
        with self._state_lock:
            self.probe_pending = False
            if item.settled and item.ok:
                self._warned = False
                return True
            self.io_failed, self.capacity_exhausted, self.queue_full = (
                was_io_failed,
                was_capacity,
                was_full,
            )
            return False

    # ---------------------------------------------------------------------- closing

    def close(self, *, drain=None):
        """Stop the writer and release the handle, inside one bound, from the owner's side.

        Returns whether the shutdown was *confirmed*: every accepted record settled and the
        writer thread finished. A False answer is a report, not a clean-up: the records still in
        flight stay counted, the handle stays with the writer that owns it, and the caller learns
        that the log was not brought to a stop rather than being told a comfortable story.
        """
        return self._shutdown(self.drain_seconds if drain is None else drain)

    async def aclose(self, timeout=None):
        """`close` without blocking the event loop while the writer drains.

        The whole shutdown - waiting for the queue, the stop request, the join and the final
        release - runs as one bounded unit on one thread. The event loop only waits for it, so a
        cancelled caller cannot leave a second closer behind: cancelling this coroutine stops the
        *wait*, and the thread it started still finishes the one shutdown that was begun.

        "The thread it started" is why the work is submitted here rather than through
        `run_in_executor`: that call only *schedules* the hand-off onto the loop, so a caller
        cancelled before the loop gets around to it would cancel the shutdown instead of just its
        own wait, and the writer would keep running with a handle nobody will release. Submitting
        the work synchronously closes that window - by the time this coroutine can be cancelled,
        the shutdown is already committed to the executor.

        A repeated call is answered from the state as it is *now*, never from a cached verdict: an
        earlier close that timed out reports `False` at that moment, but once the owner has
        finished the log really is stopped, and replaying the old `False` would tell a later caller
        that a shutdown failed when it has since completed.
        """
        bound = self.drain_seconds if timeout is None else timeout
        loop = asyncio.get_running_loop()
        pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tianshu-log-close")
        pending = loop.run_in_executor(pool, self._shutdown, bound)
        pending.add_done_callback(lambda _: pool.shutdown(wait=False))
        try:
            return await pending
        except asyncio.CancelledError:
            # The caller stopped waiting; the shutdown does not. The executor is deliberately not
            # shut down here: its one worker is inside that shutdown and will exit when it is done.
            raise

    def _shutdown(self, bound):
        """One deadline covering the whole stop: queue, stop request, join and release.

        The deadline is only worth anything if the closer can *reach* the state it needs to check
        while the bound runs out, so no step here waits on a lock that a slow IO call can hold:
        the owner's handle release closes the file outside the state lock, and this method only
        takes that lock for a reference swap or a counter read.
        """
        with self._state_lock:
            if self._shutdown_started:
                # One closer, one shutdown: a second caller must not start a competing stop. It is
                # answered immediately from the live state rather than waiting behind the first
                # caller's deadline - its own budget is its own - and never from the stale verdict
                # the first caller recorded: once the owner has finished, the log really is
                # stopped, so the recomputed answer is True.
                return not self._writer_alive() and not self._outstanding
            self._shutdown_started = True
            self.closing = True
        deadline = time.monotonic() + bound
        # The stop request comes first, and it is a piece of state rather than a queue entry: a
        # sentinel cannot be delivered into a full queue, so a shutdown must never depend on
        # finding a free slot. Records accepted before this point are still drained - the owner
        # checks the queue before it checks the stop state - and nothing new is accepted after it.
        self._stop.set()
        # Everything already accepted is given its chance to land before the writer is stopped: a
        # record that was admitted must not be discarded merely because shutdown arrived.
        self.flush(max(0.0, deadline - time.monotonic()))
        if self._writer is not None and self._writer.is_alive():
            # A sentinel is a latency optimisation, not the signal: the owner observes the stop
            # state on its own, so a sentinel that does not fit costs at most one poll interval.
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                pass
            self._writer.join(timeout=max(0.0, deadline - time.monotonic()))
        # Both halves are read *after* the join: the owner may have finished inside it, and a
        # verdict computed before would report a failure that has already been resolved.
        stopped = not self._writer_alive()
        confirmed = stopped and self.settled_quietly()
        with self._state_lock:
            if self._outstanding and stopped:
                # The owner is gone and these records never reached the file: reported, never
                # hidden - and never zeroed to make the report look clean. While the owner is
                # still running they are in flight, not lost, so they are not counted here and
                # the count is not reset either.
                self.dropped += len(self._outstanding)
                self._outstanding.clear()
            self._shutdown_confirmed = confirmed
            self.closed = True
        # Release the handle only from a position where no writer can still be inside it.
        if stopped:
            self._release_stream()
        return confirmed

    def _writer_alive(self):
        return self._writer is not None and self._writer.is_alive()

    def _join_writer(self, bound):
        """Wait for the writer to finish, within a bound, without taking anything from it.

        Kept as the narrow "wait" half of shutdown so callers that only need a quiet point do not
        have to close the adapter to get one.
        """
        deadline = time.monotonic() + bound
        while not self.settled_quietly() and time.monotonic() <= deadline:
            time.sleep(0.005)
        return self.settled_quietly()


class NullLogAdapter:
    """The port with no destination, for tests and for callers that inject nothing."""

    instance_id = ""
    service = SERVICE
    durable = False
    pending = 0

    def emit(self, *args, **kwargs):
        return False

    def submit(self, *args, **kwargs):
        return False

    async def admit(self, *args, **kwargs):
        return True

    async def flush(self, timeout=None):
        return True

    def health(self):
        return "non_durable"

    def accepts_business(self):
        return True

    def probe(self):
        return True

    def close(self, **kwargs):
        return None

    async def aclose(self, **kwargs):
        return True


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


async def admit(port, event, outcome, **fields):
    """Wait for a durable admission record through an injected port, or refuse.

    This is the one place that may wait for a disk, and it is always awaited outside any
    transaction. A port that cannot answer refuses: the caller must not act.
    """
    if port is None:
        return True
    if fields.get("correlation_id") is None:
        fields["correlation_id"] = current_correlation_id()
    try:
        return bool(await port.admit(event, outcome, **fields))
    except Exception:
        return False


def build_log_adapter(directory=None, **options):
    """Assemble the port from an explicitly configured directory, or the stderr fallback."""
    if directory is None:
        return LogAdapter(None)
    return LogAdapter(directory, **options)
