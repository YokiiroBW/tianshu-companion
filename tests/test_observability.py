"""Runtime event logging: the closed record, the static registry, durability and secrets.

Every assertion here is about the published `contracts/diagnostics/v1` document, not about a
private convention: the record carries exactly the frozen field set, one line at most 4096
bytes, an event name from the product's own registry and an outcome from the frozen
enumeration. The contract's own positive example must validate and its four negative examples
must each fail, which is how this suite proves the validator it uses is strict enough to mean
anything.
"""

import asyncio
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from support import Harness
from tianshu_companion import observability as obs
from tianshu_companion.app import create_app, log_adapter_from_environment, run_loop
from tianshu_companion.clients import JsonService

RECORD_FIELDS = set(obs.RECORD_FIELDS)
ENVIRONMENT_LOG_DIR = obs.ENVIRONMENT_LOG_DIR

CORRELATION_HEADER = obs.CORRELATION_HEADER


class _LogDirectory:
    """A real temporary log directory, configured the way a deployment configures one.

    `TIANSHU_LOG_DIR` is the one documented switch for file output, so the suite sets it
    rather than reaching into the factory: `create_app` then assembles exactly the adapter a
    deployment gets, and the records are read back from the files it really wrote.
    """

    def __init__(self, case):
        self.case = case
        self.holder = tempfile.TemporaryDirectory(prefix="tianshu-log-")
        self.path = self.holder.name
        self.adapter = None
        case.addCleanup(self._release)
        patcher = mock.patch.dict(os.environ, {ENVIRONMENT_LOG_DIR: self.path})
        patcher.start()
        case.addCleanup(patcher.stop)

    def _release(self):
        if self.adapter is not None:
            self.adapter.close()

    def attach(self, app):
        """Take the adapter the application assembled, so the test closes its descriptor."""
        self.adapter = app.state.log
        return app

    def records(self):
        return [
            json.loads(line)
            for path in sorted(Path(self.path).iterdir())
            for line in path.read_bytes().splitlines()
            if line.strip()
        ]

    def bytes(self):
        return [path.read_bytes() for path in sorted(Path(self.path).iterdir())]

    def snapshot(self):
        """Every byte under the directory, for a before/after read-only comparison."""
        return {
            path.name: path.read_bytes()
            for path in sorted(Path(self.path).iterdir())
            if path.is_file()
        }


def contract_examples():
    """The published positive/negative examples, if the frozen contract is reachable."""
    root = os.environ.get("TIANSHU_CONTRACTS")
    candidates = []
    if root:
        candidates.append(Path(root) / "diagnostics" / "v1")
    context = Path(__file__).parents[1] / ".runtime/workspace-context.json"
    if context.is_file():
        workspace = json.loads(context.read_text(encoding="utf-8"))["workspace"]
        candidates.append(Path(workspace) / "contracts/diagnostics/v1")
    for directory in candidates:
        if (directory / "examples.json").is_file():
            return directory
    return None


def _validator(document):
    """The frozen schema, compiled by the project's own pinned validator library.

    A hand-written subset reader would only prove that my own reading of the contract is
    self-consistent; using `jsonschema` (already an exact pinned dependency) proves the record
    satisfies the published document itself.
    """
    import jsonschema

    return jsonschema.Draft202012Validator(document)


def validate_record(record, schema):
    """Return a problem string, or None when the record satisfies the frozen schema."""
    validator = _validator(schema) if not hasattr(schema, "iter_errors") else schema
    problems = sorted(validator.iter_errors(record), key=lambda error: list(error.absolute_path))
    if not problems:
        return None
    first = problems[0]
    location = ".".join(str(part) for part in first.absolute_path)
    return (location + ":" if location else "") + first.validator


def _sample():
    """The frozen contract's own example record, as a mutable copy."""
    return {
        "schema_version": "1.0.0",
        "timestamp": "2026-09-21T00:00:00Z",
        "service": "companion",
        "instance_id": "00000000-0000-4000-8000-000000000001",
        "sequence": 1,
        "event_id": "00000000-0000-4000-8000-000000000002",
        "level": "INFO",
        "event": "runtime.started",
        "outcome": "succeeded",
        "correlation_id": None,
        "duration_ms": None,
        "error_code": None,
    }


class RecordTests(unittest.TestCase):
    """The record itself: closed fields, registered names, strict bounds."""

    @classmethod
    def setUpClass(cls):
        directory = contract_examples()
        if directory is None:
            raise unittest.SkipTest("published contracts/diagnostics/v1 is not reachable")
        schema = json.loads((directory / "event.schema.json").read_text(encoding="utf-8"))
        cls.document = schema
        cls.examples = json.loads((directory / "examples.json").read_text(encoding="utf-8"))
        cls.negative = json.loads(
            (directory / "negative-examples.json").read_text(encoding="utf-8")
        )

    def test_published_example_validates_and_every_negative_example_fails(self):
        """The validator used below must accept the contract's example and reject its four."""
        self.assertIsNone(validate_record(self.examples[0], self.document))
        seen = set()
        for example in self.negative:
            problem = validate_record(example, self.document)
            self.assertIsNotNone(problem, example)
            seen.add(problem.split(":")[-1])
        # One extra property, one out-of-range sequence, one wrong schema-version value and
        # one correlation ID that is a user-derived string rather than 32 hex characters.
        self.assertEqual({"additionalProperties", "minimum", "const", "pattern"}, seen)

    def test_every_emitted_record_satisfies_the_frozen_schema(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        for event in sorted(obs.EVENTS):
            adapter.emit(event, "started")
        for outcome in obs.OUTCOMES:
            adapter.emit("runtime.background_work", outcome)
        # The diagnostic stream is written by the adapter's own thread, exactly like a segment
        # file, so reaching a quiet point is what makes the sink readable - and it also proves
        # every one of these records was accepted.
        self.assertTrue(adapter.flush())
        records = sink.lines
        self.assertEqual(len(obs.EVENTS) + len(obs.OUTCOMES), len(records))
        for record in records:
            self.assertIsNone(validate_record(record, self.document), record)
        self.assertEqual([*range(1, len(records) + 1)], [record["sequence"] for record in records])

    def test_record_is_closed_and_bounded(self):
        """Exactly the frozen fields, one LF-terminated line, at most 4096 bytes."""
        base = _sample()
        encoded = obs.encode_record(dict(base))
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertEqual(RECORD_FIELDS, set(json.loads(encoded)))
        for broken in (
            {**base, "message": "secret"},
            {**base, "extras": {"a": 1}},
            {**base, "stack": "trace"},
            {key: value for key, value in base.items() if key != "event_id"},
            {**base, "event": "user.chosen.name"},
            {**base, "level": "TRACE"},
            {**base, "outcome": "maybe"},
            {**base, "sequence": 0},
            {**base, "correlation_id": "user-secret"},
            {**base, "error_code": "Not_A_Code"},
            {**base, "error_code": "message_with_uppercase"},
            {**base, "duration_ms": -1},
            {**base, "schema_version": 1},
            {**base, "service": "other"},
        ):
            with self.assertRaises(ValueError, msg=str(broken)[:120]):
                obs.encode_record(broken)

    def test_a_record_over_the_contract_line_is_refused_not_truncated(self):
        """The 4096-byte ceiling is a refusal, never a silent cut that loses the tail."""
        # The one field the contract leaves open-ended is the sequence number, so an absurd
        # one is how a line can grow past the ceiling. The digits stay under CPython's own
        # integer-conversion limit, so the record really is built before it is refused.
        huge = int("9" * 4200)
        record = {**_sample(), "sequence": huge}
        self.assertGreater(len(json.dumps(record).encode()) + 1, obs.MAX_RECORD_BYTES)
        with self.assertRaises(ValueError):
            obs.encode_record(record)
        # And the port refuses it rather than writing a truncated line.
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        adapter._sequence = huge
        self.assertFalse(adapter.emit("runtime.background_work", "succeeded"))
        self.assertEqual(1, adapter.dropped)
        self.assertEqual([], sink.lines)

    def test_unregistered_event_name_is_refused_not_written(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        with self.assertRaises(ValueError):
            adapter.emit("some.arbitrary.name", "started")
        # The admission path refuses the same way: an event name is never invented for it
        # either, and a refusal must not be mistaken for a dropped record.
        with self.assertRaises(ValueError):
            asyncio.run(adapter.admit("some.arbitrary.name", "started"))
        self.assertEqual([], sink.lines)

    def test_static_error_code_registry_replaces_unknown_codes(self):
        """A code from an exception message must never reach the file as an error code."""
        self.assertEqual("internal_error", obs.error_code("Traceback secret"))
        self.assertEqual("timeout", obs.error_code("timeout"))
        self.assertEqual("internal_error", obs.error_code(None))
        for code in obs.ERROR_CODES:
            self.assertRegex(code, obs.ERROR_RE)

    def test_failure_classification_uses_only_the_exception_type(self):
        class Token:
            pass

        self.assertEqual("timeout", obs.failure_class(TimeoutError("secret-in-message")))
        self.assertEqual("os_error", obs.failure_class(OSError("secret-in-message")))
        self.assertEqual("internal_error", obs.failure_class(Token()))
        self.assertNotIn("secret", obs.failure_class(OSError("secret-in-message")))
        # Whatever the classifier can name must also be a registered code, otherwise a real
        # failure would be filed under `internal_error` and its cause lost.
        self.assertLessEqual(set(obs.FAILURE_CLASSES.values()), set(obs.ERROR_CODES))

    def test_events_and_outcomes_are_the_registered_static_enumerations(self):
        for event in obs.EVENTS:
            self.assertRegex(event, obs.EVENT_RE)
        self.assertEqual(("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), obs.LEVELS)
        self.assertIn("unknown", obs.OUTCOMES)

    def test_every_registered_event_has_exactly_one_emission_site(self):
        """Registration and emission must agree, in the product itself, not in a rehearsal."""
        sources = "\n".join(
            (root / "src/tianshu_companion" / name).read_text(encoding="utf-8")
            for name in ("observability.py", "app.py", "clients.py", "core.py", "direct.py")
            for root in [Path(__file__).resolve().parents[1]]
        )
        import re

        emitted = set(
            re.findall(r'"((?:runtime|service|peer|turn|outbox|direct)\.[a-z_.]+)"', sources)
        )
        emitted -= set(obs.PEER_LABELS)
        # Loop names such as `direct.work` are label strings passed to `run_loop`, not events.
        emitted -= {"direct.work"}
        # The terminal-event registry names which records must be persisted, so each of its names
        # appears once as a declaration and once at the site that emits it. The declaration is not
        # an emission site, and it is excluded here by counting how often each name occurs: a name
        # that appears only inside the declaration block has no emission site, which is exactly
        # what the check below must keep refusing. The names themselves are read from the loaded
        # registry, not from the source, so this can never quietly excuse a missing emitter.
        for name in obs.TERMINAL_EVENTS:
            if sources.count(f'"{name}"') == 1:
                emitted.discard(name)
        emitted.discard(obs.SERVICE)
        # Every registered name is emitted somewhere, and nothing is emitted that is not
        # registered - the two directions of the same promise.
        self.assertEqual(set(obs.EVENTS), emitted & set(obs.EVENTS))
        self.assertEqual(set(), emitted - set(obs.EVENTS))

    def test_the_terminal_registry_matches_what_each_event_means(self):
        """Which records must be persisted is decided by meaning, and checked in both directions.

        A terminal record is one that *is* a result: something finished, failed, was cancelled,
        was deferred, or the process stopped. A record that only announces work that has not
        finished is not terminal, and forcing it to disk would be paying for a promise nobody
        made. Both directions are asserted here because a registry that drifts - a new
        `...finished` event that nobody classified, or an announcement quietly promoted - is how
        "the completion is durable" turns into a claim about some completions.
        """
        terminal = set(obs.TERMINAL_EVENTS)
        registered = set(obs.EVENTS)
        self.assertEqual(set(), terminal - registered)
        completions = {name for name in registered if name.endswith(".finished")}
        self.assertEqual(set(), completions - terminal)
        cancellations = {name for name in registered if name.endswith(".cancelled")}
        self.assertEqual(set(), cancellations - terminal)
        self.assertIn("runtime.background_failed", terminal)
        self.assertIn("runtime.background_work", terminal)
        self.assertIn("direct.delivery.deferred", terminal)
        self.assertIn("outbox.flush", terminal)
        # `direct.request.finished` is not a registered event at all: a direct request is closed
        # out by its attempt and delivery events, so it must not be declared anywhere.
        self.assertNotIn("direct.request.finished", registered | terminal)
        announcements = {
            "runtime.started",
            "runtime.stopping",
            "service.request.started",
            "peer.call.started",
            "turn.prepared",
            "turn.queued",
            "turn.generation.started",
            "turn.delivery.started",
            "direct.request.queued",
            "direct.attempt.started",
            "direct.delivery.started",
        }
        self.assertEqual(set(), announcements & terminal)
        # Together the two sets are the whole registry, so neither list can silently lose a name.
        self.assertEqual(announcements, registered - terminal - {"runtime.log_probe"})


class SecretTests(unittest.TestCase):
    """No secret reaches the log by any of the four paths the card names."""

    CANARY = "CANARY-9f3c-not-a-real-secret"

    def test_request_headers_response_bodies_and_exceptions_never_reach_the_log(self):
        async def scenario():
            h = Harness(silence_ms=0)
            logs = _LogDirectory(self)
            app = logs.attach(create_app(h.core, {"nonebot": "nonebot-token"}, None))

            import httpx

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                base_url="http://core",
            ) as client:
                # A header carrying the canary, in the correlation slot and outside it.
                await client.post(
                    "/internal/v1/conversation/ingest",
                    json=h.request(),
                    headers={
                        "Authorization": "Bearer wrong-" + self.CANARY,
                        "X-Tianshu-Correlation-Id": self.CANARY,
                        self.CANARY: self.CANARY,
                    },
                )
                # A body carrying the canary that the app answers with a fault.
                await client.post(
                    "/internal/v1/conversation/ingest",
                    content=json.dumps({"secret": self.CANARY}).encode(),
                    headers={
                        "Authorization": "Bearer nonebot-token",
                        "Content-Type": "application/json",
                    },
                )

                # An exception whose message is the canary, raised by a dependency, on the
                # turn preparation boundary the failure reporting is meant to cover.
                async def explode(*args, **kwargs):
                    raise OSError("connect failed to " + self.CANARY)

                await client_request_failure(client, h, explode)
            await h.core.close()
            return logs

        logs = asyncio.run(scenario())
        blobs = logs.bytes()
        self.assertTrue(blobs)
        records = logs.records()
        self.assertTrue(records)
        for blob in blobs:
            self.assertNotIn(self.CANARY.encode(), blob)
        # The dependency's failure is reported by a registered code of its own, and the
        # exception's message - the only place the canary appeared - is never the code.
        reported = {record["error_code"] for record in records} - {None}
        self.assertTrue(reported)
        self.assertLessEqual(reported, set(obs.ERROR_CODES))
        # Its fixed class names the kind of failure, never the text of it.
        self.assertIn("os_error", reported)

    def test_correlation_id_is_validated_replaced_and_never_echoed(self):
        async def scenario():
            h = Harness(silence_ms=0)
            logs = _LogDirectory(self)
            app = logs.attach(create_app(h.core, {"nonebot": "nonebot-token"}, None))

            import httpx

            good = "0123456789abcdef0123456789abcdef"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://core"
            ) as client:
                for offered in (good, self.CANARY, "ABCDEF", "0" * 31, "not-hex-at-all"):
                    response = await client.post(
                        "/internal/v1/conversation/ingest",
                        json=h.request(text="hello"),
                        headers={
                            "Authorization": "Bearer nonebot-token",
                            CORRELATION_HEADER: offered,
                        },
                    )
                    self.assertEqual(200, response.status_code)
                    # An invalid value is never echoed back in any form, and the valid one is
                    # not rewritten into something else.
                    echoed = response.headers.get(CORRELATION_HEADER, "")
                    if offered == good:
                        self.assertIn(echoed, ("", good))
                    else:
                        self.assertEqual("", echoed)
            await h.core.close()
            return logs, good

        logs, good = asyncio.run(scenario())
        records = logs.records()
        started = [r for r in records if r["event"] == "service.request.started"]
        self.assertEqual(5, len(started))
        # The valid value is preserved exactly; each invalid one became a fresh valid id.
        self.assertEqual(good, started[0]["correlation_id"])
        for record in started[1:]:
            self.assertRegex(record["correlation_id"], obs.CORRELATION_RE)
            self.assertNotEqual(good, record["correlation_id"])
        for blob in logs.bytes():
            self.assertNotIn(self.CANARY.encode(), blob)

    def test_peer_calls_carry_the_same_valid_correlation_id(self):
        async def scenario():
            logs = _LogDirectory(self)
            seen = []

            import httpx

            async def handler(request):
                seen.append(dict(request.headers))
                return httpx.Response(200, json={"ok": True})

            service = JsonService(
                "https://memory.synthetic.invalid",
                "synthetic-only",
                transport=httpx.MockTransport(handler),
                label="memory",
            )
            # The turn's preparation boundary uses this exact object, so the peer call really
            # happens where the application makes it, not in a rehearsal of the call.
            h = Harness(silence_ms=0)
            h.core.memory = _RecordingMemory(h.core.memory, service)
            app = logs.attach(create_app(h.core, {"nonebot": "nonebot-token"}, None))
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://core"
            ) as client:
                offered = "fedcba9876543210fedcba9876543210"
                await client.post(
                    "/internal/v1/conversation/ingest",
                    json=h.request(),
                    headers={
                        "Authorization": "Bearer nonebot-token",
                        CORRELATION_HEADER: offered,
                    },
                )
                await h.cycles()
            await service.close()
            await h.core.close()
            return logs, seen, offered

        logs, seen, offered = asyncio.run(scenario())
        self.assertTrue(seen)
        # httpx lower-cases the header names it reports, so the comparison is on the
        # documented header rather than on one particular casing of it.
        mark = CORRELATION_HEADER.lower()
        carrying = {headers[mark] for headers in seen if mark in headers}
        self.assertIn(offered, carrying)
        for headers in seen:
            if headers.get(mark) == offered:
                # The credential the peer expects is untouched, and that is the only header
                # this change adds to the call.
                self.assertEqual("Bearer synthetic-only", headers["authorization"])
        records = logs.records()
        peers = [
            r
            for r in records
            if r["event"] == "peer.call.started" and r["correlation_id"] == offered
        ]
        self.assertTrue(peers)
        # The request's own identifier is the one carried by every call the turn makes: the
        # header on the wire and the recorded correlation ID agree, call for call. Calls made
        # by the later background passes carry their own identifier and are not counted here.
        self.assertEqual(len([h for h in seen if h.get(mark) == offered]), len(peers))


async def client_request_failure(client, harness, explode):
    """Drive one queued turn into a failing dependency call, so its failure is reported.

    The turn is admitted first, while the dependencies still answer, and only then do they
    start failing: that is the real shape of a transport failure - a request that succeeded and
    a background turn that could not reach its peer afterwards. The caller's own client is
    reused so the failure is recorded through the same assembled log port as the rest of the
    scenario.
    """
    await client.post(
        "/internal/v1/conversation/ingest",
        json=harness.request(),
        headers={"Authorization": "Bearer nonebot-token"},
    )
    harness.memory.select = explode
    harness.origins.resolve = explode
    await harness.cycles()


class _RecordingMemory:
    """A stand-in that issues one real HTTP call through the shared query client."""

    def __init__(self, inner, service):
        self.inner, self.service = inner, service

    async def select(self, origin, scope, text, budget, known_version=None):
        await self.service.call("/internal/v1/memory/select", {"probe": True})
        return await self.inner.select(origin, scope, text, budget, known_version)

    def __getattr__(self, name):
        # Every other method keeps the harness stand-in's own behaviour: this wrapper adds an
        # observed call, it does not stand in for the domain.
        return getattr(self.inner, name)


class _Sink:
    """A stderr stand-in that keeps the exact bytes the adapter wrote."""

    def __init__(self):
        self.buffer = bytearray()

    def write(self, text):
        self.buffer.extend(text.encode("utf-8"))

    def flush(self):
        return None

    @property
    def lines(self):
        return [json.loads(line) for line in bytes(self.buffer).splitlines() if line.strip()]


def _close_quietly(descriptor):
    """Release a descriptor that may already be closed, without turning cleanup into a failure."""
    try:
        os.close(descriptor)
    except OSError:
        pass


def _drain_pipe(descriptor):
    """Everything readable from a non-blocking descriptor right now, never a wait."""
    chunks = []
    while True:
        try:
            chunk = os.read(descriptor, 1 << 16)
        except (BlockingIOError, InterruptedError):
            break
        except OSError:
            break
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def _records_in(directory):
    """Every valid record line in a log directory, in segment order."""
    return [
        json.loads(line)
        for path in sorted(Path(directory).iterdir(), key=lambda item: item.name)
        for line in path.read_bytes().splitlines()
        if line.strip() and line.startswith(b"{")
    ]


class AdmissionTests(unittest.TestCase):
    """Admission is durable, and a refusal happens before any side effect can be lost.

    The three promises checked here are the ones a log failure could otherwise break: the
    admission record reaches the disk *before* the work it accounts for, a destination that
    cannot take the record refuses new business instead of letting it run unlogged, and a
    refusal never becomes a retry of something already in flight.
    """

    def _adapter(self, **options):
        """A real adapter in a real directory, closed by the case even when an assertion fails.

        The directory is cleaned with `ignore_cleanup_errors` for the same reason: on Windows an
        open handle would otherwise turn a test failure into a cleanup error and hide the cause.
        """
        holder = tempfile.TemporaryDirectory(prefix="tianshu-admit-", ignore_cleanup_errors=True)
        self.addCleanup(holder.cleanup)
        adapter = obs.LogAdapter(holder.name, stderr=_Sink(), **options)
        self.addCleanup(adapter.close)
        return adapter, holder.name

    def test_admission_waits_for_a_real_write_and_the_record_is_on_disk_afterwards(self):
        adapter, directory = self._adapter()
        self.assertTrue(asyncio.run(adapter.admit("runtime.started", "succeeded")))
        # No flush, no poll, no sleep: the record is already readable from the file.
        records = [
            json.loads(line)
            for path in sorted(Path(directory).iterdir())
            for line in path.read_bytes().splitlines()
            if line.strip()
        ]
        self.assertEqual(["runtime.started"], [record["event"] for record in records])

    def test_admission_refuses_when_the_destination_cannot_take_the_record(self):
        adapter, _ = self._adapter()
        self.assertTrue(adapter.flush())
        # A destination that fails its write must refuse admission, not accept the record and
        # drop it later: the caller acts on this answer.
        original_write = adapter._write

        def failing(line, *, fsync):
            original_write(line, fsync=False)
            return False

        adapter._write = failing
        self.assertFalse(asyncio.run(adapter.admit("runtime.started", "succeeded")))
        self.assertEqual(1, adapter.dropped)
        adapter._write = original_write
        # Recovery is explicit, and it is a real write: the adapter is usable again after it.
        self.assertTrue(adapter.probe())
        self.assertEqual("ok", adapter.health())
        self.assertTrue(asyncio.run(adapter.admit("runtime.started", "succeeded")))

    def test_a_full_queue_refuses_rather_than_dropping_silently(self):
        adapter, _ = self._adapter(queue_records=1)
        # Block the writer so the single queue slot stays occupied, then ask for a second
        # record: it must be refused and counted, never accepted and forgotten.
        gate = threading.Event()
        original_write = adapter._write

        def gated(line, *, fsync):
            gate.wait(timeout=10)
            return original_write(line, fsync=False)

        adapter._write = gated
        self.assertTrue(adapter.emit("runtime.started", "succeeded"))
        self.assertFalse(adapter.emit("runtime.background_work", "succeeded"))
        self.assertEqual("writer_saturated", adapter.health())
        self.assertFalse(adapter.accepts_business())
        self.assertGreaterEqual(adapter.dropped, 1)
        gate.set()
        adapter._write = original_write
        self.assertTrue(adapter.flush())

    def test_the_business_loop_keeps_running_while_the_writer_holds_a_slow_disk(self):
        """A slow disk must not stop the event loop: only the writer waits for it."""

        async def scenario():
            adapter, _ = self._adapter()
            gate = threading.Event()
            original_write = adapter._write

            def slow(line, *, fsync):
                gate.wait(timeout=10)
                return original_write(line, fsync=False)

            adapter._write = slow
            # The admission is started but not awaited: the loop must stay responsive while the
            # writer is stuck inside the disk call.
            pending = asyncio.create_task(adapter.admit("runtime.started", "succeeded"))
            ticks = 0
            for _ in range(20):
                await asyncio.sleep(0.005)
                ticks += 1
            self.assertGreaterEqual(ticks, 20)
            self.assertFalse(pending.done())
            gate.set()
            self.assertTrue(await pending)
            adapter._write = original_write

        asyncio.run(scenario())

    def test_cancelling_an_admission_leaves_no_phantom_record(self):
        """A cancelled admission is cancelled: the caller is never told `admitted` after it
        stopped waiting, and the writer still finishes what it was already given."""

        async def scenario():
            adapter, _ = self._adapter()
            gate = threading.Event()
            original_write = adapter._write

            def slow(line, *, fsync):
                gate.wait(timeout=10)
                return original_write(line, fsync=False)

            adapter._write = slow
            pending = asyncio.create_task(adapter.admit("runtime.started", "succeeded"))
            await asyncio.sleep(0.02)
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
            gate.set()
            # The writer still finishes what it was given, and the adapter stays usable.
            self.assertTrue(adapter.flush())
            self.assertEqual("ok", adapter.health())
            adapter._write = original_write

        asyncio.run(scenario())

    def test_closing_never_races_a_live_write_handle(self):
        """`close` stops the writer and joins it before releasing the handle it owns."""
        adapter, directory = self._adapter()
        for _ in range(20):
            adapter.emit("runtime.background_work", "succeeded")
        self.assertTrue(adapter.close())
        self.assertTrue(adapter.closed)
        self.assertIsNone(adapter._stream)
        self.assertIsNone(adapter._writer)
        # A second close is a no-op, and no later emit reaches a released handle.
        self.assertTrue(adapter.close())
        self.assertFalse(adapter.emit("runtime.background_work", "succeeded"))
        self.assertEqual(0, adapter.pending)
        records = [
            json.loads(line)
            for path in sorted(Path(directory).iterdir())
            for line in path.read_bytes().splitlines()
            if line.strip()
        ]
        self.assertEqual(20, len(records))

    def test_a_close_that_times_out_never_steals_the_owners_handle_or_fakes_the_count(self):
        """One owner, one deadline: a shutdown that cannot finish reports it and stops there.

        The writer is the only thing allowed to touch the segment handle. When it is inside a
        write, a closer that reaches in anyway is a second owner: it can close a file mid-write
        and then decrement a counter the writer still owns, which is how `pending` went negative.
        The timeout is therefore a *report* - the handle stays with its owner, the in-flight count
        stays true, and the release happens on the owner's way out.
        """
        adapter, directory = self._adapter(drain_seconds=0.03)
        self.assertTrue(asyncio.run(adapter.admit("service.request.started", "started")))
        owner = adapter._writer
        stream = adapter._stream
        calls = []
        entered, release = threading.Event(), threading.Event()
        original = adapter._write

        class _Watched:
            def __getattr__(self, name):
                return getattr(stream, name)

            def close(self):
                calls.append(
                    {
                        "owner_alive": owner.is_alive(),
                        "by_owner": threading.current_thread() is owner,
                    }
                )
                return stream.close()

        adapter._stream = _Watched()

        def held(line, *, fsync):
            entered.set()
            release.wait(timeout=30)
            return original(line, fsync=fsync)

        adapter._write = held
        self.assertTrue(adapter.emit("service.request.finished", "succeeded"))
        self.assertTrue(entered.wait(timeout=10))

        async def scenario():
            started = time.monotonic()
            confirmed = await adapter.aclose(timeout=0.03)
            return confirmed, time.monotonic() - started

        confirmed, elapsed = asyncio.run(scenario())
        # Bounded, and honest about not having finished.
        self.assertFalse(confirmed)
        self.assertLess(elapsed, 2.0)
        # Nobody but the owner closed the handle, and nothing was zeroed to look clean.
        self.assertEqual([], calls)
        self.assertEqual(1, adapter.pending)
        self.assertGreaterEqual(adapter.dropped, 0)
        # Let the owner finish: it releases the handle itself, and the count lands on zero.
        release.set()
        owner.join(timeout=30)
        self.assertFalse(owner.is_alive())
        self.assertEqual(0, adapter.pending)
        self.assertIsNone(adapter._stream)
        self.assertEqual(0, adapter.dropped)
        # The record the writer was holding is on disk, so the wait was not a lost record.
        self.assertEqual(
            ["service.request.started", "service.request.finished"],
            [record["event"] for record in _records_in(directory)],
        )

    def test_cancelling_a_close_does_not_produce_a_second_closer(self):
        """A cancelled caller stops waiting; the one shutdown it started still finishes."""
        adapter, directory = self._adapter()
        for _ in range(10):
            adapter.emit("runtime.background_work", "succeeded")

        async def scenario():
            task = asyncio.create_task(adapter.aclose(timeout=10))
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            # The shutdown that was begun is the only one, and it completes: the writer exits,
            # the handle is released, and a repeated close is a no-op rather than a second close.
            for _ in range(2000):
                if adapter.closed:
                    break
                await asyncio.sleep(0.005)
            return adapter.closed

        self.assertTrue(asyncio.run(scenario()))
        self.assertFalse(adapter._writer_alive())
        self.assertIsNone(adapter._stream)
        self.assertTrue(adapter.close())
        self.assertEqual(10, len(_records_in(directory)))

    def test_a_full_queue_never_swallows_the_stop_request(self):
        """A shutdown against a full queue still ends the owner, with no sentinel hand-delivered.

        The stop request used to be a single sentinel pushed into the same bounded queue the
        records use. When that queue was full the push was simply dropped, and the owner - the only
        thread allowed to release the handle - stayed parked in `get` for ever: the records it held
        were written, the count reached zero, and the thread never left. The stop intent is now a
        piece of state the owner observes on its own, so an IO that recovers really does end the
        shutdown, and the repeated close reports the completion instead of replaying the timeout.
        """
        adapter, directory = self._adapter(queue_records=1, drain_seconds=0.03)
        self.assertTrue(asyncio.run(adapter.admit("service.request.started", "started")))
        owner = adapter._writer
        entered, release = threading.Event(), threading.Event()
        original = adapter._write

        def held(line, *, fsync):
            entered.set()
            release.wait(timeout=30)
            return original(line, fsync=fsync)

        adapter._write = held
        # One record is inside the writer, the other fills the only queue slot: the sentinel has
        # nowhere to go, which is exactly the case that used to strand the owner.
        self.assertTrue(adapter.emit("service.request.finished", "succeeded"))
        self.assertTrue(entered.wait(timeout=10))
        self.assertTrue(adapter.emit("service.request.finished", "succeeded"))

        async def scenario():
            confirmed = await adapter.aclose(timeout=0.03)
            return confirmed

        self.assertFalse(asyncio.run(scenario()))
        release.set()
        owner.join(timeout=30)
        # Nobody pushed anything into the queue by hand: the owner left because it saw the stop.
        self.assertFalse(owner.is_alive())
        self.assertEqual(0, adapter.pending)
        self.assertEqual(0, adapter.dropped)
        self.assertTrue(adapter.close())
        self.assertEqual(
            ["service.request.started", "service.request.finished", "service.request.finished"],
            [record["event"] for record in _records_in(directory)],
        )

    def test_a_close_answers_inside_its_budget_even_when_the_owners_close_is_slow(self):
        """The deadline must cover the owner's own handle release, not just the join.

        Closing a file is IO, and a filesystem can park it. When that close ran while holding the
        state lock, every thread that needed that lock - including the closer checking its own
        deadline - waited for it: a 30 ms budget returned after 250 ms. The lock is now only ever
        used to swap a reference, so the closer reports "unconfirmed" inside its budget while the
        owner is still finishing, and the handle is still released by the owner alone.
        """
        adapter, directory = self._adapter(drain_seconds=0.03)
        self.assertTrue(asyncio.run(adapter.admit("service.request.started", "started")))
        owner = adapter._writer
        stream = adapter._stream
        calls = []
        entered, release = threading.Event(), threading.Event()

        class _Held:
            def __getattr__(self, name):
                return getattr(stream, name)

            def close(self):
                calls.append(threading.current_thread() is owner)
                entered.set()
                release.wait(timeout=30)
                return stream.close()

        adapter._stream = _Held()
        timer = threading.Timer(0.25, release.set)
        timer.start()

        async def scenario():
            started = time.monotonic()
            confirmed = await adapter.aclose(timeout=0.03)
            return confirmed, time.monotonic() - started

        try:
            confirmed, elapsed = asyncio.run(scenario())
            self.assertFalse(confirmed)
            # Well inside the window the release was held for: the budget is real.
            self.assertLess(elapsed, 0.12)
            self.assertTrue(entered.wait(timeout=10))
            self.assertEqual([True], calls)
        finally:
            release.set()
            timer.join()
        owner.join(timeout=30)
        self.assertFalse(owner.is_alive())
        self.assertIsNone(adapter._stream)
        self.assertEqual(1, len(_records_in(directory)))

    def test_a_repeated_close_reports_the_shutdown_as_it_is_now(self):
        """A close that timed out must not be replayed once the owner has finished."""
        adapter, _ = self._adapter(drain_seconds=0.03)
        self.assertTrue(asyncio.run(adapter.admit("service.request.started", "started")))
        owner = adapter._writer
        entered, release = threading.Event(), threading.Event()
        original = adapter._write

        def held(line, *, fsync):
            entered.set()
            release.wait(timeout=30)
            return original(line, fsync=fsync)

        adapter._write = held
        self.assertTrue(adapter.emit("service.request.finished", "succeeded"))
        self.assertTrue(entered.wait(timeout=10))
        self.assertFalse(asyncio.run(adapter.aclose(timeout=0.03)))
        release.set()
        owner.join(timeout=30)
        self.assertFalse(owner.is_alive())
        # The log really is stopped now, so a later close says so instead of replaying the earlier
        # timeout - and it does not start a second shutdown to get there.
        self.assertTrue(adapter.close())
        self.assertTrue(adapter.close())

    def test_a_terminal_event_is_persisted_not_merely_written(self):
        """The end of a request is fsynced by the writer; a plain record need not be.

        A successful `write` means the kernel took the bytes. The contract asks the terminal
        event of a request, turn, attempt or delivery to be *durable*, so the writer confirms it
        with `fsync` - on its own thread, so the event loop and any authoritative transaction
        never wait for that disk. `flush` and `await_flush` then mean "written and persisted",
        which is why draining alone is not the answer.
        """
        adapter, directory = self._adapter()
        real = os.fsync
        with mock.patch.object(os, "fsync", wraps=real) as sync:
            # A record that only announces work is written without forcing a disk sync of its own.
            self.assertTrue(adapter.emit("runtime.started", "succeeded"))
            self.assertTrue(adapter.flush())
            after_plain = sync.call_count
            # The terminal event of a request must be durable by the time the quiet point is
            # reached, and the caller never paid for it.
            self.assertTrue(adapter.emit("service.request.finished", "succeeded"))
            started = time.monotonic()
            self.assertTrue(adapter.flush())
            elapsed = time.monotonic() - started
            after_terminal = sync.call_count
        self.assertGreater(after_terminal, after_plain)
        self.assertLess(elapsed, 30.0)
        # And closing adds no unpersisted terminal record: what the quiet point promised is
        # already on disk, and the file holds every record in order.
        self.assertTrue(adapter.close())
        self.assertEqual(
            ["runtime.started", "service.request.finished"],
            [record["event"] for record in _records_in(directory)],
        )

    def test_the_real_background_seam_persists_both_of_its_outcomes(self):
        """A pass that worked and a pass that failed are both persisted, through the real loop.

        The background seam is a completion like any other: the contract asks for the end of
        actual background work to be persisted, and `run_loop` reports exactly that - a real pass
        that did work, or a real pass that failed. The *real* `run_loop` is exercised here rather
        than a rehearsal of it, so a change to the seam cannot pass while the registry stays
        wrong.
        """
        sink = _Sink()
        adapter, _directory = self._adapter()
        counts = []
        real = os.fsync
        with mock.patch.object(os, "fsync", wraps=real) as sync:
            counts.append(sync.call_count)
            for fail in (False, True):
                stop = asyncio.Event()

                async def work():
                    if fail:
                        raise OSError("synthetic background failure")

                async def pause(seconds, wait):
                    wait.set()

                asyncio.run(run_loop("synthetic.work", work, port=adapter, wait=stop, sleep=pause))
                self.assertTrue(adapter.flush())
                counts.append(sync.call_count)
        # Both outcomes are terminal records, so each one adds its own fsync: written is not the
        # same as persisted, and the seam that reports real work is not an exception to that.
        self.assertEqual([0, 1, 2], counts)
        self.assertEqual(
            ["runtime.background_work", "runtime.background_failed"],
            [record["event"] for record in _records_in(_directory)],
        )
        self.assertEqual([], sink.lines)

    def test_a_terminal_event_still_cannot_become_a_business_retry(self):
        """A lost completion record is reported, never replayed as business work.

        The completion seam is the one place where a log failure could be turned into a second
        attempt at work that already happened. The record is refused - the caller learns it did not
        land - and nothing about the request that was already admitted is re-sent.
        """
        adapter, directory = self._adapter()
        self.assertTrue(asyncio.run(adapter.admit("service.request.started", "started")))
        self.assertTrue(adapter.flush())
        before = adapter.dropped

        # Every following append fails, exactly as a disk that has gone away behaves. The failure
        # is injected where the *writer* meets it: the handle belongs to that thread, so wrapping
        # it from here would race the write already in flight.
        def gone(line, *, fsync):
            raise OSError(28, "no space left on device")

        original = adapter._write
        adapter._write = gone
        self.assertFalse(
            asyncio.run(adapter.admit("service.request.finished", "succeeded")),
            "a completion record that was not persisted must not be admitted",
        )
        self.assertEqual(before + 1, adapter.dropped)
        # The writer survives the failed append: one record that cannot be written must not take
        # the only thread that writes with it, or every later admission becomes a silent drop.
        self.assertTrue(adapter._writer_alive())
        # Nothing about the request that was already admitted is re-sent, and the record that
        # could not be written is not silently claimed as written.
        self.assertEqual(
            ["service.request.started"], [record["event"] for record in _records_in(directory)]
        )
        adapter._write = original


class DomainOutcomeTests(unittest.TestCase):
    """Every published domain state maps onto the frozen outcome enumeration."""

    def test_every_published_domain_state_has_a_registered_outcome(self):
        # The states the published contracts and this product's durable rows actually use.
        published = (
            # conversation#send_receipt#state
            "sent",
            "failed",
            "unknown",
            # conversation#turn#delivery_state and source-sync committed_event
            "not_started",
            "partial",
            "not_required",
            # conversation#cancel_response#state
            "cancelled",
            "partially_cancelled",
            "too_late",
            # routing: functional request and attempt states
            "accepted",
            "pending",
            "dispatching",
            "submitted",
            "completed",
            "registered",
            "revoked",
            "superseded",
            "expired",
            "deferred",
        )
        for state in published:
            outcome = obs.runtime_outcome(state)
            self.assertIn(outcome, obs.OUTCOMES, state)
        # The mapping is what adapts, never the enumeration: the frozen set is unchanged.
        self.assertEqual(
            ("started", "succeeded", "failed", "cancelled", "unknown", "rejected", "degraded"),
            obs.OUTCOMES,
        )
        # A successful send is a success, not a refusal: this is the defect the mapping fixes.
        self.assertEqual("succeeded", obs.runtime_outcome("sent"))
        self.assertEqual("succeeded", obs.runtime_outcome("completed"))
        self.assertEqual("unknown", obs.runtime_outcome("unknown"))
        self.assertEqual("failed", obs.runtime_outcome("failed"))
        # A value that is not a state at all is refused rather than guessed.
        self.assertIsNone(obs.runtime_outcome("something_else"))
        self.assertIsNone(obs.runtime_outcome(None))

    def test_a_delivered_turn_is_recorded_as_succeeded_and_not_dropped(self):
        async def scenario():
            h = Harness(silence_ms=0)
            logs = _LogDirectory(self)
            app = logs.attach(create_app(h.core, {"nonebot": "nonebot-token"}, None))
            import httpx

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://core"
            ) as client:
                response = await client.post(
                    "/internal/v1/conversation/ingest",
                    json=h.request(text="hello"),
                    headers={"Authorization": "Bearer nonebot-token"},
                )
                self.assertEqual(200, response.status_code)
                for _ in range(200):
                    await h.core.tick()
                    await asyncio.sleep(0)
            await h.core.close()
            return logs

        logs = asyncio.run(scenario())
        records = logs.records()
        # The success path really is reported: a delivered send is `succeeded`, and the
        # adapter dropped nothing on the way.
        delivered = [r for r in records if r["event"] == "turn.delivery.finished"]
        self.assertTrue(delivered)
        for record in delivered:
            self.assertIn(record["outcome"], obs.OUTCOMES)
        self.assertNotIn("sent", [r["outcome"] for r in records])
        for record in records:
            self.assertIn(record["outcome"], obs.OUTCOMES)


class SequenceTests(unittest.TestCase):
    """Concurrency, uniqueness and full coverage."""

    def test_concurrent_threads_get_one_unique_monotonic_sequence_each(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        total, threads = 400, 8
        barrier = threading.Barrier(threads)

        def writer():
            barrier.wait()
            for _ in range(total // threads):
                adapter.emit("runtime.background_work", "succeeded")

        workers = [threading.Thread(target=writer) for _ in range(threads)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        self.assertTrue(adapter.flush())
        records = sink.lines
        self.assertEqual(total, len(records))
        sequences = sorted(record["sequence"] for record in records)
        self.assertEqual(list(range(1, total + 1)), sequences)
        self.assertEqual(total, len({record["event_id"] for record in records}))
        self.assertEqual(1, len({record["instance_id"] for record in records}))

    def test_instance_identity_differs_per_process_adapter(self):
        first, second = obs.LogAdapter(None, stderr=_Sink()), obs.LogAdapter(None, stderr=_Sink())
        self.assertNotEqual(first.instance_id, second.instance_id)
        self.assertEqual("companion", first.service)

    def test_n_events_all_written_and_nothing_is_sampled(self):
        """N emitted events produce N records, including every failure and unknown."""
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        events = sorted(obs.EVENTS)
        total = 0
        for event in events:
            for outcome in obs.OUTCOMES:
                adapter.emit(event, outcome)
                total += 1
        self.assertTrue(adapter.flush())
        records = sink.lines
        self.assertEqual(total, len(records))
        self.assertEqual(total, len({(r["event"], r["outcome"]) for r in records}))
        self.assertEqual(
            {outcome for outcome in obs.OUTCOMES},
            {record["outcome"] for record in records},
        )

    def test_dropped_records_are_counted_rather_than_hidden(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        adapter._sequence = int("9" * 4200)
        self.assertFalse(adapter.emit("runtime.background_work", "succeeded"))
        self.assertEqual(1, adapter.dropped)
        self.assertEqual([], sink.lines)
        # A dropped record leaves the sequence where it was, so an accepted event still
        # carries a contract-valid number instead of inheriting the refused one.
        adapter._sequence = 7
        self.assertTrue(adapter.emit("runtime.background_work", "succeeded"))
        self.assertTrue(adapter.flush())
        self.assertEqual(8, sink.lines[-1]["sequence"])


class DurabilityTests(unittest.TestCase):
    """Rotation, the directory budget, IO failure and the honest health state."""

    def test_rotation_seals_a_segment_at_the_configured_size_without_deleting(self):
        with tempfile.TemporaryDirectory() as directory:
            adapter = obs.LogAdapter(
                directory, max_segment_bytes=1024, max_directory_bytes=32 * 1024 * 1024
            )
            for _ in range(200):
                self.assertTrue(adapter.emit("runtime.background_work", "succeeded"))
            adapter.close()
            segments = sorted(
                Path(directory).iterdir(),
                key=lambda path: int(path.name.rsplit(".", 1)[1]) if "." in path.name[15:] else 0,
            )
            self.assertGreater(len(segments), 1)
            # Segments roll forward; no sealed segment is deleted or overwritten.
            self.assertEqual("companion.jsonl", segments[0].name)
            for index, segment in enumerate(segments[1:], 1):
                self.assertEqual(f"companion.jsonl.{index}", segment.name)
            for segment in segments:
                for line in segment.read_bytes().splitlines():
                    self.assertLessEqual(len(line) + 1, obs.MAX_RECORD_BYTES)
            self.assertEqual("ok", adapter.health())

    def test_directory_budget_refuses_new_business_and_reads_what_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            adapter = obs.LogAdapter(
                directory, max_segment_bytes=512, max_directory_bytes=32 * 1024 * 1024
            )
            # Shrink the budget after opening, so the refusal is reached with real content.
            adapter.max_directory_bytes = 4096
            written = 0
            # Admission is what proves a record landed, so the loop uses it: with `emit` the
            # writes would still be queued, and the count would measure nothing.
            while asyncio.run(adapter.admit("runtime.background_work", "succeeded")):
                written += 1
                if written > 100000:
                    break
            self.assertLess(written, 100000)
            self.assertGreater(written, 0)
            self.assertEqual("capacity_exhausted", adapter.health())
            self.assertFalse(adapter.accepts_business())
            # The records already written are still readable, and nothing was removed.
            existing = [path for path in Path(directory).iterdir() if path.stat().st_size]
            self.assertTrue(existing)
            total = sum(path.stat().st_size for path in Path(directory).iterdir())
            self.assertLessEqual(total, 4096 + obs.MAX_RECORD_BYTES)
            adapter.close()

    def test_io_failure_degrades_honestly_and_recovers_only_after_a_real_write(self):
        with tempfile.TemporaryDirectory() as directory:
            sink = _Sink()
            adapter = obs.LogAdapter(directory, stderr=sink)
            self.assertTrue(adapter.emit("runtime.started", "succeeded", fsync=True))
            # Reach a quiet point first: the file handle belongs to the writer thread, so the
            # failure has to be injected where the writer will meet it, not by reaching into a
            # handle another thread is using.
            self.assertTrue(adapter.flush())
            # A write that cannot succeed: every following append raises on a closed handle.
            adapter._stream.close()
            # The admission gate is where a destination failure must surface: a request that
            # cannot have its record written is refused, not admitted and then quietly dropped.
            self.assertFalse(asyncio.run(adapter.admit("runtime.background_failed", "failed")))
            self.assertEqual("log_unavailable", adapter.health())
            self.assertEqual(1, adapter.dropped)
            # The warning is fixed and safe, and it is written exactly once.
            self.assertEqual(1, bytes(sink.buffer).count(b"runtime log"))
            self.assertFalse(adapter.emit("runtime.background_failed", "failed"))
            self.assertTrue(adapter.flush())
            self.assertEqual(1, bytes(sink.buffer).count(b"runtime log"))
            # Recovery needs a genuinely successful write, not a repeated failed attempt.
            self.assertFalse(adapter.probe())
            self.assertEqual("log_unavailable", adapter.health())
            adapter._stream = None
            self.assertTrue(adapter.probe())
            self.assertEqual("ok", adapter.health())
            self.assertTrue(adapter.emit("runtime.background_work", "succeeded"))
            self.assertTrue(adapter.flush())
            adapter.close()

    def test_a_stream_nobody_reads_never_stops_the_caller(self):
        """A captured standard error whose pipe has filled must cost records, not latency.

        This is the defect that froze a whole runtime: the diagnostic channel was written
        synchronously by whoever called `emit`, which is the event loop, so once the pipe's
        buffer filled the process stopped answering - no request, no health probe, no background
        pass - until someone drained that pipe. A real pipe is used here, filled on purpose, and
        the writer must return promptly with the loss counted.
        """
        read_end, write_end = os.pipe()
        for descriptor in (read_end, write_end):
            self.addCleanup(_close_quietly, descriptor)
        # Both ends are non-blocking: the reader below must never be the thing that waits, and
        # an empty pipe is reported as empty rather than hung on.
        os.set_blocking(read_end, False)
        os.set_blocking(write_end, False)
        # The raw descriptor is the stream: the adapter is given exactly the pipe a container
        # runtime gives a captured standard error.
        adapter = obs.LogAdapter(None, stderr=write_end)
        # More records than the queue and the pipe can both hold, so the writer really meets a
        # full pipe. `emit` must never wait for that: every call returns.
        started = time.monotonic()
        accepted = 0
        for _ in range(20000):
            if adapter.emit("runtime.background_work", "succeeded"):
                accepted += 1
            if time.monotonic() - started > 20:
                self.fail("emit stopped returning while the diagnostic stream was blocked")
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 20.0)
        self.assertGreater(accepted, 0)
        # Reaching a quiet point is bounded too: whether the writer managed to place the rest of
        # the backlog or met a full pipe, `flush` answers inside its bound instead of waiting for
        # a reader that never comes.
        flushed_at = time.monotonic()
        adapter.flush(timeout=0.5)
        self.assertLess(time.monotonic() - flushed_at, 5.0)
        self.assertGreater(adapter.dropped, 0)
        # The loss is bounded by the queue, not unbounded: at most one queue's worth of records
        # can be waiting, and everything else was counted as dropped.
        self.assertLessEqual(adapter.pending, adapter.queue_records)
        # Closing is bounded as well, even with the writer parked on that pipe.
        closed_at = time.monotonic()
        adapter.close(drain=0.5)
        self.assertLess(time.monotonic() - closed_at, 30.0)
        # Draining the pipe proves the records that were written really were written, so the
        # dropped count is a bound on loss and not an excuse for writing nothing.
        drained = _drain_pipe(read_end)
        self.assertTrue(drained.startswith(b'{"schema_version"'))
        # The writer thread is a daemon that cannot outlive the interpreter, and it never held
        # the state lock while waiting, so the adapter is still fully readable afterwards.
        self.assertEqual("non_durable", adapter.health())

    def test_an_already_full_directory_refuses_the_very_first_admission(self):
        """Capacity is measured before anything can be admitted, not discovered on first write.

        This is the shape a restart has: the directory is already at its budget, and the new
        process must not be able to buy capacity the running one was refused. The baseline was
        previously taken from the writer's first append, so the *first* admission read the
        directory as empty and the file stepped over its own declared bound.
        """
        with tempfile.TemporaryDirectory(prefix="tianshu-full-", ignore_cleanup_errors=True) as d:
            # A real, valid segment already holding exactly the whole budget.
            (Path(d) / "companion.jsonl").write_bytes(b"x" * obs.MIN_DIRECTORY_BYTES)
            adapter = obs.LogAdapter(d, max_directory_bytes=obs.MIN_DIRECTORY_BYTES, stderr=_Sink())
            self.addCleanup(adapter.close)
            self.assertFalse(asyncio.run(adapter.admit("service.request.started", "started")))
            self.assertFalse(adapter.accepts_business())
            self.assertEqual("capacity_exhausted", adapter.health())
            self.assertGreaterEqual(adapter.dropped, 1)
            # Nothing was appended anywhere, and the existing segment was neither removed nor
            # rewritten: a refusal is a refusal, not a clean-up.
            total = sum(path.stat().st_size for path in Path(d).iterdir())
            self.assertEqual(obs.MIN_DIRECTORY_BYTES, total)
            self.assertEqual(
                b"x" * obs.MIN_DIRECTORY_BYTES, (Path(d) / "companion.jsonl").read_bytes()
            )

    def test_the_budget_is_never_crossed_across_restarts_and_segments(self):
        """Every byte a restart can see is inside the same budget the adapter declares.

        A segment that already exists is charged to the new process before it admits anything, so
        a restart cannot buy capacity the previous process was refused - and it cannot delete what
        that process collected to make room either.
        """
        with tempfile.TemporaryDirectory(
            prefix="tianshu-restart-", ignore_cleanup_errors=True
        ) as d:
            budget = obs.MIN_DIRECTORY_BYTES
            probe = obs.LogAdapter(d, max_directory_bytes=budget, stderr=_Sink())
            sample = probe.submit("runtime.background_work", "succeeded")
            self.assertIsNotNone(sample)
            line_bytes = sample.size
            self.assertTrue(probe.flush())
            probe.close()
            (Path(d) / "companion.jsonl").unlink()
            # Room for a handful of records, and no more.
            fits = 6
            room = budget - fits * line_bytes
            (Path(d) / "companion.jsonl").write_bytes(b"x" * room)
            first = obs.LogAdapter(
                d, max_segment_bytes=1 << 20, max_directory_bytes=budget, stderr=_Sink()
            )
            written = 0
            while asyncio.run(first.admit("runtime.background_work", "succeeded")):
                written += 1
                if written > 1000:
                    break
            self.assertEqual(fits, written)
            self.assertFalse(first.accepts_business())
            self.assertTrue(first.close())
            total = sum(path.stat().st_size for path in Path(d).iterdir())
            self.assertLessEqual(total, budget)
            self.assertEqual(budget, total)
            self.assertEqual(written, len(_records_in(d)))
            # The restart meets the same wall: a second process cannot append a single record,
            # and it does not delete what the first one collected.
            second = obs.LogAdapter(
                d, max_segment_bytes=1 << 20, max_directory_bytes=budget, stderr=_Sink()
            )
            self.addCleanup(second.close)
            self.assertFalse(asyncio.run(second.admit("service.request.started", "started")))
            self.assertEqual(total, sum(path.stat().st_size for path in Path(d).iterdir()))
            self.assertEqual(written, len(_records_in(d)))

    def test_a_directory_that_cannot_be_measured_is_not_assumed_empty(self):
        """An unreadable directory refuses business instead of writing into an unknown budget."""
        with tempfile.TemporaryDirectory(prefix="tianshu-blind-", ignore_cleanup_errors=True) as d:
            adapter = obs.LogAdapter(d, stderr=_Sink())
            self.addCleanup(adapter.close)
            # A measurement that fails is a real condition: the destination cannot be trusted.
            with mock.patch.object(
                obs.LogAdapter, "_scan_directory", side_effect=OSError("denied")
            ):
                blind = obs.LogAdapter(d, stderr=_Sink())
                self.addCleanup(blind.close)
            self.assertFalse(blind.accepts_business())
            self.assertEqual("log_unavailable", blind.health())
            self.assertFalse(asyncio.run(blind.admit("service.request.started", "started")))

    def test_reserved_records_are_counted_against_the_budget_before_they_are_written(self):
        """What is queued already occupies the budget, so a burst cannot overrun the file.

        The writer may be behind on a slow disk; the records waiting for it are still bytes the
        directory is about to hold, so they are reserved at submit time rather than discovered
        after the file has grown. The room is made by a segment that already exists, which is also
        the state a restart meets.
        """
        with tempfile.TemporaryDirectory(
            prefix="tianshu-reserve-", ignore_cleanup_errors=True
        ) as d:
            budget = obs.MIN_DIRECTORY_BYTES
            # Measure one real record first: the reservation has to be checked against actual
            # bytes, not against the contract's maximum line.
            probe = obs.LogAdapter(d, max_directory_bytes=budget, stderr=_Sink())
            sample = probe.submit("runtime.background_work", "succeeded")
            self.assertIsNotNone(sample)
            line_bytes = sample.size
            self.assertTrue(probe.flush())
            probe.close()
            (Path(d) / "companion.jsonl").unlink()
            # Room for a handful of records, and no more.
            fits = 8
            room = budget - fits * line_bytes
            (Path(d) / "companion.jsonl").write_bytes(b"x" * room)
            adapter = obs.LogAdapter(d, max_directory_bytes=budget, stderr=_Sink())
            self.addCleanup(adapter.close)
            self.assertEqual(room, adapter._directory_bytes)
            gate = threading.Event()
            original = adapter._write

            def held(line, *, fsync):
                gate.wait(timeout=60)
                return original(line, fsync=fsync)

            adapter._write = held
            accepted = 0
            for _ in range(200):
                if adapter.emit("runtime.background_work", "succeeded"):
                    accepted += 1
            # The budget was enforced while nothing at all had been written yet: every record
            # still waiting for the writer was already counted, and the count lands exactly on
            # what the remaining room can hold.
            self.assertEqual(fits, accepted)
            self.assertEqual(accepted * line_bytes, adapter._pending_bytes)
            self.assertTrue(adapter.capacity_exhausted)
            gate.set()
            adapter._write = original
            self.assertTrue(adapter.flush())
            total = sum(path.stat().st_size for path in Path(d).iterdir())
            self.assertLessEqual(total, budget)
            self.assertEqual(budget, total)

    def test_a_quiet_point_is_never_reported_while_a_record_is_still_queued(self):
        """`flush` may only answer True once the writer has really finished the record.

        The pending count is published by the producer *before* the record is queued, because
        the writer can consume it the instant it appears: with the count published afterwards,
        `flush` and `admit` could observe zero and report a durable write that had not happened.
        """
        for _ in range(200):
            sink = _Sink()
            adapter = obs.LogAdapter(None, stderr=sink)
            self.assertTrue(adapter.emit("runtime.started", "succeeded"))
            self.assertTrue(adapter.flush())
            self.assertEqual(1, len(sink.lines))
            adapter.close()

    def test_without_a_configured_directory_the_channel_is_non_durable(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        self.assertFalse(adapter.durable)
        self.assertEqual("non_durable", adapter.health())
        self.assertTrue(adapter.emit("runtime.started", "succeeded"))
        self.assertTrue(adapter.flush())
        self.assertEqual("runtime.started", sink.lines[0]["event"])
        adapter.close()

    def test_configuration_is_explicit_and_an_unusable_value_fails_startup(self):
        adapter = log_adapter_from_environment({})
        self.assertFalse(adapter.durable)
        with tempfile.TemporaryDirectory() as directory:
            explicit = log_adapter_from_environment({ENVIRONMENT_LOG_DIR: directory})
            self.assertTrue(explicit.durable)
            self.assertEqual(directory, explicit.log_dir)
            explicit.close()
        with self.assertRaises(ValueError):
            log_adapter_from_environment(
                {ENVIRONMENT_LOG_DIR: tempfile.mkdtemp(), obs.ENVIRONMENT_LOG_DIRECTORY_BYTES: "1"}
            )
        with self.assertRaises(ValueError):
            log_adapter_from_environment(
                {
                    ENVIRONMENT_LOG_DIR: tempfile.mkdtemp(),
                    obs.ENVIRONMENT_LOG_SEGMENT_BYTES: "not-a-number",
                }
            )


class BackgroundLoopTests(unittest.IsolatedAsyncioTestCase):
    """Idle passes, real work, every failure reported, bounded cancelable backoff."""

    async def test_idle_passes_are_not_business_events_and_work_is_reported(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        state = {"count": 0}
        stop = asyncio.Event()
        iterations = {"n": 0}

        async def work():
            iterations["n"] += 1
            if iterations["n"] == 3:
                state["count"] += 1
            if iterations["n"] >= 4:
                stop.set()

        await run_loop(
            "life.work",
            work,
            port=adapter,
            interval=0.001,
            wait=stop,
            counter=lambda clock=None: state["count"],
        )
        names = [record["event"] for record in sink.lines]
        # One real pass did work; the other three did nothing, and neither doing nothing nor
        # starting and stopping is an outcome, so the log is an account of what happened.
        self.assertEqual(["runtime.background_work"], names)
        self.assertEqual([1], [record["sequence"] for record in sink.lines])

    async def test_every_failure_is_reported_and_the_backoff_stays_bounded(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        stop = asyncio.Event()
        requested = []
        calls = {"n": 0}

        async def work():
            calls["n"] += 1
            if calls["n"] >= 4:
                stop.set()
            raise OSError("dependency exploded with a secret in the message")

        async def pause(seconds, wait):
            # The loop's own waiting policy is what this asserts, so the wait is recorded and
            # shortened rather than measured: a wall-clock gap also contains the scheduler's
            # own granularity, which is not the contract.
            requested.append(seconds)
            await asyncio.sleep(seconds / 50)

        await run_loop(
            "core.tick",
            work,
            port=adapter,
            interval=0.001,
            wait=stop,
            counter=lambda clock=None: 0,
            sleep=pause,
        )
        failures = [r for r in sink.lines if r["event"] == "runtime.background_failed"]
        # Four attempts, four reported failures: nothing sampled, nothing filtered.
        self.assertEqual(4, len(failures))
        self.assertEqual({"os_error"}, {record["error_code"] for record in failures})
        # Every delay doubles from the documented minimum and stays under the ceiling.
        self.assertEqual([0.05, 0.1, 0.2, 0.4], requested)
        for seconds in requested:
            self.assertLessEqual(seconds, 30.0)
        # The reported failures are in the order they happened, one per real attempt.
        self.assertEqual([1, 2, 3, 4], [record["sequence"] for record in failures])
        self.assertEqual(
            ["runtime.background_failed"] * 4,
            [record["event"] for record in sink.lines],
        )

    async def test_the_loop_is_cancellable_while_it_backs_off(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        stop = asyncio.Event()

        async def work():
            raise OSError("always broken")

        task = asyncio.create_task(
            run_loop(
                "core.tick",
                work,
                port=adapter,
                interval=0.001,
                wait=stop,
                counter=lambda clock=None: 0,
            )
        )
        await asyncio.sleep(0.08)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)
        self.assertTrue(task.done())

    async def test_a_cancelled_loop_stops_immediately(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        started = asyncio.Event()

        async def work():
            started.set()
            await asyncio.sleep(10)

        task = asyncio.create_task(
            run_loop("core.tick", work, port=adapter, interval=0.001, counter=None)
        )
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


def _failing_write(line, *, fsync):
    raise OSError("log device unavailable")


if __name__ == "__main__":
    unittest.main()
