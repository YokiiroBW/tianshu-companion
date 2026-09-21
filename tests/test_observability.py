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
        emitted.discard(obs.SERVICE)
        # Every registered name is emitted somewhere, and nothing is emitted that is not
        # registered - the two directions of the same promise.
        self.assertEqual(set(obs.EVENTS), emitted & set(obs.EVENTS))
        self.assertEqual(set(), emitted - set(obs.EVENTS))


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
            while adapter.emit("runtime.background_work", "succeeded"):
                written += 1
                if written > 100000:
                    break
            self.assertLess(written, 100000)
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
            # A write that cannot succeed: the descriptor is closed under the adapter and
            # every following write attempt fails on that same closed descriptor.
            adapter._stream.close()
            self.assertFalse(adapter.emit("runtime.background_failed", "failed"))
            self.assertEqual("log_unavailable", adapter.health())
            self.assertEqual(1, adapter.dropped)
            # The warning is fixed and safe, and it is written exactly once.
            self.assertEqual(1, bytes(sink.buffer).count(b"runtime log"))
            self.assertFalse(adapter.emit("runtime.background_failed", "failed"))
            self.assertEqual(1, bytes(sink.buffer).count(b"runtime log"))
            # Recovery needs a genuinely successful write, not a repeated failed attempt.
            self.assertFalse(adapter.probe())
            self.assertEqual("log_unavailable", adapter.health())
            adapter._stream = None
            self.assertTrue(adapter.probe())
            self.assertEqual("ok", adapter.health())
            self.assertTrue(adapter.emit("runtime.background_work", "succeeded"))
            adapter.close()

    def test_without_a_configured_directory_the_channel_is_non_durable(self):
        sink = _Sink()
        adapter = obs.LogAdapter(None, stderr=sink)
        self.assertFalse(adapter.durable)
        self.assertEqual("non_durable", adapter.health())
        self.assertTrue(adapter.emit("runtime.started", "succeeded"))
        self.assertEqual("runtime.started", sink.lines[0]["event"])

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
