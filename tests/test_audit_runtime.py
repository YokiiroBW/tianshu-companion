import asyncio
import copy
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import httpx
from support import Harness
from tianshu_companion import observability as obs
from tianshu_companion.app import create_app
from tianshu_companion.clients import JsonService
from tianshu_companion.contracts import Fault
from tianshu_companion.worker_health import WorkerHealth
from tianshu_companion.store import Store
from tianshu_companion.work_index import INDEXES


class Capture:
    def __init__(self):
        self.events = []

    def emit(self, event, outcome, **fields):
        self.events.append((event, outcome, fields))
        return True


class RuntimeFixTests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_response_and_legitimate_json(self):
        class Stream(httpx.AsyncByteStream):
            count = 0
            closed = False

            async def __aiter__(self):
                for _ in range(128):
                    self.count += 1
                    yield b"x" * 65536

            async def aclose(self):
                self.closed = True

        stream = Stream()

        async def handle(request):
            return httpx.Response(200, stream=stream)

        client = JsonService(
            "https://synthetic.invalid", "test", transport=httpx.MockTransport(handle)
        )
        try:
            with self.assertRaises(Fault):
                await client.call("/internal/command", {})
            self.assertEqual(31, stream.count)
            self.assertTrue(stream.closed)
        finally:
            await client.close()
        for status, data in [(200, {"ok": True}), (403, {"code": "forbidden"})]:
            client = JsonService(
                "https://synthetic.invalid",
                "test",
                transport=httpx.MockTransport(lambda request: httpx.Response(status, json=data)),
            )
            try:
                if status == 200:
                    self.assertEqual(data, await client.call("/internal/command", {}))
                else:
                    with self.assertRaises(Fault) as error:
                        await client.call("/internal/command", {})
                    self.assertEqual("forbidden", error.exception.code)
            finally:
                await client.close()

    async def test_command_slow_body_has_total_budget_and_no_retry(self):
        calls = []

        class Slow(httpx.AsyncByteStream):
            closed = False

            async def __aiter__(self):
                while True:
                    yield b" "
                    await asyncio.sleep(0.01)

            async def aclose(self):
                self.closed = True

        stream = Slow()

        def handle(request):
            calls.append(request)
            return httpx.Response(200, stream=stream)

        client = JsonService(
            "https://synthetic.invalid", "test", transport=httpx.MockTransport(handle)
        )
        client.COMMAND_BUDGET_SECONDS = 0.04
        try:
            with self.assertRaises(Fault):
                await asyncio.wait_for(client.call("/internal/command", {}), 0.5)
            self.assertEqual(1, len(calls))
            self.assertTrue(stream.closed)
        finally:
            await client.close()

    async def test_encoded_body_is_rejected_before_decompression(self):
        class Encoded(httpx.AsyncByteStream):
            read = False

            async def __aiter__(self):
                self.read = True
                yield b"not-an-identity-body"

        stream = Encoded()
        client = JsonService(
            "https://synthetic.invalid",
            "test",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, headers={"content-encoding": "gzip"}, stream=stream
                )
            ),
        )
        try:
            with self.assertRaises(Fault):
                await client.call("/internal/command", {})
            self.assertFalse(stream.read)
        finally:
            await client.close()

    async def test_worker_failure_idle_recovery_stall_and_stop(self):
        now = [0.0]
        health = WorkerHealth(lambda: now[0])
        gate = asyncio.Event()
        task = asyncio.create_task(gate.wait())
        health.register("tick", 0.05, task)
        self.assertFalse(health.healthy())
        health.completed("tick", failed=False)
        self.assertTrue(health.healthy())
        health.completed("tick", failed=True)
        self.assertFalse(health.healthy())
        health.completed("tick", failed=False)
        self.assertTrue(health.healthy())
        now[0] = 61
        self.assertFalse(health.healthy())
        health.completed("tick", failed=False)
        gate.set()
        await task
        self.assertFalse(health.healthy())

    async def test_real_lifespan_failed_tick_is_not_ready(self):
        h = Harness()
        broken = [True]

        async def fail():
            if broken[0]:
                raise RuntimeError("synthetic")

        h.core.tick = fail
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.dict(
                "os.environ",
                {obs.ENVIRONMENT_LOG_DIR: tmp, "TIANSHU_DIAGNOSTICS_TOKEN": "test-diagnostics"},
            ),
        ):
            app = create_app(h.core, {"nonebot": "test-bridge"})
            self.assertFalse(app.state.health.workers.healthy())
            async with app.router.lifespan_context(app):
                await asyncio.sleep(0.15)
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app), base_url="https://synthetic.invalid"
                ) as client:
                    response = await client.get(
                        "/health/ready", headers={"Authorization": "Bearer test-diagnostics"}
                    )
                    self.assertEqual(503, response.status_code)
                    self.assertEqual("failed", response.json()["checks"]["runtime"])
                    broken[0] = False
                    for _ in range(40):
                        await asyncio.sleep(0.025)
                        if app.state.health.workers.healthy():
                            break
                    response = await client.get(
                        "/health/ready", headers={"Authorization": "Bearer test-diagnostics"}
                    )
                    self.assertEqual(200, response.status_code)
            self.assertFalse(app.state.health.workers.healthy())

    async def test_debounce_restart_delivery_and_failed_commit_share_correlation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "core.db"
            h = Harness(path)
            capture = Capture()
            h.core._events = capture
            try:
                with obs.correlation_scope("a" * 32):
                    await h.ingest(text="synthetic first")
                with obs.correlation_scope("b" * 32):
                    await h.ingest(text="synthetic continuation")
                h.clock.advance(5)
                h.core._seal_due(h.clock())
                await h.core.close()
                h.core = h.new_core()
                h.core._events = capture
                h.core.recover()
                h.memory.fail_commit = True
                await h.cycles(80)
                await h.core.flush_outbox()
                self.assertEqual("a" * 32, h.turns()[0]["correlation_id"])
                relevant = [
                    (e, o, f)
                    for e, o, f in capture.events
                    if e.startswith("turn.") or e == "outbox.flush"
                ]
                self.assertTrue(
                    any(e == "turn.delivery.finished" and o == "sent" for e, o, f in relevant)
                    or any(
                        e == "turn.delivery.finished" and o == "succeeded" for e, o, f in relevant
                    )
                )
                self.assertTrue(any(e == "outbox.flush" and o == "failed" for e, o, f in relevant))
                self.assertTrue(
                    all(f.get("correlation_id") == "a" * 32 for e, o, f in relevant), relevant
                )
                self.assertEqual(
                    {"a" * 32, "b" * 32}, {i["correlation_id"] for i in h.core.store.list("inbox")}
                )
            finally:
                await h.core.close()

    async def test_idle_work_does_not_read_completed_history(self):
        h = Harness(silence_ms=0)
        try:
            await h.ingest(text="synthetic")
            await h.cycles(80)
            await h.core.flush_outbox()
            item = h.core.store.list("outbox")[0]
            with h.core.store.transaction():
                for n in range(1000):
                    extra = copy.deepcopy(item)
                    extra["id"] = f"old:{n}"
                    h.core.store.put("outbox", extra)
            queries = []
            h.core.store.db.set_trace_callback(queries.append)
            await h.core.tick()
            await h.core.flush_outbox()
            self.assertFalse(any("SELECT body FROM outbox WHERE 1=1" in q for q in queries))
            self.assertFalse(any("SELECT body FROM conversations WHERE 1=1" in q for q in queries))
            self.assertEqual([], h.core.store.work_conversations())
        finally:
            await h.core.close()

    async def test_work_index_upgrade_preserves_facts_and_creates_one_restore_point(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "old.db"
            store = Store(path)
            fact = {
                "id": "turn:kept",
                "conversation_id": "conversation:kept",
                "phase": "sent",
                "sequence": 1,
            }
            store.put("turns", fact)
            store.close()
            with closing(sqlite3.connect(path, isolation_level=None)) as db:
                for name in INDEXES:
                    db.execute("DROP INDEX " + name)
            store = Store(path)
            self.assertEqual(fact, store.get("turns", fact["id"]))
            store.close()
            backups = list(Path(tmp).glob("*.pre-work-index-*.bak"))
            self.assertEqual(1, len(backups))
            with closing(sqlite3.connect(backups[0])) as db:
                self.assertEqual(1, db.execute("SELECT COUNT(*) FROM turns").fetchone()[0])
                self.assertIsNone(
                    db.execute("SELECT 1 FROM sqlite_master WHERE name='outbox_due'").fetchone()
                )
            Store(path).close()
            self.assertEqual(backups, list(Path(tmp).glob("*.pre-work-index-*.bak")))
            with closing(sqlite3.connect(path, isolation_level=None)) as db:
                db.execute("DROP INDEX outbox_due")
                db.execute("CREATE INDEX outbox_due ON outbox(id)")
            with self.assertRaisesRegex(RuntimeError, "index definition mismatch"):
                Store(path)

    async def test_work_pages_are_fair_and_do_not_walk_completed_history(self):
        store = Store(":memory:")
        try:
            with store.transaction():
                for n in range(10000):
                    store.put(
                        "turns", {"id": f"old:{n}", "conversation_id": f"old:{n}", "phase": "sent"}
                    )
                for n in range(140):
                    store.put(
                        "turns",
                        {"id": f"new:{n:04}", "conversation_id": f"new:{n:04}", "phase": "queued"},
                    )
            steps = []
            store.db.set_progress_handler(lambda: steps.append(1) or 0, 100)
            first = store.work_conversations()
            second = store.work_conversations(first[-1])
            third = store.work_conversations(second[-1])
            self.assertEqual(140, len(set(first + second + third)))
            self.assertEqual([64, 64, 12], list(map(len, (first, second, third))))
            self.assertEqual([], store.work_turns("old:0", ["queued"]))
            self.assertEqual(1, len(store.work_turns("new:0000", ["queued"])))
            self.assertLess(len(steps), 70, "SQL VM must not walk 10,000 historical turns")
        finally:
            store.close()
