import asyncio
import unittest

import httpx
from support import Harness
from tianshu_companion.clients import JsonService
from tianshu_companion.contracts import Fault
from tianshu_companion.preparation_retry import read_retry, retry_preparation


class CongestionTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_nonexecuted_read_timeout_returns_to_the_logical_owner_once(self):
        seen = []

        async def handle(request):
            seen.append((request.content, request.headers))
            if len(seen) == 1:
                return httpx.Response(
                    408, json=dict(code="timeout", execution_state="not_started", retryable=True)
                )
            return httpx.Response(200, json={"ok": True})

        client = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handle)
        )
        self.addAsyncCleanup(client.close)
        with self.assertRaises(Fault) as caught:
            await client.call("/internal/v1/memory/context/query", {"request_id": "same"})
        self.assertTrue(caught.exception.retryable_read)
        self.assertEqual(caught.exception.read_execution, "not_started")
        self.assertEqual(len(seen), 1)

    async def test_timeout_without_nonexecution_proof_and_commands_never_replay(self):
        for path, state, retryable in [
            ("/internal/v1/memory/context/query", "unknown", True),
            ("/internal/v1/memory/context/query", "not_started", False),
            ("/internal/v1/memory/context/query", None, True),
            ("/internal/v1/identity/register", "not_started", True),
        ]:
            calls = []

            async def handle(request):
                calls.append(request)
                return httpx.Response(
                    408, json=dict(code="timeout", execution_state=state, retryable=retryable)
                )

            client = JsonService(
                "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handle)
            )
            self.addAsyncCleanup(client.close)
            with self.assertRaises(Fault):
                await client.call(
                    path, {"request_id": "same"}, uncertain_write=path.endswith("register")
                )
            self.assertEqual(len(calls), 1)

    async def test_preparation_is_globally_bounded_and_queued_conversations_drain(self):
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        gate = asyncio.Event()
        h.gateway.gates[1] = gate
        for i in range(12):
            account = str(1000 + i)
            await h.ingest(account=account, channel="private:" + account, text="刚泡了一杯茶")
        await h.cycles()
        self.assertEqual(len(h.gateway.calls), h.core.preparation_slots)
        self.assertEqual(sum(t["phase"] == "generating" for t in h.turns()), 4)
        self.assertEqual(sum(t["phase"] == "preparing" for t in h.turns()), 4)
        self.assertEqual(sum(t["phase"] == "queued" for t in h.turns()), 4)
        gate.set()
        await h.cycles(180)
        self.assertEqual(len(h.gateway.calls), 12)
        self.assertTrue(all(t["phase"] == "sent" for t in h.turns()))

    async def test_explicit_query_congestion_returns_to_owner_and_never_replays_a_command(self):
        seen = []

        async def handle(request):
            seen.append((request.content, request.headers))
            if len(seen) == 1:
                return httpx.Response(
                    429, json=dict(code="queue_full", execution_state="not_started", retryable=True)
                )
            return httpx.Response(200, json={"ok": True})

        client = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handle)
        )
        self.addAsyncCleanup(client.close)
        with self.assertRaises(Fault) as caught:
            await client.call("/internal/v1/memory/select", {"request_id": "same"})
        self.assertTrue(caught.exception.retryable_read)
        self.assertEqual(len(seen), 1)
        seen.clear()
        with self.assertRaises(Fault):
            await client.call("/internal/v1/identity/register", {"request_id": "command"})
        self.assertEqual(1, len(seen))

    def test_first_recoverable_failure_after_normal_preparation_gets_its_own_window(self):
        turn = dict(
            model_calls=0,
            created_at="2026-10-23T00:00:00Z",
            timings={"started_at": "2026-10-23T00:00:00Z"},
            source_deadline=160,
        )
        self.assertTrue(retry_preparation(turn, Fault("queue_full"), 131))
        self.assertEqual(turn["preparation_retry_started_at"], 131)
        self.assertEqual(turn["preparation_retries"], 1)
        self.assertGreater(turn["retry_at"], 131)
        self.assertIsNone(read_retry({}, Fault("queue_full"), 160, 160))

    def test_read_policy_is_bounded_and_does_not_replay_commands_models_or_unknown_writes(self):
        for error in (
            Fault("queue_full", unknown=True),
            Fault("dependency_unavailable", retryable_model=True),
            Fault("queue_full", cause={"operation": "command"}),
            Fault("queue_full", cause={"operation": "stream"}),
            Fault("queue_full", cause={"operation": "query", "http_status": 429}),
        ):
            self.assertIsNone(read_retry({}, error, 100, 200))
        error = Fault("queue_full", retryable_read=True)
        error.retry_after_seconds = 6
        for source_bound in (200, None):
            with self.subTest(source_bound=source_bound):
                state = {}
                self.assertEqual(read_retry(state, error, 100, source_bound), 6)
                self.assertEqual(read_retry(state, error, 107, source_bound), 6)
                self.assertIsNone(read_retry(state, error, 125, source_bound))
                state = {}
                for attempt in range(5):
                    self.assertIsNotNone(
                        read_retry(state, Fault("queue_full"), 100 + attempt, source_bound)
                    )
                self.assertIsNone(read_retry(state, Fault("queue_full"), 105, source_bound))

    async def test_prepare_recovery_window_bounds_running_reads(self):
        from test_delivery_v2 import source_turn

        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        turn = await source_turn(h)
        turn.update(
            phase="preparing",
            config_version=h.core.config_version,
            bootstrap_until=h.clock() + 5,
            preparation_retry_started_at=h.clock() - 29.96,
        )
        h.core.store.put("turns", turn)
        entered, stopped = asyncio.Event(), asyncio.Event()

        async def blocked(*args, **kwargs):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        h.memory.select = blocked
        await asyncio.wait_for(h.core._process(turn["id"]), 1)
        fresh = h.core.store.get("turns", turn["id"])
        self.assertTrue(entered.is_set() and stopped.is_set())
        self.assertEqual(fresh["phase"], "failed")
        self.assertEqual(fresh["model_calls"], 0)
        self.assertEqual(fresh["last_failure"]["stage"], "preparation")
        self.assertEqual(fresh["failure"], "timeout")

    async def test_synchronous_work_cannot_disable_an_expired_prepare_recovery_timer(self):
        from test_delivery_v2 import source_turn

        for stage in ("context", "store", "normal"):
            with self.subTest(stage=stage):
                h = Harness(silence_ms=0)
                try:
                    turn = await source_turn(h)
                    turn.update(
                        phase="preparing",
                        config_version=h.core.config_version,
                        bootstrap_until=h.clock() + 5,
                    )
                    if stage != "normal":
                        turn["preparation_retry_started_at"] = h.clock() - 29
                    h.core.store.put("turns", turn)
                    prepare = h.core._prepare_context
                    save = h.core._save_turn

                    async def synchronous_context(*args):
                        result = await prepare(*args)
                        h.clock.advance(31 if stage == "normal" else 2)
                        return result

                    def synchronous_store(value):
                        save(value)
                        if value["phase"] == "generating":
                            h.clock.advance(2)

                    if stage == "store":
                        h.core._save_turn = synchronous_store
                    else:
                        h.core._prepare_context = synchronous_context
                    await h.core._process(turn["id"])
                    fresh = h.core.store.get("turns", turn["id"])
                    if stage == "normal":
                        self.assertEqual(len(h.gateway.calls), 1)
                        self.assertEqual(fresh["model_calls"], 1)
                    else:
                        self.assertEqual(h.gateway.calls, [])
                        self.assertEqual(fresh["model_calls"], 0)
                        self.assertEqual(fresh["phase"], "failed")
                        self.assertEqual(fresh["failure"], "timeout")
                finally:
                    await h.core.close()

    async def test_synchronous_authority_completion_cannot_escape_read_recovery_deadline(self):
        from test_delivery_v2 import source_turn

        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        turn = await source_turn(h)
        turn.update(model_calls=1, source_deadline=h.clock() + 600)
        h.core.store.put("turns", turn)
        attempts = []

        async def synchronous_read(value):
            attempts.append(True)
            if len(attempts) == 1:
                raise Fault("dependency_unavailable", retryable_read=True)
            h.clock.advance(31)

        h.core._preflight_once = synchronous_read
        with self.assertRaises(Fault) as caught:
            await h.core._preflight(turn)
        self.assertEqual(caught.exception.code, "timeout")
        self.assertEqual(len(attempts), 2)
        self.assertEqual(h.gateway.calls, [])
        self.assertEqual(h.sender.calls, [])

    async def test_preparation_recovers_without_replaying_model(self):
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        select = h.memory.select
        attempts = 0

        async def busy(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise Fault("queue_full")
            return await select(*args, **kwargs)

        h.memory.select = busy
        await h.ingest(text="我平时喝茶")
        await h.cycles()
        self.assertEqual([], h.gateway.calls)
        self.assertEqual("preparing", h.turns()[0]["phase"])
        h.clock.advance(1)
        await h.cycles()
        self.assertEqual("sent", h.turns()[0]["phase"])
        self.assertEqual(1, len(h.gateway.calls))

    async def test_model_failure_is_not_replayed_and_preparation_retries_are_bounded(self):
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        h.gateway.fail = True
        await h.ingest(text="早上好")
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertEqual(1, len(h.gateway.calls))
        h.gateway.fail = False

        async def busy(*args, **kwargs):
            raise Fault("queue_full")

        h.memory.select = busy
        await h.ingest(text="再来一句")
        for _ in range(8):
            await h.cycles()
            h.clock.advance(3)
        self.assertEqual("failed", h.turns()[-1]["phase"])
        self.assertGreater(h.turns()[-1]["preparation_retries"], 0)
        self.assertLessEqual(h.turns()[-1]["preparation_retries"], 5)
