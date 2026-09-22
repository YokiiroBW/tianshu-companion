"""Startup disable policy, durable backlog, and uncertain writes using synthetic data."""

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx

from support import Harness, contracts
from tianshu_companion.app import build_runtime, create_app
from tianshu_companion.clients import JsonService, Memory
from tianshu_companion.contracts import Fault
from tianshu_companion.runtime_capabilities import PATH, read_capabilities


class CandidatePolicyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.h = Harness(Path(self.temp.name) / "synthetic.db", silence_ms=0)

    async def asyncTearDown(self):
        await self.h.core.close()

    async def restart(self, enabled):
        await self.h.core.close()
        self.h.core = self.h.new_core(automatic_memory_candidates=enabled)
        self.h.core.recover()

    def rows(self):
        return self.h.core.store.db.execute("SELECT * FROM outbox ORDER BY id").fetchall()

    async def turn(self):
        await self.h.ingest()
        await self.h.cycles()
        self.assertEqual("sent", self.h.turns()[-1]["phase"])

    async def test_disabled_300_turns_in_one_conversation_create_no_backlog(self):
        await self.restart(False)
        h = self.h
        for index in range(300):
            await self.turn()
            await h.core.flush_outbox()
            self.assertEqual([], self.rows(), index)
            if index == 149:
                await self.restart(False)
        self.assertEqual(300, len(h.turns()))
        self.assertEqual(300, len(h.gateway.calls))
        self.assertEqual(600, len(h.sender.calls))
        self.assertEqual([], h.memory.commits)
        state = read_capabilities(h.core)
        self.assertEqual("disabled", state["automatic_memory_candidates"]["generation"])
        self.assertEqual("paused", state["automatic_memory_candidates"]["submission"])
        self.assertFalse(any(state["automatic_memory_candidates"]["retained_outbox"].values()))
        self.assertEqual({"enabled": False, "state": "not_integrated"}, state["chat_audit"])

    async def test_omitted_and_explicit_enabled_keep_candidate_acceptance_not_memory_success(self):
        for enabled in (None, True):
            if enabled is not None:
                await self.restart(enabled)
            await self.turn()
            await self.h.core.flush_outbox()
        self.assertEqual(2, len(self.h.memory.commits))
        self.assertTrue(all(row[3] == "delivered" for row in self.rows()))
        for item in self.h.core.store.list("outbox"):
            self.assertFalse(item["receipt"]["confirmed_memory_written"])
        self.assertEqual(
            "not_verified",
            read_capabilities(self.h.core)["automatic_memory_candidates"][
                "memory_write_verification"
            ],
        )

    async def test_disabled_preserves_all_old_states_and_explicit_enable_never_replays_unknown(
        self,
    ):
        h = self.h
        for state in ("pending", "blocked_scope", "submitting", "unknown"):
            await self.turn()
            item = h.core.store.list("outbox")[-1]
            item["state"] = state
            if state == "blocked_scope":
                item["event"]["scope_version"] = None
            h.core.store.put("outbox", item)
        before = self.rows()
        await self.restart(False)
        state = read_capabilities(h.core)["automatic_memory_candidates"]
        self.assertTrue(all(state["retained_outbox"].values()))
        with mock.patch.object(h.memory, "check_sources", side_effect=AssertionError("no repair")):
            await h.core.flush_outbox()
            await h.core.repair_blocked_scope(h.turns()[1]["id"], h.memory.check_sources)
        await self.turn()
        await self.restart(False)
        await h.core.flush_outbox()
        self.assertEqual(before, self.rows())
        self.assertEqual([], h.memory.commits)
        await self.restart(True)
        await h.core.flush_outbox()
        self.assertEqual(2, len(h.memory.commits))
        states = [r[3] for r in self.rows()]
        self.assertEqual(2, states.count("unknown"))
        self.assertEqual(2, states.count("delivered"))
        await self.restart(True)
        await h.core.flush_outbox()
        self.assertEqual(2, len(h.memory.commits))

    async def test_unknown_memory_submission_is_durable_and_never_retried(self):
        await self.turn()
        h = self.h
        calls = []

        async def uncertain(event):
            calls.append(event)
            self.assertEqual("submitting", h.core.store.list("outbox")[0]["state"])
            raise Fault("result_unknown", unknown=True)

        h.memory.commit = uncertain
        await h.core.flush_outbox()
        self.assertEqual("unknown", h.core.store.list("outbox")[0]["state"])
        await self.restart(False)
        await self.restart(True)
        h.clock.advance(600)
        await h.core.flush_outbox()
        self.assertEqual(1, len(calls))

    async def test_cancelled_commit_and_concurrent_flush_cannot_duplicate_submission(self):
        await self.turn()
        entered = asyncio.Event()
        calls = []

        async def wait_forever(event):
            calls.append(event)
            entered.set()
            await asyncio.Event().wait()

        self.h.memory.commit = wait_forever
        first = asyncio.create_task(self.h.core.flush_outbox())
        await asyncio.wait_for(entered.wait(), 2)
        second = asyncio.create_task(self.h.core.flush_outbox())
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        await second
        self.assertEqual("unknown", self.h.core.store.list("outbox")[0]["state"])
        self.assertEqual(1, len(calls))

    async def test_disabled_unknown_delivery_retains_reconciliation_without_candidates(self):
        await self.restart(False)
        h = self.h
        h.sender.states = ["unknown"]
        await h.ingest()
        await h.cycles()
        self.assertEqual("reconciling", h.turns()[0]["phase"])
        await self.restart(False)
        h.clock.advance(31)
        await h.cycles()
        self.assertEqual("closed_unknown", h.turns()[0]["phase"])
        self.assertEqual(1, len(h.sender.calls))
        self.assertEqual([], self.rows())
        reply = h.core.store.list("replies")[0]
        receipt = h.sender.receipt(reply["request"])
        h.core.record_receipt(reply["id"], receipt)
        await h.core.flush_outbox()
        self.assertEqual(["local_projection"], [r[3] for r in self.rows()])
        self.assertEqual([], h.memory.commits)

    async def test_capability_http_authentication_and_byte_for_byte_read_only(self):
        await self.restart(False)
        token = "synthetic-diagnostics"
        with mock.patch.dict(os.environ, {"TIANSHU_DIAGNOSTICS_TOKEN": token}):
            app = create_app(self.h.core, {"platform": "synthetic-chat"})
        self.addCleanup(app.state.log.close)
        db = self.h.core.store.db
        before = (db.total_changes, list(db.iterdump()))
        with mock.patch.object(
            app.state.log, "emit", side_effect=AssertionError("read must not log")
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                for credential in (None, "synthetic-chat", "wrong"):
                    response = await client.get(
                        PATH, headers={"Authorization": f"Bearer {credential}"}
                    )
                    self.assertEqual(401, response.status_code)
                response = await client.get(PATH, headers={"Authorization": f"Bearer {token}"})
                self.assertEqual(200, response.status_code)
                self.assertEqual(read_capabilities(self.h.core), response.json())
        self.assertEqual(before, (db.total_changes, list(db.iterdump())))

    async def test_configuration_validation_precedes_any_database_open(self):
        for invalid in ("false", 0, 1, None, {}, []):
            with self.subTest(value=invalid), mock.patch("tianshu_companion.app.Store") as store:
                with self.assertRaisesRegex(ValueError, "automatic_memory_candidates"):
                    build_runtime({"automatic_memory_candidates": invalid})
                store.assert_not_called()
        for value in (True, False):
            core, _, clients, _ = build_runtime(
                {
                    "contracts_path": os.environ["TIANSHU_CONTRACTS"],
                    "database_path": ":memory:",
                    "automatic_memory_candidates": value,
                }
            )
            self.assertIs(value, core.automatic_memory_candidates)
            with self.assertRaises(AttributeError):
                core.automatic_memory_candidates = not value
            await core.close()
            for client in clients:
                await client.close()

    async def test_status_refuses_missing_runtime_credentials_and_unreadable_database(self):
        for configured, token, closed in (
            (True, "", False),
            (False, "token", False),
            (True, "token", True),
        ):
            with mock.patch.dict(os.environ, {"TIANSHU_DIAGNOSTICS_TOKEN": token}):
                app = create_app(self.h.core if configured else None)
            self.addCleanup(app.state.log.close)
            if closed:
                await self.h.core.close()
            try:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test"
                ) as client:
                    response = await client.get(PATH, headers={"Authorization": f"Bearer {token}"})
                    self.assertEqual(503, response.status_code)
                    self.assertEqual({"code": "dependency_unavailable"}, response.json())
            finally:
                if closed:
                    self.h.core = self.h.new_core()


class MemoryWriteOutcomeTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_client_never_treats_lost_or_invalid_receipt_as_not_started(self):
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        await h.ingest()
        await h.cycles()
        event = h.core.store.list("outbox")[0]["event"]
        for response in (
            httpx.ReadError("synthetic lost response"),
            httpx.Response(200, content=b"invalid JSON"),
            httpx.Response(200, json={}),
            httpx.Response(503, json={"code": "dependency_unavailable"}),
            httpx.Response(409, json={"code": "result_unknown", "execution_state": "unknown"}),
        ):
            calls = []

            async def handler(request):
                calls.append(json.loads(request.content))
                if isinstance(response, Exception):
                    raise response
                return response

            client = JsonService(
                "https://synthetic.invalid",
                "synthetic-token",
                transport=httpx.MockTransport(handler),
            )
            self.addAsyncCleanup(client.close)
            with self.assertRaises(Fault) as raised:
                await Memory(contracts(), client).commit(event)
            self.assertTrue(raised.exception.unknown)
            self.assertEqual([event], calls)

    async def test_explicit_not_started_or_missing_configuration_retains_retry_semantics(self):
        async def handler(request):
            return httpx.Response(
                429, json={"code": "queue_full", "execution_state": "not_started"}
            )

        for url in (None, "https://synthetic.invalid"):
            client = JsonService(url, "synthetic-token", transport=httpx.MockTransport(handler))
            self.addAsyncCleanup(client.close)
            with self.assertRaises(Fault) as raised:
                await Memory(contracts(), client).commit({})
            self.assertFalse(raised.exception.unknown)
