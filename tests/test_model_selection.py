import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from support import Harness
from tianshu_companion.model_selection import ModelSelection


class Selector:
    def __init__(self, h):
        self.h = h
        self.version = 10
        self.gates = {}
        self.requests = []
        self.revoked = False
        self.lease = 3600
        self.fail = False

    async def select(self, request):
        assert not self.h.core.store.db.in_transaction
        self.requests.append(request)
        version = self.version
        if request.actor_id in self.gates:
            await self.gates[request.actor_id].wait()
        if self.fail:
            raise RuntimeError("synthetic-private-secret")
        return ModelSelection(version, self.h.clock() + self.lease, self.revoked)


class ModelSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.h = Harness(Path(self.directory.name) / "state.db", silence_ms=0)
        self.selector = Selector(self.h)
        await self.h.core.close()
        self.h.core = self.h.new_core(default_model_selector=self.selector)

    async def asyncTearDown(self):
        await self.h.core.close()
        self.directory.cleanup()

    async def test_switch_keeps_inflight_version_and_new_turn_uses_default(self):
        h = self.h
        h.gateway.gates[1] = asyncio.Event()
        await h.ingest(text="first")
        await h.cycles()
        self.assertEqual(
            (10, "generating"), (h.turns()[0]["config_version"], h.turns()[0]["phase"])
        )
        self.selector.version = 20
        await h.ingest(text="second", actor="actor:b")
        await h.cycles()
        self.assertEqual([10, 20], [t["config_version"] for t in h.turns()])
        h.gateway.gates[1].set()
        await h.cycles(70)
        self.assertEqual(["sent", "sent"], [t["phase"] for t in h.turns()])
        self.assertEqual([10, 20], [t["config_version"] for t in h.turns()])

    async def test_slow_selection_does_not_lock_store_or_drift_other_turn(self):
        h = self.h
        self.selector.gates["actor:a"] = asyncio.Event()
        await h.ingest(text="slow")
        await h.cycles(5)
        self.selector.version = 20
        await h.ingest(text="fast", actor="actor:b")
        await h.cycles()
        self.assertEqual([None, 20], [t["config_version"] for t in h.turns()])
        self.assertEqual(2, len(self.selector.requests))
        self.selector.gates["actor:a"].set()
        await h.cycles(70)
        self.assertEqual([10, 20], [t["config_version"] for t in h.turns()])
        self.assertTrue(all(t["phase"] == "sent" for t in h.turns()))

    async def test_cancel_wins_over_late_selection_failure(self):
        h = self.h
        gate = self.selector.gates["actor:a"] = asyncio.Event()
        await h.ingest()
        await h.cycles(5)
        await h.cancel(h.turns()[0])
        self.selector.fail = True
        gate.set()
        await h.cycles()
        self.assertEqual("cancelled", h.turns()[0]["phase"])
        self.assertIsNone(h.turns()[0]["config_version"])
        self.assertFalse(h.gateway.calls)

    async def test_revocation_and_short_lease_fail_without_static_fallback(self):
        h = self.h
        for revoked, lease in [(True, 3600), (False, 10)]:
            self.selector.revoked, self.selector.lease = revoked, lease
            await h.ingest()
            await h.cycles()
            turn = h.turns()[-1]
            self.assertEqual("failed", turn["phase"])
            self.assertIsNone(turn["config_version"])
        self.assertFalse(h.gateway.calls)

    async def test_retraction_wins_over_late_successful_selection(self):
        h = self.h
        gate = self.selector.gates["actor:a"] = asyncio.Event()
        await h.ingest(message="selection:retract")
        await h.cycles(5)
        await h.ingest(message="selection:retract", revision=2, kind="retract")
        gate.set()
        await h.cycles()
        self.assertEqual("cancelled", h.turns()[0]["phase"])
        self.assertIsNone(h.turns()[0]["config_version"])
        self.assertFalse(h.gateway.calls)

    async def test_selector_exception_is_sanitized_and_context_comes_from_turn(self):
        h = self.h
        self.selector.fail = True
        await h.ingest()
        await h.cycles()
        turn = h.turns()[0]
        self.assertEqual("dependency_unavailable", turn["failure"])
        self.assertNotIn("synthetic-private-secret", str(turn))
        request = self.selector.requests[0]
        self.assertEqual(turn["id"], request.turn_id)
        for key, value in turn["scope"].items():
            self.assertEqual(value, getattr(request, key))
        self.assertEqual("companion", request.caller_service)
        self.assertEqual("companion.text", request.workload)

    async def test_timeout_fails_and_does_not_expose_adapter_error(self):
        h = self.h
        self.selector.gates["actor:a"] = asyncio.Event()
        with patch("tianshu_companion.model_selection.SELECTION_TIMEOUT", 0.001):
            await h.ingest()
            await h.cycles(70)
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertEqual("dependency_unavailable", h.turns()[0]["failure"])
        self.assertFalse(h.gateway.calls)

    async def test_restart_keeps_pinned_versions_and_does_not_reselect_interrupted(self):
        h = self.h
        h.gateway.gates[1] = asyncio.Event()
        await h.ingest()
        await h.cycles()
        pinned = h.turns()[0]["id"]
        await h.core.close()
        self.selector.version = 30
        h.core = h.new_core(default_model_selector=self.selector)
        h.core.recover()
        self.assertEqual(10, h.core.store.get("turns", pinned)["config_version"])
        self.assertEqual("failed", h.core.store.get("turns", pinned)["phase"])
        await h.ingest(text="after restart")
        await h.cycles(70)
        self.assertEqual([10, 30], [t["config_version"] for t in h.turns()])
        self.assertEqual(2, len(self.selector.requests))

    async def test_lease_consumed_waiting_for_model_slot_fails_without_reselect(self):
        h = self.h
        self.selector.lease = 70
        h.core.models = asyncio.Semaphore(0)
        await h.ingest()
        await h.cycles()
        self.assertEqual(10, h.turns()[0]["config_version"])
        self.selector.version = 20
        h.clock.advance(10)
        h.core.models.release()
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertEqual(10, h.turns()[0]["config_version"])
        self.assertEqual(1, len(self.selector.requests))
        self.assertFalse(h.gateway.calls)

    async def test_dependency_retry_keeps_selection_when_default_changes(self):
        h = self.h
        h.gateway.gates[1] = asyncio.Event()
        await h.ingest(text="制定方案")
        await h.ingest(text="按你刚才的方案办")
        await h.cycles()
        self.assertEqual("waiting_dependency", h.turns()[1]["phase"])
        self.selector.version = 20
        h.gateway.gates[1].set()
        await h.cycles(70)
        self.assertEqual([10, 10], [t["config_version"] for t in h.turns()])
        self.assertTrue(all(t["phase"] == "sent" for t in h.turns()))
        self.assertEqual(2, len(self.selector.requests))
