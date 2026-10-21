"""Scheduling uses real Store queries; models and transport remain synthetic."""

import asyncio
import copy
from unittest.mock import patch

import pytest

from support import Harness
from tianshu_companion.store import Store
from tianshu_companion.storage_postgres import translate


def occupy_preparation(h):
    gate = asyncio.Event()
    for index in range(h.core.preparation_slots):
        key = f"busy:{index}"
        h.core.store.put(
            "turns",
            dict(id=key, conversation_id="zzzz:busy", phase="preparing", retry_at=0),
        )
        h.core.jobs[key] = asyncio.create_task(gate.wait())


def queued_rooms(store, count):
    for index in range(count):
        cid = f"room:{index:03}"
        store.put("conversations", dict(id="channel:" + cid, conversation_id=cid))
        store.put(
            "turns",
            dict(
                id=f"queued:{index}",
                conversation_id=cid,
                phase="queued",
                large_context_fixture="payload" * 1000,
            ),
        )


def test_work_candidates_match_current_states_and_direct_waiting_predicate():
    async def run():
        h = Harness()
        try:
            store = h.core.store
            queued_rooms(store, 3)
            for index, phase in enumerate(
                ["queued", "queued", "preparing", "waiting_dependency", "ready_to_send"]
            ):
                store.put(
                    "turns",
                    dict(
                        id=f"mixed:{index}",
                        conversation_id="room:000",
                        phase=phase,
                        sequence=index + 1,
                        retry_at=12,
                    ),
                )
            store.put("replies", dict(id="late", conversation_id="room:001", state="unknown"))
            for index, (state, reply_state, reason) in enumerate(
                [
                    ("completed", "ready_to_deliver", "outbound_band_busy"),
                    ("failed", "ready_to_deliver", "outbound_band_busy"),
                    ("pending", "ready_to_deliver", "outbound_band_busy"),
                    ("completed", "sent", "outbound_band_busy"),
                    ("completed", "ready_to_deliver", "other"),
                ]
            ):
                cid = f"direct:{index}"
                store.put(
                    "direct_requests",
                    dict(
                        id=cid,
                        conversation_id=cid,
                        state=state,
                        reply_state=reply_state,
                        deferred_reason=reason,
                    ),
                )
            ids = store.work_conversations()
            with patch.object(store, "get", side_effect=AssertionError("Full turn lookup")):
                candidates = store.work_candidates(ids)
            mixed = candidates["room:000"]
            assert mixed["conversation"]["id"] == "channel:room:000"
            assert mixed["active"] == 3
            assert mixed["queued"] == ["queued:0", "mixed:0"]
            assert mixed["retry"] == [("mixed:2", 12), ("mixed:3", 12)]
            assert mixed["deliver"]
            assert candidates["room:001"]["deliver"]
            assert not candidates["room:002"]["deliver"]
            for index in range(5):
                cid = f"direct:{index}"
                flag = candidates.get(cid, {}).get("direct", False)
                assert flag == h.core.direct.waiting_for_band(cid)
        finally:
            await h.core.close()

    asyncio.run(run())


def test_saturated_tick_batches_real_store_reads_without_turn_bodies_or_empty_delivery():
    async def run():
        h = Harness()
        try:
            await h.core.tick()
            queued_rooms(h.core.store, 140)
            occupy_preparation(h)
            queries = []
            original_get = h.core.store.get

            def get(table, key):
                assert table != "turns", "Saturated scheduling must not load a turn body"
                return original_get(table, key)

            h.core.store.db.set_trace_callback(queries.append)
            with (
                patch.object(h.core.store, "get", get),
                patch.object(h.core.store, "turn_phases", wraps=h.core.store.turn_phases) as phases,
            ):
                await h.core.tick()
                assert phases.call_count == 1
            selects = [q for q in queries if q.startswith("SELECT")]
            assert len(selects) < 64, f"One page repeated per-room reads: {len(selects)}"
            assert not h.core.send_jobs
            assert h.core._work_cursor == "room:063"
            await h.core.tick()
            assert h.core._work_cursor == "room:127"
            await h.core.tick()
            assert h.core._work_cursor == "zzzz:busy"
            await h.core.tick()
            assert h.core._work_cursor == "room:063"
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("change", ["cancel", "quarantine", "two_active", "unreleased"])
def test_queued_hint_is_rechecked_before_claim(change):
    async def run():
        h = Harness(silence_ms=0)
        try:
            await h.ingest(text="synthetic queued request")
            queued = h.turns()[0]
            original = h.core.store.work_candidates

            def candidates(ids):
                result = original(ids)
                if change == "cancel":
                    queued["cancelled"] = True
                    h.core.store.put("turns", queued)
                elif change == "quarantine":
                    conv = h.core.store.conversation(queued["conversation_id"])
                    conv["source_quarantined"] = True
                    h.core.store.put("conversations", conv)
                elif change == "two_active":
                    for index in range(2):
                        active = copy.deepcopy(queued)
                        active.update(id=f"existing:{index}", phase="generating")
                        h.core.store.put("turns", active)
                else:
                    inbox = h.core.store.list("inbox")[0]
                    inbox["response_released"] = False
                    h.core.store.put("inbox", inbox)
                return result

            with patch.object(h.core.store, "work_candidates", candidates):
                await h.core.tick()
            assert h.core.store.get("turns", queued["id"])["phase"] == "queued"
            assert not h.core.jobs
            assert not h.gateway.calls
        finally:
            await h.core.close()

    asyncio.run(run())


def test_retry_hint_rechecks_the_current_retry_time():
    async def run():
        h = Harness(silence_ms=0)
        try:
            await h.ingest()
            turn = h.turns()[0]
            turn.update(phase="waiting_dependency", retry_at=0)
            h.core.store.put("turns", turn)
            original = h.core.store.work_candidates

            def candidates(ids):
                result = original(ids)
                turn["retry_at"] = h.clock() + 60
                h.core.store.put("turns", turn)
                return result

            with patch.object(h.core.store, "work_candidates", candidates):
                await h.core.tick()
            assert not h.core.jobs
            assert not h.gateway.calls
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("closed", [False, True])
def test_unknown_recovery_progresses_with_queued_work_and_all_preparation_slots_busy(closed):
    async def run():
        h = Harness(silence_ms=0, delivery_reconcile_timeout_ms=1000)
        try:
            h.gateway.segments = ["synthetic answer"]
            h.sender.states = ["unknown"]
            await h.ingest()
            await h.cycles()
            first = h.turns()[0]
            assert first["phase"] == "reconciling"
            if closed:
                h.clock.advance(2)
                await h.cycles()
                assert h.turns()[0]["phase"] == "closed_unknown"
            await h.ingest(text="queued while recovering")
            queued_id = h.turns()[1]["id"]
            occupy_preparation(h)
            request = h.sender.calls[0]
            h.sender.answers[request["reply_id"]] = h.sender.receipt(request)
            await h.cycles(5)
            assert h.core.store.get("replies", request["reply_id"])["state"] == "sent"
            assert h.core.store.get("turns", first["id"])["phase"] == (
                "closed_unknown" if closed else "sent"
            )
            assert h.core.store.get("turns", queued_id)["phase"] == "queued"
            assert len(h.sender.calls) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_unknown_cursor_only_wraps_after_a_nonempty_cursor():
    async def run():
        h = Harness()
        try:
            queued_rooms(h.core.store, 1)
            queries = []
            h.core.store.db.set_trace_callback(queries.append)
            await h.core._deliver("room:000")
            assert len([q for q in queries if "FROM replies INDEXED BY replies_unknown" in q]) == 1
            h.core.store.put(
                "replies", dict(id="a", conversation_id="room:000", state="unknown", turn_id="old")
            )
            h.core._reconcile_cursors["room:000"] = "z"
            queries.clear()
            await h.core._deliver("room:000")
            assert len([q for q in queries if "FROM replies INDEXED BY replies_unknown" in q]) == 2
            assert h.core._reconcile_cursors["room:000"] == "a"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_batch_queries_keep_postgres_translation_and_partial_index_predicates():
    store = Store(":memory:")
    try:
        queued_rooms(store, 1)
        statements = []
        store.db.set_trace_callback(statements.append)
        store.work_candidates(["room:000"])
        store.work_turn_count("room:000", ["preparing", "generating"])
        native = [translate(sql) for sql in statements]
        assert not any("INDEXED BY" in sql or "json_extract" in sql for sql in native)
        retry = next(sql for sql in native if "CASE WHEN status IN" in sql)
        assert "double precision" in retry
        assert "status IN ('queued','preparing','generating','waiting_dependency'," in retry
        direct = next(sql for sql in native if "FROM direct_requests" in sql)
        assert "status IN ('completed','failed')" in direct
        assert "{reply_state}" in direct and "{deferred_reason}" in direct
    finally:
        store.close()
