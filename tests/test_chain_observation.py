"""Observe real Core boundaries without changing their authority or receipt rules."""

import asyncio

import pytest

from support import Harness
from test_actor_memory import close as close_reader
from test_actor_memory import read as read_original
from test_actor_memory import setup as reader
from test_delivery_v2 import NativeQueue, source_turn
from tianshu_companion import observability as obs
from tianshu_companion.contracts import Fault


class Events:
    def __init__(self):
        self.records = []

    def emit(self, event, outcome, **fields):
        self.records.append(dict(event=event, outcome=obs.runtime_outcome(outcome), **fields))
        return True

    def stages(self, stage):
        return [row for row in self.records if row["event"].startswith(stage + ".")]


@pytest.mark.parametrize("fails", [False, True])
def test_memory_read_event_matches_actual_read_and_stops_before_model_on_failure(fails):
    async def run():
        h, events = Harness(silence_ms=0), Events()
        h.core._events = events
        try:
            with obs.correlation_scope("a" * 32):
                await h.ingest(text="用于明确读取边界的合成输入")
            if fails:
                h.memory.unavailable = True
            await h.cycles(100)
            records = events.stages("turn.memory")
            assert [row["outcome"] for row in records] == [
                "started",
                "failed" if fails else "succeeded",
            ]
            assert {row["correlation_id"] for row in records} == {"a" * 32}
            assert records[-1]["duration_ms"] >= 0
            if fails:
                assert h.gateway.calls == h.sender.calls == []
                assert records[-1]["error_code"] == "dependency_unavailable"
            else:
                assert h.gateway.calls and h.sender.calls
                assert events.stages("turn.delivery")[-1]["outcome"] == "succeeded"
            assert events.stages("turn.knowledge") == []
        finally:
            await h.core.close()

    asyncio.run(run())


def test_knowledge_read_and_withdrawal_events_wrap_owner_checks_without_logging_text():
    async def run():
        h, owner, services = await reader()
        events = Events()
        h.core._events = events
        try:
            with obs.correlation_scope("b" * 32):
                await read_original(h)
                owner.withdrawn = True
                with pytest.raises(Fault):
                    await read_original(h, key="read:withdrawn")
            records = events.stages("turn.knowledge")
            assert [row["outcome"] for row in records] == [
                "started",
                "succeeded",
                "started",
                "failed",
            ]
            assert {row["correlation_id"] for row in records} == {"b" * 32}
            assert all(
                row["duration_ms"] >= 0 for row in records if row["event"].endswith("finished")
            )
            assert all(
                set(row) <= {"event", "outcome", "correlation_id", "duration_ms", "error_code"}
                for row in records
            )
            assert owner.prose not in str(records)
        finally:
            await close_reader(h, services)

    asyncio.run(run())


def test_native_reply_unknown_is_observed_without_retry_or_delivery_success():
    async def run():
        h, events = Harness(silence_ms=0), Events()
        h.core._events = events
        queue = NativeQueue(h.clock)
        queue.states = ["unknown"]
        h.core.sender = queue
        try:
            turn = await source_turn(h)
            with obs.correlation_scope("c" * 32):
                await h.core.stream_segment(turn["id"], "合成回复")
            records = events.stages("turn.delivery")
            # An open expression with an unknown segment has a real partial
            # receipt: progress is degraded, never a confirmed delivery.
            assert [row["outcome"] for row in records] == ["started", "degraded"]
            assert records[-1]["duration_ms"] >= 0
            assert {row["correlation_id"] for row in records} == {"c" * 32}
            assert len(queue.requests) == 1
            assert h.core.store.list("replies")[0]["state"] == "unknown"
        finally:
            await h.core.close()

    asyncio.run(run())
