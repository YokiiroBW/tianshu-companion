"""Native receipt recovery in the real Core/store, with a deterministic transport."""

import asyncio
import copy

import pytest

from support import Harness
from tianshu_companion.clients import utc
from tianshu_companion.contracts import Fault


class NativeQueue:
    available = True

    def __init__(self, clock):
        self.clock = clock
        self.requests = []
        self.states = ["sent", "unknown"]
        self.final = False
        self.finalize_calls = 0
        self.cancel_calls = 0

    def expression_available(self, channel):
        return True

    def receipt(self):
        segments = []
        for request, state in zip(self.requests, self.states):
            value = request["segments"][0]
            segments.append(
                dict(
                    segment_id=value["segment_id"],
                    reply_id=value["reply_id"],
                    segment_sequence=value["segment_sequence"],
                    state=state,
                    receipt_id="receipt:" + value["segment_id"],
                    retry_safe=False,
                    channel_message_ids=["sdk:" + value["segment_id"]] if state == "sent" else [],
                )
            )
        state = "sent" if all(item["state"] == "sent" for item in segments) else "partial"
        return dict(
            schema_version=2,
            request_id=self.requests[-1]["request_id"],
            expression_id=self.requests[-1]["expression_id"],
            final=self.final,
            state=state,
            segments=segments,
            observed_at=utc(self.clock()),
        )

    async def send_expression(self, request):
        self.requests.append(copy.deepcopy(request))
        return self.receipt()

    async def query_expression(self, expression_id):
        assert expression_id == self.requests[0]["expression_id"]
        return self.receipt()

    async def finalize_expression(self, expression_id, origin):
        self.finalize_calls += 1
        raise Fault("result_unknown", unknown=True)

    async def cancel_expression(self, expression_id):
        self.cancel_calls += 1
        self.final = True
        self.states = [state if state == "sent" else "cancelled" for state in self.states]
        return self.receipt()


async def source_turn(h):
    h.core.recover()
    await h.ingest()
    h.core._seal_due(h.clock())
    turn = h.turns()[0]
    turn.update(
        phase="generating", scope_version=h.memory.scope_version, role=h.core.roles["actor:a"]
    )
    h.core._save_turn(turn)
    return turn


def test_sealed_expression_receipt_finishes_while_earlier_turn_is_generating():
    async def run():
        h = Harness(silence_ms=0)
        queue = NativeQueue(h.clock)
        queue.states = ["unknown"]
        h.sender = h.core.sender = queue
        try:
            first = await source_turn(h)
            await h.ingest(text="第二条消息先生成完了")
            h.core._seal_due(h.clock())
            second = h.turns()[-1]
            assert first["id"] != second["id"]
            second.update(
                phase="generating",
                scope_version=h.memory.scope_version,
                role=h.core.roles["actor:a"],
            )
            h.core._save_turn(second)
            await h.core.stream_segment(second["id"], "已经送出的回复。")
            with pytest.raises(Fault):
                await h.core.finalize_stream(second["id"])
            queue.final = True
            await h.core._deliver(second["conversation_id"])
            assert h.core.store.get("turns", second["id"])["phase"] == "reconciling"
            assert len(queue.requests) == 1  # Unknown is queried, never resent.
            queue.states = ["sent"]
            await h.core._deliver(second["conversation_id"])
            assert h.core.store.get("turns", second["id"])["phase"] == "sent"
            assert h.core.store.get("turns", first["id"])["phase"] == "generating"
            assert len(queue.requests) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_unchanged_receipt_poll_does_not_advance_source_or_turn_versions():
    async def run():
        h = Harness(silence_ms=0)
        queue = NativeQueue(h.clock)
        queue.states = ["unknown"]
        h.sender = h.core.sender = queue
        try:
            turn = await source_turn(h)
            await h.core.stream_segment(turn["id"], "等待投递的合成消息")
            before = h.core.store.source_head()
            version = h.core.store.get("turns", turn["id"])["version"]
            receipt = queue.receipt()
            h.clock.advance(1)
            receipt.update(request_id="query:new", observed_at=utc(h.clock()))
            h.core.record_expression(turn["id"], receipt)
            assert h.core.store.source_head() == before
            assert h.core.store.get("turns", turn["id"])["version"] == version
            queue.states = ["sent"]
            h.core.record_expression(turn["id"], queue.receipt())
            assert h.core.store.source_head() != before
        finally:
            await h.core.close()

    asyncio.run(run())


def test_finalize_unknown_restarts_queries_original_and_keeps_late_ack(tmp_path):
    async def run():
        h = Harness(tmp_path / "core.db", silence_ms=0, delivery_reconcile_timeout_ms=1000)
        queue = NativeQueue(h.clock)
        h.sender = h.core.sender = queue
        try:
            turn = await source_turn(h)
            await h.core.stream_segment(turn["id"], "已经送出的第一片段。")
            await h.core.stream_segment(turn["id"], "第二片段结果暂时未知。")
            with pytest.raises(Fault):
                await h.core.finalize_stream(turn["id"])
            h.clock.advance(2)
            await h.core._reconcile(h.core.store.get("turns", turn["id"]))
            closed = h.core.store.get("turns", turn["id"])
            assert closed["phase"] == "closed_unknown" and closed["delivery_state"] == "partial"
            assert closed["unresolved_delivery"] is True
            assert len(queue.requests) == 2
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            queue.final = True
            queue.states[1] = "sent"
            await h.core._reconcile(h.core.store.get("turns", turn["id"]))
            assert all(reply["state"] == "sent" for reply in h.core._replies(turn))
            assert h.core.store.get("turns", turn["id"])["phase"] == "closed_unknown"
            assert len(queue.requests) == 2
            with pytest.raises(Fault):
                await h.core.finalize_stream(turn["id"])
            assert h.core.store.get("turns", turn["id"])["phase"] == "closed_unknown"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_cancel_queries_same_expression_and_never_rolls_back_sent_segment():
    async def run():
        h = Harness(silence_ms=0)
        queue = NativeQueue(h.clock)
        h.sender = h.core.sender = queue
        try:
            turn = await source_turn(h)
            await h.core.stream_segment(turn["id"], "已发第一段。")
            await h.core.stream_segment(turn["id"], "待取消第二段。")
            fresh = h.core.store.get("turns", turn["id"])
            assert h.core._cancel_turn(fresh, "user_requested") == "unknown"
            await h.core._reconcile(h.core.store.get("turns", turn["id"]))
            replies = h.core._replies(turn)
            assert replies[0]["state"] == "sent" and replies[0]["receipt"]["channel_message_ids"]
            assert replies[1]["transport_state"] == "cancelled"
            assert h.core.store.get("turns", turn["id"])["phase"] == "cancelled"
            assert len(queue.requests) == 2 and queue.cancel_calls == 1
        finally:
            await h.core.close()

    asyncio.run(run())
