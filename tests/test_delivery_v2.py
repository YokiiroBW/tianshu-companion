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
            h.clock.advance(0.25)
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
            h.clock.advance(0.25)
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


class MissingReceiptQueue(NativeQueue):
    def __init__(self, clock, *, query_failure=False):
        super().__init__(clock)
        self.queries = 0
        self.query_failure = query_failure
        self.states = ["unknown"]

    async def send_expression(self, request):
        self.requests.append(copy.deepcopy(request))
        raise Fault(
            "dependency_unavailable",
            cause=dict(
                category="fault",
                peer="channel",
                operation="command",
                http_status=503,
                exception_type="Fault",
                untrusted_payload="must-not-be-copied",
            ),
        )

    async def query_expression(self, expression_id):
        assert expression_id == self.requests[0]["expression_id"]
        self.queries += 1
        if self.query_failure:
            raise Fault("dependency_unavailable")
        return None


async def missing_receipt_turn(h, *, query_failure=False):
    queue = MissingReceiptQueue(h.clock, query_failure=query_failure)
    h.sender = h.core.sender = queue
    turn = await source_turn(h)
    with pytest.raises(Fault) as caught:
        await h.core.stream_segment(turn["id"], "synthetic unknown delivery")
    error = caught.value
    assert error.code == "result_unknown" and error.unknown
    assert error.cause == dict(
        category="fault",
        peer="channel",
        operation="command",
        http_status=503,
        exception_type="Fault",
    )
    assert error.companion_failure_stage == "delivery"
    fresh = h.core.store.get("turns", turn["id"])
    fresh.update(phase="reconciling", stream_open=False)
    h.core._save_turn(fresh)
    return queue, fresh


async def reconciliation_cadence_case(h, *, query_failure=False):
    queue, turn = await missing_receipt_turn(h, query_failure=query_failure)
    key = "delivery-reconcile:" + turn["id"]
    started = h.clock()
    before = h.core.store.source_head()
    version = turn["version"]
    await h.core._reconcile(turn)
    assert queue.queries == 1
    schedule = h.core.store.get("metadata", key)
    assert schedule["deadline"] == started + 0.25
    assert h.core.store.source_head() == before
    assert h.core.store.get("turns", turn["id"])["version"] == version
    for _ in range(30):
        await h.core.tick()
    assert queue.queries == 1 and not h.core.send_jobs
    # A direct reconciliation call also checks the durable schedule.
    await h.core._reconcile(h.core.store.get("turns", turn["id"]))
    assert queue.queries == 1
    terminal_deadline = started + h.core.policy.delivery_reconcile_timeout_ms / 1000
    while h.clock() < terminal_deadline:
        schedule = h.core.store.get("metadata", key)
        assert h.clock() < schedule["deadline"] <= min(h.clock() + 5, terminal_deadline)
        h.clock.advance(schedule["deadline"] - h.clock())
        await h.core._reconcile(h.core.store.get("turns", turn["id"]))
        if h.clock() < terminal_deadline:
            assert h.core.store.source_head() == before
    closed = h.core.store.get("turns", turn["id"])
    assert closed["phase"] == "closed_unknown"
    assert closed["delivery_state"] == "unknown" and closed["unresolved_delivery"]
    assert queue.queries <= 12
    schedule = h.core.store.get("metadata", key)
    assert schedule["deadline"] == h.clock() + 5
    h.clock.advance(5)
    await h.core._reconcile(closed)
    assert h.core.store.get("metadata", key)["deadline"] == h.clock() + 5
    # Actual callbacks remain immediate, including after historical closure.
    queue.states, queue.final = ["sent"], True
    h.core.record_expression(turn["id"], queue.receipt())
    assert h.core.store.get("metadata", key) is None
    assert h.core._replies(turn)[0]["state"] == "sent"
    assert h.core.store.get("turns", turn["id"])["phase"] == "closed_unknown"
    count = queue.queries
    h.clock.advance(10)
    await h.core.tick()
    assert queue.queries == count and len(queue.requests) == 1
    assert not h.gateway.calls


@pytest.mark.parametrize("query_failure", [False, True])
def test_missing_or_failed_receipt_poll_is_durable_bounded_and_not_a_source_fact(query_failure):
    async def run():
        h = Harness(silence_ms=0)
        try:
            await reconciliation_cadence_case(h, query_failure=query_failure)
        finally:
            await h.core.close()

    asyncio.run(run())


def test_reconciliation_backoff_survives_restart_and_cancel_resets_it(tmp_path):
    async def run():
        h = Harness(tmp_path / "cadence.sqlite", silence_ms=0)
        try:
            queue, turn = await missing_receipt_turn(h)
            await h.core._reconcile(turn)
            deadline = h.core.store.get("metadata", "delivery-reconcile:" + turn["id"])["deadline"]
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            await h.core._reconcile(h.core.store.get("turns", turn["id"]))
            assert queue.queries == 1
            assert (
                h.core.store.get("metadata", "delivery-reconcile:" + turn["id"])["deadline"]
                == deadline
            )
            fresh = h.core.store.get("turns", turn["id"])
            h.core._cancel_turn(fresh, "explicit_user_cancel")
            assert h.core.store.get("metadata", "delivery-reconcile:" + turn["id"]) is None
            await h.core._reconcile(h.core.store.get("turns", turn["id"]))
            assert queue.cancel_calls == 1 and len(queue.requests) == 1
            assert h.core.store.get("turns", turn["id"])["phase"] == "cancelled"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_closed_unknown_backoff_does_not_block_another_deliverable_turn():
    async def run():
        h = Harness(silence_ms=0, delivery_reconcile_timeout_ms=1000)
        h.gateway.segments = ["synthetic reply"]
        h.sender.states = ["unknown", "sent"]
        try:
            await h.ingest(text="first synthetic request")
            await h.cycles()
            h.clock.advance(1)
            await h.cycles()
            first = h.turns()[0]
            assert first["phase"] == "closed_unknown"
            await h.ingest(text="second synthetic request")
            await h.cycles()
            assert h.turns()[1]["phase"] == "sent"
            assert h.turns()[0]["phase"] == "closed_unknown"
            assert len(h.sender.calls) == 2
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


def test_reconcile_receipt_progress_resets_backoff_without_resetting_unknown_deadline():
    async def run():
        h = Harness(silence_ms=0)
        queue = NativeQueue(h.clock)
        queue.states = ["unknown"]
        h.sender = h.core.sender = queue
        try:
            turn = await source_turn(h)
            await h.core.stream_segment(turn["id"], "synthetic receipt progress")
            with pytest.raises(Fault):
                await h.core.finalize_stream(turn["id"])
            key = "delivery-reconcile:" + turn["id"]
            for _ in range(3):
                await h.core._reconcile(h.core.store.get("turns", turn["id"]))
                schedule = h.core.store.get("metadata", key)
                h.clock.advance(schedule["deadline"] - h.clock())
            assert h.core.store.get("metadata", key)["attempts"] == 3
            queue.states = ["sent"]
            h.core.record_expression(turn["id"], queue.receipt())
            assert h.core.store.get("metadata", key) is None
            await h.core._reconcile(h.core.store.get("turns", turn["id"]))
            assert h.core.store.get("metadata", key)["attempts"] == 1
            queue.final = True
            h.core.record_expression(turn["id"], queue.receipt())
            assert h.core.store.get("metadata", key) is None
            assert h.core.store.get("turns", turn["id"])["phase"] == "sent"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_reconcile_checks_current_schedule_after_a_due_hint():
    from unittest.mock import patch

    async def run():
        h = Harness(silence_ms=0)
        try:
            queue, turn = await missing_receipt_turn(h)
            await h.core._reconcile(turn)
            h.clock.advance(0.25)
            original = h.core.store.work_candidates
            key = "delivery-reconcile:" + turn["id"]

            def hints(ids):
                result = original(ids)
                schedule = h.core.store.get("metadata", key)
                schedule["deadline"] = h.clock() + 5
                h.core.store.put("metadata", schedule)
                return result

            with patch.object(h.core.store, "work_candidates", hints):
                await h.core.tick()
            assert queue.queries == 1 and len(queue.requests) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_slow_receipt_read_cannot_delay_the_existing_unknown_terminal_deadline():
    async def run():
        h = Harness(silence_ms=0, delivery_reconcile_timeout_ms=1000)
        stopped = asyncio.Event()
        try:
            queue, turn = await missing_receipt_turn(h)
            turn["expression_started_at"] = h.clock() - 0.98
            h.core._save_turn(turn)

            async def blocked(expression_id):
                try:
                    await asyncio.Event().wait()
                finally:
                    h.clock.advance(0.04)
                    stopped.set()

            queue.query_expression = blocked
            await asyncio.wait_for(h.core._reconcile(turn), 1)
            assert stopped.is_set()
            assert h.core.store.get("turns", turn["id"])["phase"] == "closed_unknown"
            assert len(queue.requests) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_confirmed_queue_progress_releases_only_the_old_unknown_read_timer():
    async def run():
        h = Harness(silence_ms=0, delivery_reconcile_timeout_ms=1000)
        finalized = asyncio.Event()
        try:
            queue, turn = await missing_receipt_turn(h)
            turn["expression_started_at"] = h.clock() - 0.98
            h.core._save_turn(turn)
            queue.states = ["queued"]

            async def confirmed(expression_id):
                queue.queries += 1
                return queue.receipt()

            async def finalize(expression_id, origin):
                await asyncio.sleep(0.04)
                h.clock.advance(0.1)
                queue.final = True
                finalized.set()
                return queue.receipt()

            queue.query_expression = confirmed
            queue.finalize_expression = finalize
            await asyncio.wait_for(h.core._reconcile(turn), 1)
            fresh = h.core.store.get("turns", turn["id"])
            assert finalized.is_set()
            assert fresh["phase"] == "reconciling" and fresh["expression_receipt"]["final"]
            assert h.core._replies(turn)[0]["state"] == "sending"
            assert (
                h.core.store.get("metadata", "delivery-reconcile:" + turn["id"])["deadline"]
                > h.clock()
            )
            assert len(queue.requests) == queue.queries == 1
        finally:
            await h.core.close()

    asyncio.run(run())
