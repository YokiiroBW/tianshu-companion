import asyncio
import copy

import pytest

from support import Harness
from tianshu_companion.clients import utc
from tianshu_companion.context import scope_check
from tianshu_companion.contracts import Fault


async def preparing_turn(h):
    from test_delivery_v2 import source_turn

    turn = await source_turn(h)
    turn.update(
        phase="preparing",
        config_version=h.core.config_version,
        bootstrap_until=h.clock() + 5,
        source_deadline=h.clock() + 60,
    )
    h.origins.values[turn["origin"]["assertion_ref"]]["expires_at"] = utc(turn["source_deadline"])
    h.core.store.put("turns", turn)
    return turn


@pytest.mark.parametrize("query_seconds", [0, 45])
def test_preparation_lease_owner_renews_only_when_query_consumed_its_margin(query_seconds):
    async def run():
        from test_delivery_v2 import NativeQueue

        h = Harness(silence_ms=0)
        try:
            turn = await preparing_turn(h)
            queue, renewals = NativeQueue(h.clock), []

            async def context(source, scope, channel):
                renewals.append(h.clock())
                expiry = utc(h.clock() + 60)
                h.origins.values[source["origin"]["assertion_ref"]]["expires_at"] = expiry
                return dict(
                    origin=source["origin"], scope=scope, channel=channel, expires_at=expiry
                )

            queue.expression_context = context
            h.core.sender = queue
            original = h.memory.select

            async def select(*args, **kwargs):
                if all(args[3].values()):
                    h.clock.advance(query_seconds)
                return await original(*args, **kwargs)

            h.memory.select = select
            await h.core._process_turn(turn["id"])
            fresh = h.core.store.get("turns", turn["id"])
            assert fresh["phase"] == "ready_to_send"
            assert len(h.gateway.calls) == 1
            # Initial authority and fresh post-slot preflight are independent.
            # Preparation renews only when the query left <=20seconds of lease.
            assert len(renewals) == (3 if query_seconds else 2)
            assert fresh["source_deadline"] > h.clock()
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["unknown", "expired", "cancelled"])
def test_query_gap_cancellation_and_lease_failure_never_reach_model(failure):
    async def run():
        from test_delivery_v2 import NativeQueue

        h = Harness(silence_ms=0)
        try:
            turn = await preparing_turn(h)
            queue, renewals = NativeQueue(h.clock), []

            async def context(source, scope, channel):
                renewals.append(h.clock())
                if len(renewals) == 2 and failure == "unknown":
                    raise Fault("result_unknown", unknown=True)
                expiry = utc(h.clock() - 1 if len(renewals) == 2 else h.clock() + 60)
                h.origins.values[source["origin"]["assertion_ref"]]["expires_at"] = expiry
                return dict(
                    origin=source["origin"], scope=scope, channel=channel, expires_at=expiry
                )

            queue.expression_context = context
            h.core.sender = queue
            original = h.memory.select

            async def select(*args, **kwargs):
                h.clock.advance(45)
                selected = await original(*args, **kwargs)
                if failure == "cancelled":
                    fresh = h.core.store.get("turns", turn["id"])
                    fresh.update(cancelled=True, phase="cancelled")
                    h.core.store.put("turns", fresh)
                return selected

            h.memory.select = select
            await h.core._process_turn(turn["id"])
            fresh = h.core.store.get("turns", turn["id"])
            assert not h.gateway.calls and fresh["model_calls"] == 0
            assert len(renewals) == (1 if failure == "cancelled" else 2)
            if failure == "cancelled":
                assert fresh["phase"] == "cancelled"
            else:
                assert fresh["phase"] == "failed"
                assert fresh["last_failure"]["unknown"] is (failure == "unknown")
                assert not fresh.get("preparation_retries")
        finally:
            await h.core.close()

    asyncio.run(run())


def test_waiting_for_model_slot_rechecks_the_current_lease_and_revocation():
    async def run():
        from test_delivery_v2 import NativeQueue
        from tianshu_companion.model_slots import ModelSlots

        h = Harness(silence_ms=0)
        task = None
        try:
            turn = await preparing_turn(h)
            queue, renewals = NativeQueue(h.clock), []

            async def context(source, scope, channel):
                renewals.append(h.clock())
                origin = h.origins.values[source["origin"]["assertion_ref"]]
                if origin.get("revoked"):
                    raise Fault("forbidden")
                origin["expires_at"] = utc(h.clock() + 60)
                return dict(
                    origin=source["origin"],
                    scope=scope,
                    channel=channel,
                    expires_at=origin["expires_at"],
                )

            queue.expression_context = context
            h.core.sender = queue
            h.core.models = ModelSlots(1)
            await h.core.models.acquire()
            prepared = asyncio.Event()
            original = h.core._prepare_context

            async def prepare(*args):
                result = await original(*args)
                prepared.set()
                return result

            h.core._prepare_context = prepare
            task = asyncio.create_task(h.core._process_turn(turn["id"]))
            await asyncio.wait_for(prepared.wait(), 1)
            assert renewals == [h.clock()]
            h.clock.advance(61)
            h.origins.values[turn["origin"]["assertion_ref"]]["revoked"] = True
            h.core.models.release()
            await task
            fresh = h.core.store.get("turns", turn["id"])
            assert len(renewals) == 2
            assert fresh["phase"] == "failed"
            assert not h.gateway.calls and fresh["model_calls"] == 0
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await h.core.close()

    asyncio.run(run())


def test_group_history_renews_original_human_lease_and_revocation_still_denies():
    async def run():
        h = Harness(silence_ms=0)
        try:
            await h.ingest(account="1001", channel="group:123", group=True, text="周末约在公园门口")
            await h.cycles()
            previous = h.turns()[0]
            await h.ingest(account="1002", channel="group:123", group=True, text="刚才约在哪里？")
            h.core._seal_due(h.clock())
            current = h.turns()[-1]
            old_ref = previous["origin"]["assertion_ref"]
            h.origins.values[old_ref]["expires_at"] = utc(h.clock() - 1)
            previous["source_deadline"] = h.clock() - 1
            h.core.store.put("turns", previous)
            renewed = []

            class Sender:
                def expression_available(self, channel):
                    return True

                async def expression_context(self, origin, scope, channel):
                    ref = origin["origin"]["assertion_ref"]
                    assert ref == old_ref and scope == previous["scope"]
                    assert scope["person_id"] != current["scope"]["person_id"]
                    context = h.origins.values[ref]
                    if context.get("revoked"):
                        raise Fault("forbidden")
                    context["expires_at"] = utc(h.clock() + 60)
                    renewed.append(ref)
                    return dict(
                        origin=origin["origin"],
                        scope=scope,
                        channel=channel,
                        expires_at=context["expires_at"],
                    )

            h.core.sender = Sender()
            await h.core._verify_checks(current, [scope_check(previous)])
            assert renewed == [old_ref]
            forged = copy.deepcopy(scope_check(previous))
            forged["origin"] = current["origin"]
            with pytest.raises(Fault):
                await h.core._verify_checks(current, [forged])
            assert renewed == [old_ref]
            h.origins.values[old_ref]["revoked"] = True
            with pytest.raises(Fault):
                await h.core._verify_checks(current, [scope_check(previous)])
            assert renewed == [old_ref]
        finally:
            await h.core.close()

    asyncio.run(run())
