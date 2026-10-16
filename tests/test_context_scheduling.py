import asyncio

import pytest

from support import Harness
from tianshu_companion.context import scope_check
from tianshu_companion.contracts import Fault
from tianshu_companion.model_slots import ModelSlots


def test_repeated_history_is_checked_once_per_checkpoint_but_revocation_is_fresh():
    async def run():
        h = Harness(silence_ms=0)
        try:
            h.memory.history_version = 1
            await h.ingest(text="门口那盆是薄荷", group=True, channel="group:plants", account="a")
            await h.cycles()
            previous = h.turns()[0]
            check = scope_check(previous)
            calls = []
            original = h.memory.history

            async def history(*args, **kwargs):
                calls.append((args, kwargs))
                return await original(*args, **kwargs)

            h.memory.history = history
            memo = {}
            await h.core._verify_checks(previous, [check, check], verified=memo)
            await h.core._verify_checks(previous, [check], verified=memo)
            assert len(calls) == 1
            h.memory.history_version = 2
            with pytest.raises(Fault):
                await h.core._verify_checks(previous, [check])
            assert len(calls) == 2
        finally:
            await h.core.close()

    asyncio.run(run())


def test_revocation_after_context_preparation_prevents_model_generation():
    async def run():
        h = Harness(silence_ms=0)
        try:
            await h.ingest(text="周末去公园", group=True, channel="group:plans", account="a")
            await h.cycles()
            original = h.core._prepare_context
            previous = h.turns()[0]

            async def prepare(*args):
                result = await original(*args)
                h.origins.values[previous["origin"]["assertion_ref"]]["revoked"] = True
                return result

            h.core._prepare_context = prepare
            await h.ingest(text="刚才说周末去哪？", group=True, channel="group:plans", account="b")
            await h.cycles()
            assert h.turns()[-1]["phase"] == "failed"
            assert len(h.gateway.calls) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_slow_context_preparation_does_not_occupy_model_capacity():
    async def run():
        h = Harness(silence_ms=0)
        released, entered = asyncio.Event(), asyncio.Event()
        original = h.core._prepare_context
        h.core.models = ModelSlots(1)

        async def prepare(*args):
            entered.set()
            await released.wait()
            return await original(*args)

        h.core._prepare_context = prepare
        try:
            await h.ingest(text="泡了一杯茶")
            await h.cycles()
            assert entered.is_set()
            await asyncio.wait_for(h.core.models.acquire(), 0.5)
            h.core.models.release()
            released.set()
            await h.cycles(80)
            assert h.turns()[0]["phase"] == "sent"
            assert len(h.gateway.calls) == 1
        finally:
            released.set()
            await h.core.close()

    asyncio.run(run())


def test_other_room_prepares_while_model_is_busy_without_unbounded_workers():
    async def run():
        h = Harness(silence_ms=0)
        gate = asyncio.Event()
        h.core.models = ModelSlots(1)
        h.core.preparation_slots = 1
        h.gateway.gates[1] = gate
        prepared = []
        original = h.core._prepare_context

        async def prepare(turn, dependencies):
            result = await original(turn, dependencies)
            prepared.append(h.core._input_text(turn))
            return result

        h.core._prepare_context = prepare
        try:
            await h.ingest(text="第一路", channel="private:first")
            await h.cycles()
            for i in range(5):
                await h.ingest(text=f"后续{i}", channel=f"private:room{i}")
            await h.cycles()
            assert len(prepared) == 2
            assert len(h.core.jobs) == 2
            assert len(h.gateway.calls) == 1
            gate.set()
            await h.cycles(200)
            assert all(t["phase"] == "sent" for t in h.turns())
            assert len(h.gateway.calls) == 6
        finally:
            gate.set()
            await h.core.close()

    asyncio.run(run())
