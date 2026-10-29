"""Window rebuilds validate their own sources, not a historical chat's prompt."""

import asyncio
import copy

import pytest

import test_memory_v2 as v2_support
from test_conversation_windows import SummaryModel, seed, setup, small
from tianshu_companion.contracts import Fault
from tianshu_companion.conversation_windows import WindowPolicy


def test_v2_historical_anchor_rebuilds_after_window_generation_changes():
    async def run():
        fixture = v2_support.CoreEvidenceTests()
        await fixture.asyncSetUp()
        h = fixture.h
        model = SummaryModel(h)
        h.gateway = h.core.gateway = h.core.life.gateway = model
        h.core.conversation_windows.policy = WindowPolicy(max_turns=128, max_messages=1024)
        try:
            turn = await seed(h)
            assert turn["memory_context"]["bundle"]
            owner = h.core.conversation_windows
            state = owner.state(turn)
            # A completed chat used this former projection. A source/window reset
            # invalidates that projection without retracting the accepted inputs.
            turn["conversation_window"] = {
                key: state[key] for key in ("id", "generation", "version")
            }
            h.core.store.put("turns", turn)
            old_turn = copy.deepcopy(turn)
            owner.reset(state, reason="fixture_source_revision")
            rebuilt_generation = state["generation"]
            with pytest.raises(Fault, match="scope_changed"):
                await h.core._verify_checks(turn, [])  # Exact old v2 entry reproduces the cycle.
            small(owner)
            h.clock.advance(181)
            await owner.work(wait=True)
            state = owner.state(turn)
            assert state["generation"] == rebuilt_generation
            assert state["summary"] == model.output and state["covered"] > 0
            assert state["floor"] == 0 and len(model.summaries) == 1
            assert (
                h.core.store.get("turns", turn["id"])["memory_context"]
                == old_turn["memory_context"]
            )
            await owner.work(wait=True)
            assert owner.state(turn)["generation"] == rebuilt_generation
        finally:
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("withdrawn", [False, True])
def test_failed_raw_authority_preserves_range_and_recovers_without_reset(withdrawn):
    async def run():
        h, model = setup()
        try:
            turn = await seed(h)
            owner = h.core.conversation_windows
            small(owner)
            h.clock.advance(181)
            original = h.memory.check_sources
            checks = []

            async def unavailable(*args):
                checks.append(True)
                error = Fault("version_conflict" if withdrawn else "dependency_unavailable")
                error.source_invalid = withdrawn
                raise error

            h.memory.check_sources = unavailable
            before = copy.deepcopy(owner.state(turn))
            await owner.work(wait=True)
            await owner.work(wait=True)
            failed = owner.state(turn)
            assert failed["generation"] == before["generation"]
            assert failed["floor"] == before["floor"] and failed["covered"] == before["covered"]
            assert failed["turns"] == before["turns"] and failed["summary"] is None
            assert failed["capture_failure"]["stage"] == "raw_input"
            assert len(checks) == 1 and not model.summaries
            # Rechecking persistent withdrawal still must not adopt any old input.
            h.clock.advance(601)
            await owner.work(wait=True)
            assert len(checks) == 2 and not model.summaries
            assert owner.state(turn)["generation"] == before["generation"]
            if not withdrawn:
                h.memory.check_sources = original
                h.clock.advance(601)
                await owner.work(wait=True)
                assert owner.state(turn)["summary"] == model.output
                assert len(model.summaries) == 1
        finally:
            await h.core.close()

    asyncio.run(run())
