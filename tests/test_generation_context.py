"""Use actual learning/image paths with mutually exclusive generation identities."""

import asyncio
import copy
from importlib import import_module
import json

import pytest

from support import Harness
from test_comfy_deep import configured, image_value
from tianshu_companion.contracts import Fault
from tianshu_companion.model_selection import ModelSelection
from tianshu_companion.memory_learning import MODEL_ROUTE_BOUNDARY


class Selector:
    def __init__(self, clock, caller, version):
        self.clock, self.caller, self.version = clock, caller, version
        self.calls = []

    async def select(self, request):
        assert request.caller_service == self.caller
        self.calls.append(request)
        return ModelSelection(self.version, self.clock() + 3600, caller_service=self.caller)


class Model:
    available = True

    def __init__(self, version, *, delegate=None):
        self.version, self.delegate = version, delegate
        self.calls = []
        self.entered = asyncio.Event()
        self.release = None

    async def generate(self, turn, messages):
        assert turn["config_version"] == self.version
        self.calls.append(copy.deepcopy(turn))
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.delegate is not None:
            return await self.delegate(turn, messages)
        return [json.dumps({"positive": "actual translated intent"})], {}


class ForbiddenSelector:
    async def select(self, request):
        raise AssertionError("An independent background request borrowed foreground authority")


async def forbidden_generate(*_):
    raise AssertionError("An independent background request reached the foreground model")


@pytest.mark.parametrize(
    "module,class_name",
    [
        ("test_memory_learning", "MemoryLearningTests"),
        ("test_memory_summaries", "SummaryLearningTests"),
        ("test_memory_segments", "SegmentLearningTests"),
    ],
)
def test_full_learning_protocol_uses_background_grant_and_gateway(module, class_name):
    async def run():
        # Reuse the existing full candidate/summary/segment ledger fixtures rather
        # than manufacturing another simplified version of their wire documents.
        fixture = getattr(import_module(module), class_name)("runTest")
        await fixture.asyncSetUp()
        h = fixture.h
        background = Model(37, delegate=h.gateway.generate)
        selector = Selector(h.clock, "companion_background", 37)
        try:
            await h.core.close()
            h.core = h.new_core(
                background_gateway=background, background_model_selector=selector, model_slots=2
            )
            await fixture.chat()
            h.core.default_model_selector = ForbiddenSelector()
            h.gateway.generate = forbidden_generate
            if module == "test_memory_segments":
                h.clock.advance(120)
                await fixture.drain()
                tasks = fixture.tasks()
            else:
                await fixture.flush()
                tasks = [fixture.task()] if module == "test_memory_learning" else fixture.tasks()
            assert tasks and all(task["state"] == "completed" for task in tasks)
            assert len(background.calls) == len(selector.calls) == 1
            assert selector.calls[0].function_id == "memory"
            assert (
                background.calls[0]["model_selection"]["caller_service"] == "companion_background"
            )
        finally:
            await fixture.asyncTearDown()
            fixture.doCleanups()

    asyncio.run(run())


def test_background_learning_requires_its_own_binding_but_legacy_fallback_keeps_working():
    async def run():
        h = Harness()
        background = Model(41)
        scope = dict(
            actor_id="actor:a",
            person_id="person:a",
            audience="self_private",
            conversation_id="conv:a",
        )
        try:
            await h.core.close()
            h.core = h.new_core(
                background_gateway=background, default_model_selector=ForbiddenSelector()
            )
            inherited = dict(
                id="memory:one",
                config_version=1,
                model_selection=dict(caller_service="companion", expires_at=h.clock() + 3600),
            )
            item = dict(id="learning:new")
            with pytest.raises(Fault, match="dependency_unavailable"):
                await h.core.memory_learning._pin_model(item, inherited, scope)
            assert not background.calls
            h.core.life.config_version = 41
            assert await h.core.memory_learning._pin_model(item, inherited, scope) is background
            assert inherited["config_version"] == 41 and "model_selection" not in inherited
            await h.core.close()
            h.core = h.new_core()
            old = dict(id="memory:retained", config_version=19)
            assert (
                await h.core.memory_learning._pin_model(dict(id="learning:legacy"), old, scope)
                is h.gateway
            )
            assert old["config_version"] == 19
        finally:
            await h.core.close()

    asyncio.run(run())


def test_preupgrade_unknown_model_keeps_its_original_caller_and_generation_id():
    async def run():
        fixture = import_module("test_memory_learning").MemoryLearningTests("runTest")
        await fixture.asyncSetUp()
        h = fixture.h
        try:
            await fixture.chat()

            async def unknown(turn, messages):
                fixture.generated.append(copy.deepcopy(turn))
                raise Fault("result_unknown", unknown=True)

            h.gateway.generate = unknown
            await fixture.flush()
            retained = fixture.task()
            original_id = fixture.generated[0]["id"]
            original_attempts = retained["attempts"]
            # Synthetic pre-upgrade state: the old writer had no route marker or
            # generation-boundary row. Keep its actual unknown stage and budget.
            retained.pop("model_route")
            h.core.store.put("metadata", retained)
            h.core.store.delete("metadata", MODEL_ROUTE_BOUNDARY)
            await h.core.close()
            background = Model(91)
            foreground = Model(37, delegate=fixture.generate)
            h.gateway.generate = foreground.generate
            h.core = h.new_core(
                background_gateway=background,
                background_model_selector=ForbiddenSelector(),
                default_model_selector=Selector(h.clock, "companion", 37),
            )
            h.clock.advance(10)
            await fixture.flush()
            completed = fixture.task()
            assert completed["state"] == "completed"
            assert completed["model_route"] == dict(caller_service="companion", turn_id=original_id)
            assert foreground.calls[0]["id"] == original_id
            assert completed["attempts"] >= original_attempts and not background.calls
        finally:
            await fixture.asyncTearDown()
            fixture.doCleanups()

    asyncio.run(run())


def test_retained_unknown_proposal_reads_receipt_before_any_new_model_after_upgrade():
    async def run():
        fixture = import_module("test_memory_learning").MemoryLearningTests("runTest")
        await fixture.asyncSetUp()
        h = fixture.h
        try:
            await fixture.chat()
            fixture.lose_response = True
            await fixture.flush()
            retained = fixture.task()
            assert retained["output"] is not None
            submitted = copy.deepcopy([output for _, output in fixture.calls if output is not None])
            retained.pop("model_route")
            h.core.store.put("metadata", retained)
            h.core.store.delete("metadata", MODEL_ROUTE_BOUNDARY)
            await h.core.close()
            background = Model(91)
            h.gateway.generate = forbidden_generate
            h.core = h.new_core(
                background_gateway=background,
                background_model_selector=ForbiddenSelector(),
                default_model_selector=ForbiddenSelector(),
            )
            h.clock.advance(10)
            await fixture.flush()
            assert fixture.task()["state"] == "completed"
            assert [output for _, output in fixture.calls if output is not None] == submitted
            assert len(fixture.generated) == 1 and not background.calls
        finally:
            await fixture.asyncTearDown()
            fixture.doCleanups()

    asyncio.run(run())


def test_image_foreground_and_background_keep_selector_gateway_and_capacity_together():
    async def run():
        h = Harness()
        foreground, background = Model(11), Model(22)
        front_selection = Selector(h.clock, "companion", 11)
        back_selection = Selector(h.clock, "companion_background", 22)
        pending = []
        try:
            await h.core.close()
            h.core = h.new_core(
                background_gateway=background,
                background_model_selector=back_selection,
                default_model_selector=front_selection,
                model_slots=2,
            )
            h.core.gateway = foreground
            background.release = asyncio.Event()
            catalog = h.core.image_backend.catalog
            first = asyncio.create_task(
                catalog.model("actor:a", "photo:one", "system", {}, background=True)
            )
            pending.append(first)
            await asyncio.wait_for(background.entered.wait(), 1)
            second = asyncio.create_task(
                catalog.model("actor:b", "photo:two", "system", {}, background=True)
            )
            pending.append(second)
            # The other actor's background work waits at the existing background
            # cap, while an actual foreground request can use the reserve.
            await asyncio.wait_for(catalog.model("actor:a", "manual:one", "system", {}), 1)
            assert len(background.calls) == 1 and len(foreground.calls) == 1
            assert front_selection.calls[0].caller_service == "companion"
            assert all(
                request.caller_service == "companion_background" for request in back_selection.calls
            )
            background.release.set()
            await asyncio.gather(*pending)
            assert len(background.calls) == 2
            assert all(
                request.function_id == "writing"
                for request in front_selection.calls + back_selection.calls
            )
        finally:
            if background.release is not None:
                background.release.set()
            await asyncio.gather(*pending, return_exceptions=True)
            await h.core.close()

    asyncio.run(run())


@pytest.mark.parametrize("background", [False, True])
def test_image_tool_reuses_the_single_slot_already_owned_by_its_caller(background):
    async def run():
        h = Harness()
        model = Model(7)
        try:
            await h.core.close()
            h.core = h.new_core(
                background_gateway=model, life_config_version=7, model_slots=1, config_version=7
            )
            h.core.gateway = model
            slots = h.core.life.model_slots if background else h.core.models
            async with slots:
                async with asyncio.timeout(1):
                    await h.core.image_backend.catalog.model(
                        "actor:a",
                        "tool:held",
                        "system",
                        {},
                        model_slot_held=True,
                        background=background,
                    )
            assert len(model.calls) == 1
        finally:
            await h.core.close()

    asyncio.run(run())


def test_background_image_request_reaches_the_same_compiler_without_gpu_submit(tmp_path):
    async def run():
        h = Harness()
        try:
            _, catalog = await configured(h, tmp_path)
            background = Model(31, delegate=h.gateway.generate)
            h.core.life.gateway = background
            h.core.life.model_selector = Selector(h.clock, "companion_background", 31)
            h.core.life.generation_caller_service = "companion_background"
            h.core.default_model_selector = ForbiddenSelector()
            h.core.gateway.generate = forbidden_generate

            # The compiler asks for JSON matching the supplied semantic keys.
            async def translate(turn, messages):
                return [
                    json.dumps(
                        {key: value for key, value in json.loads(messages[-1]["content"]).items()}
                    )
                ], {}

            background.delegate = translate
            value = image_value("photo:actual", intent=dict(pose="reading"))
            prepared = await h.core.life_runtime.prepare_action(
                "actor:a", "image.request", value, 0, background=True
            )
            assert prepared["workflow"] is not None and len(background.calls) == 1
            assert h.core.store.get("image_jobs", value["id"]) is None
        finally:
            await h.core.close()

    asyncio.run(run())
