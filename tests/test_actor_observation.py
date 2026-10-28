"""Silent archived observations use real owner envelopes, never a dialogue turn."""

import asyncio
import copy
import json

import httpx
import pytest
from test_actor_memory import close
from test_autonomous_life import RecordedModel, RecordedOwners, autonomy_setup

from tianshu_companion.actor_memory import ActorMemory
from tianshu_companion.actor_observation import QUEUE, descriptor
from tianshu_companion.clients import JsonService, utc
from tianshu_companion.contracts import Fault, canonical, digest
from tianshu_companion.observation import Observations


def original(h, number=1, *, scope=1, text="大家讨论恒星演化，很想了解星体的后续变化。"):
    event = dict(
        schema_version=2,
        platform_id="platform:fixture",
        self_id="10001",
        namespace="qq",
        conversation_id="group:20001",
        account_id="30001",
        event_id="event:" + str(number),
        revision=1,
        sent_at=utc(h.clock()),
        text=text,
        content_state="text",
        mentioned=False,
        scope_revision=1,
    )
    return dict(
        event=event,
        source_ref="obs:" + str(number),
        source_digest=digest(event),
        scope_version=scope,
        archive_epoch=1,
    )


class ObserverOwners(RecordedOwners):
    def __init__(self, h):
        super().__init__(h)
        self.originals, self.archived, self.source_calls = {}, [], []
        self.episodes = {}
        self.generation, self.actor, self.mode = 1, "actor:a", "observe"
        self.source_access = "a" * 64

    def fact(self, source):
        request = self.originals[source["object_id"]]
        return dict(
            source=source,
            actor_id="actor:a",
            relation="observer",
            provenance="observation",
            occurred_at=request["event"]["sent_at"],
            learned_at=request["event"]["sent_at"],
            outcome="observed",
            summary="看到群成员讨论（来源原文）：" + request["event"]["text"],
            source_generation="observation:1",
            source_sequence=request["scope_version"],
            access_version=self.source_access,
            source_constraints=dict(kind="group", conversation_id="core-group:actual"),
            content_reads=[],
        )

    def processing(self, request):
        body = json.loads(request.content)
        return httpx.Response(
            200,
            json=dict(
                schema_version=1,
                request_id=body["request_id"],
                frames=[
                    dict(
                        frame=frame,
                        mode=self.mode,
                        actor_id=self.actor,
                        policy_version=self.generation,
                        processing_enabled=self.mode != "disabled"
                        and frame.get("actor_id") in {None, self.actor},
                        code=None,
                    )
                    for frame in body["frames"]
                ],
            ),
        )

    async def memory(self, request):
        body = json.loads(request.content)
        if request.url.path == "/internal/v2/memory/observations":
            self.archived.append(copy.deepcopy(body))
            return httpx.Response(
                200,
                json=dict(
                    source_ref=body["source_ref"], state="accepted", archive_state="archived"
                ),
            )
        if request.url.path.endswith("/observation-source"):
            self.source_calls.append(copy.deepcopy(body))
            self.h.contracts.check("actor-self-memory#" + body["operation"] + "_request", body)
            value = dict(schema_version=1, request_id=body["request_id"], actor_id=body["actor_id"])
            if body["operation"] == "source_facts":
                if self.withdrawn:
                    return httpx.Response(404, json=Fault("not_found").wire(body["request_id"]))
                value["facts"] = [self.fact(source) for source in body["refs"]]
            else:
                value["items"] = [
                    dict(
                        source=source,
                        state="withdrawn" if self.withdrawn else "current",
                        actor_known=True,
                        read_allowed=not self.withdrawn,
                        disclosure_allowed=False,
                        source_generation="observation:1",
                        source_sequence=self.originals[source["object_id"]]["scope_version"],
                        access_version=self.source_access,
                    )
                    for source in body["refs"]
                ]
            return httpx.Response(200, json=value)
        operation = body["operation"]
        if operation == "append_episode":
            self.memory_calls.append(copy.deepcopy(body))
            assert body["perspective"] == "observer"
            self.fact(body["source"])
            episode_id = "episode:" + body["source"]["object_id"]
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                command=operation,
                idempotency_key=body["idempotency_key"],
                state="accepted",
                episode_id=episode_id,
                revision=1,
                domain_version=23,
            )
            self.receipts[body["idempotency_key"]] = value
            self.episodes[episode_id] = body["source"]
            if self.lost_ack:
                self.lost_ack = False
                raise httpx.ReadError("lost actual append ACK")
            return httpx.Response(200, json=value)
        if operation == "get_episode":
            self.memory_calls.append(copy.deepcopy(body))
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                role_version=body["role_version"],
                operation=operation,
                context=body["context"],
                domain_version=23,
                episode=dict(
                    episode_id=body["episode_id"],
                    revision=body["expected_revision"],
                    actor_id=body["actor_id"],
                    state="active",
                    fact=self.fact(self.episodes[body["episode_id"]]),
                    recorded_at=utc(self.h.clock()),
                ),
            )
            return httpx.Response(200, json=value)
        if operation == "query":
            self.memory_calls.append(copy.deepcopy(body))
            self.material = []
            if self.derived and not self.withdrawn:
                self.material = [
                    dict(
                        ref=dict(kind="derived", id="understanding:one", revision=1),
                        kind="reflection",
                        text="看到大家讨论之后，想了解恒星演化。",
                        episode_refs=[
                            dict(episode_id=key, expected_revision=1) for key in self.episodes
                        ],
                        source_refs=list(self.episodes.values()),
                        recorded_at=utc(self.h.clock()),
                        expires_at=None,
                    )
                ]
            return httpx.Response(
                200,
                json=dict(
                    schema_version=1,
                    request_id=body["request_id"],
                    actor_id=body["actor_id"],
                    role_version=body["role_version"],
                    operation=operation,
                    context=body["context"],
                    domain_version=24,
                    checked_at=utc(self.h.clock()),
                    items=self.material,
                    omitted=False,
                ),
            )
        return await super().memory(request)


class ObserverModel(RecordedModel):
    def __init__(self, h, owners):
        super().__init__(h, owners)
        self.observed, self.observation_gate = [], None
        self.interesting = True

    async def generate(self, generation, messages):
        if "actually archived same-group" not in messages[0]["content"]:
            return await super().generate(generation, messages)
        value = json.loads(messages[1]["content"])
        self.observed.append(value)
        if self.observation_gate:
            await self.observation_gate.wait()
        refs = (
            [row["source"]["object_id"] for row in value["source_facts"]]
            if self.interesting
            else []
        )
        return [
            canonical(
                dict(
                    retain=refs,
                    topic="恒星演化" if refs else "",
                    reflection="只是看到其他成员在讨论，还需实际阅读再判断。" if refs else "",
                )
            )
        ], dict(provider="recorded-observation")


async def observer_setup():
    h, _, old_services, _ = await autonomy_setup()
    await asyncio.gather(*(service.close() for service in old_services))
    owners = ObserverOwners(h)
    services = [
        JsonService(
            "https://authority.fixture.invalid",
            "authority",
            transport=httpx.MockTransport(owners.authority),
        ),
        JsonService(
            "https://memory.fixture.invalid", "memory", transport=httpx.MockTransport(owners.memory)
        ),
        JsonService(
            "https://platform.fixture.invalid",
            "processing",
            transport=httpx.MockTransport(owners.processing),
        ),
    ]
    h.memory.client = services[1]
    h.core.actor_memory = ActorMemory(h.core, services[0])
    h.core.life.actor_memory = h.core.actor_memory
    h.core.group_processing.client = services[2]
    model = ObserverModel(h, owners)
    h.core.life.gateway = model
    h.core.life.generation_caller_service = "companion_background"
    h.core._automatic_memory_candidates = (
        False  # Distinct startup policy fixture; no user extraction.
    )
    assert h.core.actor_observation.available
    return h, owners, services, model


async def offer(h, owners, request=None):
    request = request or original(h)
    owners.originals[request["source_ref"]] = copy.deepcopy(request)
    await h.core.observation_archived(
        request, dict(source_ref=request["source_ref"], state="accepted", archive_state="archived")
    )
    h.clock.advance(6)
    return request


def job(h):
    return h.core.store.list("metadata", QUEUE)[0]


def test_observe_only_archive_silent_interest_episode_reflection_and_next_goal(tmp_path):
    async def run():
        h, owners, services, model = await observer_setup()
        try:
            request = original(h)
            owners.originals[request["source_ref"]] = request
            h.core.observations = Observations(
                tmp_path / "passive.sqlite",
                services[1],
                on_archived=h.core.observation_archived,
                can_process=h.core.group_processing.archive_start,
            )
            h.core.observations.ingest("platform", request)
            await h.core.observations.flush()
            assert owners.archived and not model.observed
            h.clock.advance(6)
            for _ in range(8):
                await h.core.memory_learning.work()
                if job(h)["state"] == "completed":
                    break
                h.clock.advance(2)
            assert job(h)["state"] == "completed"
            assert len(model.observed) == 1
            writes = [
                row
                for row in owners.memory_calls
                if row["operation"] in {"append_episode", "append_derived"}
            ]
            assert [row["operation"] for row in writes] == ["append_episode", "append_derived"]
            assert (
                writes[0]["source"] == descriptor(request)
                and writes[0]["perspective"] == "observer"
            )
            assert writes[1]["episode_refs"] == [
                dict(episode_id="episode:obs:1", expected_revision=1)
            ]
            assert not h.core.store.list("turns") and not h.memory.commits
            assert not h.core.store.list("metadata", "memory:learning")
            assert await h.core.life.autonomy.work(actor_id="actor:a")
            assert (
                model.goals[-1]["actor_self_memory"][0]["basis"][0]["fact"]["relation"]
                == "observer"
            )
            current = h.core.life.activities.current("actor:a")
            assert current["public_exploration_topic"] == "恒星演化"
            assert current["autonomy_basis"]["items"] and current["actor_only"]
            assert (
                h.core.life.summary(
                    "actor:a",
                    dict(
                        actor_id="actor:a",
                        person_id="other",
                        audience="group",
                        conversation_id="other",
                    ),
                )["activity"]
                == "处理个人事项"
            )
            assert "其他成员" not in canonical(h.core.life.materials("actor:a", "2026-10-28"))
        finally:
            await close(h, services)

    asyncio.run(run())


@pytest.mark.parametrize("change", ["forget", "access", "actor", "restart"])
def test_delayed_observation_model_cannot_revive_changed_source_or_role(change):
    async def run():
        h, owners, services, model = await observer_setup()
        try:
            await offer(h, owners)
            model.observation_gate = asyncio.Event()
            task = asyncio.create_task(h.core.actor_observation.work(job(h)))
            for _ in range(30):
                await asyncio.sleep(0)
                if model.observed:
                    break
            assert model.observed
            if change == "forget":
                owners.withdrawn = True
            elif change == "access":
                owners.source_access = "b" * 64
            elif change == "actor":
                owners.actor = "actor:b"
            else:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                h.core.actor_observation.recover()
            model.observation_gate.set()
            if change != "restart":
                await task
            assert job(h)["state"] in {"invalidated", "unknown"}
            for _ in range(3):
                await h.core.memory_learning.work()
            assert len(model.observed) == 1 and not h.core.store.list("outbox")
        finally:
            await close(h, services)

    asyncio.run(run())


def test_unknown_episode_receipt_recovery_never_repeats_command_after_group_change():
    async def run():
        h, owners, services, _ = await observer_setup()
        try:
            await offer(h, owners)
            await h.core.actor_observation.work(job(h))
            owners.lost_ack = True
            await h.core.flush_outbox()
            row = h.core.store.list("outbox")[0]
            assert row["state"] == "actor_memory_unknown"
            request = copy.deepcopy(row["request"])
            owners.generation += 1
            h.clock.advance(10)
            await h.core.flush_outbox()
            row = h.core.store.list("outbox")[0]
            assert row["state"] == "actor_memory_delivered" and row["request"] == request
            assert (
                len(
                    [
                        value
                        for value in owners.memory_calls
                        if value["operation"] == "append_episode"
                    ]
                )
                == 1
            )
            await h.core.actor_observation.work(job(h))
            assert job(h)["state"] == "invalidated"
            assert not owners.derived
        finally:
            await close(h, services)

    asyncio.run(run())


def test_uninteresting_observations_coalesce_and_foreground_has_priority():
    async def run():
        h, owners, services, model = await observer_setup()
        try:
            model.interesting = False
            for number in range(1, 4):
                request = original(h, number)
                owners.originals[request["source_ref"]] = request
                await h.core.observation_archived(request, {})
            assert len(h.core.store.list("metadata", QUEUE)) == 1
            assert len(job(h)["originals"]) == 3
            h.clock.advance(6)
            h.core.automatic_memory_foreground_demand = lambda: True
            await h.core.memory_learning.work()
            assert not model.observed
            h.core.automatic_memory_foreground_demand = lambda: False
            for _ in range(2):
                await h.core.memory_learning.work()
            assert job(h)["state"] == "ignored" and len(model.observed) == 1
            assert not h.core.store.list("outbox") and not h.core.store.list("turns")
        finally:
            await close(h, services)

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["local_epoch", "source_forget", "source_generation"])
def test_no_fresh_observer_write_after_capture_boundary_changes(boundary):
    async def run():
        h, owners, services, model = await observer_setup()
        try:
            await offer(h, owners)
            if boundary == "local_epoch":
                actor = h.core.store.get("life_actors", "actor:a")
                actor["life_runtime_epoch"] = actor.get("life_runtime_epoch", 0) + 2
                h.core.life._save("actors", actor)
            elif boundary == "source_forget":
                owners.withdrawn = True
            else:
                owners.generation += 2
            await h.core.actor_observation.work(job(h))
            assert job(h)["state"] == "invalidated"
            assert not model.observed and not h.core.store.list("outbox")
        finally:
            await close(h, services)

    asyncio.run(run())


def test_source_forget_after_completion_invalidates_hint_and_execution_basis():
    async def run():
        h, owners, services, _ = await observer_setup()
        try:
            await offer(h, owners)
            await h.core.actor_observation.work(job(h))
            await h.core.flush_outbox()
            await h.core.actor_observation.work(job(h))
            assert job(h)["state"] == "completed"
            assert await h.core.life.autonomy.work(actor_id="actor:a")
            current = h.core.life.activities.current("actor:a")
            owners.withdrawn = True
            with pytest.raises(Fault):
                await h.core.actor_memory.check(current["autonomy_basis"])
            actor = h.core.store.get("life_actors", "actor:a")
            actor["autonomy_due_at"] = h.clock()
            h.core.life._save("actors", actor)
            assert not await h.core.life.autonomy.work(actor_id="actor:a")
            assert "autonomy_observation_hint" not in h.core.store.get("life_actors", "actor:a")
        finally:
            await close(h, services)

    asyncio.run(run())


def test_unknown_derived_command_only_looks_up_original_receipt():
    async def run():
        h, owners, services, _ = await observer_setup()
        try:
            await offer(h, owners)
            await h.core.actor_observation.work(job(h))
            await h.core.flush_outbox()
            owners.derived_failure = "unknown"
            await h.core.actor_observation.work(job(h))
            assert job(h)["state"] == "selected"
            assert job(h)["last_error"] == "dependency_unavailable"
            marker = next(
                item
                for item in h.core.store.list("metadata")
                if item["id"].startswith("actor-derived:")
            )
            assert marker["state"] == "unknown"
            for _ in range(2):
                await h.core.actor_observation.work(job(h))
            writes = [row for row in owners.memory_calls if row["operation"] == "append_derived"]
            assert len(writes) == 1
            assert "autonomy_observation_hint" not in h.core.store.get("life_actors", "actor:a")
        finally:
            await close(h, services)

    asyncio.run(run())


def test_real_platform_generations_reject_actor_and_mode_aba_without_touching_other_group():
    policy = pytest.importorskip(
        "services.platform.group_policy",
        reason="joint check requires actual Platform owner package",
    )

    async def run():
        h, owners, services, model = await observer_setup()
        try:
            row = dict(host_revision=10)
            current = dict(version=2, default=policy.rule("observe", "actor:a"), groups={})
            owners.generation = policy.revision(row, "20001")
            old = await offer(h, owners, original(h, scope=10))
            for actor in ("actor:b", "actor:a"):
                following = copy.deepcopy(current)
                following["groups"]["20001"] = policy.rule("observe", actor)
                policy.advance(row, current, following, ("20001", "20002"))
                row["host_revision"] += 1
                current = following
            owners.generation = policy.revision(row, "20001")
            assert owners.generation == 12 and policy.revision(row, "20002") == 10
            await h.core.actor_observation.work(job(h))
            assert job(h)["state"] == "invalidated" and not model.observed
            await h.core.observation_archived(old, {})
            assert len(h.core.store.list("metadata", QUEUE)) == 1
            await offer(h, owners, original(h, 2, scope=12))
            pending = next(
                item for item in h.core.store.list("metadata", QUEUE) if item["state"] == "queued"
            )
            for mode in ("disabled", "observe"):
                following = copy.deepcopy(current)
                following["groups"]["20001"] = policy.rule(mode, "actor:a")
                policy.advance(row, current, following, ("20001", "20002"))
                row["host_revision"] += 1
                current = following
            owners.generation = policy.revision(row, "20001")
            assert owners.generation == 14 and policy.revision(row, "20002") == 10
            await h.core.actor_observation.work(pending)
            assert all(
                item["state"] == "invalidated" for item in h.core.store.list("metadata", QUEUE)
            )
            assert not model.observed and not h.core.store.list("outbox")
        finally:
            await close(h, services)

    asyncio.run(run())


def test_observation_external_discovery_receives_public_topic_projection():
    async def run():
        h, owners, services, _ = await observer_setup()
        try:
            await offer(h, owners)
            await h.core.actor_observation.work(job(h))
            await h.core.flush_outbox()
            await h.core.actor_observation.work(job(h))
            await h.core.life.autonomy.work(actor_id="actor:a")
            for _ in range(2):
                await h.core.life.activities.work(actor_id="actor:a")
            invoke = next(row for row in owners.platform_calls if row["operation"] == "invoke")
            assert invoke["arguments"] == dict(query="恒星演化")
            assert "讨论" not in canonical(invoke) and "group:20001" not in canonical(invoke)
        finally:
            await close(h, services)

    asyncio.run(run())
