"""Zero-input actor goal/discovery/read/interpretation over recorded owner protocols."""

import asyncio
import copy
import hashlib
import json
from datetime import datetime, timezone

import httpx
import pytest
from test_actor_memory import Owners, close, setup

from tianshu_companion.actor_memory import ActorMemory
from tianshu_companion.clients import JsonService, utc
from tianshu_companion.contracts import canonical


def save_goal(h, identity, *, state="open", due=None):
    return h.core.life.concerns.save(
        "actor:a",
        dict(
            id=identity,
            title="具体目标" + identity,
            goal="依真实资料继续了解",
            state=state,
            scope=None,
            sources=[],
            fragments=[],
            next_due_at=due,
            expires_at=None,
            result_refs=[],
        ),
        expected=0,
    )


def test_planner_selects_open_due_and_exact_current_after_historical_pages():
    async def run():
        h, _, services, _ = await autonomy_setup()
        try:
            for index in range(30):
                save_goal(h, f"a-history:{index:03}", state="completed")
            for index in range(25):
                save_goal(h, f"autonomy:open:{index:03}", due=h.clock() - 60)
            save_goal(h, "z-promise", due=h.clock())
            save_goal(h, "z-current")
            rows = h.core.life.concerns.planning(
                "actor:a", current_id="z-current", available_until=h.clock() + 3600
            )
            assert rows[0]["id"] == "z-promise" and len(rows) == 21
            assert any(row["id"] == "z-current" for row in rows)
            assert all(row["state"] == "open" for row in rows)
            assert len(h.core.life.concerns.planning("actor:a", current_id="missing")) == 20
            captured = dict(concerns=rows, routine=dict(available_until=h.clock() + 3600))
            h.core.life.autonomy.validate_choice(
                dict(
                    decision="continue",
                    concern_id="z-promise",
                    title="先完成约定",
                    goal="完成约定",
                    reason="有明确到期事项",
                    delay_seconds=60,
                ),
                captured,
            )
        finally:
            await close(h, services)

    asyncio.run(run())


def test_abandoning_other_goal_keeps_current_activity_resumable_in_same_epoch():
    async def run():
        h, _, services, _ = await autonomy_setup()
        try:
            await h.core.life.autonomy.work(actor_id="actor:a")
            current = copy.deepcopy(h.core.life.activities.current("actor:a"))
            save_goal(h, "autonomy:other")
            actor = h.core.store.get("life_actors", "actor:a")
            choice = dict(
                decision="abandon",
                concern_id="autonomy:other",
                title="放下另一个想法",
                goal="整理待办",
                reason="当下不需要",
                delay_seconds=60,
            )
            h.core.life.autonomy.apply(actor, choice, {}, None)
            assert h.core.life.activities.current("actor:a") == current
            assert h.core.life.concerns.get("actor:a", "autonomy:other")["state"] == "cancelled"
            actor = h.core.store.get("life_actors", "actor:a")
            choice.update(decision="continue", concern_id=current["autonomous_goal_id"])
            h.core.life.autonomy.apply(actor, choice, {}, None)
            assert h.core.life.activities.current("actor:a")["id"] == current["id"]
            assert h.core.life.activities.current("actor:a")["state"] == "running"
        finally:
            await close(h, services)

    asyncio.run(run())


def test_activity_recall_and_dispatcher_share_one_outbox_delivery_lock():
    async def run():
        h, owners, services, _ = await autonomy_setup()
        try:
            await h.core.life.autonomy.work(actor_id="actor:a")
            for _ in range(5):
                await h.core.life.activities.work(actor_id="actor:a")
            activity = h.core.life.activities.current("actor:a")
            context = h.core.runtime_execution.actions.context(activity, activity["step_operation"])
            flush = h.core.actor_memory.flush
            entered, release = asyncio.Event(), asyncio.Event()
            calls = []

            async def held(item):
                calls.append(item["id"])
                entered.set()
                await release.wait()
                return await flush(item)

            h.core.actor_memory.flush = held
            first = asyncio.create_task(h.core.flush_outbox())
            await asyncio.wait_for(entered.wait(), 2)
            second = asyncio.create_task(
                h.core.actor_memory.activity_recall(context, dict(text="刚才实际读到的内容"))
            )
            await asyncio.sleep(0)
            assert len(calls) == 1
            release.set()
            await asyncio.gather(first, second)
            assert len(calls) == 1
            assert (
                len([row for row in owners.memory_calls if row["operation"] == "append_episode"])
                == 1
            )
        finally:
            await close(h, services)

    asyncio.run(run())


def test_next_planner_prefers_actual_understanding_and_preference_with_long_episode_basis():
    async def run():
        h, owners, services, model = await autonomy_setup()
        derived_items = []
        try:
            owners.prose = "恒星演化和光谱观测需要依据实际资料继续比较。" * 12
            owners.reference.update(
                sha256=hashlib.sha256(owners.prose.encode()).hexdigest(),
                coverage=dict(
                    unit="characters", start=0, end=len(owners.prose), total=len(owners.prose)
                ),
            )
            memory = owners.memory

            async def ranked_owner(request):
                body = json.loads(request.content)
                response = await memory(request)
                if response.status_code != 200:
                    return response
                value = response.json()
                if body["operation"] == "append_derived":
                    if body["kind"] == "preference":
                        value["ref"]["id"] = "preference:actual"
                        owners.receipts[body["idempotency_key"]] = value
                    source = next(
                        row["source"]
                        for row in h.core.store.list("outbox")
                        if row.get("actor_id") == "actor:a"
                    )
                    derived_items.append(
                        dict(
                            ref=value["ref"],
                            kind=body["kind"],
                            text=body["text"],
                            episode_refs=body["episode_refs"],
                            source_refs=[source],
                            recorded_at=utc(h.clock()),
                            expires_at=None,
                        )
                    )
                elif body["operation"] == "query" and derived_items:
                    source = derived_items[0]["source_refs"][0]
                    facts = await h.core.actor_memory.source(
                        "memory",
                        dict(
                            schema_version=1,
                            request_id="long-fact:actual",
                            actor_id="actor:a",
                            role_version=owners.version,
                            operation="source_facts",
                            refs=[source],
                        ),
                    )
                    episode = dict(
                        derived_items[0],
                        ref=dict(kind="episode", id="episode:actual", revision=1),
                        kind="episode",
                        text=facts["facts"][0]["summary"],
                    )
                    selected, used = [], 0
                    for item in [episode, *derived_items]:
                        size = len(canonical(item).encode())
                        if (
                            item["kind"] in body["kinds"]
                            and used + size <= body["budget_bytes"]
                            and len(selected) < body["limit"]
                        ):
                            selected.append(item)
                            used += size
                    value["items"] = selected
                return httpx.Response(200, json=value)

            service = JsonService(
                "https://memory.fixture.invalid",
                "memory",
                transport=httpx.MockTransport(ranked_owner),
            )
            services.append(service)
            h.memory.client = service
            complete = model.complete

            async def long_read(generation, messages, *, tools):
                output, receipt = await complete(generation, messages, tools=tools)
                for call in output.get("tool_calls", []):
                    name = call["function"]["name"]
                    if name in {"life_reading_read", "life_activity_save"}:
                        value = json.loads(call["function"]["arguments"])
                        if name == "life_reading_read":
                            value["value"]["range"]["end"] = 120
                        else:
                            value["value"]["checkpoint"].update(
                                position=120, note="依据实际120字符区间形成认识，整篇未读完"
                            )
                        call["function"]["arguments"] = canonical(value)
                return output, receipt

            model.complete = long_read
            await h.core.life.autonomy.work(actor_id="actor:a")
            for _ in range(8):
                await h.core.life.activities.work(actor_id="actor:a")
            assert h.core.life.activities.current("actor:a") is None
            assert [row["kind"] for row in derived_items] == ["understanding"]
            basis = await h.core.actor_memory.query(
                "actor:a", dict(purpose="plan", audience=dict(kind="actor_only")), "恒星演化"
            )

            async def validate():
                await h.core.actor_memory.check(basis)

            references = derived_items[0]["episode_refs"]
            await h.core.actor_memory.append_validated_derived(
                "actor:a",
                "preference-after-actual-read",
                "actual-preference",
                dict(
                    kind="preference",
                    text="实际阅读后仍想研究恒星演化；这份兴趣可以继续修正。",
                    episode_refs=references,
                    importance="normal",
                    retention="medium",
                ),
                {canonical(ref) for ref in references},
                validate,
            )
            mixed = await h.core.actor_memory.query(
                "actor:a",
                dict(purpose="plan", audience=dict(kind="actor_only")),
                "恒星演化",
                budget_bytes=4096,
                limit=6,
            )
            assert "episode" in {item["kind"] for item in mixed["items"]}
            assert not {"understanding", "preference"} <= {item["kind"] for item in mixed["items"]}
            assert await h.core.life.autonomy.work(actor_id="actor:a")
            chosen = model.goals[-1]["actor_self_memory"]
            assert {item["kind"] for item in chosen} == {"understanding", "preference"}
            assert all(item["basis"][0]["episode_ref"] == references[0] for item in chosen)
            assert len(canonical(chosen).encode()) <= 8192
            assert h.core.life.activities.current("actor:a")["title"] == "继续研究恒星演化"
            assert not h.core.store.list("turns") and not h.memory.commits
        finally:
            await close(h, services)

    asyncio.run(run())


BINDING = dict(
    binding_id="00000000-0000-4000-8000-000000000001",
    binding_revision=3,
    instance_id="00000000-0000-4000-8000-000000000002",
    action_id="discover",
)
INVOCATION = "00000000-0000-4000-8000-000000000003"


class RecordedOwners(Owners):
    def __init__(self, h):
        super().__init__(h)
        self.platform_calls = []
        self.platform_unknown = False
        self.receipt = None
        self.derived = False

    def authority(self, request):
        body = json.loads(request.content)
        if request.url.path != "/internal/v1/actor-capabilities/operate":
            return super().authority(request)
        self.platform_calls.append(copy.deepcopy(body))
        if body["operation"] == "discover":
            return httpx.Response(
                200,
                json=dict(
                    schema_version=1,
                    request_id=body["request_id"],
                    actor_id=body["actor_id"],
                    role_version=self.version,
                    capabilities=[
                        dict(
                            binding=BINDING,
                            title="真实网页候选",
                            description="录制的内容发现协议",
                            effect="read",
                            input_schema=dict(
                                type="object",
                                properties=dict(query=dict(type="string")),
                                required=["query"],
                                additionalProperties=False,
                            ),
                            output_schema=dict(type="object"),
                            availability="ready",
                            reason=None,
                        )
                    ],
                ),
            )
        if self.platform_unknown:
            raise httpx.ReadError("synthetic lost platform response")
        if body["operation"] == "invoke":
            self.receipt = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                invocation_id=INVOCATION,
                binding=BINDING,
                intent_key=body["intent_key"],
                revision=1,
                execution="queued",
                effect="none",
                result=None,
                error_code=None,
            )
        else:
            self.receipt = dict(
                self.receipt,
                request_id=body["request_id"],
                execution="succeeded",
                revision=2,
                result=dict(
                    candidates=[
                        dict(
                            title="真实区间",
                            summary="检索摘要，不是正文",
                            read_locator=dict(
                                owner="knowledge",
                                kind="url",
                                url="https://original.fixture.invalid/article",
                            ),
                            readability="unverified",
                        )
                    ]
                ),
            )
        return httpx.Response(200, json=self.receipt)

    def content(self, request):
        body = json.loads(request.content)
        if request.url.path.endswith("/acquire"):
            assert body["source"]["url"] == "https://original.fixture.invalid/article"
            return httpx.Response(
                200,
                json=dict(
                    schema_version=1,
                    request_id=body["principal"]["request_id"],
                    content_ref=self.reference,
                    current=True,
                    access_version=1,
                    available_until=utc(self.h.clock() + 60),
                    source=dict(
                        kind="url",
                        locator=body["source"]["url"],
                        resolved_url=body["source"]["url"],
                        filename=None,
                        acquired_at=utc(self.h.clock()),
                        source_time=None,
                        media_type="text/html",
                        size=len(self.prose.encode()),
                    ),
                ),
            )
        return super().content(request)

    async def memory(self, request):
        body = json.loads(request.content)
        if body["operation"] == "query" and any(
            row.get("command") == "append_episode" for row in self.receipts.values()
        ):
            source = next(
                row["source"]
                for row in self.h.core.store.list("outbox")
                if row.get("actor_id") == "actor:a"
            )
            row = dict(
                ref=dict(kind="episode", id="episode:actual", revision=1),
                kind="episode",
                text="实际读取星空区间",
                episode_refs=[dict(episode_id="episode:actual", expected_revision=1)],
                source_refs=[source],
                recorded_at=utc(self.h.clock()),
                expires_at=None,
            )
            if self.derived:
                row.update(
                    ref=dict(kind="derived", id="understanding:one", revision=1),
                    kind="understanding",
                    text="现在想继续研究恒星演化，依据实际阅读而非标题。",
                )
            self.material = [row] if row["kind"] in body["kinds"] else []
        result = await super().memory(request)
        if body["operation"] == "append_derived":
            self.derived = True
        return result


class RecordedModel:
    available = True

    def __init__(self, h, owners):
        self.h, self.owners = h, owners
        self.goals, self.rounds = [], []
        self.gate = None
        self.fail_read = False

    async def generate(self, generation, messages):
        assert generation["actor_id"] == "actor:a"
        captured = json.loads(messages[1]["content"])
        self.goals.append(captured)
        if self.gate:
            await self.gate.wait()
        title = "继续研究恒星演化" if captured["actor_self_memory"] else "阅读恒星文章并形成看法"
        return [
            canonical(
                dict(
                    decision="new",
                    concern_id=None,
                    title=title,
                    goal="发现真实网页、读取原文、记录有依据的理解和待研究问题",
                    reason="来自人格和实际兴趣",
                    delay_seconds=600,
                )
            )
        ], dict(recorded=True)

    async def complete(self, generation, messages, *, tools):
        names = {tool["function"]["name"] for tool in tools}
        assert {
            "platform_discover",
            "platform_invoke",
            "actor_memory_recall",
            "actor_memory_remember",
            "life_reading_read",
        } <= names
        assert "memory_propose" not in names and "memory_recall" not in names
        self.rounds.append(copy.deepcopy(messages))
        n = len(self.rounds) - 1
        ref = self.owners.reference
        activity = self.h.core.life.activities.current("actor:a")
        if n == 0:
            calls = [("platform_discover", {})]
        elif n == 1:
            calls = [
                (
                    "platform_invoke",
                    dict(
                        binding=BINDING, arguments=dict(query="恒星"), intent="read-stellar-article"
                    ),
                )
            ]
        elif n == 2:
            calls = [("platform_get", dict(invocation_id=INVOCATION))]
        elif n == 3:
            calls = [
                (
                    "life_content_acquire",
                    dict(
                        expected_version=0,
                        value=dict(
                            source=dict(kind="url", url="https://original.fixture.invalid/article"),
                            purpose="read",
                        ),
                    ),
                )
            ]
        elif n == 4:
            calls = [
                (
                    "life_reading_open",
                    dict(
                        expected_version=0,
                        value=dict(
                            id="reading:autonomous", content_ref=ref, mode="solo", participants=[]
                        ),
                    ),
                ),
                (
                    "life_reading_read",
                    dict(
                        expected_version=1,
                        value=dict(
                            id="reading:autonomous", range=dict(unit="characters", start=0, end=4)
                        ),
                    ),
                ),
            ]
        elif n == 5:
            calls = [("actor_memory_recall", dict(text="实际阅读的星空区间"))]
        elif n == 6:
            calls = [
                (
                    "actor_memory_remember",
                    dict(
                        kind="understanding",
                        text="读到的区间提出了恒星演化问题；后续要继续查证。",
                        episode_refs=[dict(episode_id="episode:actual", expected_revision=1)],
                        importance="normal",
                        retention="medium",
                    ),
                )
            ]
        else:
            value = {
                key: activity[key]
                for key in (
                    "id",
                    "title",
                    "state",
                    "checkpoint",
                    "next_due_at",
                    "resume_condition",
                    "result_refs",
                )
            }
            value.update(
                state="completed",
                next_due_at=None,
                checkpoint=dict(
                    step=1,
                    position=4,
                    unit="characters",
                    note="实际原件区间及理解已可靠保存，整篇未读完",
                ),
            )
            calls = [
                ("life_activity_save", dict(expected_version=activity["version"], value=value))
            ]
        return dict(
            role="assistant",
            content=None,
            tool_calls=[
                dict(
                    id="call:" + str(n) + ":" + str(i),
                    type="function",
                    function=dict(name=name, arguments=canonical(args)),
                )
                for i, (name, args) in enumerate(calls)
            ],
        ), dict(recorded=True)


async def autonomy_setup(path=":memory:"):
    h, _, old = await setup(path)
    await asyncio.gather(*(client.close() for client in old))
    owners = RecordedOwners(h)
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
            "https://original.fixture.invalid",
            "knowledge",
            transport=httpx.MockTransport(owners.content),
        ),
    ]
    h.memory.client = services[1]
    h.core.knowledge_client = services[2]
    h.core.actor_memory = ActorMemory(h.core, services[0])
    h.core.life.actor_memory = h.core.actor_memory
    h.clock.now = datetime(2026, 10, 28, 2, tzinfo=timezone.utc).timestamp()  # 10:00 +08
    model = RecordedModel(h, owners)
    h.core.life.gateway, h.core.life.config_version = model, 1
    h.core.life.default_config_version = 1
    h.core.life.tick(force=True)
    # setup preloads an unrelated manual reading fixture; this flow must acquire its own original.
    h.core.store.db.execute("DELETE FROM life_content_refs")
    h.core.store.db.execute("DELETE FROM life_reading")
    return h, owners, services, model


def test_zero_user_input_goal_discover_actual_read_episode_understanding_and_next_goal():
    async def run():
        h, owners, services, model = await autonomy_setup()
        try:
            assert not h.core.store.list("turns")
            assert await h.core.life.autonomy.work(actor_id="actor:a")
            initial = h.core.life.activities.current("actor:a")
            assert initial["autonomous_goal_id"] and initial["state"] == "running"
            assert initial["checkpoint"]["step"] == 0
            assert not h.core.store.list("outbox")
            for _ in range(8):
                await h.core.life.activities.work(actor_id="actor:a")
            final = h.core.life.activities.get("actor:a", initial["id"])
            assert final["state"] == "completed"
            assert (
                h.core.life.concerns.get("actor:a", initial["autonomous_goal_id"])["state"]
                == "completed"
            )
            assert [
                call["operation"]
                for call in owners.memory_calls
                if call["operation"] in {"append_episode", "append_derived"}
            ] == ["append_episode", "append_derived"]
            assert owners.derived and owners.reads
            reading = h.core.store.get("life_reading", "reading:autonomous")
            assert reading["coverage"] == [dict(unit="characters", start=0, end=4)]
            assert (
                reading["state"] == "reading"
            )  # Actual goal completed; the whole original was not.
            assert not h.core.store.list("turns") and not h.memory.commits
            assert all(row.get("actor_id") == "actor:a" for row in owners.memory_calls)
            assert await h.core.life.autonomy.work(actor_id="actor:a")
            assert model.goals[-1]["actor_self_memory"][0]["kind"] == "understanding"
            assert h.core.life.activities.current("actor:a")["title"] == "继续研究恒星演化"
            scope = dict(
                actor_id="actor:a",
                person_id="person:other",
                audience="group",
                conversation_id="group:other",
            )
            assert not [
                item
                for item in h.core.life.activities.page("actor:a", scope=scope)[0]
                if item.get("autonomous_goal_id")
            ]
            assert h.core.life.summary("actor:a", scope)["activity"] == "处理个人事项"
            assert "恒星" not in canonical(h.core.life.materials("actor:a", "2026-10-28"))
        finally:
            await close(h, services)

    asyncio.run(run())


def test_goal_late_model_cannot_override_manual_hold_or_write_any_user_memory():
    async def run():
        h, owners, services, model = await autonomy_setup()
        try:
            model.gate = asyncio.Event()
            task = asyncio.create_task(h.core.life.autonomy.work(actor_id="actor:a"))
            while not model.goals:
                await asyncio.sleep(0)
            actor = h.core.store.get("life_actors", "actor:a")
            h.core.life.set_activity(
                "actor:a", "已约定事项", expected=actor["version"], hold_until=h.clock() + 600
            )
            model.gate.set()
            await task
            assert not [
                item
                for item in h.core.store.list("life_concerns")
                if item["id"].startswith("autonomy:")
            ]
            assert not owners.memory_calls or all(
                item["operation"] in {"query", "check_memory"} for item in owners.memory_calls
            )
            assert not h.memory.commits
        finally:
            await close(h, services)

    asyncio.run(run())


def test_unknown_platform_does_not_issue_another_invoke_or_roll_a_new_goal():
    async def run():
        h, owners, services, model = await autonomy_setup()
        try:
            await h.core.life.autonomy.work(actor_id="actor:a")
            activity = h.core.life.activities.current("actor:a")
            await h.core.life.activities.work(actor_id="actor:a")
            owners.platform_unknown = True
            await h.core.life.activities.work(actor_id="actor:a")
            current = h.core.life.activities.get("actor:a", activity["id"])
            assert current["state"] == "paused"
            h.clock.advance(1900)
            for _ in range(3):
                await h.core.life.activities.work(actor_id="actor:a")
                assert not await h.core.life.autonomy.work(actor_id="actor:a")
            assert (
                len([call for call in owners.platform_calls if call["operation"] == "invoke"]) == 1
            )
            assert not owners.derived and not h.core.store.list("outbox")
        finally:
            await close(h, services)

    asyncio.run(run())


def test_known_reading_round_restarts_without_replaying_discovery_or_original_progress(tmp_path):
    async def run():
        h, owners, services, model = await autonomy_setup(tmp_path / "autonomy.db")
        try:
            await h.core.life.autonomy.work(actor_id="actor:a")
            activity = h.core.life.activities.current("actor:a")
            for _ in range(5):
                await h.core.life.activities.work(actor_id="actor:a")
            progress = copy.deepcopy(h.core.store.get("life_reading", "reading:autonomous"))
            await h.core.close()
            h.core = h.new_core(
                background_gateway=model,
                life_config_version=1,
                actor_memory_authority=services[0],
                knowledge_client=services[2],
            )
            h.core.recover()
            assert h.core.life.activities.get("actor:a", activity["id"])["checkpoint"]["step"] == 0
            for _ in range(3):
                await h.core.life.activities.work(actor_id="actor:a")
            assert h.core.life.activities.get("actor:a", activity["id"])["state"] == "completed"
            assert h.core.store.get("life_reading", "reading:autonomous") == progress
            assert (
                len([call for call in owners.platform_calls if call["operation"] == "invoke"]) == 1
            )
            assert (
                len([call for call in owners.memory_calls if call["operation"] == "append_episode"])
                == 1
            )
            assert (
                len([call for call in owners.memory_calls if call["operation"] == "append_derived"])
                == 1
            )
            assert not h.core.store.list("turns") and not h.memory.commits
        finally:
            await close(h, services)

    asyncio.run(run())


@pytest.mark.parametrize("withdraw", ["original", "role"])
def test_withdrawn_original_or_role_blocks_next_model_and_has_no_derived_memory(withdraw):
    async def run():
        h, owners, services, model = await autonomy_setup()
        try:
            await h.core.life.autonomy.work(actor_id="actor:a")
            activity = h.core.life.activities.current("actor:a")
            for _ in range(5):
                await h.core.life.activities.work(actor_id="actor:a")
            count = len(model.rounds)
            if withdraw == "original":
                owners.withdrawn = True
            else:
                actor = h.core.store.get("life_actors", "actor:a")
                actor["life_runtime_epoch"] = actor.get("life_runtime_epoch", 0) + 1
                h.core.store.put("life_actors", actor)
            await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.rounds) == count
            assert h.core.life.activities.get("actor:a", activity["id"])["state"] == "paused"
            assert not owners.derived
            assert not h.memory.commits
        finally:
            await close(h, services)

    asyncio.run(run())


def test_failed_original_read_cannot_be_remembered_or_complete_the_goal():
    async def run():
        h, owners, services, model = await autonomy_setup()
        try:
            await h.core.life.autonomy.work(actor_id="actor:a")
            activity = h.core.life.activities.current("actor:a")
            for _ in range(4):
                await h.core.life.activities.work(actor_id="actor:a")
            owners.withdrawn = True
            for _ in range(4):
                await h.core.life.activities.work(actor_id="actor:a")
            assert h.core.life.activities.get("actor:a", activity["id"])["state"] != "completed"
            assert not h.core.store.list("outbox") and not owners.derived
            assert not [
                call for call in owners.memory_calls if call["operation"].startswith("append_")
            ]
        finally:
            await close(h, services)

    asyncio.run(run())


def test_defer_continue_abandon_persist_in_existing_owners_across_day_and_epoch():
    async def run():
        h, owners, services, model = await autonomy_setup()
        try:
            await h.core.life.autonomy.work(actor_id="actor:a")
            activity = h.core.life.activities.current("actor:a")
            goal_id = activity["autonomous_goal_id"]
            actor = h.core.store.get("life_actors", "actor:a")
            captured = h.core.life.autonomy.snapshot(actor, h.core.life.autonomy.window(actor))
            choice = dict(
                decision="defer",
                concern_id=goal_id,
                title="暂缓",
                goal="稍后继续原目标",
                reason="先处理作息",
                delay_seconds=86400,
            )
            h.core.life.autonomy.apply(actor, choice, captured, None)
            assert h.core.life.activities.get("actor:a", activity["id"])["state"] == "paused"
            assert h.core.life.concerns.get("actor:a", goal_id)["next_due_at"] == h.clock() + 86400
            h.clock.advance(86401)
            actor = h.core.store.get("life_actors", "actor:a")
            captured = h.core.life.autonomy.snapshot(actor, h.core.life.autonomy.window(actor))
            h.core.life.autonomy.apply(actor, dict(choice, decision="continue"), captured, None)
            assert h.core.life.activities.get("actor:a", activity["id"])["checkpoint"]["step"] == 0
            assert h.core.life.activities.get("actor:a", activity["id"])["state"] == "running"
            actor = h.core.store.get("life_actors", "actor:a")
            h.core.life.autonomy.apply(actor, dict(choice, decision="abandon"), captured, None)
            assert h.core.life.concerns.get("actor:a", goal_id)["state"] == "cancelled"
            assert h.core.life.activities.get("actor:a", activity["id"])["state"] == "cancelled"
            assert not h.core.store.list("outbox") and not owners.derived and not h.memory.commits
        finally:
            await close(h, services)

    asyncio.run(run())


def test_partial_read_revalidates_requested_range_without_fabricating_coverage():
    async def run():
        from test_actor_memory import read

        h, owners, services = await setup()
        requested = []

        def content(request):
            requested.append(json.loads(request.content)["range"])
            result = owners.content(request).json()
            result.update(
                text=owners.prose[:4],
                coverage=dict(unit="characters", start=0, end=4),
                complete=False,
                gaps=["range_truncated"],
            )
            return httpx.Response(200, json=result)

        client = JsonService(
            "https://original.fixture.invalid", "knowledge", transport=httpx.MockTransport(content)
        )
        services.append(client)
        h.core.knowledge_client = client
        try:
            await read(h, end=10)
            evidence = next(
                row
                for row in h.core.store.list("metadata")
                if row["id"].startswith("actor-memory-source:")
            )
            assert evidence["observed"]["coverage"]["end"] == 4
            assert evidence["observed"]["requested_range"]["end"] == 10
            await h.core.reading.check_read("actor:a", evidence["observed"])
            assert requested == [dict(unit="characters", start=0, end=10)] * 2
            assert h.core.store.get("life_reading", "reading:actual")["state"] == "reading"
        finally:
            await close(h, services)

    asyncio.run(run())
