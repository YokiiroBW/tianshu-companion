"""BOT reading ownership and receipt recovery over synthetic HTTP owner ports."""

import asyncio
import copy
import hashlib
import json
from datetime import datetime
from unittest.mock import patch

import httpx
import pytest

from support import Harness
from test_daily_life import Model
from test_delivery_v2 import source_turn
from test_life_background import activity_core, ActivityModel
from tianshu_companion import observability as obs
from tianshu_companion.actor_memory import ActorMemory, OUTBOX_PENDING, SOURCE_PREFIX
from tianshu_companion.app import create_app
from tianshu_companion.clients import JsonService, utc
from tianshu_companion.context import ACTOR_MEMORY_DOMAIN
from tianshu_companion.contracts import Fault, canonical, digest
from tianshu_companion.life import zone
from test_life import Model as DiaryModel


class Owners:
    def __init__(self, h):
        self.h = h
        self.version = 7  # Deliberately distinct from Companion 19 / Memory 23.
        self.calls, self.reads, self.memory_calls = [], [], []
        self.receipts = {}
        self.lost_ack = False
        self.withdrawn = False
        self.changed = False
        self.forbid_context = False
        self.disclose = True
        self.wrong_receipt_request = False
        self.derived_failure = None
        self.material = []
        self.interests = []
        self.policy = "p" * 64
        self.prose = "原件🍋：真正读到这一段，但并未读完后文。"
        self.reference = dict(
            owner="memory",
            object_id="document:actual-owner",
            version=1,
            kind="text",
            sha256=hashlib.sha256(self.prose.encode()).hexdigest(),
            sources=[],
            coverage=dict(unit="characters", start=0, end=len(self.prose), total=len(self.prose)),
        )

    def authority(self, request):
        body = json.loads(request.content)
        self.calls.append(copy.deepcopy(body))
        if body["operation"] == "discover":
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                role_version=self.version,
                capabilities=[],
            )
        else:
            if body["role_version"] != self.version:
                return httpx.Response(409, json=Fault("version_conflict").wire(body["request_id"]))
            value = {
                key: body[key]
                for key in ("schema_version", "request_id", "actor_id", "role_version")
            }
            if body["operation"] == "actor_check":
                assert body["requested_operation"] in {
                    "append_episode",
                    "source_facts",
                    "source_check",
                }
                value.update(
                    state="enabled",
                    allowed_operations=[
                        "append_episode",
                        "get_receipt",
                        "query",
                        "check_memory",
                        "append_derived",
                    ],
                    policy_version=self.policy,
                )
            else:
                value.update(
                    context=body["context"],
                    allowed=not self.forbid_context,
                    access_version=self.policy,
                )
        return httpx.Response(200, json=value)

    def content(self, request):
        body = json.loads(request.content)
        self.reads.append(copy.deepcopy(body))
        if self.withdrawn:
            return httpx.Response(
                404, json=Fault("not_found").wire(body["principal"]["request_id"])
            )
        value = body["range"]
        text = self.prose[value["start"] : value["end"]]
        if self.changed:
            text = "已更改"
        return httpx.Response(
            200,
            json=dict(
                schema_version=1,
                request_id=body["principal"]["request_id"],
                content_ref=self.reference,
                text=text,
                media_url=None,
                coverage=value,
                complete=True,
                representations=[],
                gaps=[],
                available_until=utc(self.h.clock() + 60),
                current=True,
                access_version=1,
                source=dict(
                    kind="upload",
                    locator="fixture:actual-upload",
                    resolved_url=None,
                    filename="fixture.txt",
                    acquired_at=utc(self.h.clock()),
                    source_time=None,
                    media_type="text/plain",
                    size=len(self.prose.encode()),
                ),
            ),
        )

    async def memory(self, request):
        body = json.loads(request.content)
        self.h.contracts.check("actor-self-memory#request", body)
        self.memory_calls.append(copy.deepcopy(body))
        assert body["role_version"] == self.version
        if body["operation"] == "get_receipt":
            value = self.receipts.get(body["idempotency_key"])
            if value is None:
                return httpx.Response(
                    404,
                    json=dict(
                        schema_version=1,
                        request_id=body["request_id"],
                        code="not_found",
                        retryable=False,
                        execution="not_started",
                    ),
                )
            if self.wrong_receipt_request:
                value = dict(value, request_id="unrelated:original-command")
            return httpx.Response(200, json=value)
        if body["operation"] == "append_episode":
            # Actual consumer callbacks exercise BOTH the action and its leaf;
            # this is a synthetic Memory owner, not claimed real Memory acceptance.
            source_request = dict(
                schema_version=1,
                request_id="facts:callback",
                actor_id=body["actor_id"],
                role_version=body["role_version"],
                operation="source_facts",
                refs=[body["source"]],
            )
            facts = await self.h.core.actor_memory.source("memory", source_request)
            fact = facts["facts"][0]
            refs = [body["source"], *(row["content_ref"] for row in fact["content_reads"])]
            checked = await self.h.core.actor_memory.source(
                "memory",
                dict(
                    source_request,
                    operation="source_check",
                    refs=refs,
                    context=dict(purpose="reflection", audience=dict(kind="actor_only")),
                ),
            )
            assert all(
                row["state"] == "current" and row["read_allowed"] for row in checked["items"]
            )
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                command="append_episode",
                idempotency_key=body["idempotency_key"],
                state="accepted",
                episode_id="episode:actual",
                revision=1,
                domain_version=23,
            )
            self.receipts[body["idempotency_key"]] = value
            if self.lost_ack:
                self.lost_ack = False
                raise httpx.ReadError("synthetic lost ACK")
        elif body["operation"] == "maintain":
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                command="maintain",
                idempotency_key=body["idempotency_key"],
                state="accepted",
                domain_version=23,
                expired_count=1,
                compressed_count=0,
                remaining=False,
            )
            self.receipts[body["idempotency_key"]] = value
            if self.lost_ack:
                self.lost_ack = False
                raise httpx.ReadError("synthetic lost maintenance ACK")
        elif body["operation"] in {"query", "interests"}:
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                role_version=body["role_version"],
                operation=body["operation"],
                context=body["context"],
                domain_version=23,
                checked_at=utc(self.h.clock()),
                items=self.interests if body["operation"] == "interests" else self.material,
                omitted=False,
            )
        elif body["operation"] == "check_memory":
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                role_version=body["role_version"],
                operation="check_memory",
                context=body["context"],
                domain_version=23,
                checked_at=utc(self.h.clock()),
                items=[
                    dict(
                        ref=ref,
                        current_revision=ref["revision"],
                        state="withdrawn" if self.withdrawn else "current",
                        read_allowed=not self.withdrawn,
                        disclosure_allowed=not self.withdrawn
                        and self.disclose
                        and body["context"]["audience"]["kind"] != "actor_only",
                    )
                    for ref in body["memories"]
                ],
            )
        elif body["operation"] == "get_episode":
            source = self.material[0]["source_refs"][0]
            facts = await self.h.core.actor_memory.source(
                "memory",
                dict(
                    schema_version=1,
                    request_id="basis:owner",
                    actor_id=body["actor_id"],
                    role_version=body["role_version"],
                    operation="source_facts",
                    refs=[source],
                ),
            )
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                role_version=body["role_version"],
                operation="get_episode",
                context=body["context"],
                domain_version=23,
                episode=dict(
                    episode_id=body["episode_id"],
                    revision=body["expected_revision"],
                    actor_id=body["actor_id"],
                    state="active",
                    fact=facts["facts"][0],
                    recorded_at=utc(self.h.clock()),
                ),
            )
        else:
            assert body["operation"] == "append_derived"
            if self.derived_failure is not None:
                execution = self.derived_failure
                self.derived_failure = None
                return httpx.Response(
                    429,
                    json=dict(
                        schema_version=1,
                        request_id=body["request_id"],
                        code="dependency_unavailable",
                        retryable=execution == "not_started",
                        execution=execution,
                    ),
                )
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                actor_id=body["actor_id"],
                command="append_derived",
                idempotency_key=body["idempotency_key"],
                state="accepted",
                ref=dict(kind="derived", id="understanding:one", revision=1),
                domain_version=24,
            )
            self.receipts[body["idempotency_key"]] = value
        self.h.contracts.check("actor-self-memory#response", value)
        return httpx.Response(200, json=value)


async def setup(path=":memory:"):
    h = Harness(path, silence_ms=0)
    h.core.roles["actor:a"]["version"] = 19
    h.core.recover()
    owners = Owners(h)
    services = [
        JsonService(
            "https://authority.fixture.invalid",
            "authority",
            transport=httpx.MockTransport(owners.authority),
        ),
        JsonService(
            "https://memory.fixture.invalid",
            "memory",
            transport=httpx.MockTransport(owners.memory),
            memory_dependency=True,
        ),
        JsonService(
            "https://original.fixture.invalid",
            "owner",
            transport=httpx.MockTransport(owners.content),
        ),
    ]
    h.memory.client = services[1]
    h.core.knowledge_client = services[2]
    h.core.actor_memory = ActorMemory(h.core, services[0])
    h.core.life.actor_memory = h.core.actor_memory
    h.core.store.put(
        "life_content_refs",
        dict(
            id="content:" + digest(owners.reference),
            actor_id="actor:a",
            scope=None,
            content_ref=owners.reference,
            query=None,
        ),
    )
    h.core.store.put(
        "life_reading",
        dict(
            id="reading:actual",
            actor_id="actor:a",
            conversation_id="actor:a",
            scope=None,
            version=1,
            state="open",
            runtime_epoch=h.core.store.get("life_actors", "actor:a").get("life_runtime_epoch", 0),
            content_ref=owners.reference,
            coverage=[],
            position=dict(unit="characters", start=0, end=0),
        ),
    )
    return h, owners, services


async def read(h, end=4, *, actor_observed=True, key="actual-read"):
    item = h.core.store.get("life_reading", "reading:actual")
    return await h.core.reading.action(
        "actor:actor:a",
        dict(
            schema_version=2,
            request_id=key,
            actor_id="actor:a",
            operation="reading.read",
            expected_version=item["version"],
            value=dict(id=item["id"], range=dict(unit="characters", start=0, end=end), query=None),
        ),
        return_content=actor_observed,
    )


async def close(h, services):
    await h.core.close()
    await asyncio.gather(*(service.close() for service in services))


def material(h, owners):
    source = next(
        row["source"] for row in h.core.store.list("outbox") if row["state"] in OUTBOX_PENDING
    )
    return dict(
        ref=dict(kind="episode", id="episode:actual", revision=1),
        kind="episode",
        text="实际阅读了部分原件；没有全书完成。",
        episode_refs=[dict(episode_id="episode:actual", expected_revision=1)],
        source_refs=[source],
        recorded_at=utc(h.clock()),
        expires_at=None,
    )


async def recall_basis(h, turn):
    """Persist the actual tool result beside its real assistant call identity."""
    call = dict(
        id="call:actual-recall",
        type="function",
        function=dict(name="actor_memory_recall", arguments=canonical(dict(text="实际阅读依据"))),
    )
    result, _ = await h.core.role_actions.execute(turn["id"], call)
    fresh = h.core.store.get("turns", turn["id"])
    fresh["native_execution"] = dict(
        messages=[
            dict(role="assistant", content=None, tool_calls=[call]),
            dict(
                role="tool",
                tool_call_id=call["id"],
                content=canonical(dict(state="completed", result=result)),
            ),
        ]
    )
    h.core._save_turn(fresh)


def test_actual_bot_read_atomically_records_actor_leaf_and_preserves_fictional_event():
    async def run():
        h, owners, services = await setup()
        try:
            result = await read(h)
            assert result["actual_content"]["text"] == owners.prose[:4]
            reading = h.core.store.get("life_reading", "reading:actual")
            assert reading["state"] == "reading"
            sources = [
                row for row in h.core.store.list("metadata") if row["id"].startswith(SOURCE_PREFIX)
            ]
            assert len(sources) == 2
            action = next(row for row in sources if row["fact"]["content_reads"])
            leaf = next(row for row in sources if not row["fact"]["content_reads"])
            assert action["fact"]["content_reads"][0]["content_ref"] == leaf["source"]
            assert (
                action["fact"]["outcome"] == "observed"
                and action["fact"]["provenance"] == "digital_action"
            )
            assert leaf["observed"]["content_ref"] == owners.reference
            assert leaf["observed"]["coverage"] == dict(unit="characters", start=0, end=4)
            assert action["fact"]["source_constraints"] == dict(kind="actor_only")
            assert owners.prose[:4] in action["fact"]["summary"]
            assert all(event["fictional"] for event in h.core.store.list("life_events"))
            count = len(sources)
            await read(h, actor_observed=False, key="human-browser-progress")
            assert (
                len(
                    [
                        row
                        for row in h.core.store.list("metadata")
                        if row["id"].startswith(SOURCE_PREFIX)
                    ]
                )
                == count
            )
        finally:
            await close(h, services)

    asyncio.run(run())


def test_read_source_outbox_failure_rolls_back_original_fact_transaction(monkeypatch):
    async def run():
        h, owners, services = await setup()
        original = h.core.store.put
        before = copy.deepcopy(h.core.store.get("life_reading", "reading:actual"))

        def fail(table, value):
            if table == "outbox" and value["id"].startswith("actor-memory-outbox:"):
                raise RuntimeError("synthetic persistence failure")
            return original(table, value)

        monkeypatch.setattr(h.core.store, "put", fail)
        try:
            with pytest.raises(RuntimeError):
                await read(h)
            assert h.core.store.get("life_reading", "reading:actual") == before
            assert not h.core.store.list("outbox")
            assert not [
                row for row in h.core.store.list("metadata") if row["id"].startswith(SOURCE_PREFIX)
            ]
            assert not [
                row
                for row in h.core.store.list("life_events")
                if row.get("kind") == "reading_progress"
            ]
        finally:
            await close(h, services)

    asyncio.run(run())


def test_actor_outbox_uses_authoritative_version_receipt_first_and_survives_lost_ack(tmp_path):
    async def run():
        h, owners, services = await setup(tmp_path / "actor.db")
        try:
            await read(h)
            owners.lost_ack = True
            with patch.object(obs, "emit", wraps=obs.emit) as events:
                await h.core.flush_outbox()
            item = h.core.store.list("outbox")[0]
            assert item["state"] == "actor_memory_unknown"
            assert [
                call.args[2]
                for call in events.call_args_list
                if call.args[1] == "memory.phase.finished"
            ] == ["unknown"]
            original = copy.deepcopy(item["request"])
            assert original["role_version"] == 7 and h.core.roles["actor:a"]["version"] == 19
            await h.core.close()
            h.core = h.new_core(actor_memory_authority=services[0], knowledge_client=services[2])
            h.core.recover()
            owners.version = 8
            h.clock.advance(60)
            with patch.object(obs, "emit", wraps=obs.emit) as events:
                await h.core.flush_outbox()
            final = h.core.store.get("outbox", item["id"])
            assert final["state"] == "actor_memory_delivered"
            assert final["request"] == original
            assert [row["operation"] for row in owners.memory_calls] == [
                "append_episode",
                "get_receipt",
            ]
            assert owners.memory_calls[-1]["role_version"] == 8
            assert final["receipt"]["request_id"] == original["request_id"]
            assert final["receipt"]["domain_version"] == 23
            assert [
                call.args[2]
                for call in events.call_args_list
                if call.args[1] == "memory.phase.finished"
            ] == ["succeeded"]
        finally:
            await close(h, services)

    asyncio.run(run())


def test_unknown_without_receipt_does_not_change_original_role_request_or_key():
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            owners.lost_ack = True
            await h.core.flush_outbox()
            item = h.core.store.list("outbox")[0]
            frozen = copy.deepcopy(item["request"])
            owners.receipts.clear()
            owners.version = 8
            h.clock.advance(60)
            await h.core.flush_outbox()
            final = h.core.store.get("outbox", item["id"])
            assert final["state"] == "actor_memory_unknown" and final["request"] == frozen
            assert [row["operation"] for row in owners.memory_calls] == [
                "append_episode",
                "get_receipt",
            ]
        finally:
            await close(h, services)

    asyncio.run(run())


@pytest.mark.parametrize("negative", ["withdrawn", "changed"])
def test_source_http_checks_exact_actor_leaf_and_current_original(negative):
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            action = h.core.store.list("outbox")[0]["source"]
            request = dict(
                schema_version=1,
                request_id="source:one",
                actor_id="actor:a",
                role_version=7,
                operation="source_facts",
                refs=[action],
            )
            app = create_app(h.core, tokens={"memory": "memory-reader"})
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="https://core.invalid"
            ) as client:
                result = await client.post(
                    "/internal/v1/actor-self-memory/source",
                    json=request,
                    headers={"Authorization": "Bearer memory-reader"},
                )
                assert result.status_code == 200
                leaf = result.json()["facts"][0]["content_reads"][0]["content_ref"]
                checked = dict(
                    request,
                    operation="source_check",
                    refs=[action, leaf],
                    context=dict(purpose="reflection", audience=dict(kind="actor_only")),
                )
                positive = await client.post(
                    "/internal/v1/actor-self-memory/source",
                    json=checked,
                    headers={"Authorization": "Bearer memory-reader"},
                )
                assert all(row["read_allowed"] for row in positive.json()["items"])
                setattr(owners, negative, True)
                failed = await client.post(
                    "/internal/v1/actor-self-memory/source",
                    json=checked,
                    headers={"Authorization": "Bearer memory-reader"},
                )
                assert failed.status_code == 200
                assert all(not row["read_allowed"] for row in failed.json()["items"])
                assert all(row["state"] != "current" for row in failed.json()["items"])
                other = await client.post(
                    "/internal/v1/actor-self-memory/source",
                    json=dict(checked, actor_id="actor:b"),
                    headers={"Authorization": "Bearer memory-reader"},
                )
                assert other.status_code == 200 and not any(
                    row["actor_known"] for row in other.json()["items"]
                )
        finally:
            await close(h, services)

    asyncio.run(run())


def test_external_context_requires_real_platform_authority_and_cannot_disclose_actor_only_source():
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            source = h.core.store.list("outbox")[0]["source"]
            request = dict(
                schema_version=1,
                request_id="source:external",
                actor_id="actor:a",
                role_version=7,
                operation="source_check",
                refs=[source],
                context=dict(
                    purpose="dialogue",
                    audience=dict(
                        kind="group", conversation_id="group:real", assertion_ref="assertion:real"
                    ),
                ),
            )
            result = await h.core.actor_memory.source("memory", request)
            assert (
                result["items"][0]["read_allowed"] and not result["items"][0]["disclosure_allowed"]
            )
            assert len([row for row in owners.calls if row["operation"] == "context_check"]) == 2
            owners.forbid_context = True
            with pytest.raises(Fault, match="forbidden"):
                await h.core.actor_memory.source("memory", request)
            with pytest.raises(Fault, match="forbidden"):
                await h.core.actor_memory.source("nonebot", request)
        finally:
            await close(h, services)

    asyncio.run(run())


def test_new_read_watermarks_do_not_withdraw_an_unchanged_exact_old_interval():
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            source = h.core.store.list("outbox")[0]["source"]
            request = dict(
                schema_version=1,
                request_id="source:watermark",
                actor_id="actor:a",
                role_version=7,
                operation="source_check",
                refs=[source],
                context=dict(purpose="reflection", audience=dict(kind="actor_only")),
            )
            before = await h.core.actor_memory.source("memory", request)
            await read(h, end=8, key="actual-second-interval")
            after = await h.core.actor_memory.source("memory", request)
            assert after["items"][0]["state"] == "current" and after["items"][0]["read_allowed"]
            assert after["items"][0]["source_sequence"] > before["items"][0]["source_sequence"]
            assert after["items"][0]["source"] == before["items"][0]["source"] == source
        finally:
            await close(h, services)

    asyncio.run(run())


def test_actor_query_tool_records_exact_future_lineage_and_checks_withdrawal():
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            owners.material = [material(h, owners)]
            turn = await source_turn(h)
            h.core.role_actions.tools(turn)
            tool = dict(
                id="own:recall",
                type="function",
                function=dict(
                    name="actor_memory_recall", arguments=canonical(dict(text="读过哪些原件？"))
                ),
            )
            result, _ = await h.core.role_actions.execute(turn["id"], tool)
            assert (
                result["actor_id"] == "actor:a"
                and result["items"][0]["text"] == owners.material[0]["text"]
            )
            assert result["items"][0]["basis"][0]["fact"]["provenance"] == "digital_action"
            assert result["items"][0]["basis"][0]["fact"]["outcome"] == "observed"
            fresh = h.core.store.get("turns", turn["id"])
            check = next(
                row
                for row in fresh["context_checks"]
                if row["version_domain"] == ACTOR_MEMORY_DOMAIN
            )
            assert check["actor_memory_material"]["role_version"] == 7
            assert check["actor_memory_material"]["items"] == [dict(ref=owners.material[0]["ref"])]
            await h.core.actor_memory.verify(check)
            owners.disclose = False
            with pytest.raises(Fault):
                await h.core.actor_memory.verify(check)
            owners.disclose = True
            owners.withdrawn = True
            with pytest.raises(Fault):
                await h.core.actor_memory.verify(check)
            assert (
                h.core.summary_windows._reply_dependencies(
                    dict(
                        fresh,
                        reply_context_provenance=dict(version=1, complete=True, checks=[check]),
                    ),
                    {fresh["id"]: fresh},
                )
                is None
            )
        finally:
            await close(h, services)

    asyncio.run(run())


def test_actor_derived_memory_requires_returned_episode_refs_and_keeps_original_receipt():
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            owners.material = [material(h, owners)]
            turn = await source_turn(h)
            h.core.role_actions.tools(turn)
            value = dict(
                kind="understanding",
                text="阅读区间并不等于全件完成。",
                episode_refs=owners.material[0]["episode_refs"],
                importance="normal",
                retention="medium",
            )
            with pytest.raises(Fault):
                await h.core.actor_memory.derive(turn["id"], "call:one", value)
            await recall_basis(h, turn)
            receipt, _ = await h.core.actor_memory.derive(turn["id"], "call:one", value)
            replay, _ = await h.core.actor_memory.derive(turn["id"], "call:other", value)
            assert receipt == replay
            assert [
                row["operation"]
                for row in owners.memory_calls
                if row["operation"] in {"append_derived", "get_receipt"}
            ] == [
                "append_derived",
                "get_receipt",
            ]
            assert (
                next(row for row in owners.memory_calls if row["operation"] == "append_derived")[
                    "replaces"
                ]
                == []
            )
            with pytest.raises(Fault):
                await h.core.actor_memory.derive(
                    turn["id"],
                    "call:foreign",
                    dict(value, episode_refs=[dict(episode_id="other", expected_revision=1)]),
                )
        finally:
            await close(h, services)

    asyncio.run(run())


def test_another_tool_cannot_claim_actor_recall_episode_basis_by_its_result_label():
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            owners.material = [material(h, owners)]
            turn = await source_turn(h)
            h.core.role_actions.tools(turn)
            turn["native_execution"] = dict(
                messages=[
                    dict(
                        role="assistant",
                        content=None,
                        tool_calls=[
                            dict(
                                id="call:foreign-tool",
                                type="function",
                                function=dict(name="memory_recall", arguments="{}"),
                            )
                        ],
                    ),
                    dict(
                        role="tool",
                        tool_call_id="call:foreign-tool",
                        content=canonical(
                            dict(
                                state="completed",
                                result=dict(
                                    source="actor_self_memory",
                                    actor_id="actor:a",
                                    items=owners.material,
                                ),
                            )
                        ),
                    ),
                ]
            )
            h.core._save_turn(turn)
            with pytest.raises(Fault, match="invalid_input"):
                await h.core.actor_memory.derive(
                    turn["id"],
                    "call:forged-basis",
                    dict(
                        kind="understanding",
                        text="他工具不能自报来源。",
                        episode_refs=owners.material[0]["episode_refs"],
                        importance="normal",
                        retention="medium",
                    ),
                )
            assert not owners.memory_calls
        finally:
            await close(h, services)

    asyncio.run(run())


@pytest.mark.parametrize("execution", ["not_started", "unknown"])
def test_derived_command_only_retries_a_proven_unstarted_original_request(execution):
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            owners.material = [material(h, owners)]
            turn = await source_turn(h)
            h.core.role_actions.tools(turn)
            await recall_basis(h, turn)
            value = dict(
                kind="understanding",
                text="这次区间阅读不是全件完成。",
                episode_refs=owners.material[0]["episode_refs"],
                importance="normal",
                retention="medium",
            )
            owners.derived_failure = execution
            with pytest.raises(Fault, match="dependency_unavailable") as failed:
                await h.core.actor_memory.derive(turn["id"], "call:original", value)
            assert failed.value.unknown == (execution == "unknown")
            original = copy.deepcopy(owners.memory_calls[-1])
            if execution == "not_started":
                receipt, _ = await h.core.actor_memory.derive(turn["id"], "call:new", value)
                assert receipt["request_id"] == original["request_id"]
                assert [
                    row for row in owners.memory_calls if row["operation"] == "append_derived"
                ] == [original, original]
            else:
                with pytest.raises(Fault, match="result_unknown"):
                    await h.core.actor_memory.derive(turn["id"], "call:new", value)
            assert [
                row["operation"]
                for row in owners.memory_calls
                if row["operation"] in {"append_derived", "get_receipt"}
            ] == (
                ["append_derived", "get_receipt", "append_derived"]
                if execution == "not_started"
                else ["append_derived", "get_receipt"]
            )
        finally:
            await close(h, services)

    asyncio.run(run())


def test_outbox_receipt_must_identify_the_persisted_original_request():
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            owners.lost_ack = True
            await h.core.flush_outbox()
            original = copy.deepcopy(h.core.store.list("outbox")[0])
            owners.wrong_receipt_request = True
            h.clock.advance(60)
            await h.core.flush_outbox()
            final = h.core.store.get("outbox", original["id"])
            assert final["state"] == "actor_memory_unknown"
            assert final["last_error"] == "invalid_input"
            assert final["request"] == original["request"]
            assert [row["operation"] for row in owners.memory_calls] == [
                "append_episode",
                "get_receipt",
            ]
        finally:
            await close(h, services)

    asyncio.run(run())


@pytest.mark.parametrize("purpose", ["plan", "diary"])
def test_internal_plan_uses_actor_material_but_publishable_fictional_diary_does_not(purpose):
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            owners.material = [material(h, owners)]
            model = Model() if purpose == "plan" else DiaryModel()
            h.core.life.gateway = model
            h.core.life.writing = True
            h.core.life.config_version = 1
            observed = []
            original = model.generate

            async def generate(turn, messages):
                observed.append(copy.deepcopy(messages))
                return await original(turn, messages)

            model.generate = generate
            if purpose == "plan":
                await h.core.life.daily.work(actor_id="actor:a", one=True)
            else:
                # Legacy fictional generation remains valid without the actor authority;
                # AL13 source-backed generation is covered by test_diary_sources.
                h.core.life.actor_memory = None
                actor = h.core.store.get("life_actors", "actor:a")
                world = h.core.store.get("life_worlds", actor["world_id"])
                day = str(datetime.fromtimestamp(h.clock(), zone(world["timezone"])).date())
                diary = h.core.life.request_diary("actor:a", day)
                for _ in range(3):
                    await h.core.life._diary_work("actor:a")
                    if h.core.life.diary_metadata(diary["id"])["state"] != "queued":
                        break
                assert h.core.life.diary_metadata(diary["id"])["fictional"] is True
            assert observed
            if purpose == "plan":
                assert owners.material[0]["text"] in canonical(observed)
                assert any(
                    row["operation"] == "query"
                    and row["context"]["purpose"] == purpose
                    and row["context"]["audience"] == dict(kind="actor_only")
                    for row in owners.memory_calls
                )
                assert any(row["operation"] == "check_memory" for row in owners.memory_calls)
            else:
                assert owners.material[0]["text"] not in canonical(observed)
                assert "actor_self_memory" not in canonical(observed)
                assert not owners.memory_calls
        finally:
            await close(h, services)

    asyncio.run(run())


def test_activity_retains_actor_memory_input_and_checks_it_before_the_next_actual_round():
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            owners.material = [material(h, owners)]
            model = ActivityModel()
            saved = activity_core(h, model)
            operation = "activity-step:" + digest([saved["id"], saved["version"]])
            await h.core.life.activities.work(actor_id="actor:a")
            frozen = copy.deepcopy(h.core.store.get("metadata", operation))
            assert owners.material[0]["text"] in canonical(model.calls[0][1])
            assert frozen["actor_memory_input"]["role_version"] == 7
            owners.withdrawn = True
            model.tool = False
            await h.core.life.activities.work(actor_id="actor:a")
            assert len(model.calls) == 1
            assert h.core.store.get("metadata", operation)["messages"] == frozen["messages"]
            assert h.core.life.activities.get("actor:a", saved["id"])["state"] == "paused"
        finally:
            await close(h, services)

    asyncio.run(run())
