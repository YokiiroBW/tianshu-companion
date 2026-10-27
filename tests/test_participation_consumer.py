"""Real Core dispatcher/BG slots and AL11 documents with fixed local peers."""

import asyncio
import copy
import json

import httpx
import pytest

from support import Harness
from tianshu_companion.clients import JsonService, utc
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.model_selection import ModelSelection
from tianshu_companion.participation import MARKER, candidate_ref
from tianshu_companion.app import create_app


def candidate(clock, identity="one", group="20001"):
    return dict(
        schema_version=1,
        kind="participation_candidate",
        candidate_id="candidate:" + identity,
        actor_id="actor:a",
        trigger="ordinary_group_message",
        native=dict(
            framework=dict(name="nonebot2", version="2.5.0"),
            instance_id="host:one",
            namespace="qq",
            self_id="10001",
            group_id=group,
            native_message_id=identity,
            author_account_id="10002",
            message_revision=1,
        ),
        source=dict(
            source_ref="observation:" + identity,
            source_digest="a" * 64,
            scope_version=1,
            archive_epoch=1,
        ),
        author_anchor=dict(
            stage="observed",
            account_id="10002",
            group_id=group,
            entry_id=None,
            person_id=None,
            conversation_id=None,
            binding_version=None,
        ),
        configuration=dict(
            platform_generation="platform:one",
            host_generation="observation-policy:1",
            role_version=1,
            reply_policy_revision=1,
        ),
        host_epoch="host:epoch",
        sent_at=utc(clock()),
        created_at=utc(clock()),
        expires_at=utc(clock() + 60),
        mentioned=False,
        text="我们在讨论观星路线。",
    )


class Model:
    available = True

    def __init__(self, outcome="engage"):
        self.outcome, self.calls = outcome, []
        self.error = None
        self.after = None

    async def generate(self, turn, messages):
        self.calls.append((copy.deepcopy(turn), copy.deepcopy(messages)))
        if self.after:
            self.after()
        if self.error:
            raise self.error
        return [json.dumps(dict(outcome=self.outcome))], {}


class Selector:
    def __init__(self, clock):
        self.clock, self.calls = clock, []

    async def select(self, request):
        self.calls.append(request)
        assert request.function_id == "participation"
        assert request.caller_service == "companion_background"
        assert request.person_id is None and request.audience == "group"
        assert request.turn_id.startswith("candidate:")
        assert request.conversation_id.startswith("qq-group:")
        return ModelSelection(
            config_version=17, expires_at=self.clock() + 1000, caller_service="companion_background"
        )


class Peers:
    def __init__(self, h):
        self.items = [candidate(h.clock)]
        self.calls, self.decisions = [], []
        self.lose = False
        self.revoked = False

    def handle(self, request):
        body = json.loads(request.content)
        self.calls.append((request.url.path, copy.deepcopy(body)))
        if request.url.path.endswith("candidates"):
            return httpx.Response(
                200,
                json=dict(
                    state="available",
                    candidates=[] if self.revoked or body["actor_id"] != "actor:a" else self.items,
                    next=None,
                ),
            )
        if request.url.path.endswith("decision"):
            self.decisions.append(copy.deepcopy(body))
            if self.lose:
                self.lose = False
                raise httpx.ReadTimeout("fixed lost ACK", request=request)
            if self.revoked:
                return httpx.Response(409, json=dict(code="scope_changed", retryable=False))
            return httpx.Response(
                200,
                json=dict(
                    state="accepted" if body["outcome"] == "engage" else body["outcome"],
                    decision=body,
                ),
            )
        return httpx.Response(
            200,
            json=dict(
                schema_version=1,
                request_id=body["request_id"],
                frames=[
                    dict(
                        frame=value,
                        processing_enabled=True,
                        mode="active",
                        actor_id=value.get("actor_id"),
                        policy_version=1,
                        code=None,
                    )
                    for value in body["frames"]
                ],
            ),
        )


async def setup(tmp_path, outcome="engage"):
    h = Harness(tmp_path / "core.sqlite", silence_ms=0)
    await h.core.close()
    model, selector, peers = Model(outcome), Selector(h.clock), Peers(h)
    client = JsonService(
        "https://platform.fixture.invalid",
        "fixed-companion",
        transport=httpx.MockTransport(peers.handle),
    )
    h.core = h.new_core(
        background_gateway=model, background_model_selector=selector, participation_client=client
    )
    return h, model, selector, peers, client


@pytest.mark.parametrize("outcome,decision", [("engage", "engage"), ("silent", "abstain")])
def test_actual_background_dispatch_uses_al11_and_observed_author_is_not_fabricated(
    tmp_path, outcome, decision
):
    async def run():
        h, model, selector, peers, client = await setup(tmp_path, outcome)
        peers.items.append(candidate(h.clock, "foreign", "20002"))
        peers.items[-1]["text"] = "另一群的私密上下文不能混入"
        try:
            # Use the existing automatic dispatcher, never a second worker.
            await h.core.memory_learning.work()
            assert len(model.calls) == len(selector.calls) == 1
            assert peers.decisions[0]["outcome"] == decision
            assert peers.decisions[0]["contribution"] == (
                "参与当前群聊话题" if decision == "engage" else None
            )
            assert peers.decisions[0]["candidate"] == candidate_ref(peers.items[0])
            prompt = json.loads(model.calls[0][1][1]["content"])
            assert len(prompt["messages"]) == 1
            assert "另一群" not in json.dumps(prompt, ensure_ascii=False)
            assert peers.items[0]["author_anchor"]["person_id"] is None
            assert (
                h.turns() == []
            )  # Only real Platform engage/ingest may resolve and create a turn.
            assert h.core.participation.status()["available"] is True
            assert h.core.models._available == h.core.models._capacity
        finally:
            await h.core.close()
            await client.close()

    asyncio.run(run())


@pytest.mark.parametrize("failure", [False, True])
def test_silent_and_unknown_model_attempts_also_coalesce_and_preserve_original_ttl(
    tmp_path, failure
):
    async def run():
        h, model, _, peers, client = await setup(tmp_path, "silent")
        if failure:
            model.error = Fault("result_unknown", unknown=True)
        try:
            await h.core.participation.work()
            old = copy.deepcopy(peers.items[0])
            peers.items = [candidate(h.clock, "two")]
            h.clock.advance(1)
            h.core.participation.actor_cursor = 0
            await h.core.participation.work()
            assert len(model.calls) == 1
            assert peers.decisions[-1]["outcome"] == "defer"
            assert peers.decisions[-1]["reason"] == "busy"
            cooldown = h.core.store.get(
                "metadata",
                "participation:group:"
                + digest(
                    {key: old["native"][key] for key in ("instance_id", "self_id", "group_id")}
                ),
            )
            assert cooldown["expires_at"] <= h.clock() + 60
            marker = h.core.store.get("metadata", MARKER + digest(candidate_ref(old)))
            assert marker["candidate"] == old
            assert marker["state"] == ("model_started" if failure else "settled")
        finally:
            await h.core.close()
            await client.close()

    asyncio.run(run())


def test_decision_unknown_restarts_with_same_exact_document_and_no_model_replay(tmp_path):
    async def run():
        h, model, selector, peers, client = await setup(tmp_path)
        peers.lose = True
        try:
            await h.core.participation.work()
            original = copy.deepcopy(peers.decisions[0])
            await h.core.close()
            h.core = h.new_core(
                background_gateway=model,
                background_model_selector=selector,
                participation_client=client,
            )
            await h.core.participation.work()
            assert peers.decisions == [original, original]
            assert len(model.calls) == 1
            marker = h.core.store.get("metadata", MARKER + digest(candidate_ref(peers.items[0])))
            assert marker["state"] == "settled"
        finally:
            await h.core.close()
            await client.close()

    asyncio.run(run())


def test_waiting_background_slot_refreshes_policy_and_does_not_claim_after_revoke(tmp_path):
    async def run():
        h, model, _, peers, client = await setup(tmp_path)
        try:
            async with h.core.models:
                # Use all remaining actual capacity to hold this background read.
                async with h.core.models:
                    async with h.core.models:
                        async with h.core.models:
                            task = asyncio.create_task(h.core.participation.work())
                            await asyncio.sleep(0)
                            peers.revoked = True
            await task
            assert not model.calls and not peers.decisions
            assert h.core.models._available == h.core.models._capacity
        finally:
            await h.core.close()
            await client.close()

    asyncio.run(run())


def test_background_missing_and_empty_pages_never_fall_back_to_foreground_model(tmp_path):
    async def run():
        h = Harness(tmp_path / "plain.sqlite")
        try:
            assert not h.core.participation.available
            await h.core.participation.work()
            assert h.turns() == []
        finally:
            await h.core.close()
        h, model, _, peers, client = await setup(tmp_path / "configured")
        peers.items = []
        try:
            await h.core.participation.work()
            count = len(peers.calls)
            if h.core.participation.due():
                await h.core.participation.work()
            assert len(peers.calls) == count
            assert h.core.participation.next_poll > h.clock() + 1
            assert not model.calls
        finally:
            await h.core.close()
            await client.close()

    asyncio.run(run())


def test_consumer_status_is_authenticated_pure_read_and_reports_actual_missing_background(tmp_path):
    async def run():
        h = Harness(tmp_path / "status.sqlite")
        try:
            app = create_app(h.core, {"platform": "actual-platform", "nonebot": "other-service"})
            before = h.core.store.list("metadata")
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="https://core.fixture.invalid"
            ) as client:
                path = "/internal/v1/bot-bindings/participation-status"
                value = await client.post(
                    path, json={}, headers={"Authorization": "Bearer actual-platform"}
                )
                assert value.status_code == 200
                assert value.json() == dict(
                    consumer="integrated",
                    available=False,
                    reason="background_not_configured",
                    last_error=None,
                )
                assert (
                    await client.post(
                        path, json={}, headers={"Authorization": "Bearer other-service"}
                    )
                ).status_code == 403
                assert (
                    await client.post(
                        path,
                        json={"actor_id": "actor:a"},
                        headers={"Authorization": "Bearer actual-platform"},
                    )
                ).status_code == 400
            assert before == h.core.store.list("metadata")
        finally:
            await h.core.close()

    asyncio.run(run())
