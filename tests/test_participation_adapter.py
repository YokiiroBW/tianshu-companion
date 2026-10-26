"""Real SQLite/RPC ledger tests; controlled receipts here are synthetic, not SDK proof."""

import asyncio
import copy
import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "integrations" / "shared"))
import tianshu_adapter_rpc as rpc  # noqa: E402
import participation_protocol as wire  # noqa: E402


class Fixture:
    def __init__(self, path):
        self.path = path
        self.sent = []

        async def accounts():
            return [{"id": "10001", "platform": "qq", "label": "synthetic"}]

        async def send(account, conversation, text):
            self.sent.append((account, conversation, text))
            return "99881"

        self.accounts, self.send = accounts, send
        self.service = rpc.AdapterService(path, "astrbot", accounts, send)
        self.configure_synthetic_proof()

    def configure_synthetic_proof(self):
        self.service.participation.framework = {"name": "nonebot2", "version": "2.5.0"}
        self.service.participation.controlled = lambda: True

    async def call(self, operation, document, status=200):
        actual, result = await self.service.handle(
            rpc.PREFIX + operation,
            "Bearer " + self.service.access_key,
            wire.canonical(document).encode(),
        )
        assert actual == status, (actual, result, operation)
        return result

    async def apply(self, revision=1, mode="whitelist"):
        return await self.call(
            "/observation/apply",
            dict(
                request_id=f"apply:{revision}",
                account_id="10001",
                revision=revision,
                enabled=True,
                group_policy=dict(observe=True, mode=mode, list=["20001"]),
                private_policy=dict(observe=True, mode="observe_only", list=[]),
            ),
        )

    async def event(self, native_id="30001", close=True):
        assert await self.service.capture_observation(
            "10001",
            "group:20001",
            "10002",
            native_id,
            wire.utc(time.time()),
            "A real synthetic observation",
            False,
            "text",
            True,
            "test",
            None,
            {"reply_ids": [], "mentioned_accounts": []},
        )
        row = self.service.db.execute(
            "SELECT id,payload FROM observations WHERE native_id=?", (native_id,)
        ).fetchone()
        self.row, self.event_document = row[0], json.loads(row[1])
        self.native = self.service.participation.native(self.event_document)
        if close:
            with rpc.transaction(self.service.db):
                self.service.participation.step(self.native, "explicit_reply", "not_triggered")
                self.service.participation.step(self.native, "scope", "no_route")
                self.service.participation.finish(self.native, "d" * 64, True)
        return self.native

    async def candidate(self, actor="actor:a", suffix="one"):
        old = self.service.participation._record("candidate", "candidate:" + suffix)
        if old:
            return old[2]
        now = time.time()
        doc = dict(
            schema_version=1,
            kind="participation_candidate",
            candidate_id="candidate:" + suffix,
            actor_id=actor,
            trigger="ordinary_group_message",
            native=self.native,
            source=dict(
                source_ref="observation:proof",
                source_digest=wire.digest(self.event_document),
                scope_version=1,
                archive_epoch=1,
            ),
            author_anchor=dict(
                stage="observed",
                account_id="10002",
                group_id="20001",
                entry_id=None,
                person_id=None,
                conversation_id=None,
                binding_version=None,
            ),
            configuration=dict(
                platform_generation="platform:1",
                host_generation="observation-policy:1",
                role_version=1,
                reply_policy_revision=1,
            ),
            host_epoch=self.service.participation.epoch,
            sent_at=self.event_document["sent_at"],
            created_at=wire.utc(now),
            expires_at=wire.utc(now + 60),
            mentioned=False,
            text=self.event_document["text"],
        )
        # All actors share the original event's first deadline.
        saved = self.service.participation._stored(self.row)
        if saved[2] is not None:
            doc["expires_at"] = wire.utc(saved[2])
        await self.call("/participation/candidate", doc)
        return doc

    async def decision(self, candidate, revision=1, outcome="engage"):
        doc = dict(
            schema_version=1,
            kind="semantic_participation_decision",
            candidate=dict(
                candidate_id=candidate["candidate_id"],
                actor_id=candidate["actor_id"],
                candidate_digest=wire.digest(candidate),
            ),
            decision_id="decision:" + candidate["actor_id"],
            revision=revision,
            outcome=outcome,
            reason="useful_contribution" if outcome == "engage" else "no_contribution",
            contribution="Discuss the observed music" if outcome == "engage" else None,
            decided_at=wire.utc(time.time()),
            expires_at=candidate["expires_at"],
        )
        await self.call("/participation/decision", doc)
        return doc

    async def request(self, decision, key="request:claim"):
        snapshot = await self.call("/participation/ownership", {"native": self.native})
        state = snapshot["ownership"]["ownership"]
        return dict(
            schema_version=1,
            kind="participation_claim_request",
            request_id=key,
            candidate=decision["candidate"],
            decision=dict(
                decision_id=decision["decision_id"],
                revision=decision["revision"],
                decision_digest=wire.digest(decision),
            ),
            native=self.native,
            host_epoch=state["host_epoch"],
            host_generation=state["host_generation"],
            expected_owner_revision=state["owner_revision"],
            expires_at=decision["expires_at"],
        )

    async def grant(self):
        candidate = await self.candidate()
        decision = await self.decision(candidate)
        request = await self.request(decision)
        result = await self.call("/participation/claim", request)
        assert result["result"] == "granted"
        self.candidate_document, self.decision_document, self.claim_request, self.claim_result = (
            candidate,
            decision,
            request,
            result,
        )
        return result

    async def bind(self, confirmation="confirmed"):
        candidate, grant = self.candidate_document, self.claim_result
        anchor = dict(candidate["author_anchor"], stage="registered", entry_id="entry:actor")
        registration = dict(
            schema_version=1,
            kind="participation_admission_registration",
            registration_id="registration:one",
            candidate=grant["candidate"],
            decision=grant["decision"],
            claim=grant["claim"],
            native=self.native,
            source=candidate["source"],
            author_anchor=anchor,
            configuration=candidate["configuration"],
            admission=dict(
                connection_id="connection:one",
                event_id=self.native["native_message_id"],
                account_id="10002",
                event_semantic_digest="a" * 64,
                message_id="message:one",
                revision=1,
                input_entry_id="input:one",
                input_digest="b" * 64,
                source_input_assertion_ref="source-input:one",
                actor_assertion_ref="origin:one",
                fanout_ticket="fanout:one",
                core_idempotency_key="bot:" + "c" * 64,
            ),
            registered_at=wire.utc(time.time()),
            expires_at=grant["claim"]["expires_at"],
        )
        await self.call("/participation/register", registration)
        binding = dict(
            schema_version=1,
            kind="participation_delivery_binding",
            request_id="binding:one",
            candidate=grant["candidate"],
            decision=grant["decision"],
            claim=grant["claim"],
            source=candidate["source"],
            author_anchor=dict(
                anchor,
                stage="resolved",
                person_id="person:one",
                conversation_id="conv:one",
                binding_version=1,
            ),
            configuration=candidate["configuration"],
            delivery=dict(
                connection_id="connection:one",
                turn_id="turn:one",
                reply_id="reply:one",
                attempt_id="attempt:one",
            ),
            bound_at=wire.utc(time.time()),
            expires_at=grant["claim"]["expires_at"],
            registration=dict(
                registration_id=registration["registration_id"],
                registration_digest=wire.digest(registration),
            ),
            source_confirmation=confirmation,
        )
        await self.call("/participation/bind", binding)
        self.binding = binding
        self.send_request = dict(
            connection_id="connection:one",
            account_id="10001",
            policy_revision=1,
            delivery=dict(
                reply_id="reply:one",
                attempt_id="attempt:one",
                namespace="qq",
                conversation_id="group:20001",
                thread_id=None,
                text="Synthetic response",
                turn_id="turn:one",
                segment_sequence=1,
            ),
        )


@pytest.fixture
def fixture(tmp_path):
    value = Fixture(tmp_path / "adapter.sqlite")
    yield value
    value.service.close()


def test_claim_receipt_ack_replay_conflict_and_current_expiry(fixture, monkeypatch):
    async def scenario():
        await fixture.apply()
        await fixture.event()
        grant = await fixture.grant()
        await fixture.call("/observation/ack", {"account_id": "10001", "event_ids": [fixture.row]})
        replay = await fixture.call("/participation/claim", fixture.claim_request)
        assert replay["receipt_status"] == "replayed" and replay["current_claim_state"] == "active"
        changed = dict(fixture.claim_request, expected_owner_revision=99)
        conflict = await fixture.call("/participation/claim", changed)
        assert conflict["receipt_status"] == "conflict" and conflict["claim"] is None
        late = wire.stamp(fixture.claim_request["expires_at"]) + 1
        monkeypatch.setattr(time, "time", lambda: late)
        expired = await fixture.call("/participation/claim", fixture.claim_request)
        assert expired["claim"] == grant["claim"] and expired["result"] == "granted"
        assert (
            expired["current_claim_state"] == "inactive"
            and expired["ownership"]["state"] == "expired"
        )
        wire.document(expired, "participation_claim_result")

    asyncio.run(scenario())


def test_two_actors_share_one_cas_slot(fixture):
    async def scenario():
        await fixture.apply()
        await fixture.event()
        candidates = [await fixture.candidate("actor:" + actor, actor) for actor in ("a", "b")]
        decisions = [await fixture.decision(candidate) for candidate in candidates]
        requests = [
            await fixture.request(decision, "request:" + str(i))
            for i, decision in enumerate(decisions)
        ]
        results = await asyncio.gather(
            *(fixture.call("/participation/claim", request) for request in requests)
        )
        assert sorted(item["result"] for item in results) == ["denied", "granted"]
        assert (
            fixture.service.db.execute("SELECT COUNT(*) FROM participation_state").fetchone()[0]
            == 1
        )

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["missing", "capacity", "cancelled", "ambiguous"])
def test_incomplete_or_failed_receipts_never_free(fixture, failure):
    async def scenario():
        await fixture.apply()
        await fixture.event(close=False)
        if failure != "missing":
            fixture.service.participation.step(fixture.native, "scope", failure)
        fixture.service.participation.finish(fixture.native, "d" * 64, failure != "cancelled")
        state = (await fixture.call("/participation/ownership", {"native": fixture.native}))[
            "ownership"
        ]["ownership"]
        assert state["state"] == "ownership_unknown" and state["completion_evidence"] is None

    asyncio.run(scenario())


def test_unknown_host_and_astrbot_never_grant(fixture):
    async def scenario():
        await fixture.apply()
        fixture.service.participation.controlled = lambda: False
        await fixture.event()
        assert (await fixture.call("/participation/ownership", {"native": fixture.native}))[
            "ownership"
        ]["ownership"]["state"] == "ownership_unknown"
        fixture.service.participation.framework = {"name": "astrbot", "version": "4.27.3"}
        fixture.service.participation.controlled = lambda: True  # cannot override the SDK boundary
        await fixture.event("30002")
        candidate = await fixture.candidate(suffix="astr")
        decision = await fixture.decision(candidate)
        result = await fixture.call("/participation/claim", await fixture.request(decision))
        assert result["result"] == "denied" and result["reason"] == "ownership_unknown"
        wire.document(result, "participation_claim_result")

    asyncio.run(scenario())


def test_pending_binding_confirmation_and_original_sender_receipt(fixture):
    async def scenario():
        await fixture.apply()
        await fixture.event()
        await fixture.grant()
        await fixture.bind("pending")
        await fixture.call("/observation/messages/send", fixture.send_request, 403)
        assert not fixture.sent
        confirmed = dict(fixture.binding, source_confirmation="confirmed")
        await fixture.call("/participation/bind", confirmed)
        sent = await fixture.call("/observation/messages/send", fixture.send_request)
        assert sent["state"] == "sent" and len(fixture.sent) == 1
        await fixture.apply(2, "observe_only")
        assert (await fixture.call("/participation/claim", fixture.claim_request))[
            "current_claim_state"
        ] == "inactive"
        assert await fixture.call("/observation/messages/send", fixture.send_request) == sent
        assert len(fixture.sent) == 1
        changed = copy.deepcopy(fixture.send_request)
        changed["delivery"]["attempt_id"] = "attempt:replacement"
        await fixture.call("/observation/messages/send", changed, 403)

    asyncio.run(scenario())


def test_retention_removes_source_links_but_never_downgrades_late_send(fixture, monkeypatch):
    async def scenario():
        await fixture.apply()
        await fixture.event()
        await fixture.grant()
        await fixture.bind()
        old_request = fixture.claim_request
        old_send = fixture.send_request
        await fixture.call("/observation/ack", {"account_id": "10001", "event_ids": [fixture.row]})
        monkeypatch.setattr(rpc, "MAX_HISTORY", 1)
        await fixture.event(
            "30009"
        )  # exercises original retention transaction, not a helper deletion
        result = await fixture.call("/participation/claim", old_request)
        assert result["result"] == "unavailable" and result["reason"] == "row_unavailable"
        assert (
            fixture.service.db.execute("SELECT COUNT(*) FROM participation_records").fetchone()[0]
            == 0
        )
        await fixture.call("/observation/messages/send", old_send, 403)
        changed = copy.deepcopy(old_send)
        changed["delivery"]["attempt_id"] = "attempt:new"
        await fixture.call("/observation/messages/send", changed, 403)
        assert not fixture.sent
        normal = copy.deepcopy(old_send)
        normal["delivery"].update(reply_id="reply:normal", attempt_id="attempt:normal")
        assert (await fixture.call("/observation/messages/send", normal))["state"] == "sent"
        await fixture.call("/observation/ack", {"account_id": "10001", "event_ids": [fixture.row]})
        await fixture.event("30001")  # A later real SDK replay cannot create a fresh claim slot.
        assert (await fixture.call("/participation/ownership", {"native": fixture.native}))[
            "ownership"
        ]["ownership"]["state"] == "ownership_unknown"
        assert (await fixture.call("/participation/claim", old_request))["result"] == "unavailable"

    asyncio.run(scenario())


def test_restart_and_new_decision_do_not_revive_claim(fixture):
    async def scenario():
        await fixture.apply()
        await fixture.event()
        await fixture.grant()
        await fixture.decision(fixture.candidate_document, revision=2, outcome="abstain")
        assert (await fixture.call("/participation/claim", fixture.claim_request))[
            "current_claim_state"
        ] == "inactive"
        fixture.service.close()
        fixture.service = rpc.AdapterService(
            fixture.path, "astrbot", fixture.accounts, fixture.send
        )
        fixture.configure_synthetic_proof()
        result = await fixture.call("/participation/claim", fixture.claim_request)
        assert result["result"] == "granted" and result["current_claim_state"] == "inactive"
        assert result["ownership"]["state"] == "expired"

    asyncio.run(scenario())


def test_sender_unknown_receipt_survives_expiry_and_restart(fixture, monkeypatch):
    async def scenario():
        await fixture.apply()
        await fixture.event()
        await fixture.grant()
        await fixture.bind()
        calls = []

        async def ambiguous(*args):
            calls.append(args)
            raise ConnectionError("synthetic SDK response lost")

        fixture.service.send = ambiguous
        unknown = await fixture.call("/observation/messages/send", fixture.send_request)
        assert unknown["state"] == "unknown" and len(calls) == 1
        fixture.service.close()
        fixture.service = rpc.AdapterService(fixture.path, "astrbot", fixture.accounts, ambiguous)
        fixture.configure_synthetic_proof()
        late = wire.stamp(fixture.claim_request["expires_at"]) + 1
        monkeypatch.setattr(time, "time", lambda: late)
        assert await fixture.call("/observation/messages/send", fixture.send_request) == unknown
        status = {key: fixture.send_request["delivery"][key] for key in ("reply_id", "attempt_id")}
        status["connection_id"] = fixture.send_request["connection_id"]
        assert (await fixture.call("/observation/messages/status", status))["receipt"] == unknown
        assert len(calls) == 1

    asyncio.run(scenario())


def test_changed_source_and_registration_cannot_bind_other_native(fixture):
    async def scenario():
        await fixture.apply()
        await fixture.event()
        candidate = await fixture.candidate()
        forged = copy.deepcopy(candidate)
        forged.update(candidate_id="candidate:forged", text="not the observed text")
        await fixture.call("/participation/candidate", forged, 409)
        await fixture.grant()
        await fixture.bind()
        swapped = copy.deepcopy(fixture.binding)
        swapped["delivery"]["attempt_id"] = "attempt:swapped"
        await fixture.call("/participation/bind", swapped, 409)
        assert not fixture.sent

    asyncio.run(scenario())
