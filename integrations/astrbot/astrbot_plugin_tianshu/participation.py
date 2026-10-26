"""Participation facts attached to the existing adapter observation/send ledger.

All methods run under AdapterService.lock. No worker, sender, network client or
second database is created here. The physical observations row remains the root.
"""

import importlib.metadata
import json
import sys
import time
import uuid

try:
    from . import participation_protocol as wire
except ImportError:  # canonical standalone module used by SDK-independent tests
    import participation_protocol as wire

EVENT_TABLES = (
    "participation_state",
    "participation_records",
    "participation_receipts",
    "participation_bindings",
)
TABLES = (
    *EVENT_TABLES,
    "participation_retired",
    "participation_retired_requests",
    "participation_retired_events",
)
SCHEMA = """
CREATE TABLE IF NOT EXISTS participation_state (
    event_id TEXT PRIMARY KEY, native_key TEXT NOT NULL UNIQUE,
    document TEXT NOT NULL, receipts TEXT NOT NULL, deadline REAL
);
CREATE TABLE IF NOT EXISTS participation_records (
    kind TEXT NOT NULL, id TEXT NOT NULL, event_id TEXT NOT NULL,
    digest TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(kind,id)
);
CREATE INDEX IF NOT EXISTS participation_records_event ON participation_records(event_id);
CREATE TABLE IF NOT EXISTS participation_receipts (
    request_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, digest TEXT NOT NULL, document TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS participation_receipts_event ON participation_receipts(event_id);
CREATE TABLE IF NOT EXISTS participation_bindings (
    connection_id TEXT NOT NULL, reply_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
    event_id TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(connection_id,reply_id)
);
CREATE INDEX IF NOT EXISTS participation_bindings_event ON participation_bindings(event_id);
CREATE TABLE IF NOT EXISTS participation_retired (
    connection_id TEXT NOT NULL, reply_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
    PRIMARY KEY(connection_id,reply_id)
);
CREATE TABLE IF NOT EXISTS participation_retired_requests (
    request_id TEXT PRIMARY KEY, digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS participation_retired_events (native_key TEXT PRIMARY KEY);
"""


def installed_framework(adapter):
    package, name, expected = (
        ("astrbot", "astrbot", "4.27.3")
        if adapter == "astrbot"
        else ("nonebot2", "nonebot2", "2.5.0")
    )
    if (
        adapter == "astrbot"
        and getattr(sys.modules.get("astrbot"), "__version__", None) == expected
    ):
        return {"name": name, "version": expected}
    try:
        if importlib.metadata.version(package) != expected:
            return None
        if name == "nonebot2" and importlib.metadata.version("nonebot-adapter-onebot") != "2.4.0":
            return None
    except importlib.metadata.PackageNotFoundError:
        return None
    return {"name": name, "version": expected}


class Participation:
    def __init__(self, service, error):
        self.service, self.db, self.error = service, service.db, error
        self.framework = installed_framework(service.adapter)
        self.epoch = "host-epoch:" + uuid.uuid4().hex
        self.started_at = time.time()
        self.controlled = lambda: False

    def claim_supported(self):
        return self.framework == {"name": "nonebot2", "version": "2.5.0"} and self.controlled()

    def fail(self, code="scope_changed", status=409):
        raise self.error(status, code)

    def check(self, condition, code="scope_changed", status=409):
        if not condition:
            self.fail(code, status)

    def cleanup(self):
        """Called inside the same transaction as original observation retention."""
        self.db.execute(
            "INSERT INTO participation_retired_events(native_key) SELECT native_key FROM participation_state "
            "WHERE event_id NOT IN (SELECT id FROM observations) ON CONFLICT(native_key) DO NOTHING"
        )
        self.db.execute(
            "INSERT INTO participation_retired(connection_id,reply_id,attempt_id) "
            "SELECT connection_id,reply_id,attempt_id FROM participation_bindings "
            "WHERE event_id NOT IN (SELECT id FROM observations) ON CONFLICT(connection_id,reply_id) DO NOTHING"
        )
        self.db.execute(
            "INSERT INTO participation_retired_requests(request_id,digest) "
            "SELECT request_id,digest FROM participation_receipts WHERE event_id NOT IN (SELECT id FROM observations) "
            "ON CONFLICT(request_id) DO NOTHING"
        )
        for table in EVENT_TABLES:
            self.db.execute(
                f"DELETE FROM {table} WHERE event_id NOT IN (SELECT id FROM observations)"
            )

    def _row(self, native):
        row = self.db.execute(
            "SELECT id,payload,created FROM observations WHERE account_id=? AND conversation=? AND author=? AND native_id=?",
            (
                native["self_id"],
                "group:" + native["group_id"],
                native["author_account_id"],
                native["native_message_id"],
            ),
        ).fetchone()
        if row:
            event = json.loads(row[1])
            self.check(
                event["platform_id"] == native["instance_id"] == self.service.instance_id
                and event["revision"] == native["message_revision"]
            )
        return row

    def native(self, event):
        if self.framework is None or not event["conversation_id"].startswith("group:"):
            return None
        result = dict(
            framework=self.framework,
            instance_id=event["platform_id"],
            namespace="qq",
            self_id=event["self_id"],
            group_id=event["conversation_id"].split(":", 1)[1],
            native_message_id=event["event_id"],
            author_account_id=event["account_id"],
            message_revision=event["revision"],
        )
        try:
            wire.native(result)
        except ValueError:
            return None
        return result

    def account(self, account):
        return self.db.execute(
            "SELECT revision,enabled,group_policy,last_poll FROM observation_accounts WHERE account_id=?",
            (account,),
        ).fetchone()

    @staticmethod
    def generation(account):
        return "observation-policy:" + str(account[0])

    def capture(self, event):
        native = self.native(event)
        if native is None or event["mentioned"] or event["content_state"] != "text":
            return
        row = self._row(native)
        if row is None or self._stored(row[0]) is not None:
            return
        account = self.account(event["self_id"])
        key = wire.digest(
            [
                native[k]
                for k in (
                    "instance_id",
                    "self_id",
                    "group_id",
                    "author_account_id",
                    "native_message_id",
                )
            ]
        )
        retired = self.db.execute(
            "SELECT 1 FROM participation_retired_events WHERE native_key=?", (key,)
        ).fetchone()
        state = (
            "processing"
            if not retired and self.claim_supported() and row[2] >= self.started_at
            else "ownership_unknown"
        )
        doc = dict(
            native=native,
            host_epoch=self.epoch,
            host_generation=self.generation(account),
            owner_revision=1,
            state=state,
            basis="event_processing" if state == "processing" else "no_completion_proof",
            observed_at=wire.utc(time.time()),
            claim=None,
            completion_evidence=None,
        )
        # A native identity, not a candidate/actor/connection, owns this unique slot.
        self.db.execute(
            "INSERT INTO participation_state VALUES(?,?,?,?,NULL)",
            (row[0], key, wire.canonical(doc), "{}"),
        )
        self.step(native, "capture", "captured")

    def _stored(self, event_id):
        row = self.db.execute(
            "SELECT document,receipts,deadline FROM participation_state WHERE event_id=?",
            (event_id,),
        ).fetchone()
        return (json.loads(row[0]), json.loads(row[1]), row[2]) if row else None

    def _save(self, event_id, doc, receipts=None):
        self.db.execute(
            "UPDATE participation_state SET document=? WHERE event_id=?",
            (wire.canonical(doc), event_id),
        )
        if receipts is not None:
            self.db.execute(
                "UPDATE participation_state SET receipts=? WHERE event_id=?",
                (wire.canonical(receipts), event_id),
            )

    def _transition(self, event_id, doc, state, evidence=None):
        if doc["state"] == state:
            return
        allowed = (
            doc["state"] == "processing"
            or (doc["state"] == "free" and state in {"claimed", "expired"})
            or (doc["state"] == "ownership_unknown" and state == "handled_elsewhere")
            or state == "expired"
        )
        if not allowed or doc["state"] == "expired":
            return
        doc.update(
            state=state,
            basis=dict(
                free="all_handlers_completed",
                handled_elsewhere="other_handler_entered",
                ownership_unknown="no_completion_proof",
                claimed="claim_cas",
                expired="deadline",
            )[state],
            owner_revision=doc["owner_revision"] + 1,
            observed_at=wire.utc(time.time()),
        )
        if state not in {"claimed", "expired"}:
            doc["completion_evidence"] = evidence
        self._save(event_id, doc)

    def step(self, native, name, outcome):
        row = self._row(native)
        stored = self._stored(row[0]) if row else None
        if stored is None:
            return
        doc, receipts, _ = stored
        if doc["host_epoch"] != self.epoch or doc["state"] != "processing":
            return
        receipt = dict(
            id="receipt:" + uuid.uuid4().hex, outcome=outcome, observed_at=wire.utc(time.time())
        )
        previous = receipts.get(name)
        if previous and previous["outcome"] != outcome:
            self._transition(row[0], doc, "ownership_unknown")
            return
        receipts[name] = previous or receipt
        self._save(row[0], doc, receipts)
        expected = {"capture": "captured", "explicit_reply": "not_triggered", "scope": "no_route"}
        if name in expected and outcome != expected[name]:
            self._transition(
                row[0], doc, "handled_elsewhere" if outcome == "claimed" else "ownership_unknown"
            )

    def finish(self, native, handler_digest, successful):
        row = self._row(native)
        stored = self._stored(row[0]) if row else None
        if stored is None:
            return
        doc, receipts, _ = stored
        if doc["state"] != "processing":
            return
        good = successful and self.claim_supported() and doc["host_epoch"] == self.epoch
        good = good and all(
            receipts.get(k, {}).get("outcome") == v
            for k, v in {
                "capture": "captured",
                "explicit_reply": "not_triggered",
                "scope": "no_route",
            }.items()
        )
        if not good or not self._permitted(doc):
            self._transition(row[0], doc, "ownership_unknown")
            return
        receipts["closure"] = dict(
            id="receipt:" + uuid.uuid4().hex, outcome="completed", observed_at=wire.utc(time.time())
        )
        evidence = dict(
            event_record_id=row[0],
            handler_set_digest=handler_digest,
            capture_receipt_id=receipts["capture"]["id"],
            explicit_reply_receipt_id=receipts["explicit_reply"]["id"],
            scope_receipt_id=receipts["scope"]["id"],
            closure_receipt_id=receipts["closure"]["id"],
            completed_at=receipts["closure"]["observed_at"],
        )
        self._save(row[0], doc, receipts)
        self._transition(row[0], doc, "free", evidence)

    def entered(self, native):
        row = self._row(native)
        stored = self._stored(row[0]) if row else None
        if stored:
            self._transition(row[0], stored[0], "handled_elsewhere")

    def _permitted(self, doc):
        account = self.account(doc["native"]["self_id"])
        if not account or not account[1] or time.time() - account[3] > 30:
            return False
        policy = json.loads(account[2])
        group = doc["native"]["group_id"]
        return bool(
            doc["host_epoch"] == self.epoch
            and doc["host_generation"] == self.generation(account)
            and policy["observe"]
            and (
                (policy["mode"] == "whitelist" and group in policy["list"])
                or (policy["mode"] == "blacklist" and group not in policy["list"])
            )
        )

    def _current(self, event_id):
        stored = self._stored(event_id)
        if stored is None:
            return None
        doc, _, deadline = stored
        if doc["host_epoch"] != self.epoch or (deadline is not None and deadline <= time.time()):
            self._transition(event_id, doc, "expired")
        return doc

    def _record(self, kind, identifier):
        row = self.db.execute(
            "SELECT event_id,digest,document FROM participation_records WHERE kind=? AND id=?",
            (kind, identifier),
        ).fetchone()
        return (row[0], row[1], json.loads(row[2])) if row else None

    def _put(self, kind, identifier, event_id, document):
        old = self._record(kind, identifier)
        if old:
            self.check(
                old[0] == event_id and old[1] == wire.digest(document), "idempotency_conflict"
            )
            return old[2]
        self.db.execute(
            "INSERT INTO participation_records VALUES(?,?,?,?,?)",
            (kind, identifier, event_id, wire.digest(document), wire.canonical(document)),
        )
        return document

    def _candidate(self, ref):
        record = self._record("candidate", ref["candidate_id"])
        self.check(record is not None, "not_found", 404)
        event_id, digest, candidate = record
        self.check(digest == ref["candidate_digest"] and candidate["actor_id"] == ref["actor_id"])
        self.check(self._row(candidate["native"]) is not None, "row_unavailable", 404)
        return event_id, candidate

    def _decision(self, event_id, candidate_ref, decision_ref):
        row = self._record("decision", candidate_ref["candidate_id"])
        self.check(row is not None and row[0] == event_id, "not_found", 404)
        decision = row[2]
        self.check(
            row[1] == decision_ref["decision_digest"]
            and decision["decision_id"] == decision_ref["decision_id"]
            and decision["revision"] == decision_ref["revision"]
            and decision["candidate"] == candidate_ref
        )
        return decision

    def _active(self, event_id, claim):
        doc = self._current(event_id)
        if (
            not doc
            or doc["state"] != "claimed"
            or doc["claim"] != claim
            or not self.claim_supported()
            or not self._permitted(doc)
        ):
            return False
        if wire.stamp(claim["expires_at"]) <= time.time():
            return False
        try:
            _, candidate = self._candidate(claim["candidate"])
            decision = self._decision(event_id, claim["candidate"], claim["decision"])
        except self.error:
            return False
        return (
            decision["outcome"] == "engage"
            and wire.stamp(candidate["expires_at"]) > time.time()
            and wire.stamp(decision["expires_at"]) > time.time()
        )

    def capabilities(self, request):
        wire.obj(request, dict(account_id=wire.qq))
        account = self.account(request["account_id"])
        return dict(
            available=bool(account and self.framework),
            framework=self.framework,
            host_epoch=self.epoch,
            host_generation=self.generation(account) if account else None,
            manifest_sha256=wire.MANIFEST_SHA256,
            claim_supported=bool(self.claim_supported()),
        )

    def ownership(self, request):
        wire.obj(request, dict(native=wire.native))
        row = self._row(request["native"])
        doc = self._current(row[0]) if row else None
        if doc:
            self.check(doc["native"] == request["native"])
        return dict(
            found=doc is not None,
            ownership=dict(schema_version=1, kind="host_ownership", ownership=doc) if doc else None,
        )

    def candidate(self, request):
        wire.document(request, "participation_candidate")
        row = self._row(request["native"])
        self.check(row is not None, "row_unavailable", 404)
        old = self._record("candidate", request["candidate_id"])
        if old:
            return {"candidate": self._put("candidate", request["candidate_id"], row[0], request)}
        state = self._current(row[0])
        event = json.loads(row[1])
        self.check(
            state is not None
            and state["native"] == request["native"]
            and state["host_epoch"] == request["host_epoch"] == self.epoch
        )
        self.check(
            request["source"]["source_digest"] == wire.digest(event)
            and request["source"]["scope_version"] == event["scope_revision"]
            and request["text"] == event["text"]
            and request["sent_at"] == event["sent_at"]
            and not event["mentioned"]
            and event["content_state"] == "text"
        )
        self.check(
            request["author_anchor"]["account_id"] == event["account_id"]
            and request["author_anchor"]["group_id"] == request["native"]["group_id"]
        )
        self.check(
            request["configuration"]["host_generation"] == state["host_generation"]
            and request["configuration"]["reply_policy_revision"] == event["scope_revision"]
        )
        expires = wire.stamp(request["expires_at"])
        self.check(wire.stamp(request["created_at"]) < expires and expires > time.time())
        deadline = self._stored(row[0])[2]
        self.check(deadline is None or expires <= deadline)
        self.db.execute(
            "UPDATE participation_state SET deadline=COALESCE(deadline,?) WHERE event_id=?",
            (expires, row[0]),
        )
        return {"candidate": self._put("candidate", request["candidate_id"], row[0], request)}

    def decision(self, request):
        wire.document(request, "semantic_participation_decision")
        event_id, candidate = self._candidate(request["candidate"])
        self.check(
            wire.stamp(request["decided_at"])
            < wire.stamp(request["expires_at"])
            <= wire.stamp(candidate["expires_at"])
        )
        key = request["candidate"]["candidate_id"]
        old = self._record("decision", key)
        if old:
            if old[1] == wire.digest(request):
                return {"decision": old[2]}
            self.check(request["revision"] > old[2]["revision"], "idempotency_conflict")
            self.db.execute(
                "DELETE FROM participation_records WHERE kind='decision' AND id=?", (key,)
            )
        return {"decision": self._put("decision", key, event_id, request)}

    def claim(self, request):
        wire.document(request, "participation_claim_request")
        old = self.db.execute(
            "SELECT event_id,digest,document FROM participation_receipts WHERE request_id=?",
            (request["request_id"],),
        ).fetchone()
        base = dict(
            schema_version=1,
            kind="participation_claim_result",
            request_id=request["request_id"],
            request_digest=wire.digest(request),
            candidate=request["candidate"],
            decision=request["decision"],
            receipt_status="unavailable",
            result="unavailable",
            reason="row_unavailable",
            claim=None,
            recorded_at=None,
            ownership=None,
            current_claim_state="unavailable",
            checked_at=wire.utc(time.time()),
        )
        if old and old[1] != base["request_digest"]:
            return dict(base, receipt_status="conflict", reason="idempotency_conflict")
        retired = self.db.execute(
            "SELECT digest FROM participation_retired_requests WHERE request_id=?",
            (request["request_id"],),
        ).fetchone()
        if retired:
            return (
                base
                if retired[0] == base["request_digest"]
                else dict(base, receipt_status="conflict", reason="idempotency_conflict")
            )
        row = self._row(request["native"])
        if row is None or self._stored(row[0]) is None:
            return base
        if old:
            self.check(old[0] == row[0])
            result = json.loads(old[2])
            doc = self._current(row[0])
            return dict(
                result,
                receipt_status="replayed",
                ownership=doc,
                checked_at=base["checked_at"],
                current_claim_state="active"
                if result["claim"] and self._active(row[0], result["claim"])
                else "inactive",
            )
        doc = self._current(row[0])
        reason = None
        try:
            event_id, candidate = self._candidate(request["candidate"])
            decision = self._decision(event_id, request["candidate"], request["decision"])
            self.check(event_id == row[0] and candidate["native"] == request["native"])
            self.check(decision["outcome"] == "engage")
            self.check(
                time.time()
                < wire.stamp(request["expires_at"])
                <= min(wire.stamp(candidate["expires_at"]), wire.stamp(decision["expires_at"]))
            )
        except self.error:
            reason = "not_permitted"
        if doc["state"] != "free":
            reason = (
                doc["state"]
                if doc["state"] in {"handled_elsewhere", "ownership_unknown", "expired"}
                else "not_free"
            )
        elif (
            doc["host_epoch"] != request["host_epoch"]
            or doc["host_generation"] != request["host_generation"]
        ):
            reason = "stale_generation"
        elif doc["owner_revision"] != request["expected_owner_revision"]:
            reason = "stale_revision"
        elif not self.claim_supported() or not self._permitted(doc):
            reason = "not_permitted"
        if reason is None:
            claim = dict(
                claim_id="claim:" + uuid.uuid4().hex,
                candidate=request["candidate"],
                decision=request["decision"],
                host_epoch=self.epoch,
                host_generation=doc["host_generation"],
                owner_revision=doc["owner_revision"] + 1,
                expires_at=request["expires_at"],
            )
            doc["claim"] = claim
            self._transition(row[0], doc, "claimed")
        result = dict(
            base,
            receipt_status="recorded",
            result="denied" if reason else "granted",
            reason=reason or "claim_granted",
            claim=None if reason else doc["claim"],
            recorded_at=base["checked_at"],
            ownership=doc,
            current_claim_state="inactive" if reason else "active",
        )
        self.db.execute(
            "INSERT INTO participation_receipts VALUES(?,?,?,?)",
            (request["request_id"], row[0], base["request_digest"], wire.canonical(result)),
        )
        return result

    def register(self, request):
        wire.document(request, "participation_admission_registration")
        event_id, candidate = self._candidate(request["candidate"])
        old = self._record("registration", request["registration_id"])
        if old:
            return {
                "registration": self._put(
                    "registration", request["registration_id"], event_id, request
                )
            }
        self.check(
            request["native"] == candidate["native"]
            and request["source"] == candidate["source"]
            and request["configuration"] == candidate["configuration"]
        )
        self.check(
            request["claim"]["candidate"] == request["candidate"]
            and request["claim"]["decision"] == request["decision"]
            and self._active(event_id, request["claim"])
        )
        self.check(
            request["admission"]["event_id"] == candidate["native"]["native_message_id"]
            and request["admission"]["account_id"] == candidate["native"]["author_account_id"]
        )
        self._anchor(candidate["author_anchor"], request["author_anchor"])
        self.check(
            wire.stamp(request["registered_at"])
            < wire.stamp(request["expires_at"])
            <= wire.stamp(request["claim"]["expires_at"])
        )
        prior = self.db.execute(
            "SELECT document FROM participation_records WHERE kind='registration' AND event_id=?",
            (event_id,),
        ).fetchall()
        self.check(not prior, "idempotency_conflict")
        return {
            "registration": self._put("registration", request["registration_id"], event_id, request)
        }

    def _anchor(self, earlier, later):
        self.check(all(earlier[k] == later[k] for k in ("account_id", "group_id")))
        for key in ("entry_id", "person_id", "conversation_id", "binding_version"):
            self.check(earlier[key] is None or earlier[key] == later[key])

    def bind(self, request):
        wire.document(request, "participation_delivery_binding")
        event_id, _ = self._candidate(request["candidate"])
        registration = self._record("registration", request["registration"]["registration_id"])
        self.check(
            registration is not None
            and registration[0] == event_id
            and registration[1] == request["registration"]["registration_digest"]
        )
        original = registration[2]
        self.check(
            all(
                request[k] == original[k]
                for k in ("candidate", "decision", "claim", "source", "configuration")
            )
        )
        self._anchor(original["author_anchor"], request["author_anchor"])
        delivery = request["delivery"]
        self.check(delivery["connection_id"] == original["admission"]["connection_id"])
        key = (delivery["connection_id"], delivery["reply_id"])
        old = self.db.execute(
            "SELECT attempt_id,event_id,document FROM participation_bindings WHERE connection_id=? AND reply_id=?",
            key,
        ).fetchone()
        if old:
            saved = json.loads(old[2])
            self.check(
                old[0] == delivery["attempt_id"] and old[1] == event_id, "idempotency_conflict"
            )
            self.check(
                {k: v for k, v in saved.items() if k != "source_confirmation"}
                == {k: v for k, v in request.items() if k != "source_confirmation"},
                "idempotency_conflict",
            )
            if saved["source_confirmation"] == request["source_confirmation"]:
                return {"binding": saved}
            self.check(
                saved["source_confirmation"] == "pending"
                and request["source_confirmation"] == "confirmed",
                "idempotency_conflict",
            )
        self.check(self._active(event_id, request["claim"]))
        self.check(
            wire.stamp(request["bound_at"])
            < wire.stamp(request["expires_at"])
            <= wire.stamp(original["expires_at"])
        )
        self.db.execute(
            "INSERT INTO participation_bindings VALUES(?,?,?,?,?) ON CONFLICT(connection_id,reply_id) DO UPDATE SET document=excluded.document",
            (*key, delivery["attempt_id"], event_id, wire.canonical(request)),
        )
        return {"binding": request}

    def authorize_send(self, connection, delivery, account_id):
        self.check(
            not self.db.execute(
                "SELECT 1 FROM participation_retired WHERE connection_id=? AND reply_id=?",
                (connection, delivery["reply_id"]),
            ).fetchone(),
            "forbidden",
            403,
        )
        row = self.db.execute(
            "SELECT attempt_id,event_id,document FROM participation_bindings WHERE connection_id=? AND reply_id=?",
            (connection, delivery["reply_id"]),
        ).fetchone()
        if row is None:
            return  # Ordinary, existing delivery protocol; producer never downgrades AL11.
        binding = json.loads(row[2])
        candidate_id, candidate = self._candidate(binding["candidate"])
        self.check(
            row[0] == delivery["attempt_id"]
            and candidate_id == row[1]
            and binding["delivery"]["turn_id"] == delivery["turn_id"],
            "idempotency_conflict",
        )
        self.check(
            delivery["conversation_id"] == "group:" + candidate["native"]["group_id"],
            "forbidden",
            403,
        )
        self.check(account_id == candidate["native"]["self_id"], "forbidden", 403)
        self.check(
            binding["source_confirmation"] == "confirmed"
            and wire.stamp(binding["expires_at"]) > time.time()
            and self._active(row[1], binding["claim"]),
            "forbidden",
            403,
        )

    async def operation(self, name, request):
        handlers = {
            "capabilities": self.capabilities,
            "ownership": self.ownership,
            "candidate": self.candidate,
            "decision": self.decision,
            "claim": self.claim,
            "register": self.register,
            "bind": self.bind,
        }
        handler = handlers.get(name)
        if handler is None:
            self.fail("not_found", 404)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            result = handler(request)
            self.db.execute("COMMIT")
            return result
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
