"""Closed AL11 DTO checks, pinned to the reviewed standalone adapter contract.

No workspace, network schema resolution, or optional SDK is needed by a release.
This validates shape only; Participation owns all durable/current authority checks.
"""

import hashlib
import json
import re
from datetime import datetime, timezone

MANIFEST_SHA256 = "347b60a1dde6967adf559597e5bd7dfaa948f5b268d158d04ea3e3ad78b7c826"
PROTOCOL = "tianshu.observation-participation/candidate-v1"


def canonical(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def require(value):
    if not value:
        raise ValueError("invalid participation document")


def text(value, maximum=128, pattern=None):
    require(isinstance(value, str) and 0 < len(value) <= maximum)
    if pattern:
        require(re.fullmatch(pattern, value) is not None)


def ident(value):
    text(value, pattern=r"[A-Za-z0-9][A-Za-z0-9_.:-]*")


def actor(value):
    text(value, pattern=r"actor:[A-Za-z0-9._:-]+")


def qq(value):
    text(value, 20, r"[1-9][0-9]{4,19}")


def hash_value(value):
    text(value, 64, r"[a-f0-9]{64}")


def revision(value):
    require(type(value) is int and value >= 1)


def stamp(value):
    text(
        value,
        64,
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])",
    )
    parsed = datetime.fromisoformat(
        value.replace("t", "T").replace("z", "+00:00").replace("Z", "+00:00")
    )
    require(parsed.tzinfo is not None and parsed.utcoffset() is not None)
    return parsed.timestamp()


def utc(value):
    return (
        datetime.fromtimestamp(value, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def literal(expected):
    def check(value):
        require(type(value) is type(expected) and value == expected)

    return check


def choice(*values):
    def check(value):
        require(isinstance(value, str) and value in values)

    return check


def nullable(check):
    return lambda value: None if value is None else check(value)


def obj(value, fields):
    require(isinstance(value, dict) and set(value) == set(fields))
    for name, check in fields.items():
        check(value[name])


def framework(value):
    obj(value, {"name": text, "version": text})
    require((value["name"], value["version"]) in {("nonebot2", "2.5.0"), ("astrbot", "4.27.3")})


def native(value):
    obj(
        value,
        dict(
            framework=framework,
            instance_id=ident,
            namespace=literal("qq"),
            self_id=qq,
            group_id=qq,
            native_message_id=lambda x: text(x, 256),
            author_account_id=qq,
            message_revision=revision,
        ),
    )


def source(value):
    obj(
        value,
        dict(
            source_ref=ident,
            source_digest=hash_value,
            scope_version=revision,
            archive_epoch=revision,
        ),
    )


def anchor(value):
    obj(
        value,
        dict(
            stage=choice("observed", "registered", "resolved"),
            account_id=qq,
            group_id=qq,
            entry_id=nullable(ident),
            person_id=nullable(ident),
            conversation_id=nullable(ident),
            binding_version=nullable(revision),
        ),
    )
    require((value["entry_id"] is None) == (value["stage"] == "observed"))
    for key in ("person_id", "conversation_id", "binding_version"):
        require((value[key] is not None) == (value["stage"] == "resolved"))


def configuration(value):
    obj(
        value,
        dict(
            platform_generation=ident,
            host_generation=ident,
            role_version=revision,
            reply_policy_revision=revision,
        ),
    )


def candidate_ref(value):
    obj(value, dict(candidate_id=ident, actor_id=actor, candidate_digest=hash_value))


def decision_ref(value):
    obj(value, dict(decision_id=ident, revision=revision, decision_digest=hash_value))


def claim_ref(value):
    obj(
        value,
        dict(
            claim_id=ident,
            candidate=candidate_ref,
            decision=decision_ref,
            host_epoch=ident,
            host_generation=ident,
            owner_revision=revision,
            expires_at=stamp,
        ),
    )


def evidence(value):
    obj(
        value,
        dict(
            event_record_id=ident,
            handler_set_digest=hash_value,
            capture_receipt_id=ident,
            explicit_reply_receipt_id=ident,
            scope_receipt_id=ident,
            closure_receipt_id=ident,
            completed_at=stamp,
        ),
    )


def ownership(value):
    obj(
        value,
        dict(
            native=native,
            host_epoch=ident,
            host_generation=ident,
            owner_revision=revision,
            state=choice(
                "processing", "free", "handled_elsewhere", "ownership_unknown", "claimed", "expired"
            ),
            basis=choice(
                "event_processing",
                "all_handlers_completed",
                "other_handler_entered",
                "no_completion_proof",
                "claim_cas",
                "deadline",
            ),
            observed_at=stamp,
            claim=nullable(claim_ref),
            completion_evidence=nullable(evidence),
        ),
    )
    state = value["state"]
    require(
        value["basis"]
        == dict(
            processing="event_processing",
            free="all_handlers_completed",
            handled_elsewhere="other_handler_entered",
            ownership_unknown="no_completion_proof",
            claimed="claim_cas",
            expired="deadline",
        )[state]
    )
    if state != "expired":
        require((value["claim"] is not None) == (state == "claimed"))
        require((value["completion_evidence"] is not None) == (state in {"free", "claimed"}))
    if value["native"]["framework"]["name"] == "astrbot":
        require(
            state not in {"free", "claimed"}
            and value["claim"] is None
            and value["completion_evidence"] is None
        )


def admission(value):
    fields = dict(
        connection_id=ident,
        event_id=lambda x: text(x, 256),
        account_id=qq,
        event_semantic_digest=hash_value,
        message_id=ident,
        revision=literal(1),
        input_entry_id=ident,
        input_digest=hash_value,
    )
    for name, prefix in (
        ("source_input_assertion_ref", "source-input"),
        ("actor_assertion_ref", "origin"),
        ("fanout_ticket", "fanout"),
    ):
        fields[name] = lambda x, prefix=prefix: text(x, 256, prefix + r":[A-Za-z0-9_.:-]+")
    fields["core_idempotency_key"] = lambda x: text(x, 68, r"bot:[a-f0-9]{64}")
    obj(value, fields)


def document(value, kind):
    """Check one of the seven exact candidate DTOs without adding a schema runtime."""
    require(len(canonical(value).encode("utf-8")) <= 65536)
    fields = dict(schema_version=literal(1), kind=literal(kind))
    if kind == "participation_candidate":
        fields.update(
            candidate_id=ident,
            actor_id=actor,
            trigger=literal("ordinary_group_message"),
            native=native,
            source=source,
            author_anchor=anchor,
            configuration=configuration,
            host_epoch=ident,
            sent_at=stamp,
            created_at=stamp,
            expires_at=stamp,
            mentioned=literal(False),
            text=lambda x: text(x, 8000),
        )
    elif kind == "host_ownership":
        fields.update(ownership=ownership)
    elif kind == "participation_claim_request":
        fields.update(
            request_id=ident,
            candidate=candidate_ref,
            decision=decision_ref,
            native=native,
            host_epoch=ident,
            host_generation=ident,
            expected_owner_revision=revision,
            expires_at=stamp,
        )
    elif kind == "participation_claim_result":
        fields.update(
            request_id=ident,
            request_digest=hash_value,
            candidate=candidate_ref,
            decision=decision_ref,
            receipt_status=choice("recorded", "replayed", "conflict", "unavailable"),
            result=choice("granted", "denied", "unavailable"),
            reason=choice(
                "claim_granted",
                "not_free",
                "handled_elsewhere",
                "ownership_unknown",
                "expired",
                "stale_revision",
                "stale_generation",
                "not_permitted",
                "row_unavailable",
                "idempotency_conflict",
            ),
            claim=nullable(claim_ref),
            recorded_at=nullable(stamp),
            ownership=nullable(ownership),
            current_claim_state=choice("active", "inactive", "unavailable"),
            checked_at=stamp,
        )
    elif kind == "semantic_participation_decision":
        fields.update(
            candidate=candidate_ref,
            decision_id=ident,
            revision=revision,
            outcome=choice("engage", "abstain", "defer"),
            reason=choice(
                "useful_contribution",
                "no_contribution",
                "busy",
                "not_integrated",
                "source_unavailable",
                "ownership_unavailable",
            ),
            contribution=nullable(lambda x: text(x, 240)),
            decided_at=stamp,
            expires_at=stamp,
        )
    elif kind == "participation_admission_registration":
        fields.update(
            registration_id=ident,
            candidate=candidate_ref,
            decision=decision_ref,
            claim=claim_ref,
            native=native,
            source=source,
            author_anchor=anchor,
            configuration=configuration,
            admission=admission,
            registered_at=stamp,
            expires_at=stamp,
        )
    elif kind == "participation_delivery_binding":
        fields.update(
            request_id=ident,
            candidate=candidate_ref,
            decision=decision_ref,
            claim=claim_ref,
            source=source,
            author_anchor=anchor,
            configuration=configuration,
            delivery=lambda x: obj(
                x, dict(connection_id=ident, turn_id=ident, reply_id=ident, attempt_id=ident)
            ),
            registration=lambda x: obj(
                x, dict(registration_id=ident, registration_digest=hash_value)
            ),
            bound_at=stamp,
            expires_at=stamp,
            source_confirmation=choice("pending", "confirmed"),
        )
    else:
        raise ValueError("unknown participation document")
    obj(value, fields)
    if kind == "semantic_participation_decision":
        engage = value["outcome"] == "engage"
        require(
            engage
            == (value["reason"] == "useful_contribution")
            == (value["contribution"] is not None)
        )
        require(value["reason"] != "not_integrated" or value["outcome"] == "abstain")
    elif kind == "participation_admission_registration":
        require(value["author_anchor"]["stage"] in {"registered", "resolved"})
    elif kind == "participation_delivery_binding":
        require(value["author_anchor"]["stage"] == "resolved")
    elif kind == "participation_claim_result":
        result, status = value["result"], value["receipt_status"]
        if result == "unavailable":
            require(
                value["reason"]
                == {"conflict": "idempotency_conflict", "unavailable": "row_unavailable"}.get(
                    status
                )
            )
            require(all(value[k] is None for k in ("claim", "recorded_at", "ownership")))
            require(value["current_claim_state"] == "unavailable")
        else:
            require(status in {"recorded", "replayed"} and value["recorded_at"] is not None)
            require(
                value["current_claim_state"] in {"active", "inactive"}
                and value["ownership"] is not None
            )
            if result == "granted":
                require(value["reason"] == "claim_granted" and value["claim"] is not None)
            else:
                require(
                    value["reason"]
                    not in {"claim_granted", "row_unavailable", "idempotency_conflict"}
                    and value["claim"] is None
                )
                require(value["current_claim_state"] == "inactive")
            if value["current_claim_state"] == "active":
                require(result == "granted" and value["ownership"]["state"] == "claimed")
    return value
