"""Core-owned proactive contact: explicit subscriptions, goals, reminders, scheduling.

Trusted in-process port. No HTTP route, no new cross-product contract, no Memory
database access, no other product's database. The host authenticates the caller before
calling any mutation or admin method here; a model reply or an ordinary chat message can
never register a subscription, a goal or a reminder.

Scope of the schedule
---------------------
Only explicitly registered reminders and character goals become candidates. Nothing is
inferred from chat text, profile fields, health data or guessed real-world experience.
Every decision is one of: defer, suppress, expire, cancel or ready, decided by the
actor, the recipient, the conversation permission, the subscription timezone's quiet
window, cooldown, the daily quota, unanswered-contact suppression and the due time.

Every state change happens in one owner transaction, so a quota slot is checked and
consumed atomically with the submit intent, and a restarted process reconstructs every
pending decision from absolute UTC timestamps plus the registered timezone. Cross-midnight
windows and clock adjustments are therefore recomputed, never guessed from a stored local
time.

Delivery gap (recorded, not papered over)
-----------------------------------------
The published `text-dialogue/v1` release has exactly one outbound document,
`conversation#send_request`, and it is keyed by `turn_id`/`turn_sequence`/`reply_id` of a
turn that already exists. A turn only exists after a real inbound collection was sealed,
so Core cannot use that document for a contact the user never prompted without forging
inbound input or source proof - both forbidden by this task. This module therefore owns
the schedule, the candidate and the delivery attempt, and calls an injected `dispatcher`
port. With no dispatcher (today's production default) candidates stay `ready` and are
reported with an explicit contract gap; nothing is ever reported as delivered. A
dispatcher that declares `adapter="synthetic"` may settle an attempt as `sent`, and the
candidate still reports `delivered=False` with
`delivery_evidence="synthetic_adapter_receipt"`.
"""

import asyncio
import json
import re
from datetime import datetime, time, timedelta, timezone

from .clients import uid, utc
from .contracts import Fault, digest
from .life import expected_version, text, timestamp, zone

KINDS = {"reminder", "goal"}
SUBJECT_STATES = {"active", "paused", "completed", "cancelled", "expired"}
CANDIDATE_STATES = {
    "pending",
    "deferred",
    "suppressed",
    "ready",
    "sending",
    "sent",
    "failed",
    "unknown",
    "expired",
    "cancelled",
}
OPEN_CANDIDATE = {"pending", "deferred", "suppressed", "ready"}
SETTLED_CANDIDATE = {"sent", "failed", "unknown", "expired", "cancelled"}
# Explicit retry is the only path back to dispatch after one of these.
RETRYABLE_CANDIDATE = {"failed", "unknown"}
ATTEMPT_STATES = {"submitted", "sent", "failed", "unknown"}
# Only an adapter that speaks a published, per-product delivery contract may report a
# verified delivery. Anything else is recorded but never called delivered.
ADAPTERS = {"contract", "synthetic", "unknown"}
VERIFYING_ADAPTER = "contract"
CONTRACT_GAP = (
    "text-dialogue/v1 has no proactive/outbound-initiated send document: send_request is "
    "keyed by an existing turn_id/turn_sequence/reply_id and a turn only exists after a "
    "real inbound collection was sealed. Reusing it for an unprompted contact would "
    "require forging inbound input or source proof."
)
PLACEHOLDERS = ("summary", "due_time", "due_date", "timezone")
TEMPLATE_FIELD = re.compile(r"\{([a-z_]+)\}")
MINUTE = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]")


def civil_minute(value):
    """Parse an explicit 'HH:MM' civil minute; never a free-form time string."""
    if not isinstance(value, str) or MINUTE.fullmatch(value) is None:
        raise ValueError("Quiet window needs HH:MM civil minutes")
    return int(value[:2]) * 60 + int(value[3:])


def resolve(tz, day, minute):
    """First absolute instant at or after a civil minute, DST gaps included.

    A spring-forward gap has no such wall clock, so the first valid minute after the gap
    is returned. Fall-back repeats resolve with fold=0, i.e. the earlier occurrence, so
    the answer is deterministic and stable across restarts.
    """
    for extra in range(0, 181):
        naive = datetime.combine(day, time(0, 0)) + timedelta(minutes=minute + extra)
        candidate = naive.replace(tzinfo=tz, fold=0)
        if candidate.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) == naive:
            return candidate.timestamp()
    raise ValueError("Unresolvable civil minute in this timezone")


def in_quiet(minute, start, end):
    if start == end:
        return False  # An empty window is no window, never permanent silence.
    if start < end:
        return start <= minute < end
    return minute >= start or minute < end


def quiet_end(tz, local, start, end):
    """Absolute instant at which the current quiet window ends."""
    minute = local.hour * 60 + local.minute
    if start < end or minute < end:
        # A same-day window ends later today; a wrapping window that began yesterday
        # evening ends this morning.
        day = local.date()
    else:
        day = local.date() + timedelta(days=1)
    return resolve(tz, day, end)


def template_placeholders(body):
    names = sorted(set(TEMPLATE_FIELD.findall(body)))
    if "{" in TEMPLATE_FIELD.sub("", body) or "}" in TEMPLATE_FIELD.sub("", body):
        raise ValueError("Template placeholders must be simple {name} fields")
    if set(names) - set(PLACEHOLDERS):
        raise ValueError("Template uses an unknown placeholder")
    return names


def agent_key(actor_id, subscription_id):
    return digest([actor_id, subscription_id])


class Proactive:
    """Host must authorize callers BEFORE invoking any mutation or admin method here.

    Reader identities are authenticated by the host. `guard` is a host-supplied,
    synchronous authority re-check over Core's own local facts (channel binding,
    audience, actor role, conversation quarantine). It runs at registration and again
    inside the submit transaction, so a revoked subscription, channel or role stops a
    contact before any transport call. Remote Memory revocation that has not reached Core
    is a stated gap, not something this port claims to verify.
    """

    def __init__(
        self,
        store,
        clock,
        guard,
        *,
        dispatcher=None,
        timeout=20,
        max_subjects=200,
        max_candidates=500,
        max_history=32,
        max_pruned=64,
    ):
        if not callable(guard):
            raise ValueError("A host authority guard is required")
        self.store, self.clock, self.guard = store, clock, guard
        self.dispatcher = dispatcher
        for name, value, ceiling in (
            ("timeout", timeout, 600),
            ("max_subjects", max_subjects, 2000),
            ("max_candidates", max_candidates, 5000),
            ("max_history", max_history, 256),
            ("max_pruned", max_pruned, 512),
        ):
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError("Invalid proactive bound: " + name)
            setattr(self, name, value)
        self.lock = asyncio.Lock()
        self._next_tick = 0

    def _get(self, table, key):
        item = self.store.get("proactive_" + table, key)
        if item is None:
            raise KeyError(key)
        return item

    def _save(self, table, item, expected=None):
        old = self.store.get("proactive_" + table, item["id"])
        if expected is not None:
            expected_version(expected)
            if (old or {}).get("version") != expected:
                raise ValueError("Stale version")
        item["version"] = (old or {}).get("version", 0) + 1
        item["sequence"] = item["version"]
        item["updated_at"] = self.clock()
        self.store.put("proactive_" + table, item)
        return item

    def _subject(self, kind, subject_id):
        assert kind in KINDS
        return self.store.get(
            "proactive_" + ("goals" if kind == "goal" else "reminders"), subject_id
        )

    # ------------------------------------------------------------------ templates

    def put_template(self, template_id, version, *, body, max_chars=500):
        """Immutable versioned template. Prose comes from here, never from a model line."""
        text(template_id, 128)
        if type(version) is not int or version < 1:
            raise ValueError("Invalid template version")
        if type(max_chars) is not int or not 1 <= max_chars <= 4000:
            raise ValueError("Invalid template length")
        text(body, max_chars)
        names = template_placeholders(body)
        key = digest([template_id, version])
        prior = self.store.get("proactive_templates", key)
        fields = dict(
            template_id=template_id,
            template_version=version,
            body=body,
            max_chars=max_chars,
            placeholders=names,
        )
        if prior is not None:
            if any(prior[name] != value for name, value in fields.items()):
                raise ValueError("Template version is immutable")
            return key
        self.store.put(
            "proactive_templates",
            dict(
                id=key,
                conversation_id=template_id,
                state="registered",
                deadline=None,
                version=1,
                sequence=1,
                **fields,
            ),
        )
        return key

    def _template(self, template_id, version):
        item = self.store.get("proactive_templates", digest([template_id, version]))
        if item is None:
            # Never fall back to another template version or to a chat model line.
            raise KeyError("template:" + str(template_id))
        return item

    def _render(self, template, values):
        if any(name not in values for name in template["placeholders"]):
            raise ValueError("Template placeholder unavailable")
        rendered = template["body"].format_map({name: values[name] for name in PLACEHOLDERS})
        text(rendered, template["max_chars"])
        return rendered

    def _values(self, subscription, summary, due_at):
        local = datetime.fromtimestamp(due_at, zone(subscription["timezone"]))
        return dict(
            summary=summary,
            due_time=local.strftime("%H:%M"),
            due_date=str(local.date()),
            timezone=subscription["timezone"],
        )

    def _check_render(self, subscription, summary, due_at, template):
        """Fail at registration rather than repeatedly at schedule time."""
        return self._render(template, self._values(subscription, summary, due_at))

    # -------------------------------------------------------------- subscriptions

    @staticmethod
    def subscription_id(actor_id, person_id, audience, conversation_id):
        return digest([actor_id, person_id, audience, conversation_id])

    def register_subscription(
        self,
        actor_id,
        *,
        person_id,
        audience,
        conversation_id,
        channel,
        consent,
        timezone_name="UTC",
        quiet=("22:00", "08:00"),
        cooldown_seconds=3600,
        daily_limit=2,
        unanswered_limit=2,
        expiry_seconds=21600,
        expected=None,
    ):
        """Explicit opt-in for one (actor, person, audience, conversation) destination.

        Proactive contact is off by default: without this call no candidate for this
        destination can ever reach `ready`. `consent` must name who registered it and on
        what explicit basis; chat text or a model statement is never consent.
        """
        text(actor_id, 128)
        text(person_id, 128)
        text(conversation_id, 128)
        if audience not in {"self_private", "group"}:
            raise ValueError("Unsupported audience")
        if not isinstance(channel, dict) or set(channel) != {
            "namespace",
            "binding_id",
            "channel_conversation_id",
            "thread_id",
        }:
            raise ValueError("A full channel key is required")
        if not isinstance(consent, dict) or set(consent) != {
            "registered_by",
            "basis",
            "evidence_ref",
        }:
            raise ValueError("Explicit consent record required")
        if consent["basis"] not in {"explicit_user_request", "explicit_admin_registration"}:
            raise ValueError("Unsupported consent basis")
        for name in ("registered_by", "evidence_ref"):
            text(consent[name], 256)
        if not isinstance(quiet, (list, tuple)) or len(quiet) != 2:
            raise ValueError("Quiet window needs a start and an end minute")
        zone(timezone_name)
        start, end = civil_minute(quiet[0]), civil_minute(quiet[1])
        for name, value, low, high in (
            ("cooldown_seconds", cooldown_seconds, 0, 86400),
            ("daily_limit", daily_limit, 1, 24),
            ("unanswered_limit", unanswered_limit, 1, 24),
            ("expiry_seconds", expiry_seconds, 60, 2592000),
        ):
            if type(value) is not int or not low <= value <= high:
                raise ValueError("Invalid proactive bound: " + name)
        key = self.subscription_id(actor_id, person_id, audience, conversation_id)
        old = self.store.get("proactive_subscriptions", key)
        if old and expected is None:
            raise ValueError("Expected version required")
        item = dict(
            id=key,
            conversation_id=conversation_id,
            state="active",
            deadline=None,
            actor_id=actor_id,
            person_id=person_id,
            audience=audience,
            channel=channel,
            timezone=timezone_name,
            quiet=dict(start=start, end=end, start_civil=quiet[0], end_civil=quiet[1]),
            cooldown_seconds=cooldown_seconds,
            daily_limit=daily_limit,
            unanswered_limit=unanswered_limit,
            expiry_seconds=expiry_seconds,
            # Only a real configuration change moves config_version. Contact accounting
            # and state transitions must not invalidate already scheduled candidates.
            config_version=(old or {}).get("config_version", 0) + 1,
            last_contact_at=(old or {}).get("last_contact_at"),
            unanswered_count=(old or {}).get("unanswered_count", 0),
            consent=consent,
            registered_at=(old or {}).get("registered_at", self.clock()),
            revoked_at=None,
            revocation_reason=None,
        )
        verdict = self.guard(item)
        if not verdict.get("allowed"):
            raise PermissionError(
                "Proactive destination not authorized: " + str(verdict.get("reason"))
            )
        self._save("subscriptions", item, expected)
        self.tick(force=True)
        return self.subscription_view(key)

    def revoke_subscription(self, subscription_id, *, reason, expected=None):
        """Withdraw permission. An already submitted transport call cannot be recalled."""
        text(reason, 128)
        item = self._get("subscriptions", subscription_id)
        if item["state"] == "revoked":
            return self.subscription_view(subscription_id)
        if expected is not None and item["version"] != expected:
            raise ValueError("Stale version")
        item.update(state="revoked", revoked_at=self.clock(), revocation_reason=reason)
        self._save("subscriptions", item)
        self.tick(force=True)
        return self.subscription_view(subscription_id)

    def pause_subscription(self, subscription_id, *, expected):
        item = self._get("subscriptions", subscription_id)
        if item["state"] != "active":
            raise ValueError("Subscription is not active")
        item["state"] = "paused"
        self._save("subscriptions", item, expected)
        self.tick(force=True)
        return self.subscription_view(subscription_id)

    def resume_subscription(self, subscription_id, *, expected):
        item = self._get("subscriptions", subscription_id)
        if item["state"] != "paused":
            raise ValueError("Subscription is not paused")
        if not self.guard(item).get("allowed"):
            raise PermissionError("Proactive destination not authorized")
        item["state"] = "active"
        self._save("subscriptions", item, expected)
        self.tick(force=True)
        return self.subscription_view(subscription_id)

    def subscription_view(self, subscription_id):
        item = self._get("subscriptions", subscription_id)
        return {
            key: item[key]
            for key in (
                "id",
                "actor_id",
                "person_id",
                "audience",
                "conversation_id",
                "channel",
                "timezone",
                "quiet",
                "cooldown_seconds",
                "daily_limit",
                "unanswered_limit",
                "expiry_seconds",
                "config_version",
                "state",
                "consent",
                "last_contact_at",
                "unanswered_count",
                "revoked_at",
                "revocation_reason",
                "version",
            )
        }

    def subscriptions(self, actor_id):
        return [
            self.subscription_view(item["id"])
            for item in self.store.list(
                "proactive_subscriptions", states=["active", "paused", "revoked"]
            )
            if item["actor_id"] == actor_id
        ]

    # ------------------------------------------------------------- goals/reminders

    def register_goal(
        self,
        goal_id,
        *,
        actor_id,
        subscription_id,
        summary,
        due_at,
        template_id,
        template_version,
        interval_seconds=None,
        expected=None,
    ):
        """A character's own persistent goal. Explicit registration only."""
        text(goal_id, 128)
        text(summary, 1000)
        timestamp(due_at)
        subscription = self._get("subscriptions", subscription_id)
        if subscription["actor_id"] != actor_id:
            raise ValueError("Goal actor does not own this subscription")
        if interval_seconds is not None and (
            type(interval_seconds) is not int or not 60 <= interval_seconds <= 2592000
        ):
            raise ValueError("Invalid goal interval")
        self._check_render(
            subscription, summary, due_at, self._template(template_id, template_version)
        )
        old = self.store.get("proactive_goals", goal_id)
        if old and expected is None:
            raise ValueError("Expected version required")
        if old and old["actor_id"] != actor_id:
            raise ValueError("Goal id reused by another actor")
        item = dict(
            id=goal_id,
            conversation_id=subscription_id,
            state="active",
            deadline=due_at,
            actor_id=actor_id,
            subscription_id=subscription_id,
            summary=summary,
            template_id=template_id,
            template_version=template_version,
            interval_seconds=interval_seconds,
            next_due_at=due_at,
            completed_at=None,
            cancelled_at=None,
            cancellation_reason=None,
            created_at=(old or {}).get("created_at", self.clock()),
            fictional=True,
        )
        self._save("goals", item, expected)
        self.tick(force=True)
        return self.goal_metadata(goal_id)

    def register_reminder(
        self,
        reminder_id,
        *,
        actor_id,
        subscription_id,
        summary,
        due_at,
        template_id,
        template_version,
        registered_by,
        evidence_ref,
        basis="explicit_registration",
        expected=None,
    ):
        """One-shot reminder. The only accepted basis is an explicit registration."""
        if basis != "explicit_registration":
            raise ValueError("Only explicit registration creates a reminder")
        text(registered_by, 256)
        text(evidence_ref, 256)
        text(reminder_id, 128)
        text(summary, 1000)
        timestamp(due_at)
        subscription = self._get("subscriptions", subscription_id)
        if subscription["actor_id"] != actor_id:
            raise ValueError("Reminder actor does not own this subscription")
        self._check_render(
            subscription, summary, due_at, self._template(template_id, template_version)
        )
        old = self.store.get("proactive_reminders", reminder_id)
        if old and expected is None:
            raise ValueError("Expected version required")
        item = dict(
            id=reminder_id,
            conversation_id=subscription_id,
            state="active",
            deadline=due_at,
            actor_id=actor_id,
            subscription_id=subscription_id,
            summary=summary,
            template_id=template_id,
            template_version=template_version,
            due_at=due_at,
            basis=basis,
            registered_by=registered_by,
            evidence_ref=evidence_ref,
            completed_at=None,
            cancelled_at=None,
            cancellation_reason=None,
            created_at=(old or {}).get("created_at", self.clock()),
        )
        self._save("reminders", item, expected)
        self.tick(force=True)
        return self.reminder_metadata(reminder_id)

    def _save_subject(self, kind, item, expected=None):
        return self._save("goals" if kind == "goal" else "reminders", item, expected)

    def cancel_subject(self, kind, subject_id, *, reason, expected=None):
        """Stop a goal or reminder. Pending candidates for it are cancelled by the tick."""
        text(reason, 128)
        item = self._require_subject(kind, subject_id)
        if item["state"] in {"completed", "cancelled"}:
            return self.subject_metadata(kind, subject_id)
        if expected is not None and item["version"] != expected:
            raise ValueError("Stale version")
        item.update(state="cancelled", cancelled_at=self.clock(), cancellation_reason=reason)
        self._save_subject(kind, item)
        self.tick(force=True)
        return self.subject_metadata(kind, subject_id)

    def pause_subject(self, kind, subject_id, *, expected):
        item = self._require_subject(kind, subject_id)
        if item["state"] != "active":
            raise ValueError("Subject is not active")
        item["state"] = "paused"
        self._save_subject(kind, item, expected)
        self.tick(force=True)
        return self.subject_metadata(kind, subject_id)

    def resume_subject(self, kind, subject_id, *, expected):
        item = self._require_subject(kind, subject_id)
        if item["state"] != "paused":
            raise ValueError("Subject is not paused")
        item["state"] = "active"
        if kind == "goal":
            # A resumed goal continues from now; a missed month is never replayed.
            item["next_due_at"] = max(item["next_due_at"], self.clock())
            item["deadline"] = item["next_due_at"]
        self._save_subject(kind, item, expected)
        self.tick(force=True)
        return self.subject_metadata(kind, subject_id)

    def _require_subject(self, kind, subject_id):
        assert kind in KINDS
        item = self._subject(kind, subject_id)
        if item is None:
            raise KeyError(subject_id)
        return item

    def subject_metadata(self, kind, subject_id):
        item = self._require_subject(kind, subject_id)
        keys = (
            "id",
            "actor_id",
            "subscription_id",
            "summary",
            "state",
            "version",
            "next_due_at",
            "interval_seconds",
            "due_at",
            "template_id",
            "template_version",
            "basis",
            "created_at",
            "completed_at",
            "cancelled_at",
        )
        return {key: item[key] for key in keys if key in item}

    def goal_metadata(self, goal_id):
        return self.subject_metadata("goal", goal_id)

    def reminder_metadata(self, reminder_id):
        return self.subject_metadata("reminder", reminder_id)

    def subjects(self, actor_id):
        return [
            self.subject_metadata("goal", item["id"])
            for item in self.store.list("proactive_goals")
            if item["actor_id"] == actor_id
        ] + [
            self.subject_metadata("reminder", item["id"])
            for item in self.store.list("proactive_reminders")
            if item["actor_id"] == actor_id
        ]

    # ------------------------------------------------------------------ scheduling

    def _local(self, subscription, now):
        return datetime.fromtimestamp(now, zone(subscription["timezone"]))

    def _local_date(self, subscription, now):
        return str(self._local(subscription, now).date())

    def _quota_used(self, subscription, now):
        item = self.store.get(
            "proactive_quota", digest([subscription["id"], self._local_date(subscription, now)])
        )
        return item["count"] if item else 0

    def _consume_quota(self, subscription, now):
        local_date = self._local_date(subscription, now)
        key = digest([subscription["id"], local_date])
        item = self.store.get("proactive_quota", key)
        if item is None:
            self.store.put(
                "proactive_quota",
                dict(
                    id=key,
                    conversation_id=subscription["id"],
                    state=local_date,
                    deadline=None,
                    subscription_id=subscription["id"],
                    local_date=local_date,
                    timezone=subscription["timezone"],
                    count=0,
                    version=1,
                    sequence=1,
                ),
            )
            item = self.store.get("proactive_quota", key)
        item["count"] = item["count"] + 1
        self._save("quota", item)
        return item["count"]

    def last_inbound(self, subscription):
        """Newest real inbound instant for this conversation and recipient person.

        Read from Core's own accepted collections. A proactive contact is written to
        `proactive_*` only, so it can never be mistaken for user input here.
        """
        row = self.store.db.execute(
            "SELECT MAX(json_extract(body,'$.started')) FROM collections WHERE conversation_id=? "
            "AND json_extract(body,'$.scope.person_id')=?",
            (subscription["conversation_id"], subscription["person_id"]),
        ).fetchone()
        return row[0] if row and row[0] is not None else None

    def _unanswered(self, subscription, now):
        inbound = self.last_inbound(subscription)
        last = subscription["last_contact_at"]
        if inbound is not None and (last is None or inbound > last):
            if subscription["unanswered_count"]:
                subscription["unanswered_count"] = 0
                self._save("subscriptions", subscription)
        return subscription["unanswered_count"]

    def _gate(self, candidate, subscription, subject, now):
        """Decide one candidate: (state, defer_until, reason).

        Called only inside the owner transaction, and always against values re-read in
        that transaction, so quota and cooldown accounting cannot be raced.
        """
        if subscription["state"] != "active":
            return "cancelled", None, "subscription_" + subscription["state"]
        if subscription["config_version"] != candidate["subscription_version"]:
            return "cancelled", None, "subscription_changed"
        if subject["state"] != "active":
            return "cancelled", None, "subject_" + subject["state"]
        if subject["version"] != candidate["subject_version"]:
            return "cancelled", None, "subject_changed"
        if now >= candidate["expires_at"]:
            return "expired", None, "due_window_passed"
        local = self._local(subscription, now)
        minute = local.hour * 60 + local.minute
        quiet = subscription["quiet"]
        if in_quiet(minute, quiet["start"], quiet["end"]):
            until = quiet_end(zone(subscription["timezone"]), local, quiet["start"], quiet["end"])
            if until >= candidate["expires_at"]:
                return "expired", None, "quiet_window_passed_expiry"
            return "deferred", until, "quiet_hours"
        last = subscription["last_contact_at"]
        if last is not None and now - last < subscription["cooldown_seconds"]:
            until = last + subscription["cooldown_seconds"]
            if until >= candidate["expires_at"]:
                return "expired", None, "cooldown_passed_expiry"
            return "deferred", until, "cooldown"
        if self._unanswered(subscription, now) >= subscription["unanswered_limit"]:
            return "suppressed", None, "unanswered_limit"
        if self._quota_used(subscription, now) >= subscription["daily_limit"]:
            until = resolve(zone(subscription["timezone"]), local.date() + timedelta(days=1), 0)
            if until >= candidate["expires_at"]:
                return "expired", None, "quota_passed_expiry"
            return "suppressed", until, "daily_quota"
        return "ready", None, "due"

    def _apply(self, candidate, state, until, reason, now):
        candidate.update(
            state=state,
            decision=reason,
            defer_until=until,
            closed_at=now if state in SETTLED_CANDIDATE else None,
        )
        self._save("candidates", candidate)
        if state == "expired":
            # A recurring goal moves on to its next future occurrence; a one-shot stays
            # visibly expired and is never silently re-armed.
            self._advance(candidate, skipped=True)
        return state, reason

    def _materialize(self, now):
        """Create one durable candidate per due occurrence; never a duplicate one."""
        for table, kind in (("proactive_goals", "goal"), ("proactive_reminders", "reminder")):
            rows = self.store.db.execute(
                f"SELECT id FROM {table} WHERE status='active' AND deadline<=? "
                "ORDER BY deadline,id LIMIT ?",
                (now, self.max_subjects),
            ).fetchall()
            for row in rows:
                subject = self._subject(kind, row[0])
                if subject is None:
                    continue
                occurrence = subject["next_due_at"] if kind == "goal" else subject["due_at"]
                self._candidate(kind, subject, occurrence, now)

    def _candidate(self, kind, subject, occurrence, now):
        subscription = self.store.get("proactive_subscriptions", subject["subscription_id"])
        if subscription is None:
            return None
        seen = self.store.db.execute(
            "SELECT 1 FROM proactive_candidates WHERE "
            "json_extract(body,'$.kind')=? AND json_extract(body,'$.subject_id')=? "
            "AND json_extract(body,'$.occurrence')=? LIMIT 1",
            (kind, subject["id"], occurrence),
        ).fetchone()
        if seen is not None:
            # An occurrence is dispatched at most once; only explicit retry reopens one.
            return None
        template = self._template(subject["template_id"], subject["template_version"])
        values = self._values(subscription, subject["summary"], occurrence)
        content = self._render(template, values)
        content_version = digest(
            [template["template_id"], template["template_version"], values, content]
        )
        candidate_id = digest(
            [kind, subject["id"], occurrence, subject["version"], subscription["config_version"]]
        )
        if self.store.get("proactive_candidates", candidate_id) is not None:
            return None
        self.store.put(
            "proactive_candidates",
            dict(
                id=candidate_id,
                conversation_id=subscription["conversation_id"],
                state="pending",
                deadline=occurrence,
                kind=kind,
                subject_id=subject["id"],
                subject_version=subject["version"],
                occurrence=occurrence,
                subscription_id=subscription["id"],
                subscription_version=subscription["config_version"],
                actor_id=subscription["actor_id"],
                person_id=subscription["person_id"],
                audience=subscription["audience"],
                channel=subscription["channel"],
                due_at=occurrence,
                expires_at=occurrence + subscription["expiry_seconds"],
                defer_until=None,
                decision=None,
                template_id=template["template_id"],
                template_version=template["template_version"],
                content=content,
                content_version=content_version,
                attempt=None,
                attempt_count=0,
                receipt=None,
                delivery_evidence=None,
                delivered=False,
                unresolved=False,
                contract_gap=None,
                retry_reason=None,
                created_at=now,
                closed_at=None,
                fictional=True,
                real_user_sources="excluded",
                version=1,
                sequence=1,
            ),
        )
        self._prune(kind, subject["id"])
        return candidate_id

    def _prune(self, kind, subject_id):
        rows = self.store.db.execute(
            "SELECT id,status FROM proactive_candidates WHERE "
            "json_extract(body,'$.kind')=? AND json_extract(body,'$.subject_id')=? "
            "ORDER BY json_extract(body,'$.version') DESC,id LIMIT ?",
            (kind, subject_id, self.max_history + self.max_pruned + 1),
        ).fetchall()
        terminal = [row[0] for row in rows if row[1] in SETTLED_CANDIDATE]
        for key in terminal[self.max_history : self.max_history + self.max_pruned]:
            self.store.delete("proactive_candidates", key)

    def tick(self, *, force=False):
        """Recompute defer/suppress/expire/cancel/ready. No I/O and no transport call."""
        now = timestamp(self.clock())
        if not force and now < self._next_tick:
            return
        with self.store.transaction():
            self._materialize(now)
            rows = self.store.db.execute(
                "SELECT id FROM proactive_candidates WHERE status IN "
                "('pending','deferred','suppressed','ready') "
                "ORDER BY deadline,position,id LIMIT ?",
                (self.max_candidates,),
            ).fetchall()
            for row in rows:
                candidate = self.store.get("proactive_candidates", row[0])
                if candidate is None or candidate["state"] not in OPEN_CANDIDATE:
                    continue
                subscription = self.store.get(
                    "proactive_subscriptions", candidate["subscription_id"]
                )
                subject = self._subject(candidate["kind"], candidate["subject_id"])
                if subscription is None or subject is None:
                    self._apply(candidate, "cancelled", None, "registration_missing", now)
                    continue
                state, until, reason = self._gate(candidate, subscription, subject, now)
                if (
                    candidate["state"] == state
                    and candidate["defer_until"] == until
                    and candidate["decision"] == reason
                ):
                    continue
                self._apply(candidate, state, until, reason, now)
        self._next_tick = now + 1

    # ------------------------------------------------------------------- delivery

    def _dispatcher_available(self):
        return self.dispatcher is not None and bool(getattr(self.dispatcher, "available", False))

    def _check_result(self, request, result):
        if not isinstance(result, dict):
            raise Fault("invalid_input")
        if (
            result.get("request_id") != request["request_id"]
            or result.get("attempt_id") != request["attempt_id"]
            or result.get("state") not in {"sent", "failed", "unknown"}
            or result.get("adapter") not in ADAPTERS
        ):
            raise Fault("invalid_input")
        ids = result.get("channel_message_ids")
        if not isinstance(ids, list) or any(not isinstance(x, str) or not x for x in ids):
            raise Fault("invalid_input")
        if result["state"] == "sent" and not ids:
            raise Fault("invalid_input")
        if result["state"] != "sent" and ids:
            raise Fault("invalid_input")

    @staticmethod
    def _verified(adapter, state):
        return state == "sent" and adapter == VERIFYING_ADAPTER

    @staticmethod
    def _evidence(adapter, state):
        if state != "sent":
            return None
        if adapter == VERIFYING_ADAPTER:
            return "published_contract_receipt"
        return "synthetic_adapter_receipt"

    async def work(self):
        """One bounded dispatch per pass, off the chat path and without model slots."""
        if self.lock.locked():
            return
        async with self.lock:
            self.tick(force=True)
            if not self._dispatcher_available():
                return
            row = self.store.db.execute(
                "SELECT id FROM proactive_candidates WHERE status='ready' "
                "ORDER BY deadline,position,id LIMIT 1"
            ).fetchone()
            if row is None:
                return
            await self.dispatch(row[0])

    async def dispatch(self, candidate_id):
        """Submit one ready candidate. Does nothing without an available dispatcher."""
        now = timestamp(self.clock())
        if not self._dispatcher_available():
            return self.candidate_view(candidate_id)
        with self.store.transaction():
            candidate = self.store.get("proactive_candidates", candidate_id)
            if candidate is None or candidate["state"] != "ready":
                return self.candidate_view(candidate_id)
            version = candidate["version"]
            subscription = self.store.get("proactive_subscriptions", candidate["subscription_id"])
            subject = self._subject(candidate["kind"], candidate["subject_id"])
            if subscription is None or subject is None:
                self._apply(candidate, "cancelled", None, "registration_missing", now)
                return self.candidate_view(candidate_id)
            # Source, authorization, every gate and the candidate version are re-read in
            # the same transaction that records the submit intent and the quota slot.
            state, until, reason = self._gate(candidate, subscription, subject, now)
            if state != "ready":
                self._apply(candidate, state, until, reason, now)
                return self.candidate_view(candidate_id)
            if candidate["version"] != version:
                self._apply(candidate, "cancelled", None, "candidate_changed", now)
                return self.candidate_view(candidate_id)
            verdict = self.guard(subscription)
            if not verdict.get("allowed"):
                self._apply(
                    candidate,
                    "cancelled",
                    None,
                    "authorization_revoked:" + str(verdict.get("reason")),
                    now,
                )
                return self.candidate_view(candidate_id)
            attempt_no = candidate["attempt_count"] + 1
            request_id = uid("proactive")
            attempt_id = uid("attempt")
            request = dict(
                schema_version=1,
                request_id=request_id,
                attempt_id=attempt_id,
                candidate_id=candidate["id"],
                candidate_version=version,
                kind=candidate["kind"],
                subject_id=candidate["subject_id"],
                actor_id=candidate["actor_id"],
                person_id=candidate["person_id"],
                audience=candidate["audience"],
                conversation_id=candidate["conversation_id"],
                destination=candidate["channel"],
                text=candidate["content"],
                content_version=candidate["content_version"],
                due_at=candidate["due_at"],
                submitted_at=utc(now),
            )
            self.store.put(
                "proactive_attempts",
                dict(
                    id=request_id,
                    conversation_id=candidate["conversation_id"],
                    state="submitted",
                    deadline=candidate["due_at"],
                    candidate_id=candidate["id"],
                    candidate_version=version,
                    attempt_no=attempt_no,
                    adapter=None,
                    request=request,
                    submitted=True,
                    submitted_at=now,
                    settled_at=None,
                    receipt=None,
                    response_received=False,
                    stale=False,
                    verified_delivery=False,
                    version=1,
                    sequence=attempt_no,
                ),
            )
            # The submit intent lands before the transport call: from here on a crash is
            # an unknown outcome and is never retried automatically.
            self._consume_quota(subscription, now)
            subscription.update(
                last_contact_at=now, unanswered_count=subscription["unanswered_count"] + 1
            )
            self._save("subscriptions", subscription)
            candidate.update(
                state="sending",
                attempt=request_id,
                attempt_count=attempt_no,
                decision="submitted",
                defer_until=None,
                closed_at=None,
            )
            self._save("candidates", candidate)
        try:
            result = await asyncio.wait_for(self.dispatcher.dispatch(request), timeout=self.timeout)
            self._check_result(request, result)
        except Exception:
            result = dict(
                adapter="unknown",
                request_id=request_id,
                attempt_id=attempt_id,
                state="unknown",
                channel_message_ids=[],
                observed_at=utc(self.clock()),
            )
        self.settle(request_id, result)
        # Refresh the other open candidates so their decision reflects this contact's
        # cooldown, quota and unanswered accounting.
        self.tick(force=True)
        return self.candidate_view(candidate_id)

    def settle(self, request_id, result):
        """Trusted transport callback, including a late one for an older attempt."""
        with self.store.transaction():
            attempt = self.store.get("proactive_attempts", request_id)
            if attempt is None:
                raise KeyError(request_id)
            self._check_result(attempt["request"], result)
            if attempt["state"] in {"sent", "failed"}:
                if attempt["receipt"] != result:
                    raise Fault("idempotency_conflict")
                return self.attempt_metadata(request_id)
            now = self.clock()
            candidate = self.store.get("proactive_candidates", attempt["candidate_id"])
            owns = candidate is not None and candidate.get("attempt") == attempt["id"]
            attempt.update(
                state=result["state"],
                adapter=result["adapter"],
                receipt=result,
                response_received=True,
                settled_at=now,
                stale=not owns,
                verified_delivery=self._verified(result["adapter"], result["state"]),
            )
            self._save("attempts", attempt)
            if not owns:
                # A late result for a superseded attempt settles on that attempt only: it
                # never rewrites the candidate or the newer attempt.
                return self.attempt_metadata(request_id)
            candidate.update(
                state=result["state"],
                receipt=result,
                delivered=self._verified(result["adapter"], result["state"]),
                delivery_evidence=self._evidence(result["adapter"], result["state"]),
                unresolved=result["state"] == "unknown",
                closed_at=now,
            )
            self._save("candidates", candidate)
            if result["state"] == "sent":
                self._advance(candidate)
        return self.candidate_view(attempt["candidate_id"])

    def _advance(self, candidate, *, skipped=False):
        """Advance the subject after a sent contact or a skipped occurrence.

        A failure never eats an occurrence, and a long outage skips missed slots instead
        of replaying them. A one-shot reminder or goal that expired is not re-armed.
        """
        kind = candidate["kind"]
        subject = self._subject(kind, candidate["subject_id"])
        if subject is None or subject["state"] != "active":
            return
        if kind == "goal" and subject["interval_seconds"] is not None:
            interval = subject["interval_seconds"]
            missed = int((self.clock() - candidate["occurrence"]) // interval) + 1
            next_due = candidate["occurrence"] + max(1, missed) * interval
            subject.update(next_due_at=next_due, deadline=next_due)
        elif skipped:
            return
        else:
            subject.update(state="completed", completed_at=self.clock())
        self._save_subject(kind, subject)

    def retry_candidate(self, candidate_id, *, reason, expected=None):
        """Explicit retry. A settled unknown outcome is never resent automatically."""
        text(reason, 256)
        candidate = self._get("candidates", candidate_id)
        if candidate["state"] not in RETRYABLE_CANDIDATE:
            raise ValueError("Candidate is not retryable")
        if expected is not None and candidate["version"] != expected:
            raise ValueError("Stale version")
        if self.clock() >= candidate["expires_at"]:
            self._apply(candidate, "expired", None, "due_window_passed", self.clock())
            raise ValueError("Candidate window has passed")
        candidate.update(
            state="pending",
            attempt=None,
            receipt=None,
            delivered=False,
            delivery_evidence=None,
            unresolved=False,
            retry_reason=reason,
            defer_until=None,
            decision="explicit_retry",
            closed_at=None,
        )
        self._save("candidates", candidate)
        self.tick(force=True)
        return self.candidate_view(candidate_id)

    def cancel_candidate(self, candidate_id, *, reason, expected=None):
        """Cancel before any transport submit. A submitted attempt settles itself."""
        text(reason, 256)
        candidate = self._get("candidates", candidate_id)
        if candidate["state"] == "sending":
            raise ValueError("Attempt already submitted; wait for its outcome")
        if candidate["state"] in SETTLED_CANDIDATE:
            raise ValueError("Candidate is already settled")
        if expected is not None and candidate["version"] != expected:
            raise ValueError("Stale version")
        self._apply(candidate, "cancelled", None, reason, self.clock())
        return self.candidate_view(candidate_id)

    # ---------------------------------------------------------------------- views

    def candidate_view(self, candidate_id):
        return self._view(self._get("candidates", candidate_id))

    def _view(self, item):
        view = {
            key: item[key]
            for key in (
                "id",
                "kind",
                "subject_id",
                "actor_id",
                "person_id",
                "audience",
                "conversation_id",
                "channel",
                "state",
                "decision",
                "due_at",
                "expires_at",
                "defer_until",
                "content",
                "content_version",
                "template_id",
                "template_version",
                "attempt",
                "attempt_count",
                "receipt",
                "delivered",
                "delivery_evidence",
                "unresolved",
                "retry_reason",
                "version",
                "fictional",
                "real_user_sources",
            )
        }
        view["dispatch"] = dict(
            available=self._dispatcher_available(),
            contract_gap=None if self._dispatcher_available() else CONTRACT_GAP,
        )
        return view

    def candidates(self, *, actor_id=None, states=None, limit=64):
        """Trusted read port. The host authenticates the reader before calling this."""
        if type(limit) is not int or not 1 <= limit <= 512:
            raise ValueError("Invalid limit")
        rows = self.store.db.execute(
            "SELECT body FROM proactive_candidates "
            "ORDER BY json_extract(body,'$.due_at') DESC,"
            "json_extract(body,'$.version') DESC,id LIMIT ?",
            (limit,),
        ).fetchall()
        items = [json.loads(row[0]) for row in rows]
        return [
            self._view(item)
            for item in items
            if (actor_id is None or item["actor_id"] == actor_id)
            and (states is None or item["state"] in states)
        ]

    def attempt_metadata(self, request_id):
        item = self._get("attempts", request_id)
        return {
            key: item[key]
            for key in (
                "id",
                "candidate_id",
                "candidate_version",
                "attempt_no",
                "state",
                "adapter",
                "submitted",
                "submitted_at",
                "settled_at",
                "receipt",
                "response_received",
                "stale",
                "verified_delivery",
                "version",
            )
        }

    def attempts(self, candidate_id):
        items = [
            self.attempt_metadata(item["id"])
            for item in self.store.list("proactive_attempts", states=sorted(ATTEMPT_STATES))
            if item["candidate_id"] == candidate_id
        ]
        return sorted(items, key=lambda item: item["attempt_no"])

    # ------------------------------------------------------------------- recovery

    def recover(self):
        """Invoke after acquiring the database owner lock, before accepting traffic.

        A persisted submit intent with no stored result is an unknown outcome: it is
        recorded as unknown exactly once and never resent, because the transport may
        already have delivered it. Only an explicit retry reopens the candidate.
        """
        for attempt in self.store.list("proactive_attempts", states=["submitted"]):
            self.settle(
                attempt["id"],
                dict(
                    adapter="unknown",
                    request_id=attempt["id"],
                    attempt_id=attempt["request"]["attempt_id"],
                    state="unknown",
                    channel_message_ids=[],
                    observed_at=utc(self.clock()),
                ),
            )
        self.tick(force=True)
