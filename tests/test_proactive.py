"""Synthetic proactive-contact scheduling. Never a real message, channel or device.

Every identifier, person, reminder, goal and template here is a fixture. The dispatcher
double is a synthetic adapter: it can settle an attempt, but a candidate it "sent" is
still reported as not delivered, because no published per-product proactive send
contract exists yet.
"""

import asyncio
import copy
import sqlite3
import struct
import zoneinfo
from contextlib import closing
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from support import Clock, Harness
from tianshu_companion.clients import utc
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.proactive import CONTRACT_GAP, in_quiet, quiet_end, resolve
from tianshu_companion.store import PROACTIVE_TABLES, Store

# 2026-09-14 12:00 at +08:00, the same fixed civil instant used by the life chain tests.
START = datetime(2026, 9, 14, 4, 0, tzinfo=timezone.utc).timestamp()
HOUR = 3600


def at(*parts):
    return datetime(*parts, tzinfo=timezone.utc).timestamp()


class Adapter:
    """Synthetic dispatch adapter. A receipt here never proves a real delivery."""

    def __init__(self, clock, *, available=True, adapter="synthetic"):
        self.clock, self.available, self.kind = clock, available, adapter
        self.calls, self.answers, self.gate = [], [], None

    def receipt(self, request, state="sent"):
        return dict(
            adapter=self.kind,
            request_id=request["request_id"],
            attempt_id=request["attempt_id"],
            state=state,
            channel_message_ids=(["synthetic:" + request["request_id"]] if state == "sent" else []),
            observed_at=utc(self.clock()),
        )

    async def dispatch(self, request):
        self.calls.append(copy.deepcopy(request))
        if self.gate is not None:
            await self.gate.wait()
        state = self.answers.pop(0) if self.answers else "sent"
        return self.receipt(request, state)


class Fixture(Harness):
    """Harness whose Core owns the real proactive port and a synthetic dispatcher."""

    def __init__(self, path=":memory:", *, adapter=None, **policy):
        self.adapter = adapter
        super().__init__(path, **policy)

    def new_core(self):
        if self.adapter is None:
            self.adapter = Adapter(self.clock)
        self.options["proactive_dispatcher"] = self.adapter
        return super().new_core()


def channel_key(channel):
    return dict(
        namespace="qq", binding_id="qq-private", channel_conversation_id=channel, thread_id=None
    )


async def inbound(harness, *, channel="private:a", account="a", actor="actor:a", text="你好"):
    """One real accepted inbound; returns the Core-owned channel/conversation identity."""
    await harness.core.ingest(
        "nonebot", harness.request(text, channel=channel, account=account, actor=actor)
    )
    key = channel_key(channel)
    conv = harness.core.store.get("conversations", digest(key))
    collection = harness.core.store.list("collections", conv["conversation_id"])[0]
    return SimpleNamespace(
        key=key,
        conversation=conv["conversation_id"],
        person=collection["scope"]["person_id"],
        actor=actor,
    )


async def member(harness, *, channel="private:a", account="a", actor="actor:a", **overrides):
    """One real accepted inbound, then an explicit opt-in for that same destination."""
    target = await inbound(harness, channel=channel, account=account, actor=actor)
    options = dict(
        actor_id=actor,
        person_id=target.person,
        audience="self_private",
        conversation_id=target.conversation,
        channel=target.key,
        consent=dict(
            registered_by="admin:synthetic",
            basis="explicit_user_request",
            evidence_ref="synthetic:consent",
        ),
        timezone_name="+08:00",
        quiet=("22:00", "08:00"),
        cooldown_seconds=3600,
        daily_limit=2,
        unanswered_limit=2,
        expiry_seconds=86400,
    )
    options.update(overrides)
    target.subscription = harness.core.proactive.register_subscription(**options)
    return target


def template(harness, template_id="remind", version=1, body="该{summary}了，现在{due_time}。"):
    harness.core.proactive.put_template(template_id, version, body=body)
    return dict(template_id=template_id, template_version=version)


def reminder(harness, target, reminder_id, due_at, summary="喝水", **overrides):
    options = dict(
        actor_id=target.subscription["actor_id"],
        subscription_id=target.subscription["id"],
        summary=summary,
        due_at=due_at,
        registered_by="admin:synthetic",
        evidence_ref="synthetic:registration",
    )
    options.update(template(harness))
    options.update(overrides)
    return harness.core.proactive.register_reminder(reminder_id, **options)


def goal(harness, target, goal_id, due_at, summary="练习画画", **overrides):
    options = dict(
        actor_id=target.subscription["actor_id"],
        subscription_id=target.subscription["id"],
        summary=summary,
        due_at=due_at,
    )
    options.update(template(harness))
    options.update(overrides)
    return harness.core.proactive.register_goal(goal_id, **options)


def candidate(harness, kind="reminder", subject_id=None, state=None):
    items = harness.core.proactive.candidates(states=None if state is None else [state], limit=64)
    if subject_id is not None:
        items = [c for c in items if c["subject_id"] == subject_id]
    if kind is not None:
        items = [c for c in items if c["kind"] == kind]
    assert len(items) == 1, items
    return items[0]


def states(harness, kind=None):
    return {
        c["subject_id"]: c["state"]
        for c in harness.core.proactive.candidates(limit=512)
        if kind is None or c["kind"] == kind
    }


def count_materializations(harness):
    """Wrap the materialisation step so a tick's real scan work can be measured."""
    proactive = harness.core.proactive
    original = proactive._candidate
    calls = []

    def wrapper(kind, subject, occurrence, now):
        calls.append((kind, subject["id"]))
        return original(kind, subject, occurrence, now)

    proactive._candidate = wrapper
    return calls


def register_due_batch(harness, target, prefix, count, *, kind, due):
    """Register subjects that are not yet due, so registrations materialise nothing."""
    return [
        (reminder if kind == "reminder" else goal)(harness, target, f"{prefix}{index}", due + index)
        for index in range(count)
    ]


def test_expired_head_subject_does_not_starve_the_due_queue():
    """P1 regression: an expired one-shot kept the queue head and blocked every later
    due subject, for both scheduled kinds."""

    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            expiry_seconds=3600,
            daily_limit=8,
            unanswered_limit=8,
        )
        proactive = harness.core.proactive
        proactive.max_subjects = 1
        templates = template(harness)

        head = [
            reminder(harness, target, "head:reminder", START, **templates),
            goal(harness, target, "head:goal", START, summary="过期目标", **templates),
        ]
        harness.clock.now = START + 90000  # past both contact windows
        proactive.tick(force=True)
        assert set(states(harness).values()) == {"expired"}

        # Both kinds are due now and must still reach a candidate with one scan slot.
        late = [
            reminder(harness, target, "due:reminder", harness.clock.now, **templates),
            goal(harness, target, "due:goal", harness.clock.now, summary="新目标", **templates),
        ]
        for _ in range(3):
            proactive.tick(force=True)
        current = states(harness)
        for item in late:
            assert current[item["id"]] == "ready", current
        for item in head:
            assert current[item["id"]] == "expired", current
        # The expired one-shots stay visible and are never silently re-armed.
        assert proactive.reminder_metadata("head:reminder")["state"] == "active"
        assert proactive.reminder_metadata("head:reminder")["due_at"] == START
        await harness.core.close()

    asyncio.run(scenario())


def test_ready_and_pending_heads_do_not_consume_scan_budget():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=8,
            unanswered_limit=8,
        )
        proactive = harness.core.proactive
        proactive.max_subjects = 1
        templates = template(harness)

        # Two subjects already holding an open candidate sit ahead of everything else.
        held = register_due_batch(harness, target, "held", 2, kind="reminder", due=START + 1)
        harness.clock.now = START + 10
        proactive.tick(force=True)
        assert set(states(harness).values()) == {"ready"}
        later = reminder(harness, target, "later", harness.clock.now, **templates)
        proactive.tick(force=True)
        assert states(harness)[later["id"]] == "ready"
        assert [item["id"] for item in held] == ["held0", "held1"]
        await harness.core.close()

    asyncio.run(scenario())


def test_backlog_beyond_the_batch_limit_drains_in_bounded_ticks():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=8,
            unanswered_limit=8,
            expiry_seconds=86400 * 7,
        )
        proactive = harness.core.proactive
        template(harness)
        for kind in ("reminder", "goal"):
            register_due_batch(harness, target, kind + ":", 4, kind=kind, due=START + 3600)
        proactive.max_subjects = 3
        harness.clock.now = START + 7200  # every subject is due at once

        calls = count_materializations(harness)
        per_tick = []
        for _ in range(3):
            before = len(calls)
            proactive.tick(force=True)
            per_tick.append(len(calls) - before)
        # Bounded by max_subjects per kind, and the whole backlog drains in two ticks:
        # never an unbounded full scan and never an enlarged cap.
        assert per_tick == [6, 2, 0], per_tick
        assert len(states(harness, "reminder")) == 4
        assert len(states(harness, "goal")) == 4
        assert set(states(harness).values()) == {"ready"}
        assert proactive.max_subjects == 3
        await harness.core.close()

    asyncio.run(scenario())


def test_scan_rotation_is_bounded_and_survives_a_restart(tmp_path):
    path = str(tmp_path / "rotation.db")

    async def scenario():
        harness = Fixture(path)
        harness.clock.now = START
        ghost_target = await member(
            harness,
            channel="private:a",
            actor="actor:a",
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=8,
            unanswered_limit=8,
        )
        real_target = await member(
            harness,
            channel="private:b",
            account="b",
            actor="actor:b",
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=8,
            unanswered_limit=8,
        )
        proactive = harness.core.proactive
        proactive.max_subjects = 1
        templates = template(harness)
        ghost = reminder(harness, ghost_target, "ghost", START + 1, **templates)
        real = reminder(harness, real_target, "real", START + 2, **templates)
        harness.clock.now = START + 10

        # A registration whose subscription row is gone can never produce a candidate and
        # is not excluded by the occurrence filter, so only the rotating cursor can move
        # the scan past it. Without a durable cursor the later subject starves forever.
        harness.core.store.delete("proactive_subscriptions", ghost_target.subscription["id"])
        proactive.tick(force=True)
        assert states(harness) == {}
        first = harness.core.store.get("metadata", "proactive_scan_reminder")
        assert (first["subject_id"], first["deadline"]) == (ghost["id"], START + 1)
        await harness.core.close()

        restarted = Fixture(path)
        restarted.clock.now = harness.clock.now
        rotated = restarted.core.proactive
        rotated.max_subjects = 1  # the same configured bound as before the restart
        # The rotation state is durable: restarting does not send the scan back to the
        # same unproductive head.
        assert restarted.core.store.get("metadata", "proactive_scan_reminder") == first
        # Recovery itself continues the rotation and reaches the later subject.
        restarted.core.recover()
        assert states(restarted)[real["id"]] == "ready"
        assert (
            restarted.core.store.get("metadata", "proactive_scan_reminder")["subject_id"]
            == (real["id"])
        )
        await restarted.core.close()

    asyncio.run(scenario())


def test_scan_cursor_advances_to_the_globally_last_scanned_row():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=8,
            unanswered_limit=8,
        )
        proactive = harness.core.proactive
        proactive.max_subjects = 4
        templates = template(harness)
        # Not due while registering, so the rotation state can be seeded by hand.
        for index, name in enumerate(("early", "mid", "late")):
            reminder(harness, target, name, START + 100 + index, **templates)
        harness.core.store.put(
            "metadata",
            dict(
                id="proactive_scan_reminder",
                kind="reminder",
                deadline=START + 101,
                subject_id="mid",
            ),
        )
        harness.clock.now = START + 200

        # The window after the cursor yields 'late' and the wrap-around yields the head
        # rows, so the cursor must end on the globally last row, not the last appended one.
        proactive.tick(force=True)
        assert harness.core.store.get("metadata", "proactive_scan_reminder")["subject_id"] == (
            "late"
        )
        assert set(states(harness)) == {"early", "mid", "late"}
        await harness.core.close()

    asyncio.run(scenario())


def test_unusable_registration_is_isolated_and_leaves_the_scan():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=8,
            unanswered_limit=8,
        )
        proactive = harness.core.proactive
        proactive.max_subjects = 1
        templates = template(harness)
        proactive.put_template("doomed", 1, body="该{summary}了。")
        broken = reminder(
            harness, target, "broken", START + 1, template_id="doomed", template_version=1
        )
        healthy = reminder(harness, target, "healthy", START + 2, **templates)
        # A template row that no longer exists must not fail the whole tick, and must not
        # keep consuming the only scan slot on every rotation.
        harness.core.store.delete("proactive_templates", digest(["doomed", 1]))
        harness.clock.now = START + 10
        proactive.tick(force=True)
        assert proactive.reminder_metadata(broken["id"])["blocked_reason"] == "KeyError"
        assert states(harness) == {}
        proactive.tick(force=True)
        assert states(harness)[healthy["id"]] == "ready"
        # A blocked subject no longer consumes scan budget on later rotations.
        calls = count_materializations(harness)
        proactive.tick(force=True)
        assert calls == []
        # Re-registering with a working template clears the mark and the candidate is
        # produced again on the next tick.
        proactive.register_reminder(
            "broken",
            actor_id=target.subscription["actor_id"],
            subscription_id=target.subscription["id"],
            summary="修复后",
            due_at=START + 1,
            registered_by="admin:synthetic",
            evidence_ref="synthetic:repair",
            expected=proactive.reminder_metadata("broken")["version"],
            **templates,
        )
        assert "blocked_reason" not in proactive.reminder_metadata("broken")
        proactive.tick(force=True)
        assert states(harness)["broken"] == "ready"
        await harness.core.close()

    asyncio.run(scenario())


def test_recurring_goal_keeps_progressing_under_scan_pressure():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=8,
            unanswered_limit=8,
            expiry_seconds=3600,
        )
        proactive = harness.core.proactive
        proactive.max_subjects = 1
        templates = template(harness)
        stale = reminder(harness, target, "stale", START, **templates)
        recurring = goal(
            harness,
            target,
            "goal:recurring",
            START,
            summary="循环目标",
            interval_seconds=6 * HOUR,
            **templates,
        )
        harness.clock.now = START + 90000
        proactive.tick(force=True)
        assert states(harness)[stale["id"]] == "expired"
        # The recurring goal's missed occurrences are skipped, not replayed, and the next
        # future occurrence is reached with a single slot still held by the expired one.
        assert proactive.goal_metadata(recurring["id"])["state"] == "active"
        assert proactive.goal_metadata(recurring["id"])["next_due_at"] > harness.clock.now
        for _ in range(2):
            proactive.tick(force=True)
        assert len(proactive.candidates()) == 2
        due = proactive.goal_metadata(recurring["id"])["next_due_at"]
        harness.clock.now = due
        proactive.tick(force=True)
        goal_candidates = [c for c in proactive.candidates() if c["subject_id"] == recurring["id"]]
        assert [c["state"] for c in goal_candidates] == ["ready", "expired"]
        await harness.core.close()

    asyncio.run(scenario())


def test_open_candidate_queue_rotates_instead_of_starving_later_candidates():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        # The earlier destination is inside a long quiet window, so its candidate stays
        # open without progressing; the later one must still be decided.
        quiet = await member(
            harness,
            channel="private:a",
            actor="actor:a",
            timezone_name="UTC",
            quiet=("00:00", "23:59"),
            cooldown_seconds=0,
            daily_limit=8,
            unanswered_limit=8,
        )
        active = await member(
            harness,
            channel="private:b",
            account="b",
            actor="actor:b",
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=8,
            unanswered_limit=8,
        )
        proactive = harness.core.proactive
        proactive.max_candidates = 1
        templates = template(harness)
        held = reminder(harness, quiet, "held", START, **templates)
        free = reminder(harness, active, "free", START + 1, **templates)
        harness.clock.now = START + 10
        # Evaluate from the head of the open queue, where the unproductive candidate sits.
        harness.core.store.put(
            "metadata",
            dict(id="proactive_scan_candidate", kind="candidate", deadline=None, subject_id=None),
        )
        proactive.tick(force=True)
        assert states(harness)[held["id"]] == "deferred"
        assert states(harness)[free["id"]] == "pending"
        # One evaluation slot per tick, so the second candidate is reached by rotation
        # instead of waiting behind the deferred head forever.
        proactive.tick(force=True)
        assert states(harness)[free["id"]] == "ready"
        await proactive.work()
        assert states(harness)[free["id"]] == "sent"
        await harness.core.close()

    asyncio.run(scenario())


def test_default_off_and_only_explicit_registration():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        proactive = harness.core.proactive

        # Chat text that reads like a reminder request registers nothing by itself.
        await inbound(harness, text="提醒我明天九点喝水")
        assert harness.core.store.list("proactive_candidates") == []
        assert harness.core.store.list("proactive_reminders") == []
        assert harness.core.store.list("proactive_goals") == []
        assert harness.core.store.list("proactive_subscriptions") == []
        assert proactive.candidates() == []

        target = await member(harness)
        # A goal or reminder cannot exist without an explicit subscription.
        with pytest.raises(KeyError):
            proactive.register_goal(
                "goal:none",
                actor_id="actor:a",
                subscription_id="missing",
                summary="synthetic",
                due_at=START,
                template_id="remind",
                template_version=1,
            )
        # A chat message or model statement is never an accepted reminder basis.
        with pytest.raises(ValueError):
            reminder(
                harness,
                target,
                "reminder:chat",
                START,
                basis="chat_text",
            )
        # Unknown templates and unknown placeholders are refused, never guessed.
        with pytest.raises(ValueError):
            proactive.put_template("bad", 1, body="{nickname} 好")
        with pytest.raises(KeyError):
            reminder(harness, target, "reminder:missing", START, template_id="absent")
        # A body that cannot fit its own bound is rejected at registration, not at
        # schedule time where it would block every future pass.
        proactive.put_template("tight", 1, body="{summary}到了", max_chars=12)
        with pytest.raises(ValueError):
            reminder(
                harness,
                target,
                "reminder:tight",
                START,
                summary="一个肯定超过十二个字符上限的合成提醒摘要",
                template_id="tight",
            )

        # Nothing is due yet: registration alone produces no candidate.
        assert proactive.candidates() == []
        await harness.core.close()

    asyncio.run(scenario())


def test_gates_decide_defer_suppress_expire_and_ready():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(harness)
        proactive = harness.core.proactive
        options = template(harness)

        # Local 22:30 is inside the 22:00-08:00 window: deferred, not dropped.
        evening = reminder(harness, target, "reminder:evening", at(2026, 9, 14, 14, 30), **options)
        # Local 03:00 is also quiet; the window ends the same local morning.
        night = reminder(harness, target, "reminder:night", at(2026, 9, 14, 19, 0), **options)
        harness.clock.now = at(2026, 9, 14, 19, 0)
        proactive.tick(force=True)

        assert candidate(harness, subject_id=evening["id"])["state"] == "deferred"
        assert candidate(harness, subject_id=evening["id"])["decision"] == "quiet_hours"
        assert candidate(harness, subject_id=evening["id"])["defer_until"] == at(2026, 9, 15, 0, 0)
        assert candidate(harness, subject_id=night["id"])["defer_until"] == at(2026, 9, 15, 0, 0)

        harness.clock.now = at(2026, 9, 15, 0, 0)
        proactive.tick(force=True)
        assert candidate(harness, subject_id=night["id"])["state"] == "ready"
        assert candidate(harness, subject_id=evening["id"])["state"] == "ready"

        # Cooldown orders contacts: the first lands, the second waits.
        first = candidate(harness, subject_id=night["id"])
        await proactive.dispatch(first["id"])
        settled = proactive.candidate_view(first["id"])
        assert settled["state"] == "sent" and settled["delivered"] is False
        assert settled["delivery_evidence"] == "synthetic_adapter_receipt"
        assert candidate(harness, subject_id=evening["id"])["state"] == "deferred"
        assert candidate(harness, subject_id=evening["id"])["decision"] == "cooldown"

        # Unanswered contact suppression: two sends with no reply stop the third.
        harness.clock.now = at(2026, 9, 15, 1, 0)
        proactive.tick(force=True)
        second = candidate(harness, subject_id=evening["id"])
        await proactive.dispatch(second["id"])
        assert candidate(harness, subject_id=evening["id"])["state"] == "sent"
        third = reminder(
            harness,
            target,
            "reminder:third",
            at(2026, 9, 15, 1, 0),
            summary="散步",
            **options,
        )
        harness.clock.now = at(2026, 9, 15, 3, 30)
        proactive.tick(force=True)
        suppressed = candidate(harness, subject_id=third["id"])
        assert suppressed["state"] == "suppressed" and suppressed["decision"] == "unanswered_limit"
        assert suppressed["defer_until"] is None

        # A real reply clears the unanswered counter, but the local day is already at
        # its daily cap, so the candidate is suppressed rather than sent late.
        await harness.core.ingest(
            "nonebot", harness.request("我在的", channel="private:a", message="reply:1")
        )
        proactive.tick(force=True)
        blocked = candidate(harness, subject_id=third["id"])
        assert blocked["state"] == "suppressed" and blocked["decision"] == "daily_quota"
        # The quota deferral lands at local midnight, inside the quiet window, so the
        # next real opportunity is the following local morning.
        assert blocked["defer_until"] == at(2026, 9, 15, 16, 0)
        harness.clock.now = at(2026, 9, 15, 16, 0)
        proactive.tick(force=True)
        assert candidate(harness, subject_id=third["id"])["decision"] == "quiet_hours"
        harness.clock.now = at(2026, 9, 16, 0, 30)
        proactive.tick(force=True)
        assert candidate(harness, subject_id=third["id"])["state"] == "ready"
        await harness.core.close()

    asyncio.run(scenario())


def test_due_window_expiry_is_never_a_late_contact():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness, timezone_name="+08:00", quiet=("22:00", "08:00"), expiry_seconds=3600
        )
        proactive = harness.core.proactive
        # Local 22:30: the quiet window ends long after this contact's own window, so the
        # only honest outcome is expiry. Nothing is queued up for the next morning.
        quiet_one = reminder(
            harness, target, "reminder:quiet", at(2026, 9, 14, 14, 30), **template(harness)
        )
        # Local 08:00 the next morning is not quiet, but its own window is already past.
        stale = reminder(
            harness, target, "reminder:stale", at(2026, 9, 15, 0, 0), **template(harness)
        )
        harness.clock.now = at(2026, 9, 14, 14, 30)
        proactive.tick(force=True)
        assert (
            candidate(harness, subject_id=quiet_one["id"])["decision"]
            == "quiet_window_passed_expiry"
        )

        harness.clock.now = at(2026, 9, 15, 1, 30)
        proactive.tick(force=True)
        first = candidate(harness, subject_id=quiet_one["id"])
        second = candidate(harness, subject_id=stale["id"])
        assert first["state"] == "expired" and first["delivered"] is False
        assert second["state"] == "expired" and second["decision"] == "due_window_passed"
        assert await proactive.work() is None
        assert harness.adapter.calls == []
        await harness.core.close()

    asyncio.run(scenario())


def test_daily_quota_is_consumed_atomically_under_contention():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=1,
            unanswered_limit=4,
        )
        proactive = harness.core.proactive
        options = template(harness)
        one = goal(harness, target, "goal:one", START, summary="读书", **options)
        two = goal(harness, target, "goal:two", START, summary="跑步", **options)
        proactive.tick(force=True)
        left = candidate(harness, kind="goal", subject_id=one["id"])
        right = candidate(harness, kind="goal", subject_id=two["id"])
        assert left["state"] == "ready" and right["state"] == "ready"

        # Both submit paths run before either transport call returns: only one may take
        # the single daily slot, and the loser is suppressed rather than silently sent.
        harness.adapter.gate = asyncio.Event()
        tasks = [
            asyncio.create_task(proactive.dispatch(left["id"])),
            asyncio.create_task(proactive.dispatch(right["id"])),
        ]
        await asyncio.sleep(0)
        assert len(harness.adapter.calls) == 1
        harness.adapter.gate.set()
        await asyncio.gather(*tasks)

        states = sorted(
            [
                proactive.candidate_view(left["id"])["state"],
                proactive.candidate_view(right["id"])["state"],
            ]
        )
        assert states == ["sent", "suppressed"]
        loser = (
            left["id"]
            if proactive.candidate_view(left["id"])["state"] == "suppressed"
            else right["id"]
        )
        assert proactive.candidate_view(loser)["decision"] == "daily_quota"
        assert (
            harness.core.proactive._quota_used(
                harness.core.proactive._get("subscriptions", target.subscription["id"]),
                harness.clock.now,
            )
            == 1
        )
        await harness.core.close()

    asyncio.run(scenario())


def test_cross_midnight_and_clock_adjustment_recompute_from_absolute_time():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="+09:00",
            quiet=("21:30", "06:30"),
            cooldown_seconds=0,
            daily_limit=4,
            expiry_seconds=172800,
        )
        proactive = harness.core.proactive
        # Local 21:30 at +09:00 is the first minute of the wrapped quiet window.
        evening = reminder(
            harness, target, "reminder:wrap", at(2026, 9, 14, 12, 30), **template(harness)
        )
        harness.clock.now = at(2026, 9, 14, 12, 30)
        proactive.tick(force=True)
        deferred = candidate(harness, subject_id=evening["id"])
        assert deferred["decision"] == "quiet_hours"
        # 06:30 at +09:00 the next local morning is 21:30 UTC today.
        assert deferred["defer_until"] == at(2026, 9, 14, 21, 30)

        # A wall-clock jump forward past the window resolves the same candidate.
        harness.clock.now = at(2026, 9, 14, 21, 30)
        proactive.tick(force=True)
        assert candidate(harness, subject_id=evening["id"])["state"] == "ready"

        # A wall-clock jump backwards recomputes the decision; no duplicate candidate,
        # no local time stored anywhere.
        harness.clock.now = at(2026, 9, 14, 13, 0)
        proactive.tick(force=True)
        assert candidate(harness, subject_id=evening["id"])["state"] == "deferred"
        assert len(proactive.candidates()) == 1

        # Restart-equivalent recomputation from the same durable rows is identical.
        before = candidate(harness, subject_id=evening["id"])
        proactive.tick(force=True)
        assert candidate(harness, subject_id=evening["id"]) == before
        await harness.core.close()

    asyncio.run(scenario())


def write_tzif(directory, name, transitions):
    """Minimal TZif v1 file: UTC+1 standard, UTC+2 daylight, two real transitions."""
    abbreviations = b"CET\x00CEST\x00"
    types = struct.pack(">lbb", 3600, 0, 0) + struct.pack(">lbb", 7200, 1, 4)
    data = (
        struct.pack(">%dl" % len(transitions), *transitions) + bytes([1, 0]) + types + abbreviations
    )
    header = (
        b"TZif\x00"
        + b"\x00" * 15
        + struct.pack(">6l", 0, 0, 0, len(transitions), 2, len(abbreviations))
    )
    target = directory / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(header + data)
    assert zoneinfo.ZoneInfo.from_file(target.open("rb"), key=name).key == name
    return target


def test_dst_gap_and_overlap_use_real_instants(tmp_path):
    write_tzif(
        tmp_path / "tz",
        "Synthetic/Central",
        [
            int(at(2026, 3, 29, 1, 0)),
            int(at(2026, 10, 25, 1, 0)),
        ],
    )
    zoneinfo.reset_tzpath([str(tmp_path / "tz")])
    try:
        tz = zoneinfo.ZoneInfo("Synthetic/Central")
        # 02:30 does not exist on the spring-forward day; the first real minute is 03:00.
        assert resolve(tz, date(2026, 3, 29), 150) == at(2026, 3, 29, 1, 0)
        # 02:30 happens twice on the fall-back day; fold=0 keeps the earlier instant.
        assert resolve(tz, date(2026, 10, 25), 150) == at(2026, 10, 25, 0, 30)
        assert in_quiet(90, 60, 150) and not in_quiet(30, 60, 150)
        # A same-day window ends later today; a wrapping one ends the next local morning.
        assert quiet_end(tz, datetime(2026, 3, 29, 1, 30, tzinfo=tz), 60, 150) == at(
            2026, 3, 29, 1, 0
        )
        assert quiet_end(tz, datetime(2026, 3, 29, 23, 0, tzinfo=tz), 1320, 480) == at(
            2026, 3, 30, 6, 0
        )
        assert quiet_end(tz, datetime(2026, 3, 29, 3, 0, tzinfo=tz), 1320, 480) == at(
            2026, 3, 29, 6, 0
        )

        async def scenario():
            harness = Fixture()
            harness.clock.now = at(2026, 3, 29, 0, 30)
            target = await member(
                harness,
                timezone_name="Synthetic/Central",
                quiet=("01:00", "02:30"),
                cooldown_seconds=0,
                expiry_seconds=86400,
            )
            proactive = harness.core.proactive
            entry = reminder(
                harness, target, "reminder:dst", at(2026, 3, 29, 0, 30), **template(harness)
            )
            proactive.tick(force=True)
            deferred = candidate(harness, subject_id=entry["id"])
            assert deferred["decision"] == "quiet_hours"
            # The window ends at a civil minute that does not exist; the schedule uses
            # the first real instant after the gap.
            assert deferred["defer_until"] == at(2026, 3, 29, 1, 0)
            harness.clock.now = at(2026, 3, 29, 1, 0)
            proactive.tick(force=True)
            assert candidate(harness, subject_id=entry["id"])["state"] == "ready"
            await harness.core.close()

        asyncio.run(scenario())
    finally:
        zoneinfo.reset_tzpath()


def test_revocation_cancellation_and_authority_recheck():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness, timezone_name="UTC", quiet=("00:00", "00:00"), cooldown_seconds=0
        )
        proactive = harness.core.proactive
        revoked = reminder(harness, target, "reminder:revoked", START, **template(harness))
        proactive.tick(force=True)
        proactive.revoke_subscription(target.subscription["id"], reason="user_withdrew")
        closed = candidate(harness, subject_id=revoked["id"])
        assert closed["state"] == "cancelled"
        assert closed["decision"] == "subscription_revoked"
        assert await proactive.work() is None
        assert harness.adapter.calls == []

        # A local authority change between scheduling and submission stops the contact.
        other = await member(
            harness,
            channel="private:b",
            actor="actor:b",
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
        )
        pending = reminder(harness, other, "reminder:role", START, **template(harness))
        proactive.tick(force=True)
        assert candidate(harness, subject_id=pending["id"])["state"] == "ready"
        harness.core.bindings.pop("qq-private")
        await proactive.dispatch(candidate(harness, subject_id=pending["id"])["id"])
        denied = candidate(harness, subject_id=pending["id"])
        assert denied["state"] == "cancelled"
        assert denied["decision"] == "authorization_revoked:unknown_binding"
        assert harness.adapter.calls == []

        # Explicit cancellation before submission, and cancellation of the subject.
        harness.core.bindings["qq-private"] = dict(
            service="nonebot",
            namespace="qq",
            audience="self_private",
            actor_ids=["actor:a", "actor:b"],
            classification=dict(
                value="real",
                basis="registered_input_mode",
                policy_ref="fixture:real",
                policy_version=1,
            ),
        )
        third = await member(
            harness,
            channel="private:c",
            account="a",
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
        )
        explicit = reminder(harness, third, "reminder:explicit", START, **template(harness))
        proactive.tick(force=True)
        proactive.cancel_candidate(
            candidate(harness, subject_id=explicit["id"])["id"], reason="operator_cancelled"
        )
        assert candidate(harness, subject_id=explicit["id"])["decision"] == "operator_cancelled"

        stopped = goal(harness, third, "goal:stopped", START, summary="复习", **template(harness))
        proactive.tick(force=True)
        proactive.cancel_subject("goal", stopped["id"], reason="user_completed")
        assert candidate(harness, kind="goal", subject_id=stopped["id"])["decision"] == (
            "subject_cancelled"
        )
        await harness.core.close()

    asyncio.run(scenario())


def test_late_receipt_only_settles_its_own_attempt():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness, timezone_name="UTC", quiet=("00:00", "00:00"), cooldown_seconds=0
        )
        proactive = harness.core.proactive
        entry = reminder(harness, target, "reminder:late", START, **template(harness))
        proactive.tick(force=True)
        first_id = candidate(harness, subject_id=entry["id"])["id"]

        harness.adapter.answers = ["unknown"]
        await proactive.dispatch(first_id)
        old = proactive.attempts(first_id)[0]
        assert old["state"] == "unknown" and old["verified_delivery"] is False
        assert proactive.candidate_view(first_id)["unresolved"] is True

        proactive.retry_candidate(first_id, reason="operator_retry")
        second_attempt = candidate(harness, subject_id=entry["id"])["id"]
        harness.adapter.gate = asyncio.Event()
        task = asyncio.create_task(proactive.dispatch(second_attempt))
        await asyncio.sleep(0)
        assert proactive.candidate_view(second_attempt)["state"] == "sending"
        second_request = harness.adapter.calls[-1]

        # The old attempt's real receipt arrives after the newer submit: it settles on
        # that attempt alone and never touches the newer attempt or the candidate.
        request = harness.core.store.get("proactive_attempts", old["id"])["request"]
        proactive.settle(old["id"], harness.adapter.receipt(request, "sent"))
        late = proactive.attempt_metadata(old["id"])
        assert late["state"] == "sent" and late["stale"] is True
        assert late["verified_delivery"] is False
        in_flight = proactive.candidate_view(second_attempt)
        assert in_flight["state"] == "sending" and in_flight["delivered"] is False

        harness.adapter.gate.set()
        await task
        final = proactive.candidate_view(second_attempt)
        assert final["state"] == "sent"
        assert final["receipt"]["request_id"] == second_request["request_id"]
        assert [a["attempt_no"] for a in proactive.attempts(second_attempt)] == [1, 2]
        await harness.core.close()

    asyncio.run(scenario())


def test_unknown_is_never_resent_and_survives_a_restart(tmp_path):
    path = str(tmp_path / "proactive.db")

    async def scenario():
        harness = Fixture(path)
        harness.clock.now = START
        target = await member(
            harness, timezone_name="UTC", quiet=("00:00", "00:00"), cooldown_seconds=0
        )
        proactive = harness.core.proactive
        entry = reminder(harness, target, "reminder:unknown", START, **template(harness))
        proactive.tick(force=True)
        slot = candidate(harness, subject_id=entry["id"])["id"]

        # A submit that never returns is abandoned by the crash; the durable intent stays.
        harness.adapter.gate = asyncio.Event()
        task = asyncio.create_task(proactive.dispatch(slot))
        await asyncio.sleep(0)
        assert proactive.candidate_view(slot)["state"] == "sending"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await harness.core.close()

        restarted = Fixture(path)
        restarted.clock.now = harness.clock.now
        restarted.core.recover()
        settled = restarted.core.proactive.candidate_view(slot)
        assert settled["state"] == "unknown" and settled["unresolved"] is True
        assert settled["delivered"] is False
        assert restarted.adapter.calls == []
        for _ in range(5):
            await restarted.core.proactive.work()
        assert restarted.adapter.calls == []
        assert [a["attempt_no"] for a in restarted.core.proactive.attempts(slot)] == [1]

        # Only an explicit retry reopens it, with a new attempt.
        restarted.core.proactive.retry_candidate(slot, reason="operator_retry")
        await restarted.core.proactive.work()
        assert restarted.core.proactive.candidate_view(slot)["state"] == "sent"
        assert [a["attempt_no"] for a in restarted.core.proactive.attempts(slot)] == [1, 2]
        await restarted.core.close()

    asyncio.run(scenario())


def test_no_dispatcher_means_no_delivery_claim_and_no_quota_use():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness, timezone_name="UTC", quiet=("00:00", "00:00"), cooldown_seconds=0
        )
        proactive = harness.core.proactive
        entry = reminder(harness, target, "reminder:gap", START, **template(harness))
        harness.adapter.available = False
        proactive.tick(force=True)

        view = candidate(harness, subject_id=entry["id"])
        assert view["state"] == "ready"
        assert view["delivered"] is False and view["delivery_evidence"] is None
        assert view["dispatch"] == dict(available=False, contract_gap=CONTRACT_GAP)
        for _ in range(3):
            await proactive.work()
        assert harness.adapter.calls == []
        assert proactive.attempts(view["id"]) == []
        subscription = proactive._get("subscriptions", target.subscription["id"])
        assert proactive._quota_used(subscription, harness.clock.now) == 0
        assert harness.core.store.list("proactive_attempts") == []
        await harness.core.close()

    asyncio.run(scenario())


def test_proactive_submit_never_blocks_ordinary_chat():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness, timezone_name="UTC", quiet=("00:00", "00:00"), cooldown_seconds=0
        )
        proactive = harness.core.proactive
        entry = reminder(harness, target, "reminder:slow", START, **template(harness))
        proactive.tick(force=True)
        slot = candidate(harness, subject_id=entry["id"])["id"]
        assert proactive.candidate_view(slot)["state"] == "ready"

        harness.adapter.gate = asyncio.Event()
        submit = asyncio.create_task(proactive.work())
        await asyncio.sleep(0)
        assert len(harness.adapter.calls) == 1

        # The chat path keeps its own tick, model call and delivery while the proactive
        # submit is still waiting on the transport.
        await harness.core.ingest(
            "nonebot",
            harness.request("普通聊天", channel="private:chat", account="b", actor="actor:b"),
        )
        harness.clock.advance(6)
        await harness.cycles()
        turns = harness.core.store.list("turns")
        assert all(t["phase"] == "sent" for t in turns)
        chat = [t for t in turns if t["scope"]["actor_id"] == "actor:b"]
        assert len(chat) == 1 and chat[0]["phase"] == "sent"
        assert harness.sender.calls[-1]["text"]

        harness.adapter.gate.set()
        await submit
        assert proactive.candidate_view(slot)["state"] == "sent"
        assert proactive.candidate_view(slot)["delivered"] is False
        await harness.core.close()

    asyncio.run(scenario())


def test_core_tick_and_read_ports_stay_scoped():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        left = await member(
            harness,
            channel="private:a",
            actor="actor:a",
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
        )
        right = await member(
            harness,
            channel="private:b",
            account="b",
            actor="actor:b",
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
        )
        head = harness.core.store.source_head()
        proactive = harness.core.proactive
        reminder(harness, left, "reminder:a", START, **template(harness))
        goal(harness, right, "goal:b", START, summary="整理房间", **template(harness))

        # The ordinary Core tick owns the schedule; no separate driver is required.
        await harness.core.tick()
        assert {c["actor_id"] for c in proactive.candidates()} == {"actor:a", "actor:b"}
        assert [c["subject_id"] for c in proactive.candidates(actor_id="actor:a")] == ["reminder:a"]
        assert [s["id"] for s in proactive.subjects("actor:b")] == ["goal:b"]
        assert [s["id"] for s in proactive.subscriptions("actor:a")] == [left.subscription["id"]]

        await proactive.work()
        assert proactive.candidates(states=["sent"])[0]["delivered"] is False
        # Proactive state is Core-owned derived data: the real source chain is untouched.
        assert harness.core.store.source_head() == head
        await harness.core.close()

    asyncio.run(scenario())


def test_goal_recurrence_advances_only_after_a_sent_contact():
    async def scenario():
        harness = Fixture()
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=4,
            unanswered_limit=4,
        )
        proactive = harness.core.proactive
        entry = goal(
            harness,
            target,
            "goal:daily",
            START,
            summary="练习",
            interval_seconds=6 * HOUR,
            **template(harness),
        )
        proactive.tick(force=True)
        slot = candidate(harness, kind="goal", subject_id=entry["id"])["id"]

        harness.adapter.answers = ["failed"]
        await proactive.dispatch(slot)
        assert proactive.candidate_view(slot)["state"] == "failed"
        assert proactive.goal_metadata(entry["id"])["next_due_at"] == START
        assert proactive.candidate_view(slot)["delivered"] is False

        proactive.retry_candidate(slot, reason="operator_retry")
        harness.clock.now = START + 60
        await proactive.work()
        assert proactive.candidate_view(slot)["state"] == "sent"
        assert proactive.goal_metadata(entry["id"])["next_due_at"] == START + 6 * HOUR
        assert proactive.candidate_view(slot)["delivered"] is False
        await harness.core.close()

    asyncio.run(scenario())


def test_edges_are_closed_and_recovery_is_idempotent(tmp_path):
    path = str(tmp_path / "edges.db")

    async def scenario():
        harness = Fixture(path)
        harness.clock.now = START
        target = await member(
            harness,
            timezone_name="UTC",
            quiet=("00:00", "00:00"),
            cooldown_seconds=0,
            daily_limit=4,
            unanswered_limit=4,
            expiry_seconds=3600,
        )
        proactive = harness.core.proactive
        templates = template(harness)

        # A recurring goal whose occurrence expired moves on instead of replaying it.
        entry = goal(
            harness,
            target,
            "goal:recurring",
            START,
            summary="练习",
            interval_seconds=6 * HOUR,
            **templates,
        )
        proactive.tick(force=True)
        first = candidate(harness, kind="goal", subject_id=entry["id"])
        harness.clock.now = START + 2 * HOUR
        proactive.tick(force=True)
        assert proactive.candidate_view(first["id"])["decision"] == "due_window_passed"
        assert proactive.goal_metadata(entry["id"])["state"] == "active"
        assert proactive.goal_metadata(entry["id"])["next_due_at"] == START + 6 * HOUR
        assert len(proactive.candidates()) == 1

        harness.clock.now = START + 6 * HOUR
        proactive.tick(force=True)
        second = candidate(harness, kind="goal", subject_id=entry["id"], state="ready")
        assert second["id"] != first["id"] and second["state"] == "ready"

        # A settled attempt is idempotent for the same receipt and conflicts otherwise.
        harness.adapter.answers = ["failed"]
        await proactive.dispatch(second["id"])
        request = harness.core.store.get(
            "proactive_attempts", proactive.candidate_view(second["id"])["attempt"]
        )["request"]
        receipt = harness.adapter.receipt(request, "failed")
        assert proactive.settle(request["request_id"], receipt)["state"] == "failed"
        with pytest.raises(Fault):
            proactive.settle(request["request_id"], harness.adapter.receipt(request, "sent"))

        # Retry after the contact window has passed expires instead of sending late.
        harness.clock.now = START + 8 * HOUR
        with pytest.raises(ValueError):
            proactive.retry_candidate(second["id"], reason="operator_retry")
        assert proactive.candidate_view(second["id"])["state"] == "expired"
        assert proactive.candidate_view(second["id"])["delivered"] is False

        # A stale expected version is refused without changing anything.
        reminder(harness, target, "reminder:stale", START + 9 * HOUR, **templates)
        harness.clock.now = START + 9 * HOUR
        proactive.tick(force=True)
        third = candidate(harness, subject_id="reminder:stale")
        with pytest.raises(ValueError):
            proactive.cancel_candidate(third["id"], reason="operator", expected=1)
        assert proactive.candidate_view(third["id"])["state"] == "ready"

        # Recovery is safe to repeat: the second pass has nothing left to settle.
        await harness.core.close()
        restarted = Fixture(path)
        restarted.clock.now = harness.clock.now
        restarted.core.recover()
        restarted.core.recover()
        assert [a["state"] for a in restarted.core.proactive.attempts(second["id"])] == ["failed"]
        assert restarted.adapter.calls == []
        await restarted.core.close()

    asyncio.run(scenario())


def test_v5_migration_backup_rollback_and_source_preservation(tmp_path):
    path = tmp_path / "synthetic.db"
    store = Store(path)
    head = store.source_head()
    store.put("conversations", {"id": "synthetic", "private": "synthetic preserved"})
    store.close()
    with closing(sqlite3.connect(path)) as db, db:
        for table in PROACTIVE_TABLES:
            db.execute(f"DROP TABLE {table}")
        db.execute("PRAGMA user_version=5")
        # A conflicting index name makes migration fail midway, after prior DDL.
        db.execute("CREATE TABLE proactive_goals_queue (id TEXT)")
    with pytest.raises(sqlite3.OperationalError):
        Store(path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 5
        assert not db.execute(
            "SELECT name FROM sqlite_master WHERE name='proactive_candidates'"
        ).fetchone()
        db.execute("DROP TABLE proactive_goals_queue")
        db.commit()
    with closing(Store(path)) as store:
        assert store.source_head() == head
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 10
        assert store.get("conversations", "synthetic")["private"] == "synthetic preserved"
        assert store.list("proactive_candidates") == []
    backups = sorted(tmp_path.glob("*.pre-life-runtime-v10-*.bak"))
    assert len(backups) == 2  # the failed attempt and the retry each took one
    # Both attempts started below v7, so the persona step is crossed inside those same
    # migrations: a multi-version jump takes one recovery backup, at the highest structural
    # step, and never a second one for an earlier step in the same run.
    assert not list(tmp_path.glob("*.pre-persona-ops-v9-*.bak"))
    for backup in backups:
        with closing(sqlite3.connect(backup)) as db:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 5
            assert (
                "synthetic preserved" in db.execute("SELECT body FROM conversations").fetchone()[0]
            )


def test_clock_helper_is_not_required_for_the_schedule():
    clock = Clock()
    clock.advance(60)
    assert clock() == clock.now
