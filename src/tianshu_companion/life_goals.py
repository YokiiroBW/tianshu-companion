"""Rolling choices in the existing concern/activity owners, not a second schedule."""

import asyncio
import copy
from datetime import datetime, timedelta

from .clients import utc
from .contracts import Fault, canonical, digest, strict_json
from .life import text, zone
from .life_daily import BASE_SCHEDULE


class AutonomousGoals:
    def __init__(self, execution):
        self.execution, self.core = execution, execution.core
        self.life, self.store = self.core.life, self.core.store

    def recover(self):
        for actor in self.store.list("life_actors"):
            marker = self.store.get("metadata", actor.get("autonomy_operation", ""))
            if marker and marker["state"] in {"preparing", "submitted"}:
                marker["state"] = "unknown" if marker["state"] == "submitted" else "interrupted"
                self.store.put("metadata", marker)

    def window(self, actor):
        now = self.core.clock()
        phase, _ = self.life._phase(actor, now)
        protected = {
            entry["activity"]
            for entry in BASE_SCHEDULE
            if entry["minute"] in {0, 420, 720, 1080, 1380}
        }
        manual = actor.get("manual")
        if (
            phase["activity"] in protected
            or manual
            and (manual["hold_until"] is None or manual["hold_until"] > now)
        ):
            return None
        world = self.life._get("worlds", actor["world_id"])
        local = datetime.fromtimestamp(now, zone(world["timezone"]))
        minute = local.hour * 60 + local.minute
        next_minute = next(
            (entry["minute"] for entry in actor["schedule"] if entry["minute"] > minute),
            actor["schedule"][0]["minute"] + 1440,
        )
        boundary = local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
            minutes=next_minute
        )
        end = boundary.timestamp()
        # Existing explicit promises reserve their due interval before discretionary work.
        reminder = self.store.db.execute(
            "SELECT deadline FROM proactive_reminders WHERE status='active' AND json_extract(body,'$.actor_id')=? AND deadline>=? ORDER BY deadline,id LIMIT 1",
            (actor["id"], now - 300),
        ).fetchone()
        if reminder:
            end = min(end, reminder[0] - 60)
        if end - now < 60:
            return None
        return dict(phase=copy.deepcopy(phase), available_until=end, available_seconds=end - now)

    def current(self, actor):
        return self.life.activities.current(actor["id"], actor.get("activity_scope"))

    async def verified_records(self, records):
        """Withdrawn derived questions are not planner input or compulsory commitments."""
        result = []
        for record in records:
            if record and record.get("autonomy_basis"):
                try:
                    await self.core.actor_memory.check(record["autonomy_basis"])
                except Fault as error:
                    if error.code in {
                        "scope_changed",
                        "not_found",
                        "forbidden",
                        "version_conflict",
                    }:
                        continue
                    raise
            if record:
                result.append(record)
        return result

    async def verify_records(self, records):
        for record in records:
            if record and record.get("autonomy_basis"):
                await self.core.actor_memory.check(record["autonomy_basis"])

    def snapshot(self, actor, window):
        activity = self.current(actor)
        concerns = self.life.concerns.planning(
            actor["id"],
            current_id=(activity or {}).get("autonomous_goal_id"),
            available_until=window["available_until"],
        )
        return dict(
            actor_epoch=actor.get("life_runtime_epoch", 0),
            personality_version=actor["personality_version"],
            schedule_version=actor["schedule_version"],
            manual=actor.get("manual"),
            activity=activity,
            concerns=concerns,
            routine=window,
            ephemeral_attention=actor.get("life_interests", []),
            observation_hint=actor.get("autonomy_observation_hint"),
        )

    def check(self, actor_id, captured):
        actor = self.store.get("life_actors", actor_id)
        if (
            not actor
            or not actor.get("life_enabled", True)
            or not self.core.reading.actor_enabled(actor_id)
        ):
            raise Fault("forbidden")
        window = self.window(actor)
        if window is None:
            raise Fault("scope_changed")
        current = self.snapshot(actor, window)
        for field in (
            "actor_epoch",
            "personality_version",
            "schedule_version",
            "manual",
            "activity",
            "concerns",
            "ephemeral_attention",
            "observation_hint",
        ):
            if current[field] != captured[field]:
                raise Fault("scope_changed")
        if window["phase"] != captured["routine"]["phase"]:
            raise Fault("scope_changed")
        return actor

    async def work(self, *, actor_id):
        actor = self.store.get("life_actors", actor_id)
        if (
            not actor
            or not actor.get("autonomous")
            or not actor.get("life_enabled", True)
            or not hasattr(self.life.gateway, "complete")
        ):
            return False
        now = self.core.clock()
        if (
            actor.get("autonomy_epoch") == actor.get("life_runtime_epoch", 0)
            and actor.get("autonomy_due_at", 0) > now
        ):
            return False
        window = self.window(actor)
        if window is None or not self.life.generation_available():
            return False
        hint = actor.get("autonomy_observation_hint")
        if hint:
            try:
                await self.core.actor_memory.check(hint["material"])
            except Fault as error:
                if getattr(error, "context_dependency_invalid", False) or error.code in {
                    "not_found",
                    "forbidden",
                    "version_conflict",
                }:
                    actor.pop("autonomy_observation_hint", None)
                    actor["autonomy_due_at"] = now
                    self.life._save("actors", actor)
                return False
        current = self.current(actor)
        if current and current.get("step_operation"):
            progress = self.store.get("metadata", current["step_operation"])
            if progress and progress["state"] in {"unknown", "submitted", "tools_pending"}:
                return False  # Replanning cannot turn an uncertain side effect into a new call.
        if (
            current
            and current["state"] in {"running", "paused"}
            and not current.get("autonomous_goal_id")
            and not current["id"].startswith("routine:")
        ):
            return False  # Existing promised/manual work retains priority.
        captured = self.snapshot(actor, window)
        # Unscoped autonomy must not copy a person's private concern into a global intention.
        actor["autonomy_sequence"] = actor.get("autonomy_sequence", 0) + 1
        operation = "goal-choice:" + digest(
            [actor_id, actor.get("life_runtime_epoch", 0), actor["autonomy_sequence"]]
        )
        actor.update(
            autonomy_operation=operation,
            autonomy_due_at=now + 1800,
            autonomy_epoch=actor.get("life_runtime_epoch", 0),
        )
        self.life._save("actors", actor)
        marker = dict(
            id=operation, actor_id=actor_id, state="preparing", captured=captured, created_at=now
        )
        self.store.put("metadata", marker)
        try:
            visible_capture = copy.deepcopy(captured)
            visible_capture["concerns"] = await self.verified_records(captured["concerns"])
            active = await self.verified_records([captured["activity"]])
            visible_capture["activity"] = active[0] if active else None
            source_records = visible_capture["concerns"] + active
            memory = None
            if self.core.actor_memory.available:
                previous = self.store.get(
                    "life_activities", actor.get("autonomy_last_activity", "")
                )
                memory_query = "自身兴趣、未完成的探索和需要修正的认识"
                hint = actor.get("autonomy_observation_hint")
                if hint:
                    await self.core.actor_memory.check(hint["material"])
                    memory_query += "；最近旁听形成的问题：" + hint["topic"]
                if (
                    previous
                    and previous["actor_id"] == actor_id
                    and await self.verified_records([previous])
                ):
                    source_records.append(previous)
                    memory_query += "；最近实际活动：" + previous["title"]
                memory = await self.core.actor_memory.planning_query(
                    actor_id,
                    dict(purpose="plan", audience=dict(kind="actor_only")),
                    memory_query,
                    limit=6,
                )
            generation, lease = await self.execution.generation(actor_id, operation)
            material = dict(
                now=utc(now),
                persona=self.life.persona_reader(actor_id),
                **visible_capture,
                actor_self_memory=memory["items"] if memory else [],
                capabilities=self.core.skills.result(actor_id)["skills"],
                meaning="Intentions, receipts and failures are distinct. Today's attention is temporary, never an enduring preference or permission. Private actor material stays actor-only.",
            )
            messages = [
                dict(
                    role="system",
                    content=(
                        "Choose one concrete next personal goal from persona, actual long-term interests, today's temporary attention, unfinished concerns, actual reading and available time. The routine is a boundary; preserve meals/sleep and explicit promises. You can continue, defer, abandon/reconsider or start a new goal. Never invent offline progress or user actions. Actual reading occurs later through tools. Return only JSON {decision:new|continue|defer|abandon|rest, concern_id:null|string, title:string, goal:string, reason:string, delay_seconds:integer}. Optionally include observation_inspired:boolean, true only when decision=new actually chooses the current observation_hint topic; otherwise false or omit. This hint is a one-time opportunity, not a standing instruction for unrelated goals. New requires a specific attainable title/goal. Continue/defer/abandon names an existing concern_id. Delay is 60..86400 seconds; rest is a valid choice. A new question from actual reading can become a goal; preferences require grounded actor-memory receipts, not this choice."
                    ),
                ),
                dict(role="user", content=canonical(material)),
            ]
            async with self.life.model_slots:
                self.check(actor_id, captured)
                await self.verify_records(source_records)
                if memory:
                    await self.core.actor_memory.check(memory)
                if actor.get("autonomy_observation_hint"):
                    await self.core.actor_memory.check(
                        actor["autonomy_observation_hint"]["material"]
                    )
                self.life.verify_generation_lease(lease)
                marker.update(state="submitted", messages=messages, memory=memory)
                self.store.put("metadata", marker)
                output, receipt = await asyncio.wait_for(
                    self.life.gateway.generate(generation, messages), 60
                )
            self.life.verify_generation_lease(lease)
            await self.verify_records(source_records)
            if memory:
                await self.core.actor_memory.check(memory)
            if actor.get("autonomy_observation_hint"):
                await self.core.actor_memory.check(actor["autonomy_observation_hint"]["material"])
            self.check(actor_id, captured)
            choice = strict_json("\n".join(output))
            self.validate_choice(choice, visible_capture)
            with self.store.transaction():
                actor = self.check(actor_id, captured)
                self.apply(actor, choice, visible_capture, memory)
                marker.update(
                    state="completed",
                    choice=choice,
                    receipt=receipt,
                    completed_at=self.core.clock(),
                )
                self.store.put("metadata", marker)
        except asyncio.CancelledError:
            marker.update(state="unknown" if marker["state"] == "submitted" else "cancelled")
            self.store.put("metadata", marker)
            raise
        except (Fault, ValueError, KeyError, OSError, TypeError, TimeoutError) as error:
            marker.update(
                state="unknown"
                if isinstance(error, TimeoutError) or isinstance(error, Fault) and error.unknown
                else "failed",
                error=error.code if isinstance(error, Fault) else "invalid_input",
            )
            self.store.put("metadata", marker)
        return True

    @staticmethod
    def validate_choice(choice, captured):
        required = {
            "decision",
            "concern_id",
            "title",
            "goal",
            "reason",
            "delay_seconds",
        }
        if (
            not isinstance(choice, dict)
            or not required <= set(choice)
            or set(choice) - required - {"observation_inspired"}
        ):
            raise Fault("invalid_input")
        if "observation_inspired" in choice and (
            type(choice["observation_inspired"]) is not bool
            or choice["observation_inspired"]
            and (choice["decision"] != "new" or not captured.get("observation_hint"))
        ):
            raise Fault("invalid_input")
        if (
            choice["decision"] not in {"new", "continue", "defer", "abandon", "rest"}
            or type(choice["delay_seconds"]) is not int
            or not 60 <= choice["delay_seconds"] <= 86400
        ):
            raise Fault("invalid_input")
        for field, maximum in (("title", 120), ("goal", 4000), ("reason", 1000)):
            text(choice[field], maximum)
        if choice["decision"] in {"continue", "defer", "abandon"} and choice["concern_id"] not in {
            item["id"] for item in captured["concerns"]
        }:
            raise Fault("invalid_input")
        if choice["decision"] in {"new", "rest"} and choice["concern_id"] is not None:
            raise Fault("invalid_input")
        if choice["decision"] in {"defer", "abandon"} and not choice["concern_id"].startswith(
            "autonomy:"
        ):
            raise Fault("forbidden")
        commitments = [
            item
            for item in captured["concerns"]
            if not item["id"].startswith("autonomy:")
            and item.get("next_due_at") is not None
            and item["next_due_at"] <= captured["routine"]["available_until"]
        ]
        if commitments:
            first = min(commitments, key=lambda item: (item["next_due_at"], item["id"]))
            if choice["decision"] != "continue" or choice["concern_id"] != first["id"]:
                raise Fault("forbidden")

    def apply(self, actor, choice, captured, memory):
        now, decision = self.core.clock(), choice["decision"]
        hint = actor.pop("autonomy_observation_hint", None)
        if decision in {"new", "continue"}:
            actor["autonomy_due_at"] = now + choice["delay_seconds"]
        if hint or decision in {"new", "continue"}:
            # Applied choices consume this opportunity atomically. Existing goals
            # retain their own cause; later unrelated choices get no stale query override.
            self.life._save("actors", actor)
        inspired = decision == "new" and choice.get("observation_inspired", False) and hint
        if inspired:
            proof = hint["material"]
            memory = copy.deepcopy(memory or proof)
            known = {canonical(item["ref"]) for item in memory["items"]}
            memory["items"].extend(
                copy.deepcopy(item)
                for item in proof["items"]
                if canonical(item["ref"]) not in known
            )
        current = self.current(actor)
        goal_id = choice["concern_id"]
        if current and current["state"] in {"running", "paused"}:
            if (
                decision in {"new", "rest"}
                or decision in {"abandon", "defer"}
                and current.get("autonomous_goal_id") == goal_id
                or decision == "continue"
                and current.get("autonomous_goal_id") != goal_id
            ):
                state = "cancelled" if decision == "abandon" else "paused"
                if current["state"] != state:
                    self.life.activities.transition(
                        actor["id"],
                        current["id"],
                        state,
                        expected=current["version"],
                        reason=choice["reason"],
                    )
        if decision in {"rest", "abandon", "defer"}:
            if goal_id:
                old = self.life.concerns.get(actor["id"], goal_id)
                value = {
                    key: old[key]
                    for key in (
                        "id",
                        "title",
                        "goal",
                        "state",
                        "scope",
                        "sources",
                        "next_due_at",
                        "expires_at",
                        "result_refs",
                    )
                }
                value.update(
                    fragments=[],
                    state="cancelled" if decision == "abandon" else "open",
                    next_due_at=None if decision == "abandon" else now + choice["delay_seconds"],
                )
                self.life.concerns.save(actor["id"], value, expected=old["version"])
            actor = self.store.get("life_actors", actor["id"])
            actor["autonomy_due_at"] = now + choice["delay_seconds"]
            self.life._save("actors", actor)
            return
        if decision == "new":
            goal_id = "autonomy:" + digest([actor["id"], actor["autonomy_operation"]])
            concern = self.life.concerns.save(
                actor["id"],
                dict(
                    id=goal_id,
                    title=choice["title"],
                    goal=choice["goal"],
                    state="open",
                    scope=None,
                    sources=[],
                    fragments=[],
                    next_due_at=now,
                    expires_at=None,
                    result_refs=[],
                    actor_only=True,
                ),
                expected=0,
            )
            concern.update(actor_only=True, autonomy_basis=memory)
            if inspired:
                concern["public_exploration_topic"] = hint["topic"]
            self.store.put("life_concerns", concern)
        else:
            concern = self.life.concerns.get(actor["id"], goal_id)
            if concern["next_due_at"] is not None and concern["next_due_at"] > now:
                return
        if concern.get("autonomy_basis"):
            basis = copy.deepcopy(concern["autonomy_basis"])
            if memory and basis["role_version"] == memory["role_version"]:
                known = {canonical(item["ref"]) for item in basis["items"]}
                basis["items"].extend(
                    item for item in memory["items"] if canonical(item["ref"]) not in known
                )
            memory = basis
        activity_id = "goal-activity:" + digest([goal_id, actor.get("life_runtime_epoch", 0)])
        prior = self.store.get("life_activities", activity_id)
        if prior and prior["state"] in {"completed", "cancelled"}:
            return
        activity = self.life.activities.save(
            actor["id"],
            dict(
                id=activity_id,
                title=concern["title"],
                state="running",
                checkpoint=copy.deepcopy(prior["checkpoint"])
                if prior
                else dict(step=0, position=0, unit="step", note=concern["goal"]),
                next_due_at=now,
                resume_condition=choice["reason"],
                sources=[
                    dict(owner="companion", object_id=concern["id"], version=concern["version"])
                ],
                result_refs=copy.deepcopy((prior or {}).get("result_refs", [])),
                scope=None,
                actor_only=True,
                autonomous_goal_id=goal_id,
                autonomy_basis=memory,
            ),
            expected=(prior or {}).get("version", 0),
        )
        activity.update(autonomous_goal_id=goal_id, actor_only=True, autonomy_basis=memory)
        if concern.get("public_exploration_topic"):
            activity["public_exploration_topic"] = concern["public_exploration_topic"]
        self.store.put("life_activities", activity)
