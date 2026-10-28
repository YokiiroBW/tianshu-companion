"""Continuous actor activities. A calendar intention never certifies execution."""

import asyncio
import json

from .contracts import Fault, digest
from .life import text, timestamp
from .life_work import require_version, source_refs, visible

STATES = {"planned", "running", "paused", "completed", "cancelled"}


class Activities:
    def __init__(self, life):
        self.life, self.store = life, life.store
        self.executor = None
        self._active = set()

    def get(self, actor_id, activity_id):
        item = self.store.get("life_activities", activity_id)
        if not item or item["actor_id"] != actor_id:
            raise Fault("not_found")
        return item

    def current(self, actor_id, scope=None):
        actor = self.store.get("life_actors", actor_id)
        item = self.store.get("life_activities", (actor or {}).get("activity_id", ""))
        return item if item and visible(item, scope) else None

    def save(self, actor_id, value, *, expected):
        text(value["id"], 128)
        text(value["title"], 4000)
        if value["state"] not in STATES:
            raise Fault("invalid_input")
        source_refs(value["sources"])
        if value["scope"] is not None and value["scope"].get("actor_id") != actor_id:
            raise Fault("forbidden")
        if value["next_due_at"] is not None:
            timestamp(value["next_due_at"])
        checkpoint = value["checkpoint"]
        if (
            type(checkpoint["step"]) is not int
            or checkpoint["step"] < 0
            or checkpoint["position"] < 0
            or checkpoint["unit"] not in {"step", "characters", "seconds", "pages"}
        ):
            raise Fault("invalid_input")
        with self.store.transaction():
            actor = self.life._get("actors", actor_id)
            old = self.store.get("life_activities", value["id"])
            if old and old["actor_id"] != actor_id:
                raise Fault("not_found")
            require_version(old, expected)
            current = self.current(actor_id, actor.get("activity_scope"))
            if (
                value["state"] == "running"
                and current
                and current["id"] != value["id"]
                and current["state"] == "running"
            ):
                raise Fault("version_conflict", current_version=current["version"])
            item = dict(
                value,
                actor_id=actor_id,
                conversation_id=actor_id,
                version=expected + 1,
                sequence=int(self.life.clock()),
                deadline=value["next_due_at"],
                started_at=(old or {}).get("started_at", self.life.clock()),
                updated_at=self.life.clock(),
                runtime_epoch=actor.get("life_runtime_epoch", 0),
            )
            for field in ("autonomous_goal_id", "actor_only", "autonomy_basis"):
                if old and field in old:
                    item[field] = old[field]
            if old and old.get("step_operation"):
                uncertain = self.store.get("metadata", old["step_operation"])
                if uncertain and uncertain["state"] in {"unknown", "submitted", "tools_pending"}:
                    item["step_operation"] = old["step_operation"]
            self.store.put("life_activities", item)
            if value["state"] in {"running", "paused"}:
                actor.update(
                    activity_id=item["id"],
                    activity_scope=item["scope"],
                    activity="自主探索" if item.get("actor_only") else item["title"],
                    changed_at=self.life.clock(),
                )
                self.life._save("actors", actor)
            elif actor.get("activity_id") == item["id"]:
                actor.update(activity_id=None, activity_scope=None, cursor=None)
                self.life._save("actors", actor)
            # Only accepted changes to an actual process create an event boundary.
            if item["state"] != "planned":
                event = dict(
                    id="activity-event:" + digest([item["id"], item["version"]]),
                    world_id=actor["world_id"],
                    participants=[actor_id],
                    visible_to=[],
                    summary={
                        "running": "开始",
                        "paused": "暂时暂停",
                        "completed": "完成",
                        "cancelled": "取消",
                    }[item["state"]]
                    + item["title"],
                    occurred_at=self.life.clock(),
                    fictional=True,
                    kind="activity_boundary",
                    activity_id=item["id"],
                    activity_version=item["version"],
                    scope=item["scope"],
                    actor_only=item.get("actor_only", False),
                )
                self.life._event(event)
            return item

    def transition(self, actor_id, activity_id, state, *, expected, reason=None):
        item = self.get(actor_id, activity_id)
        allowed = {
            "paused": {"running"},
            "running": {"paused", "planned"},
            "cancelled": {"running", "paused", "planned"},
            "completed": {"running", "paused"},
        }
        if item["state"] not in allowed[state]:
            raise Fault("invalid_input")
        value = {
            key: item[key]
            for key in (
                "id",
                "title",
                "state",
                "checkpoint",
                "next_due_at",
                "resume_condition",
                "sources",
                "result_refs",
                "scope",
            )
        }
        value["state"] = state
        if state != "running":
            value["next_due_at"] = None
        elif value["next_due_at"] is None:
            value["next_due_at"] = self.life.clock()
        if reason:
            text(reason, 4000)
            value["resume_condition"] = reason
        return self.save(actor_id, value, expected=expected)

    def page(self, actor_id, *, scope=None, after=None, limit=20, object_id=None):
        rows = self.store.db.execute(
            "SELECT body FROM life_activities WHERE conversation_id=? AND ((? IS NULL AND id>?) OR id=?) ORDER BY id LIMIT ?",
            (actor_id, object_id, after or "", object_id, limit + 1),
        ).fetchall()
        values = [json.loads(row[0]) for row in rows]
        return (
            [item for item in values[:limit] if visible(item, scope)],
            values[limit - 1]["id"] if len(values) > limit else None,
        )

    def recover(self):
        """Keep checkpoints and original due points; do not manufacture offline steps."""
        for item in self.store.list("life_activities", states=["running"]):
            actor = self.store.get("life_actors", item["actor_id"])
            if actor and actor.get("activity_id") == item["id"]:
                item["unobserved_until"] = self.life.clock()
                self.store.put("life_activities", item)
                operation = item.get("step_operation") or "activity-step:" + digest(
                    [item["id"], item["version"]]
                )
                marker = self.store.get("metadata", operation)
                if marker and marker["state"] in {"submitted", "tools_pending"}:
                    self._interrupt(item, "interrupted_step")

    async def work(self, *, actor_id=None):
        if self.executor is None or not self.life.generation_available():
            return
        if actor_id is not None:
            actor = self.store.get("life_actors", actor_id)
            item = self.store.get("life_activities", (actor or {}).get("activity_id", ""))
            if (
                not item
                or item["state"] not in {"running", "paused"}
                or item.get("next_due_at") is None
                or item["next_due_at"] > self.life.clock()
            ):
                return
        else:
            row = self.store.db.execute(
                "SELECT body FROM life_activities WHERE status IN ('running','paused') AND deadline<=? ORDER BY deadline,id LIMIT 1",
                (self.life.clock(),),
            ).fetchone()
            if row is None:
                return
            item = json.loads(row[0])
        actor = self.store.get("life_actors", item["actor_id"])
        if (
            not actor
            or not actor.get("life_enabled", True)
            or actor.get("activity_id") != item["id"]
        ):
            return
        operation = item.get("step_operation") or "activity-step:" + digest(
            [item["id"], item["version"]]
        )
        if operation in self._active:
            return
        existing = self.store.get("metadata", operation)
        if existing and existing["state"] != "pending":
            # A submitted model/tool call might have executed. A new operation is not
            # proof that it did not; only an explicit checkpoint change can restart it.
            return
        if item["state"] == "paused" and not existing:
            item = self.transition(
                item["actor_id"], item["id"], "running", expected=item["version"]
            )
            operation = "activity-step:" + digest([item["id"], item["version"]])
            existing = None
        self._active.add(operation)
        try:
            if not existing:
                self.store.put(
                    "metadata",
                    dict(
                        id=operation,
                        state="pending",
                        activity_id=item["id"],
                        version=item["version"],
                        submitted_at=self.life.clock(),
                    ),
                )
            continuing = await self.executor(item, operation)
            if continuing is False:
                return True
            marker = self.store.get("metadata", operation)
            marker.update(state="completed", settled_at=self.life.clock())
            self.store.put("metadata", marker)
        except asyncio.CancelledError:
            marker = self.store.get("metadata", operation)
            if marker and marker["state"] != "pending":
                self._interrupt(item, "interrupted_step")
            raise
        except (Fault, ValueError, KeyError, OSError):
            self._interrupt(item, "step_unavailable")
        finally:
            self._active.remove(operation)
        return True

    def _interrupt(self, item, reason):
        current = self.get(item["actor_id"], item["id"])
        if current["state"] == "running" and current["version"] == item["version"]:
            self.transition(
                item["actor_id"], item["id"], "paused", expected=item["version"], reason=reason
            )
