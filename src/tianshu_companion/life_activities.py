"""Continuous actor activities. A calendar intention never certifies execution."""

import json
import asyncio

from .contracts import Fault, digest
from .life import text, timestamp
from .life_work import require_version, source_refs, visible

STATES = {"planned", "running", "paused", "completed", "cancelled"}


class Activities:
    def __init__(self, life):
        self.life, self.store = life, life.store
        self.executor = None
        self.lock = asyncio.Lock()

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
            self.store.put("life_activities", item)
            if value["state"] in {"running", "paused"}:
                actor.update(
                    activity_id=item["id"],
                    activity_scope=item["scope"],
                    activity=item["title"],
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
                operation = "activity-step:" + digest([item["id"], item["version"]])
                marker = self.store.get("metadata", operation)
                if marker and marker["state"] == "submitted":
                    self._interrupt(item, "interrupted_step")

    async def work(self):
        if self.lock.locked() or self.executor is None or not self.life.generation_available():
            return
        row = self.store.db.execute(
            "SELECT body FROM life_activities WHERE status='running' AND deadline<=? ORDER BY deadline,id LIMIT 1",
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
        async with self.lock:
            operation = "activity-step:" + digest([item["id"], item["version"]])
            existing = self.store.get("metadata", operation)
            if existing:
                return  # Interrupted/unknown execution is never silently re-issued.
            self.store.put(
                "metadata",
                dict(
                    id=operation,
                    state="submitted",
                    activity_id=item["id"],
                    version=item["version"],
                    submitted_at=self.life.clock(),
                ),
            )
            try:
                await self.executor(item, operation)
                self.store.put(
                    "metadata",
                    dict(
                        id=operation,
                        state="completed",
                        activity_id=item["id"],
                        version=item["version"],
                        settled_at=self.life.clock(),
                    ),
                )
            except asyncio.CancelledError:
                self._interrupt(item, "interrupted_step")
                raise
            except (Fault, ValueError, KeyError, OSError):
                self._interrupt(item, "step_unavailable")

    def _interrupt(self, item, reason):
        current = self.get(item["actor_id"], item["id"])
        if current["state"] == "running" and current["version"] == item["version"]:
            self.transition(
                item["actor_id"], item["id"], "paused", expected=item["version"], reason=reason
            )
