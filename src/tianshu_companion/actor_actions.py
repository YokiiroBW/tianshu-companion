"""Action evidence from original local owners; never learn generated prose as fact."""

import json
import uuid

from .clients import utc
from .contracts import Fault, digest

SOURCE_PREFIX = "actor-memory-source:"
STATE_PREFIX = "actor-memory-state:"


class ActorActions:
    def __init__(self, memory):
        self.memory, self.core, self.store = memory, memory.core, memory.store

    def record(
        self,
        actor,
        table,
        item,
        outcome,
        summary,
        *,
        scope=None,
        fields=(),
        occurred_at=None,
        parents=(),
    ):
        if not self.memory.available:
            return
        observed = dict(
            table=table, object_id=item["id"], values={key: digest(item.get(key)) for key in fields}
        )
        if parents:
            observed["parents"] = [
                dict(
                    table=parent_table,
                    object_id=parent["id"],
                    values={key: digest(parent.get(key)) for key in parent_fields},
                )
                for parent_table, parent, parent_fields in parents
            ]
        source = dict(
            producer="companion",
            object_id="action:" + digest([actor, table, item["id"]]),
            revision=digest(observed),
            content_hash=digest(observed),
        )
        key = SOURCE_PREFIX + digest(source)
        if self.store.get("metadata", key):
            return
        constraints = (
            dict(kind="actor_only")
            if scope is None
            else dict(
                kind="group" if scope["audience"] == "group" else "private",
                conversation_id=scope["conversation_id"],
            )
        )
        if scope is not None and scope["audience"] != "group":
            constraints["person_id"] = scope["person_id"]
        state = self.store.get("metadata", STATE_PREFIX + actor) or dict(
            id=STATE_PREFIX + actor, generation=uuid.uuid4().hex, sequence=0
        )
        state["sequence"] += 1
        now = utc(self.core.clock())
        fact = dict(
            actor_id=actor,
            source=source,
            relation="participant",
            provenance="digital_action",
            occurred_at=utc(occurred_at)
            if isinstance(occurred_at, (int, float))
            else occurred_at or now,
            learned_at=now,
            outcome=outcome,
            summary=summary,
            source_generation=state["generation"],
            source_sequence=state["sequence"],
            access_version=digest([constraints, observed]),
            source_constraints=constraints,
            content_reads=[],
        )
        self.core.contracts.check("actor-self-memory#source_fact", fact)
        self.store.put("metadata", state)
        self.store.put(
            "metadata",
            dict(
                id=key,
                actor_id=actor,
                source=source,
                state="current",
                fact=fact,
                action_owner=observed,
            ),
        )
        self.store.put(
            "outbox",
            dict(
                id="actor-memory-outbox:" + digest(source),
                conversation_id=actor,
                sequence=state["sequence"],
                state="actor_memory_pending",
                deadline=self.core.clock(),
                attempts=0,
                actor_id=actor,
                source=source,
                event=dict(kind="actor_action_observed", source=source, actor_id=actor),
            ),
        )

    def check_local(self, row):
        proof = row["action_owner"]
        owners = []
        for owner in [proof, *proof.get("parents", [])]:
            current = self.store.get(owner["table"], owner["object_id"])
            if current is None:
                raise Fault("not_found")
            if any(digest(current.get(key)) != value for key, value in owner["values"].items()):
                raise Fault("scope_changed")
            if owner["table"] == "life_activities" and current.get("completion_evidence"):
                evidence = current["completion_evidence"]
                receipt = self.store.get("metadata", evidence["receipt_id"])
                if not receipt or digest(receipt["result"]) != evidence["result_digest"]:
                    raise Fault("scope_changed")
            owners.append(current)
        return owners

    async def check(self, row):
        for current in self.check_local(row):
            if current.get("autonomy_basis"):
                await self.memory.check(current["autonomy_basis"])
        self.check_local(row)

    def activity(self, item):
        if item["state"] != "completed":
            return
        marker = self.store.get("metadata", item.get("step_operation", ""))
        if not item.get("completion_evidence") and (not marker or marker["state"] != "completed"):
            return
        self.record(
            item["actor_id"],
            "life_activities",
            item,
            "observed",
            "系统活动流程“"
            + item["title"][:1000]
            + "”已记录完成；活动名是目标描述，仅证明流程状态，不证明未核验的外部动作或内容理解。",
            scope=item["scope"],
            fields=(
                "actor_id",
                "title",
                "state",
                "checkpoint",
                "scope",
                "autonomy_basis",
                "completion_evidence",
            ),
            occurred_at=item["updated_at"],
        )

    def writing(self, work, revision):
        if revision["source"].get("kind") != "gateway":
            return
        self.record(
            work["actor_id"],
            "write_revisions",
            revision,
            "completed",
            "为作品《"
            + work["title"]
            + "》生成并保存一个章节修订；作品名与正文是创作，不是现实经历。",
            scope=work.get("scope"),
            fields=("chapter_id", "content", "source"),
            occurred_at=revision["created_at"],
            parents=[("write_works", work, ("actor_id", "scope", "title"))],
        )

    def image(self, item):
        if item["state"] not in {"completed", "failed", "unknown", "cancelled"}:
            return
        if item["state"] == "completed" and not item.get("artifacts"):
            return
        actor = item["conversation_id"]
        self.record(
            actor,
            "image_jobs",
            item,
            item["state"],
            "图片生成任务结果："
            + item["state"]
            + (
                "，已生成" + str(len(item["artifacts"])) + "张图片"
                if item["state"] == "completed"
                else ""
            )
            + "；仅记录生成动作，图片不是现实事件证据。",
            scope=item.get("scope"),
            fields=("conversation_id", "state", "artifacts", "scope"),
            occurred_at=item.get("completed_at") or item.get("created_at"),
        )

    def contact(self, item):
        if not item.get("confirmed_sent"):
            return
        self.record(
            item["actor_id"],
            "delivery_contacts",
            item,
            "completed",
            "渠道回执确认至少一个消息片段已发送；不表示整条表达全部送达或对方已阅读。",
            scope=item["scope"],
            fields=("actor_id", "expression_id", "scope", "confirmed_sent"),
            occurred_at=item["observed_at"],
        )

    def delivery_result(self, table, item, scope, receipt, *, field):
        state = receipt["state"]
        if state not in {"failed", "unknown", "cancelled"}:
            return
        self.record(
            scope["actor_id"],
            table,
            item,
            state,
            "消息发送回执结果：" + state + "；未确认送达，不表示对方收到或阅读。",
            scope=scope,
            fields=(field,),
            occurred_at=receipt.get("observed_at"),
        )

    def import_history(self, actor, value, request_id):
        self.memory._local_actor(actor)
        if not self.memory.available:
            raise Fault("dependency_unavailable")
        owner, after, limit = value["owner"], value["after"] or "", value["limit"]
        if owner == "writing":
            rows = self.store.db.execute(
                "SELECT r.body,w.body FROM write_revisions r JOIN write_chapters c ON r.conversation_id=c.id JOIN write_works w ON c.conversation_id=w.id WHERE w.conversation_id=? AND r.id>? ORDER BY r.id LIMIT ?",
                (actor, after, limit + 1),
            ).fetchall()
        else:
            table = {
                "activities": "life_activities",
                "images": "image_jobs",
                "contacts": "delivery_contacts",
            }[owner]
            actor_column = (
                "json_extract(body,'$.actor_id')" if owner == "contacts" else "conversation_id"
            )
            rows = self.store.db.execute(
                f"SELECT body FROM {table} WHERE {actor_column}=? AND id>? ORDER BY id LIMIT ?",
                (actor, after, limit + 1),
            ).fetchall()
        imported = 0
        with self.store.transaction():
            for row in rows[:limit]:
                item = json.loads(row[0])
                before = self.store.get("metadata", STATE_PREFIX + actor)
                if owner == "writing":
                    self.writing(json.loads(row[1]), item)
                elif owner == "activities":
                    self.activity(item)
                elif owner == "images":
                    self.image(item)
                else:
                    self.contact(item)
                imported += self.store.get("metadata", STATE_PREFIX + actor) != before
        return dict(
            id="actor-import:" + digest([actor, request_id]),
            version=1,
            state="completed",
            operation_ref=request_id,
            next_cursor=json.loads(rows[limit - 1][0])["id"] if len(rows) > limit else None,
            imported=imported,
            skipped=min(len(rows), limit) - imported,
            complete=len(rows) <= limit,
        )
