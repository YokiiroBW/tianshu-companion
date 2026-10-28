"""Contextual background learning and receipt-first recovery of retained fact jobs."""

import asyncio
import copy
import json

from .contracts import Fault, canonical, digest
from .model_selection import SelectionRequest, pin_generation
from .memory_extraction import ProposalStructureError, parse_proposal, legacy_messages
from .short_context import sources_current
from .memory_segments import MemorySegments
from . import observability as obs

QUEUE = "memory:learning"
MODEL_ROUTE_BOUNDARY = "memory-learning-generation-route-boundary"
MAX_ATTEMPTS = 3
MAX_RECOVERY_ATTEMPTS = 6
MAX_TOTAL_ATTEMPTS = 3 * MAX_RECOVERY_ATTEMPTS
MODEL_TIMEOUT_SECONDS = 30
MUTATION_WAIT = {"capture_wait", "commit_wait"}


def memory_scopes_overlap(left, right):
    """Automatic writes stay in their source audience and conversation.

    Explicit private associations can cross people/conversations; keep their
    barrier conservative until Memory's complete association snapshot is known.
    Group profiles and history may include other people in the same room.
    """
    return left["actor_id"] == right["actor_id"] and (
        left["conversation_id"] == right["conversation_id"]
        or left["audience"] == right["audience"] == "self_private"
    )


class MemoryLearning:
    def __init__(self, core):
        self.core = core
        self.dispatch_lock = asyncio.Lock()
        self.lock = asyncio.Lock()
        self.jobs = {}
        self.closed = False
        self._phase = None
        self._family = 0
        self._foreground_finished = 0
        self.mutation_scopes = {}
        self.segments = MemorySegments(core, QUEUE)
        boundary = core.store.get("metadata", "memory-learning-schema5-boundary")
        if boundary is None:
            boundary = dict(
                id="memory-learning-schema5-boundary",
                outbox_rowid=core.store.db.execute(
                    "SELECT COALESCE(MAX(rowid),0) FROM outbox"
                ).fetchone()[0],
            )
            core.store.put("metadata", boundary)
        self.legacy_outbox_rowid = boundary["outbox_rowid"]
        model_boundary = core.store.get("metadata", MODEL_ROUTE_BOUNDARY)
        if model_boundary is None:
            model_boundary = dict(
                id=MODEL_ROUTE_BOUNDARY,
                metadata_rowid=core.store.db.execute(
                    "SELECT COALESCE(MAX(rowid),0) FROM metadata"
                ).fetchone()[0],
            )
            core.store.put("metadata", model_boundary)
        self.legacy_model_rowid = model_boundary["metadata_rowid"]

    def admission_blocked(self, scope):
        if not self.core.automatic_memory_candidates:
            return False
        if any(memory_scopes_overlap(active, scope) for active in self.mutation_scopes.values()):
            return True
        # Persisted barriers survive restart. Waiting in one room must neither
        # hold the commit lock nor prevent extraction in independent rooms.
        return any(
            memory_scopes_overlap(item["scope"], scope)
            and not self.core.group_processing.paused(item)
            for item in (
                json.loads(row[0])
                for row in self.core.store.db.execute(
                    "SELECT body FROM metadata WHERE conversation_id=? AND status='pending' "
                    "AND json_extract(body,'$.stage') IN ('capture_wait','commit_wait')",
                    (QUEUE,),
                )
            )
        )

    def queue(self, item):
        receipt = item.get("receipt") or {}
        if "materials" in receipt:
            self._offer_materials(
                receipt,
                context_windows=item.get("material_request", {}).get("context_windows", []),
                processing_frames=self.core.group_processing.frames(item) or [],
            )
            return
        job = receipt.get("candidate_job_ref")
        if not job:
            return
        key = "memory-learn:" + digest(job)
        if self.core.store.get("metadata", key):
            return
        schema = getattr(self.core.memory, "extraction_schema", 5)
        if schema == 6:
            # This is an original singleton acceptance, not a new segment.
            schema = 5
        row = self.core.store.db.execute(
            "SELECT rowid FROM outbox WHERE id=?", (item["id"],)
        ).fetchone()
        if schema == 5 and row is not None and row[0] <= self.legacy_outbox_rowid:
            # Before schema5 activation Memory may already have locked this
            # retained event to3/4. Use the original explicit negotiation once;
            # new events never pay this remote compatibility step.
            schema = 4
        self.core.store.put(
            "metadata",
            dict(
                id=key,
                conversation_id=QUEUE,
                state="pending",
                deadline=0,
                attempts=0,
                recovery_failures={},
                turn_id=item["event"]["aggregate_id"],
                input_revision=item["event"]["input_revision"],
                job_id=job,
                output=None,
                receipt=None,
                proposal_failures=0,
                scope=item["event"]["scope"],
                scope_key=digest(item["event"]["scope"]),
                sequence=item["event"]["turn_sequence"],
                extraction_schema=schema,
            ),
        )

    def _offer_materials(self, response, *, context_windows=(), processing_frames=()):
        grouped = {}
        for material in response["materials"]:
            frame = material["frame"]
            key = canonical(frame)
            if key not in grouped:
                grouped[key] = (frame, [])
            grouped[key][1].append(
                {name: value for name, value in material.items() if name != "frame"}
            )
        with self.core.store.transaction():
            for frame, materials in grouped.values():
                windows = [
                    window
                    for window in context_windows
                    if all(window["manifest"]["scope"][key] == frame[key] for key in frame)
                ]
                self.segments.offer(
                    frame,
                    materials,
                    context_windows=windows,
                    omitted_context=not windows,
                    processing_frames=[
                        dict(value, actor_id=frame["actor_id"]) for value in processing_frames
                    ],
                )

    async def observation_archived(self, request, response):
        """Persist the archive handoff without waiting for semantic processing."""
        if getattr(self.core.memory, "extraction_schema", 5) != 6:
            return
        key = "memory-observation:" + digest(request["source_ref"])
        with self.core.store.transaction():
            if self.core.store.get("metadata", key) is None:
                self.core.store.put(
                    "metadata",
                    dict(
                        id=key,
                        conversation_id=QUEUE,
                        state="material_pending",
                        deadline=0,
                        attempts=0,
                        source_ref=request["source_ref"],
                    ),
                )

    async def _flush_observation_material(self, item):
        try:
            response = await self.core.memory.accept_materials(
                dict(schema_version=1, observation_refs=[item["source_ref"]])
            )
            with self.core.store.transaction():
                self._offer_materials(
                    response, processing_frames=self.core.group_processing.frames(item) or []
                )
                item.update(state="material_accepted", deadline=0, last_error=None)
                self.core.store.put("metadata", item)
        except (Fault, OSError, TimeoutError) as error:
            item["attempts"] += 1
            code = error.code if isinstance(error, Fault) else "dependency_unavailable"
            terminal = (
                isinstance(error, Fault)
                and not error.unknown
                and code
                in {
                    "forbidden",
                    "unauthorized",
                    "scope_changed",
                    "not_found",
                    "invalid_input",
                    "idempotency_conflict",
                }
            )
            item.update(
                state="material_rejected" if terminal else "material_pending",
                deadline=0
                if terminal
                else self.core.clock() + min(300, 2 ** min(item["attempts"], 8)),
                last_error=code,
            )
            self.core.store.put("metadata", item)
        return True

    def _order_pending(self):
        """Upgrade retained jobs in bounded pages without changing retry evidence."""
        if getattr(self, "_ordered", False):
            return False
        store = self.core.store
        rows = store.db.execute(
            "SELECT body FROM metadata WHERE conversation_id=? AND status='pending' "
            "AND json_extract(body,'$.scope_key') IS NULL LIMIT 64",
            (QUEUE,),
        ).fetchall()
        with store.transaction():
            for row in rows:
                item = json.loads(row[0])
                turn = store.get("turns", item["turn_id"])
                # A missing source is still handled by the existing forbidden path.
                scope = turn["scope"] if turn else {"missing_turn": item["turn_id"]}
                item.update(
                    scope=scope, scope_key=digest(scope), sequence=turn["sequence"] if turn else 0
                )
                store.put("metadata", item)
        self._ordered = len(rows) < 64
        return bool(rows)

    def _next(self, *, retry_only=False, drain_only=False):
        # A delayed extraction must not be overtaken by its own correction:
        # Memory's known_facts would otherwise omit the original. Other scopes
        # can progress during its cooldown. Both predecessor probes are indexed.
        store = self.core.store
        # New jobs have deadline=0. An endless stream of them must not keep an
        # already-due retry at the end of the queue. Grant it at least every
        # fourth selection, still subject to the same scope predecessors.
        if not retry_only and getattr(self, "_fresh_grants", 0) >= 3:
            retry = self._next(retry_only=True, drain_only=drain_only)
            if retry is not None:
                return retry
        cursor_name = (
            ("_drain_retry_cursor" if retry_only else "_drain_cursor")
            if drain_only
            else ("_retry_cursor" if retry_only else "_due_cursor")
        )
        cursor = getattr(self, cursor_name, (-1, -1, ""))
        retry_clause = "AND deadline>0 " if retry_only else ""
        drain_clause = (
            "AND json_extract(body,'$.stage') IN ('capture_wait','commit_wait') "
            if drain_only
            else ""
        )
        rows = store.db.execute(
            "SELECT deadline,position,id FROM metadata WHERE conversation_id=? "
            "AND status='pending' AND deadline<=? "
            + retry_clause
            + drain_clause
            + "AND (deadline,position,id)>(?,?,?) "
            "ORDER BY deadline,position,id LIMIT 64",
            (QUEUE, self.core.clock(), *cursor),
        ).fetchall()
        # One bounded page per tick; an idle page wraps on the next tick. Do not
        # let many followers of a cooling-down source monopolize a queue scan.
        setattr(self, cursor_name, tuple(rows[-1]) if len(rows) == 64 else (-1, -1, ""))
        for deadline, position, key in rows:
            if key in self.jobs and not self.jobs[key].done():
                continue
            row = store.db.execute(
                "SELECT m.body FROM metadata m WHERE m.id=? AND NOT EXISTS ("
                "SELECT 1 FROM metadata p WHERE p.conversation_id=m.conversation_id "
                "AND p.status='pending' "
                "AND json_extract(p.body,'$.scope_key')=json_extract(m.body,'$.scope_key') "
                "AND (p.position,p.id)<(m.position,m.id)) AND NOT EXISTS ("
                "SELECT 1 FROM outbox o WHERE o.conversation_id=json_extract(m.body,'$.scope.conversation_id') "
                "AND o.status IN ('pending','unknown','submitting','blocked_scope') "
                "AND json_extract(o.body,'$.event.scope.actor_id')=json_extract(m.body,'$.scope.actor_id') "
                "AND json_extract(o.body,'$.event.scope.person_id')=json_extract(m.body,'$.scope.person_id') "
                "AND json_extract(o.body,'$.event.scope.audience')=json_extract(m.body,'$.scope.audience') "
                "AND o.position<m.position)",
                (key,),
            ).fetchone()
            if row:
                item = json.loads(row[0])
                if self.core.group_processing.paused(item):
                    continue
                if (
                    item.get("stage") not in MUTATION_WAIT
                    and self._candidate_mutates(item, None)
                    and self.core.has_active_dialogue(item["scope"])
                ):
                    continue
                if item.get("stage") in MUTATION_WAIT:
                    try:
                        self._source_turn(item)
                    except Fault as error:
                        self._failed(item, error.code, error)
                        store.put("metadata", item)
                        self.core.counted("memory.work")
                        continue
                    if self.core.has_active_dialogue(item["scope"]):
                        continue
                # Resume after the selected job rather than skipping the rest
                # of its page. Scope ordering is independent of this fair cursor.
                setattr(
                    self,
                    cursor_name,
                    (deadline, position, key) if len(rows) == 64 else (-1, -1, ""),
                )
                self._fresh_grants = (
                    0 if deadline > 0 else min(3, getattr(self, "_fresh_grants", 0) + 1)
                )
                return item
        return None

    def backfill(self):
        """Bounded one-time catch-up of already delivered candidates, without scanning bodies."""
        store = self.core.store
        cursor = store.get("metadata", "memory-learning-backfill") or {
            "id": "memory-learning-backfill",
            "cursor": 0,
        }
        rows = store.db.execute(
            "SELECT rowid,body FROM outbox WHERE rowid>? ORDER BY rowid LIMIT 64",
            (cursor["cursor"],),
        ).fetchall()
        with store.transaction():
            for row in rows:
                item = json.loads(row[1])
                if item["state"] == "delivered":
                    self.queue(item)
                cursor["cursor"] = row[0]
            if rows:
                store.put("metadata", cursor)
        return bool(rows)

    def dialogue_status(self, turn):
        """Expose automatic processing separately from the model's explicit write skill."""
        core = self.core
        enabled = (
            core.automatic_memory_candidates
            and core._turn_allows(turn, "memory.write")
            and hasattr(core.memory, "process_candidate")
        )
        preparation = turn.get("preparation")
        recall = core._turn_allows(turn, "memory.read") and preparation is not None
        return {
            "automatic_extraction_after_delivery": "enabled" if enabled else "disabled",
            "automatic_learning_mode": (
                "conversation_segment_multi_person"
                if getattr(core.memory, "extraction_schema", 3) == 6
                else "context_window_summary_and_optional_portrait"
                if getattr(core.memory, "extraction_schema", 3) == 5
                else "retained_fact_extraction"
            ),
            "automatic_recall_before_reply": "completed" if recall else "unavailable",
            "recalled_units": len(preparation.get("selected_units", [])) if recall else 0,
            "recall_coverage": "query_relevant_visible_scope_only",
            "proves_global_absence": False,
        }

    def dialogue_context(self, turn):
        return (
            "\nTrusted memory processing: "
            + canonical(self.dialogue_status(turn))
            + "\n技能目录只描述显式工具，自动召回和回复后的自动提取是独立路径。"
            "召回已完成但结果为空，表示本次没有找到相关资料，不表示系统不能检索。"
            "无论本次结果为空或只含现名，都不能据此断言从未说过、没有保存、旧名没有记录。"
            "本次查询仅覆盖当前可见范围中的相关项；没有拿到的信息就说这次没查到，"
            "不要推断其他范围有无记录，也不要主动对未被询问的信息作否定结论。"
            "自动提取启用时，不要因为显式记忆工具不可用就声称无法保存或更正；"
            "后台是否已经保存仍需实际回执，启用状态不是保存凭证。"
            "通常直接依据相关记录回答；仅在用户询问来源、保存情况或不确定性影响答案时"
            "解释这些边界。自动整理的 inferred 标签不是对明确原话的怀疑；"
            "姓名、地点和偏好有直接来源时，按‘你提到的是……’回答即可，"
            "不要为这个标签附加‘只是推断，请再确认’或主动建议再保存一次。"
            "只保留原话本身的条件和歧义，不在普通回忆回答中复述内部编号、版本或分类。"
            "来源提取不是亲眼核实，也不等于作者从未说过；不能把摘要冒充逐字原文。"
        )

    async def reconcile(self, job_id):
        """Explicit receipt-only recovery. Never regenerate or resend a bounded-out proposal."""
        async with self.dispatch_lock:
            key = self._learning_key(job_id)
            running = self.jobs.pop(key, None)
            if running is not None:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
            return await self._reconcile(job_id, key)

    def _learning_key(self, job_id):
        key = "memory-learn:" + digest(job_id)
        if self.core.store.get("metadata", key) is not None:
            return key
        index = self.core.store.get("metadata", "memory-job:" + digest(job_id))
        return index["learning_id"] if index is not None else key

    async def _reconcile(self, job_id, key):
        item = self.core.store.get("metadata", key)
        if not item:
            raise Fault("not_found")
        if item.get("extraction_schema") == 6:
            if not item.get("job_id"):
                raise Fault("result_unknown", unknown=True)
            response = await self._candidate(item, None, reconcile=True)
            if response["state"] in {"completed", "invalidated"}:
                self._finish_segment(item, response)
            return response
        turn = self.core.store.get("turns", item["turn_id"])
        if not turn or not self.core._turn_allows(turn, "memory.write"):
            raise Fault("forbidden")
        self.core._check_role(turn)
        response = await self._candidate(item, None, reconcile=True)
        if item.get("extraction_schema") == 5:
            if response["state"] in {"completed", "invalidated"}:
                self._finish_summary(item, response)
            return response
        if response["state"] in {"completed", "invalidated"}:
            item.update(
                state=response["state"],
                receipt=response["result"],
                last_error=None,
                output=None,
                deadline=0,
            )
            self.core.store.put("metadata", item)
        return response

    def _automatic_due(self):
        store, now = self.core.store, self.core.clock()
        from .actor_memory import OUTBOX_PENDING

        actor_due = self.core.actor_memory.available and store.due(
            "outbox", OUTBOX_PENDING, now, limit=1
        )
        participation_due = self.core.participation.due()
        observer_due = self.core.actor_observation.due()
        if not self.core.automatic_memory_candidates:
            return bool(actor_due or participation_due or observer_due)
        return bool(
            actor_due
            or participation_due
            or observer_due
            or store.db.execute(
                "SELECT 1 FROM metadata WHERE conversation_id=? "
                "AND status IN ('pending','material_pending') AND deadline<=? LIMIT 1",
                (QUEUE, now),
            ).fetchone()
            or store.due(
                "outbox",
                ["blocked_scope", "pending", "unknown", "relationship_pending"],
                now,
                limit=1,
            )
        )

    def foreground_finished(self):
        """A real foreground completion can grant one old automatic phase after three.

        Credit is bounded, and idle foreground traffic cannot bank future work.
        This counter belongs to automatic I/O, independently of model-slot fairness.
        """
        if (
            not self.core.automatic_memory_candidates
            and not self.core.actor_memory.available
            and not self.core.participation.available
            and not self.core.actor_observation.available
        ):
            return
        self._foreground_finished = (
            min(3, self._foreground_finished + 1) if self._automatic_due() else 0
        )

    def _unblocks_foreground(self, family, item):
        if family == "learning":
            return item.get("stage") in MUTATION_WAIT and not self.core.group_processing.paused(
                item
            )
        if family != "outbox":
            return False
        scope = item["event"]["scope"]
        return any(
            memory_scopes_overlap(json.loads(row[0]), scope)
            for row in self.core.store.db.execute(
                "SELECT CAST(json_extract(body,'$.scope') AS TEXT) FROM metadata "
                "WHERE conversation_id=? AND status='pending' "
                "AND json_extract(body,'$.stage') IN ('capture_wait','commit_wait')",
                (QUEUE,),
            )
        )

    def _start_phase(self, item=None):
        """Admit actual work, never spend a retry budget on ordinary queue waiting."""
        phase = self._phase
        # Explicit receipt reconciliation and direct internal callers retain their
        # existing semantics. Only the automatic dispatcher owns this decision.
        if phase is None or (item is not None and phase["item"]["id"] != item["id"]):
            return True
        observer = (
            phase["family"] == "actor_observation"
            or phase["family"] == "actor_memory"
            and phase["item"].get("observer_task_id")
        )
        if (
            phase["family"] != "participation"
            and not observer
            and self.core.group_processing.paused(phase["item"])
        ):
            return False
        if phase["started"]:
            return True
        if not self._unblocks_foreground(phase["family"], phase["item"]):
            if self.core.automatic_memory_foreground_demand():
                if self._foreground_finished < 3:
                    return False
                self._foreground_finished = 0
            else:
                # A quiet opportunity must not also bank an extra busy grant.
                self._foreground_finished = 0
        phase["started"] = True
        obs.emit(self.core.events, "memory.phase.started", "started")
        return True

    def _phase_page(self, family, states):
        """Rotate bounded paused pages without overtaking an original room predecessor."""
        core = self.core
        cursor_name = "_phase_cursor_" + family
        cursor = getattr(self, cursor_name, (-1, ""))
        table = "metadata" if family in {"observation", "processing_learning"} else "outbox"
        predicate = "conversation_id=? AND " if table == "metadata" else ""
        args = [QUEUE] if table == "metadata" else []
        predecessors = (
            "('pending','unknown','submitting','blocked_scope','relationship_pending','relationship_submitting') "
            if family == "relationship"
            else "('pending','unknown','submitting','blocked_scope') "
        )
        predecessor = (
            "AND NOT EXISTS (SELECT 1 FROM outbox p WHERE p.conversation_id=outbox.conversation_id "
            "AND p.status IN "
            + predecessors
            + "AND json_extract(p.body,'$.event.scope.actor_id')=json_extract(outbox.body,'$.event.scope.actor_id') "
            "AND (p.position,p.id)<(outbox.position,outbox.id)) "
            if table == "outbox"
            else ""
        )
        rows = core.store.db.execute(
            f"SELECT id,deadline FROM {table} WHERE "
            + predicate
            + "status IN ("
            + ",".join("?" for _ in states)
            + ") AND deadline<=? "
            + "AND (deadline,id)>(?,?) "
            + predecessor
            + "ORDER BY deadline,id LIMIT 64",
            (*args, *states, core.clock(), *cursor),
        ).fetchall()
        setattr(self, cursor_name, (rows[-1][1], rows[-1][0]) if len(rows) == 64 else (-1, ""))
        items = core.store.get_many(table, [row[0] for row in rows])
        return [items[row[0]] for row in rows]

    def _select_phase(self, *, outbox_only):
        core, now = self.core, self.core.clock()
        if core.automatic_memory_candidates and not outbox_only and self._ordered:
            drain = self._next(drain_only=True)
            if drain is not None:
                # These real persisted exclusion barriers are holding queued
                # foreground work. Finish their original recovery first.
                return dict(family="learning", item=drain, started=False)
        families = (
            ("outbox", "relationship")
            if outbox_only
            else ("outbox", "observation", "learning", "relationship")
        )
        if not core.automatic_memory_candidates:
            families = ()
        if core.actor_memory.available:
            families += ("actor_memory",)
        if not outbox_only and core.participation.available:
            families += ("participation",)
        if not outbox_only and core.actor_observation.available:
            families += ("actor_observation",)
        if not families:
            return None
        busy = core.automatic_memory_foreground_demand()
        for offset in range(len(families)):
            index = (self._family + offset) % len(families)
            family = families[index]
            if family == "participation":
                item = dict(id="participation:poll") if core.participation.due() else None
            elif family == "actor_observation":
                item = core.actor_observation.select()
            elif family == "actor_memory":
                from .actor_memory import OUTBOX_PENDING

                rows = core.store.due("outbox", OUTBOX_PENDING, now, limit=1)
                item = rows[0] if rows else None
            elif family in {"outbox", "relationship"}:
                if family == "relationship" and core.relationships is None:
                    continue
                states = (
                    ["relationship_pending"]
                    if family == "relationship"
                    else ["blocked_scope", "pending", "unknown"]
                )
                rows = self._phase_page(family, states)
                item = (
                    next((row for row in rows if not core.group_processing.paused(row)), None)
                    if family == "relationship"
                    else next(
                        (
                            row
                            for row in rows
                            if core._turn_allows(
                                core.store.get("turns", row["event"]["aggregate_id"]),
                                "memory.write",
                            )
                            and not core.group_processing.paused(row)
                            and (
                                not busy
                                or self._foreground_finished >= 3
                                or self._unblocks_foreground(family, row)
                            )
                        ),
                        None,
                    )
                )
            elif family == "observation":
                if getattr(core.memory, "extraction_schema", 5) != 6:
                    continue
                item = next(
                    (
                        row
                        for row in self._phase_page("observation", ["material_pending"])
                        if not core.group_processing.paused(row)
                    ),
                    None,
                )
            else:
                item = (
                    self._next(drain_only=busy and self._foreground_finished < 3)
                    if self._ordered and hasattr(core.memory, "process_candidate")
                    else None
                )
            if item is None:
                continue
            if (
                busy
                and self._foreground_finished < 3
                and not self._unblocks_foreground(family, item)
            ):
                continue
            self._family = (index + 1) % len(families)
            return dict(family=family, item=item, started=False)
        return None

    def _phase_outcome(self, phase, failure):
        family = phase["family"]
        if family == "participation":
            item = phase["item"]
        else:
            table = (
                "metadata"
                if family in {"learning", "observation", "actor_observation"}
                else "outbox"
            )
            try:
                # A family can re-read and update its own JSON object. The
                # selected object is not proof of the durable phase result.
                item = self.core.store.get(table, phase["item"]["id"])
            except Exception:
                # Observation must not turn a settled write into a retry or
                # hide the original exception if the diagnostic read fails.
                return "unknown"
            if item is None:
                return "unknown"
        terminal = {
            "learning": {"completed": "succeeded", "invalidated": "cancelled"},
            "observation": {"material_accepted": "succeeded", "material_rejected": "cancelled"},
            "outbox": {
                "delivered": "succeeded",
                "discarded_source": "cancelled",
                "unknown": "unknown",
                "submitting": "unknown",
            },
            "relationship": {
                "delivered": "succeeded",
                "relationship_rejected": "cancelled",
                "relationship_unknown": "unknown",
                "relationship_submitting": "unknown",
            },
            "actor_memory": {
                "actor_memory_delivered": "succeeded",
                "actor_memory_invalidated": "cancelled",
                "actor_memory_unknown": "unknown",
                "actor_memory_submitting": "unknown",
            },
            "actor_observation": {
                "completed": "succeeded",
                "ignored": "succeeded",
                "invalidated": "cancelled",
                "unknown": "unknown",
            },
        }.get(family, {})
        state = item.get("state")
        if state in terminal:
            return terminal[state]
        if isinstance(failure, asyncio.CancelledError):
            return "cancelled"
        if (
            failure is not None
            or state == "blocked"
            or item.get("last_error")
            or item.get("relationship_error")
        ):
            return "failed"
        # A completed local phase can leave a real job pending. It is progress,
        # not proof of a committed memory, relationship settlement or send.
        return "degraded"

    async def _run_phase(self, phase):
        self._phase = phase
        core, item = self.core, phase["item"]
        failure = None
        try:
            observer = (
                phase["family"] == "actor_observation"
                or phase["family"] == "actor_memory"
                and item.get("observer_task_id")
            )
            if (
                phase["family"] != "participation"
                and not observer
                and not await core.group_processing.start(item)
            ):
                return
            if phase["family"] == "learning":
                await self._work(item)
            elif self._start_phase():
                if phase["family"] == "participation":
                    await core.participation.work()
                elif phase["family"] == "actor_observation":
                    await core.actor_observation.work(item)
                elif phase["family"] == "observation":
                    await self._flush_observation_material(item)
                else:
                    async with core._outbox_lock:
                        if phase["family"] == "outbox":
                            await core._flush_outbox(item)
                        elif phase["family"] == "actor_memory":
                            fresh = core.store.get("outbox", item["id"])
                            from .actor_memory import OUTBOX_PENDING

                            if fresh["state"] in OUTBOX_PENDING:
                                await core.actor_memory.flush(fresh)
                        else:
                            await core.relationships.flush(core, selected=item)
                core.counted("memory.work")
        except BaseException as error:
            failure = error
            raise
        finally:
            if phase["started"]:
                obs.emit(core.events, "memory.phase.finished", self._phase_outcome(phase, failure))
            self._phase = None
            if not self._automatic_due():
                self._foreground_finished = 0

    async def work(self, *, wait=True, outbox_only=False):
        """One automatic phase at a time, selected from the existing durable queues."""
        core = self.core
        async with self.dispatch_lock:
            if self.closed:
                return
            for key, job in list(self.jobs.items()):
                if job.done():
                    del self.jobs[key]
                    if not job.cancelled():
                        job.result()
            if not self.jobs and core.group_processing.available:
                await core.group_processing.refresh(
                    self._phase_page("processing_learning", ["pending", "material_pending"])
                    + self._phase_page(
                        "processing_outbox",
                        ["blocked_scope", "pending", "unknown", "relationship_pending"],
                    )
                )
            if (
                not core.automatic_memory_candidates
                and not core.actor_memory.available
                and not core.participation.available
                and not core.actor_observation.available
            ):
                return
            changed = self.backfill() if core.automatic_memory_candidates else False
            if (
                core.automatic_memory_candidates
                and getattr(core.memory, "extraction_schema", 5) == 6
            ):
                changed = self.segments.seal_due() or changed
            if core.automatic_memory_candidates:
                changed = self._recover_legacy() or changed
                changed = self._order_pending() or changed
            if changed:
                core.counted("memory.work")
            if self.jobs:
                # Explicit learning drains may await the current phase; an
                # outbox pump must not wait for an unrelated held model.
                selected = [] if outbox_only else list(self.jobs.values())
            else:
                phase = self._select_phase(outbox_only=outbox_only)
                if phase is None:
                    return
                # This is new asynchronous work. It has its own execution ID,
                # not the already completed HTTP or chat request's lifetime.
                with obs.correlation_scope(obs.new_correlation_id()):
                    self.jobs[phase["item"]["id"]] = asyncio.create_task(self._run_phase(phase))
                selected = list(self.jobs.values())
        if wait and selected:
            try:
                done, _ = await asyncio.wait(selected, return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                for job in selected:
                    job.cancel()
                await asyncio.gather(*selected, return_exceptions=True)
                raise
            for job in done:
                if not job.cancelled():
                    job.result()

    async def drain(self):
        await asyncio.gather(*tuple(self.jobs.values()), return_exceptions=True)

    async def close(self):
        async with self.dispatch_lock:
            self.closed = True
            for job in self.jobs.values():
                job.cancel()
            await asyncio.gather(*self.jobs.values(), return_exceptions=True)
            self.jobs.clear()

    @staticmethod
    def _candidate_mutates(item, output):
        return output != {"memories": []} and not (
            item.get("extraction_schema") == 6 and item.get("job_id") is not None and output is None
        )

    async def _candidate(self, item, output, *, reserve_attempt=False, reconcile=False):
        # Retained protocols can capture an episode on a null lookup. Schema6
        # claims at seal: a known job's null-output window/receipt read creates
        # no learned content and must not install a foreground mutation gate.
        # Serial source I/O leaves the shared peer read slots available to live
        # dialogue. Models remain outside this lock; waiting spends no attempt
        # and creates no new mutation gate. The existing budget includes waiting.
        if not reconcile:
            if (
                self._phase is not None
                and not self._phase["started"]
                and item.get("stage") not in MUTATION_WAIT
                and self._candidate_mutates(item, output)
                and self.core.has_active_dialogue(item["scope"])
            ):
                return None
            if not self._start_phase(item):
                return None
        async with asyncio.timeout(45):
            async with self.lock:
                try:
                    if not reconcile and not await self.core.group_processing.start(item):
                        return None
                    turn = self._source_turn(item)
                    mutates = self._candidate_mutates(item, output)
                    if mutates and self.core.has_active_dialogue(turn["scope"]):
                        if reconcile:
                            # Explicit receipt recovery neither regenerates nor
                            # installs an orphaned durable admission gate.
                            raise Fault("queue_full")
                        waiting = (
                            "commit_wait"
                            if output is not None or item.get("stage") == "commit_wait"
                            else "capture_wait"
                        )
                        if item.get("stage") != waiting:
                            item["stage"] = waiting
                            self.core.store.put("metadata", item)
                            self.core.counted("memory.work")
                        return None
                    if mutates:
                        self.mutation_scopes[item["id"]] = turn["scope"]
                    if reserve_attempt:
                        # Only actual receipt/capture I/O spends an attempt.
                        item["attempts"] += 1
                    if not reconcile:
                        if output is not None:
                            item["stage"] = "commit"
                        elif item.get("stage") not in MUTATION_WAIT:
                            item["stage"] = "receipt"
                        self.core.store.put("metadata", item)
                    return await self._candidate_request(item, output)
                finally:
                    self.mutation_scopes.pop(item["id"], None)

    def _source_turn(self, item):
        core = self.core
        if item.get("extraction_schema") == 6:
            frame = item["scope"]
            if (
                not core.automatic_memory_candidates
                or frame["actor_id"] not in core.roles
                or not core._role_allows(frame["actor_id"], "memory.write")
            ):
                raise Fault("forbidden")
            # Routing and live role control only, never a fabricated Core turn
            # or a substitute for Memory's complete all-author source proof.
            return dict(id=item["segment_id"], scope=frame)
        turn = core.store.get("turns", item["turn_id"])
        if (
            not core.automatic_memory_candidates
            or turn is None
            or turn["scope"]["actor_id"] not in core.roles
            or not core._turn_allows(turn, "memory.write")
        ):
            raise Fault("forbidden")
        if (
            turn["cancelled"]
            or item.get("scope", turn["scope"]) != turn["scope"]
            or item.get("input_revision", turn["bundle"]["collection_revision"])
            != turn["bundle"]["collection_revision"]
            or not sources_current(core.store, turn)
        ):
            raise Fault("scope_changed")
        core._check_role(turn)
        return turn

    def _summary_subject(self, item):
        # Metadata scope is the job's original owner; a frozen window must
        # agree with it. Receipt-only recovery need not create a new window.
        scope = item.get("scope")
        if scope is None:
            scope = self._source_turn(item)["scope"]
        if item.get("window") is not None and item["window"]["manifest"]["scope"] != scope:
            raise Fault("scope_changed")
        return scope["person_id"]

    async def _candidate_request(self, item, output):
        # Missing pins belong to retained pre-upgrade proposals/jobs. Never infer
        # a version from their JSON, and never regenerate an uncertain proposal.
        schema = item.get("extraction_schema", 3)
        if schema == 6:
            if not item.get("job_id"):
                response = await self.core.memory.create_segment(item["create_request"])
                item.update(
                    job_id=response["job_id"],
                    stage="receipt",
                    last_error=None,
                    recovery_reason=None,
                )
                with self.core.store.transaction():
                    self.core.store.put("metadata", item)
                    self.core.store.put(
                        "metadata",
                        dict(id="memory-job:" + digest(item["job_id"]), learning_id=item["id"]),
                    )
                # Creation has its own durable identity and uncertain replay.
                # The next phase can read this same job without creating again.
                return None
            return await self.core.memory.process_candidate(
                item["job_id"], output, extraction_schema=6, batch_ref=item.get("batch_ref")
            )
        if schema == 5:
            from .memory_summaries import check_response_subject

            subject = self._summary_subject(item)
            if (
                output is not None
                and "subject_person_id" in output
                and output["subject_person_id"] != subject
            ):
                raise Fault("invalid_input")
            response = await self.core.memory.process_candidate(
                item["job_id"],
                output,
                extraction_schema=5,
                job_ids=item.get("job_ids", [item["job_id"]]),
                window=None
                if item.get("batch_ref") or item.get("capture_uncertain")
                else item.get("window"),
                batch_ref=item.get("batch_ref"),
                subject_person_id=subject,
            )
            check_response_subject(
                response,
                subject,
                unknown=output is not None
                or not (item.get("batch_ref") or item.get("capture_uncertain")),
            )
            return response
        if schema == 4 or item.get("extraction_negotiated"):
            response = await self.core.memory.process_candidate(
                item["job_id"], output, extraction_schema=schema
            )
            if output is None:
                item["extraction_schema"] = response["extraction_schema"]
                item["extraction_negotiated"] = True
                self.core.store.put("metadata", item)
            return response
        if schema != 3:
            raise Fault("invalid_input")
        return await self.core.memory.process_candidate(item["job_id"], output)

    def _capture_window(self, item):
        """Freeze this job's complete new input and bounded conversation context."""
        if item.get("window") is not None:
            return
        turn = self._source_turn(item)
        pending = dict(
            job_id=item["job_id"],
            job_ids=[item["job_id"]],
            output=None,
            supported_extraction_schemas=[5],
            window=None,
        )
        overhead = len(canonical(pending).encode("utf-8")) - len("null")
        item.update(
            job_ids=[item["job_id"]],
            window=self.core.summary_windows.read([turn["id"]], max_bytes=262144 - overhead),
        )
        self.core.store.put("metadata", item)

    def _check_summary_window(self, item):
        window = item.get("window")
        if window is None:
            raise Fault("scope_changed")
        checked = self.core.summary_windows.check(
            "memory",
            dict(
                schema_version=1,
                request_id="summary-check:" + digest(item["job_id"]),
                manifest=window["manifest"],
            ),
        )
        if checked["state"] != "valid":
            raise Fault("scope_changed")

    def _finish_summary(self, item, response):
        from .memory_summaries import check_response_subject

        check_response_subject(response, self._summary_subject(item), unknown=True)
        receipt = response["result"]
        if response["job_ids"] != [item["job_id"]]:
            raise Fault("invalid_input", unknown=True)
        if (
            item.get("batch_ref")
            and response.get("batch_ref", item["batch_ref"]) != item["batch_ref"]
        ):
            raise Fault("invalid_input", unknown=True)
        if response["state"] == "completed":
            if (
                response["job_ids"] != item["job_ids"]
                or receipt["covered_job_ids"] != item["job_ids"]
                or receipt["cursor"]["job_ids"] != item["job_ids"]
                or receipt["cursor"]["window_id"] != item["window"]["manifest"]["window_id"]
            ):
                raise Fault("invalid_input", unknown=True)
        item.update(
            state=response["state"],
            receipt=receipt,
            output=None,
            deadline=0,
            last_error=None,
            recovery_reason=None,
            batch_ref=response.get("batch_ref"),
        )
        if receipt is not None:
            item.update(
                cursor=receipt["cursor"],
                experience_state=receipt["experience_state"],
                summary_state=receipt["summary_state"],
                portrait_state=receipt["portrait"]["state"],
            )
        self.core.store.put("metadata", item)

    def _pin_segment_window(self, item, response):
        from .memory_summaries import check_segment_window

        window = check_segment_window(self.core.contracts, response["segment_window"])
        if window["segment_id"] != item["segment_id"] or window["frame"] != item["scope"]:
            raise Fault("scope_changed")
        if item.get("segment_window") is not None and item["segment_window"] != window:
            raise Fault("scope_changed")
        if item.get("batch_ref") is not None and item["batch_ref"] != response["batch_ref"]:
            raise Fault("scope_changed")
        members = {member["member_id"]: member for member in window["members"]}
        material = {row["material_ref"]: row for row in item["materials"]}
        expected_people = {row["person_id"] for row in material.values()}
        targets = {target["scope"]["person_id"]: target for target in window["targets"]}
        if set(targets) != expected_people:
            raise Fault("invalid_input")
        for person, target in targets.items():
            expected = [
                row["material_ref"] for row in item["materials"] if row["person_id"] == person
            ]
            if target["new_member_ids"] != expected:
                raise Fault("invalid_input")
            for identity in expected:
                if (
                    len(members[identity]["text"].encode("utf-8"))
                    != material[identity]["text_bytes"]
                ):
                    raise Fault("invalid_input")
        item.update(
            segment_window=copy.deepcopy(window), batch_ref=response["batch_ref"], stage="receipt"
        )
        self.core.store.put("metadata", item)

    def _finish_segment(self, item, response):
        from .memory_summaries import check_segment_receipt

        if response["state"] == "completed":
            if item.get("segment_window") is None:
                raise Fault("invalid_input", unknown=True)
            check_segment_receipt(
                self.core.contracts, response, item["segment_window"], item["output"]
            )
        item.update(
            state=response["state"],
            receipt=copy.deepcopy(response) if response["state"] == "completed" else None,
            deadline=0,
            last_error=None if response["state"] == "completed" else "scope_changed",
            recovery_reason=None,
        )
        if response["state"] == "completed":
            item["summary_state"] = (
                "committed"
                if any(result["memory_refs"] for result in response["results"])
                else "skipped"
            )
            item["portrait_state"] = (
                "conflict"
                if any(result["portrait_state"] == "conflict" for result in response["results"])
                else "committed"
                if any(result["portrait_state"] == "committed" for result in response["results"])
                else "not_requested"
            )
        self.core.store.put("metadata", item)

    async def _pin_model(self, item, generation, scope):
        life = self.core.life
        route = item.get("model_route")
        if route is None:
            row = self.core.store.db.execute(
                "SELECT rowid FROM metadata WHERE id=?", (item["id"],)
            ).fetchone()
            retained = row is not None and row[0] <= self.legacy_model_rowid
            caller = "companion" if retained else life.generation_caller_service
            route = dict(
                caller_service=caller,
                turn_id=generation["id"]
                if caller == "companion"
                else "memory-generation:" + digest([generation["id"], caller]),
            )
            # A restart or later deployment change cannot reclassify an already
            # selected/submitted model attempt under a new routing grant ID.
            item["model_route"] = route
            self.core.store.put("metadata", item)
        independent = route["caller_service"] == "companion_background"
        if route["caller_service"] not in {"companion", "companion_background"} or (
            independent and life.generation_caller_service != "companion_background"
        ):
            raise Fault("dependency_unavailable")
        generation["id"] = generation["conversation_id"] = route["turn_id"]
        await pin_generation(
            generation,
            SelectionRequest(
                turn_id=generation["id"],
                **scope,
                function_id="memory",
                caller_service=route["caller_service"],
            ),
            life.model_selector if independent else self.core.default_model_selector,
            self.core.clock,
            static_version=life.config_version if independent else self.core.config_version,
            independent=independent,
        )
        return life.gateway if independent else self.core.gateway

    async def _summary_model(self, item, generation):
        """Shared model-slot, selection and timeout owner for contextual protocols."""
        from .memory_summaries import messages, segment_messages

        item["stage"] = "model"
        self.core.store.put("metadata", item)
        async with self.core.models.background:
            self._source_turn(item)
            if item["extraction_schema"] == 5:
                self._check_summary_window(item)
            if not self._start_phase(item):
                return None
            gateway = await self._pin_model(item, generation, generation["scope"])
            self._source_turn(item)
            if not await self.core.group_processing.start(item):
                return None
            if item["extraction_schema"] == 6:
                # The observation part has no local Core admission. Refresh the
                # complete real Memory/Platform proof after both awaited gates.
                response = await self._candidate(
                    item, None, reserve_attempt=item.get("last_error") is not None
                )
                if response is None:
                    return None
                if response["state"] in {"completed", "invalidated"}:
                    self._finish_segment(item, response)
                    return None
                self._pin_segment_window(item, response)
                if not await self.core.group_processing.start(item):
                    return None
                supplied = segment_messages(self.core.contracts, item["segment_window"])
            else:
                self._check_summary_window(item)
                supplied = messages(self.core.contracts, item["window"], item["portrait_input"])
            item["stage"] = "model"
            self.core.store.put("metadata", item)
            segments, _ = await asyncio.wait_for(
                gateway.generate(generation, supplied), MODEL_TIMEOUT_SECONDS
            )
            return "\n".join(segments)

    async def _segment_work(self, item):
        from .memory_summaries import check_segment_output, parse_segment

        if not item.get("job_id"):
            await self._candidate(item, None, reserve_attempt=True)
            return
        # Retained/uncertain outputs always reconcile first. A model-ready
        # frozen window, however, is not a reason to repeat the initial full
        # receipt read before waiting for the real model/selection gates.
        if item["output"] is not None or item.get("segment_window") is None:
            response = await self._candidate(
                item,
                None,
                reserve_attempt=item.get("last_error") is not None
                and item.get("stage") != "commit_wait",
            )
            if response is None:
                return
            if response["state"] in {"completed", "invalidated"}:
                self._finish_segment(item, response)
                return
            self._pin_segment_window(item, response)
            if item["output"] is None:
                item.update(stage="model", last_error=None, recovery_reason=None)
                self.core.store.put("metadata", item)
                return
        if item["output"] is None:
            identity = "memory-generation:" + digest(item["segment_id"])
            generation = dict(
                id=identity,
                conversation_id=identity,
                # A real target supplies configuration routing only; all
                # target/source authority remains in the complete window.
                scope=copy.deepcopy(item["segment_window"]["targets"][0]["scope"]),
                config_version=self.core.config_version,
                generation_budget=dict(max_tokens=8192, reasoning_effort="none"),
            )
            raw = await self._summary_model(item, generation)
            if raw is None:
                return
            item["stage"] = "proposal"
            item["output"] = parse_segment(self.core.contracts, raw, item["segment_window"])
            item["stage"] = "commit"
            item["last_error"] = None
            item["recovery_reason"] = None
            self.core.store.put("metadata", item)
            return
        check_segment_output(self.core.contracts, item["output"], item["segment_window"])
        response = await self._candidate(item, item["output"])
        if response is None:
            return
        if response["state"] not in {"completed", "invalidated"}:
            raise Fault("dependency_unavailable", unknown=True)
        self._finish_segment(item, response)

    async def _summary_work(self, item):
        from .memory_summaries import parse

        self._capture_window(item)
        response = await self._candidate(
            item,
            None,
            reserve_attempt=item.get("stage") != "commit_wait",
        )
        if response is None:
            return
        if response["state"] == "unclaimed":
            if (
                not item.get("capture_uncertain")
                or item.get("batch_ref")
                or item["output"] is not None
                or response["job_ids"] != item["job_ids"]
            ):
                raise Fault("invalid_input")
            # Memory proved the receipt lookup found no claim. Resend only the
            # frozen original window, never select or trim a replacement.
            item.update(capture_uncertain=False, capture_replayed=True, stage="receipt")
            self.core.store.put("metadata", item)
            self._check_summary_window(item)
            response = await self._candidate(item, None)
            if response is None:
                return
        if response["state"] == "context_changed":
            if (
                item.get("batch_ref")
                or item.get("capture_uncertain")
                or item.get("capture_replayed")
                or item.get("context_recaptured")
                or item["output"] is not None
                or response["job_ids"] != item["job_ids"]
                or response["window_id"] != item["window"]["manifest"]["window_id"]
            ):
                raise Fault("scope_changed")
            item.update(
                window=self.core.summary_windows.without_context(
                    item["window"], response["invalid_context_member_ids"]
                ),
                context_recaptured=True,
                stage="receipt",
            )
            self.core.store.put("metadata", item)
            self._check_summary_window(item)
            response = await self._candidate(item, None)
            if response is None:
                return
            if response["state"] == "context_changed":
                raise Fault("scope_changed")
        if response["state"] in {"completed", "invalidated"}:
            self._finish_summary(item, response)
            return
        if response["state"] != "pending" or response["job_ids"] != item["job_ids"]:
            raise Fault("invalid_input")
        if item.get("batch_ref") and response["batch_ref"] != item["batch_ref"]:
            raise Fault("invalid_input")
        if response["window_id"] != item["window"]["manifest"]["window_id"]:
            raise Fault("scope_changed")
        item.update(
            batch_ref=response["batch_ref"],
            portrait_input=response["portrait_input"],
            stage="receipt",
            capture_uncertain=False,
        )
        self.core.store.put("metadata", item)
        self._check_summary_window(item)
        if item["output"] is None:
            turn = self._source_turn(item)
            generation = copy.deepcopy(turn)
            generation["id"] = "memory-generation:" + digest(item["job_id"])
            generation["conversation_id"] = generation["id"]
            generation["generation_budget"] = dict(max_tokens=8192, reasoning_effort="none")
            raw = await self._summary_model(item, generation)
            if raw is None:
                return
            item["stage"] = "proposal"
            item["output"] = parse(
                self.core.contracts,
                raw,
                subject_person_id=self._summary_subject(item),
            )
            item["stage"] = "commit"
            self.core.store.put("metadata", item)
        self._source_turn(item)
        self._check_summary_window(item)
        response = await self._candidate(item, item["output"])
        if response is None:
            return
        if response["state"] not in {"completed", "invalidated"}:
            raise Fault("dependency_unavailable", unknown=True)
        self._finish_summary(item, response)

    async def _work(self, item):
        core = self.core
        # Old retained jobs lack category evidence. Keep their spent attempts
        # conservatively in the original temporary-failure budget, never reset it.
        failures = item.setdefault("recovery_failures", {"temporary": item["attempts"]})
        resuming_commit = item.get("stage") == "commit_wait" and item["output"] is not None
        if not resuming_commit and (
            item["attempts"] >= MAX_TOTAL_ATTEMPTS
            or any(count >= MAX_RECOVERY_ATTEMPTS for count in failures.values())
            or item.get("proposal_failures", 0) >= MAX_ATTEMPTS
        ):
            item.update(state="blocked", deadline=0)
            core.store.put("metadata", item)
            return
        try:
            turn = self._source_turn(item)
            if item.get("stage") == "proposal":
                # A previous proposal failure is not a new model attempt.
                # Recovery first reads Memory, retaining any parsed output.
                item["stage"] = "commit" if item["output"] is not None else "receipt"
                core.store.put("metadata", item)
            if item.get("extraction_schema") == 6:
                await self._segment_work(item)
                core.store.put("metadata", item)
                core.counted("memory.work")
                return
            if item.get("extraction_schema") == 5:
                await self._summary_work(item)
                core.store.put("metadata", item)
                core.counted("memory.work")
                return
            response = await self._candidate(item, None, reserve_attempt=not resuming_commit)
            if response is None:
                return
            item["stage"] = "receipt"
            if not resuming_commit:
                core.store.put("metadata", item)
            turn = self._source_turn(item)
            if response["state"] not in {"pending", "completed", "invalidated"}:
                raise Fault("invalid_input")
            if response["state"] == "pending":
                if item["output"] is None:
                    from .memory_admission import empty_extraction_reason, extraction_review_hints

                    content = response["content"]
                    schema4 = item.get("extraction_schema") == 4
                    skip_reason = None if schema4 else empty_extraction_reason(content)
                    if (
                        core._event_reality(turn) not in {"real", "fictional"}
                        or not any(c["text"].strip() for c in content)
                        or skip_reason
                    ):
                        item["output"] = {"memories": []}
                        item["admission_reason"] = skip_reason or "ineligible_reality"
                    else:
                        generation = copy.deepcopy(turn)
                        generation["id"] = "memory-generation:" + digest(item["job_id"])
                        # A separate provider session prevents extraction instructions or JSON
                        # from becoming the next chat turn's hidden provider history.
                        generation["conversation_id"] = generation["id"]
                        # Automatic extraction is a bounded JSON task. Its own
                        # first request disables reasoning. A rejected/invalid
                        # proposal uses low effort to repair grounding rather
                        # than repeating an ineffective fast attempt. Chat is unchanged.
                        generation["generation_budget"] = dict(
                            max_tokens=8192,
                            reasoning_effort="low"
                            if item.get("proposal_failures") or item.get("empty_review")
                            else "none",
                        )
                        item["stage"] = "model"
                        core.store.put("metadata", item)
                        async with core.models.background:
                            turn = self._source_turn(item)
                            gateway = await self._pin_model(item, generation, turn["scope"])
                            self._source_turn(item)
                            if not await core.group_processing.start(item):
                                return
                            hints = (
                                []
                                if schema4
                                else extraction_review_hints(
                                    content, response.get("known_facts", [])
                                )
                            )
                            messages = legacy_messages(
                                core.contracts,
                                response,
                                hints=hints,
                                rejected=bool(item.get("proposal_rejected")),
                                empty_review=item.get("empty_review"),
                                diagnostics=item.get("proposal_diagnostics"),
                            )
                            segments, _ = await asyncio.wait_for(
                                gateway.generate(generation, messages), MODEL_TIMEOUT_SECONDS
                            )
                        raw = "\n".join(segments)
                        item["stage"] = "proposal"
                        try:
                            output = parse_proposal(core.contracts, raw, schema4=schema4)
                        except ProposalStructureError as error:
                            # This call only parses the new model result. It has
                            # not cached or sent a proposal; use the existing
                            # bounded correction path, never commit recovery.
                            item.update(
                                output=None,
                                proposal_rejected=True,
                                proposal_diagnostics=error.diagnostics,
                            )
                            raise
                        item["output"] = output
                        item.pop("proposal_diagnostics", None)
                        if item["output"] == {"memories": []} and not item.get("empty_review"):
                            if hints:
                                # Yield the background slot and durably reserve one review.
                                # A restart resumes it within the existing job retry budget;
                                # it cannot submit an empty receipt prematurely. This path
                                # is never entered after an uncertain Memory commit.
                                item.update(output=None, empty_review=hints, stage="empty_review")
                                core.store.put("metadata", item)
                                core.counted("memory.work")
                                return
                    # Persist exact proposal before the uncertain remote commit; on restart
                    # read its receipt first. Only a valid Memory-owned pending
                    # response permits the same job/exact output retry: Memory's
                    # atomic job ledger consumes it once, including a lost ack.
                    item["stage"] = "commit"
                    core.store.put("metadata", item)
                response = await self._candidate(item, item["output"])
                if response is None:
                    return
                if response["state"] == "rejected":
                    # Only this explicit Memory-owned rejection proves the proposal did
                    # not commit. Transport/schema failures retain the original proposal.
                    item["output"] = None
                    item["proposal_rejected"] = True
                    item["stage"] = "proposal"
                    raise Fault("invalid_input")
            if response["state"] not in {"completed", "invalidated"}:
                raise Fault("dependency_unavailable")
            item.update(
                state=response["state"],
                receipt=response["result"],
                last_error=None,
                output=None,
                deadline=0,
                recovery_reason=None,
            )
        except asyncio.CancelledError as error:
            failed_stage = item.get("stage")
            self._failed(item, "cancelled", error, failed_stage=failed_stage)
            core.store.put("metadata", item)
            raise
        except (Fault, OSError, TimeoutError, ValueError, TypeError, KeyError) as error:
            code = (
                error.code
                if isinstance(error, Fault)
                else (
                    "dependency_unavailable"
                    if isinstance(error, (OSError, TimeoutError))
                    else "invalid_input"
                )
            )
            failed_stage = item.get("stage")
            self._failed(item, code, error, failed_stage=failed_stage)
        core.store.put("metadata", item)
        core.counted("memory.work")

    def _failed(self, item, code, error=None, *, failed_stage=None):
        stage = item.get("stage")
        if stage in MUTATION_WAIT:
            failed_stage = failed_stage or stage
            item["stage"] = stage = "receipt"
        if (
            item.get("extraction_schema") == 5
            and stage == "receipt"
            and getattr(error, "unknown", False)
            and not item.get("batch_ref")
        ):
            item["capture_uncertain"] = True
        diagnostic = obs.failure_diagnostic(
            error,
            code=code,
            attempt=item["attempts"],
            stage=failed_stage or stage,
            observed_at=self.core.clock(),
        )
        # This is a fixed machine projection, not the peer's error body or the
        # exception message. Keep the first cause and latest cause across success
        # and restart without an unbounded event history or a new ledger.
        item.setdefault("first_failure", diagnostic)
        item["last_failure"] = diagnostic
        item["failure_count"] = item.get("failure_count", 0) + 1
        authorization = code in {"forbidden", "unauthorized", "scope_changed", "not_found"}
        proposal_failure = stage == "proposal" and not authorization
        if proposal_failure:
            item["proposal_failures"] = item.get("proposal_failures", 0) + 1
        # A null-output receipt/content read cannot submit a proposal, even if
        # its timeout envelope says execution_state=unknown. Supplier pressure
        # and safe reads must not spend each other's recovery allowance.
        kind = (
            "receipt"
            if stage == "receipt"
            else "rate_limit"
            if stage == "model" and (code == "queue_full" or getattr(error, "rate_limited", False))
            else "temporary"
        )
        failures = item.setdefault("recovery_failures", {})
        if not authorization and not proposal_failure:
            failures[kind] = failures.get(kind, 0) + 1
        count = item.get("proposal_failures", 0) if proposal_failure else failures.get(kind, 0)
        terminal = (
            authorization
            or item.get("proposal_failures", 0) >= MAX_ATTEMPTS
            or any(value >= MAX_RECOVERY_ATTEMPTS for value in failures.values())
            or item["attempts"] >= MAX_TOTAL_ATTEMPTS
        )
        delay = 2 ** min(count, 8)
        if stage in {"model", "proposal"} and item["output"] is None:
            # No proposal reached Memory: retries only spend model capacity.
            # Keep a bounded exponential delay without the remote-commit cooldown.
            delay = min(32, delay)
        elif count >= MAX_ATTEMPTS:
            delay = min(300, 60 * 2 ** (count - MAX_ATTEMPTS))
        # Persist the provider's valid lower bound; never hold a worker or a
        # model slot asleep, and never shorten Retry-After to our backoff cap.
        delay = max(delay, getattr(error, "retry_after_seconds", None) or 0)
        item.update(
            state="blocked" if terminal else "pending",
            last_error=code,
            deadline=0 if terminal else self.core.clock() + delay,
            recovery_reason=(
                "authorization"
                if authorization
                else "proposal_rejected"
                if proposal_failure
                else "rate_limit_cooldown"
                if kind == "rate_limit"
                else "receipt_read_cooldown"
                if kind == "receipt"
                else "temporary_failure_cooldown"
            ),
        )

    def _recover_legacy(self):
        """One bounded pass over retained pre-upgrade failures, never reset attempts."""
        store = self.core.store
        if getattr(self, "_legacy_complete", False):
            return False
        if (
            store.db.execute(
                "SELECT 1 FROM metadata WHERE conversation_id=? AND status='blocked' LIMIT 1",
                (QUEUE,),
            ).fetchone()
            is None
        ):
            self._legacy_complete = True
            return False
        cursor = store.get("metadata", "memory-learning-recovery") or {
            "id": "memory-learning-recovery",
            "cursor": 0,
        }
        rows = store.db.execute(
            "SELECT rowid,body FROM metadata WHERE rowid>? ORDER BY rowid LIMIT 64",
            (cursor["cursor"],),
        ).fetchall()
        if not rows:
            self._legacy_complete = True
            return False
        with store.transaction():
            for row in rows:
                item = json.loads(row[1])
                if (
                    item.get("conversation_id") == QUEUE
                    and item.get("extraction_schema") != 6
                    and item.get("state") == "blocked"
                    and item.get("attempts", MAX_RECOVERY_ATTEMPTS) < MAX_RECOVERY_ATTEMPTS
                    and item.get("last_error")
                    in {
                        "dependency_unavailable",
                        "timeout",
                        "queue_full",
                        "cancelled",
                        "result_unknown",
                    }
                ):
                    item.update(
                        state="pending",
                        deadline=self.core.clock() + 60,
                        recovery_reason="temporary_failure_cooldown",
                    )
                    # Recovery can encounter an old blocked row on a later page,
                    # after the initial pending-order upgrade already finished.
                    if "scope_key" not in item:
                        self._ordered = False
                    store.put("metadata", item)
                cursor["cursor"] = row[0]
            if rows:
                store.put("metadata", cursor)
        return bool(rows)
