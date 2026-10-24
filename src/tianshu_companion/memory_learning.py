"""Contextual background learning and receipt-first recovery of retained fact jobs."""

import asyncio
import copy
import json

from .contracts import Fault, canonical, digest
from .model_selection import SelectionRequest, resolve_selection
from .memory_extraction import ProposalStructureError, parse_proposal, legacy_messages
from .short_context import sources_current
from . import observability as obs

QUEUE = "memory:learning"
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
        self.mutation_scopes = {}
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

    def admission_blocked(self, scope):
        if not self.core.automatic_memory_candidates:
            return False
        if any(memory_scopes_overlap(active, scope) for active in self.mutation_scopes.values()):
            return True
        # Persisted barriers survive restart. Waiting in one room must neither
        # hold the commit lock nor prevent extraction in independent rooms.
        return any(
            memory_scopes_overlap(json.loads(row[0]), scope)
            for row in self.core.store.db.execute(
                "SELECT CAST(json_extract(body,'$.scope') AS TEXT) FROM metadata WHERE conversation_id=? "
                "AND status='pending' AND json_extract(body,'$.stage') IN ('capture_wait','commit_wait')",
                (QUEUE,),
            )
        )

    def queue(self, item):
        receipt = item.get("receipt") or {}
        job = receipt.get("candidate_job_ref")
        if not job:
            return
        key = "memory-learn:" + digest(job)
        if self.core.store.get("metadata", key):
            return
        schema = getattr(self.core.memory, "extraction_schema", 5)
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

    def _next(self, *, retry_only=False):
        # A delayed extraction must not be overtaken by its own correction:
        # Memory's known_facts would otherwise omit the original. Other scopes
        # can progress during its cooldown. Both predecessor probes are indexed.
        store = self.core.store
        # New jobs have deadline=0. An endless stream of them must not keep an
        # already-due retry at the end of the queue. Grant it at least every
        # fourth selection, still subject to the same scope predecessors.
        if not retry_only and getattr(self, "_fresh_grants", 0) >= 3:
            retry = self._next(retry_only=True)
            if retry is not None:
                return retry
        cursor_name = "_retry_cursor" if retry_only else "_due_cursor"
        cursor = getattr(self, cursor_name, (-1, -1, ""))
        retry_clause = "AND deadline>0 " if retry_only else ""
        rows = store.db.execute(
            "SELECT deadline,position,id FROM metadata WHERE conversation_id=? "
            "AND status='pending' AND deadline<=? "
            + retry_clause
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
            "automatic_learning_mode": "context_window_summary_and_optional_portrait"
            if getattr(core.memory, "extraction_schema", 3) == 5
            else "retained_fact_extraction",
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
            key = "memory-learn:" + digest(job_id)
            running = self.jobs.pop(key, None)
            if running is not None:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
            return await self._reconcile(job_id, key)

    async def _reconcile(self, job_id, key):
        item = self.core.store.get("metadata", key)
        if not item:
            raise Fault("not_found")
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

    async def work(self, *, wait=True):
        """Pump bounded independent jobs; the runtime loop need not await model I/O."""
        core = self.core
        async with self.dispatch_lock:
            if self.closed:
                return
            for key, job in list(self.jobs.items()):
                if job.done():
                    del self.jobs[key]
                    if not job.cancelled():
                        job.result()
            if not core.automatic_memory_candidates or not hasattr(
                core.memory, "process_candidate"
            ):
                return
            changed = self.backfill()
            changed = self._recover_legacy() or changed
            changed = self._order_pending() or changed
            if changed:
                core.counted("memory.work")
            if not self._ordered:
                return
            # Bounded lookahead overlaps receipt/source I/O with generation. It
            # does not increase provider capacity or bypass scope predecessors.
            limit = max(2, 2 * core.models.background_limit)
            while len(self.jobs) < limit:
                item = self._next()
                if item is None:
                    break
                self.jobs[item["id"]] = asyncio.create_task(self._work(item))
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

    async def close(self):
        async with self.dispatch_lock:
            self.closed = True
            for job in self.jobs.values():
                job.cancel()
            await asyncio.gather(*self.jobs.values(), return_exceptions=True)
            self.jobs.clear()

    async def _candidate(self, item, output, *, reserve_attempt=False, reconcile=False):
        # A null lookup can capture an episode. All candidate calls therefore
        # share the same mutation/admission owner, including explicit recovery.
        # Serial source I/O leaves the shared peer read slots available to live
        # dialogue. Models remain outside this lock; waiting spends no attempt
        # and creates no new mutation gate. The existing budget includes waiting.
        async with asyncio.timeout(45):
            async with self.lock:
                try:
                    turn = self._source_turn(item)
                    mutates = output != {"memories": []}
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

    async def _candidate_request(self, item, output):
        # Missing pins belong to retained pre-upgrade proposals/jobs. Never infer
        # a version from their JSON, and never regenerate an uncertain proposal.
        schema = item.get("extraction_schema", 3)
        if schema == 5:
            return await self.core.memory.process_candidate(
                item["job_id"],
                output,
                extraction_schema=5,
                job_ids=item.get("job_ids", [item["job_id"]]),
                window=None
                if item.get("batch_ref") or item.get("capture_uncertain")
                else item.get("window"),
                batch_ref=item.get("batch_ref"),
            )
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

    async def _summary_work(self, item):
        from .memory_summaries import messages, parse

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
            item["stage"] = "model"
            self.core.store.put("metadata", item)
            async with self.core.models.background:
                self._source_turn(item)
                self._check_summary_window(item)
                if self.core.default_model_selector is not None:
                    selected = await resolve_selection(
                        self.core.default_model_selector,
                        SelectionRequest(
                            turn_id=generation["id"], **turn["scope"], function_id="memory"
                        ),
                        self.core.clock,
                    )
                    generation["config_version"] = selected.config_version
                self._source_turn(item)
                self._check_summary_window(item)
                segments, _ = await asyncio.wait_for(
                    self.core.gateway.generate(
                        generation,
                        messages(self.core.contracts, item["window"], item["portrait_input"]),
                    ),
                    MODEL_TIMEOUT_SECONDS,
                )
            item["stage"] = "proposal"
            item["output"] = parse(self.core.contracts, "\n".join(segments))
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
                            if core.default_model_selector is not None:
                                selected = await resolve_selection(
                                    core.default_model_selector,
                                    SelectionRequest(
                                        turn_id=generation["id"],
                                        **turn["scope"],
                                        function_id="memory",
                                    ),
                                    core.clock,
                                )
                                generation["config_version"] = selected.config_version
                            self._source_turn(item)
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
                                core.gateway.generate(generation, messages), MODEL_TIMEOUT_SECONDS
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
