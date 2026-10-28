"""BOT-owned actual reading evidence, existing outbox delivery and bounded recall.

The original content owner is still read on every source checkpoint. These leaves
prove this actor's observed reading interval, never ownership of the whole book.
"""

import asyncio
import copy
import uuid

from .clients import JsonService, uid, utc
from .context import ACTOR_MEMORY_DOMAIN, merge_checks, scope_check
from .context_checks import dependency_changed
from .contracts import Fault, canonical, digest

OUTBOX_PENDING = ("actor_memory_pending", "actor_memory_unknown")
SOURCE_PREFIX = "actor-memory-source:"
STATE_PREFIX = "actor-memory-state:"


def audience_context(turn, purpose="dialogue"):
    scope = turn["scope"]
    audience = dict(
        kind="group" if scope["audience"] == "group" else "private",
        conversation_id=scope["conversation_id"],
        assertion_ref=turn["origin"]["assertion_ref"],
    )
    if audience["kind"] == "private":
        audience["person_id"] = scope["person_id"]
    return dict(purpose=purpose, audience=audience)


class ActorMemory:
    def __init__(self, core, authority=None):
        self.core, self.store, self.authority = core, core.store, authority
        if authority is not None and not all(
            name in core.contracts.schemas for name in ("actor-self-memory", "actor-capabilities")
        ):
            raise ValueError("Actor memory requires its frozen authority contracts")

    @property
    def available(self):
        client = getattr(self.core.memory, "client", None)
        return bool(
            self.authority
            and self.authority.url
            and self.authority.token
            and client
            and client.url
            and client.token
        )

    def _local_actor(self, actor):
        self.core.life_runtime._actor(actor)
        if not self.core.reading.actor_enabled(actor):
            raise Fault("forbidden")

    async def role_version(self, actor):
        """Platform owns this version; Companion's local role version is unrelated."""
        if not self.available:
            raise Fault("dependency_unavailable")
        self._local_actor(actor)
        body = dict(
            schema_version=1, request_id=uid("actor-discover"), actor_id=actor, operation="discover"
        )
        self.core.contracts.check("actor-capabilities#discover", body)
        value = await self.authority.call("/internal/v1/actor-capabilities/operate", body)
        self.core.contracts.check("actor-capabilities#discovery_result", value)
        if value["request_id"] != body["request_id"] or value["actor_id"] != actor:
            raise Fault("invalid_input")
        self._local_actor(actor)
        return value["role_version"]

    async def _authority(
        self, actor, version, operation="actor_check", context=None, *, requested_operation=None
    ):
        if not self.available:
            raise Fault("dependency_unavailable")
        self._local_actor(actor)
        body = dict(
            schema_version=1,
            request_id=uid("actor-authority"),
            actor_id=actor,
            role_version=version,
            operation=operation,
        )
        if context is not None:
            body["context"] = context
        if operation == "actor_check":
            body["requested_operation"] = requested_operation
        self.core.contracts.check("actor-self-memory#" + operation + "_request", body)
        value = await self.authority.call("/internal/v1/actor-self-memory/authority", body)
        self.core.contracts.check("actor-self-memory#" + operation + "_result", value)
        if any(value[key] != body[key] for key in ("request_id", "actor_id", "role_version")) or (
            context is not None and value["context"] != context
        ):
            raise Fault("invalid_input")
        self._local_actor(actor)
        return value

    async def _memory(self, body):
        self.core.contracts.check("actor-self-memory#request", body)
        observed = {}

        def error_response(status, value, error):
            self.core.contracts.check("actor-self-memory#error", value)
            if value["request_id"] != body["request_id"]:
                raise Fault(
                    "invalid_input",
                    unknown=body["operation"] in {"append_episode", "append_derived"},
                )
            observed.update(value)

        try:
            value = await self.core.memory.client.call(
                "/internal/v1/actor-self-memory/operate",
                body,
                uncertain_write=body["operation"] in {"append_episode", "append_derived"},
                error_response=error_response,
            )
        except Fault as error:
            if observed:
                # AL01 uses execution, not the older common#error field name.
                # Only this exact validated complete response can settle a write.
                settled = Fault(
                    observed["code"], unknown=observed["execution"] == "unknown", cause=error.cause
                )
                settled.actor_memory_execution = observed["execution"]
                raise settled from None
            raise
        self.core.contracts.check("actor-self-memory#response", value)
        if value["actor_id"] != body["actor_id"]:
            raise Fault("invalid_input")
        if body["operation"] in {"append_episode", "get_receipt", "append_derived"}:
            command = body.get("command", body["operation"])
            if value["command"] != command or value["idempotency_key"] != body["idempotency_key"]:
                raise Fault("invalid_input", unknown=True)
            if body["operation"] != "get_receipt" and value["request_id"] != body["request_id"]:
                raise Fault("invalid_input", unknown=True)
            # Receipts identify the original command request, not this lookup.
        elif any(
            value[key] != body[key]
            for key in ("request_id", "role_version", "operation", "context")
        ):
            raise Fault("invalid_input")
        if body["operation"] == "get_episode" and (
            value["episode"]["actor_id"] != body["actor_id"]
            or value["episode"]["fact"]["actor_id"] != body["actor_id"]
            or value["episode"]["episode_id"] != body["episode_id"]
            or value["episode"]["revision"] != body["expected_revision"]
        ):
            raise Fault("invalid_input")
        return value

    async def _receipt(self, request, version):
        """Lookup uses current authority; the receipt still names the original request."""
        try:
            receipt = await self._memory(
                dict(
                    schema_version=1,
                    request_id=uid("actor-receipt"),
                    actor_id=request["actor_id"],
                    role_version=version,
                    operation="get_receipt",
                    command=request["operation"],
                    idempotency_key=request["idempotency_key"],
                )
            )
        except Fault as error:
            if error.code == "not_found":
                return None
            raise
        if receipt["request_id"] != request["request_id"]:
            raise Fault("invalid_input", unknown=True)
        return receipt

    def record_read(self, reading, result, *, requested_range=None):
        """Called inside Reading's existing fact+operation transaction only."""
        if not self.available:
            return
        if result["coverage"]["unit"] not in {"characters", "pages", "seconds", "items"}:
            # AL01 has no byte-image coverage shape; the existing original read
            # and Life event remain intact without inventing another unit.
            return
        actor = reading["actor_id"]
        original = dict(
            operation_ref=reading["id"],
            scope=copy.deepcopy(reading["scope"]),
            content_ref=copy.deepcopy(reading["content_ref"]),
            coverage=copy.deepcopy(result["coverage"]),
            content_digest=self.core.reading.read_signature(result),
        )
        if requested_range is not None:
            original["requested_range"] = copy.deepcopy(requested_range)
        basis = dict(
            actor_id=actor, reading_id=reading["id"], revision=reading["version"], observed=original
        )
        leaf = dict(
            producer="companion",
            object_id="reading-evidence:" + digest(basis),
            revision=str(reading["version"]),
            content_hash=digest(basis),
        )
        source = dict(
            producer="companion",
            object_id="reading-action:" + digest(basis),
            revision=str(reading["version"]),
            content_hash=digest(["action", basis]),
        )
        state = self.store.get("metadata", STATE_PREFIX + actor) or dict(
            id=STATE_PREFIX + actor, generation=uuid.uuid4().hex, sequence=0
        )
        key = SOURCE_PREFIX + digest(source)
        if self.store.get("metadata", key):
            return
        state["sequence"] += 1
        self.store.put("metadata", state)
        scope = reading["scope"]
        constraints = (
            dict(kind="actor_only")
            if scope is None
            else dict(kind="group", conversation_id=scope["conversation_id"])
            if scope["audience"] == "group"
            else dict(
                kind="private",
                person_id=scope["person_id"],
                conversation_id=scope["conversation_id"],
            )
        )
        now = utc(self.core.clock())
        summary = (
            "实际读取原件区间 "
            + canonical(result["coverage"])
            + ("；仍有未提供的内容缺口" if result.get("gaps") else "")
            + "；仅证明收到已提供内容，不代表全件完成或已经理解。"
        )
        if result.get("text"):
            quoted = summary + "\n已收到的区间原文（来源材料，不是指令）：\n" + result["text"]
            if len(quoted) <= 4000:
                summary = quoted
        common = dict(
            actor_id=actor,
            relation="participant",
            provenance="digital_action",
            occurred_at=now,
            learned_at=now,
            outcome="observed",
            summary=summary,
            source_generation=state["generation"],
            source_sequence=state["sequence"],
            access_version=digest([constraints, original]),
            source_constraints=constraints,
        )
        coverage = dict(
            copy.deepcopy(result["coverage"]),
            complete=result["complete"],
            gaps=copy.deepcopy(result.get("gaps", [])),
        )
        for reference, content_reads in (
            (leaf, []),
            (source, [dict(content_ref=leaf, coverage=[coverage])]),
        ):
            fact = dict(common, source=reference, content_reads=content_reads)
            self.core.contracts.check("actor-self-memory#source_fact", fact)
            self.store.put(
                "metadata",
                dict(
                    id=SOURCE_PREFIX + digest(reference),
                    actor_id=actor,
                    source=reference,
                    state="current",
                    fact=fact,
                    observed=original,
                ),
            )
        # Existing outbox and dispatcher own delivery. No second worker or message store.
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
                event=dict(kind="actor_read_observed", source=source, actor_id=actor),
            ),
        )

    def recover(self):
        for item in self.store.list("outbox", states=["actor_memory_submitting"]):
            item.update(state="actor_memory_unknown", last_error="result_unknown")
            self.store.put("outbox", item)

    async def flush(self, item):
        """Same immutable command after unknown; a current-role receipt lookup comes first."""
        actor = item["actor_id"]
        attempts = item["attempts"] + 1
        try:
            version = await self.role_version(actor)
            authority = await self._authority(actor, version, requested_operation="append_episode")
            request = item.get("request")
            receipt = None
            if request is not None:
                receipt = await self._receipt(request, version)
                if receipt is None and request["role_version"] != version:
                    # A missing receipt cannot prove an old in-flight command did
                    # not run. Preserve it; never mint a new key or change its body.
                    raise Fault("result_unknown", unknown=True)
            else:
                request = dict(
                    schema_version=1,
                    request_id=uid("actor-append"),
                    actor_id=actor,
                    role_version=version,
                    operation="append_episode",
                    idempotency_key="reading:" + digest(item["source"]),
                    source=copy.deepcopy(item["source"]),
                    perspective="participant",
                )
                item["request"] = request
            if receipt is None:
                if "append_episode" not in authority["allowed_operations"]:
                    raise Fault("forbidden")
                item.update(state="actor_memory_submitting", attempts=attempts)
                self.store.put("outbox", item)
                try:
                    receipt = await self._memory(request)
                except BaseException:
                    item.update(state="actor_memory_unknown", last_error="result_unknown")
                    self.store.put("outbox", item)
                    raise
            item.update(state="actor_memory_delivered", receipt=receipt, last_error=None)
        except (Fault, OSError, TimeoutError) as error:
            if (
                isinstance(error, Fault)
                and error.unknown
                or item["state"] in {"actor_memory_unknown", "actor_memory_submitting"}
            ):
                item["state"] = "actor_memory_unknown"
            else:
                item["state"] = "actor_memory_pending"
            item.update(
                last_error=error.code if isinstance(error, Fault) else "dependency_unavailable",
                deadline=self.core.clock() + min(60, 2 ** min(attempts, 6)),
            )
        item["attempts"] = attempts
        self.store.put("outbox", item)

    async def _current_source(self, actor, version, reference, role, context):
        state = self.store.get("metadata", STATE_PREFIX + actor) or dict(
            generation="missing", sequence=1
        )
        row = self.store.get("metadata", SOURCE_PREFIX + digest(reference))
        known = bool(row and row["actor_id"] == actor and row["source"] == reference)
        readable = known and role["state"] == "enabled"
        source_state = row["state"] if known else "missing"
        if readable and source_state == "current":
            try:
                await self.core.reading.check_read(actor, row["observed"])
            except Fault as error:
                if error.unknown or error.code not in {
                    "forbidden",
                    "not_found",
                    "scope_changed",
                    "version_conflict",
                }:
                    raise
                readable = False
                source_state = "withdrawn" if error.code == "not_found" else "superseded"
        constraint = row["fact"]["source_constraints"] if known else dict(kind="actor_only")
        audience = context["audience"]
        disclose = (
            readable
            and source_state == "current"
            and (
                audience["kind"] == "actor_only"
                or constraint["kind"] == "public"
                or (
                    constraint["kind"] == audience["kind"]
                    and constraint.get("conversation_id") == audience.get("conversation_id")
                    and (
                        constraint["kind"] != "private"
                        or constraint["person_id"] == audience["person_id"]
                    )
                )
            )
        )
        return row, dict(
            source=reference,
            state=source_state,
            actor_known=known,
            read_allowed=readable and source_state == "current",
            disclosure_allowed=bool(disclose),
            source_generation=state["generation"],
            source_sequence=max(1, state["sequence"]),
            access_version=digest([role["policy_version"], constraint, source_state]),
        )

    async def source(self, service, request):
        if service != "memory":
            raise Fault("forbidden")
        operation = request.get("operation")
        if operation not in {"source_facts", "source_check"}:
            raise Fault("invalid_input")
        self.core.contracts.check("actor-self-memory#" + operation + "_request", request)
        actor, version = request["actor_id"], request["role_version"]
        watermark = self.store.get("metadata", STATE_PREFIX + actor)
        role = await self._authority(actor, version, requested_operation=operation)
        context = request.get(
            "context", dict(purpose="reflection", audience=dict(kind="actor_only"))
        )
        allowed = None
        if context["audience"]["kind"] != "actor_only":
            allowed = await self._authority(actor, version, "context_check", context)
            if not allowed["allowed"]:
                raise Fault("forbidden")
        values = []
        for reference in request["refs"]:
            if reference["producer"] != "companion":
                raise Fault("forbidden")
            row, check = await self._current_source(actor, version, reference, role, context)
            if operation == "source_facts":
                if row is None or not check["read_allowed"]:
                    raise Fault("not_found")
                values.append(
                    dict(
                        copy.deepcopy(row["fact"]),
                        **{
                            key: check[key]
                            for key in ("source_generation", "source_sequence", "access_version")
                        },
                    )
                )
            else:
                values.append(check)
        # Both source epochs and current Platform authority are checked after owner awaits.
        closing = await self._authority(actor, version, requested_operation=operation)
        if closing != role:
            # request_id differs on each real authority read; compare its actual policy.
            if any(
                closing[key] != role[key]
                for key in ("state", "allowed_operations", "policy_version")
            ):
                raise Fault("scope_changed")
        if self.store.get("metadata", STATE_PREFIX + actor) != watermark:
            raise Fault("scope_changed")
        if allowed is not None:
            final_context = await self._authority(actor, version, "context_check", context)
            if (
                not final_context["allowed"]
                or final_context["access_version"] != allowed["access_version"]
            ):
                raise Fault("forbidden")
        response = dict(
            schema_version=1,
            request_id=request["request_id"],
            actor_id=actor,
            **{"facts" if operation == "source_facts" else "items": values},
        )
        self.core.contracts.check("actor-self-memory#" + operation + "_result", response)
        return response

    async def query(self, actor, context, text, *, kinds=None, limit=8, budget_bytes=8192):
        try:
            async with asyncio.timeout(JsonService.QUERY_BUDGET_SECONDS):
                return await self._query(
                    actor, context, text, kinds=kinds, limit=limit, budget_bytes=budget_bytes
                )
        except TimeoutError:
            error = Fault("timeout", retryable_read=True)
            error.origin_read_unavailable = True
            raise error from None

    async def _query(self, actor, context, text, *, kinds, limit, budget_bytes):
        version = await self.role_version(actor)
        response = await self._memory(
            dict(
                schema_version=1,
                request_id=uid("actor-query"),
                actor_id=actor,
                role_version=version,
                operation="query",
                text=text,
                context=context,
                kinds=["episode", "understanding", "preference", "reflection"]
                if kinds is None
                else kinds,
                limit=limit,
                budget_bytes=budget_bytes,
            )
        )
        if (
            len(canonical(response["items"]).encode()) > budget_bytes
            or len(response["items"]) > limit
        ):
            raise Fault("invalid_input")
        items, episodes = [], {}
        for item in response["items"]:
            basis = []
            for reference in item["episode_refs"]:
                key = canonical(reference)
                if key not in episodes:
                    result = await self._memory(
                        dict(
                            schema_version=1,
                            request_id=uid("actor-basis"),
                            actor_id=actor,
                            role_version=version,
                            operation="get_episode",
                            context=context,
                            **reference,
                        )
                    )
                    episodes[key] = result["episode"]["fact"]
                basis.append(dict(episode_ref=reference, fact=episodes[key]))
            whole = dict(item, basis=basis)
            if len(canonical([*items, whole]).encode()) > budget_bytes:
                response["omitted"] = True
                continue
            items.append(whole)
        response["items"] = items
        await self.check(response)
        return response

    async def check(self, material):
        refs = [row["ref"] for row in material["items"]]
        if not refs:
            # Empty recall is not an identity grant.
            value = await self._authority(
                material["actor_id"], material["role_version"], "context_check", material["context"]
            )
            if not value["allowed"]:
                raise Fault("forbidden")
            return
        response = await self._memory(
            dict(
                schema_version=1,
                request_id=uid("actor-check"),
                actor_id=material["actor_id"],
                role_version=material["role_version"],
                operation="check_memory",
                context=material["context"],
                memories=refs,
            )
        )
        if len(response["items"]) != len(refs) or any(
            row["ref"] != ref for row, ref in zip(response["items"], refs)
        ):
            raise Fault("invalid_input")
        external = material["context"]["audience"]["kind"] != "actor_only"
        if any(
            row["state"] != "current"
            or row["current_revision"] != ref["revision"]
            or not row["read_allowed"]
            or (external and not row["disclosure_allowed"])
            for row, ref in zip(response["items"], refs)
        ):
            raise dependency_changed()

    async def verify(self, check):
        await self.check(check["actor_memory_material"])

    async def recall(self, turn_id, value):
        turn = self.store.get("turns", turn_id)
        if not self.core._turn_allows(turn, "memory.read"):
            raise Fault("forbidden")
        if not isinstance(value, dict) or set(value) - {"text", "kinds", "limit"}:
            raise Fault("invalid_input")
        await self.core._preflight(turn)
        material = await self.query(
            turn["scope"]["actor_id"],
            audience_context(turn),
            value.get("text"),
            kinds=value.get("kinds"),
            limit=value.get("limit", 8),
        )
        await self.core._preflight(self.store.get("turns", turn_id))
        self.core.skills.check_selected(self.store.get("turns", turn_id), "actor_memory_recall")
        if material["items"]:
            check = scope_check(turn)
            for key in ("scope_version", "history_snapshot", "memory_snapshot"):
                del check[key]
            proof = {
                key: copy.deepcopy(material[key]) for key in ("actor_id", "role_version", "context")
            }
            proof["items"] = [dict(ref=copy.deepcopy(row["ref"])) for row in material["items"]]
            check.update(version_domain=ACTOR_MEMORY_DOMAIN, actor_memory_material=proof)
            with self.store.transaction():
                fresh = self.store.get("turns", turn_id)
                self.core._check_preparation_origin(turn, fresh)
                self.core.skills.check_selected(fresh, "actor_memory_recall")
                fresh["context_checks"] = merge_checks(fresh.get("context_checks", []), [check])
                self.core._save_turn(fresh)
        return dict(
            state="completed",
            source="actor_self_memory",
            actor_id=material["actor_id"],
            items=material["items"],
            omitted=material["omitted"],
        ), []

    async def derive(self, turn_id, call_id, value):
        turn = self.store.get("turns", turn_id)
        if not self.core._turn_allows(turn, "memory.write"):
            raise Fault("forbidden")
        if not isinstance(value, dict) or set(value) != {
            "kind",
            "text",
            "episode_refs",
            "importance",
            "retention",
        }:
            raise Fault("invalid_input")

        async def validate():
            await self.core._preflight(self.store.get("turns", turn_id))
            self.core.skills.check_selected(
                self.store.get("turns", turn_id), "actor_memory_remember"
            )

        return await self._derive(
            turn["scope"]["actor_id"],
            turn_id,
            call_id,
            value,
            (turn.get("native_execution") or {}).get("messages", []),
            validate,
        ), []

    async def activity_recall(self, context, value):
        actions = self.core.runtime_execution.actions
        activity, marker = actions.check(context)
        if not actions.allows(context, "memory.read"):
            raise Fault("forbidden")
        if not isinstance(value, dict) or set(value) - {"text", "kinds", "limit"}:
            raise Fault("invalid_input")
        if activity["scope"] is not None:
            # A private/group activity is not an actor-only authority assertion.
            raise Fault("forbidden")
        rows = self.store.db.execute(
            "SELECT body FROM outbox WHERE conversation_id=? AND status IN ('actor_memory_pending','actor_memory_unknown') AND deadline<=? ORDER BY position,id LIMIT 4",
            (context["actor_id"], self.core.clock()),
        ).fetchall()
        import json

        for row in rows:
            await self.flush(json.loads(row[0]))
        actions.check(context)
        material = await self.query(
            context["actor_id"],
            dict(purpose="plan", audience=dict(kind="actor_only")),
            value.get("text"),
            kinds=value.get("kinds"),
            limit=value.get("limit", 8),
        )
        actions.check(context)
        self.core.skills.check_activity_selected(context, "actor_memory_recall")
        marker = self.store.get("metadata", context["operation_id"])
        checks = marker.get("actor_memory_checks", [])
        checks.append(material)
        if len(checks) > 16:
            raise Fault("budget_exceeded")
        marker["actor_memory_checks"] = checks
        self.store.put("metadata", marker)
        pending = (
            self.store.db.execute(
                "SELECT 1 FROM outbox WHERE conversation_id=? AND status IN ('actor_memory_pending','actor_memory_unknown','actor_memory_submitting') LIMIT 1",
                (context["actor_id"],),
            ).fetchone()
            is not None
        )
        return dict(
            state="completed",
            source="actor_self_memory",
            actor_id=context["actor_id"],
            items=material["items"],
            omitted=material["omitted"],
            pending_experiences=pending,
        ), []

    async def activity_derive(self, context, call_id, value):
        actions = self.core.runtime_execution.actions
        activity, marker = actions.check(context)
        if activity["scope"] is not None or not actions.allows(context, "memory.write"):
            raise Fault("forbidden")
        if not isinstance(value, dict) or set(value) != {
            "kind",
            "text",
            "episode_refs",
            "importance",
            "retention",
        }:
            raise Fault("invalid_input")

        async def validate():
            actions.check(context)
            self.core.skills.check_activity_selected(context, "actor_memory_remember")
            current = self.store.get("metadata", context["operation_id"])
            for material in current.get("actor_memory_checks", []):
                await self.check(material)

        return await self._derive(
            context["actor_id"],
            context["operation_id"],
            call_id,
            value,
            marker.get("messages", []),
            validate,
        ), []

    async def _derive(self, actor, operation_id, call_id, value, messages, validate):
        # Shared dialogue/activity implementation. Only actual recall tool receipts
        # supply episode references; each caller supplies its own real authority check.
        known, pending = set(), {}
        for message in messages:
            if message.get("role") == "assistant":
                for call in message.get("tool_calls", []):
                    pending[call["id"]] = call["function"]["name"]
            elif message.get("role") == "tool":
                if pending.pop(message.get("tool_call_id"), None) != "actor_memory_recall":
                    continue
                result = json_result(message.get("content", ""))
                if result.get("source") == "actor_self_memory" and result.get("actor_id") == actor:
                    known.update(
                        canonical(reference)
                        for item in result.get("items", [])
                        for reference in item["episode_refs"]
                    )
        if (
            not isinstance(value["episode_refs"], list)
            or not value["episode_refs"]
            or any(canonical(ref) not in known for ref in value["episode_refs"])
        ):
            raise Fault("invalid_input")
        await validate()
        version = await self.role_version(actor)
        key = "actor-derived:" + digest([operation_id, value])
        marker = self.store.get("metadata", key)
        receipt = None
        if marker is not None:
            receipt = await self._receipt(marker["request"], version)
            if receipt is None and (
                marker["state"] != "not_started" or marker["request"]["role_version"] != version
            ):
                raise Fault("result_unknown", unknown=True)
        else:
            body = dict(
                schema_version=1,
                request_id=uid("actor-derived"),
                actor_id=actor,
                role_version=version,
                operation="append_derived",
                idempotency_key=digest([operation_id, value]),
                replaces=[],
                **value,
            )
            self.core.contracts.check("actor-self-memory#append_derived", body)
            marker = dict(id=key, request=body, state="not_started", tool_call_id=call_id)
        if receipt is None:
            await validate()
            marker["state"] = "unknown"
            self.store.put("metadata", marker)
            try:
                receipt = await self._memory(marker["request"])
            except Fault as error:
                if getattr(error, "actor_memory_execution", None) == "not_started":
                    marker["state"] = "not_started"
                    self.store.put("metadata", marker)
                raise
        marker.update(state="completed", receipt=receipt)
        self.store.put("metadata", marker)
        await validate()
        return receipt


def json_result(content):
    """Only the existing durable tool envelope supplies episode references."""
    from .contracts import strict_json

    try:
        value = strict_json(content)
        result = (
            value.get("result", {})
            if isinstance(value, dict) and value.get("state") == "completed"
            else {}
        )
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError):
        return {}
