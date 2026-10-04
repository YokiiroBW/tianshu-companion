"""Actual background actor work and outbound envelopes using the existing owners."""

import copy
import json

from .clients import utc, epoch, query
from .contracts import Fault, canonical, digest, strict_json
from .role_actions import OPERATIONS
from .short_context import sources_current


class RuntimeExecution:
    adapter = "contract"
    native_delivery = True

    def __init__(self, core):
        self.core, self.store = core, core.store

    def offer_event(self, event):
        if event.get("kind") not in {
            "reading_progress",
            "writing_completed",
            "image_completed",
            "activity_boundary",
        }:
            return
        if event["kind"] == "activity_boundary":
            activity = self.store.get("life_activities", event["activity_id"])
            if not activity or activity["state"] != "completed":
                return
        for actor in event["participants"]:
            key = "proactive-event:" + digest([event["id"], actor])
            if not self.store.get("metadata", key):
                self.store.put(
                    "metadata",
                    dict(
                        id=key,
                        conversation_id="proactive:event",
                        state="pending",
                        event_id=event["id"],
                        actor_id=actor,
                        cursor=None,
                        sequence=int(self.core.clock()),
                    ),
                )

    def offer_pending_events(self):
        row = self.store.db.execute(
            "SELECT body FROM metadata WHERE conversation_id='proactive:event' AND status='pending' ORDER BY position,id LIMIT 1"
        ).fetchone()
        if not row:
            return
        task = json.loads(row[0])
        event = self.store.get("life_events", task["event_id"])
        if not event:
            task["state"] = "cancelled"
            self.store.put("metadata", task)

            return
        actor = task["actor_id"]
        rows = self.store.db.execute(
            "SELECT body FROM proactive_subscriptions WHERE status='active' AND json_extract(body,'$.actor_id')=? AND id>? ORDER BY id LIMIT 17",
            (actor, task["cursor"] or ""),
        ).fetchall()
        with self.store.transaction():
            for row in rows[:16]:
                subscription = json.loads(row[0])
                scope = {
                    key: subscription[key]
                    for key in ("actor_id", "person_id", "audience", "conversation_id")
                }
                if event.get("scope") is not None and event["scope"] != scope:
                    continue
                motive_id = "event-motive:" + digest([event["id"], subscription["id"]])
                if self.store.get("proactive_motives", motive_id):
                    continue
                self.core.proactive.register_motive(
                    actor,
                    dict(
                        id=motive_id,
                        subscription_id=subscription["id"],
                        summary=event["summary"],
                        sources=[dict(owner="companion", object_id=event["id"], version=1)],
                        content_refs=event.get("content_refs", []),
                        due_at=self.core.clock(),
                        expires_at=self.core.clock() + subscription["expiry_seconds"],
                        weight=1.0,
                    ),
                    expected=0,
                )
            task["cursor"] = (
                json.loads(rows[min(len(rows), 16) - 1][0])["id"] if rows else task["cursor"]
            )
            task["state"] = "pending" if len(rows) > 16 else "completed"
            self.store.put("metadata", task)

    def feedback_for_contacts(self):
        marker = self.store.get("metadata", "contact-feedback:cursor") or dict(
            id="contact-feedback:cursor", cursor=""
        )
        rows = self.store.db.execute(
            "SELECT body FROM delivery_contacts WHERE id>? AND json_extract(body,'$.feedback_state') IN ('waiting','unanswered') ORDER BY id LIMIT 16",
            (marker["cursor"],),
        ).fetchall()
        if not rows and marker["cursor"]:
            marker["cursor"] = ""
            rows = self.store.db.execute(
                "SELECT body FROM delivery_contacts WHERE json_extract(body,'$.feedback_state') IN ('waiting','unanswered') ORDER BY id LIMIT 16"
            ).fetchall()
        for row in rows:
            contact = json.loads(row[0])
            marker["cursor"] = contact["id"]
            subscription = self.store.get("proactive_subscriptions", contact["subscription_id"])
            if not subscription or not self.core.reading.actor_enabled(contact["actor_id"]):
                continue
            inbound = self.core.proactive.last_inbound(subscription)
            responded = inbound is not None and inbound > epoch(contact["observed_at"])
            if not responded and (
                contact["feedback_state"] != "waiting"
                or self.core.clock() < contact["response_deadline"]
            ):
                continue
            with self.store.transaction():
                contact.update(
                    feedback_state="responded" if responded else "unanswered",
                    version=contact["version"] + 1,
                )
                self.store.put("delivery_contacts", contact)
                self.core.life.affect.feedback(
                    contact["actor_id"],
                    dict(
                        scope=contact["scope"],
                        event_id=contact["id"] + (":response" if responded else ":waiting"),
                        kind="recovery" if responded else "neutral",
                        reason="收到对方的实际回应"
                        if responded
                        else "暂未收到回应；原因未知，先等一等",
                        sources=[
                            dict(
                                owner="companion",
                                object_id=contact["id"],
                                version=contact["version"],
                            )
                        ],
                        half_life_seconds=3600,
                    ),
                )
        self.store.put("metadata", marker)

    @property
    def available(self):
        return bool(getattr(self.core.sender, "available", False)) and callable(
            getattr(self.core.sender, "expression_available", None)
        )

    def sender(self, channel):
        sender = self.core.web_sender if channel["namespace"] == "web" else self.core.sender
        check = getattr(sender, "expression_available", None)
        if not check or not check(channel):
            raise Fault("dependency_unavailable")
        return sender

    async def generation(self, actor, operation):
        task = dict(actor_id=actor, generation_turn_id=operation, config_version=None)
        await self.core.life.select_generation(task)
        return dict(
            id=operation,
            actor_id=actor,
            conversation_id="life:" + digest(actor),
            config_version=task["config_version"],
        ), task

    def current_sources(self, values):
        for source in values:
            if source["owner"] != "companion":
                continue  # External ownership is checked asynchronously below.
            found = None
            for table in (
                "life_activities",
                "life_concerns",
                "life_events",
                "image_media",
                "write_revisions",
                "turns",
                "proactive_motives",
                "proactive_goals",
                "proactive_reminders",
                "delivery_contacts",
            ):
                found = self.store.get(table, source["object_id"])
                if found:
                    break
            if not found:
                raise Fault("scope_changed")
            version = (
                found["bundle"]["collection_revision"]
                if "bundle" in found
                else found.get("version", 1)
            )
            if version != source["version"] or found.get("state") in {
                "cancelled",
                "withdrawn",
                "removed",
            }:
                raise Fault("scope_changed")
            if "bundle" in found and (
                found.get("cancelled")
                or found.get("display_invalidated")
                or not sources_current(self.store, found)
            ):
                raise Fault("scope_changed")

    async def validate_sources(self, candidate, origin=None):
        self.current_sources(candidate["sources"])
        for source in candidate["sources"]:
            if source["owner"] == "companion":
                continue
            # Content refs are checked by their actual owner when materializing originals.
            if any(source in reference["sources"] for reference in candidate["content_refs"]):
                await self.core.reading.materialize(
                    candidate["actor_id"],
                    {
                        key: candidate[key]
                        for key in ("actor_id", "person_id", "audience", "conversation_id")
                    },
                    [
                        reference
                        for reference in candidate["content_refs"]
                        if source in reference["sources"]
                    ],
                    origin,
                    actor_owned=True,
                )
                continue
            if source["owner"] != "memory":
                raise Fault("dependency_unavailable")
            row = self.store.db.execute(
                "SELECT t.body FROM turns t, json_each(t.body,'$.preparation.selected_units') u WHERE t.conversation_id=? AND json_extract(t.body,'$.scope.actor_id')=? AND json_extract(t.body,'$.scope.person_id')=? AND json_extract(t.body,'$.scope.audience')=? AND json_extract(u.value,'$.record_id')=? AND json_extract(u.value,'$.record_version')=? ORDER BY t.position DESC LIMIT 1",
                (
                    candidate["conversation_id"],
                    candidate["actor_id"],
                    candidate["person_id"],
                    candidate["audience"],
                    source["object_id"],
                    source["version"],
                ),
            ).fetchone()
            if row is None:
                raise Fault("scope_changed")
            turn = json.loads(row[0])
            unit = next(
                item
                for item in turn["preparation"]["selected_units"]
                if item["record_id"] == source["object_id"]
            )
            # The source authority checks exact original revisions under the current runtime
            # publisher grant. It neither needs an expired input origin nor a top-N retrieval.
            current_version = await self.core.memory.check_sources(turn, unit["sources"])
            if current_version != turn["scope_version"]:
                raise Fault("scope_changed")

    async def activity_step(self, activity, operation):
        """Resume a real persisted checkpoint. Tools, original reads and receipts share chat owners."""
        actor = activity["actor_id"]
        self.core.reading.actor_operation(actor, activity["id"])
        schema = self.core.contracts.schemas["life-runtime"]
        operations = [
            name
            for name in OPERATIONS
            if name not in {"affect.feedback", "reading.pause", "reading.resume", "reading.close"}
        ]
        tools = [
            dict(
                type="function",
                function=dict(
                    name="life_" + name.replace(".", "_"),
                    description="Actual actor action "
                    + name
                    + "; use current CAS and original references. Sources/scope are supplied by runtime.",
                    parameters=dict(
                        type="object",
                        properties=dict(
                            expected_version=dict(type="integer", minimum=0),
                            value=self.core.role_actions._expand(
                                schema["$defs"][name.replace(".", "_") + "_value"], schema
                            ),
                        ),
                        required=["expected_version", "value"],
                        additionalProperties=False,
                    ),
                ),
            )
            for name in operations
        ]
        generation, lease = await self.generation(actor, operation)
        persona = self.core.life.persona_reader(actor)
        messages = [
            dict(
                role="system",
                content="Continue this actor's independent activity from its actual checkpoint. Calendar plans are intentions. Only successful tool receipts and original content are completed work. Never invent offline progress. Read original content before claiming understanding. Update activity.save checkpoint and next_due_at for continuation; finish or pause explicitly. No real user memory writes or permission grants.",
            ),
            dict(
                role="user",
                content=canonical(
                    dict(
                        persona=persona,
                        activity=activity,
                        concerns=self.core.life.concerns.page(
                            actor, scope=activity["scope"], open_only=True
                        )[0],
                        now=utc(self.core.clock()),
                    )
                ),
            ),
        ]
        receipts = []
        async with self.core.models:
            for _ in range(8):
                self.core.reading.actor_operation(actor, activity["id"])
                message, receipt = await self.core.gateway.complete(
                    generation, messages, tools=tools
                )
                receipts.append(receipt)
                messages.append(message)
                calls = message.get("tool_calls") or []
                if not calls:
                    break
                for call in calls:
                    try:
                        value = strict_json(call["function"]["arguments"])
                        name = next(
                            name
                            for name in operations
                            if "life_" + name.replace(".", "_") == call["function"]["name"]
                        )
                        supplied = copy.deepcopy(value["value"])
                        properties = schema["$defs"][name.replace(".", "_") + "_value"][
                            "properties"
                        ]
                        sources = [
                            dict(
                                owner="companion",
                                object_id=activity["id"],
                                version=activity["version"],
                            )
                        ]
                        if "sources" in properties:
                            supplied["sources"] = sources
                        if "scope" in properties:
                            supplied["scope"] = activity["scope"]
                        if "query" in properties:
                            if activity["scope"] is not None:
                                raise Fault("dependency_unavailable")
                            # Null-scope actor principal uses its actual activity instead of an origin.
                            supplied["query"] = None
                        for fragment in supplied.get("fragments", []):
                            fragment["sources"] = sources
                        request = dict(
                            schema_version=2,
                            request_id="activity-action:" + digest([operation, call["id"]]),
                            actor_id=actor,
                            operation=name,
                            expected_version=value["expected_version"],
                            value=supplied,
                        )
                        if name.startswith("reading.") or name == "content.acquire":
                            result = await self.core.reading.action(
                                "actor:" + actor,
                                request,
                                operation_ref=activity["id"],
                                return_content=True,
                            )
                        else:
                            self.core.contracts.check("life-runtime#manage_request", request)
                            prepared = await self.core.life_runtime.prepare_action(
                                actor, name, supplied, value["expected_version"]
                            )
                            result = self.core.life.concerns.operation(
                                "actor:" + actor,
                                request,
                                lambda: self.core.life_runtime._execute(
                                    actor,
                                    name,
                                    supplied,
                                    value["expected_version"],
                                    request["request_id"],
                                    actor,
                                    prepared=prepared,
                                ),
                            )
                        actual = result.pop("actual_content", None)
                        attachments = (
                            self.core.role_actions.content_messages(actual) if actual else []
                        )
                        if actual:
                            result["actual_content"] = {
                                key: actual[key] for key in ("text", "coverage", "complete", "gaps")
                            }
                        result = dict(state="completed", result=result)
                    except (Fault, ValueError, KeyError, StopIteration) as error:
                        result = dict(
                            state="unknown"
                            if isinstance(error, Fault) and error.unknown
                            else "rejected",
                            error_code=error.code if isinstance(error, Fault) else "invalid_input",
                        )
                        attachments = []
                    messages.append(
                        dict(role="tool", tool_call_id=call["id"], content=canonical(result))
                    )
                    messages.extend(attachments)
                if len(canonical(messages).encode()) > 4_000_000:
                    raise Fault("budget_exceeded")
            else:
                raise Fault("budget_exceeded")
        self.core.life.verify_generation_lease(lease)
        current = self.core.life.activities.get(actor, activity["id"])
        # Thinking is not a fabricated completed activity. A model that took no advancing
        # action leaves the original checkpoint paused for a deliberate next decision.
        if current["version"] == activity["version"] and current["state"] == "running":
            self.core.life.activities.transition(
                actor,
                current["id"],
                "paused",
                expected=current["version"],
                reason="等待下一步活动决定",
            )
        self.store.put(
            "metadata",
            dict(
                id="activity-result:" + digest(operation),
                activity_id=activity["id"],
                state="completed",
                receipts=receipts,
                thought=message.get("content"),
                settled_at=self.core.clock(),
            ),
        )

    async def current_context(self, candidate):
        subscription = self.store.get("proactive_subscriptions", candidate["subscription_id"])
        if (
            not subscription
            or subscription["state"] != "active"
            or subscription["config_version"] != candidate["subscription_version"]
        ):
            raise Fault("scope_changed")
        actor = self.store.get("life_actors", candidate["actor_id"])
        if (
            not self.core.reading.actor_enabled(candidate["actor_id"])
            or not actor
            or not actor.get("life_enabled", True)
            or candidate.get("runtime_epoch", actor.get("life_runtime_epoch", 0))
            != actor.get("life_runtime_epoch", 0)
        ):
            raise Fault("scope_changed")
        scope = {
            key: candidate[key] for key in ("actor_id", "person_id", "audience", "conversation_id")
        }
        origin = dict(
            kind="proactive",
            actor_id=candidate["actor_id"],
            motive_id=candidate["subject_id"],
            motive_version=candidate["subject_version"],
            sources=candidate.get("sources", []),
        )
        return await self.sender(subscription["channel"]).expression_context(
            origin, scope, subscription["channel"]
        )

    async def proactive_expression(self, candidate):
        current = await self.current_context(candidate)
        await self.validate_sources(candidate, current["origin"])
        generation, lease = await self.generation(
            candidate["actor_id"],
            "proactive-expression:" + digest([candidate["id"], candidate["subject_version"]]),
        )
        material = dict(
            summary=candidate["summary"],
            sources=candidate["sources"],
            content_refs=candidate["content_refs"],
            actor=self.core.life.persona_reader(candidate["actor_id"]),
            life=self.core.life.summary(candidate["actor_id"], current["scope"]),
            short_affect=self.core.life.affect.snapshot(candidate["actor_id"], current["scope"]),
        )
        recall = await self.core.memory.select(
            current["origin"], current["scope"], candidate["summary"], dict(tokens=2048, bytes=8192)
        )
        material["current_person_context"] = recall["selected_units"]
        originals, attachments = [], []
        for reference in candidate["content_refs"]:
            stored = self.core.reading._reference(
                candidate["actor_id"], current["scope"], reference
            )
            coverage = reference["coverage"]
            if coverage["end"] is None:
                raise Fault("invalid_input")
            range_ = {key: coverage[key] for key in ("unit", "start", "end")}
            limits = {"characters": 4000, "pages": 2, "seconds": 30}
            if range_["unit"] in limits:
                range_["end"] = min(range_["end"], range_["start"] + limits[range_["unit"]])
            original = await self.core.reading._read(
                candidate["actor_id"],
                stored.get("scope"),
                reference,
                range_,
                query(current["origin"]),
                stored.get("operation_ref") or candidate["id"],
            )
            originals.append(
                dict(
                    content_ref=reference,
                    **{
                        field: original[field] for field in ("text", "coverage", "complete", "gaps")
                    },
                )
            )
            attachments.extend(self.core.role_actions.content_messages(original))
        material["actual_originals"] = originals
        async with self.core.models:
            output, receipt = await self.core.gateway.generate(
                generation,
                [
                    dict(
                        role="system",
                        content="Decide whether this sourced actor motive warrants contacting the person now. Use the actor's ordinary voice. Return JSON {send:boolean,text:string}. If declining, send=false and text empty. Do not invent recipient circumstances. Actual originals include exact observed coverage and gaps: do not claim anything outside them was read. No model grants or template prose.",
                    ),
                    dict(role="user", content=canonical(material)),
                ]
                + attachments,
            )
        self.core.life.verify_generation_lease(lease)
        result = strict_json("\n".join(output))
        if (
            set(result) != {"send", "text"}
            or type(result["send"]) is not bool
            or not isinstance(result["text"], str)
        ):
            raise Fault("invalid_input")
        return result["text"] if result["send"] else None, receipt

    async def dispatch(self, request):
        candidate = self.store.get("proactive_candidates", request["candidate_id"])
        sources = candidate.get("sources", [])
        current = await self.current_context(candidate)
        await self.validate_sources(candidate, current["origin"])
        scope = {
            key: request[key] for key in ("actor_id", "person_id", "audience", "conversation_id")
        }
        origin = dict(
            kind="proactive",
            actor_id=request["actor_id"],
            motive_id=request["subject_id"],
            motive_version=candidate["subject_version"],
            sources=sources,
        )
        envelope = dict(
            schema_version=2,
            request_id=request["request_id"],
            expression_id="expression:" + digest(request["candidate_id"]),
            origin=origin,
            scope=scope,
            channel=request["destination"],
            final=True,
            segments=[
                dict(
                    segment_id="segment:" + digest(request["candidate_id"]),
                    reply_id="segment:" + digest(request["candidate_id"]),
                    segment_sequence=1,
                    text=request["text"],
                    content_refs=candidate.get("content_refs", []),
                )
            ],
        )
        self.core.contracts.check("bot-delivery#send_request", envelope)
        media = await self.core.reading.materialize(
            request["actor_id"],
            scope,
            candidate.get("content_refs", []),
            current["origin"],
            actor_owned=True,
        )
        if media:
            envelope["segments"][0]["media"] = media
            self.core.contracts.check("bot-delivery#send_request", envelope)
        attempt = self.store.get("proactive_attempts", request["request_id"])
        attempt["delivery"] = envelope
        self.store.put("proactive_attempts", attempt)
        receipt = await self.sender(envelope["channel"]).send_expression(envelope)
        return self.proactive_receipt(request, receipt, scope)

    def proactive_receipt(self, request, receipt, scope):
        self.core.contracts.check("bot-delivery#send_receipt", receipt)
        if receipt["expression_id"] != "expression:" + digest(request["candidate_id"]):
            raise Fault("invalid_input")
        self.core.record_contact(
            receipt["expression_id"],
            scope,
            receipt,
            kind="proactive",
            subscription_id=self.store.get("proactive_candidates", request["candidate_id"])[
                "subscription_id"
            ],
        )
        return dict(
            adapter="contract",
            request_id=request["request_id"],
            attempt_id=request["attempt_id"],
            state=receipt["state"],
            channel_message_ids=[
                message
                for segment in receipt["segments"]
                for message in segment["channel_message_ids"]
            ],
            observed_at=receipt["observed_at"],
            native_receipt=receipt,
        )

    async def reconcile_proactive(self, attempt):
        envelope = attempt.get("delivery")
        if not envelope:
            return None
        candidate = self.store.get("proactive_candidates", attempt["request"]["candidate_id"])
        sender = self.sender(envelope["channel"])
        if candidate and candidate["state"] == "cancelled":
            receipt = await sender.cancel_expression(envelope["expression_id"])
        else:
            receipt = await sender.query_expression(envelope["expression_id"])
        return (
            self.proactive_receipt(attempt["request"], receipt, envelope["scope"])
            if receipt
            else None
        )

    def direct_available(self, request):
        try:
            self.sender(request["channel"])
            return True
        except Fault:
            return False

    async def deliver_direct(self, request_id):
        direct = self.core.direct
        with self.store.transaction():
            item = self.store.get("direct_requests", request_id)
            if not item or item["reply_state"] != "ready_to_deliver":
                return direct.request_view(request_id)
            if not direct._authorize(item)["allowed"]:
                raise Fault("forbidden")
            scope = dict(
                actor_id=item["actor_id"],
                person_id=item["person_id"],
                audience=item["audience"],
                conversation_id=item["conversation_id"],
            )
            origin = dict(
                kind="direct",
                actor_id=item["actor_id"],
                direct_request_id=item["id"],
                origin=item["origin_ref"],
                sources=[],
            )
            segment = "segment:" + digest([request_id, "reply"])
            envelope = dict(
                schema_version=2,
                request_id="direct-delivery:" + digest(request_id),
                expression_id="expression:" + digest(request_id),
                origin=origin,
                scope=scope,
                channel=item["channel"],
                final=True,
                segments=[
                    dict(
                        segment_id=segment,
                        reply_id=segment,
                        segment_sequence=1,
                        text=item["reply"]["text"],
                        content_refs=[],
                    )
                ],
            )
            self.core.contracts.check("bot-delivery#send_request", envelope)
            item.update(
                reply_state="submitting",
                delivery_v2=envelope,
                updated_at=self.core.clock(),
                version=item["version"] + 1,
            )
            self.store.put("direct_requests", item)
        try:
            receipt = await self.sender(item["channel"]).send_expression(envelope)
        except (Fault, OSError):
            receipt = None
        self.settle_direct(request_id, receipt)
        return direct.request_view(request_id)

    def settle_direct(self, request_id, receipt):
        with self.store.transaction():
            item = self.store.get("direct_requests", request_id)
            if receipt:
                self.core.contracts.check("bot-delivery#send_receipt", receipt)
                if receipt["expression_id"] != item["delivery_v2"]["expression_id"]:
                    raise Fault("invalid_input")
                self.core.record_contact(
                    receipt["expression_id"], item["delivery_v2"]["scope"], receipt
                )
            item.update(
                reply_state=receipt["state"] if receipt else "unknown",
                delivery_receipt=receipt,
                unresolved=not receipt or receipt["state"] in {"queued", "sending", "unknown"},
                updated_at=self.core.clock(),
                version=item["version"] + 1,
            )
            self.store.put("direct_requests", item)
            self.core.direct._notify(request_id)

    async def reconcile_direct(self, item):
        sender = self.sender(item["channel"])
        expression_id = item["delivery_v2"]["expression_id"]
        receipt = await (
            sender.cancel_expression(expression_id)
            if item.get("cancel_requested")
            else sender.query_expression(expression_id)
        )
        if receipt:
            self.settle_direct(item["id"], receipt)

    async def image_notices(self):
        """Finish an explicit user image request, including after a worker restart."""
        rows = self.store.db.execute(
            "SELECT body FROM metadata WHERE id LIKE 'image-notice:%' AND status IN ('waiting','queued','sending','unknown') ORDER BY id LIMIT 4"
        ).fetchall()
        for row in rows:
            notice = json.loads(row[0])
            job = self.store.get("image_jobs", notice["job_id"])
            if not job:
                continue
            turn = self.store.get("turns", notice["turn_id"])
            if not turn or turn["cancelled"]:
                notice["state"] = "cancelled"
                self.store.put("metadata", notice)
                continue
            if job["state"] in {"failed", "cancelled"}:
                notice["state"] = job["state"]
                self.store.put("metadata", notice)
                continue
            if job["state"] != "completed":
                continue
            try:
                sender = self.sender(notice["channel"])
                if notice.get("envelope"):
                    receipt = await sender.query_expression(notice["envelope"]["expression_id"])
                    if receipt is None:
                        continue  # Missing ACK is not proof of nonexecution.
                else:
                    await self.core._preflight(turn)
                    references = [
                        self.store.get("image_media", artifact["media_id"])["content_ref"]
                        for artifact in job["artifacts"]
                    ]
                    media = await self.core.reading.materialize(
                        notice["actor_id"], notice["scope"], references, notice["origin"]
                    )
                    expression = "image-expression:" + digest(job["id"])
                    segment = "image-segment:" + digest(job["id"])
                    envelope = dict(
                        schema_version=2,
                        request_id="image-send:" + digest(job["id"]),
                        expression_id=expression,
                        origin=self.core.response_origin(turn),
                        scope=notice["scope"],
                        channel=notice["channel"],
                        final=True,
                        segments=[
                            dict(
                                segment_id=segment,
                                reply_id=segment,
                                segment_sequence=1,
                                text="",
                                content_refs=references,
                                media=media,
                            )
                        ],
                    )
                    self.core.contracts.check("bot-delivery#send_request", envelope)
                    notice.update(state="unknown", envelope=envelope)
                    self.store.put("metadata", notice)
                    receipt = await sender.send_expression(envelope)
                self.core.record_contact(receipt["expression_id"], notice["scope"], receipt)
                notice.update(state=receipt["state"], receipt=receipt)
                self.store.put("metadata", notice)
            except (Fault, OSError):
                pass
