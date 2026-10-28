"""The existing outbound order and receipt owner, shared by chat, Direct and proactive."""

import asyncio

from .clients import command, epoch, uid, utc
from .contracts import Fault, digest
from . import observability as obs

TERMINAL = {"sent", "failed", "cancelled", "observed", "closed_unknown"}
SOURCE_RENEWAL_MARGIN_SECONDS = 20


def _receipt_semantic(value):
    return {k: v for k, v in value.items() if k not in {"request_id", "observed_at"}}


class Delivery:
    def expression_sender(self, turn):
        sender = self._sender_for(turn)
        channel = turn["bundle"]["collection_key"]["channel"]
        check = getattr(sender, "expression_available", None)
        return sender if check and check(channel) else None

    def response_origin(self, turn):
        return dict(
            kind="response",
            actor_id=turn["scope"]["actor_id"],
            turn_id=turn["id"],
            origin=turn["origin"],
            sources=self.life.concerns.captured_sources(turn),
        )

    def _source_recovery_bound(self, turn):
        """A nonrenewable grant also bounds its live read-recovery owner.

        An existing continuation port permits the next fresh checkpoint to try
        renewing a grant, even after its old deadline. It does not authorize
        that attempt or extend any captured proof; the port and checkpoint
        still enforce the actual entry expiry, permissions and source state.
        """
        sender = self.expression_sender(turn)
        if getattr(sender, "expression_context", None) is None:
            return turn["source_deadline"]
        return None

    async def continue_delivery_origin(self, turn, *, read_budget=0):
        sender = self.expression_sender(turn)
        context = getattr(sender, "expression_context", None)
        if context is None:
            return  # Legacy transports keep their existing authorization contract.
        current = await context(
            self.response_origin(turn), turn["scope"], turn["bundle"]["collection_key"]["channel"]
        )
        if (
            current["origin"] != turn["origin"]
            or current["scope"] != turn["scope"]
            or current["channel"] != turn["bundle"]["collection_key"]["channel"]
        ):
            raise Fault("scope_changed")
        # Every checkpoint still reads the actual delivery authority. A longer
        # remote lease need not change our healthy conservative proof deadline:
        # doing so would invalidate other turns reading this same human source.
        deadline = epoch(current["expires_at"])
        now = self.clock()
        if deadline <= now:
            raise Fault("forbidden")
        with self.store.transaction():
            fresh = self.store.get("turns", turn["id"])
            durable = fresh["source_deadline"] if fresh else turn["source_deadline"]
            # Re-read after the await: another checkpoint may already have
            # renewed this source. Never overwrite it with the caller's old copy
            # or keep a deadline beyond a shorter actual authority response.
            margin = SOURCE_RENEWAL_MARGIN_SECONDS + read_budget
            adopted = deadline if durable <= now + margin else min(durable, deadline)
            if fresh and adopted != durable:
                fresh["source_deadline"] = adopted
                if (
                    adopted > durable
                    and fresh.get("bootstrap_mapping")
                    and fresh["bootstrap_until"] <= now
                ):
                    fresh["bootstrap_until"] = min(adopted, now + 5)
                # Lease bookkeeping does not revise the turn's source/event version.
                self.store.put("turns", fresh)
        turn["source_deadline"] = adopted

    async def stream_segment(self, turn_id, text="", content_refs=None):
        turn = self.store.get("turns", turn_id)
        sender = self.expression_sender(turn)
        if sender is None:
            raise Fault("dependency_unavailable")
        await self._preflight(turn)
        media = await self.reading.materialize(
            turn["scope"]["actor_id"], turn["scope"], content_refs or [], turn["origin"]
        )
        with self.store.transaction():
            turn = self.store.get("turns", turn_id)
            if turn["cancelled"] or turn["phase"] in TERMINAL:
                raise Fault("scope_changed")
            sequence = len(self._replies(turn)) + 1
            if sequence > 64:
                raise Fault("budget_exceeded")
            previous_media = [
                item
                for reply in self._replies(turn)
                for segment in reply.get("v2_request", {}).get("segments", [])
                for item in segment.get("media", [])
            ]
            if (
                len(previous_media) + len(media) > 4
                or sum(
                    3 * (len(item["data"]) // 4) - item["data"].count("=")
                    for item in previous_media + media
                )
                > 33554432
            ):
                raise Fault("budget_exceeded")
            expression_id = turn.setdefault("expression_id", "expression:" + digest(turn_id))
            segment_id = "segment:" + digest([expression_id, sequence])
            request = dict(
                schema_version=2,
                request_id="append:" + digest(segment_id),
                expression_id=expression_id,
                origin=self.response_origin(turn),
                scope=turn["scope"],
                channel=turn["bundle"]["collection_key"]["channel"],
                final=False,
                segments=[
                    dict(
                        segment_id=segment_id,
                        reply_id=segment_id,
                        segment_sequence=sequence,
                        text=text,
                        content_refs=content_refs or [],
                    )
                ],
            )
            self.contracts.check("bot-delivery#send_request", request)
            if media:
                request["segments"][0]["media"] = media
                self.contracts.check("bot-delivery#send_request", request)
            self.store.put(
                "replies",
                dict(
                    id=segment_id,
                    conversation_id=turn["conversation_id"],
                    turn_id=turn_id,
                    sequence=turn["sequence"] * 100 + sequence,
                    segment_sequence=sequence,
                    segment_count=64,
                    state="sending",
                    text=text,
                    content_refs=content_refs or [],
                    receipt=None,
                    request=None,
                    v2_request=request,
                    attempted_at=self.clock(),
                    unknown_since=None,
                ),
            )
            turn["stream_open"] = True
            turn.setdefault("expression_started_at", self.clock())
            self._save_turn(turn)
        with obs.trace_stage(self.events, "turn.delivery", uncertain=True) as observed:
            try:
                receipt = await sender.send_expression(request)
            except Exception as error:
                with self.store.transaction():
                    reply = self.store.get("replies", segment_id)
                    reply.update(state="unknown", unknown_since=self.clock())
                    self.store.put("replies", reply)
                diagnostic = obs.failure_diagnostic(
                    error,
                    code="result_unknown",
                    attempt=1,
                    stage="delivery",
                    observed_at=self.clock(),
                )
                failure = Fault(
                    "result_unknown",
                    unknown=True,
                    cause={
                        key: diagnostic[key]
                        for key in (
                            "category",
                            "peer",
                            "operation",
                            "http_status",
                            "exception_type",
                        )
                        if key in diagnostic
                    },
                )
                failure.companion_failure_stage = "delivery"
                raise failure from None
            self.record_expression(turn_id, receipt)
            observed["outcome"] = obs.runtime_outcome(receipt["state"]) or "unknown"

    def record_contact(
        self, expression_id, scope, receipt, *, kind="response", subscription_id=None
    ):
        """A contact exists only after at least one channel-confirmed sent segment."""
        if not any(
            segment["state"] in {"sent", "unknown"} and segment["channel_message_ids"]
            for segment in receipt["segments"]
        ):
            return False
        key = "contact:" + digest(expression_id)
        if prior := self.store.get("delivery_contacts", key):
            if getattr(self, "actor_memory", None) is not None and any(
                segment["state"] == "sent" and segment["channel_message_ids"]
                for segment in receipt["segments"]
            ):
                with self.store.transaction():
                    prior["confirmed_sent"] = True
                    self.store.put("delivery_contacts", prior)
                    self.actor_memory.actions.contact(prior)
            return False
        with self.store.transaction():
            self.store.put(
                "delivery_contacts",
                dict(
                    id=key,
                    conversation_id=scope["conversation_id"],
                    actor_id=scope["actor_id"],
                    person_id=scope["person_id"],
                    scope=scope,
                    expression_id=expression_id,
                    observed_at=receipt["observed_at"],
                    state="sent",
                    version=1,
                    kind=kind,
                    subscription_id=subscription_id,
                    feedback_state="waiting" if kind == "proactive" else "not_required",
                    response_deadline=self.clock() + 3600 if kind == "proactive" else None,
                ),
            )
            if getattr(self, "actor_memory", None) is not None and any(
                segment["state"] == "sent" and segment["channel_message_ids"]
                for segment in receipt["segments"]
            ):
                contact = self.store.get("delivery_contacts", key)
                contact["confirmed_sent"] = True
                self.store.put("delivery_contacts", contact)
                self.actor_memory.actions.contact(contact)
        return True

    def record_expression(self, turn_id, receipt):
        self.contracts.check("bot-delivery#send_receipt", receipt)
        with self.store.transaction():
            turn = self.store.get("turns", turn_id)
            if receipt["expression_id"] != turn.get("expression_id"):
                raise Fault("invalid_input")
            previous = turn.get("expression_receipt")

            terminal_transition = (
                turn["phase"] not in TERMINAL
                and not turn.get("stream_open")
                and receipt["final"]
                and all(
                    s["state"] not in {"queued", "sending", "unknown"} for s in receipt["segments"]
                )
            )
            if (
                previous
                and _receipt_semantic(previous) == _receipt_semantic(receipt)
                and not terminal_transition
            ):
                # Poll time/request correlation is not a new source fact. Repeated
                # pending receipts must not invalidate every reader's Core barrier.
                return
            self.store.delete("metadata", "delivery-reconcile:" + turn_id)
            for segment in receipt["segments"]:
                reply = self.store.get("replies", segment["reply_id"])
                if (
                    not reply
                    or reply["turn_id"] != turn_id
                    or reply["segment_sequence"] != segment["segment_sequence"]
                ):
                    raise Fault("invalid_input")
                state = segment["state"]
                if reply["state"] == "sent" and (
                    state != "sent"
                    or segment["channel_message_ids"] != reply["receipt"]["channel_message_ids"]
                ):
                    raise Fault("idempotency_conflict")
                previous_receipt = reply.get("receipt")
                legacy = dict(
                    schema_version=1,
                    request_id=reply["v2_request"]["request_id"],
                    reply_id=reply["id"],
                    segment_sequence=reply["segment_sequence"],
                    attempt_id=segment["receipt_id"] or "pending:" + digest(reply["id"]),
                    state=state if state in {"sent", "failed", "unknown"} else "unknown",
                    channel_message_ids=segment["channel_message_ids"],
                    observed_at=receipt["observed_at"],
                    retry_safe=segment["retry_safe"],
                )
                # Native queue states stay native locally; old read wires expose only confirmed outcomes.
                reply.update(
                    state="sending"
                    if state in {"queued", "sending"}
                    else "failed"
                    if state == "cancelled"
                    else state,
                    transport_state=state,
                    receipt=legacy if state not in {"queued", "sending"} else None,
                    expression_receipt=receipt,
                )
                if state == "unknown":
                    reply["unknown_since"] = reply["unknown_since"] or self.clock()
                self.store.put("replies", reply)
                if (
                    turn["phase"] in TERMINAL
                    and reply["receipt"]
                    and reply["receipt"] != previous_receipt
                ):
                    self._project_delivery(turn, reply, reply["receipt"])
            turn["expression_receipt"] = receipt
            self.record_contact(receipt["expression_id"], turn["scope"], receipt)
            if not turn.get("stream_open") and turn["phase"] not in TERMINAL:
                if receipt["state"] == "sent" and receipt["final"]:
                    self._finish(turn, "sent", "sent")
                elif receipt["state"] in {"failed", "partial", "cancelled"} and not any(
                    segment["state"] in {"queued", "sending", "unknown"}
                    for segment in receipt["segments"]
                ):
                    partial = any(segment["state"] == "sent" for segment in receipt["segments"])
                    self._finish(
                        turn,
                        "cancelled" if turn["cancelled"] else "failed",
                        "partial" if partial else "failed",
                    )
                else:
                    turn.update(
                        phase="reconciling",
                        unresolved_delivery=True,
                        delivery_state="unknown" if receipt["state"] == "unknown" else "partial",
                    )
                    self._save_turn(turn)
            else:
                self._save_turn(turn)

            if getattr(self, "actor_memory", None) is not None:
                self.actor_memory.actions.delivery_result(
                    "turns", turn, turn["scope"], receipt, field="expression_receipt"
                )

    async def finalize_stream(self, turn_id):
        turn = self.store.get("turns", turn_id)
        if not turn.get("expression_id"):
            return
        turn["stream_open"] = False
        with self.store.transaction():
            replies = self._replies(turn)
            for reply in replies:
                reply["segment_count"] = len(replies)
                self.store.put("replies", reply)
            if turn["phase"] not in TERMINAL:
                turn["phase"] = "reconciling"
            self._save_turn(turn)
        await self.continue_delivery_origin(turn)
        receipt = await self.expression_sender(turn).finalize_expression(
            turn["expression_id"], self.response_origin(turn)
        )
        if receipt:
            self.record_expression(turn_id, receipt)

    def _sender_for(self, turn):
        namespace = turn["bundle"]["collection_key"]["channel"]["namespace"]
        if namespace == "web":
            if self.web_sender is None:
                raise Fault("dependency_unavailable")
            return self.web_sender
        return self.sender  # The deployment binding chooses legacy or Platform bot polling.

    def open_send_band(self, conversation_key, *, unit_id, current=None, wait_for_turn=False):
        """Place one reply unit in the conversation's single increasing outbound order.

        The shared exit accepts one strictly increasing position per conversation
        (`turn_sequence * 100 + segment_sequence`), so a unit may only reuse **its own** band
        while nothing else has taken a later one - otherwise the exit rejects the reply as an
        older position and that segment is lost. Every unit therefore takes its band here:

        - a companion turn keeps one band for all of its segments, so the turn stays a single
          ordered unit and its unknown receipts and retries keep pointing at one position;
        - a functional reply must wait while a companion turn still has segments to hand to
          the exit (`wait_for_turn`), which is a legal message boundary, not a wait for the
          chat model: that turn's text already exists. When the boundary is reached the
          functional reply takes the next band and the turn is finished, so no identity is
          split. If a turn was already parked (unknown/cancelled) when another unit overtook
          it, its next segment simply takes a fresh band instead of being rejected.

        Reuse is never inferred from the number alone. Seal order (`turn_sequence`) and outbound
        order (`send_band`) are two counters: a functional reply takes a band without sealing a
        turn, so a later seal can carry a number that is already another unit's band. A band is
        therefore reusable only when it is above every band handed out so far, or when the
        recorded owner is this very unit - `current == high` with a different owner is a
        collision and must allocate instead of reusing.

        Returns `(band, waiting_on)`. `waiting_on` names the turn that must finish first, and
        `band` is None only when the conversation row is gone (caller keeps its own value).
        """
        conversation = self.store.get("conversations", conversation_key)
        if conversation is None:
            return None, None
        if wait_for_turn:
            owner = self._outbound_band_owner(conversation)
            if owner is not None:
                return None, owner
        high = conversation.get("send_band") or 0
        owner = conversation.get("send_band_owner")
        if current is not None and (current > high or (current == high and owner == unit_id)):
            # `current > high` is unused by construction (the mark only ever rises), and
            # `current == high` is this unit's own band only when the owner says so. Reusing it
            # keeps all of the unit's segments in one ordered band, so its identity survives
            # for receipts, retries and restarts. The high-water mark moves up with it,
            # otherwise a later unit could be given a band below this one and this unit's
            # remaining segments would be rejected as older positions.
            conversation["send_band"] = current
            conversation["send_band_owner"] = unit_id
            self.store.put("conversations", conversation)
            return current, None
        band = max(conversation.get("turn_sequence", 0), high) + 1
        conversation["send_band"] = band
        conversation["send_band_owner"] = unit_id
        self.store.put("conversations", conversation)
        return band, None

    def _outbound_band_owner(self, conversation):
        """The companion turn that holds the current band and still owes the exit segments.

        A turn only holds it once it has actually started handing segments over (`send_sequence`
        set or a segment no longer pending). A turn that is still generating has not taken a
        band yet, so it must never make a functional reply wait for the chat model - it takes a
        fresh band above the functional reply when its own first segment is ready instead.
        Once a turn has started, it holds the boundary to its terminal phase, because its
        remaining segments belong to the same band and would be rejected if another unit took a
        later position in between.
        """
        owner = conversation.get("send_band_owner")
        if owner is None:
            return None
        turn = self.store.get("turns", owner)
        if turn is None or turn["phase"] in TERMINAL:
            return None
        started = turn.get("send_sequence") is not None or any(
            reply["state"] != "pending" for reply in self._replies(turn)
        )
        return owner if started else None

    async def _deliver(self, cid):
        cursors = getattr(self, "_reconcile_cursors", {})
        self._reconcile_cursors = cursors
        replies = self.store.unknown_replies(cid, cursors.get(cid, ""))
        if not replies and cursors.get(cid):
            replies = self.store.unknown_replies(cid)
        if replies:
            cursors[cid] = replies[-1]["id"]
        else:
            cursors.pop(cid, None)
        for turn_id in dict.fromkeys(r["turn_id"] for r in replies):
            closed = self.store.get("turns", turn_id)
            if closed and closed["phase"] == "closed_unknown":
                with obs.correlation_scope(self._turn_correlation(closed)):
                    await self._reconcile(closed)
        reconciled = set()
        for sealed in self.store.work_turns(cid, ["reconciling"]):
            if sealed.get("expression_id") and not sealed.get("stream_open"):
                # Platform already owns this expression's outbound order. Reading
                # its terminal receipt must not wait for an older model to finish.
                # Reuse recovery: unknown results never create another send.
                with obs.correlation_scope(self._turn_correlation(sealed)):
                    await self._reconcile(sealed)
                reconciled.add(sealed["id"])
        turn = self.store.first_work_turn(cid)
        if (
            not turn
            or turn["id"] in reconciled
            or turn["phase"] not in {"ready_to_send", "sending", "reconciling"}
        ):
            return
        with obs.correlation_scope(self._turn_correlation(turn)):
            await self._deliver_turn(turn)

    def _turn_correlation(self, turn):
        if not obs.valid_correlation_id(turn.get("correlation_id")):
            turn["correlation_id"] = obs.new_correlation_id()
            self.store.put("turns", turn)
        return turn["correlation_id"]

    async def _deliver_turn(self, turn):
        if turn.get("expression_id"):
            await self._reconcile(turn)
            return
        cid = turn["conversation_id"]
        abandoned = [r for r in self._replies(turn) if r["state"] == "sending"]
        if abandoned:
            with self.store.transaction():
                for reply in abandoned:
                    reply.update(state="unknown", unknown_since=reply["attempted_at"])
                    self.store.put("replies", reply)
                turn.update(phase="reconciling", delivery_state="unknown", unresolved_delivery=True)
                self._save_turn(turn)
        if turn["phase"] == "reconciling":
            await self._reconcile(turn)
            return
        try:
            sender = self._sender_for(turn)
            if not getattr(sender, "available", True):
                raise Fault("dependency_unavailable")
            await self._preflight(turn)
        except Exception as error:
            with self.store.transaction():
                turn = self.store.get("turns", turn["id"])
                turn["failure"] = error.code if isinstance(error, Fault) else type(error).__name__
                partial = any(r["state"] == "sent" for r in self._replies(turn))
                self._finish(turn, "failed", "partial" if partial else "failed")
            return
        with self.store.transaction():
            turn = self.store.get("turns", turn["id"])
            if turn["cancelled"] or turn["phase"] in TERMINAL:
                return
            replies = self._replies(turn)
            reply = next((r for r in replies if r["state"] == "pending"), None)
            if reply is None:
                self._finish(turn, "sent", "sent")
                return
            sequence = turn.get("send_sequence") or turn["sequence"]
            band, _ = self.open_send_band(
                digest(turn["bundle"]["collection_key"]["channel"]),
                unit_id=turn["id"],
                current=sequence,
            )
            if band is not None:
                # One band for the whole turn (every segment stays in it, so the turn keeps
                # a single identity for receipts, retries and the platform's ordering), with
                # a fresh band only when another unit has already overtaken this turn.
                sequence = band
                turn["send_sequence"] = sequence
            request = dict(
                command=command(turn["origin"], reply["id"], self.clock()),
                conversation_id=cid,
                turn_id=turn["id"],
                turn_sequence=sequence,
                reply_id=reply["id"],
                actor_id=turn["scope"]["actor_id"],
                destination=turn["bundle"]["collection_key"]["channel"],
                segment_sequence=reply["segment_sequence"],
                segment_count=reply["segment_count"],
                text=reply["text"],
            )
            reply.update(state="sending", request=request, attempted_at=self.clock())
            self.store.put("replies", reply)
            turn["phase"] = "sending"
            self._save_turn(turn)
        # The intent is durable before the transport call, so an event recorded here can
        # never be the only evidence that a send was attempted.
        with obs.trace_stage(self.events, "turn.delivery", uncertain=True) as observed:
            try:
                receipt = await asyncio.wait_for(sender.send(request), timeout=20)
                self._check_receipt(request, receipt)
            except Exception:
                receipt = dict(
                    schema_version=1,
                    request_id=request["command"]["request_id"],
                    reply_id=reply["id"],
                    segment_sequence=reply["segment_sequence"],
                    attempt_id=uid("unknown"),
                    state="unknown",
                    channel_message_ids=[],
                    observed_at=utc(self.clock()),
                    retry_safe=False,
                )
            # Only the actual receipt decides a confirmed send. The trace is
            # observational and never retries an unknown transport outcome.
            self.record_receipt(reply["id"], receipt)
            observed.update(
                outcome=obs.runtime_outcome(receipt["state"]) or "unknown",
                error_code=None if receipt["state"] == "sent" else "result_unknown",
            )

    def _check_receipt(self, request, receipt):
        self.contracts.check("conversation#send_receipt", receipt)
        if (
            receipt["reply_id"] != request["reply_id"]
            or receipt["segment_sequence"] != request["segment_sequence"]
            or receipt["request_id"] != request["command"]["request_id"]
        ):
            raise Fault("invalid_input")

    def record_receipt(self, reply_id, receipt):
        """Trusted channel reconciliation callback; never exposed as an unauthenticated endpoint."""
        with self.store.transaction():
            reply = self.store.get("replies", reply_id)
            self._check_receipt(reply["request"], receipt)
            if reply["state"] in {"sent", "failed"}:
                if reply["receipt"] != receipt:
                    raise Fault("idempotency_conflict")
                return
            if reply["receipt"] and _receipt_semantic(reply["receipt"]) == _receipt_semantic(
                receipt
            ):
                return
            turn = self.store.get("turns", reply["turn_id"])
            self.store.delete("metadata", "delivery-reconcile:" + turn["id"])
            if receipt["state"] == "sent":
                self.record_contact(
                    "legacy:" + turn["id"],
                    turn["scope"],
                    dict(observed_at=receipt["observed_at"], segments=[receipt]),
                )
            reply.update(state=receipt["state"], receipt=receipt)
            if receipt["state"] == "unknown":
                reply["unknown_since"] = reply["unknown_since"] or self.clock()
            self.store.put("replies", reply)
            if turn["phase"] in TERMINAL:
                turn["unresolved_delivery"] = any(
                    r["state"] == "unknown" for r in self._replies(turn)
                )
                if turn["phase"] == "closed_unknown":
                    # Preserve the closed_unknown historical wire invariant. The
                    # per-reply projection carries the later verified fact.
                    turn["unresolved_delivery"] = True
                self._save_turn(turn)
                self._project_delivery(turn, reply, receipt)
                return
            replies = self._replies(turn)
            partial = any(r["state"] == "sent" for r in replies)
            if receipt["state"] == "unknown":
                turn.update(phase="reconciling", delivery_state="unknown", unresolved_delivery=True)
                self._save_turn(turn)
            elif turn["cancelled"]:
                self._finish(turn, "cancelled", "partial" if partial else "not_required")
            elif receipt["state"] == "failed":
                self._finish(turn, "failed", "partial" if partial else "failed")
            elif all(r["state"] == "sent" for r in replies):
                self._finish(turn, "sent", "sent")
            else:
                turn.update(phase="sending", delivery_state="partial")
                self._save_turn(turn)

    def _project_delivery(self, turn, reply, receipt):
        event_id = uid("delivery")
        conv = self.store.get("conversations", digest(turn["bundle"]["collection_key"]["channel"]))
        conv["projection_version"] = conv.get("projection_version", 0) + 1
        self.store.put("conversations", conv)
        event = dict(
            schema_version=1,
            event_id=event_id,
            event_type="conversation.projection_changed",
            owner="companion",
            aggregate_id=turn["conversation_id"],
            aggregate_version=conv["projection_version"],
            occurred_at=utc(self.clock()),
            causation_id=receipt["attempt_id"],
            cursor=event_id,
            scope_version=turn["scope_version"],
            change="delivery_changed",
            reply=dict(
                reply_id=reply["id"],
                turn_id=turn["id"],
                turn_sequence=turn["sequence"],
                actor_id=turn["scope"]["actor_id"],
                reply_sequence=reply["sequence"],
                text=reply["text"],
                delivery_target=turn["bundle"]["collection_key"]["channel"]["namespace"],
                delivery_state=receipt["state"],
                committed_at=utc(self.clock()),
            ),
        )
        self.contracts.check("web#projection_event", event)
        self.store.put(
            "outbox",
            dict(
                id=event_id,
                conversation_id=turn["conversation_id"],
                sequence=turn["sequence"],
                state="local_projection",
                event=event,
            ),
        )

    async def _reconcile(self, turn):
        deadline = self._reconciliation_deadline(turn)
        if deadline is not None and self.clock() >= deadline:
            self._close_reconciliation_if_due(turn["id"])
        schedule = self.store.get("metadata", "delivery-reconcile:" + turn["id"])
        if schedule and schedule["deadline"] > self.clock():
            return
        try:
            remaining = None if deadline is None else deadline - self.clock()
            if remaining is not None and remaining > 0:
                async with asyncio.timeout(remaining) as unknown_timeout:
                    await self._reconcile_once(turn, unknown_timeout=unknown_timeout)
            else:
                await self._reconcile_once(turn)
        except TimeoutError:
            pass  # Read closure is unknown; no send or generation is repeated.
        finally:
            self._close_reconciliation_if_due(turn["id"])
            self._defer_reconciliation(turn["id"])

    def _reconciliation_deadline(self, turn, replies=None):
        if turn["phase"] in TERMINAL:
            return None
        if turn.get("expression_id"):
            receipt = turn.get("expression_receipt")
            if receipt and not any(s["state"] == "unknown" for s in receipt["segments"]):
                return None  # Confirmed queue progress keeps its existing lifecycle.
            starts = [turn.get("expression_started_at", self.clock())]
        else:
            replies = self._replies(turn) if replies is None else replies
            starts = [
                r["unknown_since"]
                for r in replies
                if r["state"] == "unknown" and r.get("unknown_since") is not None
            ]
        return min(starts) + self.policy.delivery_reconcile_timeout_ms / 1000 if starts else None

    def _close_reconciliation_if_due(self, turn_id):
        with self.store.transaction():
            turn = self.store.get("turns", turn_id)
            if turn is None or turn["phase"] in TERMINAL:
                return
            deadline = self._reconciliation_deadline(turn)
            if deadline is None or self.clock() < deadline:
                return
            receipt = turn.get("expression_receipt")
            partial = receipt and any(s["channel_message_ids"] for s in receipt["segments"])
            self._finish(turn, "closed_unknown", "partial" if partial else "unknown")

    def _defer_reconciliation(self, turn_id):
        # Cadence is bookkeeping, not a source fact or a new work queue. Only
        # existing pending replies/expressions can cause another owner pass.
        key = "delivery-reconcile:" + turn_id
        with self.store.transaction():
            turn = self.store.get("turns", turn_id)
            if turn is None:
                return
            replies = self._replies(turn)
            unknown = [r for r in replies if r["state"] == "unknown"]
            receipt = turn.get("expression_receipt")
            pending = (
                bool(unknown)
                or turn.get("expression_id")
                and (
                    receipt is None
                    or not receipt["final"]
                    or any(
                        s["state"] in {"queued", "sending", "unknown"} for s in receipt["segments"]
                    )
                )
            )
            if not pending:
                self.store.delete("metadata", key)
                return
            previous = self.store.get("metadata", key)
            attempts = (previous or {}).get("attempts", 0) + 1
            now = self.clock()
            deadline = now + min(5.0, 0.25 * 2 ** min(attempts - 1, 5))
            terminal_deadline = self._reconciliation_deadline(turn, replies)
            if terminal_deadline is not None:
                deadline = min(deadline, terminal_deadline)
            self.store.put(
                "metadata",
                dict(
                    id=key,
                    conversation_id=turn["conversation_id"],
                    state="scheduled",
                    deadline=deadline,
                    attempts=attempts,
                ),
            )

    async def _reconcile_once(self, turn, *, unknown_timeout=None):
        if turn.get("expression_id"):
            sender = self.expression_sender(turn)
            if sender:
                try:
                    if turn["cancelled"]:
                        receipt = await sender.cancel_expression(turn["expression_id"])
                    else:
                        receipt = await sender.query_expression(turn["expression_id"])
                    if receipt:
                        self.record_expression(turn["id"], receipt)
                        if (
                            unknown_timeout is not None
                            and self._reconciliation_deadline(self.store.get("turns", turn["id"]))
                            is None
                        ):
                            # The bounded unknown lookup obtained actual queue
                            # progress. Its old horizon must not cancel the
                            # existing finalize lifecycle that follows.
                            unknown_timeout.reschedule(None)
                        if (
                            not receipt["final"]
                            and not turn.get("stream_open")
                            and not turn["cancelled"]
                        ):
                            await self.finalize_stream(turn["id"])
                except (Fault, OSError):
                    pass
            return
        for reply in self._replies(turn):
            if reply["state"] != "unknown":
                continue
            try:
                receipt = await asyncio.wait_for(
                    self._sender_for(turn).reconcile(reply["request"]), timeout=10
                )
                if receipt:
                    self.record_receipt(reply["id"], receipt)
            except (Fault, TimeoutError, OSError):
                pass
