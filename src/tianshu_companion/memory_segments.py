"""Durable conversation segments pumped only by the existing learning worker.

The metadata rows below assign material to a segment; they are not a completion
ledger. Only Memory's original job receipt can attest that material was handled.
"""

import copy
import json

from .clients import uid
from .contracts import Fault, canonical, digest

QUIET_SECONDS = 120
MAX_WAIT_SECONDS = 600
MAX_MEMBERS = 32
MAX_TARGETS = 8
MAX_TEXT_BYTES = 48 * 1024


class MemorySegments:
    def __init__(self, core, queue):
        self.core, self.queue = core, queue

    def _collecting(self, frame):
        row = self.core.store.db.execute(
            "SELECT body FROM metadata WHERE conversation_id=? AND status='collecting' "
            "AND json_extract(body,'$.scope_key')=? ORDER BY position,id LIMIT 1",
            (self.queue, digest(frame)),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def _new(self, frame, now):
        store = self.core.store
        sequence = store.get("metadata", "memory-segment-sequence") or dict(
            id="memory-segment-sequence", value=0
        )
        sequence["value"] += 1
        store.put("metadata", sequence)
        segment_id = uid("memory-segment")
        return dict(
            id="memory-learn:" + digest(segment_id),
            conversation_id=self.queue,
            extraction_schema=6,
            segment_id=segment_id,
            scope=copy.deepcopy(frame),
            scope_key=digest(frame),
            sequence=sequence["value"],
            state="collecting",
            started_at=now,
            last_new_at=now,
            deadline=now + QUIET_SECONDS,
            materials=[],
            context_refs=[],
            context_windows=[],
            omitted_context=False,
            attempts=0,
            proposal_failures=0,
            recovery_failures={},
            output=None,
            receipt=None,
        )

    @staticmethod
    def _fits(materials):
        return (
            sum(material["member_count"] for material in materials) <= MAX_MEMBERS
            and len({material["person_id"] for material in materials}) <= MAX_TARGETS
            and sum(material["text_bytes"] for material in materials) <= MAX_TEXT_BYTES
        )

    def offer(self, frame, materials, *, context_windows=(), omitted_context=False):
        """Assign authoritative refs once, without claiming a job or a model slot.

        Material refs must be normalized by Memory across dialogue and passive
        observation. Repeated delivery never advances the quiet deadline.
        """
        store, now = self.core.store, self.core.clock()
        if not materials:
            return False
        if set(frame) != {"actor_id", "audience", "conversation_id"}:
            raise Fault("forbidden")
        # This is an already accepted source handoff, not permission to create
        # a job or learned content. Retain it even if the role was withdrawn
        # before backfill; the existing worker checks live authority before I/O.
        changed = False
        with store.transaction():
            segment = self._collecting(frame)
            if segment is not None and segment["deadline"] <= now:
                self.seal(segment, "deadline")
                segment = None
            for material in materials:
                if (
                    set(material)
                    != {"material_ref", "person_id", "text_bytes", "member_count", "source_key"}
                    or type(material["text_bytes"]) is not int
                    or material["text_bytes"] < 0
                    or type(material["member_count"]) is not int
                    or material["member_count"] < 1
                    or not isinstance(material["material_ref"], str)
                    or not material["material_ref"]
                    or not isinstance(material["person_id"], str)
                    or not material["person_id"]
                ):
                    raise Fault("invalid_input")
                key = "memory-material:" + digest([frame, material["material_ref"]])
                previous = store.get("metadata", key)
                if previous is not None:
                    if previous["material"] != material or previous["scope"] != frame:
                        raise Fault("idempotency_conflict")
                    continue
                if not self._fits([material]):
                    raise Fault("budget_exceeded")
                if segment is not None and any(
                    assigned["source_key"] == material["source_key"]
                    for assigned in segment["materials"]
                ):
                    # A selector can have successive authoritative revisions.
                    # Keep the old atomic input intact; it must pass Memory's
                    # current-source checks rather than borrow the new revision.
                    self.seal(segment, "source_revision")
                    segment = None
                if segment is not None and not self._fits(segment["materials"] + [material]):
                    if context_windows:
                        segment["omitted_context"] = True
                    self.seal(segment, "budget")
                    segment = None
                if segment is None:
                    segment = self._new(frame, now)
                segment["materials"].append(copy.deepcopy(material))
                segment["omitted_context"] |= omitted_context
                segment["last_new_at"] = now
                segment["deadline"] = min(
                    now + QUIET_SECONDS, segment["started_at"] + MAX_WAIT_SECONDS
                )
                store.put(
                    "metadata",
                    dict(
                        id=key,
                        state="assigned",
                        scope=copy.deepcopy(frame),
                        material=copy.deepcopy(material),
                        segment_id=segment["segment_id"],
                    ),
                )
                changed = True
            if segment is not None:
                # These are optional original owner-checked windows, never new
                # material. Keep bounded, exact copies; their own checks remain
                # required if Memory adopts them into the sealed input.
                known = {window["manifest"]["window_id"] for window in segment["context_windows"]}
                for window in context_windows:
                    identity = window["manifest"]["window_id"]
                    if identity not in known and len(known) < 8:
                        segment["context_windows"].append(copy.deepcopy(window))
                        known.add(identity)
                    elif identity not in known:
                        segment["omitted_context"] = True
                    if not window["manifest"]["coverage"]["complete"]:
                        segment["omitted_context"] = True
                store.put("metadata", segment)
        return changed

    def seal(self, segment, reason):
        """Freeze a create request before its first remote, potentially uncertain write."""
        if segment["state"] != "collecting":
            return
        # Optional windows may repeat old human members. Count their unique
        # public IDs, but conservatively keep new opaque material refs separate:
        # only Memory knows whether these two source representations coincide.
        # Omitting a whole context window is preferable to overfilling a valid
        # new segment or dropping any of its complete new human input.
        windows, members = [], {}
        omitted = segment["omitted_context"]
        for window in reversed(segment["context_windows"]):
            added = {row["member_id"]: row["text"] for row in window["members"]}
            if any(
                identity in members and members[identity] != text
                for identity, text in added.items()
            ):
                raise Fault("idempotency_conflict")
            combined = members | added
            if (
                sum(row["member_count"] for row in segment["materials"]) + len(combined)
                > MAX_MEMBERS
                or sum(row["text_bytes"] for row in segment["materials"])
                + sum(len(text.encode("utf-8")) for text in combined.values())
                > MAX_TEXT_BYTES
            ):
                omitted = True
                continue
            windows.insert(0, copy.deepcopy(window))
            members = combined
        request = dict(
            schema_version=1,
            segment_id=segment["segment_id"],
            frame=copy.deepcopy(segment["scope"]),
            material_refs=[material["material_ref"] for material in segment["materials"]],
            context_refs=copy.deepcopy(segment["context_refs"]),
            context_windows=windows,
            omitted_context=omitted,
        )
        while len(canonical(request).encode("utf-8")) > 262144 and request["context_windows"]:
            request["context_windows"].pop(0)
            request["omitted_context"] = True
        if len(canonical(request).encode("utf-8")) > 262144:
            raise Fault("budget_exceeded")
        segment.update(
            state="pending",
            stage="create",
            deadline=0,
            close_reason=reason,
            create_request=request,
        )
        self.core.store.put("metadata", segment)

    def seal_due(self):
        store = self.core.store
        rows = store.db.execute(
            "SELECT body FROM metadata WHERE conversation_id=? AND status='collecting' "
            "AND deadline<=? ORDER BY deadline,position,id LIMIT 64",
            (self.queue, self.core.clock()),
        ).fetchall()
        with store.transaction():
            for row in rows:
                self.seal(json.loads(row[0]), "deadline")
        return bool(rows)
