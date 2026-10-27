"""Current automatic-work eligibility, separate from historical source authority."""

import copy
import json

from .clients import uid
from .contracts import Fault, canonical

PATH = "/internal/v2/observation-source/processing"


class GroupProcessing:
    def __init__(self, core, client=None):
        self.core, self.client = core, client
        self.current = {}
        self.mapping_required = set()

    @property
    def available(self):
        return bool(self.client and self.client.url and self.client.token)

    def frames(self, item):
        if item.get("processing_frames"):
            return copy.deepcopy(item["processing_frames"])
        if item.get("source_ref"):
            return (
                self.core.observations.processing_frames(item["source_ref"])
                if hasattr(self.core, "observations")
                else None
            )
        event = item.get("event", {})
        scope = item.get("scope") or event.get("scope")
        turn_id = item.get("turn_id") or event.get("aggregate_id")
        turn = self.core.store.get("turns", turn_id) if turn_id else None
        if turn is not None:
            scope = turn["scope"]
            channel = turn["bundle"]["collection_key"]["channel"]
        elif scope:
            if (
                scope.get("audience") not in {"group", "self_private"}
                or "conversation_id" not in scope
            ):
                return []
            rows = self.core.store.db.execute(
                "SELECT body FROM conversations WHERE conversation_id=? LIMIT 2",
                (scope["conversation_id"],),
            ).fetchall()
            channel = json.loads(rows[0][0]).get("channel") if len(rows) == 1 else None
        else:
            return []  # Internal actor work has no external BOT group policy.
        if scope.get("audience") not in {"group", "self_private"}:
            return []
        if channel is not None and channel.get("namespace") != "qq":
            return []
        if channel is None:
            return None  # Old opaque material with no proven native/channel mapping.
        return [dict(channel=copy.deepcopy(channel), actor_id=scope["actor_id"])]

    def paused(self, item):
        if not self.available:
            return False
        frames = self.frames(item)
        return frames is None or any(
            self.current.get(canonical(frame), {}).get("processing_enabled") is False
            for frame in frames
        )

    async def refresh(self, items):
        """Batch hints select work; every actual remote stage obtains a fresh hint."""
        if not self.available:
            return
        unique = {}
        for item in items:
            frames = self.frames(item)
            if frames is None:
                self.mapping_required.add(item["id"] if "id" in item else item["source_ref"])
                continue
            self.mapping_required.discard(item.get("id", item.get("source_ref")))
            for frame in frames:
                unique.setdefault(canonical(frame), frame)
        entries = list(unique.items())
        for offset in range(0, len(entries), 100):
            batch = entries[offset : offset + 100]
            request = dict(
                schema_version=1, request_id=uid("processing"), frames=[frame for _, frame in batch]
            )
            try:
                response = await self.client.call(PATH, request)
                results = response["frames"]
                if (
                    response.get("schema_version") != 1
                    or response.get("request_id") != request["request_id"]
                    or len(results) != len(batch)
                ):
                    raise Fault("dependency_unavailable")
                for (_, frame), result in zip(batch, results, strict=True):
                    if (
                        result.get("frame") != frame
                        or type(result.get("processing_enabled")) is not bool
                    ):
                        raise Fault("dependency_unavailable")
                    if result["processing_enabled"] and frame.get("actor_id") not in (
                        None,
                        result.get("actor_id"),
                    ):
                        raise Fault("dependency_unavailable")
                for (key, _), result in zip(batch, results, strict=True):
                    self.current[key] = copy.deepcopy(result)
            except (Fault, OSError, TimeoutError, KeyError, TypeError, ValueError):
                for key, frame in batch:
                    self.current[key] = dict(
                        frame=frame, processing_enabled=False, code="processing_unavailable"
                    )

    async def start(self, item):
        await self.refresh([item])
        return not self.paused(item)

    def status(self):
        return dict(
            enabled=self.available,
            mapping_required_observed=len(self.mapping_required),
            history_read_policy="unchanged",
        )

    async def archive_start(self, item):
        await self.refresh([item])
        if not self.available:
            return True
        frames = self.frames(item)
        return frames is not None and all(
            (value := self.current.get(canonical(frame), {})).get("processing_enabled") is True
            or (value.get("code") == "actor_unbound" and value.get("mode") != "disabled")
            for frame in frames
        )
