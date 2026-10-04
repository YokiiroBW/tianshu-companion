"""Short lived feelings with scope and evidence; never changes relationship scores."""

import math
import json

from .contracts import Fault, digest, canonical
from .life import text
from .life_work import source_refs, visible

VALENCE = {"warm": 0.6, "neutral": 0.0, "dislike": -0.5, "distress": -0.7, "recovery": 0.2}


class Affect:
    def __init__(self, life):
        self.life, self.store = life, life.store

    def feedback(self, actor_id, value):
        self.life._get("actors", actor_id)
        if value["kind"] not in VALENCE:
            raise Fault("invalid_input")
        if value["scope"] is not None and value["scope"].get("actor_id") != actor_id:
            raise Fault("forbidden")
        text(value["event_id"], 128)
        text(value["reason"], 4000)
        source_refs(value["sources"])
        half_life = value["half_life_seconds"]
        if type(half_life) is not int or not 60 <= half_life <= 604800:
            raise Fault("invalid_input")
        key = "affect:" + digest([actor_id, value["event_id"]])
        old = self.store.get("life_affect", key)
        if old:
            if old["input_hash"] != digest(value):
                raise Fault("idempotency_conflict")
            return old
        item = dict(
            value,
            id=key,
            actor_id=actor_id,
            conversation_id=actor_id,
            state="active",
            valence=VALENCE[value["kind"]],
            input_hash=digest(value),
            sequence=int(self.life.clock()),
            created_at=self.life.clock(),
            version=1,
        )
        self.store.put("life_affect", item)
        return item

    def snapshot(self, actor_id, scope=None):
        now = self.life.clock()
        rows = self.store.db.execute(
            "SELECT body FROM life_affect WHERE conversation_id=? AND status='active' AND (json_extract(body,'$.scope') IS NULL OR json_extract(body,'$.scope')=?) ORDER BY position DESC,id DESC LIMIT 32",
            (actor_id, canonical(scope)),
        ).fetchall()
        items = [item for row in rows if visible(item := json.loads(row[0]), scope)]
        parts = []
        for item in items:
            elapsed = max(0, now - item["created_at"])
            weight = math.exp2(-elapsed / item["half_life_seconds"])
            if weight >= 0.01:
                parts.append(
                    dict(
                        id=item["id"],
                        kind=item["kind"],
                        reason=item["reason"],
                        intensity=round(weight, 4),
                        sources=item["sources"],
                    )
                )
        score = max(
            -1,
            min(
                1,
                sum(
                    item["valence"]
                    * math.exp2(-max(0, now - item["created_at"]) / item["half_life_seconds"])
                    for item in items
                ),
            ),
        )
        return dict(
            actor_id=actor_id,
            valence=round(score, 4),
            feelings=parts,
            observed_at=now,
            relationship_effect="none",
        )
