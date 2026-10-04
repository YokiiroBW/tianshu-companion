"""Role-owned open intentions and independently sourced understanding fragments."""

import json

from .contracts import Fault, canonical, digest
from .life import text, timestamp

STATES = {"open", "paused", "completed", "cancelled"}


def require_version(current, expected):
    version = (current or {}).get("version", 0)
    if type(expected) is not int or expected != version:
        raise Fault("version_conflict", current_version=version)


def source_refs(values):
    if not isinstance(values, list) or len(values) > 64:
        raise Fault("invalid_input")
    for value in values:
        if (
            not isinstance(value, dict)
            or set(value) != {"owner", "object_id", "version"}
            or value["owner"] not in {"companion", "memory", "platform", "assetlibrary"}
            or type(value["version"]) is not int
            or value["version"] < 1
        ):
            raise Fault("invalid_input")
        text(value["object_id"], 128)
    return values


def visible(record, scope):
    """Actor-owned life or the exact original private scope; caller grants are checked outside."""
    return record.get("scope") is None or record["scope"] == scope


class LifeWork:
    def __init__(self, life):
        self.life, self.store = life, life.store

    def get(self, actor_id, concern_id):
        item = self.store.get("life_concerns", concern_id)
        if not item or item["actor_id"] != actor_id:
            raise Fault("not_found")
        return item

    def save(self, actor_id, value, *, expected):
        self.life._get("actors", actor_id)
        text(value["id"], 128)
        text(value["title"], 4000)
        text(value["goal"], 4000)
        if value["state"] not in STATES:
            raise Fault("invalid_input")
        if value["scope"] is not None and value["scope"].get("actor_id") != actor_id:
            raise Fault("forbidden")
        source_refs(value["sources"])
        for key in ("next_due_at", "expires_at"):
            if value[key] is not None:
                timestamp(value[key])
        if len(value["fragments"]) > 32 or len(value["result_refs"]) > 64:
            raise Fault("budget_exceeded")
        with self.store.transaction():
            old = self.store.get("life_concerns", value["id"])
            if old and old["actor_id"] != actor_id:
                raise Fault("not_found")
            require_version(old, expected)
            fragments = {part["id"]: part for part in (old or {}).get("fragments", [])}
            for part in value["fragments"]:
                text(part["id"], 128)
                text(part["text"], 4000)
                source_refs(part["sources"])
                previous = fragments.get(part["id"])
                require_version(previous, part["expected_version"])
                if part["certainty"] not in {"observed", "reported", "inferred", "uncertain"}:
                    raise Fault("invalid_input")
                if part["expires_at"] is not None:
                    timestamp(part["expires_at"])
                fragments[part["id"]] = dict(
                    {k: v for k, v in part.items() if k != "expected_version"},
                    version=(previous or {}).get("version", 0) + 1,
                    valid=True,
                )
            item = dict(
                value,
                actor_id=actor_id,
                conversation_id=actor_id,
                fragments=list(fragments.values()),
                version=expected + 1,
                sequence=int(self.life.clock()),
                deadline=value["next_due_at"],
                created_at=(old or {}).get("created_at", self.life.clock()),
                updated_at=self.life.clock(),
                intent_valid=True,
            )
            self.store.put("life_concerns", item)
            return item

    def close(self, actor_id, concern_id, *, expected, reason):
        text(reason, 4000)
        with self.store.transaction():
            item = self.get(actor_id, concern_id)
            require_version(item, expected)
            item.update(
                state="completed",
                close_reason=reason,
                next_due_at=None,
                deadline=None,
                version=expected + 1,
                updated_at=self.life.clock(),
            )
            self.store.put("life_concerns", item)
            return item

    def page(self, actor_id, *, scope=None, after=None, limit=20, open_only=False, object_id=None):
        if type(limit) is not int or not 1 <= limit <= 50:
            raise Fault("invalid_input")
        rows = self.store.db.execute(
            "SELECT body FROM life_concerns WHERE conversation_id=? AND ((? IS NULL AND id>?) OR id=?) ORDER BY id LIMIT ?",
            (actor_id, object_id, after or "", object_id, limit + 1),
        ).fetchall()
        values = [json.loads(row[0]) for row in rows]
        # Page over the source index; hidden rows still advance the cursor.
        next_cursor = values[limit - 1]["id"] if len(values) > limit else None
        items = []
        now = self.life.clock()
        for item in values[:limit]:
            if not visible(item, scope) or open_only and item["state"] != "open":
                continue
            if item["expires_at"] is not None and item["expires_at"] <= now:
                continue
            item["fragments"] = [
                part
                for part in item["fragments"]
                if part.get("valid", True)
                and (part["expires_at"] is None or part["expires_at"] > now)
            ]
            items.append(item)
        return items, next_cursor

    def invalidate(self, owner, object_id, version=None):
        """A withdrawn observation cannot erase a separately established intention."""

        def affected(values):
            return any(
                source["owner"] == owner
                and source["object_id"] == object_id
                and (version is None or source["version"] == version)
                for source in values
            )

        with self.store.transaction():
            for item in self.store.list("life_concerns"):
                changed = False
                if affected(item["sources"]):
                    item.update(intent_valid=False, state="paused", deadline=None)
                    changed = True
                for part in item["fragments"]:
                    if affected(part["sources"]) and part.get("valid", True):
                        part.update(valid=False, version=part["version"] + 1)
                        changed = True
                if changed:
                    item["version"] += 1
                    self.store.put("life_concerns", item)

    def captured_sources(self, turn, evidence=()):
        values = [
            {
                "owner": "companion",
                "object_id": turn["id"],
                "version": turn["bundle"]["collection_revision"],
            }
        ]
        values += [
            {
                "owner": "memory",
                "object_id": unit["record_id"],
                "version": unit.get("version", unit.get("revision", unit.get("record_version", 1))),
            }
            for unit in evidence
        ]
        return list({digest(source): source for source in values}.values())[:64]

    def operation(self, service, request, execute):
        """Only mutation commands need an atomic operation receipt."""
        key = "life-operation:" + digest([service, request["request_id"]])
        signature = self.signature(request)
        with self.store.transaction():
            old = self.store.get("metadata", key)
            if old:
                if old["signature"] != signature:
                    raise Fault("idempotency_conflict")
                return old["result"]
            result = execute()
            # Reject NaN and overlarge content before the mutation transaction commits.
            if len(canonical(result).encode()) > 1048576:
                raise Fault("budget_exceeded")
            self.store.put("metadata", dict(id=key, signature=signature, result=result))
            return result

    def replay(self, service, request):
        old = self.store.get(
            "metadata", "life-operation:" + digest([service, request["request_id"]])
        )
        if old and old["signature"] != self.signature(request):
            raise Fault("idempotency_conflict")
        return old["result"] if old else None

    @staticmethod
    def signature(request):
        # Origin assertions rotate. Current authorization is rechecked before ACK;
        # business scope and operation remain the stable idempotency identity.
        value = dict(request)
        payload = value.get("value")
        if isinstance(payload, dict) and "query" in payload:
            value["value"] = {key: item for key, item in payload.items() if key != "query"}
        return digest(value)
