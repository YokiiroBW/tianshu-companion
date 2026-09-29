"""Durable passive inbox. It never imports or calls dialogue dispatch."""

import json
import re
import sqlite3
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path

from .contracts import Fault, canonical, digest

QQ = re.compile(r"^[1-9][0-9]*$")
CONVERSATION = re.compile(r"^(group|private):[1-9][0-9]*$")
IDENT = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
MAX_PENDING = 10000


def validate(request):
    if not isinstance(request, dict) or set(request) != {
        "event",
        "source_ref",
        "source_digest",
        "scope_version",
        "archive_epoch",
    }:
        raise Fault("invalid_input")
    event = request["event"]
    fields = {
        "schema_version",
        "platform_id",
        "self_id",
        "namespace",
        "conversation_id",
        "account_id",
        "event_id",
        "revision",
        "sent_at",
        "text",
        "content_state",
        "mentioned",
        "scope_revision",
    }
    if not isinstance(event, dict) or set(event) != fields | (
        {"nickname", "group_card"} if event.get("schema_version") == 3 else set()
    ):
        raise Fault("invalid_input")
    if (
        type(event["schema_version"]) is not int
        or event["schema_version"] not in (2, 3)
        or event["revision"] != 1
        or event["namespace"] != "qq"
        or not IDENT.fullmatch(str(event["platform_id"]))
        or type(event["self_id"]) is not str
        or not QQ.fullmatch(event["self_id"])
        or type(event["conversation_id"]) is not str
        or not CONVERSATION.fullmatch(event["conversation_id"])
        or type(event["account_id"]) is not str
        or not QQ.fullmatch(event["account_id"])
        or event["self_id"] == event["account_id"]
        or not IDENT.fullmatch(str(event["event_id"]))
        or not isinstance(event["sent_at"], str)
        or len(event["sent_at"]) > 40
        or not isinstance(event["text"], str)
        or len(event["text"]) > 8000
        or event["content_state"] not in {"text", "unsupported"}
        or type(event["mentioned"]) is not bool
        or type(event["scope_revision"]) is not int
        or event["scope_revision"] < 1
        or type(request["scope_version"]) is not int
        or request["scope_version"] < 1
        or type(request["archive_epoch"]) is not int
        or request["archive_epoch"] < 1
        or not IDENT.fullmatch(str(request["source_ref"]))
        or not re.fullmatch(r"[0-9a-f]{64}", str(request["source_digest"]))
        or digest(event) != request["source_digest"]
    ):
        raise Fault("invalid_input")
    if event["schema_version"] == 3:
        if event["conversation_id"].startswith("private:") and event["group_card"] is not None:
            raise Fault("invalid_input")
        for name in (event["nickname"], event["group_card"]):
            if name is not None and (
                type(name) is not str
                or not 1 <= len(name.strip()) <= 80
                or any(
                    ord(char) < 32
                    or ord(char) == 127
                    or 0x202A <= ord(char) <= 0x202E
                    or 0x2066 <= ord(char) <= 0x2069
                    for char in name
                )
            ):
                raise Fault("invalid_input")
    if event["content_state"] == "unsupported" and event["text"]:
        raise Fault("invalid_input")
    if event["conversation_id"].startswith("private:") and (
        event["conversation_id"][8:] != event["account_id"] or event["mentioned"]
    ):
        raise Fault("invalid_input")
    try:
        when = datetime.fromisoformat(event["sent_at"].replace("Z", "+00:00"))
        if when.tzinfo is None:
            raise ValueError("timezone required")
    except ValueError:
        raise Fault("invalid_input") from None


class Observations:
    def __init__(self, path, memory_client):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.memory = memory_client
        with closing(self._db()) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS inbox (
                    source_ref TEXT PRIMARY KEY, digest TEXT NOT NULL,
                    instance_id TEXT NOT NULL, self_id TEXT NOT NULL,
                    conversation TEXT NOT NULL, author TEXT NOT NULL,
                    archive_epoch INTEGER NOT NULL,
                    event_id TEXT NOT NULL, observed_at REAL NOT NULL,
                    payload TEXT NOT NULL, archive_state TEXT NOT NULL,
                    error_code TEXT, updated_at REAL NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL DEFAULT 0,
                    UNIQUE(instance_id,self_id,conversation,author,event_id)
                );
                CREATE INDEX IF NOT EXISTS inbox_pending ON inbox(archive_state,observed_at,source_ref);
                CREATE INDEX IF NOT EXISTS inbox_page ON inbox(instance_id,self_id,archive_epoch,conversation,source_ref);
            """)

    def _db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def ingest(self, service, request):
        if service != "platform":
            raise Fault("forbidden")
        validate(request)
        event = request["event"]
        fingerprint = digest(request)
        with closing(self._db()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                "SELECT digest,archive_state FROM inbox WHERE source_ref=?",
                (request["source_ref"],),
            ).fetchone()
            if old:
                if old["digest"] != fingerprint:
                    raise Fault("idempotency_conflict")
                return {
                    "source_ref": request["source_ref"],
                    "state": "duplicate",
                    "archive_state": old["archive_state"],
                }
            count = db.execute(
                "SELECT COUNT(*) FROM inbox WHERE archive_state='pending_memory'"
            ).fetchone()[0]
            if count >= MAX_PENDING:
                raise Fault("dependency_unavailable")
            try:
                db.execute(
                    "INSERT INTO inbox(source_ref,digest,instance_id,self_id,conversation,"
                    "author,archive_epoch,event_id,observed_at,payload,archive_state,"
                    "error_code,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        request["source_ref"],
                        fingerprint,
                        event["platform_id"],
                        event["self_id"],
                        event["conversation_id"],
                        event["account_id"],
                        request["archive_epoch"],
                        event["event_id"],
                        time.time(),
                        canonical(request),
                        "pending_memory",
                        None,
                        time.time(),
                    ),
                )
            except sqlite3.IntegrityError:
                raise Fault("idempotency_conflict") from None
        return {
            "source_ref": request["source_ref"],
            "state": "accepted",
            "archive_state": "pending_memory",
        }

    async def flush(self):
        """Retry pending transfers; a failure never changes durable acceptance."""
        with closing(self._db()) as db:
            rows = db.execute(
                "SELECT source_ref,payload,attempts FROM inbox "
                "WHERE archive_state='pending_memory' AND next_attempt_at<=? "
                "ORDER BY next_attempt_at,observed_at,source_ref LIMIT 20",
                (time.time(),),
            ).fetchall()
        worked = False
        for row in rows:
            worked = True
            try:
                payload = json.loads(row["payload"])
                payload.pop("archive_epoch")
                response = await self.memory.call(
                    "/internal/v2/memory/observations", payload, uncertain_write=True
                )
                if (
                    response.get("source_ref") != row["source_ref"]
                    or response.get("state") not in {"accepted", "duplicate"}
                    or response.get("archive_state") != "archived"
                ):
                    raise Fault("dependency_unavailable")
                with closing(self._db()) as db, db:
                    db.execute(
                        "UPDATE inbox SET archive_state='archived',error_code=NULL,"
                        "updated_at=? WHERE source_ref=? AND archive_state='pending_memory'",
                        (time.time(), row["source_ref"]),
                    )
            except (Fault, ValueError, OSError, sqlite3.Error) as exc:
                # The accepted row remains durable. Expose a stable failure code.
                code = exc.code if isinstance(exc, Fault) else "dependency_unavailable"
                terminal = code in {
                    "forbidden",
                    "not_found",
                    "scope_changed",
                    "idempotency_conflict",
                    "invalid_input",
                }
                state = (
                    "revoked"
                    if code in {"forbidden", "not_found", "scope_changed"}
                    else ("failed" if terminal else "pending_memory")
                )
                attempts = row["attempts"] + 1
                due = 0 if terminal else time.time() + min(2 ** min(attempts, 8), 300)
                with closing(self._db()) as db, db:
                    db.execute(
                        "UPDATE inbox SET archive_state=?,error_code=?,updated_at=?,"
                        "attempts=?,next_attempt_at=? WHERE source_ref=? "
                        "AND archive_state='pending_memory'",
                        (state, code, time.time(), attempts, due, row["source_ref"]),
                    )
                continue
        return worked

    def query(self, service, request):
        if service != "platform":
            raise Fault("forbidden")
        if not isinstance(request, dict) or set(request) != {
            "instance_id",
            "self_id",
            "conversation_id",
            "limit",
            "cursor",
            "archive_epoch",
        }:
            raise Fault("invalid_input")
        instance, account, conversation = (
            request["instance_id"],
            request["self_id"],
            request["conversation_id"],
        )
        if (
            not IDENT.fullmatch(str(instance))
            or type(account) is not str
            or not QQ.fullmatch(account)
            or (
                conversation is not None
                and (type(conversation) is not str or not CONVERSATION.fullmatch(conversation))
            )
            or type(request["limit"]) is not int
            or not 1 <= request["limit"] <= 100
            or (type(request["archive_epoch"]) is not int or request["archive_epoch"] < 1)
        ):
            raise Fault("invalid_input")
        cursor = request["cursor"]
        if cursor is not None and not IDENT.fullmatch(str(cursor)):
            raise Fault("invalid_input")
        with closing(self._db()) as db:
            rows = db.execute(
                "SELECT source_ref,conversation,author,event_id,observed_at,archive_state,error_code "
                "FROM inbox WHERE instance_id=? AND self_id=? AND archive_epoch=? "
                "AND (? IS NULL OR conversation=?) AND (? IS NULL OR source_ref>?) "
                "ORDER BY source_ref LIMIT ?",
                (
                    instance,
                    account,
                    request["archive_epoch"],
                    conversation,
                    conversation,
                    cursor,
                    cursor,
                    request["limit"] + 1,
                ),
            ).fetchall()
            backlog = db.execute(
                "SELECT archive_state,COUNT(*) AS n FROM inbox "
                "WHERE instance_id=? AND self_id=? AND archive_epoch=? "
                "GROUP BY archive_state",
                (instance, account, request["archive_epoch"]),
            ).fetchall()
        return {
            "items": [dict(item) for item in rows[: request["limit"]]],
            "backlog": {item["archive_state"]: item["n"] for item in backlog},
            "next_cursor": rows[request["limit"] - 1]["source_ref"]
            if len(rows) > request["limit"]
            else None,
        }

    async def query_archive(self, service, request):
        local = self.query(service, request)
        if request["conversation_id"] is None:
            return {
                **local,
                "memory_state": "conversation_required",
                "archive_items": [],
                "archive_next_cursor": None,
            }
        try:
            remote_request = {
                key: value for key, value in request.items() if key != "archive_epoch"
            }
            remote = await self.memory.call(
                "/internal/v2/memory/observations/query", remote_request
            )
            if not isinstance(remote.get("items"), list):
                raise Fault("dependency_unavailable")
            return {
                **local,
                "memory_state": "available",
                "archive_items": remote["items"],
                "archive_next_cursor": remote.get("next_cursor"),
            }
        except Fault:
            return {
                **local,
                "memory_state": "unavailable",
                "archive_items": [],
                "archive_next_cursor": None,
            }
