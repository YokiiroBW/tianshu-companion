"""Durable, host-independent implementation of tianshu.bot-adapter/v1.

The release builder vendors this file into each standalone host plugin.  It has
no Companion, AstrBot, NoneBot, or web-framework imports.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Awaitable, Callable


PREFIX = "/tianshu/adapter/v1"
PROTOCOL = "tianshu.bot-adapter/v1"
MAX_REQUEST = 65536
MAX_RESPONSE = 262144
MAX_PENDING = 10000
MAX_HISTORY = 20000
IDENT = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
QQ = re.compile(r"^(group|private):([1-9][0-9]*)$")


class RpcError(Exception):
    def __init__(self, status: int, code: str, retryable: bool = False):
        super().__init__(code)
        self.status, self.code, self.retryable = status, code, retryable


def _bad() -> None:
    raise RpcError(400, "invalid_input")


def _ident(value: Any) -> str:
    if not isinstance(value, str) or not IDENT.fullmatch(value):
        _bad()
    return value


def _qq(value: Any) -> str:
    if not isinstance(value, str) or not QQ.fullmatch(value):
        _bad()
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class AdapterService:
    """One SQLite owner. The caller supplies actual connected SDK accounts and send."""

    def __init__(
        self,
        path: Path,
        adapter: str,
        accounts: Callable[[], Awaitable[list[dict[str, str]]]],
        send: Callable[[str, str, str], Awaitable[str | int]],
    ):
        if adapter not in {"astrbot", "nonebot"}:
            raise ValueError("adapter")
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS bindings (
                connection_id TEXT PRIMARY KEY, revision INTEGER NOT NULL,
                account_id TEXT NOT NULL, conversation TEXT NOT NULL,
                authors TEXT NOT NULL, enabled INTEGER NOT NULL, digest TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS apply_requests (
                request_id TEXT PRIMARY KEY, digest TEXT NOT NULL, response TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY, connection_id TEXT NOT NULL,
                native_id TEXT NOT NULL, author TEXT NOT NULL,
                payload TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0,
                created REAL NOT NULL,
                UNIQUE(connection_id,native_id,author)
            );
            CREATE INDEX IF NOT EXISTS events_pending ON events(connection_id,acked,created);
            CREATE TABLE IF NOT EXISTS deliveries (
                connection_id TEXT NOT NULL, reply_id TEXT NOT NULL,
                attempt_id TEXT NOT NULL, digest TEXT NOT NULL,
                receipt TEXT NOT NULL,
                PRIMARY KEY(connection_id,reply_id,attempt_id)
            );
            """
        )
        try:
            if os.name != "nt":
                os.chmod(path, 0o600)
        except OSError:
            pass
        self.access_key = self._meta("access_key", lambda: secrets.token_urlsafe(48))
        self.instance_id = self._meta("instance_id", lambda: uuid.uuid4().hex)
        # An interrupted SDK call is unknown; it is never retried after restart.
        self.db.execute(
            "UPDATE deliveries SET receipt=json_set(receipt,'$.state','unknown') "
            "WHERE json_extract(receipt,'$.state')='inflight'"
        )
        self.adapter, self.accounts, self.send = adapter, accounts, send
        self.lock = asyncio.Lock()

    def _meta(self, key: str, maker: Callable[[], str]) -> str:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row:
            return row[0]
        value = maker()
        self.db.execute("INSERT INTO meta(key,value) VALUES(?,?)", (key, value))
        return value

    def close(self) -> None:
        self.db.close()

    def _binding(self, connection_id: str) -> tuple | None:
        return self.db.execute(
            "SELECT revision,account_id,conversation,authors,enabled,digest "
            "FROM bindings WHERE connection_id=?", (connection_id,)
        ).fetchone()

    async def capture(self, account_id: str, conversation: str, author: str, native_id: str,
                      sent_at: str, text: str) -> bool:
        """Return true only after an enabled, exact-author event is durable."""
        if not isinstance(text, str) or not 1 <= len(text) <= 8000:
            return False
        try:
            account_id, conversation, author, native_id = (
                _ident(account_id), _qq(conversation), _ident(author), _ident(native_id)
            )
        except RpcError:
            return False
        if not isinstance(sent_at, str) or len(sent_at) > 40:
            return False
        async with self.lock:
            rows = self.db.execute(
                "SELECT connection_id,revision,authors FROM bindings "
                "WHERE account_id=? AND conversation=? AND enabled=1",
                (account_id, conversation),
            ).fetchall()
            matching = [row for row in rows if author in json.loads(row[2])]
            if len(matching) != 1:
                return False
            connection_id, revision, _ = matching[0]
            count = self.db.execute("SELECT COUNT(*) FROM events WHERE acked=0").fetchone()[0]
            history = self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            if count >= MAX_PENDING or history >= MAX_HISTORY:
                return False
            payload = {
                "schema_version": 1, "connection_id": connection_id,
                "platform_id": self.instance_id, "self_id": account_id,
                "event_id": native_id, "revision": revision,
                "namespace": "qq", "conversation_id": conversation,
                "thread_id": None, "account_id": author,
                "sent_at": sent_at, "text": text,
            }
            self.db.execute(
                "INSERT OR IGNORE INTO events(id,connection_id,native_id,author,payload,created) "
                "VALUES(?,?,?,?,?,?)",
                (uuid.uuid4().hex, connection_id, native_id, author, _json(payload), time.time()),
            )
            return True

    async def handle(self, path: str, authorization: str | None, body: bytes) -> tuple[int, dict]:
        if not hmac.compare_digest(authorization or "", "Bearer " + self.access_key):
            return 401, {"code": "unauthorized", "retryable": False}
        if len(body) > MAX_REQUEST:
            return 400, {"code": "invalid_input", "retryable": False}
        try:
            request = json.loads(body)
            if not isinstance(request, dict):
                _bad()
            async with self.lock:
                routes = {
                    PREFIX + "/capabilities": self._capabilities,
                    PREFIX + "/bindings/apply": self._apply,
                    PREFIX + "/bindings/status": self._binding_status,
                    PREFIX + "/events/poll": self._poll,
                    PREFIX + "/events/ack": self._ack,
                    PREFIX + "/messages/send": self._send,
                    PREFIX + "/messages/status": self._send_status,
                }
                handler = routes.get(path)
                if handler is None:
                    raise RpcError(404, "not_found")
                result = await handler(request)
                if len(_json(result).encode()) > MAX_RESPONSE:
                    raise RpcError(503, "dependency_unavailable", True)
                return 200, result
        except (ValueError, TypeError, UnicodeError, KeyError):
            return 400, {"code": "invalid_input", "retryable": False}
        except RpcError as error:
            return error.status, {"code": error.code, "retryable": error.retryable}
        except sqlite3.Error:
            return 503, {"code": "dependency_unavailable", "retryable": True}
        except Exception:
            return 503, {"code": "dependency_unavailable", "retryable": True}

    async def _capabilities(self, request: dict) -> dict:
        if request:
            _bad()
        raw = await self.accounts()
        accounts = []
        for item in raw:
            if not isinstance(item, dict) or item.get("platform") != "qq":
                continue
            try:
                identifier = _ident(str(item["id"]))
            except (KeyError, RpcError):
                continue
            label = item.get("label")
            accounts.append({"id": identifier, "platform": "qq",
                             "label": str(label)[:128] if label else identifier})
        return {"protocol": PROTOCOL, "adapter": self.adapter,
                "instance_id": self.instance_id, "accounts": accounts,
                "capabilities": ["text"], "max_outbound_utf8_bytes": 32768}

    async def _apply(self, request: dict) -> dict:
        request_id = _ident(request.get("request_id"))
        connection_id = _ident(request.get("connection_id"))
        revision = request.get("revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            _bad()
        account_id = _ident(request.get("account_id"))
        conversation = request.get("conversation")
        if not isinstance(conversation, dict) or set(conversation) != {"kind", "id"}:
            _bad()
        target = _qq(f"{conversation['kind']}:{conversation['id']}")
        authors = request.get("allowed_authors")
        if not isinstance(authors, list) or len(authors) > 256:
            _bad()
        authors = sorted(set(_ident(value) for value in authors))
        enabled = request.get("enabled")
        if not isinstance(enabled, bool) or (enabled and not authors):
            _bad()
        semantic = {"connection_id": connection_id, "revision": revision,
                    "account_id": account_id, "conversation": target,
                    "authors": authors, "enabled": enabled}
        digest = _digest(semantic)
        prior_request = self.db.execute(
            "SELECT digest,response FROM apply_requests WHERE request_id=?", (request_id,)
        ).fetchone()
        if prior_request:
            if prior_request[0] != digest:
                raise RpcError(409, "idempotency_conflict")
            return json.loads(prior_request[1])
        if self.db.execute("SELECT COUNT(*) FROM apply_requests").fetchone()[0] >= MAX_HISTORY:
            raise RpcError(429, "busy", True)
        current = self._binding(connection_id)
        if current and revision <= current[0]:
            if revision == current[0] and digest == current[5]:
                response = {"connection_id": connection_id, "revision": revision,
                            "enabled": enabled}
                self.db.execute("INSERT INTO apply_requests VALUES(?,?,?)",
                                (request_id, digest, _json(response)))
                return response
            raise RpcError(409, "version_conflict")
        if enabled:
            online = {str(item["id"]) for item in await self.accounts()
                      if isinstance(item, dict) and item.get("platform") == "qq" and "id" in item}
            if account_id not in online:
                raise RpcError(503, "dependency_unavailable", True)
            for other_id, other_authors in self.db.execute(
                "SELECT connection_id,authors FROM bindings WHERE account_id=? "
                "AND conversation=? AND enabled=1 AND connection_id<>?",
                (account_id, target, connection_id),
            ):
                if set(authors) & set(json.loads(other_authors)):
                    raise RpcError(409, "idempotency_conflict")
        response = {"connection_id": connection_id, "revision": revision, "enabled": enabled}
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "INSERT INTO bindings VALUES(?,?,?,?,?,?,?) ON CONFLICT(connection_id) "
                "DO UPDATE SET revision=excluded.revision,account_id=excluded.account_id,"
                "conversation=excluded.conversation,authors=excluded.authors,"
                "enabled=excluded.enabled,digest=excluded.digest",
                (connection_id, revision, account_id, target, _json(authors), int(enabled), digest),
            )
            # A changed scope invalidates every queued event from the old scope.
            self.db.execute("DELETE FROM events WHERE connection_id=? AND acked=0", (connection_id,))
            self.db.execute("INSERT INTO apply_requests VALUES(?,?,?)",
                            (request_id, digest, _json(response)))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        return response

    async def _binding_status(self, request: dict) -> dict:
        connection_id = _ident(request.get("connection_id"))
        row = self._binding(connection_id)
        return {"found": bool(row), "binding": None if not row else {
            "connection_id": connection_id, "revision": row[0], "enabled": bool(row[4])}}

    async def _poll(self, request: dict) -> dict:
        connection_id = _ident(request.get("connection_id"))
        limit = request.get("limit")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 20:
            _bad()
        row = self._binding(connection_id)
        if not row:
            raise RpcError(404, "not_found")
        if not row[4]:
            return {"events": []}
        rows = self.db.execute(
            "SELECT id,payload FROM events WHERE connection_id=? AND acked=0 "
            "ORDER BY created,id LIMIT ?", (connection_id, limit),
        ).fetchall()
        return {"events": [{"id": key, "event": json.loads(value)} for key, value in rows]}

    async def _ack(self, request: dict) -> dict:
        connection_id = _ident(request.get("connection_id"))
        ids = request.get("event_ids")
        if not isinstance(ids, list) or len(ids) > 20:
            _bad()
        ids = list(dict.fromkeys(_ident(value) for value in ids))
        acknowledged = []
        for key in ids:
            row = self.db.execute("SELECT 1 FROM events WHERE id=? AND connection_id=?",
                                  (key, connection_id)).fetchone()
            if row:
                self.db.execute("UPDATE events SET acked=1 WHERE id=?", (key,))
                acknowledged.append(key)
        return {"acknowledged": acknowledged}

    async def _send(self, request: dict) -> dict:
        connection_id = _ident(request.get("connection_id"))
        delivery = request.get("delivery")
        if not isinstance(delivery, dict):
            _bad()
        reply_id = _ident(delivery.get("reply_id"))
        attempt_id = _ident(delivery.get("attempt_id"))
        digest = _digest(delivery)
        key = (connection_id, reply_id, attempt_id)
        previous = self.db.execute(
            "SELECT digest,receipt FROM deliveries WHERE connection_id=? AND reply_id=? AND attempt_id=?",
            key,
        ).fetchone()
        if previous:
            if previous[0] != digest:
                raise RpcError(409, "idempotency_conflict")
            receipt = json.loads(previous[1])
            if receipt["state"] == "inflight":
                receipt["state"] = "unknown"
            return receipt
        if self.db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] >= MAX_HISTORY:
            raise RpcError(429, "busy", True)
        binding = self._binding(connection_id)
        if not binding or not binding[4]:
            raise RpcError(403, "forbidden")
        receipt = {"reply_id": reply_id, "attempt_id": attempt_id,
                   "state": "failed", "channel_message_ids": []}
        text = delivery.get("text")
        target = delivery.get("conversation_id")
        valid = (delivery.get("namespace") == "qq" and
                 delivery.get("thread_id") is None and
                 target == binding[2] and isinstance(text, str) and bool(text) and
                 len(text.encode("utf-8")) <= 32768)
        if not valid:
            self.db.execute("INSERT INTO deliveries VALUES(?,?,?,?,?)", (*key, digest, _json(receipt)))
            return receipt
        receipt["state"] = "inflight"
        self.db.execute("INSERT INTO deliveries VALUES(?,?,?,?,?)", (*key, digest, _json(receipt)))
        try:
            native_id = await asyncio.wait_for(self.send(binding[1], target, text), 15)
            if isinstance(native_id, bool) or not isinstance(native_id, (str, int)):
                raise ValueError("native receipt missing")
            native_id = str(native_id)
            if not native_id or len(native_id) > 128 or any(ch.isspace() for ch in native_id):
                raise ValueError("native receipt missing")
            receipt.update(state="sent", channel_message_ids=[native_id])
        except Exception:
            receipt["state"] = "unknown"
        self.db.execute("UPDATE deliveries SET receipt=? WHERE connection_id=? AND reply_id=? "
                        "AND attempt_id=?", (_json(receipt), *key))
        return receipt

    async def _send_status(self, request: dict) -> dict:
        key = (_ident(request.get("connection_id")), _ident(request.get("reply_id")),
               _ident(request.get("attempt_id")))
        row = self.db.execute(
            "SELECT receipt FROM deliveries WHERE connection_id=? AND reply_id=? AND attempt_id=?",
            key,
        ).fetchone()
        receipt = json.loads(row[0]) if row else None
        if receipt and receipt["state"] == "inflight":
            receipt["state"] = "unknown"
        return {"found": bool(row), "receipt": receipt}


def _main() -> None:
    """Local terminal-only reveal; never exposed through RPC or ordinary logs."""
    import argparse
    parser = argparse.ArgumentParser(description="Display an installed Tianshu plugin key locally")
    parser.add_argument("command", choices=["show-key"])
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    uri = args.database.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as db:
        row = db.execute("SELECT value FROM meta WHERE key='access_key'").fetchone()
    if row is None:
        parser.error("adapter has not initialized")
    print(row[0])


if __name__ == "__main__":
    _main()
