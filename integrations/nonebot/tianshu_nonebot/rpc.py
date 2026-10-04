"""Durable, host-independent implementation of tianshu.bot-adapter/v1.

The release builder vendors this file into each standalone host plugin.  It has
no Companion, AstrBot, NoneBot, or web-framework imports.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

PREFIX = "/tianshu/adapter/v1"
PROTOCOL = "tianshu.bot-adapter/v1"
MAX_REQUEST = 65536
MAX_MEDIA_REQUEST = 45 * 1024 * 1024
# The Platform adapter client reads at most 64 KiB of raw JSON response bytes.
MAX_RESPONSE = 65536
MAX_PENDING = 10000
# Bounds retained observation payloads, not lifetime idempotency receipts.
MAX_HISTORY = 20000
OBSERVATION_LEASE_SECONDS = 30
CLAIM_DECISION_SECONDS = 5
IDENT = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
QQ = re.compile(r"^(group|private):([1-9][0-9]*)$")
QQ_ID = re.compile(r"^[1-9][0-9]*$")


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


def request_limit(path: str) -> int:
    return (
        MAX_MEDIA_REQUEST
        if path in {PREFIX + "/messages/send", PREFIX + "/observation/messages/send"}
        else MAX_REQUEST
    )


def media_parts(delivery: dict) -> list[dict]:
    """Already-authorized inline bytes only; never fetch private URLs in a host."""
    media = delivery.get("media", [])
    refs = delivery.get("content_refs", [])
    if not isinstance(media, list) or len(media) > 4 or not isinstance(refs, list):
        _bad()
    total = 0
    for item in media:
        if not isinstance(item, dict) or set(item) != {
            "content_ref",
            "media_type",
            "encoding",
            "data",
            "sha256",
        }:
            _bad()
        ref, mime = item["content_ref"], item["media_type"]
        if (
            not isinstance(ref, dict)
            or ref not in refs
            or item["encoding"] != "base64"
            or not isinstance(item["data"], str)
        ):
            _bad()
        if mime not in {
            "image/png",
            "image/jpeg",
            "image/webp",
            "audio/wav",
            "audio/mpeg",
            "audio/ogg",
            "video/mp4",
        }:
            _bad()
        kind = mime.split("/", 1)[0]
        if ref.get("kind") != kind or ref.get("sha256") != item["sha256"]:
            _bad()
        try:
            raw = base64.b64decode(item["data"], validate=True)
        except (ValueError, binascii.Error):
            _bad()
        total += len(raw)
        if not raw or total > 32 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != item["sha256"]:
            _bad()
    if any(
        isinstance(ref, dict)
        and ref.get("kind") in {"image", "audio", "video"}
        and not any(item["content_ref"] == ref for item in media)
        for ref in refs
    ):
        _bad()
    return media


def onebot_media(media: list[dict]) -> list[dict]:
    """OneBot v11 native image/record/video file segments with exact original bytes."""
    return [
        {
            "type": "record"
            if item["media_type"].startswith("audio/")
            else item["media_type"].split("/", 1)[0],
            "data": {"file": "base64://" + item["data"]},
        }
        for item in media
    ]


def _qq(value: Any) -> str:
    if not isinstance(value, str) or not QQ.fullmatch(value):
        _bad()
    return value


def _qq_account(value: Any) -> str:
    if type(value) is not str or not QQ_ID.fullmatch(value):
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
        send_media: Callable[[str, str, str, list[dict]], Awaitable[str | int]] | None = None,
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
            CREATE INDEX IF NOT EXISTS events_unacknowledged ON events(acked) WHERE acked=0;
            CREATE TABLE IF NOT EXISTS deliveries (
                connection_id TEXT NOT NULL, reply_id TEXT NOT NULL,
                attempt_id TEXT NOT NULL, digest TEXT NOT NULL,
                receipt TEXT NOT NULL,
                PRIMARY KEY(connection_id,reply_id,attempt_id)
            );
            CREATE TABLE IF NOT EXISTS observation_accounts (
                account_id TEXT PRIMARY KEY, revision INTEGER NOT NULL,
                enabled INTEGER NOT NULL, digest TEXT NOT NULL,
                group_policy TEXT NOT NULL, private_policy TEXT NOT NULL,
                last_poll REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS observation_requests (
                request_id TEXT PRIMARY KEY, digest TEXT NOT NULL, response TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS observations (
                id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
                conversation TEXT NOT NULL, author TEXT NOT NULL,
                native_id TEXT NOT NULL, payload TEXT NOT NULL,
                acked INTEGER NOT NULL DEFAULT 0, eligible INTEGER NOT NULL,
                claimed INTEGER NOT NULL DEFAULT 0,
                claim_resolved INTEGER NOT NULL,
                claim_deadline REAL NOT NULL,
                created REAL NOT NULL,
                UNIQUE(account_id,conversation,author,native_id)
            );
            CREATE INDEX IF NOT EXISTS observations_pending
                ON observations(account_id,acked,created,id);
            CREATE TABLE IF NOT EXISTS observation_health (
                account_id TEXT PRIMARY KEY, dropped INTEGER NOT NULL DEFAULT 0,
                last_error TEXT, last_error_at REAL
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
        # ACK means the consumer has durably accepted the payload. Keep only the
        # event identity so an old SDK replay can never become a new pending event.
        self.db.execute("UPDATE events SET payload='{}' WHERE acked=1 AND payload<>'{}'")
        self.adapter, self.accounts, self.send = adapter, accounts, send
        self.send_media = send_media
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
            "FROM bindings WHERE connection_id=?",
            (connection_id,),
        ).fetchone()

    async def capture(
        self,
        account_id: str,
        conversation: str,
        author: str,
        native_id: str,
        sent_at: str,
        text: str,
        nickname: str | None = None,
        group_card: str | None = None,
    ) -> bool:
        """Return true only after an enabled, exact-author event is durable."""
        if not isinstance(text, str) or not 1 <= len(text) <= 8000:
            return False
        try:
            account_id, conversation, author, native_id = (
                _qq_account(account_id),
                _qq(conversation),
                _qq_account(author),
                _ident(native_id),
            )
        except RpcError:
            return False
        if not isinstance(sent_at, str) or len(sent_at) > 40:
            return False
        async with self.lock:
            rows = self.db.execute(
                "SELECT connection_id,authors FROM bindings "
                "WHERE account_id=? AND conversation=? AND enabled=1",
                (account_id, conversation),
            ).fetchall()
            matching = [row for row in rows if author in json.loads(row[1])]
            if len(matching) != 1:
                return False
            connection_id, _ = matching[0]
            if self.db.execute(
                "SELECT 1 FROM events WHERE connection_id=? AND native_id=? AND author=?",
                (connection_id, native_id, author),
            ).fetchone():
                return True
            count = self.db.execute("SELECT COUNT(*) FROM events WHERE acked=0").fetchone()[0]
            if count >= MAX_PENDING:
                return False
            # Message revision belongs to the event contract, not binding updates.
            payload = {
                "schema_version": 2 if nickname or group_card else 1,
                "connection_id": connection_id,
                "platform_id": self.instance_id,
                "self_id": account_id,
                "event_id": native_id,
                "revision": 1,
                "namespace": "qq",
                "conversation_id": conversation,
                "thread_id": None,
                "account_id": author,
                "sent_at": sent_at,
                "text": text,
            }
            if nickname or group_card:
                payload.update(nickname=nickname, group_card=group_card)
            self.db.execute(
                "INSERT OR IGNORE INTO events(id,connection_id,native_id,author,payload,created) "
                "VALUES(?,?,?,?,?,?)",
                (uuid.uuid4().hex, connection_id, native_id, author, _json(payload), time.time()),
            )
            return True

    async def capture_observation(
        self,
        account_id: str,
        conversation: str,
        author: str,
        native_id: str,
        sent_at: str,
        text: str,
        mentioned: bool,
        content_state: str = "text",
        identity_v3: bool = False,
        nickname: str | None = None,
        group_card: str | None = None,
    ) -> bool:
        """Durably observe an enrolled SDK account without claiming a reply."""
        try:
            account_id, conversation, author, native_id = (
                _qq_account(account_id),
                _qq(conversation),
                _qq_account(author),
                _ident(native_id),
            )
        except RpcError:
            return False
        if (
            author == account_id
            or not isinstance(sent_at, str)
            or len(sent_at) > 40
            or type(mentioned) is not bool
            or content_state not in {"text", "unsupported"}
            or not isinstance(text, str)
            or len(text) > 8000
            or (content_state == "unsupported" and text)
            or (conversation.startswith("private:") and (conversation[8:] != author or mentioned))
        ):
            return False
        try:
            if datetime.fromisoformat(sent_at.replace("Z", "+00:00")).tzinfo is None:
                return False
        except ValueError:
            return False
        async with self.lock:
            enrolled = self.db.execute(
                "SELECT enabled,revision,group_policy,private_policy,last_poll "
                "FROM observation_accounts WHERE account_id=?",
                (account_id,),
            ).fetchone()
            if not enrolled or not enrolled[0]:
                return False
            kind, target = conversation.split(":", 1)
            policy = json.loads(enrolled[2 if kind == "group" else 3])
            if not policy["observe"]:
                return False
            old = self.db.execute(
                "SELECT 1 FROM observations WHERE account_id=? AND conversation=? "
                "AND author=? AND native_id=?",
                (account_id, conversation, author, native_id),
            ).fetchone()
            if old:
                return True
            if (
                self.db.execute("SELECT COUNT(*) FROM observations WHERE acked=0").fetchone()[0]
                >= MAX_PENDING
            ):
                self._observation_drop(account_id, "pending_capacity")
                return False
            # Acknowledged host rows are transport history, not the authoritative archive.
            self.db.execute(
                "DELETE FROM observations WHERE acked=1 AND created<?", (time.time() - 30 * 86400,)
            )
            history = self.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            if history >= MAX_HISTORY:
                self.db.execute(
                    "DELETE FROM observations WHERE id IN (SELECT id FROM observations "
                    "WHERE acked=1 ORDER BY created,id LIMIT ?)",
                    (history - MAX_HISTORY + 1,),
                )
            if self.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] >= MAX_HISTORY:
                self._observation_drop(account_id, "history_capacity")
                return False
            payload = {
                "schema_version": 3 if identity_v3 else 2,
                "platform_id": self.instance_id,
                "self_id": account_id,
                "namespace": "qq",
                "conversation_id": conversation,
                "account_id": author,
                "event_id": native_id,
                "revision": 1,
                "sent_at": sent_at,
                "text": text,
                "content_state": content_state,
                "mentioned": mentioned,
                "scope_revision": enrolled[1],
            }
            if identity_v3:
                payload.update(nickname=nickname, group_card=group_card)
            matched = target in policy["list"]
            permitted = (policy["mode"] == "whitelist" and matched) or (
                policy["mode"] == "blacklist" and not matched
            )
            eligible = bool(
                time.time() - enrolled[4] <= OBSERVATION_LEASE_SECONDS
                and policy["observe"]
                and permitted
                and content_state == "text"
                and text.strip()
                and (kind == "private" or mentioned)
            )
            self.db.execute(
                "INSERT INTO observations(id,account_id,conversation,author,native_id,payload,"
                "eligible,claim_resolved,claim_deadline,created) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    uuid.uuid4().hex,
                    account_id,
                    conversation,
                    author,
                    native_id,
                    _json(payload),
                    int(eligible),
                    int(not eligible),
                    time.time() + CLAIM_DECISION_SECONDS,
                    time.time(),
                ),
            )
            return True

    def _observation_drop(self, account_id: str, code: str) -> None:
        self.db.execute(
            "INSERT INTO observation_health VALUES(?,1,?,?) "
            "ON CONFLICT(account_id) DO UPDATE SET dropped=dropped+1,"
            "last_error=excluded.last_error,last_error_at=excluded.last_error_at",
            (account_id, code, time.time()),
        )

    async def observation_claimed(
        self, account_id: str, conversation: str, author: str, native_id: str
    ) -> bool:
        async with self.lock:
            row = self.db.execute(
                "SELECT claimed,eligible,payload,claim_resolved,claim_deadline "
                "FROM observations WHERE account_id=? AND conversation=? "
                "AND author=? AND native_id=?",
                (account_id, conversation, author, native_id),
            ).fetchone()
            if not row:
                return False
            if row[0]:
                # Ownership was already confirmed and persisted. Redelivery must
                # not hand the same native message to another reply plugin.
                return True
            if row[3]:
                return False
            if time.time() >= row[4]:
                self._resolve_observation_claim(account_id, conversation, author, native_id, False)
                return False
            current = self.db.execute(
                "SELECT enabled,revision,group_policy,private_policy,last_poll "
                "FROM observation_accounts WHERE account_id=?",
                (account_id,),
            ).fetchone()
            if not current or not current[0]:
                self._resolve_observation_claim(account_id, conversation, author, native_id, False)
                return False
            event = json.loads(row[2])
            if event["scope_revision"] != current[1]:
                self._resolve_observation_claim(account_id, conversation, author, native_id, False)
                return False
            if time.time() - current[4] > OBSERVATION_LEASE_SECONDS:
                self._resolve_observation_claim(account_id, conversation, author, native_id, False)
                return False
            kind, target = conversation.split(":", 1)
            policy = json.loads(current[2 if kind == "group" else 3])
            matched = target in policy["list"]
            permitted = bool(
                policy["observe"]
                and (
                    (policy["mode"] == "whitelist" and matched)
                    or (policy["mode"] == "blacklist" and not matched)
                )
            )
            self._resolve_observation_claim(account_id, conversation, author, native_id, permitted)
            return permitted

    def _resolve_observation_claim(self, account_id, conversation, author, native_id, claimed):
        self.db.execute(
            "UPDATE observations SET claim_resolved=1,claimed=? WHERE account_id=? "
            "AND conversation=? AND author=? AND native_id=? AND claim_resolved=0",
            (int(claimed), account_id, conversation, author, native_id),
        )

    def authorized(self, authorization):
        return hmac.compare_digest(
            (authorization or "").encode("utf-8"), ("Bearer " + self.access_key).encode("utf-8")
        )

    async def handle(self, path: str, authorization: str | None, body: bytes) -> tuple[int, dict]:
        if not self.authorized(authorization):
            return 401, {"code": "unauthorized", "retryable": False}
        if len(body) > request_limit(path):
            return 400, {"code": "invalid_input", "retryable": False}
        try:
            request = json.loads(body)
            if not isinstance(request, dict):
                _bad()
            async with self.lock:
                routes = {
                    PREFIX + "/observation/capabilities": self._observation_capabilities,
                    PREFIX + "/observation/apply": self._observation_apply,
                    PREFIX + "/observation/status": self._observation_status,
                    PREFIX + "/observation/poll": self._observation_poll,
                    PREFIX + "/observation/ack": self._observation_ack,
                    PREFIX + "/observation/messages/send": self._observation_send,
                    PREFIX + "/observation/messages/status": self._observation_send_status,
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
            accounts.append(
                {
                    "id": identifier,
                    "platform": "qq",
                    "label": str(label)[:128] if label else identifier,
                }
            )
        return {
            "protocol": PROTOCOL,
            "adapter": self.adapter,
            "instance_id": self.instance_id,
            "accounts": accounts,
            "capabilities": ["text"],
            "max_outbound_utf8_bytes": 32768,
        }

    async def _apply(self, request: dict) -> dict:
        request_id = _ident(request.get("request_id"))
        connection_id = _ident(request.get("connection_id"))
        revision = request.get("revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            _bad()
        account_id = _qq_account(request.get("account_id"))
        conversation = request.get("conversation")
        if not isinstance(conversation, dict) or set(conversation) != {"kind", "id"}:
            _bad()
        target = _qq(f"{conversation['kind']}:{conversation['id']}")
        authors = request.get("allowed_authors")
        if not isinstance(authors, list) or len(authors) > 256:
            _bad()
        authors = sorted(set(_qq_account(value) for value in authors))
        enabled = request.get("enabled")
        if not isinstance(enabled, bool) or (enabled and not authors):
            _bad()
        semantic = {
            "connection_id": connection_id,
            "revision": revision,
            "account_id": account_id,
            "conversation": target,
            "authors": authors,
            "enabled": enabled,
        }
        digest = _digest(semantic)
        prior_request = self.db.execute(
            "SELECT digest,response FROM apply_requests WHERE request_id=?", (request_id,)
        ).fetchone()
        if prior_request:
            if prior_request[0] != digest:
                raise RpcError(409, "idempotency_conflict")
            return json.loads(prior_request[1])
        current = self._binding(connection_id)
        if current and revision <= current[0]:
            if revision == current[0] and digest == current[5]:
                response = {
                    "connection_id": connection_id,
                    "revision": revision,
                    "enabled": enabled,
                }
                self.db.execute(
                    "INSERT INTO apply_requests VALUES(?,?,?)",
                    (request_id, digest, _json(response)),
                )
                return response
            raise RpcError(409, "version_conflict")
        if enabled:
            online = {
                str(item["id"])
                for item in await self.accounts()
                if isinstance(item, dict) and item.get("platform") == "qq" and "id" in item
            }
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
            self.db.execute(
                "DELETE FROM events WHERE connection_id=? AND acked=0", (connection_id,)
            )
            self.db.execute(
                "INSERT INTO apply_requests VALUES(?,?,?)", (request_id, digest, _json(response))
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        return response

    async def _observation_capabilities(self, request: dict) -> dict:
        if request:
            _bad()
        result = await self._capabilities({})
        return {
            "protocol": "tianshu.bot-observation/v2",
            "adapter": self.adapter,
            "instance_id": self.instance_id,
            "accounts": result["accounts"],
            "content": ["text", "unsupported"],
            "reply_policy": "platform_owned",
        }

    async def _observation_apply(self, request: dict) -> dict:
        if set(request) != {
            "request_id",
            "account_id",
            "revision",
            "enabled",
            "group_policy",
            "private_policy",
        }:
            _bad()
        request_id = _ident(request["request_id"])
        account_id = _qq_account(request["account_id"])
        revision = request["revision"]
        enabled = request["enabled"]
        if type(revision) is not int or revision < 1 or type(enabled) is not bool:
            _bad()
        policies = {}
        for kind in ("group", "private"):
            policy = request[kind + "_policy"]
            if (
                not isinstance(policy, dict)
                or set(policy) != {"observe", "mode", "list"}
                or type(policy["observe"]) is not bool
                or policy["mode"] not in {"observe_only", "whitelist", "blacklist"}
                or not isinstance(policy["list"], list)
                or len(policy["list"]) > 256
                or len(set(policy["list"])) != len(policy["list"])
            ):
                _bad()
            for item in policy["list"]:
                if not isinstance(item, str) or not QQ_ID.fullmatch(item):
                    _bad()
            policies[kind + "_policy"] = policy
        semantic = {"account_id": account_id, "revision": revision, "enabled": enabled, **policies}
        digest = _digest(semantic)
        prior = self.db.execute(
            "SELECT digest,response FROM observation_requests WHERE request_id=?", (request_id,)
        ).fetchone()
        if prior:
            if prior[0] != digest:
                raise RpcError(409, "idempotency_conflict")
            return json.loads(prior[1])
        current = self.db.execute(
            "SELECT revision,digest FROM observation_accounts WHERE account_id=?", (account_id,)
        ).fetchone()
        if current and revision <= current[0]:
            if revision != current[0] or digest != current[1]:
                raise RpcError(409, "version_conflict")
        if enabled:
            online = {
                str(item.get("id"))
                for item in await self.accounts()
                if isinstance(item, dict) and item.get("platform") == "qq"
            }
            if account_id not in online:
                raise RpcError(503, "dependency_unavailable", True)
        response = dict(semantic)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "INSERT INTO observation_accounts VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(account_id) DO UPDATE SET revision=excluded.revision,"
                "enabled=excluded.enabled,digest=excluded.digest,"
                "group_policy=excluded.group_policy,private_policy=excluded.private_policy,"
                "last_poll=excluded.last_poll",
                (
                    account_id,
                    revision,
                    int(enabled),
                    digest,
                    _json(policies["group_policy"]),
                    _json(policies["private_policy"]),
                    time.time(),
                ),
            )
            self.db.execute(
                "INSERT INTO observation_requests VALUES(?,?,?)",
                (request_id, digest, _json(response)),
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        return response

    async def _observation_status(self, request: dict) -> dict:
        if set(request) != {"account_id"}:
            _bad()
        account_id = _qq_account(request["account_id"])
        row = self.db.execute(
            "SELECT revision,enabled,last_poll FROM observation_accounts WHERE account_id=?",
            (account_id,),
        ).fetchone()
        pending = self.db.execute(
            "SELECT COUNT(*) FROM observations WHERE account_id=? AND acked=0", (account_id,)
        ).fetchone()[0]
        health = self.db.execute(
            "SELECT dropped,last_error,last_error_at FROM observation_health WHERE account_id=?",
            (account_id,),
        ).fetchone()
        return {
            "found": bool(row),
            "account": None
            if not row
            else {
                "account_id": account_id,
                "revision": row[0],
                "enabled": bool(row[1]),
                "lease_active": time.time() - row[2] <= OBSERVATION_LEASE_SECONDS,
                "pending": pending,
                "dropped": health[0] if health else 0,
                "last_error": health[1] if health else None,
                "last_error_at": health[2] if health else None,
            },
        }

    async def _observation_poll(self, request: dict) -> dict:
        if set(request) != {"account_id", "limit"}:
            _bad()
        account_id = _qq_account(request["account_id"])
        limit = request["limit"]
        if type(limit) is not int or not 1 <= limit <= 20:
            _bad()
        self.db.execute(
            "UPDATE observation_accounts SET last_poll=? WHERE account_id=? AND enabled=1",
            (time.time(), account_id),
        )
        self.db.execute(
            "UPDATE observations SET claim_resolved=1 WHERE account_id=? "
            "AND acked=0 AND claim_resolved=0 AND claim_deadline<=?",
            (account_id, time.time()),
        )
        rows = self.db.execute(
            "SELECT id,payload,claimed FROM observations WHERE account_id=? AND acked=0 "
            "AND claim_resolved=1 "
            "ORDER BY created,id LIMIT ?",
            (account_id, limit),
        ).fetchall()
        events = []
        size = len(_json({"events": []}).encode("utf-8"))
        for key, value, claimed in rows:
            item = {"id": key, "event": json.loads(value), "reply_claimed": bool(claimed)}
            addition = len(_json(item).encode("utf-8")) + (1 if events else 0)
            if size + addition > MAX_RESPONSE:
                if not events:
                    raise RpcError(503, "dependency_unavailable", True)
                break
            events.append(item)
            size += addition
        return {"events": events}

    async def _observation_ack(self, request: dict) -> dict:
        if set(request) != {"account_id", "event_ids"}:
            _bad()
        account_id = _qq_account(request["account_id"])
        ids = request["event_ids"]
        if not isinstance(ids, list) or len(ids) > 20:
            _bad()
        acknowledged = []
        for key in dict.fromkeys(_ident(item) for item in ids):
            if self.db.execute(
                "SELECT 1 FROM observations WHERE id=? AND account_id=?", (key, account_id)
            ).fetchone():
                self.db.execute("UPDATE observations SET acked=1 WHERE id=?", (key,))
                acknowledged.append(key)
        return {"acknowledged": acknowledged}

    async def _observation_send(self, request: dict) -> dict:
        if set(request) != {"connection_id", "account_id", "policy_revision", "delivery"}:
            _bad()
        connection_id = _ident(request["connection_id"])
        account_id = _qq_account(request["account_id"])
        revision = request["policy_revision"]
        delivery = request["delivery"]
        if (
            type(revision) is not int
            or revision < 1
            or not isinstance(delivery, dict)
            or set(delivery) - {"content_refs", "media"}
            != {
                "reply_id",
                "attempt_id",
                "namespace",
                "conversation_id",
                "thread_id",
                "text",
                "turn_id",
                "segment_sequence",
            }
        ):
            _bad()
        reply_id = _ident(delivery.get("reply_id"))
        attempt_id = _ident(delivery.get("attempt_id"))
        _ident(delivery.get("turn_id"))
        if type(delivery.get("segment_sequence")) is not int or delivery["segment_sequence"] < 1:
            _bad()
        key = (connection_id, reply_id, attempt_id)
        semantic = _digest(request)
        old = self.db.execute(
            "SELECT digest,receipt FROM deliveries WHERE connection_id=? AND reply_id=? AND attempt_id=?",
            key,
        ).fetchone()
        if old:
            if old[0] != semantic:
                raise RpcError(409, "idempotency_conflict")
            receipt = json.loads(old[1])
            if receipt["state"] == "inflight":
                receipt["state"] = "unknown"
            return receipt
        row = self.db.execute(
            "SELECT enabled,revision,group_policy,private_policy FROM observation_accounts "
            "WHERE account_id=?",
            (account_id,),
        ).fetchone()
        if not row or not row[0] or row[1] != revision:
            raise RpcError(403, "forbidden")
        target = _qq(delivery.get("conversation_id"))
        kind, number = target.split(":", 1)
        policy = json.loads(row[2 if kind == "group" else 3])
        permitted = (policy["mode"] == "whitelist" and number in policy["list"]) or (
            policy["mode"] == "blacklist" and number not in policy["list"]
        )
        text = delivery.get("text")
        media = media_parts(delivery)
        if (
            not policy["observe"]
            or not permitted
            or delivery.get("namespace") != "qq"
            or delivery.get("thread_id") is not None
            or not isinstance(text, str)
            or not text
            or len(text.encode("utf-8")) > 32768
        ):
            raise RpcError(403, "forbidden")
        receipt = {
            "reply_id": reply_id,
            "attempt_id": attempt_id,
            "state": "inflight",
            "channel_message_ids": [],
        }
        self.db.execute(
            "INSERT INTO deliveries VALUES(?,?,?,?,?)", (*key, semantic, _json(receipt))
        )
        if media and self.send_media is None:
            receipt["state"] = "failed"
            self.db.execute(
                "UPDATE deliveries SET receipt=? WHERE connection_id=? AND reply_id=? AND attempt_id=?",
                (_json(receipt), *key),
            )
            return receipt
        try:
            action = (
                self.send_media(account_id, target, text, media)
                if media
                else self.send(account_id, target, text)
            )
            native_id = await asyncio.wait_for(action, 15)
            if isinstance(native_id, bool) or not isinstance(native_id, (str, int)):
                raise ValueError("native receipt missing")
            native_id = str(native_id)
            if not native_id or len(native_id) > 128 or any(ch.isspace() for ch in native_id):
                raise ValueError("native receipt missing")
            receipt.update(state="sent", channel_message_ids=[native_id])
        except Exception:
            receipt["state"] = "unknown"
        self.db.execute(
            "UPDATE deliveries SET receipt=? WHERE connection_id=? AND reply_id=? AND attempt_id=?",
            (_json(receipt), *key),
        )
        return receipt

    async def _observation_send_status(self, request: dict) -> dict:
        if set(request) != {"connection_id", "reply_id", "attempt_id"}:
            _bad()
        return await self._send_status(request)

    async def _binding_status(self, request: dict) -> dict:
        connection_id = _ident(request.get("connection_id"))
        row = self._binding(connection_id)
        return {
            "found": bool(row),
            "binding": None
            if not row
            else {"connection_id": connection_id, "revision": row[0], "enabled": bool(row[4])},
        }

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
            "ORDER BY created,id LIMIT ?",
            (connection_id, limit),
        ).fetchall()
        # The Platform adapter client rejects responses larger than 64 KiB.
        # Return an ordered prefix so an ACK exposes the next pending rows.
        events = []
        size = len(_json({"events": []}).encode("utf-8"))
        for key, value in rows:
            item = {"id": key, "event": json.loads(value)}
            addition = len(_json(item).encode("utf-8")) + (1 if events else 0)
            if size + addition > MAX_RESPONSE:
                if not events:
                    raise RpcError(503, "dependency_unavailable", True)
                break
            events.append(item)
            size += addition
        return {"events": events}

    async def _ack(self, request: dict) -> dict:
        connection_id = _ident(request.get("connection_id"))
        ids = request.get("event_ids")
        if not isinstance(ids, list) or len(ids) > 20:
            _bad()
        ids = list(dict.fromkeys(_ident(value) for value in ids))
        acknowledged = []
        for key in ids:
            row = self.db.execute(
                "SELECT 1 FROM events WHERE id=? AND connection_id=?", (key, connection_id)
            ).fetchone()
            if row:
                self.db.execute("UPDATE events SET acked=1,payload='{}' WHERE id=?", (key,))
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
        binding = self._binding(connection_id)
        if not binding or not binding[4]:
            raise RpcError(403, "forbidden")
        receipt = {
            "reply_id": reply_id,
            "attempt_id": attempt_id,
            "state": "failed",
            "channel_message_ids": [],
        }
        text = delivery.get("text")
        media = media_parts(delivery)
        target = delivery.get("conversation_id")
        valid = (
            delivery.get("namespace") == "qq"
            and delivery.get("thread_id") is None
            and target == binding[2]
            and isinstance(text, str)
            and bool(text)
            and len(text.encode("utf-8")) <= 32768
        )
        if not valid or media and self.send_media is None:
            self.db.execute(
                "INSERT INTO deliveries VALUES(?,?,?,?,?)", (*key, digest, _json(receipt))
            )
            return receipt
        receipt["state"] = "inflight"
        self.db.execute("INSERT INTO deliveries VALUES(?,?,?,?,?)", (*key, digest, _json(receipt)))
        try:
            action = (
                self.send_media(binding[1], target, text, media)
                if media
                else self.send(binding[1], target, text)
            )
            native_id = await asyncio.wait_for(action, 15)
            if isinstance(native_id, bool) or not isinstance(native_id, (str, int)):
                raise ValueError("native receipt missing")
            native_id = str(native_id)
            if not native_id or len(native_id) > 128 or any(ch.isspace() for ch in native_id):
                raise ValueError("native receipt missing")
            receipt.update(state="sent", channel_message_ids=[native_id])
        except Exception:
            receipt["state"] = "unknown"
        self.db.execute(
            "UPDATE deliveries SET receipt=? WHERE connection_id=? AND reply_id=? AND attempt_id=?",
            (_json(receipt), *key),
        )
        return receipt

    async def _send_status(self, request: dict) -> dict:
        key = (
            _ident(request.get("connection_id")),
            _ident(request.get("reply_id")),
            _ident(request.get("attempt_id")),
        )
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
