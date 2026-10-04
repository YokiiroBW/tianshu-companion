"""Text-only AstrBot boundary and durable, conservative delivery journal.

This module deliberately has no dependency on the companion engine. Platform owns
source registration, scope, model dispatch and the Core reply queue. The plugin only
observes authenticated SDK events and sends claimed replies via that SDK.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sqlite3
import time
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlsplit

from .rpc import RpcError, media_parts

EVENT_ROUTE = "/internal/v1/bot/events"
EVENT_STATUS_ROUTE = "/internal/v1/bot/events/status"
HEARTBEAT_ROUTE = "/internal/v1/bot/heartbeat"
CLAIM_ROUTE = "/internal/v1/bot/replies/claim"
ACK_ROUTE = "/internal/v1/bot/replies/ack"
MAX_OUTBOUND_TEXT_BYTES = 32768  # Core's per-segment UTF-8 bound (core.py).
_QQ_CONVERSATION = re.compile(r"^(group|private):([1-9][0-9]*)$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


class BoundaryError(ValueError):
    """A fixed-code boundary failure; never include user content or credentials."""


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _identifier(value: Any) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise BoundaryError("invalid_identifier")
    return value


def _platform_id(value: Any) -> str:
    # AstrBot allows user-selected instance names, including non-ASCII names.
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or value != value.strip()
        or any(ord(char) < 32 for char in value)
    ):
        raise BoundaryError("invalid_platform_id")
    return value


def _message_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise BoundaryError("invalid_message_id")
    result = str(value)
    if not result or len(result) > 128 or any(c.isspace() for c in result):
        raise BoundaryError("invalid_message_id")
    return result


def _conversation(value: Any) -> str:
    if not isinstance(value, str) or not _QQ_CONVERSATION.fullmatch(value):
        raise BoundaryError("invalid_conversation")
    return value


def _qq_id(value: Any) -> str:
    if type(value) is int and value > 0:
        return str(value)
    if type(value) is str and re.fullmatch(r"[1-9][0-9]*", value):
        return value
    raise BoundaryError("invalid_qq_id")


def _display(value: Any) -> str | None:
    if type(value) is not str:
        return None
    text = "".join(
        c
        for c in value
        if ord(c) >= 32
        and ord(c) != 127
        and not 0x202A <= ord(c) <= 0x202E
        and not 0x2066 <= ord(c) <= 0x2069
    ).strip()[:80]
    return text or None


def _utc_time(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BoundaryError("invalid_timestamp")
    try:
        result = datetime.fromtimestamp(value, timezone.utc)
    except (OverflowError, OSError, ValueError) as error:
        raise BoundaryError("invalid_timestamp") from error
    return result.isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class Settings:
    base_url: str
    connection_id: str
    token: str
    platform_id: str
    self_id: str
    allowed_conversations: frozenset[str]
    ca_file: str | None = None
    trigger_prefix: str = "天枢 "
    capture_all_text: bool = False
    poll_seconds: float = 2.0
    heartbeat_seconds: float = 30.0
    allow_http_loopback: bool = False

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Settings:
        url = str(config.get("base_url") or "").rstrip("/")
        parts = urlsplit(url)
        loopback = parts.hostname in {"127.0.0.1", "::1"}
        allow_loopback = config.get("allow_http_loopback") is True
        if (
            not parts.hostname
            or parts.username
            or parts.password
            or parts.query
            or parts.fragment
            or parts.path not in {"", "/"}
            or (
                parts.scheme != "https"
                and not (parts.scheme == "http" and loopback and allow_loopback)
            )
        ):
            raise BoundaryError("invalid_base_url")
        connection_id = _identifier(config.get("connection_id"))
        platform_id = _platform_id(config.get("platform_id"))
        token = config.get("token")
        if not isinstance(token, str) or not token or any(c.isspace() for c in token):
            raise BoundaryError("invalid_token")
        self_id = _qq_id(config.get("self_id"))
        raw_allowed = config.get("allowed_conversations")
        if not isinstance(raw_allowed, list) or not raw_allowed:
            raise BoundaryError("empty_allowlist")
        allowed = frozenset(_conversation(value) for value in raw_allowed)
        prefix = config.get("trigger_prefix", "天枢 ")
        if not isinstance(prefix, str) or len(prefix) > 64:
            raise BoundaryError("invalid_trigger")
        capture_all = config.get("capture_all_text") is True
        if not capture_all and not prefix:
            raise BoundaryError("invalid_trigger")
        try:
            poll = float(config.get("poll_seconds", 2))
            heartbeat = float(config.get("heartbeat_seconds", 30))
        except (TypeError, ValueError) as error:
            raise BoundaryError("invalid_interval") from error
        if not 1 <= poll <= 60 or not 10 <= heartbeat <= 300:
            raise BoundaryError("invalid_interval")
        raw_ca_file = config.get("ca_file", "")
        if not isinstance(raw_ca_file, str) or (
            raw_ca_file and not Path(raw_ca_file).is_absolute()
        ):
            raise BoundaryError("invalid_ca_file")
        return cls(
            base_url=url,
            connection_id=connection_id,
            token=token,
            platform_id=platform_id,
            self_id=self_id,
            allowed_conversations=allowed,
            ca_file=raw_ca_file or None,
            trigger_prefix=prefix,
            capture_all_text=capture_all,
            poll_seconds=poll,
            heartbeat_seconds=heartbeat,
            allow_http_loopback=allow_loopback,
        )


def normalize_event(event: Any, settings: Settings) -> dict[str, Any] | None:
    """Return an exact QQ text event, or leave the event to other AstrBot plugins.

    AstrBot 4.27.3 sets message_obj.timestamp to conversion time and can drop an
    unsupported raw segment. Therefore use its authenticated aiocqhttp raw event
    for the original timestamp and complete OneBot message chain, while requiring
    the parsed message ID and SDK getters to agree with it.
    """
    if event.get_platform_name() != "aiocqhttp" or event.get_platform_id() != settings.platform_id:
        return None
    message = event.message_obj
    raw = getattr(message, "raw_message", None)
    if not isinstance(raw, Mapping) or raw.get("post_type") != "message":
        return None
    kind = raw.get("message_type")
    if kind not in {"group", "private"}:
        return None
    try:
        self_id = _qq_id(event.get_self_id())
        account = _qq_id(event.get_sender_id())
        raw_self = _qq_id(raw.get("self_id"))
        raw_account = _qq_id(raw.get("user_id"))
        sender = raw.get("sender")
        if isinstance(sender, Mapping) and sender.get("user_id") is not None:
            if _qq_id(sender["user_id"]) != account:
                return None
        group = _qq_id(event.get_group_id()) if kind == "group" else ""
        raw_group = _qq_id(raw.get("group_id")) if kind == "group" else ""
    except BoundaryError:
        return None
    if (
        self_id != settings.self_id
        or raw_self != self_id
        or account == self_id
        or raw_account != account
    ):
        return None
    if kind == "group" and raw_group != group:
        return None
    conversation = f"group:{group}" if group else f"private:{account}"
    if conversation not in settings.allowed_conversations:
        return None
    try:
        event_id = _message_id(message.message_id)
        if _message_id(raw.get("message_id")) != event_id:
            return None
        sent_at = _utc_time(raw.get("time"))
    except (AttributeError, BoundaryError):
        return None
    chain = raw.get("message")
    if not isinstance(chain, list) or not chain:
        return None
    texts: list[str] = []
    for segment in chain:
        if not isinstance(segment, Mapping) or not isinstance(segment.get("data"), Mapping):
            return None
        kind, data = segment.get("type"), segment["data"]
        if kind == "text" and isinstance(data.get("text"), str):
            texts.append(data["text"])
        elif kind == "at" and group:
            try:
                if _qq_id(data.get("qq")) == settings.self_id:
                    continue
            except BoundaryError:
                pass
        else:
            # This release has no asset reference, mention or reply-reference field.
            # Do not silently turn a mixed-media message into a text-only fact.
            return None
    text = "".join(texts).strip()
    if not text or text.startswith("/"):
        return None
    if not settings.capture_all_text:
        if not text.startswith(settings.trigger_prefix):
            return None
        text = text[len(settings.trigger_prefix) :].strip()
    if not 1 <= len(text) <= 8000:
        return None
    nickname = _display(sender.get("nickname")) if isinstance(sender, Mapping) else None
    group_card = _display(sender.get("card")) if group and isinstance(sender, Mapping) else None
    return {
        "schema_version": 2 if nickname or group_card else 1,
        "connection_id": settings.connection_id,
        "platform_id": settings.platform_id,
        "self_id": settings.self_id,
        "event_id": event_id,
        "revision": 1,
        "namespace": "qq",
        "conversation_id": conversation,
        "thread_id": None,
        "account_id": account,
        "sent_at": sent_at,
        "text": text,
        **({"nickname": nickname, "group_card": group_card} if nickname or group_card else {}),
    }


def _ack(
    delivery: dict[str, Any], connection_id: str, state: str, ids: list[str]
) -> dict[str, Any]:
    return {
        "connection_id": connection_id,
        "reply_id": delivery["reply_id"],
        "attempt_id": delivery["attempt_id"],
        "state": state,
        "channel_message_ids": ids,
    }


class Journal:
    """One-process SQLite owner. Pending native attempts recover as unknown."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS inbound (
              event_key TEXT PRIMARY KEY, digest TEXT NOT NULL,
              connection_id TEXT NOT NULL, event_id TEXT NOT NULL,
              account_id TEXT NOT NULL, state TEXT NOT NULL,
              response TEXT, last_checked REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS deliveries (
              reply_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
              digest TEXT NOT NULL, state TEXT NOT NULL, ack TEXT,
              ack_confirmed INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(reply_id, attempt_id)
            );
            CREATE INDEX IF NOT EXISTS inbound_reconcile ON inbound(state,last_checked);
            CREATE INDEX IF NOT EXISTS deliveries_ack_pending ON deliveries(ack_confirmed)
              WHERE ack IS NOT NULL;
            """
        )
        row = self.db.execute("SELECT value FROM meta WHERE key='instance_id'").fetchone()
        if row is None:
            self.instance_id = uuid.uuid4().hex
            self.db.execute("INSERT INTO meta VALUES ('instance_id', ?)", (self.instance_id,))
        else:
            self.instance_id = row[0]
        # A crash can occur immediately before or after the SDK call. Both are
        # indistinguishable on restart; the only safe automatic answer is unknown.
        rows = self.db.execute(
            "SELECT reply_id,attempt_id FROM deliveries WHERE state='intent'"
        ).fetchall()
        with self._transaction():
            for reply_id, attempt_id in rows:
                ack = _canonical(
                    _ack({"reply_id": reply_id, "attempt_id": attempt_id}, "", "unknown", [])
                )
                self.db.execute(
                    "UPDATE deliveries SET state='unknown',ack=? WHERE reply_id=? AND attempt_id=?",
                    (ack, reply_id, attempt_id),
                )

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def _transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def record_event(self, payload: dict[str, Any]) -> str:
        key = _digest([payload["connection_id"], payload["event_id"], payload["account_id"]])
        semantic = _digest(payload)
        with self._transaction():
            row = self.db.execute("SELECT digest FROM inbound WHERE event_key=?", (key,)).fetchone()
            if row is not None:
                return "duplicate" if row[0] == semantic else "conflict"
            self.db.execute(
                "INSERT INTO inbound (event_key,digest,connection_id,event_id,account_id,state) VALUES (?,?,?,?,?,?)",
                (
                    key,
                    semantic,
                    payload["connection_id"],
                    payload["event_id"],
                    payload["account_id"],
                    "intent",
                ),
            )
        return "new"

    def finish_event(self, payload: dict[str, Any], response: dict[str, Any] | None) -> None:
        key = _digest([payload["connection_id"], payload["event_id"], payload["account_id"]])
        state = response["state"] if response else "unknown"
        self.db.execute(
            "UPDATE inbound SET state=?,response=? WHERE event_key=?",
            (state, _canonical(response) if response else None, key),
        )

    def events_to_check(self) -> list[dict[str, str]]:
        rows = self.db.execute(
            "SELECT event_key,connection_id,event_id,account_id FROM inbound "
            "WHERE state IN ('intent','unknown') AND last_checked<? "
            "ORDER BY last_checked LIMIT 10",
            (time.time() - 30,),
        ).fetchall()
        return [
            dict(event_key=key, connection_id=connection, event_id=event, account_id=account)
            for key, connection, event, account in rows
        ]

    def record_event_status(self, event_key: str, response: dict[str, Any] | None) -> None:
        state = response["state"] if response else "unknown"
        self.db.execute(
            "UPDATE inbound SET state=?,response=COALESCE(?,response),last_checked=? WHERE event_key=?",
            (state, _canonical(response) if response else None, time.time(), event_key),
        )

    def record_delivery(
        self, delivery: dict[str, Any], connection_id: str
    ) -> tuple[str, dict[str, Any] | None]:
        reply_id, attempt_id = delivery["reply_id"], delivery["attempt_id"]
        semantic = _digest(delivery)
        with self._transaction():
            row = self.db.execute(
                "SELECT digest,ack FROM deliveries WHERE reply_id=? AND attempt_id=?",
                (reply_id, attempt_id),
            ).fetchone()
            if row:
                return ("duplicate" if row[0] == semantic else "conflict"), (
                    json.loads(row[1]) if row[1] else None
                )
            prior = self.db.execute(
                "SELECT 1 FROM deliveries WHERE reply_id=? LIMIT 1", (reply_id,)
            ).fetchone()
            if prior:
                ack = _ack(delivery, connection_id, "unknown", [])
                self.db.execute(
                    "INSERT INTO deliveries (reply_id,attempt_id,digest,state,ack) VALUES (?,?,?,?,?)",
                    (reply_id, attempt_id, semantic, "unknown", _canonical(ack)),
                )
                return "prior_reply", ack
            self.db.execute(
                "INSERT INTO deliveries (reply_id,attempt_id,digest,state) VALUES (?,?,?,?)",
                (reply_id, attempt_id, semantic, "intent"),
            )
        return "new", None

    def finish_delivery(self, delivery: dict[str, Any], ack: dict[str, Any]) -> None:
        self.db.execute(
            "UPDATE deliveries SET state=?,ack=?,ack_confirmed=0 WHERE reply_id=? AND attempt_id=?",
            (ack["state"], _canonical(ack), delivery["reply_id"], delivery["attempt_id"]),
        )

    def mark_ack_pending(self, reply_id: str, attempt_id: str) -> None:
        self.db.execute(
            "UPDATE deliveries SET ack_confirmed=0 WHERE reply_id=? AND attempt_id=? AND ack IS NOT NULL",
            (reply_id, attempt_id),
        )

    def pending_acks(self, connection_id: str) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT ack FROM deliveries WHERE ack IS NOT NULL AND ack_confirmed=0 ORDER BY rowid LIMIT 100"
        ).fetchall()
        result = []
        for (value,) in rows:
            ack = json.loads(value)
            ack["connection_id"] = connection_id
            result.append(ack)
        return result

    def mark_acked(self, ack: dict[str, Any]) -> None:
        self.db.execute(
            "UPDATE deliveries SET ack_confirmed=1 WHERE reply_id=? AND attempt_id=? AND ack IS NOT NULL",
            (ack["reply_id"], ack["attempt_id"]),
        )


class Runner:
    """Injectable HTTP/native ports; production main.py binds real SDK and TLS."""

    def __init__(
        self,
        settings: Settings,
        journal: Journal,
        post: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]],
        send_native: Callable[[str, str], Awaitable[str]],
        report: Callable[[str], None] | None = None,
    ):
        self.settings = settings
        self.journal = journal
        self.post = post
        self.send_native = send_native
        self.report = report or (lambda _code: None)

    async def on_event(self, event: Any) -> bool:
        payload = normalize_event(event, self.settings)
        if payload is None:
            return False
        try:
            status = self.journal.record_event(payload)
        except sqlite3.Error:
            self.report("journal_unavailable")
            return False
        # Stop only an explicitly configured, exact text message, before awaiting
        # Platform. AstrBot must not launch a second model response for this event.
        event.stop_event()
        if status != "new":
            if status == "conflict":
                self.report("event_conflict")
            return True
        response = None
        try:
            value = await self.post(EVENT_ROUTE, payload)
            if (
                isinstance(value, dict)
                and value.get("event_id") == payload["event_id"]
                and value.get("state") in {"accepted", "not_started", "unknown"}
            ):
                response = value
            else:
                self.report("invalid_event_receipt")
        except Exception:
            self.report("event_submission_unknown")
        self.journal.finish_event(payload, response)
        return True

    async def heartbeat(self) -> None:
        response = await self.post(
            HEARTBEAT_ROUTE,
            {"connection_id": self.settings.connection_id, "instance_id": self.journal.instance_id},
        )
        if not isinstance(response, dict) or response.get("state") != "online":
            raise BoundaryError("invalid_heartbeat")

    async def reconcile_events(self) -> None:
        for entry in self.journal.events_to_check():
            try:
                response = await self.post(
                    EVENT_STATUS_ROUTE,
                    {key: entry[key] for key in ("connection_id", "event_id", "account_id")},
                )
                if (
                    isinstance(response, dict)
                    and response.get("found") is True
                    and response.get("state") in {"accepted", "not_started", "unknown"}
                ):
                    self.journal.record_event_status(entry["event_key"], response)
                elif isinstance(response, dict) and response.get("found") is False:
                    self.journal.record_event_status(entry["event_key"], None)
                else:
                    self.report("invalid_event_status")
            except Exception:
                self.report("event_status_unavailable")
                break

    async def flush_acks(self) -> bool:
        for ack in self.journal.pending_acks(self.settings.connection_id):
            try:
                response = await self.post(ACK_ROUTE, ack)
                if (
                    isinstance(response, dict)
                    and response.get("reply_id") == ack["reply_id"]
                    and response.get("state") == ack["state"]
                ):
                    self.journal.mark_acked(ack)
                else:
                    self.report("invalid_ack_receipt")
                    return False
            except Exception:
                self.report("ack_pending")
                return False
        return not self.journal.pending_acks(self.settings.connection_id)

    async def poll_once(self) -> None:
        await self.reconcile_events()
        if not await self.flush_acks():
            return
        result = await self.post(
            CLAIM_ROUTE,
            {
                "connection_id": self.settings.connection_id,
                "instance_id": self.journal.instance_id,
                "limit": 1,
            },
        )
        deliveries = result.get("deliveries") if isinstance(result, dict) else None
        if not isinstance(deliveries, list) or len(deliveries) > 1:
            raise BoundaryError("invalid_claim")
        for delivery in deliveries:
            await self._deliver(delivery)

    async def _deliver(self, delivery: Any) -> None:
        if not isinstance(delivery, dict):
            self.report("invalid_delivery")
            return
        try:
            reply_id = _identifier(delivery.get("reply_id"))
            attempt_id = _identifier(delivery.get("attempt_id"))
        except BoundaryError:
            self.report("invalid_delivery")
            return
        # Freeze the platform's entire semantic document without persisting text.
        normalized = dict(delivery, reply_id=reply_id, attempt_id=attempt_id)
        status, previous_ack = self.journal.record_delivery(normalized, self.settings.connection_id)
        if status != "new":
            if status == "conflict":
                self.report("delivery_conflict")
            if previous_ack:
                self.journal.mark_ack_pending(reply_id, attempt_id)
                await self.flush_acks()
            return
        state, ids = "unknown", []
        try:
            conversation = _conversation(delivery.get("conversation_id"))
            text = delivery.get("text")
            if (
                delivery.get("namespace") != "qq"
                or conversation not in self.settings.allowed_conversations
                or delivery.get("thread_id") is not None
                or not isinstance(text, str)
                or not text
            ):
                raise BoundaryError("delivery_scope_invalid")
            media = media_parts(delivery)
            encoded_bytes = len(text.encode("utf-8"))
        except (BoundaryError, UnicodeError, RpcError):
            self.report("delivery_scope_invalid")
        else:
            if encoded_bytes > MAX_OUTBOUND_TEXT_BYTES:
                # A known pre-SDK rejection. Do not split one Platform reply into
                # multiple sends and change its receipt/idempotency identity.
                state = "failed"
                self.report("delivery_text_over_limit")
            else:
                try:
                    action = (
                        self.send_native(conversation, text, media)
                        if media
                        else self.send_native(conversation, text)
                    )
                    native_id = await asyncio.wait_for(action, timeout=15)
                    ids = [_message_id(native_id)]
                    state = "sent"
                except Exception:
                    # Once the native call starts, its outcome is unknown without
                    # a stable channel ID; never send this reply again automatically.
                    self.report("native_result_unknown")
        ack = _ack(normalized, self.settings.connection_id, state, ids)
        self.journal.finish_delivery(normalized, ack)
        await self.flush_acks()
