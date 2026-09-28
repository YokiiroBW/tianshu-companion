"""Small durable adapter ledger. Each native attempt is committed before the SDK call."""

import hashlib
import json
import sqlite3
from pathlib import Path


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


class Conflict(ValueError):
    pass


class Journal:
    def __init__(self, path):
        path = Path(path)
        if not path.is_absolute():
            raise ValueError("Journal path must be absolute")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        if self.db.execute("PRAGMA user_version").fetchone()[0] not in (0, 1):
            raise ValueError("Unsupported NoneBot journal schema")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS inbound (
              key TEXT PRIMARY KEY, signature TEXT NOT NULL, body TEXT NOT NULL,
              state TEXT NOT NULL, response TEXT
            );
            CREATE TABLE IF NOT EXISTS outbound (
              reply_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL,
              signature TEXT NOT NULL, state TEXT NOT NULL,
              ids TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0
            );
            PRAGMA user_version=1;
            """
        )

    def bind(self, connection_id, platform_id, bot_self_id):
        identity = _json([connection_id, platform_id, str(bot_self_id)])
        self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute("SELECT value FROM metadata WHERE key='identity'").fetchone()
            if existing and existing["value"] != identity:
                raise Conflict("The journal is bound to another bot connection")
            if not existing:
                self.db.execute("INSERT INTO metadata(key,value) VALUES('identity',?)", (identity,))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def close(self):
        self.db.close()

    def capture(self, body):
        key = _digest([body["connection_id"], body["event_id"], body["account_id"]])
        signature = _digest(body)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT signature FROM inbound WHERE key=?", (key,)).fetchone()
            if row and row["signature"] != signature:
                raise Conflict("Event key was reused with different content")
            if not row:
                self.db.execute(
                    "INSERT INTO inbound(key,signature,body,state) VALUES(?,?,?,'pending')",
                    (key, signature, _json(body)),
                )
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        return key

    def pending_events(self):
        return [
            (r["key"], json.loads(r["body"]))
            for r in self.db.execute(
                "SELECT key,body FROM inbound WHERE state='pending' ORDER BY rowid"
            )
        ]

    def recover_events(self):
        # A prior HTTP request may have crossed the wire. Never infer nonexecution.
        self.db.execute("UPDATE inbound SET state='unknown' WHERE state='inflight'")

    def start_event(self, key):
        return (
            self.db.execute(
                "UPDATE inbound SET state='inflight' WHERE key=? AND state='pending'", (key,)
            ).rowcount
            == 1
        )

    def event_unknown(self, key):
        self.db.execute(
            "UPDATE inbound SET state='unknown' WHERE key=? AND state='inflight'", (key,)
        )

    def unknown_events(self):
        return [
            (r["key"], json.loads(r["body"]))
            for r in self.db.execute(
                "SELECT key,body FROM inbound WHERE state='unknown' ORDER BY rowid",
            )
        ]

    def resolve_event(self, key, response):
        self.db.execute(
            "UPDATE inbound SET state=?,response=? WHERE key=? AND state='unknown'",
            (response["state"], _json(response), key),
        )

    def accepted(self, key, response):
        self.db.execute(
            "UPDATE inbound SET state=?,response=? WHERE key=? AND state='inflight'",
            (response["state"], _json(response), key),
        )

    def claim(self, delivery):
        """Return True exactly once. A prior claim is never safe to repeat after restart."""
        reply_id, attempt_id = delivery["reply_id"], delivery["attempt_id"]
        signature = _digest(delivery)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT attempt_id,signature,state FROM outbound WHERE reply_id=?", (reply_id,)
            ).fetchone()
            if row and (row["attempt_id"] != attempt_id or row["signature"] != signature):
                raise Conflict("Reply ID was reused with another attempt or payload")
            first = row is None
            if first:
                self.db.execute(
                    "INSERT INTO outbound(reply_id,attempt_id,signature,state,ids) "
                    "VALUES(?,?,?,'claimed','[]')",
                    (reply_id, attempt_id, signature),
                )
            elif row["state"] == "claimed":
                self.db.execute("UPDATE outbound SET state='unknown' WHERE reply_id=?", (reply_id,))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        return first

    def settle(self, reply_id, state, ids=()):
        if state not in {"sent", "failed", "unknown"}:
            raise ValueError("Invalid native outcome")
        if state == "sent" and not ids:
            raise ValueError("Sent requires a real SDK ID")
        row = self.db.execute("SELECT state FROM outbound WHERE reply_id=?", (reply_id,)).fetchone()
        if not row or row["state"] != "claimed":
            raise Conflict("The native attempt is not claimable")
        self.db.execute(
            "UPDATE outbound SET state=?,ids=? WHERE reply_id=?",
            (state, _json(list(ids)), reply_id),
        )

    def unacked(self):
        # A crash during a native call leaves claimed, which is permanently unknown.
        self.db.execute("UPDATE outbound SET state='unknown' WHERE state='claimed'")
        return [
            dict(
                connection_id=None,
                reply_id=r["reply_id"],
                attempt_id=r["attempt_id"],
                state=r["state"],
                channel_message_ids=json.loads(r["ids"]),
            )
            for r in self.db.execute(
                "SELECT reply_id,attempt_id,state,ids FROM outbound WHERE acked=0 ORDER BY rowid"
            )
        ]

    def acked(self, reply_id):
        self.db.execute("UPDATE outbound SET acked=1 WHERE reply_id=?", (reply_id,))
