"""Single-owner SQLite repository. Network calls never run in a transaction."""

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path

from .contracts import canonical

TABLES = {
    "conversations",
    "collections",
    "inbox",
    "turns",
    "replies",
    "outbox",
    "commands",
    "physicals",
    "admissions",
    "metadata",
}
FACT_TABLES = {"physicals", "admissions", "turns", "replies", "outbox"}


class Store:
    def __init__(self, path):
        self._lock = None
        if str(path) != ":memory:":
            path = Path(path).resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            self._lock = open(str(path) + ".owner", "a+b")
            self._lock.write(b"0")
            self._lock.flush()
            self._lock.seek(0)
            try:
                if __import__("os").name == "nt":
                    import msvcrt

                    msvcrt.locking(self._lock.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                self._lock.close()
                raise RuntimeError("Database already has a running owner") from None
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        try:
            self._initialize(path)
        except BaseException:
            self.close()
            raise

    def _initialize(self, path):
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1, 2):
            raise RuntimeError("Unsupported database schema version")
        # Backup the complete SQLite view (including WAL) before structural migration.
        # A unique file never overwrites earlier recovery evidence.
        if version == 1 and str(path) != ":memory:":
            backup = sqlite3.connect(str(path) + ".pre-source-v2-" + uuid.uuid4().hex + ".bak")
            try:
                self.db.backup(backup)
            finally:
                backup.close()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        for table in sorted(TABLES):
            self.db.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ("
                "id TEXT PRIMARY KEY, conversation_id TEXT, position INTEGER, "
                "status TEXT, deadline REAL, body TEXT NOT NULL)"
            )
            self.db.execute(
                f"CREATE INDEX IF NOT EXISTS {table}_queue ON "
                f"{table}(conversation_id,status,position)"
            )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS turns_recent ON turns(conversation_id,position)"
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS inbox_source ON inbox(conversation_id,"
            "json_extract(body,'$.base'),json_extract(body,'$.revision') DESC)"
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS replies_turn ON replies("
            "json_extract(body,'$.turn_id'),status,position)"
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS physicals_source ON physicals(conversation_id,"
            "json_extract(body,'$.base'),json_extract(body,'$.revision') DESC)"
        )
        self._fact_dirty = False
        with self.transaction():
            if not self.get("metadata", "source_head"):
                if version == 2:
                    raise RuntimeError("Missing durable source head; trusted recovery required")
                self.put(
                    "metadata",
                    dict(id="source_head", generation="generation:" + uuid.uuid4().hex, sequence=0),
                )
            self.db.execute("PRAGMA user_version=2")

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        self._fact_dirty = False
        try:
            yield
            if self._fact_dirty:
                head = self.get("metadata", "source_head")
                head["sequence"] += 1
                self.put("metadata", head)
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        finally:
            self._fact_dirty = False

    def put(self, table, item):
        assert table in TABLES
        if not self.db.in_transaction:
            with self.transaction():
                self.put(table, item)
            return
        previous = self.get(table, item["id"]) if table in FACT_TABLES else None
        if (
            table in FACT_TABLES
            and previous != item
            and (table != "outbox" or previous is None or previous["event"] != item["event"])
        ):
            self._fact_dirty = True
        self.db.execute(
            f"INSERT INTO {table} VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET conversation_id=excluded.conversation_id, "
            "position=excluded.position,status=excluded.status,deadline=excluded.deadline,"
            "body=excluded.body",
            (
                item["id"],
                item.get("conversation_id"),
                item.get("sequence", 0),
                item.get("phase", item.get("state")),
                item.get("deadline"),
                canonical(item),
            ),
        )

    def get(self, table, key):
        assert table in TABLES
        row = self.db.execute(f"SELECT body FROM {table} WHERE id=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def list(self, table, conversation_id=None, states=None):
        assert table in TABLES
        query, args = f"SELECT body FROM {table} WHERE 1=1", []
        if conversation_id is not None:
            query += " AND conversation_id=?"
            args.append(conversation_id)
        if states:
            query += " AND status IN (" + ",".join("?" for _ in states) + ")"
            args.extend(states)
        query += " ORDER BY position,id"
        return [json.loads(row[0]) for row in self.db.execute(query, args)]

    def close(self):
        self.db.close()
        if self._lock:
            self._lock.close()

    def recent_turns(self, conversation_id, before_sequence, limit):
        rows = self.db.execute(
            "SELECT body FROM turns WHERE conversation_id=? AND position<? "
            "ORDER BY position DESC LIMIT ?",
            (conversation_id, before_sequence, limit),
        )
        return [json.loads(row[0]) for row in rows]

    def latest_source(self, conversation_id, base):
        row = self.db.execute(
            "SELECT body FROM physicals WHERE conversation_id=? AND json_extract(body,'$.base')=? "
            "ORDER BY json_extract(body,'$.revision') DESC LIMIT 1",
            (conversation_id, base),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def source_head(self):
        item = self.get("metadata", "source_head")
        return {k: item[k] for k in ("generation", "sequence")}

    def receipt_input(self, receipt_id):
        row = self.db.execute(
            "SELECT body FROM inbox WHERE json_extract(body,'$.receipt.receipt_id')=?",
            (receipt_id,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def turn_replies(self, turn_id, state=None):
        sql, args = "SELECT body FROM replies WHERE json_extract(body,'$.turn_id')=?", [turn_id]
        if state is not None:
            sql += " AND status=?"
            args.append(state)
        return [json.loads(row[0]) for row in self.db.execute(sql + " ORDER BY position", args)]
