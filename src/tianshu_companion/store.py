"""Single-owner SQLite repository. Network calls never run in a transaction."""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .contracts import canonical

TABLES = {"conversations", "collections", "inbox", "turns", "replies", "outbox", "commands"}


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
        if self.db.execute("PRAGMA user_version").fetchone()[0] not in (0, 1):
            self.close()
            raise RuntimeError("Unsupported database schema version")
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
        self.db.execute("PRAGMA user_version=1")

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def put(self, table, item):
        assert table in TABLES
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
