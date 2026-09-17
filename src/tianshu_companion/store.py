"""Single-owner SQLite repository. Network calls never run in a transaction."""

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path

from .contracts import canonical

LIFE_TABLES = {
    "life_worlds",
    "life_rooms",
    "life_actors",
    "life_events",
    "life_known",
    "life_recipes",
    "life_materials",
    "life_diaries",
    "life_revisions",
    "life_access",
}

IMAGE_TABLES = {"image_outfits", "image_jobs"}

WRITING_TABLES = {
    "write_works",
    "write_chapters",
    "write_revisions",
    "write_reviews",
    "write_publications",
    "write_materials",
    "write_requests",
    "write_recipes",
    "write_access",
}

PROACTIVE_TABLES = {
    "proactive_subscriptions",
    "proactive_templates",
    "proactive_goals",
    "proactive_reminders",
    "proactive_candidates",
    "proactive_attempts",
    "proactive_quota",
}

# Explicit functional commands: the registry, the durable request bound to a message
# version, and the per-attempt history. `direct_attempts` is derived history that may be
# pruned; the registry row and the request row are the durable facts.
DIRECT_TABLES = {"direct_commands", "direct_requests", "direct_attempts"}

# Registered character personas. `personas` is the live pointer row, `revisions` and
# `publications` are append-only history, `imports` keys the idempotent deployment import,
# and `operations` is the request ledger: the identity one write was executed under and the
# result it committed. No row here can confer a permission: persona content is character
# text only.
PERSONA_TABLES = {
    "persona_personas",
    "persona_revisions",
    "persona_publications",
    "persona_approvals",
    "persona_rollbacks",
    "persona_imports",
    "persona_access",
    "persona_operations",
}

TABLES = (
    LIFE_TABLES
    | IMAGE_TABLES
    | WRITING_TABLES
    | PROACTIVE_TABLES
    | DIRECT_TABLES
    | PERSONA_TABLES
    | {
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
)
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
        if version not in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9):
            raise RuntimeError("Unsupported database schema version")
        # Backup the complete SQLite view (including WAL) before structural migration.
        # A unique file never overwrites earlier recovery evidence.
        #
        # One recovery backup per open, labelled with the *highest* structural step this
        # process will apply, so the file always has a name a restore can be reasoned about:
        # every version below the current one needs the current step, and an older file needs
        # all the steps above it too. A v5 file opened by a v9 process therefore takes one
        # `.pre-persona-ops-v9-` backup covering the whole chain; it is taken before any DDL,
        # so no intermediate step can start without a restore point. An intermediate restore
        # point requires upgrading one step at a time.
        if 1 <= version <= 8 and str(path) != ":memory:":
            label = {
                1: ".pre-source-v2-",
                2: ".pre-life-v3-",
                3: ".pre-images-v4-",
                4: ".pre-writing-v5-",
                5: ".pre-proactive-v6-",
                6: ".pre-routing-v7-",
                7: ".pre-persona-v8-",
                8: ".pre-persona-ops-v9-",
            }[version]
            backup = sqlite3.connect(str(path) + label + uuid.uuid4().hex + ".bak")
            try:
                self.db.backup(backup)
            finally:
                backup.close()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        for table in sorted(
            TABLES
            - LIFE_TABLES
            - IMAGE_TABLES
            - WRITING_TABLES
            - PROACTIVE_TABLES
            - DIRECT_TABLES
            - PERSONA_TABLES
        ):
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
            "CREATE INDEX IF NOT EXISTS turns_scope_recent ON turns(conversation_id,"
            "json_extract(body,'$.scope.actor_id'),json_extract(body,'$.scope.person_id'),"
            "json_extract(body,'$.scope.audience'),position)"
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS inbox_source ON inbox(conversation_id,"
            "json_extract(body,'$.base'),json_extract(body,'$.revision') DESC)"
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS inbox_author ON inbox(conversation_id,"
            "json_extract(body,'$.request.author.namespace'),"
            "json_extract(body,'$.request.author.immutable_account_id'),position)"
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
        self._in_transaction = False
        with self.transaction():
            if not self.get("metadata", "source_head"):
                if version >= 2:
                    raise RuntimeError("Missing durable source head; trusted recovery required")
                self.put(
                    "metadata",
                    dict(id="source_head", generation="generation:" + uuid.uuid4().hex, sequence=0),
                )
            for table in sorted(
                LIFE_TABLES
                | IMAGE_TABLES
                | WRITING_TABLES
                | PROACTIVE_TABLES
                | DIRECT_TABLES
                | PERSONA_TABLES
            ):
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
                "CREATE INDEX IF NOT EXISTS proactive_candidates_subject ON "
                "proactive_candidates(json_extract(body,'$.kind'),"
                "json_extract(body,'$.subject_id'),position)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS proactive_candidates_due ON "
                "proactive_candidates(status,deadline,position)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS proactive_subject_due ON "
                "proactive_goals(status,deadline)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS proactive_reminder_due ON "
                "proactive_reminders(status,deadline)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS direct_commands_scope ON direct_commands("
                "json_extract(body,'$.name'),json_extract(body,'$.command_id'),position)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS direct_requests_pending ON "
                "direct_requests(status,position,id)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS direct_requests_version ON direct_requests("
                "json_extract(body,'$.channel_key'),"
                "json_extract(body,'$.message_id'),status,position)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS direct_attempts_request ON direct_attempts("
                "json_extract(body,'$.request_id'),position,id)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS persona_revisions_subject ON persona_revisions("
                "json_extract(body,'$.subject'),position,id)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS persona_approvals_revision ON persona_approvals("
                "json_extract(body,'$.revision_id'),position,id)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS persona_publications_subject ON persona_publications("
                "json_extract(body,'$.subject'),position,id)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS persona_operations_request ON persona_operations("
                "json_extract(body,'$.request_id'),json_extract(body,'$.operation'),position,id)"
            )
            self.db.execute("PRAGMA user_version=9")

    @contextmanager
    def transaction(self):
        """One transaction per outermost `with`, so composed writes commit atomically.

        SQLite has no nested transactions and `BEGIN` inside one is an error, so an inner
        `with store.transaction():` joins the transaction already open: only the outermost
        block commits (and only it advances the fact-stream head), and an exception anywhere
        inside still rolls the whole thing back. That is what lets a composed operation - a
        business write plus the ledger row that records its result - be one atomic step.
        """
        if self._in_transaction:
            yield
            return
        self.db.execute("BEGIN IMMEDIATE")
        self._in_transaction = True
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
            self._in_transaction = False
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

    def delete(self, table, key):
        """Bounded housekeeping delete for derived rows; never used for fact tables."""
        assert table in TABLES and table not in FACT_TABLES
        self.db.execute(f"DELETE FROM {table} WHERE id=?", (key,))

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

    def previous_scope_turn(self, scope, before_sequence):
        """One indexed predecessor; retain invalid/unsent candidates for strict checks.

        Filtering on validity here would silently substitute an older plan when
        the actual preceding result is revoked, failed or still being delivered.
        """
        row = self.db.execute(
            "SELECT body FROM turns WHERE conversation_id=? "
            "AND json_extract(body,'$.scope.actor_id')=? "
            "AND json_extract(body,'$.scope.person_id')=? "
            "AND json_extract(body,'$.scope.audience')=? "
            "AND json_extract(body,'$.scope.conversation_id')=? "
            "AND position<? ORDER BY position DESC LIMIT 1",
            (
                scope["conversation_id"],
                scope["actor_id"],
                scope["person_id"],
                scope["audience"],
                scope["conversation_id"],
                before_sequence,
            ),
        ).fetchone()
        return json.loads(row[0]) if row else None

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
