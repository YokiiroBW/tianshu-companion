"""Single-owner SQLite repository. Network calls never run in a transaction."""

import json
import sqlite3
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path

from .contracts import canonical
from .life_read_index import create_page_index, prepare_page_index
from . import work_index
from .payload_files import PayloadFiles

CHANNEL_ALIAS_PREFIX = "source-channel-alias:"


def _exact_scope_filter(scope):
    keys = ("actor_id", "person_id", "audience", "conversation_id")
    return (
        " AND ".join(f"json_extract(body,'$.scope.{key}')=?" for key in keys),
        tuple(scope[key] for key in keys),
    )


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

RUNTIME_TABLES = {
    "life_activities",
    "life_concerns",
    "life_affect",
    "life_album",
    "image_media",
    "delivery_contacts",
    "proactive_motives",
    "life_reading",
    "life_content_refs",
}

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
    | RUNTIME_TABLES
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
    def __init__(self, path, *, settings=None, storage_runtime=None):
        self._lock = None
        self.storage_runtime = storage_runtime
        self._finalize_storage = storage_runtime is None
        self._temporary_files = None
        self._postgres_owner = False
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
        self.path = str(path)
        if str(path) == ":memory:":
            self._temporary_files = tempfile.TemporaryDirectory(prefix="companion-payloads-")
            file_root = Path(self._temporary_files.name)
        else:
            file_root = Path(str(path) + ".files")
        self.payloads = PayloadFiles(file_root)
        self.db = None
        try:
            if str(path) != ":memory:" or settings is not None or storage_runtime is not None:
                from tianshu_storage import StorageOptions, StorageRuntime
                from .storage_postgres import claim_owner, dialect, prepare

                if self.storage_runtime is None:
                    self.storage_runtime = StorageRuntime(
                        StorageOptions.from_settings(settings or {}),
                        service="companion",
                        state_directory=Path(str(path) + ".storage"),
                    )
                    self.storage_runtime.start()
                self.database = self.storage_runtime.database(
                    "main",
                    path,
                    prepare=lambda connection, source: prepare(connection, source, self.payloads),
                    dialect=dialect,
                )
                self.backend = self.database.backend
                self.db = self.database.connect(isolation_level=None)
                if self.backend == "postgresql":
                    claim_owner(self.db)
                    self._postgres_owner = True
                    self.db.companion_indexes = True
            else:
                self.backend = "sqlite"
                self.db = sqlite3.connect(path, isolation_level=None)
            self.db.row_factory = sqlite3.Row
            self._initialize(path)
            self._migrate_payloads()
            if self.storage_runtime is not None and self._finalize_storage:
                self.storage_runtime.finalize()
        except BaseException:
            self.close()
            raise

    def _initialize(self, path):
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10):
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
        backed_up = False
        if self.backend == "sqlite" and 1 <= version <= 9 and str(path) != ":memory:":
            label = ".pre-life-runtime-v10-"
            backup = sqlite3.connect(str(path) + label + uuid.uuid4().hex + ".bak")
            try:
                self.db.backup(backup)
            finally:
                backup.close()
            backed_up = True
        # The life-read page needs one derived index, and adding it to a database that already
        # holds facts is a structural step like any other: it is detected - and given a
        # restore point of its own, WAL included - before this open runs any DDL. A database
        # that already carries the correct index adds no file and no fact, and an open that
        # already took a complete pre-DDL backup above reuses it instead of adding a second.
        if self.backend == "sqlite":
            backed_up = work_index.prepare(self.db, path, restored=backed_up, fresh=version == 0)
            prepare_page_index(self.db, path, restore_point_taken=backed_up, fresh=version == 0)
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
            - RUNTIME_TABLES
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
                | RUNTIME_TABLES
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
            # One paging index per persona history: the page read walks a character's history
            # in `(position, id)` order with a range seek, so it needs an index whose key is
            # exactly `(conversation_id, position, id)`. The `_queue` index above leads with
            # `status`, which a page read does not constrain, so without these SQLite would
            # answer `ORDER BY position,id` with a temporary b-tree and visit every record of
            # the character to return a short page. Derived indexes only: no fact, no field,
            # no version and no lock changes, and `IF NOT EXISTS` keeps the open idempotent.
            for table in (
                "persona_revisions",
                "persona_publications",
                "persona_approvals",
                "persona_rollbacks",
            ):
                self.db.execute(
                    f"CREATE INDEX IF NOT EXISTS {table}_page ON "
                    f"{table}(conversation_id,position,id)"
                )
            # The one derived index the authorized diary page reads through. Its definition
            # and its restore point belong to the read module; the create runs here, inside
            # the initialization transaction, so a request path can never alter the schema.
            create_page_index(self.db)
            work_index.create(self.db)
            for table in sorted(RUNTIME_TABLES):
                self.db.execute(
                    f"CREATE INDEX IF NOT EXISTS {table}_page ON {table}(conversation_id,id)"
                )
            self.db.execute("PRAGMA user_version=10")

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
        lease_only = (
            table == "turns"
            and previous is not None
            and {
                k: v for k, v in previous.items() if k not in {"source_deadline", "bootstrap_until"}
            }
            == {k: v for k, v in item.items() if k not in {"source_deadline", "bootstrap_until"}}
        )
        if (
            table in FACT_TABLES
            and previous != item
            # These two lease fields are not exported by source-facts/read. A
            # continuation must persist, but cannot invalidate an unchanged owner
            # snapshot. Every other turn change, including version, still does.
            and not lease_only
            and (table != "outbox" or previous is None or previous["event"] != item["event"])
        ):
            self._fact_dirty = True
        self.db.execute(
            f"INSERT INTO {table}(id,conversation_id,position,status,deadline,body) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET conversation_id=excluded.conversation_id, "
            "position=excluded.position,status=excluded.status,deadline=excluded.deadline,"
            "body=excluded.body",
            (
                item["id"],
                item.get("conversation_id"),
                item.get("sequence", 0),
                item.get("phase", item.get("state")),
                item.get("deadline"),
                canonical(self.payloads.freeze(table, item)),
            ),
        )

    def get(self, table, key):
        assert table in TABLES
        row = self.db.execute(f"SELECT body FROM {table} WHERE id=?", (key,)).fetchone()
        if row is None and table == "conversations":
            alias = self.get("metadata", CHANNEL_ALIAS_PREFIX + key)
            if alias is not None:
                # One hop to a real row, never another alias or a second set of
                # ingest/turn/send counters. All original callers get its ID.
                row = self.db.execute(
                    "SELECT body FROM conversations WHERE id=?", (alias["conversation_key"],)
                ).fetchone()
                if row is None:
                    raise ValueError("Missing canonical conversation")
        return self.payloads.restore(table, json.loads(row[0])) if row else None

    def get_many(self, table, keys):
        """Bounded primary-key reads; missing rows remain missing, never substituted."""
        assert table in TABLES
        keys = list(dict.fromkeys(keys))
        result = {}
        for offset in range(0, len(keys), 256):
            batch = keys[offset : offset + 256]
            placeholders = ",".join("?" for _ in batch)
            for row in self.db.execute(
                f"SELECT id,body FROM {table} WHERE id IN ({placeholders})", batch
            ):
                result[row[0]] = self.payloads.restore(table, json.loads(row[1]))
        if table == "conversations":
            missing = [key for key in keys if key not in result]
            aliases = self.get_many("metadata", [CHANNEL_ALIAS_PREFIX + key for key in missing])
            owners = list(dict.fromkeys(row["conversation_key"] for row in aliases.values()))
            canonical_rows = {}
            for offset in range(0, len(owners), 256):
                batch = owners[offset : offset + 256]
                for row in self.db.execute(
                    "SELECT id,body FROM conversations WHERE id IN ("
                    + ",".join("?" for _ in batch)
                    + ")",
                    batch,
                ):
                    canonical_rows[row[0]] = self.payloads.restore(table, json.loads(row[1]))
            for key in missing:
                alias = aliases.get(CHANNEL_ALIAS_PREFIX + key)
                if alias is not None:
                    owner = canonical_rows.get(alias["conversation_key"])
                    if owner is None:
                        raise ValueError("Missing canonical conversation")
                    # Preserve requested keys: source-facts groups selectors by
                    # their actual source channel, not the canonical row ID.
                    result[key] = owner
        return result

    def turn_phases(self, keys):
        """Scheduling reads columns, without decoding provider/context payloads."""
        keys = list(dict.fromkeys(key for key in keys if isinstance(key, str)))
        result = {}
        for offset in range(0, len(keys), 256):
            batch = keys[offset : offset + 256]
            result.update(
                self.db.execute(
                    "SELECT id,status FROM turns WHERE id IN ("
                    + ",".join("?" for _ in batch)
                    + ")",
                    batch,
                )
            )
        return result

    def active_scopes(self, states, actor):
        return [
            json.loads(row[0])
            for row in self.db.execute(
                "SELECT CAST(json_extract(body,'$.scope') AS TEXT) FROM turns INDEXED BY turns_work "
                "WHERE status IN ('queued','preparing','generating','waiting_dependency',"
                "'ready_to_send','sending','reconciling') AND status IN ("
                + ",".join("?" for _ in states)
                + ") AND json_extract(body,'$.scope.actor_id')=?",
                (*states, actor),
            )
        ]

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
        return [
            self.payloads.restore(table, json.loads(row[0])) for row in self.db.execute(query, args)
        ]

    def close(self):
        if self.db is not None:
            try:
                if self._postgres_owner:
                    try:
                        self.db.native(
                            "SELECT pg_advisory_unlock(hashtextextended(current_schema(),23300))"
                        )
                    except sqlite3.Error:
                        pass  # A lost PG session has already released its owner.
            finally:
                self._postgres_owner = False
                try:
                    self.db.close()
                except sqlite3.Error:
                    pass
        if self._lock:
            self._lock.close()
        if self.storage_runtime is not None:
            self.storage_runtime.close()
        if self._temporary_files is not None:
            self._temporary_files.cleanup()

    def _migrate_payloads(self):
        """Representation-only migration: retain every source fact and frozen wire."""
        backed_up = False
        with self.transaction():
            for table in ("replies", "write_materials"):
                cursor = self.db.execute(f"SELECT id,body FROM {table} ORDER BY id")
                while rows := cursor.fetchmany(64):
                    for identity, raw in rows:
                        item = json.loads(raw)
                        stored = self.payloads.freeze(table, item)
                        if stored == item:
                            continue
                        if self.payloads.restore(table, json.loads(canonical(stored))) != item:
                            raise RuntimeError("Companion payload migration verification failed")
                        if self.backend == "sqlite" and self.path != ":memory:" and not backed_up:
                            # Back up through a separate reader: this writer is still
                            # unmodified and its reserved lock includes the WAL view.
                            with sqlite3.connect(self.path) as source:
                                with sqlite3.connect(
                                    self.path + ".pre-payload-files-" + uuid.uuid4().hex + ".bak"
                                ) as backup:
                                    source.backup(backup)
                            backed_up = True
                        self.db.execute(
                            f"UPDATE {table} SET body=? WHERE id=?", (canonical(stored), identity)
                        )

    def due(self, table, states, now, limit=64, conversation_id=None):
        assert table in {"outbox", "collections"}
        sql = f"SELECT body FROM {table} WHERE status IN ({','.join('?' for _ in states)}) AND deadline<=?"
        args = [*states, now]
        if conversation_id is not None:
            sql += " AND conversation_id=?"
            args.append(conversation_id)
        # Quarantined collectors cannot progress, so they must not occupy a page forever.
        if table == "collections":
            sql += " AND EXISTS (SELECT 1 FROM conversations c WHERE c.conversation_id=collections.conversation_id AND coalesce(json_extract(c.body,'$.source_quarantined'),0)=0)"
        sql += (
            " ORDER BY deadline,position,id LIMIT ?"
            if table == "collections"
            else " ORDER BY deadline,id LIMIT ?"
        )
        return [json.loads(row[0]) for row in self.db.execute(sql, [*args, limit])]

    def work_conversations(self, after="", limit=64):
        # Limit each indexed range before merging: neither old facts nor a large
        # backlog of other conversations is materialized by every worker pass.
        predicates = {
            "turns": "status IN ('queued','preparing','generating','waiting_dependency','ready_to_send','sending','reconciling')",
            "replies": "status='unknown'",
            "direct_requests": "status IN ('completed','failed') AND json_extract(body,'$.reply_state')='ready_to_deliver' AND json_extract(body,'$.deferred_reason')='outbound_band_busy'",
        }
        ids = set()
        indexes = {
            "turns": "turns_work",
            "replies": "replies_unknown",
            "direct_requests": "direct_waiting_band",
        }
        for table, predicate in predicates.items():
            sql = f"SELECT DISTINCT conversation_id FROM {table} INDEXED BY {indexes[table]} WHERE {predicate} AND conversation_id>? ORDER BY conversation_id LIMIT ?"
            ids.update(row[0] for row in self.db.execute(sql, (after, limit)))
        return sorted(ids)[:limit]

    def conversation(self, cid):
        row = self.db.execute(
            "SELECT body FROM conversations WHERE conversation_id=? LIMIT 1", (cid,)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def work_candidates(self, conversation_ids):
        """One tick's hints, without loading queued or in-flight turn payloads.

        The caller holds its normal scheduling transaction. Claims and delivery
        still reread their current facts; nothing here survives the tick.
        """
        conversation_ids = list(dict.fromkeys(conversation_ids))
        result = {
            cid: dict(
                conversation=None,
                active=0,
                queued=[],
                retry=[],
                recovery={},
                reconcile={},
                deliver=False,
                direct=False,
            )
            for cid in conversation_ids
        }
        for offset in range(0, len(conversation_ids), 256):
            batch = conversation_ids[offset : offset + 256]
            placeholders = ",".join("?" for _ in batch)
            for cid, body in self.db.execute(
                f"SELECT conversation_id,body FROM conversations WHERE conversation_id IN ({placeholders})",
                batch,
            ):
                result[cid]["conversation"] = json.loads(body)
            for (
                key,
                cid,
                phase,
                retry_at,
                recovery_started,
                source_deadline,
                reconcile_at,
            ) in self.db.execute(
                "SELECT t.id,t.conversation_id,t.status,CASE WHEN t.status IN ('preparing','waiting_dependency') "
                "THEN json_extract(t.body,'$.retry_at') END,"
                "CASE WHEN t.status IN ('preparing','waiting_dependency') "
                "THEN CAST(json_extract(t.body,'$.preparation_retry_started_at') AS REAL) END,"
                "CASE WHEN t.status IN ('preparing','waiting_dependency') "
                "THEN CAST(json_extract(t.body,'$.source_deadline') AS REAL) END,coalesce(m.deadline,0) "
                "FROM turns t INDEXED BY turns_work LEFT JOIN metadata m "
                "ON m.id='delivery-reconcile:'||t.id "
                "WHERE t.status IN ('queued','preparing','generating','waiting_dependency',"
                "'ready_to_send','sending','reconciling') "
                f"AND t.conversation_id IN ({placeholders}) ORDER BY t.conversation_id,t.position,t.id",
                batch,
            ):
                candidate = result[cid]
                if phase == "queued":
                    if len(candidate["queued"]) < 2:
                        candidate["queued"].append(key)
                else:
                    candidate["active"] += 1
                if phase in {"preparing", "waiting_dependency"}:
                    candidate["retry"].append((key, retry_at))
                    if recovery_started is not None and source_deadline is not None:
                        candidate["recovery"][key] = (recovery_started, source_deadline)
                if phase in {"ready_to_send", "sending"}:
                    candidate["deliver"] = True
                elif phase == "reconciling":
                    candidate["reconcile"][key] = reconcile_at
            for cid, turn_id, reconcile_at in self.db.execute(
                "SELECT DISTINCT r.conversation_id,json_extract(r.body,'$.turn_id'),coalesce(m.deadline,0) "
                "FROM replies r INDEXED BY replies_unknown LEFT JOIN metadata m "
                "ON m.id='delivery-reconcile:'||CAST(json_extract(r.body,'$.turn_id') AS TEXT) "
                f"WHERE r.status='unknown' AND r.conversation_id IN ({placeholders})",
                batch,
            ):
                result[cid]["reconcile"][turn_id] = reconcile_at
            for (cid,) in self.db.execute(
                "SELECT DISTINCT conversation_id FROM direct_requests INDEXED BY direct_waiting_band "
                "WHERE status IN ('completed','failed') "
                "AND json_extract(body,'$.reply_state')='ready_to_deliver' "
                "AND json_extract(body,'$.deferred_reason')='outbound_band_busy' "
                f"AND conversation_id IN ({placeholders})",
                batch,
            ):
                result[cid]["direct"] = True
        return result

    def work_turn_count(self, cid, states):
        row = self.db.execute(
            "SELECT COUNT(*) FROM turns INDEXED BY turns_work WHERE conversation_id=? "
            "AND status IN ('queued','preparing','generating','waiting_dependency',"
            "'ready_to_send','sending','reconciling') "
            f"AND status IN ({','.join('?' for _ in states)})",
            (cid, *states),
        ).fetchone()
        return row[0]

    def first_work_turn(self, cid):
        row = self.db.execute(
            "SELECT body FROM turns INDEXED BY turns_work WHERE conversation_id=? AND status IN ('queued','preparing','generating','waiting_dependency','ready_to_send','sending','reconciling') ORDER BY position,id LIMIT 1",
            (cid,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def work_turns(self, cid, states):
        sql = "SELECT body FROM turns INDEXED BY turns_work WHERE conversation_id=? AND status IN ('queued','preparing','generating','waiting_dependency','ready_to_send','sending','reconciling')"
        sql += f" AND status IN ({','.join('?' for _ in states)}) ORDER BY position,id"
        return [json.loads(row[0]) for row in self.db.execute(sql, (cid, *states))]

    def turn_outbox(self, turn_id, state):
        return [
            json.loads(row[0])
            for row in self.db.execute(
                "SELECT body FROM outbox WHERE json_extract(body,'$.event.aggregate_id')=? AND status=? LIMIT 64",
                (turn_id, state),
            )
        ]

    def outboxes_for_turns(self, turn_ids):
        """Read only the requested turns through the existing outbox-turn index."""
        turn_ids = list(dict.fromkeys(turn_ids))
        result = {key: [] for key in turn_ids}
        for offset in range(0, len(turn_ids), 256):
            batch = turn_ids[offset : offset + 256]
            rows = self.db.execute(
                "SELECT body FROM outbox WHERE json_extract(body,'$.event.aggregate_id') IN ("
                + ",".join("?" for _ in batch)
                + ") ORDER BY position,id",
                batch,
            )
            for row in rows:
                item = self.payloads.restore("outbox", json.loads(row[0]))
                result[item["event"]["aggregate_id"]].append(item)
        return result

    def unknown_replies(self, cid, after="", limit=32):
        return [
            json.loads(row[0])
            for row in self.db.execute(
                "SELECT body FROM replies INDEXED BY replies_unknown WHERE conversation_id=? AND status='unknown' AND id>? ORDER BY id LIMIT ?",
                (cid, after, limit),
            )
        ]

    def collector_context(self, collection):
        author = collection["collection_key"]["author"]
        rows = self.db.execute(
            """SELECT body FROM inbox INDEXED BY inbox_context WHERE conversation_id=?
          AND json_extract(body,'$.actor_id')=? AND json_extract(body,'$.stale')=0
          AND position>=? AND NOT (json_extract(body,'$.request.author.namespace')=?
          AND json_extract(body,'$.request.author.immutable_account_id')=?)
          ORDER BY position DESC,id DESC LIMIT 32""",
            (
                collection["conversation_id"],
                collection["scope"]["actor_id"],
                collection["sequence"],
                author["namespace"],
                author["immutable_account_id"],
            ),
        )
        return [json.loads(row[0])["source"] for row in rows][::-1]

    def window_rows(self, prefix, *, after="", limit=32):
        """Primary-key range pages for window state; no metadata-wide body scan."""
        rows = self.db.execute(
            "SELECT body FROM metadata WHERE id>=? AND id<? AND id>? ORDER BY id LIMIT ?",
            (prefix, prefix + "\uffff", after, limit),
        )
        return [json.loads(row[0]) for row in rows]

    def window_turns(self, scope, after, limit):
        keys = ("actor_id", "audience") + (() if scope["audience"] == "group" else ("person_id",))
        predicate = " AND ".join("json_extract(body,'$.scope." + k + "')=?" for k in keys)
        rows = self.db.execute(
            "SELECT body FROM turns WHERE conversation_id=? AND position>? AND "
            + predicate
            + " ORDER BY position LIMIT ?",
            (scope["conversation_id"], after, *(scope[k] for k in keys), limit),
        )
        return [self.payloads.restore("turns", json.loads(row[0])) for row in rows]

    def recent_turns(self, conversation_id, before_sequence, limit, *, scope=None):
        # This is the short-context reader, not a full turn export. A completed
        # row also contains provider/checkpoint/retry material which no history
        # candidate uses. Keep it persisted and load full rows only for selected
        # turns at their verification boundary.
        fields = (
            "id",
            "conversation_id",
            "sequence",
            "phase",
            "failure",
            "model_calls",
            "bundle",
            "scope",
            "scope_version",
            "binding_version",
            "context_revision",
            "history_snapshot",
            "input_history_snapshot",
            "cancelled",
            "delivery_state",
            "short_context",
        )
        paths = ",".join("'$." + field + "'" for field in fields)
        sql = f"SELECT json_extract(body,{paths}) FROM turns WHERE conversation_id=? AND position<?"
        parameters = (conversation_id, before_sequence)
        if scope is not None:
            predicate, values = _exact_scope_filter(scope)
            sql += " AND " + predicate
            parameters += values
        rows = self.db.execute(sql + " ORDER BY position DESC LIMIT ?", (*parameters, limit))
        return [dict(zip(fields, json.loads(row[0]), strict=True)) for row in rows]

    def previous_scope_turn(self, scope, before_sequence):
        """One indexed predecessor; retain invalid/unsent candidates for strict checks.

        Filtering on validity here would silently substitute an older plan when
        the actual preceding result is revoked, failed or still being delivered.
        """
        predicate, values = _exact_scope_filter(scope)
        row = self.db.execute(
            f"SELECT body FROM turns WHERE conversation_id=? AND {predicate} "
            "AND position<? ORDER BY position DESC LIMIT 1",
            (
                scope["conversation_id"],
                *values,
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

    def latest_sources(self, conversation_id, bases):
        """One indexed latest-revision probe per key, batched into bounded SQL reads.

        Do not scan every historical revision with a window function: an old,
        frequently edited source must cost one index probe here too.
        """
        bases = list(dict.fromkeys(bases))
        result = {}
        for offset in range(0, len(bases), 256):
            batch = bases[offset : offset + 256]
            values = ",".join("(?)" for _ in batch)
            rows = self.db.execute(
                f"WITH wanted(base) AS (VALUES {values}) SELECT base,"
                "(SELECT body FROM physicals WHERE conversation_id=? "
                "AND json_extract(body,'$.base')=wanted.base "
                "ORDER BY json_extract(body,'$.revision') DESC LIMIT 1) FROM wanted",
                [*batch, conversation_id],
            )
            result.update((row[0], json.loads(row[1])) for row in rows if row[1] is not None)
        return result

    def source_head(self):
        item = self.get("metadata", "source_head")
        return {k: item[k] for k in ("generation", "sequence")}

    def receipt_input(self, receipt_id):
        row = self.db.execute(
            "SELECT body FROM inbox INDEXED BY inbox_receipt "
            "WHERE json_extract(body,'$.receipt.receipt_id')=? LIMIT 1",
            (receipt_id,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def turn_replies(self, turn_id, state=None):
        return self.replies_for_turns([turn_id], state)[turn_id]

    def replies_for_turns(self, turn_ids, state=None):
        turn_ids = list(dict.fromkeys(turn_ids))
        result = {key: [] for key in turn_ids}
        for offset in range(0, len(turn_ids), 256):
            batch = turn_ids[offset : offset + 256]
            sql = "SELECT body FROM replies WHERE json_extract(body,'$.turn_id') IN ("
            sql += ",".join("?" for _ in batch) + ")"
            args = list(batch)
            if state is not None:
                sql += " AND status=?"
                args.append(state)
            for row in self.db.execute(sql + " ORDER BY position", args):
                reply = self.payloads.restore("replies", json.loads(row[0]))
                result[reply["turn_id"]].append(reply)
        return result
