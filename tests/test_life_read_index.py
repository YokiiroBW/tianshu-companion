"""The derived life-read index: detection, restore point, idempotence, refusal.

The index is the one structural change this task makes, and it is derived: no table, no field,
no `user_version` step. These cases check the rules around it - that a same-named index with a
different definition stops startup before anything runs, that a database which already holds
facts gets a complete restore point (WAL included) before the first new DDL while a fresh one
gets none, that the creation is transactional and idempotent, and that an open which already
took this open's pre-DDL backup reuses it instead of piling up copies.
"""

import sqlite3
import uuid
from unittest.mock import patch

import pytest

from tianshu_companion.life_read_index import (
    BACKUP_LABEL,
    CREATE_SQL,
    INDEX_NAME,
    create_page_index,
    prepare_page_index,
    shape,
    stored_definition,
)
from tianshu_companion.store import Store

FROZEN = (
    "CREATE INDEX IF NOT EXISTS life_diaries_published_page ON life_diaries("
    "conversation_id,json_extract(body,'$.day') DESC,id DESC) "
    "WHERE json_extract(body,'$.published_revision') IS NOT NULL"
)


def backups(path):
    return sorted(path.parent.glob(path.name + BACKUP_LABEL + "*.bak"))


def definition(path):
    with sqlite3.connect(path) as connection:
        return stored_definition(connection)


def rows(path):
    with sqlite3.connect(path) as connection:
        return [
            tuple(row) for row in connection.execute("SELECT id,body FROM life_actors ORDER BY id")
        ]


def seeded(path, *, facts=("actor:a", "actor:b")):
    """A real store with facts, its WAL still live, and the derived index removed."""
    store = Store(path)
    with store.transaction():
        for actor_id in facts:
            store.put("life_actors", dict(id=actor_id, mood="calm", version=1))
    assert path.with_name(path.name + "-wal").stat().st_size > 0, "the facts must still be in WAL"
    store.db.execute("DROP INDEX " + INDEX_NAME)
    return store


def test_the_frozen_definition_and_its_comparison_shape():
    assert CREATE_SQL == FROZEN
    stored = FROZEN.replace("IF NOT EXISTS ", "")
    assert shape(stored) == shape(CREATE_SQL)
    assert shape("  CREATE INDEX IF NOT EXISTS  " + stored.split("INDEX ", 1)[1] + "  ") == shape(
        CREATE_SQL
    )
    # Whitespace and keyword case carry no meaning, but the quoted JSON paths do: a different
    # path is a different index, not a different spelling of this one.
    for variant in (
        stored.replace("'$.day'", "'$.Day'"),
        stored.replace("conversation_id,", ""),
        stored.replace(" DESC", ""),
        stored.split(" WHERE ")[0],
        stored.replace("life_diaries(", "life_other("),
    ):
        assert shape(variant) != shape(CREATE_SQL), variant
    assert shape(None) == (None, ()) and shape(None) != shape(CREATE_SQL)


def test_a_fresh_database_gets_the_index_without_a_restore_point(tmp_path):
    path = tmp_path / "fresh.db"
    store = Store(path)
    try:
        assert shape(definition(path)) == shape(CREATE_SQL)
        assert backups(path) == []
        assert definition(path).endswith("IS NOT NULL")
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 9
    finally:
        store.close()
    memory = Store(":memory:")
    try:
        assert stored_definition(memory.db) is not None
    finally:
        memory.close()


def head(path):
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT body FROM metadata WHERE id='source_head'").fetchone()
        return row[0] if row else None


def test_a_database_that_lost_the_index_is_backed_up_before_the_ddl(tmp_path):
    path = tmp_path / "existing.db"
    seeded(path).close()
    before, head_before = rows(path), head(path)
    assert definition(path) is None
    store = Store(path)  # the real open: detect, restore point, then create
    try:
        (backup,) = backups(path)
        assert stored_definition(store.db) is not None
        assert store.source_head() is not None
    finally:
        store.close()
    # The restore point holds the facts as they were before the new DDL, readable on its own,
    # and it does not contain the index that was about to be created.
    assert rows(backup) == before
    assert head(backup) == head_before
    assert definition(backup) is None
    assert definition(path) is not None
    assert rows(path) == before
    assert head(path) == head_before
    reopened = Store(path)
    reopened.close()
    assert backups(path) == [backup]  # a correct reopen adds no second copy


def test_the_restore_point_includes_the_live_wal(tmp_path):
    """The facts are still only in the write-ahead log when the backup is taken."""
    path = tmp_path / "wal.db"
    store = Store(path)
    try:
        with store.transaction():
            store.put("life_actors", dict(id="actor:wal", mood="calm", version=1))
        wal = path.with_name(path.name + "-wal")
        assert wal.stat().st_size > 0
        # Nothing checkpoints here: the row lives in the WAL only until one happens, so a
        # backup that ignored the WAL could not contain it.
        store.db.execute("DROP INDEX " + INDEX_NAME)
        live = rows(path)
        prepare_page_index(store.db, path, restore_point_taken=False, fresh=False)
        (backup,) = backups(path)
        assert rows(backup) == live
        assert [row[0] for row in live] == ["actor:wal"]
    finally:
        store.close()


def test_a_wrong_definition_with_the_same_name_stops_startup(tmp_path):
    path = tmp_path / "wrong.db"
    store = Store(path)
    store.db.execute("DROP INDEX " + INDEX_NAME)
    store.db.execute("CREATE INDEX " + INDEX_NAME + " ON life_diaries(conversation_id)")
    store.close()
    with pytest.raises(RuntimeError) as raised:
        Store(path)
    assert INDEX_NAME in str(raised.value)
    assert backups(path) == []
    assert definition(path) == "CREATE INDEX " + INDEX_NAME + " ON life_diaries(conversation_id)"
    # The refusal must release the single-owner lock, so an operator can fix the schema.
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version").fetchone()


def test_a_failed_restore_point_leaves_no_new_ddl(tmp_path):
    path = tmp_path / "failed.db"
    store = seeded(path)
    try:
        assert definition(path) is None
        with sqlite3.connect(path) as connection:
            before = connection.execute(
                "SELECT name,sql FROM sqlite_master ORDER BY name"
            ).fetchall()
        with pytest.raises(sqlite3.OperationalError):
            # The backup path is inside a directory that does not exist, which is the same
            # class of failure as a full disk or a permission denial.
            prepare_page_index(
                store.db, tmp_path / "missing" / "db", restore_point_taken=False, fresh=False
            )
        with sqlite3.connect(path) as connection:
            after = connection.execute(
                "SELECT name,sql FROM sqlite_master ORDER BY name"
            ).fetchall()
        assert after == before
        assert definition(path) is None
        with sqlite3.connect(path) as connection:
            connection.execute("PRAGMA user_version").fetchone()
    finally:
        store.close()


def test_a_v8_open_reuses_its_migration_backup_and_adds_no_second_copy(tmp_path):
    path = tmp_path / "v8.db"
    store = Store(path)
    store.db.execute("DROP INDEX " + INDEX_NAME)
    store.db.execute("PRAGMA user_version=8")
    store.close()
    reopened = Store(path)
    try:
        assert stored_definition(reopened.db) is not None
        assert reopened.db.execute("PRAGMA user_version").fetchone()[0] == 9
    finally:
        reopened.close()
    names = sorted(item.name for item in tmp_path.iterdir())
    assert [name for name in names if BACKUP_LABEL in name] == []
    assert [name for name in names if ".pre-persona-ops-v9-" in name]


def test_the_index_is_created_inside_the_initialization_transaction(tmp_path):
    path = tmp_path / "atomic.db"
    first = Store(path)
    first.db.execute("DROP INDEX " + INDEX_NAME)
    first.db.execute("PRAGMA user_version=8")
    first.close()
    calls = []

    def failing(connection):
        create_page_index(connection)
        calls.append(stored_definition(connection))
        raise RuntimeError("synthetic failure after the index create")

    with patch("tianshu_companion.store.create_page_index", failing):
        with pytest.raises(RuntimeError):
            Store(path)
    assert calls, "the real create must have run inside the transaction"
    with sqlite3.connect(path) as connection:
        assert stored_definition(connection) is None
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8


def test_reopening_a_correct_index_changes_no_fact_and_no_file(tmp_path):
    path = tmp_path / "stable.db"
    store = Store(path)
    with store.transaction():
        store.put("life_actors", dict(id="actor:a", mood="calm", version=1))
    head, before = store.source_head(), rows(path)
    store.close()
    for _ in range(2):
        reopened = Store(path)
        try:
            assert reopened.source_head() == head
            assert stored_definition(reopened.db) is not None
            assert reopened.db.execute("PRAGMA user_version").fetchone()[0] == 9
        finally:
            reopened.close()
    assert rows(path) == before
    assert backups(path) == []


def test_preparation_is_read_only_when_it_has_nothing_to_add(tmp_path):
    path = tmp_path / "present.db"
    store = Store(path)
    try:
        with sqlite3.connect(path) as connection:
            before = connection.execute(
                "SELECT name,sql FROM sqlite_master ORDER BY name"
            ).fetchall()
        assert (
            prepare_page_index(store.db, path, restore_point_taken=False, fresh=False) == INDEX_NAME
        )
        with sqlite3.connect(path) as connection:
            after = connection.execute(
                "SELECT name,sql FROM sqlite_master ORDER BY name"
            ).fetchall()
        assert after == before
        assert backups(path) == []
    finally:
        store.close()


def test_the_backup_name_is_unique_per_attempt(tmp_path):
    path = tmp_path / "unique.db"
    store = seeded(path)
    try:
        prepare_page_index(store.db, path, restore_point_taken=False, fresh=False)
        prepare_page_index(store.db, path, restore_point_taken=False, fresh=False)
        created = backups(path)
    finally:
        store.close()
    assert len(created) == 2
    assert len({item.name for item in created}) == 2
    for item in created:
        assert uuid.UUID(item.name.split(BACKUP_LABEL)[1][:32])
        assert rows(item)
