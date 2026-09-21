"""The life-read query port: one short read transaction, one index seek, no writes.

These cases are about the storage contract the read feature depends on, not about the read
rules (those live in `test_life_read.py`): that every operation runs inside a `BEGIN`/`COMMIT`
read transaction with no write statement anywhere, that a page is answered by the derived
index without a temporary B-tree and without a cost that grows with unrelated rows, and that
the keyset step neither repeats nor drops a row.
"""

import json

import pytest

from test_life_read import build, equipped, facts
from tianshu_companion.life_read_queries import (
    DIARIES_PAGE,
    DIARIES_PAGE_AFTER,
    LifeReadQueries,
)
from tianshu_companion.store import Store

INDEX = "life_diaries_published_page"
WRITE_WORDS = ("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "PRAGMA")


class Recorder:
    """A connection stand-in that records exactly the statements the port runs."""

    def __init__(self, connection):
        self.connection, self.statements = connection, []

    def execute(self, sql, parameters=()):
        self.statements.append((" ".join(sql.split()), parameters))
        return self.connection.execute(sql, parameters)

    def statements_matching(self, fragment):
        return [(sql, values) for sql, values in self.statements if fragment in sql]


def bulk(store, actor_id, *, published, unpublished=0, other_actor=0):
    """Rows written straight to the store, for storage-level cases only."""
    with store.transaction():
        for index in range(published):
            store.put(
                "life_diaries",
                dict(
                    id="%s:p%04d" % (actor_id, index),
                    conversation_id=actor_id,
                    day="2026-09-%02d" % (index % 28 + 1),
                    published_revision="revision:%d" % index,
                    state="published",
                ),
            )
        for index in range(unpublished):
            store.put(
                "life_diaries",
                dict(
                    id="%s:u%05d" % (actor_id, index),
                    conversation_id=actor_id,
                    day="2026-08-%02d" % (index % 28 + 1),
                    published_revision=None,
                    state="draft",
                ),
            )
        for index in range(other_actor):
            store.put(
                "life_diaries",
                dict(
                    id="actor:other:%05d" % index,
                    conversation_id="actor:other",
                    day="2026-07-%02d" % (index % 28 + 1),
                    published_revision="revision:other:%d" % index,
                    state="published",
                ),
            )


def steps(connection, run):
    """Count SQLite virtual-machine instructions for one call."""
    counted = []

    def handler():
        counted.append(1)
        return 0

    connection.set_progress_handler(handler, 1)
    try:
        result = run()
    finally:
        connection.set_progress_handler(None, 0)
    return len(counted), result


def page_steps(unrelated):
    store = Store(":memory:")
    bulk(store, "actor:a", published=30, unpublished=40, other_actor=unrelated)
    count, rows = steps(store.db, lambda: LifeReadQueries(store.db).diaries_page("actor:a", 21))
    store.close()
    return count, len(rows)


def test_the_read_transaction_is_a_short_begin_commit_that_never_writes():
    fixture = build()
    recorder = Recorder(fixture.store.db)
    queries = LifeReadQueries(recorder)
    with queries.reading():
        queries.actor("actor:a")
        queries.access("actor:a")
        queries.world("world:home")
        queries.room("room:study")
        queries.diary("diary:a")
        queries.revision("revision:a")
        queries.diaries_page("actor:a", 21, ("2026-09-20", "diary:a"))
    statements = [sql for sql, _ in recorder.statements]
    assert statements[0] == "BEGIN"
    assert statements[-1] == "COMMIT"
    assert all(sql.startswith("SELECT") for sql in statements[1:-1])
    assert not [sql for sql in statements if any(word in sql.upper() for word in WRITE_WORDS)]
    assert not fixture.store.db.in_transaction


def test_an_error_inside_the_read_rolls_back_and_leaves_the_store_usable():
    fixture = build()
    recorder = Recorder(fixture.store.db)
    queries = LifeReadQueries(recorder)
    with pytest.raises(RuntimeError):
        with queries.reading():
            queries.actor("actor:a")
            raise RuntimeError("synthetic read failure")
    statements = [sql for sql, _ in recorder.statements]
    assert statements[0] == "BEGIN" and statements[-1] == "ROLLBACK"
    assert "COMMIT" not in statements
    assert not fixture.store.db.in_transaction
    with queries.reading():
        queries.actor("actor:a")
    assert not fixture.store.db.in_transaction


def test_the_page_read_is_an_index_seek_without_a_temporary_btree():
    fixture = build()
    bulk(fixture.store, "actor:a", published=30, unpublished=40, other_actor=2000)
    recorder = Recorder(fixture.store.db)
    queries = LifeReadQueries(recorder)
    queries.diaries_page("actor:a", 21)
    queries.diaries_page("actor:a", 21, ("2026-09-20", "actor:a:p0003"))
    first, second = (query for query in recorder.statements if "life_diaries" in query[0])
    assert first[0] == " ".join(DIARIES_PAGE.split())
    assert second[0] == " ".join(DIARIES_PAGE_AFTER.split())
    for sql, parameters in (first, second):
        plan = fixture.store.db.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
        detail = " | ".join(str(row[-1]) for row in plan)
        assert INDEX in detail, detail
        assert "TEMP B-TREE" not in detail.upper(), detail


def test_page_cost_does_not_grow_with_unrelated_or_unpublished_rows():
    small, small_rows = page_steps(20)
    large, large_rows = page_steps(20000)
    assert small_rows == large_rows == 21  # one row past the page, never the whole history
    assert large < 1000, large
    assert large - small < 50, (small, large)


def test_keyset_paging_returns_each_published_row_exactly_once():
    fixture = build()
    bulk(fixture.store, "actor:a", published=55, unpublished=20, other_actor=30)
    queries = LifeReadQueries(fixture.store.db)
    whole = queries.diaries_page("actor:a", 1000)
    expected = [(row["day"], row["id"]) for row in whole]
    seen, after = [], None
    while True:
        rows = queries.diaries_page("actor:a", 11, after)
        more = len(rows) > 10
        page = rows[:10]
        seen.extend((row["day"], row["id"]) for row in page)
        if not more:
            break
        after = (page[-1]["day"], page[-1]["id"])
    assert seen == expected
    assert len(seen) == 55
    assert all(row["id"].startswith("actor:a:p") for row in whole)
    assert all(
        row["id"].startswith("actor:other") for row in queries.diaries_page("actor:other", 1000)
    )


def test_the_port_runs_no_statement_outside_a_read_transaction():
    fixture, port = equipped()
    fixture.world()
    fixture.room()
    fixture.actor("actor:a")
    fixture.grant("actor:a")
    recorder = Recorder(fixture.store.db)
    port.queries.db = recorder
    before = facts(fixture.store)
    port.handle("story_reader", "actors", {"schema_version": 1})
    port.handle("story_reader", "snapshot", {"schema_version": 1, "actor_id": "actor:a"})
    statements = [sql for sql, _ in recorder.statements]
    assert statements.count("BEGIN") == statements.count("COMMIT") == 2
    assert not [sql for sql in statements if any(word in sql.upper() for word in WRITE_WORDS)]
    assert facts(fixture.store) == before


def test_single_row_reads_are_primary_key_lookups_and_parse_the_body():
    fixture = build()
    fixture.world()
    fixture.room()
    fixture.actor("actor:a")
    recorder = Recorder(fixture.store.db)
    queries = LifeReadQueries(recorder)
    assert queries.world("world:home")["id"] == "world:home"
    assert queries.room("room:study")["world_id"] == "world:home"
    assert queries.actor("actor:a")["id"] == "actor:a"
    assert queries.actor("actor:missing") is None
    assert queries.access("actor:a") is None
    assert queries.diary("diary:missing") is None
    assert queries.revision("revision:missing") is None
    for sql, parameters in recorder.statements:
        assert sql.endswith("WHERE id=?") and len(parameters) == 1


def test_the_page_count_is_exactly_what_the_caller_asked_for():
    store = Store(":memory:")
    bulk(store, "actor:a", published=12)
    queries = LifeReadQueries(store.db)
    assert len(queries.diaries_page("actor:a", 5)) == 5
    assert len(queries.diaries_page("actor:a", 12)) == 12
    assert len(queries.diaries_page("actor:a", 13)) == 12
    assert (
        json.loads(json.dumps(queries.diaries_page("actor:a", 1)[0]))["conversation_id"]
        == "actor:a"
    )
    store.close()
