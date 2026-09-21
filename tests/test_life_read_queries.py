"""The life-read query port: one short read transaction, one index seek, no writes.

These cases are about the storage contract the read feature depends on, not about the read
rules (those live in `test_life_read.py`): that every operation runs inside a `BEGIN`/`COMMIT`
read transaction with no write statement anywhere, that a page is answered by the derived
index without a temporary B-tree and without a cost that grows with rows it did not return -
neither other actors' rows nor the same actor's rows the cursor already passed - and that the
keyset step neither repeats nor drops a row.
"""

import json

import pytest

from test_life_read import build, equipped, facts
from tianshu_companion.life_read_queries import (
    DIARIES_EARLIER_DAY,
    DIARIES_PAGE,
    DIARIES_SAME_DAY,
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


def plan(connection, sql, parameters):
    """The query plan of one statement, as SQLite reports it."""
    return " | ".join(
        str(row[-1]) for row in connection.execute("EXPLAIN QUERY PLAN " + sql, parameters)
    )


def layered(store, actor_id, days):
    """Published diaries with explicit days and ids, written straight to the store."""
    with store.transaction():
        for day, ids in days:
            for diary_id in ids:
                store.put(
                    "life_diaries",
                    dict(
                        id=diary_id,
                        conversation_id=actor_id,
                        day=day,
                        published_revision="revision:" + diary_id,
                        state="published",
                    ),
                )


def same_day(store, actor_id, count):
    """One actor, one day, `count` published diaries with increasing ids."""
    layered(store, actor_id, [("2026-09-20", ["d%06d" % index for index in range(count)])])


def deep_page_steps(count, limit=11, after=("2026-09-20", "d000020")):
    """Steps and page of a same-day continuation over `count` rows of one actor's history."""
    store = Store(":memory:")
    same_day(store, "actor:a", count)
    queries = LifeReadQueries(store.db)
    cost, rows = steps(store.db, lambda: queries.diaries_page("actor:a", limit, after))
    store.close()
    return cost, [row["id"] for row in rows]


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
    first, second, third = (query for query in recorder.statements if "life_diaries" in query[0])
    assert first[0] == " ".join(DIARIES_PAGE.split())
    assert second[0] == " ".join(DIARIES_SAME_DAY.split())
    assert third[0] == " ".join(DIARIES_EARLIER_DAY.split())
    for sql, parameters in (first, second, third):
        detail = plan(fixture.store.db, sql, parameters)
        assert INDEX in detail, detail
        assert "TEMP B-TREE" not in detail.upper(), detail
    # Naming the index is not enough: the plan must constrain the range columns, or SQLite
    # seeks on the conversation and then judges every stored row of that actor.
    assert plan(fixture.store.db, DIARIES_SAME_DAY, ("actor:a", "2026-09-20", "x", 21)).endswith(
        "(conversation_id=? AND <expr>=? AND id<?)"
    )
    assert plan(fixture.store.db, DIARIES_EARLIER_DAY, ("actor:a", "2026-09-20", 21)).endswith(
        "(conversation_id=? AND <expr><?)"
    )


def test_a_same_day_deep_page_costs_the_same_at_every_history_size():
    """The cost follows the rows returned, not the rows the cursor already passed.

    One actor, one day, the same cursor and the same 11-row answer: a hundred rows of history
    and ten thousand must cost the same. The row-value comparison this replaced sought only on
    `conversation_id` and then judged the tuple per row, so the same page cost 1,061 steps over
    100 rows and 109,961 over 10,000.
    """
    small, small_rows = deep_page_steps(100)
    large, large_rows = deep_page_steps(10000)
    assert small_rows == large_rows == ["d%06d" % index for index in range(19, 8, -1)]
    assert large < 1000, large
    assert large - small < 50, (small, large)


def test_the_older_day_range_runs_only_when_the_cursor_day_is_exhausted():
    fixture = build()
    same_day(fixture.store, "actor:a", 40)
    recorder = Recorder(fixture.store.db)
    queries = LifeReadQueries(recorder)
    # The cursor's day still holds more rows than the page needs: one statement is enough.
    assert len(queries.diaries_page("actor:a", 11, ("2026-09-20", "d000030"))) == 11
    assert [sql for sql, _ in recorder.statements].count(" ".join(DIARIES_EARLIER_DAY.split())) == 0
    # Near the end of the day the remainder is filled from the next older day, in order.
    layered(fixture.store, "actor:a", [("2026-09-19", ["e%06d" % index for index in range(30)])])
    rows = queries.diaries_page("actor:a", 11, ("2026-09-20", "d000003"))
    assert [(row["day"], row["id"]) for row in rows] == [
        ("2026-09-20", "d000002"),
        ("2026-09-20", "d000001"),
        ("2026-09-20", "d000000"),
        *[("2026-09-19", "e%06d" % index) for index in range(29, 21, -1)],
    ]


def test_the_two_continuation_ranges_never_exceed_the_requested_limit():
    fixture = build()
    layered(
        fixture.store,
        "actor:a",
        [
            ("2026-09-20", ["d%06d" % index for index in range(4)]),
            ("2026-09-19", ["e%06d" % index for index in range(20)]),
        ],
    )
    recorder = Recorder(fixture.store.db)
    queries = LifeReadQueries(recorder)
    rows = queries.diaries_page("actor:a", 11, ("2026-09-20", "d000003"))
    limits = [values[-1] for sql, values in recorder.statements if "life_diaries" in sql]
    assert len(limits) == 2 and limits == [11, 8]  # the older range only asks for the remainder
    assert len(rows) == 11
    assert [row["id"] for row in rows][:3] == ["d000002", "d000001", "d000000"]
    assert all(row["day"] == "2026-09-19" for row in rows[3:])
    # A page larger than the remaining history returns what exists, with no lookahead row.
    assert len(queries.diaries_page("actor:a", 40, ("2026-09-20", "d000003"))) == 23


def test_paging_continues_after_a_deleted_cursor_row_and_stops_on_the_last_page():
    fixture = build()
    layered(
        fixture.store,
        "actor:a",
        [
            ("2026-09-20", ["d%06d" % index for index in range(5)]),
            ("2026-09-19", ["e%06d" % index for index in range(5)]),
            ("2026-09-18", ["f%06d" % index for index in range(3)]),
        ],
    )
    queries = LifeReadQueries(fixture.store.db)
    whole = [(row["day"], row["id"]) for row in queries.diaries_page("actor:a", 100)]
    seen, after = [], None
    while True:
        rows = queries.diaries_page("actor:a", 4, after)
        more = len(rows) > 3
        page = rows[:3]
        seen.extend((row["day"], row["id"]) for row in page)
        if not more:
            break
        after = (page[-1]["day"], page[-1]["id"])
        # The cursor is a position: the row it names may be gone and the walk still continues.
        fixture.store.delete("life_diaries", page[-1]["id"])
    assert seen == whole and len(seen) == 13
    assert queries.diaries_page("actor:a", 4, ("2026-09-18", "f000000")) == []


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
