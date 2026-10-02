"""Every SELECT the authorized life-read port needs, behind one read-only connection port.

This module exists so the read feature has exactly one place that spells a table name or a
SELECT, and so that place can never write. It opens a short SQLite read transaction
(`BEGIN` / `COMMIT`, never `BEGIN IMMEDIATE`), it holds no application object, it has no
`await`, and it exposes only single-row primary-key reads plus one bounded page query. The
`store.transaction()` context manager is deliberately not used here: that wrapper is the
write path, it advances the fact-stream head when facts change, and a read must not be able
to reach either.

Three properties are load-bearing:

- **The page is an index range seek, not a scan.** The one new derived index is keyed
  `(conversation_id, json_extract(body,'$.day') DESC, id DESC)` over the published rows, and
  every page statement matches it exactly - same WHERE columns, same ORDER BY, same partial
  predicate - so a page of twenty rows costs a seek instead of visiting every diary of every
  actor. `life_diaries_queue` leads with `status`, which a page read does not constrain, and
  `position` is always 0 for diaries (they carry no sequence), so neither can order by day.
  A continuation is two seeks into that same index (see `DIARIES_SAME_DAY` /
  `DIARIES_EARLIER_DAY`), not one pass over everything the cursor already passed.
- **The cursor is a position, never a grant.** `after` only selects a range of an index the
  reader is already allowed to read, so a cursor naming a row that has since been deleted
  still pages correctly and grants nothing.
- **Rows are parsed here, judged there.** This module returns the stored documents; the
  consistency rules about what those documents must mean live in `life_read`.
"""

import json
from contextlib import contextmanager

# The page read, in three statements. `LIMIT ?` is always at most `page + 1`, and the extra
# row is only ever used to decide whether a next page exists.
#
# The continuation after a cursor is deliberately *not* one row-value comparison. Written as
# `(day,id) < (?,?)` SQLite can only seek on `conversation_id` and then evaluates the tuple
# once per stored row, so a deep page costs one step per row already passed: 100 and 10,000
# same-day rows cost 1,061 and 109,961 virtual-machine steps for the same 11-row answer. The
# cursor is therefore split into the index's own two ranges - the rest of the cursor's day,
# then strictly older days - each of which the derived index seeks into:
#
#   (conversation_id=? AND <day>=? AND id<?)   and   (conversation_id=? AND <day><?)
#
# Both keep the index order (`day DESC, id DESC`; with the day pinned by equality the id
# order is the same) so neither needs a sort, and the earlier-day statement only runs when
# the same-day range did not already fill the page. The pair returns at most `limit` rows in
# total and the two ranges are disjoint, so the step is still exact - no repeats, no gaps.
DIARIES_PAGE = (
    "SELECT body FROM life_diaries WHERE conversation_id=? "
    "AND json_extract(body,'$.published_revision') IS NOT NULL "
    "ORDER BY json_extract(body,'$.day') DESC,id DESC LIMIT ?"
)
DIARIES_SAME_DAY = (
    "SELECT body FROM life_diaries WHERE conversation_id=? "
    "AND json_extract(body,'$.day')=? "
    "AND json_extract(body,'$.published_revision') IS NOT NULL "
    "AND id<? ORDER BY id DESC LIMIT ?"
)
DIARIES_EARLIER_DAY = (
    "SELECT body FROM life_diaries WHERE conversation_id=? "
    "AND json_extract(body,'$.day')<? "
    "AND json_extract(body,'$.published_revision') IS NOT NULL "
    "ORDER BY json_extract(body,'$.day') DESC,id DESC LIMIT ?"
)
# One primary-key read per relation. Spelled out rather than assembled, so no statement in
# this module is ever built from anything that could vary.
ACTOR = "SELECT body FROM life_actors WHERE id=?"
ACCESS = "SELECT body FROM life_access WHERE id=?"
WORLD = "SELECT body FROM life_worlds WHERE id=?"
ROOM = "SELECT body FROM life_rooms WHERE id=?"
DIARY = "SELECT body FROM life_diaries WHERE id=?"
REVISION = "SELECT body FROM life_revisions WHERE id=?"
PLAN = "SELECT body FROM metadata WHERE id=?"
EVENT = "SELECT body FROM life_events WHERE id=?"
TIMELINE_PAGE = (
    "SELECT body FROM life_known WHERE conversation_id=? AND status=? "
    "ORDER BY position DESC,id DESC LIMIT ?"
)
TIMELINE_SAME_POSITION = (
    "SELECT body FROM life_known WHERE conversation_id=? AND status=? AND position=? "
    "AND id<? ORDER BY id DESC LIMIT ?"
)
TIMELINE_EARLIER_POSITION = (
    "SELECT body FROM life_known WHERE conversation_id=? AND status=? AND position<? "
    "ORDER BY position DESC,id DESC LIMIT ?"
)


class LifeReadQueries:
    """One narrow port: single-row primary-key reads and one bounded page query.

    The caller supplies the process's single-owner SQLite connection; nothing else about the
    store, the core or the HTTP layer is reachable from here. Every statement is a fixed
    parameterised string, so no caller input ever becomes SQL text.
    """

    def __init__(self, connection):
        self.db = connection

    @contextmanager
    def reading(self):
        """One short read transaction around a whole operation.

        A deferred `BEGIN` is enough: every statement here is a read, the transaction is
        never held across an `await` or an HTTP send, and an error rolls it back instead of
        leaving a half-applied snapshot behind. `BEGIN IMMEDIATE` (the write path) is not
        used, so a read can never take the write lock or advance the fact head.
        """
        self.db.execute("BEGIN")
        try:
            yield self
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")

    def _row(self, statement, key):
        row = self.db.execute(statement, (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def actor(self, actor_id):
        return self._row(ACTOR, actor_id)

    def access(self, actor_id):
        """The persisted story grant for one actor; absent means locked, never default-open."""
        return self._row(ACCESS, actor_id)

    def runtime_role(self, actor_id):
        return self._row(PLAN, "runtime-role:" + actor_id)

    def runtime_actor_ids(self, after, limit):
        rows = self.db.execute(
            "SELECT id FROM metadata WHERE id>? AND id<? ORDER BY id LIMIT ?",
            ("runtime-role:" + (after or ""), "runtime-role;", limit),
        ).fetchall()
        return [row[0][len("runtime-role:") :] for row in rows]

    def world(self, world_id):
        return self._row(WORLD, world_id)

    def room(self, room_id):
        return self._row(ROOM, room_id)

    def diary(self, diary_id):
        return self._row(DIARY, diary_id)

    def revision(self, revision_id):
        return self._row(REVISION, revision_id)

    def plan(self, plan_id):
        return self._row(PLAN, plan_id)

    def event(self, event_id):
        return self._row(EVENT, event_id)

    def timeline_page(self, actor_id, day, limit, after=None):
        if after is None:
            rows = self.db.execute(TIMELINE_PAGE, (actor_id, day, limit)).fetchall()
        else:
            position, known_id = after
            rows = self.db.execute(
                TIMELINE_SAME_POSITION, (actor_id, day, position, known_id, limit)
            ).fetchall()
            if len(rows) < limit:
                rows += self.db.execute(
                    TIMELINE_EARLIER_POSITION, (actor_id, day, position, limit - len(rows))
                ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def diaries_page(self, actor_id, limit, after=None):
        """One bounded page of published diaries, newest day first.

        `limit` is `page + 1` by convention: the caller asks for one row more than it will
        return, which is how it learns whether a next page exists without counting the
        history. `after` is the `(day, diary_id)` position of the last row of the previous
        page and never widens access. The continuation is two index ranges - the rest of the
        cursor's day, then older days if that was not enough - so the cost follows the rows
        returned rather than the rows already passed.
        """
        if after is None:
            rows = self.db.execute(DIARIES_PAGE, (actor_id, limit)).fetchall()
        else:
            day, diary_id = after
            rows = self.db.execute(DIARIES_SAME_DAY, (actor_id, day, diary_id, limit)).fetchall()
            remaining = limit - len(rows)
            if remaining > 0:
                rows += self.db.execute(DIARIES_EARLIER_DAY, (actor_id, day, remaining)).fetchall()
        return [json.loads(row[0]) for row in rows]
