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
  the page statement matches it exactly - same WHERE columns, same ORDER BY, same partial
  predicate - so a page of twenty rows costs a seek instead of visiting every diary of every
  actor. `life_diaries_queue` leads with `status`, which a page read does not constrain, and
  `position` is always 0 for diaries (they carry no sequence), so neither can order by day.
- **The cursor is a position, never a grant.** `after` is compared as a row value, so a
  cursor naming a row that has since been deleted still pages correctly and grants nothing.
- **Rows are parsed here, judged there.** This module returns the stored documents; the
  consistency rules about what those documents must mean live in `life_read`.
"""

import json
from contextlib import contextmanager

# The page read. `(day,id) < (?,?)` is the keyset step and is written as one row value so it
# stays a single range seek; `LIMIT ?` is always `page + 1`, and the extra row is only ever
# used to decide whether a next page exists.
DIARIES_PAGE = (
    "SELECT body FROM life_diaries WHERE conversation_id=? "
    "AND json_extract(body,'$.published_revision') IS NOT NULL "
    "ORDER BY json_extract(body,'$.day') DESC,id DESC LIMIT ?"
)
DIARIES_PAGE_AFTER = (
    "SELECT body FROM life_diaries WHERE conversation_id=? "
    "AND json_extract(body,'$.published_revision') IS NOT NULL "
    "AND (json_extract(body,'$.day'),id)<(?,?) "
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

    def world(self, world_id):
        return self._row(WORLD, world_id)

    def room(self, room_id):
        return self._row(ROOM, room_id)

    def diary(self, diary_id):
        return self._row(DIARY, diary_id)

    def revision(self, revision_id):
        return self._row(REVISION, revision_id)

    def diaries_page(self, actor_id, limit, after=None):
        """One bounded page of published diaries, newest day first.

        `limit` is `page + 1` by convention: the caller asks for one row more than it will
        return, which is how it learns whether a next page exists without counting the
        history. `after` is the `(day, diary_id)` position of the last row of the previous
        page and never widens access.
        """
        if after is None:
            rows = self.db.execute(DIARIES_PAGE, (actor_id, limit)).fetchall()
        else:
            rows = self.db.execute(DIARIES_PAGE_AFTER, (actor_id, after[0], after[1], limit))
        return [json.loads(row[0]) for row in rows]
