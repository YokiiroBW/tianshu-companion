"""The one derived index the life-read page needs, and the restore point that precedes it.

Reading a page of published diaries newest-first cannot use any index the store already
creates: `life_diaries_queue` leads with `status`, which a page read does not constrain, and
diaries are written without a sequence, so `position` is always 0 and cannot order by day. A
page is therefore answered by one new derived index, and this module owns its whole life
cycle - the definition, the check that an index already carrying the name really is that
index, the restore point taken before it is created, and the creation itself.

Three rules are load-bearing:

- **Detection happens before any DDL of the open.** A database that already holds facts and
  lacks the index gets a full SQLite backup (WAL included) first, so a failure while creating
  it still leaves a restorable file; a database that is being created empty needs no restore
  point, and a database that already carries the correct index adds no file and no fact. An
  open that already took a complete pre-DDL backup reuses it instead of piling up copies.
- **A wrong definition is a startup failure, not something to `IF NOT EXISTS` past.** The
  name is the contract; an index with that name over different columns, a different order or
  a different partial predicate would silently answer a different question (and would still
  be reported as the fast path), so it stops the process instead.
- **The index is derived.** No table, no field, no `user_version` step, no lock: dropping and
  recreating it changes performance only, and `IF NOT EXISTS` keeps every open idempotent.
"""

import re
import sqlite3
import uuid

INDEX_NAME = "life_diaries_published_page"
# Byte-identical to the definition this task froze. SQLite stores the statement with
# `IF NOT EXISTS` removed and everything else verbatim, so comparison below normalises only
# whitespace, keyword case and that clause - never the quoted JSON paths, which are
# case-sensitive (`'$.Day'` is a different key, not a different spelling).
CREATE_SQL = (
    "CREATE INDEX IF NOT EXISTS life_diaries_published_page ON life_diaries("
    "conversation_id,json_extract(body,'$.day') DESC,id DESC) "
    "WHERE json_extract(body,'$.published_revision') IS NOT NULL"
)
TIMELINE_INDEX = "life_known_timeline_page"
TIMELINE_CREATE_SQL = (
    "CREATE INDEX IF NOT EXISTS life_known_timeline_page ON life_known("
    "conversation_id,status,position DESC,id DESC)"
)
BACKUP_LABEL = ".pre-life-read-index-"
_CLAUSE = re.compile(r"\bif not exists\b", re.IGNORECASE)
_LITERAL = re.compile(r"'(?:[^']|'')*'")


def shape(statement):
    """One comparable shape for an index statement.

    Case and whitespace of SQL keywords and identifiers carry no meaning, but a quoted
    literal does - it is a JSON path here - so the literals are compared separately and
    exactly. `None` (an index with no stored SQL) shapes to nothing and never matches.
    """
    if not isinstance(statement, str):
        return None, ()
    text = _CLAUSE.sub(" ", statement)
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower(), tuple(_LITERAL.findall(text))


def stored_definition(connection):
    """The stored SQL of the index carrying this name, or None when it does not exist."""
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (INDEX_NAME,)
    ).fetchone()
    return row[0] if row else None


def restore_point(connection, path):
    """One complete backup of the database as this connection sees it, WAL included.

    A unique name per attempt means an earlier recovery file is never overwritten, and a
    failure propagates: the caller must not run DDL after a backup that did not happen.
    """
    backup = sqlite3.connect(str(path) + BACKUP_LABEL + uuid.uuid4().hex + ".bak")
    try:
        connection.backup(backup)
    finally:
        backup.close()


def prepare_page_index(connection, path, *, restore_point_taken, fresh):
    """Detect the derived index and take the restore point this open needs.

    Called before the open runs any DDL. Returns the index name when a correct index is
    already present, or None when this open still has to create it. A same-named index with
    a different definition raises, which stops startup before any structural statement runs.
    """
    missing = False
    for name, expected in ((INDEX_NAME, CREATE_SQL), (TIMELINE_INDEX, TIMELINE_CREATE_SQL)):
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,)
        ).fetchone()
        definition = row[0] if row else None
        if definition is None:
            missing = True
        elif shape(definition) != shape(expected):
            raise RuntimeError(name + " exists with a different definition; refusing to start")
    if not missing:
        return INDEX_NAME
    # Missing. A fresh database is being created by this open and there is nothing to
    # restore; an existing one either already has this open's complete pre-DDL backup or
    # needs a restore point of its own now.
    if not fresh and not restore_point_taken and str(path) != ":memory:":
        restore_point(connection, path)
    return None


def create_page_index(connection):
    """Create the derived index inside the caller's existing initialization transaction.

    Never called per request, and idempotent: an open that already found a correct index
    still reaches this line and changes nothing.
    """
    connection.execute(CREATE_SQL)
    connection.execute(TIMELINE_CREATE_SQL)
