"""Companion-owned PostgreSQL schema and the SQL forms used by its repositories.

The document body remains canonical TEXT for byte-exact wire and source digests.
JSON paths become typed native jsonb expressions; rowid scans have a real identity
sequence. No business query, predicate or partial-index condition is discarded.
"""

import re
import json
import hashlib
import copy
import uuid

from tianshu_storage import MemoryCursor

JSON_VALUE = re.compile(r"json_extract\((\w+(?:\.\w+)?),'\$\.([^']+)'\)")
JSON_ARRAY = re.compile(r"json_extract\((\w+(?:\.\w+)?),('\$\.[^']+'(?:,'\$\.[^']+')+)\)")
NUMBERS = {
    "revision",
    "command_version",
    "record_version",
    "version",
    "attempt",
    "started",
    "occurrence",
    "due_at",
    "weight",
    "retry_at",
    "next_due_at",
}
BOOLEANS = {"stale", "source_quarantined", "unresolved"}
OBJECTS = {
    "scope",
    "collection_key.author",
    "collection_key.channel",
    "bundle.collection_key.author",
    "bundle.collection_key.channel",
}


def json_expression(match):
    column, path = match.groups()
    native_path = "{" + ",".join(path.split(".")) + "}"
    value = f"({column}::jsonb #>> '{native_path}')"
    if path in OBJECTS:
        return f"NULLIF(({column}::jsonb #> '{native_path}'),'null'::jsonb)"
    # Root LifeWork expiry is an epoch number. Nested authority/source material
    # may instead use ISO expires_at strings, so do not classify that suffix globally.
    if path == "expires_at" or path.rsplit(".", 1)[-1] in NUMBERS:
        kind = (
            "bigint"
            if path.rsplit(".", 1)[-1]
            in {"revision", "command_version", "record_version", "version", "attempt"}
            else "double precision"
        )
        return f"({value}::{kind})"
    if path in BOOLEANS:
        return f"(CASE {value} WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE {value}::integer END)"
    return value


def json_array_projection(match):
    """Keep SQLite multi-path projection values typed, including objects/nulls."""
    column, paths = match.groups()
    values = [
        column + "::jsonb #> '{" + ",".join(path.split(".")) + "}'"
        for path in re.findall(r"'\$\.([^']+)'", paths)
    ]
    return "jsonb_build_array(" + ",".join(values) + ")::text"


def translate(sql):
    # Only the fixed syntax used by this product is translated. Unknown syntax
    # continues to PostgreSQL and fails visibly rather than being ignored.
    sql = sql.replace(
        "json_each(t.body,'$.preparation.selected_units') u",
        "LATERAL jsonb_array_elements(t.body::jsonb #> '{preparation,selected_units}') AS u(value)",
    )
    sql = JSON_ARRAY.sub(json_array_projection, sql)
    sql = JSON_VALUE.sub(json_expression, sql)
    sql = sql.replace("? IS NULL", "CAST(? AS TEXT) IS NULL")
    sql = re.sub(r"\s+INDEXED BY \w+", "", sql, flags=re.I)
    sql = re.sub(r"\bREAL\b", "DOUBLE PRECISION", sql, flags=re.I)
    return sql


def hash_row(checksum, row):
    from .contracts import canonical

    checksum.update(canonical(list(row)).encode("utf-8"))
    checksum.update(b"\n")


def verify_rows(connection, table, columns, order, checksum, count):
    """Bounded server-side readback verifies every imported field, not counts alone."""
    actual = hashlib.sha256()
    found = 0
    with connection.raw.cursor(name="ts233_verify_" + uuid.uuid4().hex) as cursor:
        cursor.itersize = 64
        cursor.execute(f"SELECT {columns} FROM {table} ORDER BY {order}")
        for row in cursor:
            hash_row(actual, row)
            found += 1
    if count != found or actual.digest() != checksum.digest():
        raise RuntimeError("Companion migration field verification failed")


def existing_schema(connection, required):
    """A ready shared binding is recovery evidence, never permission to recreate facts."""
    state = connection.native(
        "SELECT state FROM __tianshu_storage_binding WHERE singleton=1"
    ).fetchone()
    if state is None or state[0] not in {"initializing", "ready"}:
        raise RuntimeError("Companion PostgreSQL binding state invalid")
    if state[0] == "initializing":
        return False
    tables = {
        row[0]
        for row in connection.native(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname=current_schema()"
        )
    }
    if not set(required) <= tables:
        raise RuntimeError("Companion PostgreSQL tables missing")
    return True


def dialect(connection, sql, parameters):
    text = sql.strip().rstrip(";")
    if text.upper() == "PRAGMA USER_VERSION":
        return connection.native("SELECT version FROM companion_schema WHERE id=1")
    if text.upper().startswith("PRAGMA USER_VERSION="):
        return connection.native(
            "UPDATE companion_schema SET version=? WHERE id=1", (int(text.split("=")[1]),)
        )
    if text.upper().startswith(
        ("PRAGMA JOURNAL_MODE", "PRAGMA SYNCHRONOUS", "PRAGMA BUSY_TIMEOUT")
    ):
        return MemoryCursor()
    if text.upper() == "BEGIN IMMEDIATE":
        result = connection.native("BEGIN")
        connection.native("SELECT pg_advisory_xact_lock(hashtextextended(current_schema(),233))")
        return result
    if text.upper() == "BEGIN":
        return connection.native("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    native = translate(sql)
    if getattr(connection, "companion_indexes", False) and re.match(
        r"CREATE INDEX IF NOT EXISTS ", text, re.I
    ):
        name = text.split()[5]
        old = connection.native(
            "SELECT statement,definition FROM companion_indexes WHERE name=?", (name,)
        ).fetchone()
        actual = connection.native(
            "SELECT pg_get_indexdef(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=? AND c.relkind='i'",
            (name,),
        ).fetchone()
        if old is not None and (old[0] != native or (actual is not None and actual[0] != old[1])):
            raise RuntimeError("Companion PostgreSQL index definition mismatch: " + name)
        if actual is not None and old is None:
            raise RuntimeError("Unverified Companion PostgreSQL index: " + name)
        result = connection.native(native, parameters)
        if old is None:
            definition = connection.native(
                "SELECT pg_get_indexdef(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=? AND c.relkind='i'",
                (name,),
            ).fetchone()[0]
            connection.native(
                "INSERT INTO companion_indexes VALUES(?,?,?)", (name, native, definition)
            )
        return result
    if native != sql:
        return connection.native(native, parameters)
    return None


def prepare(connection, source, payloads=None):
    from .store import TABLES

    version = source.execute("PRAGMA user_version").fetchone()[0] if source else 0
    if version not in range(11):
        raise RuntimeError("Unsupported Companion migration source")
    if existing_schema(connection, TABLES | {"companion_schema", "companion_indexes"}):
        existing = connection.native("SELECT version FROM companion_schema WHERE id=1").fetchone()
        if existing is None or existing[0] not in range(11) or source is not None:
            raise RuntimeError("Unsupported Companion target schema")
        return
    connection.native(
        "CREATE TABLE IF NOT EXISTS companion_schema(id INTEGER PRIMARY KEY,version INTEGER NOT NULL)"
    )
    connection.native(
        "CREATE TABLE IF NOT EXISTS companion_indexes(name TEXT PRIMARY KEY,statement TEXT NOT NULL,definition TEXT NOT NULL)"
    )
    existing = connection.native("SELECT version FROM companion_schema WHERE id=1").fetchone()
    if existing:
        if source is not None:
            raise RuntimeError("Companion target already initialized")
        return
    for table in sorted(TABLES):
        connection.native(
            f'CREATE TABLE {table}(id TEXT COLLATE "C" PRIMARY KEY,conversation_id TEXT COLLATE "C",position BIGINT,'
            'status TEXT COLLATE "C",deadline DOUBLE PRECISION,body TEXT COLLATE "C" NOT NULL,'
            "rowid BIGINT GENERATED BY DEFAULT AS IDENTITY UNIQUE)"
        )
        if (
            source
            and source.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
        ):
            rows = source.execute(
                f"SELECT id,conversation_id,position,status,deadline,body,rowid FROM {table} ORDER BY rowid"
            )
            count = 0
            checksum = hashlib.sha256()
            for row in rows:
                row = list(row)
                if payloads is not None and table in {"replies", "write_materials"}:
                    from .contracts import canonical

                    original = payloads.restore(table, json.loads(row[5]))
                    stored = payloads.freeze(table, original)
                    if payloads.restore(table, copy.deepcopy(stored)) != original:
                        raise RuntimeError("Companion payload migration verification failed")
                    row[5] = canonical(stored)
                connection.native(f"INSERT INTO {table} VALUES(?,?,?,?,?,?,?)", tuple(row))
                hash_row(checksum, row)
                count += 1
            verify_rows(
                connection,
                table,
                "id,conversation_id,position,status,deadline,body,rowid",
                "rowid",
                checksum,
                count,
            )
            # Explicit rowids preserve both learning cursors and their continuation.
            connection.native(
                f"SELECT setval(pg_get_serial_sequence('{table}','rowid'),"
                f"GREATEST(COALESCE((SELECT MAX(rowid) FROM {table}),0)+1,1),false)"
            )
    connection.native("INSERT INTO companion_schema VALUES(1,?)", (version,))


def claim_owner(connection):
    row = connection.native(
        "SELECT pg_try_advisory_lock(hashtextextended(current_schema(),23300))"
    ).fetchone()
    if not row[0]:
        raise RuntimeError("Database already has a running owner")
