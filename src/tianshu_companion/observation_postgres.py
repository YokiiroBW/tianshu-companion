"""Native schema/import for the independently named passive-observation database."""

import hashlib

from .storage_postgres import existing_schema, hash_row, verify_rows


def prepare(connection, source):
    if existing_schema(connection, {"inbox"}):
        return
    connection.native("""CREATE TABLE IF NOT EXISTS inbox (
        source_ref TEXT COLLATE "C" PRIMARY KEY,digest TEXT NOT NULL,
        instance_id TEXT NOT NULL,self_id TEXT NOT NULL,
        conversation TEXT NOT NULL,author TEXT NOT NULL,archive_epoch BIGINT NOT NULL,
        event_id TEXT NOT NULL,observed_at DOUBLE PRECISION NOT NULL,
        payload TEXT NOT NULL,archive_state TEXT NOT NULL,error_code TEXT,
        updated_at DOUBLE PRECISION NOT NULL,attempts BIGINT NOT NULL DEFAULT 0,
        next_attempt_at DOUBLE PRECISION NOT NULL DEFAULT 0,
        UNIQUE(instance_id,self_id,conversation,author,event_id))""")
    connection.native(
        "CREATE INDEX IF NOT EXISTS inbox_pending ON inbox(archive_state,observed_at,source_ref)"
    )
    connection.native(
        "CREATE INDEX IF NOT EXISTS inbox_page ON inbox(instance_id,self_id,archive_epoch,conversation,source_ref)"
    )
    if source is not None:
        columns = [row[1] for row in source.execute("PRAGMA table_info(inbox)")]
        if columns:
            names = ",".join(columns)
            count = 0
            checksum = hashlib.sha256()
            for row in source.execute("SELECT " + names + " FROM inbox ORDER BY source_ref"):
                connection.native(
                    "INSERT INTO inbox("
                    + names
                    + ") VALUES("
                    + ",".join("?" for _ in columns)
                    + ")",
                    tuple(row),
                )
                hash_row(checksum, row)
                count += 1
            verify_rows(connection, "inbox", names, "source_ref", checksum, count)
