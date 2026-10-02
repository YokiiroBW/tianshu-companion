"""Derived indexes for bounded runtime work, with a pre-DDL restore point."""

import sqlite3
import uuid
from .life_read_index import shape

INDEXES = {
    "outbox_turn": "CREATE INDEX IF NOT EXISTS outbox_turn ON outbox(json_extract(body,'$.event.aggregate_id'),status)",
    "outbox_due": "CREATE INDEX IF NOT EXISTS outbox_due ON outbox(status,deadline,id)",
    "collections_due": "CREATE INDEX IF NOT EXISTS collections_due ON collections(status,deadline,position,id)",
    "turns_work": "CREATE INDEX IF NOT EXISTS turns_work ON turns(conversation_id,position,id) WHERE status IN ('queued','preparing','generating','waiting_dependency','ready_to_send','sending','reconciling')",
    "replies_unknown": "CREATE INDEX IF NOT EXISTS replies_unknown ON replies(conversation_id,id,json_extract(body,'$.turn_id')) WHERE status='unknown'",
    "inbox_context": "CREATE INDEX IF NOT EXISTS inbox_context ON inbox(conversation_id,json_extract(body,'$.actor_id'),position,id) WHERE json_extract(body,'$.stale')=0",
    "direct_waiting_band": "CREATE INDEX IF NOT EXISTS direct_waiting_band ON direct_requests(conversation_id,id) WHERE status IN ('completed','failed') AND json_extract(body,'$.reply_state')='ready_to_deliver' AND json_extract(body,'$.deferred_reason')='outbound_band_busy'",
    "life_generation_due": "CREATE INDEX IF NOT EXISTS life_generation_due ON metadata(conversation_id,deadline,position,id) WHERE status IN ('queued','unavailable','failed') AND json_extract(body,'$.attempt')<3",
    "metadata_history_page": "CREATE INDEX IF NOT EXISTS metadata_history_page ON metadata(conversation_id,status,position DESC,id DESC)",
}


def prepare(connection, path, *, restored, fresh):
    missing = False
    for name, sql in INDEXES.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,)
        ).fetchone()
        if row is None:
            missing = True
        elif shape(row[0]) != shape(sql):
            raise RuntimeError("Runtime work index definition mismatch: " + name)
    if missing and not fresh and not restored and str(path) != ":memory:":
        backup = sqlite3.connect(str(path) + ".pre-work-index-" + uuid.uuid4().hex + ".bak")
        try:
            connection.backup(backup)
        finally:
            backup.close()
        return True
    return restored


def create(connection):
    for sql in INDEXES.values():
        connection.execute(sql)
