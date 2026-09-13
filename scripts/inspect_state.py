"""Read-only local diagnostics. Content requires an explicit operator flag."""

import argparse
import json
import sqlite3
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("database", type=Path)
parser.add_argument("--include-context", action="store_true")
args = parser.parse_args()
db = sqlite3.connect(args.database.resolve().as_uri() + "?mode=ro", uri=True)
db.execute("BEGIN")
try:
    turns = [json.loads(row[0]) for row in db.execute("SELECT body FROM turns ORDER BY position")]
    result = dict(
        turns=[
            {
                key: turn.get(key)
                for key in (
                    "id",
                    "sequence",
                    "phase",
                    "version",
                    "result_version",
                    "delivery_state",
                    "model_calls",
                    "timings",
                    "failure",
                    "unresolved_delivery",
                )
            }
            for turn in turns
        ],
        replies=[],
        outbox=[],
    )
    for row in db.execute("SELECT body FROM replies ORDER BY position"):
        reply = json.loads(row[0])
        result["replies"].append(
            {k: reply.get(k) for k in ["id", "turn_id", "state", "attempted_at", "receipt"]}
        )
    for row in db.execute("SELECT body FROM outbox ORDER BY position"):
        item = json.loads(row[0])
        result["outbox"].append({k: item.get(k) for k in ["id", "state", "attempts", "last_error"]})
    if args.include_context:
        result["contexts"] = [
            {k: turn.get(k) for k in ["id", "bundle", "preparation", "role"]} for turn in turns
        ]
    print(json.dumps(result, ensure_ascii=False, indent=2))
finally:
    db.rollback()
    db.close()
