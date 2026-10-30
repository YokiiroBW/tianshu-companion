"""PC-05: review historical pauses without executing work. Dry-run is the default.

Run only during the existing stopped-writer maintenance boundary. The backup is
COPY data for the two affected tables, plus exact before/after rows for a scoped
rollback. Credentials remain in the configured settings file.
"""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import time

import psycopg
from psycopg import sql


def convergence(activities, actors, now, *, remove_placeholder=False):
    changes = []
    for item in activities:
        actor = actors.get(item["actor_id"])
        stale = (
            not actor
            or actor.get("activity_id") != item["id"]
            or item.get("runtime_epoch") != actor.get("life_runtime_epoch", 0)
        )
        if item["state"] != "paused" or not (item.get("next_due_at") is None or stale):
            continue
        after = copy.deepcopy(item)
        after.update(
            state="dormant",
            next_due_at=None,
            deadline=None,
            dormant_since=now,
            resume_condition="legacy_pause_requires_explicit_review",
            updated_at=now,
            version=item["version"] + 1,
            sequence=int(now),
        )
        changes.append(dict(table="life_activities", id=item["id"], before=item, after=after))
    retired = {change["id"] for change in changes}
    for actor in actors.values():
        if actor["id"] == "actor:household" and remove_placeholder:
            changes.append(dict(table="life_actors", id=actor["id"], before=actor, after=None))
        elif actor.get("activity_id") in retired:
            after = copy.deepcopy(actor)
            after.update(
                activity_id=None,
                activity_scope=None,
                cursor=None,
                changed_at=now,
                version=actor["version"] + 1,
            )
            changes.append(dict(table="life_actors", id=actor["id"], before=actor, after=after))
    return changes


def read_tables(db):
    activities = [
        json.loads(row[0]) for row in db.execute("SELECT body FROM life_activities ORDER BY id")
    ]
    actors = [json.loads(row[0]) for row in db.execute("SELECT body FROM life_actors ORDER BY id")]
    return activities, {item["id"]: item for item in actors}


def write_changes(db, changes, *, rollback=False):
    for change in changes:
        before, after = (
            (change["after"], change["before"]) if rollback else (change["before"], change["after"])
        )
        table = sql.Identifier(change["table"])
        row = db.execute(
            sql.SQL("SELECT body FROM {} WHERE id=%s FOR UPDATE").format(table), (change["id"],)
        ).fetchone()
        if (json.loads(row[0]) if row else None) != before:
            raise ValueError("changed_row_requires_review")
        if after is None:
            db.execute(sql.SQL("DELETE FROM {} WHERE id=%s").format(table), (change["id"],))
        else:
            db.execute(
                sql.SQL(
                    "INSERT INTO {}(id,conversation_id,position,status,deadline,body) VALUES(%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(id) DO UPDATE SET conversation_id=excluded.conversation_id,position=excluded.position,"
                    "status=excluded.status,deadline=excluded.deadline,body=excluded.body"
                ).format(table),
                (
                    after["id"],
                    after.get("conversation_id"),
                    after.get("sequence", 0),
                    after.get("phase", after.get("state")),
                    after.get("deadline"),
                    json.dumps(after, ensure_ascii=False, separators=(",", ":")),
                ),
            )


def collect(
    settings, *, apply=False, backup=None, expected=None, remove_placeholder=False, rollback=False
):
    cfg = json.loads(Path(settings).read_bytes())
    manifest = json.loads(
        Path(cfg["database_path"] + ".storage/.tianshu-storage-companion.json").read_bytes()
    )
    if manifest["backend"] != "postgresql" or manifest["state"] != "active":
        raise ValueError("active_postgresql_binding_required")
    schema = manifest["databases"]["main"]
    with (
        psycopg.connect(
            cfg["storage_backends"]["postgresql_url"],
            autocommit=True,
            options="-c default_transaction_read_only=" + ("off" if apply else "on"),
        ) as db,
        db.transaction(),
    ):
        db.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ "
            + ("READ WRITE" if apply else "READ ONLY")
        )
        db.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema)))
        db.execute("SET LOCAL statement_timeout='15s'")
        activities, actors = read_tables(db)
        changes = convergence(
            activities, actors, time.time(), remove_placeholder=remove_placeholder
        )
        summary = dict(
            schema=schema,
            dry_run=not apply,
            changes=[
                dict(
                    table=c["table"],
                    id=c["id"],
                    from_state=c["before"].get("state"),
                    to_state=(c["after"] or {}).get("state"),
                    deleted=c["after"] is None,
                )
                for c in changes
            ],
            activity_count=len(activities),
            actor_count=len(actors),
            historical_activity_deletions=0,
            model_calls=0,
            messages_sent=0,
        )
        if not apply:
            return summary
        directory = Path(backup)
        if rollback:
            saved = json.loads((directory / "changes.private.json").read_bytes())
            write_changes(db, saved, rollback=True)
            return dict(status="scoped_rows_rolled_back", count=len(saved))
        expected_summary = json.loads(Path(expected).read_bytes())
        if summary["changes"] != expected_summary["changes"]:
            raise ValueError("dry_run_changed")
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        for table in ("life_activities", "life_actors"):
            path = directory / (table + ".copy")
            with (
                path.open("xb") as out,
                db.cursor().copy(
                    sql.SQL("COPY {} TO STDOUT").format(sql.Identifier(table))
                ) as stream,
            ):
                for data in stream:
                    out.write(data)
                out.flush()
                os.fsync(out.fileno())
        (directory / "changes.private.json").write_text(
            json.dumps(changes, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        write_changes(db, changes)
        remaining = db.execute(
            "SELECT count(*) FROM life_activities WHERE status='paused' AND deadline IS NULL"
        ).fetchone()[0]
        if remaining:
            raise ValueError("paused_blackhole_remains")
        summary.update(
            dry_run=False,
            status="converged",
            paused_null_remaining=remaining,
            backup_sha256={
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in directory.iterdir()
                if path.is_file()
            },
        )
        return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("settings")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup")
    parser.add_argument("--expected")
    parser.add_argument("--remove-installation-placeholder", action="store_true")
    parser.add_argument("--rollback", action="store_true")
    args = parser.parse_args()
    if args.apply and (not args.backup or not args.rollback and not args.expected):
        parser.error("--apply requires --backup and --expected (or --rollback)")
    result = collect(
        args.settings,
        apply=args.apply,
        backup=args.backup,
        expected=args.expected,
        remove_placeholder=args.remove_installation_placeholder,
        rollback=args.rollback,
    )
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"status": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
