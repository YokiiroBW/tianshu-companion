"""Read-only PC baseline. No application assembly, owner lease, model or send.

Run with the deployment's Companion settings and storage manifest. Credentials
stay in memory; output contains aggregates and fixed SQL, never document bodies.
Database metrics share one repeatable-read, read-only PostgreSQL transaction.
File logs have their own explicit observation window and coverage statement.
"""

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import time

import psycopg
from psycopg import sql


PROACTIVE_TABLES = (
    "proactive_reminders",
    "proactive_candidates",
    "proactive_attempts",
    "proactive_goals",
    "proactive_motives",
    "proactive_quota",
    "proactive_subscriptions",
    "proactive_templates",
)


def database_metrics(connection, schema, now, hours=24):
    queries = []

    def read(name, statement, parameters=()):
        queries.append(dict(name=name, sql=statement, parameters=list(parameters)))
        return connection.execute(statement, parameters or None).fetchall()

    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        connection.execute("SET LOCAL statement_timeout = '10s'")
        connection.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema)))
        readonly = connection.execute("SHOW transaction_read_only").fetchone()[0]
        snapshot = connection.execute("SELECT pg_current_snapshot()::text").fetchone()[0]
        states = read(
            "goal_choice_all",
            "SELECT status, count(*) FROM metadata WHERE id LIKE 'goal-choice:%' GROUP BY status",
        )
        recent = read(
            "goal_choice_24h",
            "SELECT status, count(*) FROM metadata WHERE id LIKE 'goal-choice:%%' AND (body::jsonb->>'created_at')::double precision >= %s GROUP BY status",
            (now - hours * 3600,),
        )
        actors = read(
            "goal_choice_by_actor",
            "SELECT body::jsonb->>'actor_id', status, count(*) FROM metadata WHERE id LIKE 'goal-choice:%' GROUP BY 1, 2",
        )
        failure_fields = read(
            "goal_choice_failures",
            "SELECT body::jsonb->'failure_diagnostic'->>'peer', body::jsonb->'failure_diagnostic'->>'route', body::jsonb->'failure_diagnostic'->>'code', body::jsonb->'failure_diagnostic'->>'stage', count(*) FROM metadata WHERE id LIKE 'goal-choice:%' AND status <> 'completed' GROUP BY 1, 2, 3, 4",
        )
        activities = read(
            "activities", "SELECT status, count(*) FROM life_activities GROUP BY status"
        )
        null_due = read(
            "paused_null_due",
            "SELECT count(*), min((body::jsonb->>'updated_at')::double precision) FROM life_activities WHERE status='paused' AND body::jsonb->>'next_due_at' IS NULL",
        )[0]
        stalled = read(
            "activity_stalls",
            "SELECT count(*) FROM life_activities WHERE status IN ('paused','dormant') AND COALESCE((body::jsonb->>'dormant_since')::double precision, (body::jsonb->>'updated_at')::double precision, (body::jsonb->>'created_at')::double precision) < %s",
            (now - 86400,),
        )[0][0]
        actor_rows = read(
            "actors",
            "SELECT id, body::jsonb->>'cursor', body::jsonb->>'changed_at', body::jsonb->>'mood', jsonb_array_length(COALESCE(body::jsonb->'life_interests','[]'::jsonb)), body::jsonb->>'life_enabled', body::jsonb->>'autonomous' FROM life_actors ORDER BY id",
        )
        diaries = read("diaries", "SELECT status, count(*) FROM life_diaries GROUP BY status")
        diary_days = read(
            "diary_days",
            "SELECT body::jsonb->>'day', status, count(*) FROM life_diaries GROUP BY 1, 2 ORDER BY 1 DESC LIMIT 120",
        )
        concerns = read(
            "concerns",
            "SELECT count(*), count(*) FILTER (WHERE id LIKE 'autonomy:%'), count(*) FILTER (WHERE body::jsonb->>'next_due_at' IS NULL) FROM life_concerns",
        )[0]
        events = read("events", "SELECT body::jsonb->>'kind', count(*) FROM life_events GROUP BY 1")
        proactive = {
            table: read(table, "SELECT count(*) FROM " + table)[0][0] for table in PROACTIVE_TABLES
        }
        generation = read(
            "daily_generation",
            "SELECT conversation_id, status, count(*) FROM metadata WHERE conversation_id LIKE 'life:generation:%' GROUP BY 1, 2",
        )
        generation_usage = read(
            "generation_receipt_usage",
            "SELECT conversation_id, count(*) FILTER (WHERE body::jsonb#>>'{receipt,usage,input_tokens}' IS NOT NULL), sum((body::jsonb#>>'{receipt,usage,input_tokens}')::bigint), sum((body::jsonb#>>'{receipt,usage,output_tokens}')::bigint) FROM metadata WHERE conversation_id LIKE 'life:generation:%' GROUP BY 1",
        )
        choice_usage = read(
            "choice_receipt_usage",
            "SELECT count(*) FILTER (WHERE body::jsonb#>>'{receipt,usage,input_tokens}' IS NOT NULL), avg((body::jsonb#>>'{receipt,usage,input_tokens}')::bigint) FROM metadata WHERE id LIKE 'goal-choice:%'",
        )[0]
        proactive_events = read(
            "proactive_events",
            "SELECT count(*) FROM metadata WHERE conversation_id='proactive:event'",
        )[0][0]
    total, completed = sum(dict(recent).values()), dict(recent).get("completed", 0)
    all_states = dict(states)
    return dict(
        transaction=dict(
            read_only=readonly, isolation="repeatable read", snapshot=snapshot, schema=schema
        ),
        goal_choice=dict(
            all_states=all_states,
            all_success_rate=all_states.get("completed", 0) / sum(all_states.values())
            if sum(all_states.values())
            else None,
            window_hours=hours,
            window_states=dict(recent),
            window_total=total,
            window_success_rate=completed / total if total else None,
            by_actor=[dict(actor_id=a, state=s, count=n) for a, s, n in actors],
            failures=[
                dict(peer=p, route=r, code=c, stage=s, count=n) for p, r, c, s, n in failure_fields
            ],
        ),
        activities=dict(
            states=dict(activities),
            paused_null_next_due=null_due[0],
            paused_null_rate=null_due[0] / dict(activities)["paused"]
            if dict(activities).get("paused")
            else None,
            oldest_paused_null_updated_at=null_due[1],
            stalled_over_24h=stalled,
        ),
        actors=[
            dict(
                zip(
                    (
                        "actor_id",
                        "cursor",
                        "changed_at",
                        "mood",
                        "interest_count",
                        "life_enabled",
                        "autonomous",
                    ),
                    row,
                )
            )
            for row in actor_rows
        ],
        diaries=dict(
            states=dict(diaries), days=[dict(day=d, state=s, count=n) for d, s, n in diary_days]
        ),
        proactive=dict(
            table_rows=proactive,
            all_tables_empty=not any(proactive.values()),
            event_markers=proactive_events,
        ),
        concerns=dict(total=concerns[0], autonomy_prefixed=concerns[1], null_next_due=concerns[2]),
        events=dict(total=sum(n for _, n in events), by_kind=dict(events)),
        generation=[dict(scope=c, state=s, count=n) for c, s, n in generation],
        receipt_usage=dict(
            generation=[
                dict(
                    scope=c,
                    measured_receipts=n,
                    input_tokens=int(i) if i is not None else None,
                    output_tokens=int(o) if o is not None else None,
                )
                for c, n, i, o in generation_usage
            ],
            goal_choice=dict(
                measured_receipts=choice_usage[0],
                mean_input_tokens=float(choice_usage[1]) if choice_usage[1] is not None else None,
            ),
            basis="Persisted original receipts only; absent usage remains unknown, never zero.",
        ),
        queries=queries,
    )


def log_metrics(directory, now, minutes, byte_limit=256 * 1024 * 1024):
    cutoff, counts, failures = now - minutes * 60, Counter(), Counter()
    files, read_bytes, malformed, coverage, peer_calls, attributed = [], 0, 0, True, 0, 0
    first, last = None, None
    traces = []
    for path in sorted(
        Path(directory).glob("*.jsonl*"), key=lambda p: p.stat().st_mtime, reverse=True
    ):
        if path.stat().st_mtime < cutoff:
            continue
        if read_bytes + path.stat().st_size > byte_limit:
            coverage = False
            break
        files.append(path.name)
        with path.open("rb") as stream:
            for line in stream:
                read_bytes += len(line)
                if len(line) > 4096:
                    malformed += 1
                    continue
                try:
                    event = json.loads(line)
                    moment = datetime.fromisoformat(
                        event["timestamp"].replace("Z", "+00:00")
                    ).timestamp()
                except (ValueError, TypeError, KeyError):
                    malformed += 1
                    continue
                if not cutoff <= moment <= now:
                    continue
                first = moment if first is None else min(first, moment)
                last = moment if last is None else max(last, moment)
                name, code = event.get("event", "unknown"), event.get("error_code")
                counts[name] += 1
                if code:
                    failures[(name, code)] += 1
                if name == "peer.call.finished":
                    peer_calls += 1
                    attrs = event.get("attributes") or {}
                    if attrs.get("target") not in (None, "unknown") and attrs.get("route") not in (
                        None,
                        "unknown",
                    ):
                        attributed += 1
                    if code and len(traces) < 12:
                        traces.append(
                            dict(
                                timestamp=event["timestamp"],
                                correlation_id=event.get("correlation_id"),
                                error_code=code,
                                **{
                                    key: attrs[key]
                                    for key in (
                                        "target",
                                        "route",
                                        "principal",
                                        "http_status",
                                        "attempt",
                                    )
                                    if key in attrs
                                },
                            )
                        )
    failed, worked = counts["runtime.background_failed"], counts["runtime.background_work"]
    return dict(
        window_minutes=minutes,
        interval_start=cutoff,
        interval_end=now,
        files=files,
        bytes_read=read_bytes,
        within_byte_budget=coverage,
        malformed_lines=malformed,
        first_event=first,
        last_event=last,
        event_counts=dict(counts),
        errors=[dict(event=e, code=c, count=n) for (e, c), n in sorted(failures.items())],
        auth_rejected_per_minute=counts["http.auth.rejected"] / minutes,
        background_failure_per_work=failed / worked if worked else None,
        background_failure_per_pass=failed / (worked + failed) if worked + failed else None,
        background_work_per_minute=worked / minutes,
        idle_tick_per_minute=None,
        idle_tick_note="Idle passes are intentionally not logged; work events do not prove idle ticks.",
        peer_finished=peer_calls,
        peer_attributed=attributed,
        attribution_rate=attributed / peer_calls if peer_calls else None,
        failure_traces=traces,
    )


def collect(settings, *, manifest=None, minutes=5, log_directories=None):
    config = json.loads(Path(settings).read_text("utf-8"))
    guard_path = (
        Path(manifest)
        if manifest
        else Path(config["database_path"] + ".storage/.tianshu-storage-companion.json")
    )
    guard = json.loads(guard_path.read_text("utf-8"))
    if guard.get("backend") != "postgresql" or guard.get("state") != "active":
        raise ValueError("active_postgresql_binding_required")
    schema = guard["databases"]["main"]
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema):
        raise ValueError("invalid_schema")
    now = time.time()
    # The read-only default also protects the connection setup, before BEGIN.
    with psycopg.connect(
        config["storage_backends"]["postgresql_url"],
        autocommit=True,
        connect_timeout=10,
        options="-c default_transaction_read_only=on",
    ) as connection:
        metrics = database_metrics(connection, schema, now)
    return dict(
        schema_version=1,
        collected_at=datetime.fromtimestamp(now, timezone.utc).isoformat(),
        database=metrics,
        logs={
            name: log_metrics(path, now, minutes) for name, path in (log_directories or {}).items()
        },
        limitations=[
            "Database metrics are one PostgreSQL snapshot; file log windows are sampled separately.",
            "No idle-tick counter exists in persisted logs.",
            "Historical all-attempt rates and the 24-hour decision rate are reported separately.",
        ],
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings", required=True)
    parser.add_argument("--manifest")
    parser.add_argument("--companion-logs")
    parser.add_argument("--platform-logs")
    parser.add_argument("--log-minutes", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.log_minutes <= 1440:
        parser.error("--log-minutes must be 1..1440")
    directories = {
        key: value
        for key, value in (("companion", args.companion_logs), ("platform", args.platform_logs))
        if value
    }
    try:
        result = collect(
            args.settings,
            manifest=args.manifest,
            minutes=args.log_minutes,
            log_directories=directories,
        )
    except Exception as error:
        # Driver messages can contain a connection string or a server diagnostic.
        print(
            json.dumps(dict(status="collection_failed", error_type=type(error).__name__)),
            file=sys.stderr,
        )
        return 1
    text = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
