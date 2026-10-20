"""Real SQL/file boundaries, using synthetic payloads and opt-in isolated PG."""

import asyncio
import base64
import json
import os
import sqlite3
import shutil
import subprocess
import sys
from pathlib import Path
import uuid

import pytest

from support import Harness
from tianshu_companion.contracts import Fault, canonical, digest
from tianshu_companion.store import Store


def media_reply():
    data = b"synthetic image bytes" * 4096
    request = dict(
        request_id="request",
        segments=[
            dict(
                media=[
                    dict(
                        encoding="base64",
                        data=base64.b64encode(data).decode(),
                        media_type="image/png",
                    )
                ]
            )
        ],
    )
    return dict(
        id="reply",
        state="unknown",
        v2_request=request,
        sequence=101,
        conversation_id="conversation",
        turn_id="turn",
        receipt=None,
    ), data


def test_large_payload_recovery_and_missing_file_refuses_content(tmp_path):
    path = tmp_path / "main.sqlite"
    reply, data = media_reply()
    store = Store(path)
    store.put("replies", reply)
    material = dict(id="material", state="pinned", material=[dict(text="素材" * 50000)])
    store.put("write_materials", material)
    raw = json.loads(store.db.execute("SELECT body FROM replies").fetchone()[0])
    ref = raw["v2_request"]["segments"][0]["media"][0]["payload_file"]
    assert "data" not in raw["v2_request"]["segments"][0]["media"][0]
    assert raw["v2_request_digest"] == digest(reply["v2_request"])
    assert store.payloads.files.read_bytes(ref) == data
    assert "material_file" in json.loads(
        store.db.execute("SELECT body FROM write_materials").fetchone()[0]
    )
    head = store.source_head()
    store.close()
    store = Store(path)
    try:
        assert store.get("replies", "reply") == reply
        assert store.get("write_materials", "material") == material
        assert store.source_head() == head
        (tmp_path / "main.sqlite.files" / ref["relative_path"]).unlink()
        with pytest.raises((Fault, sqlite3.Error)):
            store.get("replies", "reply")
        assert store.db.execute("SELECT status FROM replies").fetchone()[0] == "unknown"
    finally:
        store.close()


def test_existing_inline_payload_is_externalized_without_source_fact(tmp_path):
    path = tmp_path / "existing.sqlite"
    reply, _ = media_reply()
    store = Store(path)
    store.put("replies", dict(reply, v2_request=None))
    head = store.source_head()
    store.close()
    with sqlite3.connect(path) as db:
        db.execute("UPDATE replies SET body=? WHERE id='reply'", (canonical(reply),))
    store = Store(path)
    try:
        assert store.get("replies", "reply") == reply
        assert store.source_head() == head
        assert list(tmp_path.glob("*.pre-payload-files-*.bak"))
    finally:
        store.close()


@pytest.fixture
def pg_settings():
    url = os.environ.get("TS233_POSTGRES_URL")
    if not url:
        pytest.skip("TS233_POSTGRES_URL is required for isolated real PostgreSQL")
    return {"storage_backends": {"postgresql_url": url, "namespace": "ts233_" + uuid.uuid4().hex}}


def test_real_postgres_migrates_facts_files_owner_and_no_sqlite_fallback(tmp_path, pg_settings):
    path = tmp_path / "main.sqlite"
    reply, _ = media_reply()
    local = Store(path)
    local.put("replies", reply)
    head = local.source_head()
    local.close()
    original = path.read_bytes()
    store = Store(path, settings=pg_settings)
    try:
        assert store.backend == "postgresql"
        assert store.get("replies", "reply") == reply
        assert store.source_head() == head
        store.put("metadata", dict(id="pg-only", value=True))
        assert store.source_head() == head
        assert store.unknown_replies("conversation", limit=3)[0]["id"] == "reply"
        other = tmp_path / "other-host.sqlite"
        shutil.copytree(str(path) + ".storage", str(other) + ".storage")
        with pytest.raises(RuntimeError, match="running owner"):
            Store(other, settings=pg_settings)
    finally:
        store.close()
    assert path.read_bytes() == original
    with pytest.raises(sqlite3.Error):
        Store(path, settings={})
    store = Store(path, settings=pg_settings)
    try:
        assert store.get("metadata", "pg-only")["value"] is True
    finally:
        store.close()


def test_real_postgres_life_writing_and_cas(tmp_path, pg_settings):
    from test_writing import approve_publish, build, draft

    store = Store(tmp_path / "life.sqlite", settings=pg_settings)
    try:
        port, life, _, model = build(store, chapters=1)
        draft(port, model)
        approve_publish(port, "work:1")
        assert port.chapter_metadata("work:1")["state"] == "published"
        previous = port.work_metadata("work")["version"]
        port.update_outline("work", "changed", author="admin", expected=previous)
        with pytest.raises(ValueError):
            port.update_outline("work", "stale", author="admin", expected=previous)
        before = store.source_head()
        from tianshu_companion.life_read_queries import LifeReadQueries

        with LifeReadQueries(store.db).reading() as reads:
            assert reads.actor("a") is not None
        assert store.source_head() == before
        assert life.snapshot("a")["actor"]["id"] == "a"
    finally:
        store.close()


def test_real_postgres_core_restart_retains_unknown_without_resend(tmp_path, pg_settings):
    async def run():
        from tianshu_companion.core import Core

        h = Harness(path=str(tmp_path / "core.sqlite"), silence_ms=0)
        await h.core.close()

        def reopen():
            return Core(
                Store(tmp_path / "core.sqlite", settings=pg_settings),
                h.contracts,
                h.origins,
                h.memory,
                h.gateway,
                h.sender,
                **h.options,
            )

        h.core = reopen()
        h.sender.states = ["unknown"]
        try:
            h.core.recover()
            await h.ingest()
            await h.cycles(60)
            assert h.core.store.list("turns")
            assert len(h.sender.calls) == 1
            assert h.core.store.list("replies")[0]["state"] == "unknown"
            await h.core.close()
            h.core = reopen()
            h.core.recover()
            await h.cycles(30)
            assert len(h.sender.calls) == 1
            assert h.core.store.list("replies")[0]["state"] == "unknown"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_real_postgres_nonebot_journal_migrates_claims_and_event_order(tmp_path, pg_settings):
    from tianshu_nonebot.journal import Conflict, Journal

    path = tmp_path / "journal.sqlite"
    journal = Journal(path)
    journal.bind("connection", "nonebot", "42")
    event = dict(connection_id="connection", event_id="e", account_id="7", text="synthetic")
    key = journal.capture(event)
    delivery = dict(reply_id="reply", attempt_id="attempt", text="synthetic")
    assert journal.claim(delivery)
    journal.close()
    journal = Journal(path, storage_backends=pg_settings["storage_backends"])
    assert journal.storage_runtime.backend == "postgresql"
    try:
        journal.bind("connection", "nonebot", "42")
        assert journal.pending_events() == [(key, event)]
        assert not journal.claim(delivery)
        assert journal.unacked()[0]["state"] == "unknown"
        with pytest.raises(Conflict):
            journal.claim(dict(delivery, text="changed"))
        assert journal.start_event(key)
    finally:
        journal.close()
    journal = Journal(path, storage_backends=pg_settings["storage_backends"])
    try:
        journal.recover_events()
        assert journal.unknown_events() == [(key, event)]
        assert journal.pending_events() == []
    finally:
        journal.close()


def test_real_postgres_nonebot_rpc_observation_and_restart(tmp_path, pg_settings, monkeypatch):
    import test_observation_adapter as existing

    original = existing.AdapterService

    def service(*args, **kwargs):
        result = original(*args, **kwargs, storage_backends=pg_settings["storage_backends"])
        assert result.storage_runtime.backend == "postgresql"
        return result

    monkeypatch.setattr(existing, "AdapterService", service)
    existing.test_group_private_modes_and_restart(tmp_path)


def test_real_postgres_passive_observations_keep_acceptance_and_archive(tmp_path, pg_settings):
    from tianshu_storage import StorageOptions, StorageRuntime
    from tianshu_companion.observation import Observations

    class Memory:
        async def call(self, path, payload, **kwargs):
            return dict(
                source_ref=payload["source_ref"], state="accepted", archive_state="archived"
            )

    event = dict(
        schema_version=2,
        platform_id="synthetic",
        self_id="42",
        namespace="qq",
        conversation_id="private:7",
        account_id="7",
        event_id="event",
        revision=1,
        sent_at="2026-10-20T01:00:00Z",
        text="synthetic",
        content_state="text",
        mentioned=False,
        scope_revision=1,
    )
    request = dict(
        event=event,
        source_ref="source",
        source_digest=digest(event),
        scope_version=1,
        archive_epoch=1,
    )
    path = tmp_path / "observations.sqlite"
    source = Observations(path, Memory())
    assert source.ingest("platform", request)["state"] == "accepted"
    runtime = StorageRuntime(
        StorageOptions.from_settings(pg_settings),
        service="companion",
        state_directory=tmp_path / "storage",
    )
    runtime.start()
    try:
        target = Observations(path, Memory(), storage_runtime=runtime)
        runtime.finalize()
        assert target.database.backend == "postgresql"
        assert target.ingest("platform", request)["state"] == "duplicate"
        assert asyncio.run(target.flush())
        result = target.query(
            "platform",
            dict(
                instance_id="synthetic",
                self_id="42",
                conversation_id=None,
                limit=20,
                cursor=None,
                archive_epoch=1,
            ),
        )
        assert result["backlog"] == {"archived": 1}
    finally:
        runtime.close()


def test_real_postgres_json_types_array_scope_and_read_only(tmp_path, pg_settings):
    store = Store(tmp_path / "types.sqlite", settings=pg_settings)
    try:
        assert store.backend == "postgresql"
        item = dict(
            id="turn",
            sequence=1,
            phase="queued",
            conversation_id="c",
            scope={"person_id": "p"},
            preparation={"selected_units": [{"record_id": "record", "record_version": 12}]},
            stale=False,
            version=12,
        )
        store.put("turns", item)
        sql = "SELECT t.body FROM turns t, json_each(t.body,'$.preparation.selected_units') u WHERE json_extract(u.value,'$.record_id')=? AND json_extract(u.value,'$.record_version')=?"
        assert json.loads(store.db.execute(sql, ("record", 12)).fetchone()[0]) == item
        assert (
            store.db.execute(
                "SELECT id FROM turns WHERE json_extract(body,'$.version')>2 AND json_extract(body,'$.stale')=0"
            ).fetchone()[0]
            == "turn"
        )
        assert (
            store.db.execute(
                "SELECT id FROM turns WHERE json_extract(body,'$.scope')=?",
                (canonical(item["scope"]),),
            ).fetchone()[0]
            == "turn"
        )
        store.db.execute("BEGIN")
        try:
            with pytest.raises(sqlite3.Error):
                store.db.execute("DELETE FROM turns")
        finally:
            store.db.execute("ROLLBACK")
        assert store.get("turns", "turn") == item
        assert (
            store.db.execute(
                "SELECT id FROM turns WHERE id LIKE 'tu%' AND id=?", ("turn",)
            ).fetchone()[0]
            == "turn"
        )
    finally:
        store.close()


def test_real_postgres_lost_owner_session_closes_and_can_reopen(tmp_path, pg_settings):
    path = tmp_path / "lost.sqlite"
    store = Store(path, settings=pg_settings)
    assert store.backend == "postgresql"
    store.put("metadata", dict(id="committed", value="kept"))
    store.db.raw.close()
    with pytest.raises(sqlite3.Error):
        store.put("metadata", dict(id="must-not-fallback", value=True))
    store.close()
    recovered = Store(path, settings=pg_settings)
    try:
        assert recovered.get("metadata", "committed")["value"] == "kept"
        assert recovered.get("metadata", "must-not-fallback") is None
    finally:
        recovered.close()


@pytest.mark.parametrize("component", ["companion", "journal"])
def test_real_postgres_failed_activation_releases_owner(
    tmp_path, pg_settings, monkeypatch, component
):
    from tianshu_storage import StorageRuntime
    from tianshu_companion.app import build_runtime
    from tianshu_nonebot.journal import Journal

    path = tmp_path / "failed-activation.sqlite"

    def fail(runtime):
        assert runtime.backend == "postgresql"
        raise RuntimeError("synthetic activation failure")

    with monkeypatch.context() as patch:
        patch.setattr(StorageRuntime, "finalize", fail)
        with pytest.raises(RuntimeError, match="synthetic activation failure"):
            if component == "companion":
                build_runtime(
                    dict(
                        pg_settings,
                        database_path=str(path),
                        contracts_path=os.environ["TIANSHU_CONTRACTS"],
                        config_version=1,
                    )
                )
            else:
                Journal(path, storage_backends=pg_settings["storage_backends"])
    if component == "companion":
        recovered = Store(path, settings=pg_settings)
    else:
        recovered = Journal(path, storage_backends=pg_settings["storage_backends"])
    recovered.close()


def test_real_postgres_read_only_maintenance_uses_bound_backend(tmp_path, pg_settings):
    from tianshu_nonebot.rpc import AdapterService
    from tianshu_nonebot.storage import read_access_key

    async def accounts():
        return []

    async def send(*args):
        raise AssertionError("Read-only maintenance cannot send")

    path = tmp_path / "rpc.sqlite"
    service = AdapterService(
        path, "nonebot", accounts, send, storage_backends=pg_settings["storage_backends"]
    )
    try:
        state_path = service.storage_runtime.state_path
        before = state_path.read_bytes()
        assert read_access_key(path, pg_settings) == service.access_key
        assert state_path.read_bytes() == before
    finally:
        service.close()
    main = tmp_path / "main.sqlite"
    store = Store(main, settings=pg_settings)
    try:
        store.put("turns", dict(id="current-pg", phase="queued", sequence=1))
        head = store.source_head()
        config = tmp_path / "synthetic-storage.json"
        config.write_text(json.dumps(pg_settings), encoding="utf-8")
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                str(Path(__file__).parents[1] / "scripts/inspect_state.py"),
                str(main),
                "--storage-config",
                str(config),
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=True,
        )
        assert json.loads(result.stdout)["turns"][0]["id"] == "current-pg"
        assert store.source_head() == head
    finally:
        store.close()
