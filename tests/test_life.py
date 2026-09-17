"""Synthetic isolated life boundaries; model doubles exist only in this test module."""

import asyncio
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from unittest.mock import patch

import httpx
import pytest

from support import Clock, Harness, contracts
from tianshu_companion.clients import Gateway, JsonService, utc
from tianshu_companion.life import DEFAULT_RECIPE, Life, zone
from tianshu_companion.store import LIFE_TABLES, Store


class Model:
    available = True

    def __init__(self):
        self.calls = []
        self.output = "I enjoyed the quiet fictional garden today."
        self.fail = False

    async def generate(self, turn, messages):
        self.calls.append((turn, messages))
        if self.fail:
            raise RuntimeError("synthetic failure")
        return [self.output], {"fixture": True, "config_version": turn["config_version"]}


def setup(store=None, *, writing=True, version=19, tz="UTC"):
    store = store or Store(":memory:")
    clock = Clock()
    clock.now = datetime(2026, 9, 14, 12, tzinfo=timezone.utc).timestamp()
    model = Model()
    life = Life(store, clock, model, version, asyncio.Semaphore(4), writing=writing)
    life.create_world("world", timezone_name=tz)
    life.create_room("room", "world")
    for actor in ("a", "b"):
        life.configure_actor(
            actor,
            "room",
            personality_version=1,
            schedule=[
                dict(minute=0, activity="sleeping", controls={"desk_light": 0, "window": 0}),
                dict(minute=420, activity="reading", controls={"desk_light": 1, "window": 1}),
                dict(minute=1320, activity="sleeping", controls={"desk_light": 0, "window": 0}),
            ],
        )
    return life, clock, model


@pytest.fixture
def env():
    life, clock, model = setup()
    yield life, clock, model
    life.store.close()


def test_shared_room_and_independent_actor_state_and_event_knowledge(env):
    life, clock, _ = env
    a = life.snapshot("a")
    b = life.snapshot("b")
    assert a["room"] == b["room"]
    life.set_activity("a", "painting", expected=a["actor"]["version"])
    assert life.snapshot("a")["actor"]["activity"] == "painting"
    assert life.snapshot("b")["actor"]["activity"] == "reading"
    life.record_event("secret", "world", "A found a fictional blue shell", participants=["a"])
    assert "secret" not in [m["event_id"] for m in life.materials("b", "2026-09-14")]
    with pytest.raises(PermissionError):
        life.tell("secret", "b", "a")
    life.tell("secret", "a", "b")
    life.tell("secret", "a", "b")
    known = [m for m in life.materials("b", "2026-09-14") if m["event_id"] == "secret"]
    assert len(known) == 1 and known[0]["via"] == "told_by" and known[0]["source_actor"] == "a"
    with pytest.raises(ValueError):
        life.record_event("secret", "world", "changed", participants=["a"])
    clock.advance(2)
    assert (
        life.record_event("secret", "world", "A found a fictional blue shell", participants=["a"])[
            "occurred_at"
        ]
        < clock()
    )


def test_manual_room_hold_indefinite_expiring_and_resume(env):
    life, clock, _ = env
    room = life.snapshot("a")["room"]
    held = life.set_room("room", {"desk_light": 0, "window": 0}, expected=room["version"])
    life.tick(force=True)
    assert life.snapshot("b")["room"] == held
    with pytest.raises(ValueError):
        life.set_room("room", {"window": 1}, expected=room["version"])
    for invalid_version in (None, True, 0):
        with pytest.raises(ValueError):
            life.set_room("room", {"window": 1}, expected=invalid_version)
    room = life.resume_room("room", ["desk_light"], expected=held["version"])
    room = life.snapshot("a")["room"]
    assert room["controls"]["desk_light"]["value"] == 1
    assert room["controls"]["window"]["value"] == 0
    life.set_room("room", {"window": 0.2}, expected=room["version"], hold_until=clock() + 10)
    clock.advance(11)
    resumed = life.snapshot("a")["room"]["controls"]["window"]
    assert resumed["value"] == 1 and resumed["mode"] == "auto"
    assert resumed["changed_at"] == clock() and resumed["from_value"] == 0.2


def test_manual_actor_expiry_and_explicit_resume(env):
    life, clock, _ = env
    a = life.snapshot("a")["actor"]
    life.set_activity("a", "drawing", expected=a["version"], hold_until=clock() + 5)
    assert life.snapshot("a")["actor"]["activity"] == "drawing"
    clock.advance(5)
    assert life.snapshot("a")["actor"]["activity"] == "reading"
    a = life.snapshot("a")["actor"]
    a = life.set_activity("a", "drawing", expected=a["version"])
    life.resume_actor("a", expected=a["version"])
    assert life.snapshot("a")["actor"]["activity"] == "reading"


def test_restart_bounded_catchup_and_preserves_hold(tmp_path):
    path = tmp_path / "isolated.db"
    life, clock, model = setup(Store(path))
    initial = life.snapshot("a")
    life.set_room("room", {"window": 0.3}, expected=initial["room"]["version"])
    head = life.store.source_head()
    old_events = len(life.store.list("life_events"))
    life.store.close()
    clock.advance(30 * 86400 + 12 * 3600)
    store = Store(path)
    try:
        restarted = Life(store, clock, model, 19, asyncio.Semaphore(4), writing=True)
        restarted.recover()
        current = restarted.snapshot("a")
        assert current["actor"]["activity"] == "sleeping"
        assert current["actor"]["changed_at"] == clock()
        assert current["room"]["controls"]["window"]["value"] == 0.3
        assert len(store.list("life_events")) == old_events + 2
        restarted.tick(force=True)
        assert len(store.list("life_events")) == old_events + 2
        assert store.source_head() == head  # fictional life never enters source-facts stream
        assert not list(tmp_path.glob("*.bak"))  # v3 restart isn't a migration
    finally:
        store.close()


def test_timezone_midnight_learning_date_and_world_version(env):
    life, clock, _ = env
    world = life._get("worlds", "world")
    life.update_world("world", setting="Fiction", timezone_name="+08:00", expected=world["version"])
    clock.now = datetime(2026, 9, 14, 15, 59, tzinfo=timezone.utc).timestamp()
    life.record_event("before", "world", "A fictional star", participants=["a"])
    clock.advance(120)
    life.tell("before", "a", "b")
    assert life.materials("a", "2026-09-14")[0]["event_id"] == "before"
    assert life.materials("a", "2026-09-15") == []
    assert life.materials("b", "2026-09-15")[0]["via"] == "told_by"
    assert life.snapshot("b")["actor"]["activity"] == "sleeping"
    assert zone("+14:00").utcoffset(None).total_seconds() == 14 * 3600
    with pytest.raises(Exception):
        zone("+19:00")


@pytest.mark.parametrize(
    "values",
    [
        {"real_device": 1},
        {"window": -1},
        {"window": float("nan")},
        {"window": True},
        {"window": 1.1},
    ],
)
def test_control_validation(env, values):
    life, _, _ = env
    with pytest.raises(ValueError):
        life.set_room("room", values, expected=1)


def test_empty_and_overflow_material_skip_without_model(env):
    life, _, model = env
    assert life.request_diary("a", "2026-09-13")["state"] == "skipped"
    for i in range(65):
        life.record_event(f"e:{i}", "world", "fiction", participants=["a"])
    assert life.request_diary("a", "2026-09-14")["state"] == "material_overflow"
    assert model.calls == []


@pytest.mark.parametrize(
    "writing,version,available", [(False, 19, True), (True, None, True), (True, 19, False)]
)
def test_writing_requires_independent_explicit_configuration(writing, version, available):
    life, _, model = setup(writing=writing, version=version)
    try:
        model.available = available
        life.record_event("e", "world", "fiction", participants=["a"])
        assert life.request_diary("a", "2026-09-14")["state"] == "unavailable"
        asyncio.run(life.work())
        assert model.calls == []
    finally:
        life.store.close()


def test_draft_revision_publish_lock_and_admin_are_separate(env):
    life, _, model = env
    life.record_event("private", "world", "fictional shell", participants=["a"])
    meta = life.request_diary("a", "2026-09-14")
    assert "material" not in meta and "content" not in meta
    asyncio.run(life.work())
    meta = life.diary_metadata(meta["id"])
    assert meta["state"] == "draft"
    assert model.calls[0][0]["config_version"] == 19
    generated = life.admin_read_revision(meta["current_revision"])
    assert generated["source"]["kind"] == "gateway"
    # Guard actual content and materials table reads, not merely response shape.
    original_get = life.store.get

    def guard(table, key):
        assert table not in {"life_revisions", "life_materials"}
        return original_get(table, key)

    with patch.object(life.store, "get", guard), pytest.raises(PermissionError):
        life.read_diary(meta["id"], reader="reader")
    access = life.set_diary_access("a", readers=["reader"])
    with pytest.raises(PermissionError):
        life.read_diary(meta["id"], reader="reader")  # story grant never exposes draft
    revision = life.revise_diary(
        meta["id"],
        "A quiet fictional afternoon.",
        editor="admin",
        reason="Remove repetition",
        expected=meta["version"],
    )
    updated = life.diary_metadata(meta["id"])
    assert life.admin_read_revision(revision)["parent"] == generated["id"]
    with pytest.raises(ValueError):
        life.publish_diary(meta["id"], reviewer="admin", expected=meta["version"])
    published = life.publish_diary(meta["id"], reviewer="admin", expected=updated["version"])
    assert life.read_diary(meta["id"], reader="reader")["id"] == published
    assert "source" not in life.read_diary(meta["id"], reader="reader")
    latest = life.diary_metadata(meta["id"])
    assert life.publish_diary(meta["id"], reviewer="admin", expected=latest["version"]) == published
    assert life.request_diary("a", "2026-09-14")["id"] == meta["id"]
    life.set_diary_access("a", readers=[], expected=access["version"])
    with pytest.raises(PermissionError):
        life.read_diary(meta["id"], reader="reader")


def test_recipe_immutable_and_empty_model_is_failed(env):
    life, _, model = env
    recipe = dict(DEFAULT_RECIPE, max_chars=20)
    life.put_recipe(recipe)
    with pytest.raises(ValueError):
        life.put_recipe(dict(recipe, style="Changed without new version"))
    life.put_recipe(dict(recipe, version=2))
    life.record_event("e", "world", "fictional moment", participants=["a"])
    meta = life.request_diary("a", "2026-09-14")
    model.output = ""
    asyncio.run(life.work())
    failed = life.diary_metadata(meta["id"])
    assert failed["state"] == "failed" and failed["current_revision"] is None
    model.output = "Quiet fiction."
    life.retry_diary(meta["id"], expected=failed["version"])
    asyncio.run(life.work())
    assert life.diary_metadata(meta["id"])["state"] == "draft"


def test_interrupted_recovery_requires_explicit_retry(env):
    life, _, _ = env
    life.record_event("e", "world", "fiction", participants=["a"])
    meta = life.request_diary("a", "2026-09-14")
    item = life._get("diaries", meta["id"])
    item["state"] = "generating"
    life._save("diaries", item)
    life.recover()
    assert life.diary_metadata(meta["id"])["state"] == "interrupted"


def test_migration_backup_complete_and_life_ddl_rollback(tmp_path):
    path = tmp_path / "v2.db"
    store = Store(path)
    head = store.source_head()
    store.put("conversations", {"id": "synthetic", "private": "synthetic preserved"})
    store.close()
    with closing(sqlite3.connect(path)) as db, db:
        for table in LIFE_TABLES:
            db.execute(f"DROP TABLE {table}")
        db.execute("PRAGMA user_version=2")
        # A conflicting index name makes migration fail midway, after prior DDL.
        db.execute("CREATE TABLE life_diaries_queue (id TEXT)")
    with pytest.raises(sqlite3.OperationalError):
        Store(path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 2
        assert (
            db.execute("SELECT count(*) FROM sqlite_master WHERE name='life_actors'").fetchone()[0]
            == 0
        )
        db.execute("DROP TABLE life_diaries_queue")
        db.commit()
    store = Store(path)  # owner lock released after migration failure
    try:
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 8
        assert store.source_head() == head
        assert store.get("conversations", "synthetic")["private"] == "synthetic preserved"
        backups = list(tmp_path.glob("*.pre-life-v3-*.bak"))
        assert len(backups) == 2
        # A single structural step from v2 crosses straight to the current version, so no
        # intermediate v7 -> v8 backup is taken here.
        assert not list(tmp_path.glob("*.pre-persona-v8-*.bak"))
        for backup in backups:
            with closing(sqlite3.connect(backup)) as db:
                assert db.execute("PRAGMA user_version").fetchone()[0] == 2
                assert (
                    "synthetic preserved"
                    in db.execute("SELECT body FROM conversations").fetchone()[0]
                )
    finally:
        store.close()


def test_core_context_summary_in_shared_budget_and_no_private_history():
    async def scenario():
        h = Harness()
        life = h.core.life
        life.create_world("w")
        life.create_room("r", "w")
        life.configure_actor(
            "actor:a", "r", personality_version=1, schedule=[dict(minute=0, activity="reading")]
        )
        try:
            receipt = await h.core.ingest("nonebot", h.request())
            h.clock.advance(6)
            await h.core.tick()
            await h.cycles()
            turns = h.core.store.list("turns")
            assert turns and turns[0]["context_budget_used"]["bytes"] <= 16384
            prompt = json.loads(h.gateway.calls[0][1][-1]["content"])
            assert prompt["fictional_life"][0]["activity"] == "reading"
            assert prompt["fictional_life"][0]["fictional"] is True
            assert receipt
        finally:
            await h.core.close()

    asyncio.run(scenario())


def test_real_gateway_adapter_uses_separate_version_and_validated_receipt(env):
    life, _, _ = env
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path == "/v1/chat/completions":
            assert request.headers["X-Tianshu-Config-Version"] == "19"
            assert request.headers["X-Tianshu-Workload"] == "companion.text"
            assert "model" not in json.loads(request.content)
            return httpx.Response(
                200,
                json=dict(
                    id="fixture:completion",
                    object="chat.completion",
                    choices=[
                        dict(
                            message=dict(role="assistant", content="Fictional quiet day."),
                            finish_reason="stop",
                        )
                    ],
                    usage=None,
                ),
            )
        return httpx.Response(
            200,
            json=dict(
                schema_version=1,
                request_id=request.url.path.rsplit("/", 1)[-1],
                config_version=19,
                provider_id="provider:synthetic",
                requested_model=None,
                resolved_model="diary-model",
                protocol="openai-chat-completions",
                outcome="succeeded",
                upstream_request_id=None,
                usage=None,
                fallback_used=False,
                observed_at=utc(life.clock()),
                credential_namespace="fixture:namespace",
                caller_service="companion",
                requested_reasoning={},
                effective_reasoning={},
                applied_policies=[dict(field="model", mode="workload_binding", config_version=19)],
                usage_complete=False,
                native_usage=None,
            ),
        )

    async def scenario():
        client = JsonService(
            "https://synthetic.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        try:
            life.gateway = Gateway(contracts(), client)
            life.record_event("e", "world", "fictional garden", participants=["a"])
            meta = life.request_diary("a", "2026-09-14")
            await life.work()
            final = life.diary_metadata(meta["id"])
            assert final["state"] == "draft"
            assert (
                life.admin_read_revision(final["current_revision"])["source"]["receipt"][
                    "resolved_model"
                ]
                == "diary-model"
            )
            assert len(requests) == 2
        finally:
            await client.close()

    asyncio.run(scenario())


def test_writing_config_is_pinned_until_explicit_retry(env):
    life, _, model = env
    life.record_event("event", "world", "Fictional sun", participants=["a"])
    meta = life.request_diary("a", "2026-09-14")
    life.config_version = 20
    asyncio.run(life.work())
    assert model.calls[0][0]["config_version"] == 19
    assert life.diary_metadata(meta["id"])["state"] == "draft"


def test_invalid_life_config_releases_database_owner(tmp_path):
    h = Harness(tmp_path / "fixture.db")
    asyncio.run(h.core.close())
    h.options["life_writing"] = "yes"
    closed = []
    original_close = Store.close

    def close(store):
        closed.append(store)
        original_close(store)

    with patch.object(Store, "close", close), pytest.raises(ValueError):
        h.new_core()
    assert len(closed) == 1
    with closing(Store(tmp_path / "fixture.db")) as store:
        assert store.source_head()


@pytest.mark.parametrize("hold_seconds", [None, 10])
@pytest.mark.parametrize("move_room", [False, True])
def test_actor_reconfiguration_preserves_manual_activity_until_release(
    env, hold_seconds, move_room
):
    life, clock, _ = env
    actor = life.snapshot("a")["actor"]
    until = clock() + hold_seconds if hold_seconds is not None else None
    held = life.set_activity("a", "painting", expected=actor["version"], hold_until=until)
    clock.advance(2)
    room = "room"
    if move_room:
        life.create_world("other-world", timezone_name="+08:00")
        room = "other-room"
        life.create_room(room, "other-world")
    updated = life.configure_actor(
        "a",
        room,
        personality_version=2,
        mood="happy",
        schedule=[dict(minute=0, activity="writing")],
        expected=held["version"],
    )
    current = life.snapshot("a")["actor"]
    assert current["personality_version"] == 2 and current["mood"] == "happy"
    assert current["schedule"] == updated["schedule"] and current["room_id"] == room
    assert current["manual"] == held["manual"]
    assert current["activity"] == "painting" and current["changed_at"] == held["changed_at"]
    if hold_seconds is None:
        clock.advance(86400)
        current = life.snapshot("a")["actor"]
        assert current["activity"] == "painting"
        life.resume_actor("a", expected=current["version"])
    else:
        clock.now = until
    resumed = life.snapshot("a")["actor"]
    assert resumed["activity"] == "writing" and resumed["manual"] is None


def test_retry_diary_requires_positive_current_version_without_mutation(env):
    life, _, _ = env
    life.writing = False
    meta = life.request_diary("a", "2026-09-14")
    assert meta["state"] == "unavailable" and meta["version"] == 1
    for invalid in (None, True, 0):
        with pytest.raises(ValueError):
            life.retry_diary(meta["id"], expected=invalid)
        assert life.diary_metadata(meta["id"]) == meta
    newer = life.retry_diary(meta["id"], expected=meta["version"])
    assert newer["version"] == 2
    with pytest.raises(ValueError):
        life.retry_diary(meta["id"], expected=meta["version"])
    assert life.diary_metadata(meta["id"]) == newer
    life.writing = True
    queued = life.retry_diary(meta["id"], expected=newer["version"])
    assert queued["state"] == "queued"
    with pytest.raises(ValueError):
        life.retry_diary(meta["id"], expected=newer["version"])
    assert life.diary_metadata(meta["id"]) == queued
