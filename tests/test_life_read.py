"""Authorized life-read port: identity, projection, capture proofs, budgets, zero writes.

Every fact here is a synthetic fictional life row in an isolated database. The model double
is local to this module and never leaves it; the real HTTPS process is covered separately in
`test_life_read_https.py`. The fixture helpers below are imported **by name** from that module
and the two query/index modules, so pytest never collects this file's cases twice.
"""

import asyncio
import contextlib
import json
from datetime import datetime, timezone

import httpx
import pytest

from support import Clock, Harness, contracts
from tianshu_companion.app import create_app
from tianshu_companion.clients import Gateway, JsonService, utc
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.life import DEFAULT_RECIPE, Life
from tianshu_companion.life_read import LifeRead, readers
from tianshu_companion.life_read_queries import LifeReadQueries
from tianshu_companion.store import TABLES, Store

SERVICE = "story_reader"
OTHER_SERVICE = "other_reader"
READER = "reader:story"
OTHER_READER = "reader:other"
DEPLOYED = {"reader_id": READER, "actor_ids": ["actor:a", "actor:b"]}
FIXED_NOW = datetime(2026, 9, 21, 12, tzinfo=timezone.utc).timestamp()
TOKENS = {SERVICE: "fixture-story-token", OTHER_SERVICE: "fixture-other-token"}
LIST_CEILING = 262144
REVISION_CEILING = 1048576


def recipe(ident="daily", version=1):
    return dict(DEFAULT_RECIPE, id=ident, version=version)


def receipt(config_version, observed_at, request_id="model:fixture"):
    """A contract-valid gateway route receipt: the shape `Gateway.generate` verifies."""
    return dict(
        schema_version=1,
        request_id=request_id,
        config_version=config_version,
        provider_id="provider:synthetic",
        requested_model=None,
        resolved_model="diary-model",
        protocol="openai-chat-completions",
        outcome="succeeded",
        upstream_request_id=None,
        usage=None,
        fallback_used=False,
        observed_at=utc(observed_at),
        credential_namespace="fixture:namespace",
        caller_service="companion",
        requested_reasoning={},
        effective_reasoning={},
        applied_policies=[
            dict(field="model", mode="workload_binding", config_version=config_version)
        ],
        usage_complete=False,
        native_usage=None,
    )


class Fixture:
    """Synthetic life facts written in the exact row shapes the life engine produces.

    Only the storage shapes are reproduced here - configuration-addressed diary ids, one
    immutable revision per draft with a parent link and a frozen material hash - so the edge
    cases the engine cannot be asked to create (a cycle, a 65-link chain, a foreign revision)
    exist without weakening the checks they drive.
    `test_the_port_reads_rows_the_life_engine_wrote` reads genuinely engine-written rows
    through the same port.
    """

    def __init__(self, store, clock, *, config_version=19):
        self.store, self.clock, self.config_version = store, clock, config_version
        self.diaries = {}

    def _save(self, table, item):
        old = self.store.get(table, item["id"])
        item["version"] = (old or {}).get("version", 0) + 1
        with self.store.transaction():
            self.store.put(table, item)
        return item

    def world(self, world_id="world:home", *, timezone_name="UTC"):
        return self._save(
            "life_worlds",
            dict(
                id=world_id,
                timezone=timezone_name,
                setting="A fictional home",
                setting_version=1,
                fictional=True,
                time_basis="UTC epoch seconds; configured civil timezone",
                created_at=self.clock(),
            ),
        )

    def room(self, room_id="room:study", *, world_id="world:home"):
        return self._save(
            "life_rooms",
            dict(
                id=room_id,
                world_id=world_id,
                fictional=True,
                controls={
                    name: dict(value=0.0, mode="auto", hold_until=None, changed_at=self.clock())
                    for name in ("desk_light", "window")
                },
            ),
        )

    def actor(
        self,
        actor_id,
        *,
        room_id="room:study",
        world_id="world:home",
        activity=None,
        outfit_ref=None,
        mood="calm",
    ):
        return self._save(
            "life_actors",
            dict(
                id=actor_id,
                room_id=room_id,
                world_id=world_id,
                personality_version=1,
                mood=mood,
                outfit_ref=outfit_ref,
                schedule=[dict(minute=0, activity="sleeping", controls={})],
                schedule_version=1,
                activity=activity,
                changed_at=self.clock(),
                cursor=None,
                manual=None,
                recipe="daily",
                fictional=True,
            ),
        )

    def grant(self, actor_id, readers=(READER,)):
        return self._save(
            "life_access", dict(id=actor_id, conversation_id=actor_id, readers=sorted(readers))
        )

    def generate(
        self,
        actor_id,
        day,
        content,
        *,
        config_version=None,
        recipe_row=None,
        material=None,
        created_at=None,
    ):
        """A diary row plus its first revision, addressed and marked exactly as `Life` does."""
        recipe_row = dict(recipe_row or recipe())
        material = material or digest(["material", actor_id, day])
        diary = self._save(
            "life_diaries",
            dict(
                id=digest([actor_id, day, recipe_row, material]),
                conversation_id=actor_id,
                day=day,
                fictional=True,
                recipe=recipe_row,
                material_version=material,
                state="queued",
                config_version=self.config_version if config_version is None else config_version,
                current_revision=None,
                published_revision=None,
                created_at=self.clock(),
                real_chat_sources="excluded: no current source/access proof",
            ),
        )
        self.diaries[diary["id"]] = diary
        self.revise(
            diary["id"],
            content,
            source=dict(
                kind="gateway",
                receipt=receipt(diary["config_version"], created_at or self.clock()),
            ),
            created_at=created_at,
        )
        return diary

    def revise(self, diary_id, content, *, source, parent=None, material=None, created_at=None):
        diary = self.diaries[diary_id]
        revision_id = digest([diary_id, diary["version"], content, source])
        with self.store.transaction():
            self.store.put(
                "life_revisions",
                dict(
                    id=revision_id,
                    conversation_id=diary_id,
                    content=content,
                    source=source,
                    parent=diary["current_revision"] if parent is None else parent,
                    material_version=material or diary["material_version"],
                    fictional=True,
                    created_at=self.clock() if created_at is None else created_at,
                ),
            )
        diary["current_revision"] = revision_id
        diary["state"] = "draft"
        self._save("life_diaries", diary)
        return revision_id

    def publish(self, diary_id, *, revision_id=None):
        diary = self.diaries[diary_id]
        diary["published_revision"] = (
            diary["current_revision"] if revision_id is None else revision_id
        )
        diary["state"] = "published"
        return self._save("life_diaries", diary)


def build(path=":memory:", **options):
    store = Store(path)
    clock = Clock()
    clock.now = FIXED_NOW
    return Fixture(store, clock, **options)


def equipped(fixture=None, *, deployed=DEPLOYED, service=SERVICE):
    """One port over the fixture store with the given deployment entry."""
    fixture = fixture or build()
    port = LifeRead(
        LifeReadQueries(fixture.store.db),
        readers={service: deployed},
        contracts=contracts(),
        clock=fixture.clock,
    )
    return fixture, port


def refuses(port, operation, body, *, service=SERVICE):
    with pytest.raises(Fault) as raised:
        port.handle(service, operation, body)
    return raised.value.code


def facts(store):
    """Every durable row plus the schema version: the before/after proof of a read."""
    snapshot = {
        table: [tuple(row) for row in store.db.execute(f"SELECT id,body FROM {table} ORDER BY id")]
        for table in sorted(TABLES)
    }
    snapshot["user_version"] = store.db.execute("PRAGMA user_version").fetchone()[0]
    return snapshot


def live(actor="actor:a", **options):
    """A world, a room, one granted actor and a port over them."""
    fixture, port = equipped(**options)
    fixture.world()
    fixture.room()
    fixture.actor(actor)
    fixture.grant(actor)
    return fixture, port


def revision_body(diary, actor_id="actor:a", **overrides):
    body = {
        "schema_version": 1,
        "actor_id": actor_id,
        "diary_id": diary["id"],
        "revision_id": diary["published_revision"],
        "expected_diary_version": diary["version"],
    }
    return {**body, **overrides}


def published(fixture, actor="actor:a", *, day="2026-09-20", content="Published text.", **options):
    diary = fixture.generate(actor, day, content, **options)
    fixture.publish(diary["id"])
    return diary


# ------------------------------------------------------------------ deployment mapping


def test_reader_mapping_is_strict_and_names_only_known_callers():
    assert readers(None) is None
    assert readers({}) == {}
    entry = {"reader_id": READER, "actor_ids": ["actor:a"]}
    assert readers({SERVICE: entry}, {SERVICE}) == {
        SERVICE: {"reader_id": READER, "actor_ids": ("actor:a",)}
    }
    bad = (
        ({SERVICE: entry}, {"platform"}),
        ({"nobody": entry}, {"platform"}),
        ({SERVICE: dict(entry, extra=1)}, {SERVICE}),
        ({SERVICE: {"reader_id": READER}}, {SERVICE}),
        ({SERVICE: dict(entry, reader_id="")}, {SERVICE}),
        ({SERVICE: dict(entry, reader_id=None)}, {SERVICE}),
        ({SERVICE: dict(entry, actor_ids=[])}, {SERVICE}),
        ({SERVICE: dict(entry, actor_ids=["actor:a"] * 2)}, {SERVICE}),
        ({SERVICE: dict(entry, actor_ids=["actor:%d" % i for i in range(65)])}, {SERVICE}),
        ({SERVICE: dict(entry, actor_ids="actor:a")}, {SERVICE}),
        ({SERVICE: dict(entry, actor_ids=[None])}, {SERVICE}),
        ({SERVICE: entry, OTHER_SERVICE: entry}, {SERVICE, OTHER_SERVICE}),
        ([entry], {SERVICE}),
    )
    for document, services in bad:
        with pytest.raises(ValueError):
            readers(document, services)


def test_the_reader_identity_never_comes_from_the_request_body():
    fixture, port = equipped()
    fixture.world()
    fixture.room()
    fixture.actor("actor:a")
    fixture.grant("actor:a", readers=[OTHER_READER])
    assert refuses(port, "snapshot", {"schema_version": 1, "actor_id": "actor:a"}) == "not_found"
    # The caller cannot name its own reader, and cannot widen its grant through the listing.
    assert (
        refuses(port, "snapshot", {"schema_version": 1, "actor_id": "actor:a", "reader_id": READER})
        == "invalid_input"
    )
    assert port.handle(SERVICE, "actors", {"schema_version": 1})["items"] == []


def test_unregistered_caller_missing_actor_and_revoked_grant_are_indistinguishable():
    fixture, port = equipped()
    fixture.world()
    fixture.room()
    fixture.actor("actor:a")
    assert refuses(port, "snapshot", {"schema_version": 1, "actor_id": "actor:a"}) == "not_found"
    assert refuses(port, "snapshot", {"schema_version": 1, "actor_id": "actor:zz"}) == "not_found"
    assert refuses(port, "actors", {"schema_version": 1}, service="nobody") == "forbidden"
    fixture.grant("actor:a")
    assert (
        port.handle(SERVICE, "snapshot", {"schema_version": 1, "actor_id": "actor:a"})["actor_id"]
        == "actor:a"
    )
    # The grant is read per request, so revocation is already in force on the next call.
    fixture.grant("actor:a", readers=[OTHER_READER])
    assert refuses(port, "snapshot", {"schema_version": 1, "actor_id": "actor:a"}) == "not_found"


def test_http_identity_missing_section_and_request_ceiling():
    h = Harness()
    app = create_app(h.core, TOKENS, {SERVICE: DEPLOYED})
    off = create_app(h.core, TOKENS)
    unauthorized = {"schema_version": 1}

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://life"
        ) as client:
            plain = await client.post("/internal/v1/life-read/actors", json=unauthorized)
            assert plain.status_code == 401
            wrong = await client.post(
                "/internal/v1/life-read/actors",
                json=unauthorized,
                headers={"Authorization": "Bearer wrong"},
            )
            assert wrong.status_code == 401
            other = await client.post(
                "/internal/v1/life-read/actors",
                json=unauthorized,
                headers={"Authorization": "Bearer " + TOKENS[OTHER_SERVICE]},
            )
            assert other.status_code == 403 and other.json()["code"] == "forbidden"
            over = await client.post(
                "/internal/v1/life-read/diaries",
                content=b'{"schema_version":1,"actor_id":"' + b"a" * 16400 + b'"}',
                headers={
                    "Authorization": "Bearer " + TOKENS[SERVICE],
                    "Content-Type": "application/json",
                },
            )
            assert over.status_code == 400 and over.json()["code"] == "invalid_input"
            inside = await client.post(
                "/internal/v1/life-read/actors",
                content=b'{"schema_version":1}',
                headers={
                    "Authorization": "Bearer " + TOKENS[SERVICE],
                    "Content-Type": "application/json",
                },
            )
            assert inside.status_code == 200
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=off), base_url="http://life"
        ) as client:
            missing = await client.post(
                "/internal/v1/life-read/diaries",
                json={"schema_version": 1, "actor_id": "actor:a"},
                headers={"Authorization": "Bearer " + TOKENS[SERVICE]},
            )
            assert missing.status_code == 503
            assert missing.json()["code"] == "dependency_unavailable"

    asyncio.run(run())
    asyncio.run(h.core.close())


# ------------------------------------------------------------------------------ actors


def test_actors_lists_only_double_authorized_actors_in_unicode_order():
    fixture, port = equipped(
        deployed={"reader_id": READER, "actor_ids": ["actor:a", "actor:b", "actor:Z"]}
    )
    fixture.world()
    fixture.room()
    for actor_id in ("actor:b", "actor:a", "actor:Z", "actor:c"):
        fixture.actor(actor_id)
    fixture.grant("actor:a")
    fixture.grant("actor:Z")
    fixture.grant("actor:c")  # granted but outside the deployment: never enumerated
    page = port.handle(SERVICE, "actors", {"schema_version": 1})
    assert page["fictional"] is True and page["next_after_actor_id"] is None
    assert [item["actor_id"] for item in page["items"]] == ["actor:Z", "actor:a"]
    assert set(page["items"][0]) == {"actor_id", "actor_version", "world_id", "room_id"}
    assert page["items"][0]["actor_version"] == 1
    assert page["items"][0]["world_id"] == "world:home"
    assert page["items"][0]["room_id"] == "room:study"


def test_actors_paging_walks_one_page_at_a_time_without_gaps_or_repeats():
    letters = "abcdefghij"
    fixture, port = equipped(
        deployed={"reader_id": READER, "actor_ids": ["actor:" + x for x in letters]}
    )
    fixture.world()
    fixture.room()
    for letter in letters:
        fixture.actor("actor:" + letter)
        fixture.grant("actor:" + letter)
    seen, after = [], None
    while True:
        body = {"schema_version": 1, "limit": 3}
        if after is not None:
            body["after_actor_id"] = after
        page = port.handle(SERVICE, "actors", body)
        seen.extend(item["actor_id"] for item in page["items"])
        after = page["next_after_actor_id"]
        if after is None:
            break
        assert len(page["items"]) == 3
    assert seen == ["actor:" + letter for letter in letters]
    for bad in (
        {"after_actor_id": "x" * 129},
        {"after_actor_id": None},
        {"limit": 0},
        {"limit": 51},
        {"limit": True},
        {"limit": "3"},
    ):
        assert refuses(port, "actors", {"schema_version": 1, **bad}) == "invalid_input"


# ---------------------------------------------------------------------------- snapshot


def test_snapshot_projects_persisted_state_and_keeps_absent_fields_null():
    fixture, port = equipped()
    fixture.world()
    fixture.room()
    fixture.actor("actor:a", outfit_ref="outfit:reading-v1")
    fixture.grant("actor:a")
    state = port.handle(SERVICE, "snapshot", {"schema_version": 1, "actor_id": "actor:a"})
    assert state == dict(
        schema_version=1,
        fictional=True,
        actor_id="actor:a",
        actor_version=1,
        world_id="world:home",
        world_version=1,
        room_id="room:study",
        room_version=1,
        timezone="UTC",
        activity=None,
        mood="calm",
        outfit_ref="outfit:reading-v1",
        changed_at=FIXED_NOW,
        observed_at=FIXED_NOW,
        state_basis="last_persisted",
    )


@pytest.mark.parametrize("damage", ["room", "world", "crossed", "version", "mood", "timezone"])
def test_snapshot_refuses_corrupt_relations_and_types(damage):
    fixture, port = live()
    actor = fixture.store.get("life_actors", "actor:a")
    if damage == "room":
        fixture.store.delete("life_rooms", "room:study")
    elif damage == "world":
        fixture.store.delete("life_worlds", "world:home")
    elif damage == "crossed":
        fixture.room("room:other", world_id="world:elsewhere")
        actor["room_id"] = "room:other"
        fixture.store.put("life_actors", actor)
    elif damage == "version":
        actor["version"] = "1"
        fixture.store.put("life_actors", actor)
    elif damage == "mood":
        actor["mood"] = None
        fixture.store.put("life_actors", actor)
    else:
        world = fixture.world()
        world["timezone"] = 7
        fixture.store.put("life_worlds", world)
    assert (
        refuses(port, "snapshot", {"schema_version": 1, "actor_id": "actor:a"})
        == "dependency_unavailable"
    )


def test_snapshot_reports_the_stored_row_and_never_ticks_the_world():
    fixture, port = live()
    actor = fixture.store.get("life_actors", "actor:a")
    actor["schedule"] = [dict(minute=0, activity="reading", controls={"desk_light": 1})]
    actor["cursor"] = None  # a settled cursor would otherwise hide a tick
    fixture.store.put("life_actors", actor)
    before = facts(fixture.store)
    state = port.handle(SERVICE, "snapshot", {"schema_version": 1, "actor_id": "actor:a"})
    assert state["activity"] is None and state["state_basis"] == "last_persisted"
    assert facts(fixture.store) == before


# ----------------------------------------------------------------------------- diaries


def test_diaries_lists_published_rows_only_and_never_exposes_the_draft():
    fixture, port = live()
    first = published(fixture, day="2026-09-20", content="First published text.")
    second = published(fixture, day="2026-09-19", content="Superseded text.")
    newer = fixture.revise(
        second["id"],
        "Unpublished draft text.",
        source=dict(kind="human", editor="editor:secret", reason="internal note"),
    )
    draft_only = fixture.generate("actor:a", "2026-09-18", "Never published.")
    page = port.handle(SERVICE, "diaries", {"schema_version": 1, "actor_id": "actor:a"})
    assert [item["diary_id"] for item in page["items"]] == [first["id"], second["id"]]
    assert page["next_after"] is None and page["fictional"] is True
    entry = page["items"][1]
    assert entry["state"] == "draft"  # revised after publication: the live state stays true
    assert entry["published_revision_id"] != newer
    assert set(entry) == {
        "diary_id",
        "actor_id",
        "day",
        "state",
        "version",
        "published_revision_id",
        "fictional",
        "captured",
    }
    assert set(entry["captured"]) == {"recipe_id", "recipe_version", "material_version"}
    rendered = json.dumps(page)
    for hidden in (
        draft_only["id"],
        newer,
        "current_revision",
        "Unpublished draft text.",
        "config_version",
        "editor:secret",
        "internal note",
    ):
        assert hidden not in rendered


def test_diaries_paging_is_keyed_on_day_and_id_and_survives_a_deleted_cursor_row():
    fixture, port = live()
    for index in range(7):
        day = "2026-09-%02d" % (10 + index % 3)  # repeated days: day alone cannot page
        published(fixture, day=day, content="Text %d." % index, material="m%d" % index)
    ordered = sorted(
        fixture.diaries,
        key=lambda diary_id: (fixture.diaries[diary_id]["day"], diary_id),
        reverse=True,
    )
    seen, after, deleted = [], None, None
    while True:
        body = {"schema_version": 1, "actor_id": "actor:a", "limit": 2}
        if after is not None:
            body["after"] = after
        page = port.handle(SERVICE, "diaries", body)
        seen.extend(item["diary_id"] for item in page["items"])
        after = page["next_after"]
        if after is None:
            break
        if len(seen) == 4:
            # A cursor is a position, not a row reference: the row it names is deleted once
            # it has been handed out, and the remaining pages still continue without a gap or
            # a repeat because the step is a keyset comparison, not a reference lookup.
            deleted = after["diary_id"]
            fixture.store.delete("life_diaries", deleted)
    assert seen == ordered
    assert deleted in ordered and len(seen) == 7


@pytest.mark.parametrize(
    "damage", ["missing", "foreign", "material", "day", "state", "unmarked", "unmarked_revision"]
)
def test_diaries_refuses_a_corrupt_row_instead_of_skipping_it(damage):
    fixture, port = live()
    published(fixture, day="2026-09-20", content="Good text.")
    broken = published(fixture, day="2026-09-19", content="Broken text.", material="mmm")
    pointer = broken["published_revision"]
    if damage == "missing":
        fixture.store.delete("life_revisions", pointer)
    elif damage == "unmarked":
        broken["fictional"] = False
        fixture.store.put("life_diaries", broken)
    elif damage == "unmarked_revision":
        revision = fixture.store.get("life_revisions", pointer)
        revision["fictional"] = None
        fixture.store.put("life_revisions", revision)
    elif damage in ("foreign", "material"):
        revision = fixture.store.get("life_revisions", pointer)
        revision["conversation_id" if damage == "foreign" else "material_version"] = (
            "actor:b" if damage == "foreign" else "material:other"
        )
        fixture.store.put("life_revisions", revision)
    else:
        broken["day" if damage == "day" else "state"] = "2026-9-19" if damage == "day" else None
        fixture.store.put("life_diaries", broken)
    assert (
        refuses(port, "diaries", {"schema_version": 1, "actor_id": "actor:a"})
        == "dependency_unavailable"
    )


def test_diaries_cursor_and_limits_are_strict():
    fixture, port = live()
    base = {"schema_version": 1, "actor_id": "actor:a"}
    for after in (
        "2026-09-20",
        None,
        {"day": "2026-9-20", "diary_id": "x"},
        {"day": "2026-02-30", "diary_id": "x"},
        {"day": "2026-09-20", "diary_id": ""},
        {"day": "2026-09-20"},
        {"day": "2026-09-20", "diary_id": "x", "extra": 1},
    ):
        assert refuses(port, "diaries", dict(base, after=after)) == "invalid_input"
    for bad in ({"limit": 0}, {"limit": 51}, {"limit": False}, {"limit": None}, {"offset": 0}):
        assert refuses(port, "diaries", dict(base, **bad)) == "invalid_input"
    assert port.handle(SERVICE, "diaries", base)["items"] == []


# ----------------------------------------------------------------------------- revision


def test_revision_returns_the_published_pointer_with_its_capture():
    fixture, port = live()
    diary = published(fixture)
    document = port.handle(SERVICE, "revision", revision_body(diary))
    assert document == dict(
        schema_version=1,
        fictional=True,
        actor_id="actor:a",
        diary_id=diary["id"],
        diary_version=diary["version"],
        revision_id=diary["published_revision"],
        content="Published text.",
        created_at=FIXED_NOW,
        captured=dict(
            recipe_id="daily",
            recipe_version=1,
            material_version=diary["material_version"],
            config_version=19,
        ),
    )


def test_revision_compares_the_version_before_reading_any_content():
    fixture, port = live()
    diary = published(fixture)
    statements = []
    connection = fixture.store.db

    class Recorder:
        def execute(self, sql, parameters=()):
            statements.append(sql)
            return connection.execute(sql, parameters)

    port.queries.db = Recorder()
    code = refuses(port, "revision", revision_body(diary, expected_diary_version=99))
    assert code == "version_conflict"
    assert [sql for sql in statements if "life_diaries" in sql]
    assert not [sql for sql in statements if "life_revisions" in sql]


def test_revision_never_substitutes_the_current_draft():
    fixture, port = live()
    diary = published(fixture)
    draft = fixture.revise(
        diary["id"], "Later draft text.", source=dict(kind="human", editor="e", reason="r")
    )
    assert refuses(port, "revision", revision_body(diary, revision_id=draft)) == "not_found"
    assert (
        refuses(port, "revision", revision_body(diary, revision_id="revision:unknown"))
        == "not_found"
    )
    # Another actor cannot reach this diary, whatever revision id it names.
    assert refuses(port, "revision", revision_body(diary, actor_id="actor:b")) == "not_found"
    assert refuses(port, "revision", revision_body(diary, actor_id="actor:c")) == "not_found"
    for bad in (
        {"expected_diary_version": 0},
        {"expected_diary_version": True},
        {"expected_diary_version": None},
        {"diary_id": ""},
        {"revision_id": "x" * 129},
    ):
        assert refuses(port, "revision", revision_body(diary, **bad)) == "invalid_input"


def test_revision_inherits_a_human_revision_capture_from_its_gateway_parent():
    fixture, port = live()
    diary = fixture.generate("actor:a", "2026-09-20", "Generated text.")
    human = fixture.revise(
        diary["id"],
        "Human edited text.",
        source=dict(kind="human", editor="editor:secret", reason="internal note"),
    )
    fixture.publish(diary["id"], revision_id=human)
    document = port.handle(SERVICE, "revision", revision_body(diary))
    assert document["content"] == "Human edited text."
    assert document["revision_id"] == human
    assert document["captured"]["config_version"] == 19
    rendered = json.dumps(document)
    for hidden in ("editor:secret", "internal note", "receipt", "resolved_model", "provider_id"):
        assert hidden not in rendered


def test_revision_refuses_a_dangling_published_pointer():
    fixture, port = live()
    diary = published(fixture)
    fixture.store.delete("life_revisions", diary["published_revision"])
    assert refuses(port, "revision", revision_body(diary)) == "dependency_unavailable"


@pytest.mark.parametrize(
    "damage",
    [
        "missing_parent",
        "cycle",
        "too_deep",
        "material",
        "foreign",
        "unmarked",
        "kind",
        "no_gateway",
    ],
)
def test_revision_refuses_a_broken_parent_chain(damage):
    fixture, port = live()
    diary = fixture.generate("actor:a", "2026-09-20", "Generated text.")
    gateway_revision = diary["current_revision"]
    if damage == "too_deep":
        # 64 human links over one gateway revision: the 65th ancestor is past the budget.
        for index in range(64):
            fixture.revise(
                diary["id"], "Link %d." % index, source=dict(kind="human", editor="e", reason="r")
            )
    elif damage != "missing_parent":
        fixture.revise(
            diary["id"], "Human text.", source=dict(kind="human", editor="e", reason="r")
        )
    fixture.publish(diary["id"])
    published_id = diary["published_revision"]
    if damage == "missing_parent":
        fixture.store.delete("life_revisions", gateway_revision)
    elif damage == "cycle":
        revision = fixture.store.get("life_revisions", published_id)
        revision["parent"] = published_id
        fixture.store.put("life_revisions", revision)
    elif damage == "material":
        revision = fixture.store.get("life_revisions", published_id)
        revision["material_version"] = "material:other"
        fixture.store.put("life_revisions", revision)
    elif damage == "foreign":
        revision = fixture.store.get("life_revisions", published_id)
        revision["conversation_id"] = "actor:b"
        fixture.store.put("life_revisions", revision)
    elif damage == "unmarked":
        revision = fixture.store.get("life_revisions", published_id)
        revision.pop("fictional")
        fixture.store.put("life_revisions", revision)
    elif damage == "kind":
        revision = fixture.store.get("life_revisions", published_id)
        revision["source"] = dict(kind="imported")
        fixture.store.put("life_revisions", revision)
    elif damage == "no_gateway":
        revision = fixture.store.get("life_revisions", published_id)
        revision["source"] = dict(kind="human", editor="e", reason="r")
        revision["parent"] = None
        fixture.store.put("life_revisions", revision)
    expected = "budget_exceeded" if damage == "too_deep" else "dependency_unavailable"
    assert refuses(port, "revision", revision_body(diary)) == expected


def test_a_64_link_chain_still_proves_its_capture():
    fixture, port = live()
    diary = fixture.generate("actor:a", "2026-09-20", "Generated text.")
    for index in range(63):
        fixture.revise(
            diary["id"], "Link %d." % index, source=dict(kind="human", editor="e", reason="r")
        )
    fixture.publish(diary["id"])
    document = port.handle(SERVICE, "revision", revision_body(diary))
    assert document["captured"]["config_version"] == 19


@pytest.mark.parametrize(
    "damage", ["receipt_contract", "receipt_caller", "receipt_outcome", "disagrees", "no_receipt"]
)
def test_revision_refuses_a_receipt_that_does_not_prove_the_capture(damage):
    fixture, port = live()
    diary = published(fixture)
    revision = fixture.store.get("life_revisions", diary["published_revision"])
    if damage == "no_receipt":
        revision["source"] = dict(kind="gateway")
    elif damage == "receipt_contract":
        revision["source"]["receipt"]["config_version"] = "19"
    elif damage == "receipt_caller":
        revision["source"]["receipt"]["caller_service"] = "other"
    elif damage == "receipt_outcome":
        revision["source"]["receipt"]["outcome"] = "unknown"
    else:
        revision["source"]["receipt"]["config_version"] = 20
    fixture.store.put("life_revisions", revision)
    assert refuses(port, "revision", revision_body(diary)) == "dependency_unavailable"


def test_a_retry_that_moved_the_diary_configuration_is_refused_not_relabelled():
    fixture, port = live()
    diary = published(fixture)
    # `Life.retry_diary` rewrites the row's configuration version while the published
    # revision keeps the one it was really generated under: the two disagreeing is refused
    # rather than answered with the newer draft's configuration.
    diary["config_version"] = 20
    fixture.store.put("life_diaries", diary)
    assert refuses(port, "revision", revision_body(diary)) == "dependency_unavailable"


def test_revision_proves_the_diary_id_binding():
    fixture, port = live()
    diary = published(fixture, material="original")
    forged = fixture.store.get("life_diaries", diary["id"])
    forged["material_version"] = "material:forged"
    revision = fixture.store.get("life_revisions", diary["published_revision"])
    revision["material_version"] = "material:forged"
    fixture.store.put("life_revisions", revision)
    fixture.store.put("life_diaries", forged)
    assert refuses(port, "revision", revision_body(diary)) == "dependency_unavailable"


# ------------------------------------------------------------------- budgets and writes


def test_response_budget_refuses_rather_than_truncating():
    fixture, port = live()
    heavy = [
        published(fixture, day="2026-09-%02d" % (10 + index), material="m%d" % index)
        for index in range(3)
    ]
    for diary in heavy:
        diary["state"] = "s" * (LIST_CEILING // 3 + 1000)
        fixture.store.put("life_diaries", diary)
    assert (
        refuses(port, "diaries", {"schema_version": 1, "actor_id": "actor:a"}) == "budget_exceeded"
    )
    for diary in heavy:
        diary["state"] = "s" * 10
        fixture.store.put("life_diaries", diary)
    page = port.handle(SERVICE, "diaries", {"schema_version": 1, "actor_id": "actor:a"})
    assert len(page["items"]) == 3  # the same rows, whole, once they fit
    huge = published(
        fixture, day="2026-08-01", content="c" * (REVISION_CEILING + 1), material="huge"
    )
    assert refuses(port, "revision", revision_body(huge)) == "budget_exceeded"


def test_every_operation_leaves_all_facts_and_the_source_head_unchanged():
    fixture, port = equipped(deployed={"reader_id": READER, "actor_ids": ["actor:a", "actor:b"]})
    fixture.world()
    fixture.room()
    fixture.actor("actor:a", activity="reading", outfit_ref="outfit:one")
    fixture.actor("actor:b")
    fixture.grant("actor:a")
    fixture.grant("actor:b")
    diary = published(fixture)
    fixture.generate("actor:b", "2026-09-20", "Other actor text.")
    before, head = facts(fixture.store), fixture.store.source_head()
    assert port.handle(SERVICE, "actors", {"schema_version": 1, "limit": 1})["items"]
    port.handle(SERVICE, "snapshot", {"schema_version": 1, "actor_id": "actor:a"})
    port.handle(SERVICE, "diaries", {"schema_version": 1, "actor_id": "actor:a", "limit": 1})
    port.handle(SERVICE, "revision", revision_body(diary))
    # Refusals are reads too: a locked actor, a malformed request, a stale version and an
    # unregistered service all leave the same facts behind.
    assert refuses(port, "snapshot", {"schema_version": 1, "actor_id": "actor:zz"}) == "not_found"
    assert refuses(port, "diaries", {"schema_version": 1, "actor_id": "actor:a", "limit": 0}) == (
        "invalid_input"
    )
    assert refuses(port, "revision", revision_body(diary, expected_diary_version=99)) == (
        "version_conflict"
    )
    assert refuses(port, "actors", {"schema_version": 1}, service="nobody") == "forbidden"
    assert facts(fixture.store) == before
    assert fixture.store.source_head() == head


def test_a_cancelled_request_stops_itself_and_writes_nothing():
    h = Harness()
    h.clock.now = FIXED_NOW
    fixture = Fixture(h.core.store, h.clock)
    fixture.world()
    fixture.room()
    fixture.actor("actor:a")
    fixture.grant("actor:a")
    published(fixture)
    app = create_app(h.core, TOKENS, {SERVICE: DEPLOYED})
    before = facts(h.core.store)
    received, sent = asyncio.Event(), []

    async def receive():
        received.set()
        await asyncio.sleep(3600)  # the body never arrives; the request is cancelled here
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    async def run():
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/internal/v1/life-read/diaries",
            "raw_path": b"/internal/v1/life-read/diaries",
            "query_string": b"",
            "root_path": "",
            "headers": [
                (b"authorization", b"Bearer " + TOKENS[SERVICE].encode()),
                (b"content-type", b"application/json"),
            ],
            "client": ("127.0.0.1", 12345),
            "server": ("127.0.0.1", 80),
        }
        task = asyncio.create_task(app(scope, receive, send))
        await received.wait()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert task.done()

    asyncio.run(run())
    assert facts(h.core.store) == before
    assert not [message for message in sent if message["type"] == "http.response.start"]

    async def again():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://life"
        ) as client:
            return await client.post(
                "/internal/v1/life-read/diaries",
                json={"schema_version": 1, "actor_id": "actor:a"},
                headers={"Authorization": "Bearer " + TOKENS[SERVICE]},
            )

    ok = asyncio.run(again())
    assert ok.status_code == 200 and ok.json()["items"]
    assert facts(h.core.store) == before
    asyncio.run(h.core.close())


# ------------------------------------------------------------- genuine engine-written rows


def test_the_port_reads_rows_the_life_engine_wrote():
    """One diary produced by the real `Life` chain, then read through the authorized port."""
    store = Store(":memory:")
    clock = Clock()
    clock.now = FIXED_NOW
    asked = []

    async def handler(request):
        if request.url.path == "/v1/chat/completions":
            return httpx.Response(
                200,
                json=dict(
                    id="fixture:completion",
                    object="chat.completion",
                    choices=[
                        dict(
                            message=dict(role="assistant", content="A fictional quiet day."),
                            finish_reason="stop",
                        )
                    ],
                    usage=None,
                ),
            )
        payload = receipt(19, clock())
        payload["request_id"] = request.url.path.rsplit("/", 1)[-1]
        asked.append(payload["request_id"])
        return httpx.Response(200, json=payload)

    client = JsonService(
        "https://synthetic.invalid", "fixture", transport=httpx.MockTransport(handler)
    )
    life = Life(store, clock, Gateway(contracts(), client), 19, asyncio.Semaphore(4), writing=True)

    async def work():
        try:
            await life.work()
        finally:
            await client.close()

    life.create_world("world:home", timezone_name="UTC")
    life.create_room("room:study", "world:home")
    life.configure_actor(
        "actor:a",
        "room:study",
        personality_version=1,
        schedule=[dict(minute=0, activity="sleeping", controls={"desk_light": 0})],
    )
    life.record_event("event:one", "world:home", "A fictional garden", participants=["actor:a"])
    # The synthetic clock's local date is 2026-09-21, so that is the day the event was
    # learned on and therefore the day whose material can produce a diary.
    meta = life.request_diary("actor:a", "2026-09-21")
    asyncio.run(work())
    life.publish_diary(
        meta["id"], reviewer="reviewer:fixture", expected=life.diary_metadata(meta["id"])["version"]
    )
    life.set_diary_access("actor:a", readers=[READER])
    assert asked, "the synthetic gateway must have been asked exactly once"
    port = LifeRead(
        LifeReadQueries(store.db), readers={SERVICE: DEPLOYED}, contracts=contracts(), clock=clock
    )
    page = port.handle(SERVICE, "diaries", {"schema_version": 1, "actor_id": "actor:a"})
    assert [item["diary_id"] for item in page["items"]] == [meta["id"]]
    entry = page["items"][0]
    stored = life.diary_metadata(meta["id"])
    assert entry["captured"]["material_version"] == stored["material_version"]
    assert entry["captured"]["recipe_id"] == stored["recipe_id"]
    assert entry["version"] == stored["version"]
    document = port.handle(
        SERVICE,
        "revision",
        {
            "schema_version": 1,
            "actor_id": "actor:a",
            "diary_id": meta["id"],
            "revision_id": entry["published_revision_id"],
            "expected_diary_version": entry["version"],
        },
    )
    assert document["content"] == "A fictional quiet day."
    assert document["captured"]["config_version"] == 19
    assert document["captured"]["material_version"] == stored["material_version"]
    store.close()
