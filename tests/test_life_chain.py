"""TS-070 chain evidence: shared world -> per-actor knowledge -> day state ->
outfit capture -> diary material/draft -> image task snapshot, across a restart.

Every identifier, event, image and model answer here is synthetic. The ComfyUI
counterpart is the local stub HTTP server from test_images, never a real GPU.
"""

import asyncio
import copy
import hashlib
import json
from datetime import datetime, timezone

import pytest

from support import Clock, Harness
from test_images import complete, png, workflow  # noqa: F401
from test_images import server as comfy_server  # noqa: F401  (fixture import)
from test_life import Model
from tianshu_companion.images import ComfyUI, Images
from tianshu_companion.life import Life
from tianshu_companion.store import Store

DAY = "2026-09-14"
SCHEDULE = [
    dict(minute=0, activity="sleeping", controls={"desk_light": 0, "window": 0}),
    dict(minute=420, activity="reading", controls={"desk_light": 1, "window": 1}),
    dict(minute=1320, activity="sleeping", controls={"desk_light": 0, "window": 0}),
]


def chain(path=":memory:", *, writing=True):
    """One shared world, one room, two actors, fixed civil time 12:00 at +08:00."""
    store = Store(path)
    clock = Clock()
    clock.now = datetime(2026, 9, 14, 4, 0, tzinfo=timezone.utc).timestamp()
    model = Model()
    life = Life(store, clock, model, 19, asyncio.Semaphore(4), writing=writing)
    life.create_world("world", timezone_name="+08:00", setting="A fictional cottage")
    life.create_room("room", "world")
    for actor in ("a", "b"):
        life.configure_actor(actor, "room", personality_version=1, schedule=SCHEDULE)
    return life, clock, model


def payloads(model):
    """Diary requests actually sent to the model, keyed by fictional actor."""
    sent = {}
    for _, messages in model.calls:
        body = json.loads(messages[-1]["content"])
        sent[body["actor_id"]] = body
    return sent


def test_chain_knowledge_state_diary_capture(tmp_path):
    async def run():
        life, clock, model = chain(tmp_path / "chain.db")
        try:
            head = life.store.source_head()
            first = life.snapshot("a")
            assert first["actor"]["activity"] == "reading"
            assert first["room"]["controls"]["window"]["value"] == 1
            assert first["room"] == life.snapshot("b")["room"]  # one shared room authority

            # Shared world: participants and witnesses learn; a private event stays
            # with its actor and is not reachable by the other one.
            life.record_event(
                "e:garden", "world", "A and B planted fictional mint", participants=["a", "b"]
            )
            life.record_event(
                "e:shell", "world", "A found a fictional blue shell alone", participants=["a"]
            )
            life.record_event(
                "e:storm",
                "world",
                "A watched the fictional storm",
                participants=["a"],
                visible_to=["b"],
            )
            a_events = [item["event_id"] for item in life.materials("a", DAY)]
            b_events = [item["event_id"] for item in life.materials("b", DAY)]
            assert {"e:garden", "e:shell", "e:storm"} <= set(a_events)
            assert {"e:garden", "e:storm"} <= set(b_events)
            assert "e:shell" not in b_events
            via = {item["event_id"]: item["via"] for item in life.materials("b", DAY)}
            assert via["e:garden"] == "participated" and via["e:storm"] == "witnessed"
            with pytest.raises(PermissionError):
                life.tell("e:shell", "b", "a")  # B never learned it, so B cannot retell it

            # Manual holds survive reconciliation; B keeps its own activity.
            held = life.set_activity(
                "a", "painting", expected=life.snapshot("a")["actor"]["version"]
            )
            room = life.snapshot("a")["room"]
            life.set_room("room", {"window": 0.2}, expected=room["version"])
            clock.now = datetime(2026, 9, 14, 14, 0, tzinfo=timezone.utc).timestamp()
            life.tick(force=True)
            current = life.snapshot("a")
            assert current["actor"]["activity"] == "painting"
            assert current["actor"]["manual"] == held["manual"]
            assert current["room"]["controls"]["window"]["value"] == 0.2
            assert current["room"]["controls"]["window"]["mode"] == "manual"
            assert life.snapshot("b")["actor"]["activity"] == "sleeping"

            # Only an explicit release or the hold deadline returns to the schedule.
            a = life.snapshot("a")["actor"]
            life.set_activity("a", "painting", expected=a["version"], hold_until=clock() + 60)
            room = life.snapshot("a")["room"]
            life.set_room(
                "room", {"window": 0.4}, expected=room["version"], hold_until=clock() + 60
            )
            clock.advance(61)
            released = life.snapshot("a")
            assert (
                released["actor"]["activity"] == "sleeping" and released["actor"]["manual"] is None
            )
            assert released["room"]["controls"]["window"]["mode"] == "auto"

            # Diary material and draft carry the snapshot they were built from.
            meta_b = life.request_diary("b", DAY)
            meta_a = life.request_diary("a", DAY)
            for meta in (meta_b, meta_a):
                assert meta["fictional"] is True
                assert meta["recipe_id"] == "daily" and meta["recipe_version"] == 1
                assert meta["config_version"] == 19 and meta["material_version"]
                assert meta["real_chat_sources"].startswith("excluded")
                assert meta["state"] == "queued"
            assert meta_a["material_version"] != meta_b["material_version"]

            for _ in range(3):
                await life.work()
            sent = payloads(model)
            assert set(sent) == {"a", "b"}
            assert "fictional blue shell" in json.dumps(sent["a"])
            assert "fictional blue shell" not in json.dumps(sent["b"])
            assert all(item["fictional"] is True for item in sent["b"]["material"])
            draft_b = life.diary_metadata(meta_b["id"])
            assert draft_b["state"] == "draft"
            assert draft_b["fictional"] is True
            assert draft_b["material_version"] == meta_b["material_version"]

            # Publication is per captured material version; later events make a new
            # task instead of silently rewriting what was already reviewed.
            life.set_diary_access("b", readers=["reader"])
            revision = life.publish_diary(
                meta_b["id"], reviewer="admin", expected=draft_b["version"]
            )
            published = life.read_diary(meta_b["id"], reader="reader")
            assert published["id"] == revision
            assert published["fictional"] is True
            assert published["material_version"] == meta_b["material_version"]
            assert "source" not in published
            life.record_event("e:teacup", "world", "B found a fictional teacup", participants=["b"])
            later = life.request_diary("b", DAY)
            assert later["id"] != meta_b["id"]
            assert later["material_version"] != meta_b["material_version"]
            assert later["state"] == "queued"
            assert life.read_diary(meta_b["id"], reader="reader") == published

            # Fictional life never enters the real source-fact stream.
            assert life.store.source_head() == head
            events = len(life.store.list("life_events"))
            calls = len(model.calls)
            life.store.close()

            # Restart: one publication per captured material version survives, the
            # same day's material is not re-enqueued, and reconciliation is not replayed.
            store = Store(tmp_path / "chain.db")
            restarted = Life(store, clock, model, 19, asyncio.Semaphore(4), writing=True)
            try:
                restarted.recover()
                for _ in range(2):
                    restarted.tick(force=True)
                assert len(store.list("life_events")) == events
                assert restarted.read_diary(meta_b["id"], reader="reader") == published
                same = restarted.request_diary("b", DAY)
                assert same["id"] == later["id"] and same["state"] == "queued"
                assert (
                    restarted.publish_diary(
                        meta_b["id"],
                        reviewer="admin",
                        expected=restarted.diary_metadata(meta_b["id"])["version"],
                    )
                    == revision
                )
                await restarted.work()
                assert len(model.calls) == calls + 1  # only the new material version
                assert restarted.read_diary(meta_b["id"], reader="reader") == published
                assert store.source_head() == head
            finally:
                store.close()
        finally:
            life.store.close()

    asyncio.run(run())


def test_chain_shared_room_conflict_and_manual_hold():
    life, clock, _ = chain()
    try:
        other = life.snapshot("b")["actor"]
        life.configure_actor(
            "b",
            "room",
            personality_version=1,
            schedule=[
                dict(minute=0, activity="sleeping", controls={"desk_light": 0}),
                dict(minute=420, activity="writing", controls={"desk_light": 0, "window": 1}),
            ],
            expected=other["version"],
        )
        settled = life.snapshot("a")["room"]
        # Actor id ascending: the later actor's wish wins the shared control.
        assert settled["controls"]["desk_light"]["value"] == 0
        assert settled["controls"]["window"]["value"] == 1

        room = life.snapshot("a")["room"]
        held = life.set_room("room", {"desk_light": 1}, expected=room["version"])
        for _ in range(2):
            life.tick(force=True)
        assert life.snapshot("b")["room"]["controls"]["desk_light"]["value"] == 1
        resumed = life.resume_room("room", ["desk_light"], expected=held["version"])
        life.tick(force=True)
        assert resumed and life.snapshot("a")["room"]["controls"]["desk_light"]["value"] == 0
    finally:
        life.store.close()


def test_chain_image_capture_survives_state_change_and_restart(
    tmp_path,
    comfy_server,  # noqa: F811  (synthetic ComfyUI fixture imported from test_images)
):
    state, url = comfy_server

    async def run():
        path = tmp_path / "chain.db"
        life, clock, model = chain(path)
        images = Images(
            life,
            transport=ComfyUI(url, timeout=0.3),
            workflow=workflow(),
            staging=tmp_path / "staged",
        )
        images.put_outfit(
            "outfit:reading",
            description="Fictional robe",
            prompt="fictional blue robe",
            reference="synthetic.png",
        )
        images.put_outfit(
            "outfit:painting", description="Fictional smock", prompt="fictional smock"
        )
        images.select_outfit(
            "a", "outfit:painting", expected=life.snapshot("a")["actor"]["version"]
        )
        images.select_outfit("b", "outfit:reading", expected=life.snapshot("b")["actor"]["version"])
        job = images.request("req:a", "a", parameters={"seed": 11})
        captured = copy.deepcopy(job)
        assert job["fictional"] is True and job["state"] == "queued"
        assert job["snapshot"]["actor"]["activity"] == "reading"
        assert job["snapshot"]["actor"]["outfit_ref"] == "outfit:painting"
        assert job["outfit"]["version"] == 1
        assert job["snapshot"]["actor"]["version"] >= 1
        assert job["snapshot"]["room"]["version"] >= 1
        assert job["snapshot"]["world"]["version"] >= 1
        assert "fictional smock" in job["graph"]["1"]["inputs"]["text"]

        # Live state moves on; the stored task keeps its capture.
        actor = life.snapshot("a")["actor"]
        life.set_activity("a", "sleeping", expected=actor["version"])
        images.put_outfit(
            "outfit:painting",
            description="Fictional smock v2",
            prompt="fictional new smock",
            expected=1,
        )
        images.select_outfit("a", "outfit:reading", expected=life.snapshot("a")["actor"]["version"])
        room = life.snapshot("a")["room"]
        life.set_room("room", {"window": 0.9}, expected=room["version"])
        assert images.get("req:a") == captured
        second = images.request("req:b", "b", parameters={"seed": 12})
        assert second["snapshot"]["actor"]["activity"] == "reading"
        assert second["snapshot"]["actor"]["outfit_ref"] == "outfit:reading"
        assert second["outfit"]["version"] == 1
        # B's own task captures B's current state; A moved on without touching either.
        moved_on = life.snapshot("a")["actor"]
        assert moved_on["version"] > captured["snapshot"]["actor"]["version"]
        assert moved_on["activity"] == "sleeping" and moved_on["outfit_ref"] == "outfit:reading"
        assert second["snapshot"]["actor"]["version"] == life.snapshot("b")["actor"]["version"]

        # Real local round trip against the synthetic ComfyUI stub. B's unsubmitted
        # task is cancelled locally first, so the pass can only continue A's prompt.
        images.cancel("req:b")
        assert images.get("req:b")["state"] == "cancelled"
        await images.work()
        assert len(state.posts) == 1
        complete(state, captured)
        await images.work()
        done = images.get("req:a")
        assert done["state"] == "completed" and len(state.posts) == 1
        artifact = done["artifacts"][0]
        assert (
            artifact["sha256"] == hashlib.sha256(png()).hexdigest()
            and artifact["archived"] is False
        )
        for key in ("snapshot", "outfit", "graph", "workflow_version", "fictional"):
            assert done[key] == captured[key]
        assert life.snapshot("a")["actor"]["activity"] == "sleeping"  # image never rewrote life
        events = len(life.store.list("life_events"))
        head = life.store.source_head()

        # Restart: no resubmission, no duplicate reconciliation, no lost capture.
        await images.transport.close()
        life.store.close()
        store = Store(path)
        restarted = Life(store, clock, model, 19, asyncio.Semaphore(4), writing=True)
        restarted_images = Images(
            restarted,
            transport=ComfyUI(url, timeout=0.3),
            workflow=workflow(),
            staging=tmp_path / "staged",
        )
        try:
            restarted.recover()
            restarted_images.recover()
            after = restarted_images.get("req:a")
            assert after["state"] == "completed"
            for key in ("snapshot", "outfit", "graph", "workflow_version"):
                assert after[key] == captured[key]
            for _ in range(2):
                restarted.tick(force=True)
            assert len(store.list("life_events")) == events
            assert store.list("image_jobs", states=["queued", "running", "unknown"]) == []
            await restarted_images.work()
            assert len(state.posts) == 1
            # Restart does not re-enqueue an already captured material version.
            task = restarted.request_diary("a", DAY)
            assert task["state"] == "queued"
            assert restarted.request_diary("a", DAY)["id"] == task["id"]
            assert store.list("life_diaries", states=["generating"]) == []
            assert store.source_head() == head
        finally:
            await restarted_images.transport.close()
            store.close()

    asyncio.run(run())


def test_chain_chat_summary_and_image_capture_share_one_state(tmp_path):
    """C3: the same state drives chat statements and outfit references, while an
    already captured image task keeps its own versions."""

    async def run():
        h = Harness()
        life = h.core.life
        life.create_world("world")
        life.create_room("room", "world")
        life.configure_actor(
            "actor:a",
            "room",
            personality_version=1,
            schedule=[dict(minute=0, activity="reading", controls={"desk_light": 1})],
        )
        # No HTTP happens while creating a task; the transport is only used by work().
        images = Images(
            life,
            transport=ComfyUI("http://127.0.0.1:1"),
            workflow=workflow(),
            staging=tmp_path / "staged",
        )
        images.put_outfit("outfit:reading", description="Fictional robe", prompt="fictional robe")
        images.select_outfit(
            "actor:a", "outfit:reading", expected=life.snapshot("actor:a")["actor"]["version"]
        )
        job = images.request("req:chat", "actor:a")
        captured = copy.deepcopy(job)
        try:
            actor = life.snapshot("actor:a")["actor"]
            life.set_activity("actor:a", "painting", expected=actor["version"])
            room = life.snapshot("actor:a")["room"]
            life.set_room("room", {"desk_light": 0}, expected=room["version"])
            assert await h.core.ingest("nonebot", h.request(actor="actor:a"))
            h.clock.advance(6)
            await h.cycles()

            summary = json.loads(h.gateway.calls[0][1][-1]["content"])["fictional_life"][0]
            live = life.snapshot("actor:a")
            assert summary["fictional"] is True
            assert summary["actor_id"] == "actor:a"
            assert summary["actor_version"] == live["actor"]["version"]
            assert summary["room_version"] == live["room"]["version"]
            assert summary["world_version"] == live["world"]["version"]
            assert summary["activity"] == "painting" and summary["outfit_ref"] == "outfit:reading"

            # The earlier image task still reports the capture it was created from.
            assert images.get("req:chat") == captured
            assert captured["snapshot"]["actor"]["activity"] == "reading"
            assert captured["snapshot"]["actor"]["version"] < live["actor"]["version"]
            assert captured["snapshot"]["room"]["version"] < live["room"]["version"]
        finally:
            await images.transport.close()
            await h.core.close()

    asyncio.run(run())
