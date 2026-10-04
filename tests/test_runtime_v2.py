"""Production owner behavior with isolated accounts and transport doubles, not external acceptance."""

import asyncio
import copy
import hashlib
import sqlite3
import json

import httpx
import pytest

from support import Harness
from test_proactive import member, START
from tianshu_companion.app import create_app
from tianshu_companion.clients import utc
from tianshu_companion.contracts import Fault
from tianshu_companion.images import Images
from tianshu_companion.life_album import Album
from tianshu_companion.store import Store, RUNTIME_TABLES


def scope_for(target):
    return dict(
        actor_id=target.actor,
        person_id=target.person,
        audience="self_private",
        conversation_id=target.conversation,
    )


def feedback(scope, event, kind="warm"):
    return dict(
        scope=scope,
        event_id=event,
        kind=kind,
        reason="合成已观察反馈",
        sources=[],
        half_life_seconds=3600,
    )


def activity(id="activity:read", scope=None):
    return dict(
        id=id,
        title="研究恒星演化",
        state="running",
        checkpoint=dict(step=2, position=48, unit="pages", note="读到原件第48页"),
        next_due_at=START + 60,
        resume_condition=None,
        sources=[],
        result_refs=[],
        scope=scope,
    )


def platform_query(h, account="a", actor="actor:a", channel="private:a"):
    request = h.request(account=account, actor=actor, channel=channel)
    query = {key: request["command"][key] for key in ("schema_version", "request_id", "origin")}
    context = h.origins.values[query["origin"]["assertion_ref"]]
    context.update(issuer="platform", authenticated_service="platform")
    return query


def test_event_fanout_reaches_subscription_49_after_restart(tmp_path):
    async def run():
        h = Harness(tmp_path / "core.db")
        h.clock.now = START
        h.core.recover()
        try:
            target = await member(h)
            subscription = target.subscription
            for index in range(48):
                item = copy.deepcopy(subscription)
                item["id"] = f"subscription:extra:{index:02}"
                h.core.store.put("proactive_subscriptions", item)
            actor = h.core.life._get("actors", "actor:a")
            event = dict(
                id="event:actual-reading",
                world_id=actor["world_id"],
                participants=[actor["id"]],
                visible_to=[],
                summary="完成原件阅读片段",
                occurred_at=h.clock(),
                fictional=True,
                kind="reading_progress",
                scope=None,
                content_refs=[],
            )
            h.core.life._event(event)
            h.core.runtime_execution.offer_pending_events()
            assert len(h.core.store.list("proactive_motives")) == 16
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            for _ in range(3):
                h.core.runtime_execution.offer_pending_events()
            motives = h.core.store.list("proactive_motives")
            assert len(motives) == 49
            assert len({m["subscription_id"] for m in motives}) == 49
            h.core.runtime_execution.offer_pending_events()
            assert h.core.store.list("proactive_motives") == motives
        finally:
            await h.core.close()

    asyncio.run(run())


def test_affect_filters_private_scope_before_bounded_limit():
    async def run():
        h = Harness()
        h.core.recover()
        try:
            own = dict(
                actor_id="actor:a",
                person_id="person:a",
                audience="self_private",
                conversation_id="conversation:a",
            )
            other = dict(own, person_id="person:b", conversation_id="conversation:b")
            saved = h.core.life.affect.feedback("actor:a", feedback(own, "own:event"))
            h.clock.advance(1)
            for index in range(48):
                h.core.life.affect.feedback("actor:a", feedback(other, f"other:{index}"))
            view = h.core.life.affect.snapshot("actor:a", own)
            assert [item["id"] for item in view["feelings"]] == [saved["id"]]
            assert view["relationship_effect"] == "none"
            assert h.core.life.affect.snapshot("actor:a", None)["feelings"] == []
        finally:
            await h.core.close()

    asyncio.run(run())


def test_confirmed_proactive_contact_silence_and_recovery_are_once_and_durable(tmp_path):
    async def run():
        h = Harness(tmp_path / "core.db")
        h.clock.now = START
        h.core.recover()
        try:
            target = await member(h)
            scope = scope_for(target)
            receipt = dict(
                observed_at=utc(h.clock()), segments=[dict(state="queued", channel_message_ids=[])]
            )
            assert not h.core.record_contact(
                "expression:one",
                scope,
                receipt,
                kind="proactive",
                subscription_id=target.subscription["id"],
            )
            receipt["segments"][0].update(state="sent", channel_message_ids=["synthetic:ack"])
            assert h.core.record_contact(
                "expression:one",
                scope,
                receipt,
                kind="proactive",
                subscription_id=target.subscription["id"],
            )
            assert not h.core.record_contact(
                "expression:one",
                scope,
                receipt,
                kind="proactive",
                subscription_id=target.subscription["id"],
            )
            h.core.runtime_execution.feedback_for_contacts()
            assert h.core.life.affect.snapshot(target.actor, scope)["feelings"] == []
            h.clock.advance(3601)
            h.core.runtime_execution.feedback_for_contacts()
            first = h.core.life.affect.snapshot(target.actor, scope)
            assert first["valence"] == 0 and len(first["feelings"]) == 1
            assert first["feelings"][0]["kind"] == "neutral"
            assert "原因未知" in first["feelings"][0]["reason"]
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            h.core.runtime_execution.feedback_for_contacts()
            assert len(h.core.store.list("life_affect")) == 1
            await h.ingest(text="刚忙完，现在回来啦")
            h.core.runtime_execution.feedback_for_contacts()
            h.core.runtime_execution.feedback_for_contacts()
            snapshot = h.core.life.affect.snapshot(target.actor, scope)
            assert len(h.core.store.list("life_affect")) == 2
            assert any(item["kind"] == "recovery" for item in snapshot["feelings"])
            assert all(item["kind"] != "dislike" for item in snapshot["feelings"])
            contact = h.core.store.list("delivery_contacts")[0]
            assert contact["feedback_state"] == "responded"
        finally:
            await h.core.close()

    asyncio.run(run())


def test_cross_day_restart_keeps_activity_checkpoint_without_fabricated_steps(tmp_path):
    async def run():
        h = Harness(tmp_path / "core.db")
        h.clock.now = START
        h.core.recover()
        try:
            saved = h.core.life.activities.save("actor:a", activity(), expected=0)
            h.core.life.activities.transition(
                "actor:a", saved["id"], "paused", expected=1, reason="明天继续"
            )
            checkpoint = saved["checkpoint"]
            await h.core.close()
            h.clock.advance(86400 * 2)
            h.core = h.new_core()
            h.core.recover()
            restored = h.core.life.activities.current("actor:a")
            assert restored["checkpoint"] == checkpoint and restored["state"] == "paused"
            assert restored["version"] == 2
            h.core.life.tick(force=True)
            assert h.core.life.activities.current("actor:a")["id"] == saved["id"]
        finally:
            await h.core.close()

    asyncio.run(run())


def test_ensure_http_without_chat_is_stable_and_disabled_actor_denied(tmp_path):
    async def run():
        h = Harness(tmp_path / "core.db")
        h.core.recover()
        try:
            h.core.bindings["qq-private"]["service"] = "platform"
            h.core.life.set_diary_access("actor:a", readers=["reader:operator"])
            app = create_app(
                h.core,
                {"platform": "test-token"},
                life_readers={"platform": dict(reader_id="reader:operator", actor_ids=["actor:a"])},
            )
            query = platform_query(h)
            body = dict(schema_version=2, query=query, actor_id="actor:a")
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post(
                    "/internal/v2/life/conversation/ensure",
                    json=body,
                    headers={"Authorization": "Bearer test-token"},
                )
                assert response.status_code == 200, response.text
                scope = response.json()["scope"]
                again = await client.post(
                    "/internal/v2/life/conversation/ensure",
                    json=body,
                    headers={"Authorization": "Bearer test-token"},
                )
                assert again.json()["scope"] == scope
            assert h.core.store.list("physicals") == [] and h.core.store.list("turns") == []
            assert h.gateway.calls == []
            await h.core.close()
            h.core = h.new_core()
            h.core.bindings["qq-private"]["service"] = "platform"
            h.core.recover()
            create_app(
                h.core,
                {"platform": "test-token"},
                life_readers={"platform": dict(reader_id="reader:operator", actor_ids=["actor:a"])},
            )
            assert (await h.core.life_runtime.ensure_conversation("platform", body))[
                "scope"
            ] == scope
            h.core.life.synchronize_role("actor:a", enabled=False, personality_version=1)
            with pytest.raises(Fault):
                await h.core.life_runtime.ensure_conversation("platform", body)
        finally:
            await h.core.close()

    asyncio.run(run())


def test_image_reference_first_read_version_can_be_saved_over_http():
    async def run():
        h = Harness()
        h.core.recover()
        try:
            h.core.life.set_diary_access("actor:a", readers=["reader:operator"])
            app = create_app(
                h.core,
                {"platform": "test-token"},
                life_readers={"platform": dict(reader_id="reader:operator", actor_ids=["actor:a"])},
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                headers={"Authorization": "Bearer test-token"},
            ) as client:
                view = await client.post(
                    "/internal/v2/life/read",
                    json=dict(
                        schema_version=2,
                        query=platform_query(h),
                        actor_id="actor:a",
                        scope=None,
                        resource="image_reference",
                        object_id=None,
                        expected_version=None,
                        limit=20,
                        after=None,
                    ),
                )
                assert view.status_code == 200, view.text
                initial = view.json()["items"][0]
                request = dict(
                    schema_version=2,
                    request_id="reference:first",
                    actor_id="actor:a",
                    operation="actor.image-reference.configure",
                    expected_version=initial["version"],
                    value=dict(content_ref=None, scope=None, query=None),
                )
                response = await client.post("/internal/v2/life/manage", json=request)
                assert response.status_code == 200, response.text
                assert response.json()["result"]["version"] == 2
                replay = await client.post("/internal/v2/life/manage", json=request)
                assert replay.json() == response.json()
                stale = await client.post(
                    "/internal/v2/life/manage", json={**request, "request_id": "reference:stale"}
                )
                assert stale.status_code == 409
        finally:
            await h.core.close()

    asyncio.run(run())


def test_withdrawn_turn_sources_rejected_but_unrelated_intention_retained():
    async def run():
        h = Harness(silence_ms=0)
        h.core.recover()
        try:
            await h.ingest()
            await h.cycles()
            turn = h.turns()[0]
            source = dict(
                owner="companion",
                object_id=turn["id"],
                version=turn["bundle"]["collection_revision"],
            )
            h.core.runtime_execution.current_sources([source])
            saved = h.core.life.activities.save("actor:a", activity(), expected=0)
            turn["cancelled"] = True
            h.core._save_turn(turn)
            with pytest.raises(Fault):
                h.core.runtime_execution.current_sources([source])
            h.core.runtime_execution.current_sources(
                [dict(owner="companion", object_id=saved["id"], version=1)]
            )
        finally:
            await h.core.close()

    asyncio.run(run())


def test_old_origin_does_not_block_exact_memory_source_validation():
    async def run():
        h = Harness(silence_ms=0)
        h.core.recover()
        try:
            await h.ingest()
            await h.cycles()
            turn = h.turns()[0]
            unit = dict(
                record_id="record:actual",
                record_version=3,
                sources=[m["source"] for m in turn["bundle"]["messages"]],
            )
            turn["preparation"]["selected_units"] = [unit]
            h.core._save_turn(turn)
            h.origins.values[turn["origin"]["assertion_ref"]]["expires_at"] = utc(h.clock() - 1)
            candidate = dict(
                turn["scope"],
                sources=[dict(owner="memory", object_id=unit["record_id"], version=3)],
                content_refs=[],
            )
            observed = []

            async def exact(found, sources):
                observed.append(sources)
                return h.memory.scope_version

            h.memory.check_sources = exact
            await h.core.runtime_execution.validate_sources(candidate)
            assert observed == [unit["sources"]]
            h.memory.scope_version += 1
            with pytest.raises(Fault):
                await h.core.runtime_execution.validate_sources(candidate)
        finally:
            await h.core.close()

    asyncio.run(run())


def test_completed_old_image_descriptors_are_adopted_in_bounded_restart_passes(tmp_path):
    from test_images import png
    from test_life import setup

    database = tmp_path / "core.db"
    staging = tmp_path / "originals"
    staging.mkdir()
    raw = png()
    sha = hashlib.sha256(raw).hexdigest()
    (staging / "old.png").write_bytes(raw)
    life, clock, model = setup(Store(database))
    images = Images(life, staging=staging)
    life.album = Album(life, images)
    for index in range(5):
        life.store.put(
            "image_jobs",
            dict(
                id=f"old:{index}",
                conversation_id="a",
                state="completed",
                created_at=START,
                artifacts=[
                    dict(
                        staging_name="missing.png" if index == 4 else "old.png",
                        sha256=sha,
                        size=len(raw),
                        media_type="image/png",
                        width=1,
                        height=1,
                        archived=True,
                    )
                ],
            ),
        )
    images.recover()
    assert len(life.store.list("image_media")) == 4
    assert all(item["completed_at"] is None for item in life.store.list("image_media"))
    life.store.close()
    from tianshu_companion.life import Life

    life = Life(Store(database), clock, model, 19, asyncio.Semaphore(4))
    images = Images(life, staging=staging)
    life.album = Album(life, images)
    images.recover()
    media = life.store.list("image_media")
    assert len(media) == 5
    assert len(life.store.list("life_album")) == 5
    assert sum(item["state"] == "unavailable" for item in media) == 1
    available = next(item for item in media if item["state"] == "available")
    assert life.album.read("a", available["id"])[0] == raw
    with pytest.raises(Fault):
        life.album.read("b", available["id"])
    events = [
        item for item in life.store.list("life_events") if item.get("kind") == "image_completed"
    ]
    assert events == []
    images.recover()
    assert len(life.store.list("image_media")) == 5
    life.store.close()


def test_v9_upgrade_backs_up_wal_and_preserves_facts_single_owner_and_new_writes(tmp_path):
    async def run():
        database = tmp_path / "companion.db"
        h = Harness(database, silence_ms=0)
        h.core.recover()
        await h.ingest(text="需保留的原始来源")
        original = h.core.store.list("physicals")
        head = h.core.store.get("metadata", "source_head")
        await h.core.close()
        legacy = sqlite3.connect(database, isolation_level=None)
        try:
            for table in RUNTIME_TABLES:
                legacy.execute(f"DROP TABLE {table}")
            legacy.execute("PRAGMA user_version=9")
            legacy.execute("PRAGMA journal_mode=WAL")
            legacy.execute(
                "INSERT INTO metadata(id,body) VALUES (?,?)",
                ("legacy:wal", json.dumps(dict(id="legacy:wal", value="committed in WAL"))),
            )
            upgraded = Store(database)
            assert upgraded.db.execute("PRAGMA user_version").fetchone()[0] == 10
            assert upgraded.list("physicals") == original
            assert upgraded.get("metadata", "source_head") == head
            assert upgraded.get("metadata", "legacy:wal")["value"] == "committed in WAL"
            backup_paths = list(tmp_path.glob("companion.db.pre-life-runtime-v10-*.bak"))
            assert len(backup_paths) == 1
            with sqlite3.connect(backup_paths[0]) as backup:
                assert backup.execute("PRAGMA user_version").fetchone()[0] == 9
                assert backup.execute("SELECT body FROM metadata WHERE id='legacy:wal'").fetchone()
                assert (
                    backup.execute("SELECT 1 FROM sqlite_master WHERE name='life_album'").fetchone()
                    is None
                )
            with pytest.raises(RuntimeError, match="running owner"):
                Store(database)
            new = dict(
                id="activity:after-upgrade",
                conversation_id="actor:a",
                state="paused",
                checkpoint=dict(step=3),
            )
            upgraded.put("life_activities", new)
            upgraded.close()
        finally:
            legacy.close()
        reopened = Store(database)
        assert reopened.get("life_activities", new["id"]) == new
        assert reopened.list("physicals") == original
        assert len(list(tmp_path.glob("companion.db.pre-life-runtime-v10-*.bak"))) == 1
        reopened.close()

    asyncio.run(run())
