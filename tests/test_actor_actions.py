"""Original action owners, transactional delivery and bounded explicit history imports."""

import asyncio

import pytest

from test_actor_memory import setup, close, SOURCE_PREFIX, read, material
from test_runtime_v2 import activity, platform_query
from tianshu_companion.app import create_app
import httpx
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.clients import utc


def sources(h):
    return [row for row in h.core.store.list("metadata") if row["id"].startswith(SOURCE_PREFIX)]


def image(h, key="image:one", state="completed"):
    return dict(
        id=key,
        conversation_id="actor:a",
        version=1,
        state=state,
        artifacts=[dict(sha256="a" * 64)],
        failure=None,
        scope=None,
        created_at=h.clock() - 100,
        completed_at=h.clock(),
        cancel_requested=False,
    )


def test_image_fact_is_atomic_idempotent_and_rechecks_original_owner(monkeypatch):
    async def run():
        h, owners, services = await setup()
        try:
            original = h.core.store.put

            def fail(table, item):
                if table == "outbox":
                    raise RuntimeError("isolated crash before original transaction commits")
                return original(table, item)

            monkeypatch.setattr(h.core.store, "put", fail)
            with pytest.raises(RuntimeError):
                h.core.images._save_job(image(h))
            assert h.core.store.get("image_jobs", "image:one") is None
            assert sources(h) == []
            monkeypatch.setattr(h.core.store, "put", original)
            h.core.images._save_job(image(h))
            h.core.images._save_job(image(h))
            assert len(sources(h)) == 1
            row = sources(h)[0]
            assert row["fact"]["content_reads"] == []
            assert row["fact"]["outcome"] == "completed"
            request = dict(
                schema_version=1,
                request_id="image-check",
                actor_id="actor:a",
                role_version=owners.version,
                operation="source_check",
                refs=[row["source"]],
                context=dict(purpose="plan", audience=dict(kind="actor_only")),
            )
            assert (await h.core.actor_memory.source("memory", request))["items"][0]["read_allowed"]
            h.core.store.delete("image_jobs", "image:one")
            check = (await h.core.actor_memory.source("memory", request))["items"][0]
            assert check["state"] == "withdrawn" and not check["read_allowed"]
        finally:
            await close(h, services)

    asyncio.run(run())


def test_unknown_images_and_send_receipts_never_become_completed():
    async def run():
        h, owners, services = await setup()
        try:
            h.core.images._save_job(image(h, state="unknown"))
            assert sources(h)[0]["fact"]["outcome"] == "unknown"
            scope = dict(
                actor_id="actor:a",
                person_id="person:a",
                audience="self_private",
                conversation_id="private:a",
            )
            receipt = dict(
                expression_id="expression:a",
                observed_at=utc(h.clock()),
                segments=[dict(state="unknown", channel_message_ids=["uncertain:id"])],
            )
            h.core.record_contact("expression:a", scope, receipt)
            assert len(sources(h)) == 1
            receipt["segments"][0]["state"] = "sent"
            h.core.record_contact("expression:a", scope, receipt)
            h.core.record_contact("expression:a", scope, receipt)
            assert len(sources(h)) == 2
            fact = next(
                row["fact"]
                for row in sources(h)
                if row["fact"]["source_constraints"]["kind"] == "private"
            )
            assert fact["source_constraints"] == dict(
                kind="private", person_id="person:a", conversation_id="private:a"
            )
            assert "至少一个" in fact["summary"]
        finally:
            await close(h, services)

    asyncio.run(run())


def test_explicit_history_import_is_actor_bounded_and_replay_safe():
    async def run():
        h, owners, services = await setup()
        try:
            for key in ("image:1", "image:2", "image:3"):
                h.core.store.put("image_jobs", image(h, key))
            foreign = image(h, "image:other")
            foreign["conversation_id"] = "actor:b"
            h.core.store.put("image_jobs", foreign)
            value = dict(owner="images", after=None, limit=2)
            first = h.core.actor_memory.actions.import_history("actor:a", value, "import:1")
            assert first["imported"] == 2 and not first["complete"]
            replay = h.core.actor_memory.actions.import_history("actor:a", value, "import:2")
            assert replay["imported"] == 0 and replay["skipped"] == 2
            last = h.core.actor_memory.actions.import_history(
                "actor:a", dict(value, after=first["next_cursor"]), "import:3"
            )
            assert last["imported"] == 1 and last["complete"] and last["next_cursor"] is None
            assert all(row["actor_id"] == "actor:a" for row in sources(h))
        finally:
            await close(h, services)

    asyncio.run(run())


def test_only_settled_activity_execution_records_process_completion():
    async def run():
        h, owners, services = await setup()
        try:
            value = activity()
            value["state"] = "completed"
            item = h.core.life.activities.save("actor:a", value, expected=0)
            assert not sources(h)  # A caller's completion label is not execution evidence.
            item["step_operation"] = "step:actual"
            h.core.store.put("life_activities", item)
            h.core.store.put("metadata", dict(id="step:actual", state="unknown"))
            h.core.actor_memory.actions.activity(item)
            assert not sources(h)
            h.core.store.put("metadata", dict(id="step:actual", state="completed"))
            with h.core.store.transaction():
                h.core.actor_memory.actions.activity(item)
            assert len(sources(h)) == 1
            assert item["title"] in sources(h)[0]["fact"]["summary"]
        finally:
            await close(h, services)

    asyncio.run(run())


def test_known_pre_submit_failure_retries_but_uncertain_step_is_preserved():
    async def run():
        h, owners, services = await setup()
        try:
            item = h.core.life.activities.save("actor:a", activity(), expected=0)
            key = "activity-step:" + digest([item["id"], item["version"]])
            h.core.store.put("metadata", dict(id=key, state="pending"))
            h.core.life.activities._interrupt(item, "dependency_unavailable")
            paused = h.core.life.activities.get("actor:a", item["id"])
            assert paused["next_due_at"] == h.clock() + 60
            assert h.core.store.get("metadata", key)["state"] == "failed"
            resumed = h.core.life.activities.transition(
                "actor:a", item["id"], "running", expected=paused["version"]
            )
            key = "activity-step:" + digest([resumed["id"], resumed["version"]])
            h.core.store.put("metadata", dict(id=key, state="submitted"))
            h.core.life.activities._interrupt(resumed, "interrupted_step")
            paused = h.core.life.activities.get("actor:a", item["id"])
            assert paused["next_due_at"] is None
            assert h.core.store.get("metadata", key)["state"] == "unknown"
        finally:
            await close(h, services)

    asyncio.run(run())


def test_owner_maintenance_reconciles_lost_ack_without_new_command():
    async def run():
        h, owners, services = await setup()
        try:
            owners.lost_ack = True
            assert await h.core.actor_memory.maintain("actor:a")
            marker = h.core.store.get("metadata", "actor-maintenance:actor:a")
            assert marker["state"] == "unknown"
            first = marker["request"]
            assert not await h.core.actor_memory.maintain("actor:a")
            h.clock.now += 61
            assert await h.core.actor_memory.maintain("actor:a")
            marker = h.core.store.get("metadata", "actor-maintenance:actor:a")
            assert marker["state"] == "completed" and marker["request"] == first
            assert [item["operation"] for item in owners.memory_calls].count("maintain") == 1
        finally:
            await close(h, services)

    asyncio.run(run())


def test_generated_revision_action_excludes_human_edit_and_creative_body():
    async def run():
        h, owners, services = await setup()
        try:
            work = h.core.writing.create_work(
                "work:one", "actor:a", title="Story", outline="Fiction", characters=[]
            )
            chapter = h.core.writing.add_chapter(
                "chapter:one", work["id"], title="Beginning", goal="Fiction"
            )
            body = "This fictional dragon never happened in reality."
            h.core.writing.revise_chapter(
                chapter["id"], body, editor="human", reason="manual", expected=chapter["version"]
            )
            assert not sources(h)
            chapter = h.core.writing._get("chapters", chapter["id"])
            with h.core.store.transaction():
                h.core.writing._revision(
                    chapter,
                    body,
                    dict(kind="gateway", receipt={"isolated": True}),
                    chapter["basis"],
                )
            assert len(sources(h)) == 1 and body not in str(sources(h))
            assert sources(h)[0]["fact"]["content_reads"] == []
            original = sources(h)[0]
            await h.core.actor_memory.actions.check(original)
            work["title"] = "Renamed creative work"
            work["scope"] = dict(
                actor_id="actor:a",
                person_id="person:a",
                audience="self_private",
                conversation_id="private:a",
            )
            h.core.store.put("write_works", work)
            with pytest.raises(Fault):
                await h.core.actor_memory.actions.check(original)
            revision = h.core.store.get("write_revisions", original["action_owner"]["object_id"])
            with h.core.store.transaction():
                h.core.actor_memory.actions.writing(work, revision)
            changed = next(row for row in sources(h) if row["source"] != original["source"])
            assert changed["source"]["object_id"] == original["source"]["object_id"]
            assert changed["source"]["revision"] != original["source"]["revision"]
            assert changed["fact"]["source_constraints"]["kind"] == "private"
            await h.core.actor_memory.actions.check(changed)
            result = h.core.actor_memory.actions.import_history(
                "actor:a", dict(owner="writing", after=None, limit=1), "writing:import"
            )
            assert result["imported"] == 0
        finally:
            await close(h, services)

    asyncio.run(run())


def test_autonomy_projection_and_explicit_import_use_existing_authorized_http():
    async def run():
        h, owners, services = await setup()
        try:
            h.core.life.set_diary_access("actor:a", readers=["reader:operator"])
            app = create_app(
                h.core,
                {"platform": "test-token"},
                life_readers={"platform": dict(reader_id="reader:operator", actor_ids=["actor:a"])},
            )
            actor = h.core.store.get("life_actors", "actor:a")
            actor["autonomous"] = True
            h.core.store.put("life_actors", actor)
            row = h.core.life.activities.save("actor:a", activity(), expected=0)
            row.update(
                actor_only=True,
                step_operation="step:unknown",
                resume_condition="Existing true reason",
            )
            h.core.store.put("life_activities", row)
            h.core.store.put(
                "metadata", dict(id="step:unknown", state="unknown", error="result_unknown")
            )
            h.core.store.put("image_jobs", image(h))
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                headers={"Authorization": "Bearer test-token"},
            ) as client:
                query = platform_query(h)
                request = dict(
                    schema_version=2,
                    query=query,
                    actor_id="actor:a",
                    resource="autonomy",
                    object_id=None,
                    expected_version=None,
                    limit=10,
                    after=None,
                    scope=None,
                )
                result = await client.post("/internal/v2/life/read", json=request)
                assert result.status_code == 200, result.text
                view = result.json()["items"][0]
                assert view["state"] == "unknown" and view["step_state"] == "unknown"
                assert view["current_step"] == row["checkpoint"] and view["title"] == row["title"]
                request["actor_id"] = "actor:b"
                denied = await client.post("/internal/v2/life/read", json=request)
                assert denied.status_code in {403, 404}
                batch = dict(
                    schema_version=2,
                    request_id="import:http",
                    actor_id="actor:a",
                    operation="actor-memory.import",
                    expected_version=0,
                    value=dict(owner="images", after=None, limit=1),
                )
                imported = await client.post("/internal/v2/life/manage", json=batch)
                assert imported.status_code == 200, imported.text
                assert imported.json()["result"]["imported"] == 1
                replay = await client.post("/internal/v2/life/manage", json=batch)
                assert replay.json() == imported.json()
        finally:
            await close(h, services)

    asyncio.run(run())


def test_long_term_interest_is_separate_from_query_and_withdrawal_blocks_cached_plan():
    async def run():
        h, owners, services = await setup()
        try:
            await read(h)
            base = material(h, owners)
            owners.material = [base]
            owners.interests = [
                dict(
                    base,
                    ref=dict(kind="derived", id="preference:one", revision=1),
                    kind="preference",
                    text="A supported enduring interest",
                    topic="astronomy",
                    stance="curious",
                    support_count=1,
                    history=[],
                    history_omitted=False,
                )
            ]
            proof = await h.core.actor_memory.planning_query(
                "actor:a",
                dict(purpose="plan", audience=dict(kind="actor_only")),
                "a different current task",
            )
            assert proof["items"][0]["topic"] == "astronomy"
            assert proof["items"][0]["support_count"] == 1
            owners.withdrawn = True
            with pytest.raises(Fault):
                await h.core.actor_memory.check(proof)
            assert not h.memory.commits
        finally:
            await close(h, services)

    asyncio.run(run())
