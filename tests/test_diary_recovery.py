"""PC-06: bounded complete sources and machine-readable diary failures."""

import asyncio
import copy

from test_diary_sources import Memory
from test_life import setup
from tianshu_companion.actor_memory import ActorMemory
from tianshu_companion.contracts import Fault, canonical
from tianshu_companion.diary_materials import MATERIAL_BUDGET_BYTES


def test_nonempty_incomplete_sources_cannot_generate_even_after_explicit_retry():
    async def run():
        life, _, model = setup()
        memory = life.actor_memory = Memory(life)
        query = memory.query

        async def omitted(*args, **kwargs):
            page = await query(*args, **kwargs)
            return dict(page, complete=False, omitted=True)

        memory.query = omitted
        try:
            prepared = await life.diary_sources.prepare("a", "2026-09-13")
            assert prepared["state"] == "material_incomplete"
            assert prepared["status_reason"]["code"] == "source_omitted"
            row = life.store.get("life_diaries", prepared["id"])
            # Old versions could queue these rows. The worker must also guard them.
            row["state"] = "queued"
            life.store.put("life_diaries", row)
            await life._diary_work("a")
            assert life.diary_metadata(row["id"])["state"] == "material_incomplete"
            assert not model.calls and not life.store.list("life_revisions")
        finally:
            life.store.close()

    asyncio.run(run())


def test_diary_material_ceiling_retains_proofs_without_claiming_a_complete_day():
    async def run():
        life, _, model = setup()
        memory = life.actor_memory = Memory(life)
        queries, checked = [], []

        async def page(actor, context, **options):
            index = len(queries)
            items = []
            for position in range(6):
                item = copy.deepcopy(memory.item)
                item["ref"]["id"] = f"episode:{index}:{position}"
                item["episode_refs"][0]["episode_id"] = item["ref"]["id"]
                item["fact"]["summary"] = "材" * 1300  # UTF-8 bytes, within source limits.
                items.append(item)
            result = dict(
                actor_id=actor,
                role_version=7,
                context=context,
                items=items,
                omitted=False,
                complete=False,
                next_cursor=f"cursor:{index + 1}",
            )
            assert len(canonical(items).encode()) <= options["budget_bytes"]
            queries.append(copy.deepcopy(result))
            return result

        async def check(value):
            checked.append(copy.deepcopy(value))

        memory.query, memory.check = page, check
        try:
            prepared = await life.diary_sources.prepare("a", "2026-09-13")
            row = life.store.get("life_diaries", prepared["id"])
            material = life.store.get("life_materials", prepared["id"])["material"]
            assert row["state"] == "material_incomplete"
            assert row["status_reason"]["code"] == "material_budget_exceeded"
            assert not material["provenance"]["selection_complete"]
            assert len(canonical(material).encode()) <= MATERIAL_BUDGET_BYTES
            assert row["source_capture"]["proofs"] == queries
            assert all(page in checked for page in queries)
            assert not model.calls
        finally:
            life.store.close()

    asyncio.run(run())


def test_unavailable_and_failed_diaries_explain_the_original_stage_without_error_text():
    async def run():
        life, _, model = setup(writing=False)
        life.actor_memory = Memory(life)
        try:
            prepared = await life.diary_sources.prepare("a", "2026-09-13")
            assert prepared["state"] == "unavailable"
            assert prepared["status_reason"]["code"] == "writing_disabled"
            life.writing = True
            life.retry_diary(prepared["id"], expected=prepared["version"])

            async def fail(turn, messages):
                raise Fault(
                    "forbidden",
                    cause=dict(
                        peer="gateway",
                        http_status=403,
                        response_body="SECRET_CANARY",
                    ),
                )

            model.generate = fail
            await life._diary_work("a")
            failed = life.diary_metadata(prepared["id"])
            assert failed["state"] == "failed"
            assert failed["last_failure"]["code"] == "forbidden"
            assert failed["last_failure"]["stage"] == "generation"
            assert failed["last_failure"]["http_status"] == 403
            assert "status_reason" not in failed
            assert "SECRET_CANARY" not in canonical(life.store.get("life_diaries", prepared["id"]))
        finally:
            life.store.close()

    asyncio.run(run())


def test_timeline_keeps_the_owner_fact_once_and_rechecks_its_original_refs():
    async def run():
        owner = object.__new__(ActorMemory)
        item = dict(
            ref=dict(kind="episode", id="episode:one", revision=3),
            episode_refs=[dict(episode_id="episode:one", expected_revision=3)],
            text="原" * 3900,
            fact=dict(summary="原" * 3900),
        )
        calls, checks = [], []
        context = dict(purpose="diary", audience=dict(kind="actor_only"))

        async def version(actor):
            return 7

        async def memory(body):
            calls.append(body)
            assert body["operation"] == "query"  # No duplicate get_episode hydration.
            return dict(
                actor_id="a",
                role_version=7,
                context=context,
                items=[item],
                mode="timeline",
                window=body["window"],
                complete=True,
                omitted=False,
                next_cursor=None,
            )

        async def check(page):
            checks.append(copy.deepcopy(page))

        owner.role_version, owner._memory, owner.check = version, memory, check
        result = await owner._query(
            "a",
            context,
            None,
            kinds=None,
            limit=16,
            budget_bytes=32768,
            window=dict(start="2026-09-13T00:00:00Z", end="2026-09-14T00:00:00Z"),
        )
        assert result["items"] == [item] and result["complete"] and not result["omitted"]
        assert len(canonical(result["items"]).encode()) < 32768
        assert len(calls) == 1 and checks == [result]

    asyncio.run(run())


def test_validated_missing_receipts_do_not_open_a_service_failure_circuit():
    from test_actor_memory import close, setup as actor_setup
    from tianshu_companion.background_peers import background_calls

    async def run():
        h, owners, services = await actor_setup()
        try:
            owner = h.core.actor_memory
            request = dict(
                actor_id="actor:a",
                operation="append_episode",
                idempotency_key="receipt:missing",
                request_id="req:original",
            )
            with background_calls():
                for _ in range(5):
                    assert await owner._receipt(request, owners.version) is None
            assert len(owners.memory_calls) == 5
            assert services[1].background_peers.snapshot() == []
        finally:
            await close(h, services)

    asyncio.run(run())
