"""Opt-in real Memory/Companion consumer joint; HTTPS owner fixtures are synthetic.

TS307_MEMORY_V2_REPO explicitly selects the coordinator's candidate. This test
imports it read-only and uses temporary SQLite plus real source authority; it
does not call a model, production database, device, or live account.
"""

import asyncio
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

import httpx
import pytest

from tianshu_companion.clients import JsonService, Memory
from tianshu_companion.contracts import Contracts as ConsumerContracts, Fault
from tianshu_companion.memory_context import legacy_check

if not os.environ.get("TS307_MEMORY_V2_REPO"):
    pytest.skip("requires explicit Memory v2 candidate for consumer joint", allow_module_level=True)

peer = Path(os.environ["TS307_MEMORY_V2_REPO"]).resolve()
sys.dont_write_bytecode = True
sys.path[:0] = [str(peer / "src"), str(peer / "tests")]
spec = importlib.util.spec_from_file_location("q21_producer_v2", peer / "tests/test_memory_v2.py")
producer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(producer)
source_contracts = producer.source_contracts
source_examples = producer.source_examples
sync = producer.sync


@pytest.fixture
def contracts():
    from tianshu_memory.contracts import Contracts as ProducerContracts

    return ProducerContracts(os.environ["TIANSHU_CONTRACTS"])


@pytest.fixture(scope="session")
def certificates():
    # Copy the producer's unchanged tooling into our disposable checkout so
    # its own output-boundary guard stays enabled and no peer file is written.
    with tempfile.TemporaryDirectory(
        prefix="q21-joint-cert-", dir=Path(__file__).resolve().parents[1] / ".runtime"
    ) as directory:
        root = Path(directory)
        (root / "tests").mkdir()
        script = root / "tests/tls_certificates.py"
        shutil.copyfile(peer / "tests/tls_certificates.py", script)
        target = root / ".runtime"
        subprocess.run(
            [sys.executable, str(script), str(target)],
            check=True,
            capture_output=True,
            timeout=30,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        yield target


def client(h, *, transport=None):
    wire = JsonService(
        "https://synthetic-memory.invalid",
        "test-only-companion-secret",
        transport=transport or httpx.ASGITransport(app=h.client.app),
    )
    return Memory(ConsumerContracts(os.environ["TIANSHU_CONTRACTS"]), wire), wire


def test_preparation_catalog_uses_one_real_barrier_and_keeps_failure_components(sync):
    from tianshu_companion.clients import query

    producer.setup(sync)

    async def run():
        memory, wire = client(sync)
        calls = []
        authority = sync.service.source_authority
        original = authority.barrier

        def barrier(*args, **kwargs):
            calls.append(True)
            return original(*args, **kwargs)

        authority.barrier = barrier
        try:
            origin, scope = sync.command()["origin"], sync.scope()
            selected = await memory.query_context(
                origin, scope, "松林", dict(tokens=8192, bytes=8192)
            )
            current = dict(
                version_domain="text-input/v1",
                origin=origin,
                scope=scope,
                history_snapshot=selected["bundle"]["input_history_snapshot"],
            )
            valid = legacy_check(current, query(origin))
            wrong = copy.deepcopy(current)
            wrong["version_domain"] = "text-dialogue/v1"
            wrong["history_snapshot"]["history_checks"][0]["history_version"] += 1
            rejected = legacy_check(wrong, query(origin))
            before = len(calls)
            result = await memory.prepare_context(
                origin, scope, selected["bundle"], [rejected, valid], [valid["check_id"]]
            )
            assert result == {rejected["check_id"]: "rejected", valid["check_id"]: "valid"}
            assert len(calls) == before + 1
            with pytest.raises(Fault) as required:
                await memory.prepare_context(
                    origin, scope, selected["bundle"], [rejected], [rejected["check_id"]]
                )
            assert required.value.code == "scope_changed"
            assert required.value.cause["http_status"] == 200
            assert not getattr(required.value, "memory_selection_invalid", False)
            requests = [
                dict(
                    query=query(origin),
                    requester_scope=scope,
                    target=dict(kind="person", person_id=scope["person_id"]),
                    query_text="松林",
                    selection=["interest"],
                    known_scope_version=None,
                    budget=dict(tokens=4096, bytes=4096),
                )
                for _ in range(4)
            ]
            before = len(calls)
            profiles = await memory.profiles_batch(origin, scope, requests)
            assert len(profiles) == 4 and all(not r["selected_units"] for r in profiles)
            assert len(calls) == before + 1

            # The real producer must isolate an unproved historical origin while
            # retaining the complete main/required source barrier. revoked:true
            # is deliberately invalid schema, never a forged valid denial.
            optional_ref = "synthetic:optional-origin"
            context = copy.deepcopy(sync.contexts[origin["assertion_ref"]])
            context["assertion_ref"] = optional_ref
            sync.contexts[optional_ref] = context
            optional = [
                legacy_check(
                    dict(current, origin=dict(assertion_ref=optional_ref), version_domain=domain),
                    query(dict(assertion_ref=optional_ref)),
                )
                for domain in ("text-input/v1", "text-dialogue/v1")
            ]
            mutate = sync.mutate
            try:
                for mode in ("403", "invalid_context"):

                    def unproved(kind, body, value):
                        mutate(kind, body, value)
                        if kind == "origin":
                            bad = body["assertion_ref"] == optional_ref
                            sync.status["origin"] = 403 if bad and mode == "403" else 200
                            if bad and mode == "invalid_context":
                                value["context"]["revoked"] = True

                    sync.mutate = unproved
                    before = len(calls)
                    result = await memory.prepare_context(
                        origin, scope, selected["bundle"], [*optional, valid], [valid["check_id"]]
                    )
                    assert result == {
                        **{item["check_id"]: "unavailable" for item in optional},
                        valid["check_id"]: "valid",
                    }
                    assert len(calls) == before + 1
                    with pytest.raises(Fault) as necessary:
                        await memory.prepare_context(
                            origin, scope, selected["bundle"], optional, [optional[-1]["check_id"]]
                        )
                    assert not getattr(necessary.value, "memory_selection_invalid", False)
            finally:
                sync.mutate = mutate
                sync.status.pop("origin", None)
            producer.candidate(sync, "我打算周五去松林。", 1)
            with pytest.raises(Fault) as changed:
                await memory.prepare_context(origin, scope, selected["bundle"], [], [])
            assert changed.value.memory_selection_invalid
            assert changed.value.cause["http_status"] == 200
        finally:
            authority.barrier = original
            await wire.close()

    asyncio.run(run())


def test_episode_capture_between_query_and_verify_requires_complete_reselection(sync):
    from tianshu_companion.preparation_retry import retry_preparation, verify_prepared_memory

    producer.setup(sync)

    async def run():
        memory, wire = client(sync)
        try:
            origin, scope = sync.command()["origin"], sync.scope()
            old = await memory.query_context(origin, scope, "松林", dict(tokens=8192, bytes=8192))
            # This real candidate read captures a new episode before any model
            # output/commit. The unchanged old selection must now fail fresh verification.
            producer.candidate(sync, "我打算周五去松林。", 1)
            turn = dict(
                origin=origin,
                scope=scope,
                memory_context=old,
                preparation={"old": True},
                model_calls=0,
                source_deadline=time.time() + 60,
            )
            with pytest.raises(Fault) as caught:
                await verify_prepared_memory(memory, turn, [])
            assert caught.value.code == "scope_changed" and caught.value.memory_selection_invalid
            assert retry_preparation(turn, caught.value, time.time())
            assert turn["memory_context"] is None and turn["preparation"] is None
            fresh = await memory.query_context(origin, scope, "松林", dict(tokens=8192, bytes=8192))
            assert fresh["bundle"]["bundle_id"] != old["bundle"]["bundle_id"]
            assert any(item["kind"] == "episode" for item in fresh["evidence"])
            turn["memory_context"] = fresh
            await verify_prepared_memory(memory, turn, [])
        finally:
            await wire.close()

    asyncio.run(run())


def test_episode_capture_during_query_ranking_returns_to_complete_preparation(sync):
    from test_retrieval import Backend
    from tianshu_companion.preparation_retry import retry_preparation, verify_prepared_memory
    from tianshu_memory.memory_reindex import MemoryReindexer
    from tianshu_memory.retrieval import Retrieval

    producer.setup(sync)
    producer.candidate(sync, "我喜欢咖啡。", 1)
    backend = Backend("hybrid")
    sync.service.retrieval = Retrieval(backend)
    sync.service.retrieval.store = sync.store
    assert MemoryReindexer(sync.service).index_step()["indexed"] == 1

    def capture_during_ranking():
        # This runs after query releases its first source transaction and before
        # the second full barrier. A candidate read alone captures the new episode.
        producer.candidate(sync, "我带咖啡去松林。", 2)

    backend.hook = capture_during_ranking
    backend.calls.clear()

    async def run():
        memory, wire = client(sync)
        try:
            origin, scope = sync.command()["origin"], sync.scope()
            turn = dict(
                origin=origin,
                scope=scope,
                preparation=None,
                memory_context=None,
                model_calls=0,
                source_deadline=time.time() + 60,
            )
            with pytest.raises(Fault) as caught:
                await memory.query_context(origin, scope, "咖啡", dict(tokens=8192, bytes=8192))
            failure = caught.value
            assert failure.code == "scope_changed" and failure.cause["http_status"] == 409
            assert failure.read_execution == "not_started" and not failure.unknown
            assert failure.memory_selection_invalid and not failure.retryable_read
            assert any(operation == "embed" for operation, _ in backend.calls)
            assert retry_preparation(turn, failure, time.time())
            assert turn["memory_context"] is None and turn["preparation"] is None
            fresh = await memory.query_context(origin, scope, "咖啡", dict(tokens=8192, bytes=8192))
            assert {row["text"] for row in fresh["evidence"] if row["kind"] == "episode"} == {
                "我喜欢咖啡。",
                "我带咖啡去松林。",
            }
            turn["memory_context"] = fresh
            await verify_prepared_memory(memory, turn, [])
        finally:
            await wire.close()

    asyncio.run(run())


def test_candidate_owner_defers_real_episode_capture_until_dialogue_boundary(sync):
    from support import Harness
    from test_candidate_extraction import TEXT, prepare
    from tianshu_companion.contracts import digest

    producer.setup(sync)
    job = prepare(sync)  # A real accepted job, before its first content/capture read.

    async def run():
        memory, wire = client(sync)
        h = Harness(silence_ms=0)
        calls = []
        actual = memory.process_candidate

        async def process(job_id, output, **kwargs):
            calls.append(copy.deepcopy(output))
            return await actual(job_id, output, **kwargs)

        async def empty_proposal(turn, messages):
            return ['{"memories":[]}'], {}

        memory.process_candidate = process
        h.core.memory = memory
        h.core.gateway.generate = empty_proposal
        scope = sync.scope()
        physical, admission = copy.deepcopy(sync.physicals[0]), copy.deepcopy(sync.admissions[0])
        message = dict(
            message_key=dict(physical["key"], revision=physical["revision"]),
            source=admission["source"],
        )
        # Both sides use these same explicit synthetic admitted source facts.
        # Core's real source-current and active-scope checks remain enabled.
        h.core.store.put(
            "physicals",
            dict(
                id=digest(message["message_key"]),
                conversation_id=scope["conversation_id"],
                base=digest(physical["key"]),
                revision=physical["revision"],
                fact=physical,
            ),
        )
        h.core.store.put(
            "admissions",
            dict(
                id=digest(admission["selector"]),
                conversation_id=scope["conversation_id"],
                fact=admission,
            ),
        )
        turn = dict(
            id="turn:joint-source",
            scope=scope,
            conversation_id=scope["conversation_id"],
            role=h.core.roles[scope["actor_id"]],
            phase="sent",
            cancelled=False,
            bundle=dict(messages=[message], collection_revision=1),
            config_version=1,
        )
        h.core.store.put("turns", turn)
        active = dict(turn, id="turn:joint-active", phase="generating")
        h.core.store.put("turns", active)
        item = dict(
            id="memory-learn:" + digest(job),
            conversation_id="memory:learning",
            job_id=job,
            turn_id=turn["id"],
            scope=scope,
            output=None,
            attempts=0,
            proposal_failures=0,
            recovery_failures={},
            extraction_schema=4,
            state="pending",
            deadline=0,
        )
        h.core.store.put("metadata", item)
        worker = h.core.memory_learning
        try:
            origin = sync.command()["origin"]
            before = await memory.query_context(
                origin, scope, "咖啡", dict(tokens=8192, bytes=8192)
            )
            assert before["evidence"] == []
            await worker._work(item)
            waiting = h.core.store.get("metadata", item["id"])
            assert waiting["stage"] == "capture_wait" and waiting["attempts"] == 0
            assert calls == [] and worker.admission_blocked(scope)
            await memory.verify_context(origin, scope, before["bundle"])
            active["phase"] = "sent"
            h.core.store.put("turns", active)
            await worker._work(waiting)
            completed = h.core.store.get("metadata", item["id"])
            assert completed["state"] == "completed" and completed["attempts"] == 1
            assert calls == [None, {"memories": []}]
            assert not worker.admission_blocked(scope)
            with pytest.raises(Fault) as changed:
                await memory.verify_context(origin, scope, before["bundle"])
            assert changed.value.code == "scope_changed"
            fresh = await memory.query_context(origin, scope, "咖啡", dict(tokens=8192, bytes=8192))
            assert [row["text"] for row in fresh["evidence"] if row["kind"] == "episode"] == [TEXT]
            assert (await worker.reconcile(job))["state"] == "completed"
            await memory.verify_context(origin, scope, fresh["bundle"])
            with sync.store.transaction() as db:
                assert db.execute("SELECT count(*) FROM memory_documents").fetchone()[0] == 1
        finally:
            await h.core.close()
            await wire.close()

    asyncio.run(run())


def test_real_producer_typed_read_and_combined_legacy_verification(sync):
    producer.setup(sync)
    text = "我喜欢乌龙茶。"
    job, _ = producer.candidate(sync, text)

    async def run():
        memory, wire = client(sync)
        try:
            pending = await memory.process_candidate(job, None, extraction_schema=4)
            assert pending["extraction_schema"] == 4
            output = dict(
                memories=[
                    dict(
                        conditions=[],
                        negations=[],
                        valid_time="当前",
                        evidence=[dict(source_index=0, quote=text)],
                        fact=producer.fact("乌龙茶"),
                    )
                ]
            )
            assert (await memory.process_candidate(job, output, extraction_schema=4))[
                "state"
            ] == "completed"
            origin, scope = sync.command()["origin"], sync.scope()
            result = await memory.query_context(
                origin, scope, "我喜欢什么茶", dict(tokens=8192, bytes=8192)
            )
            assert any(row.get("fact", {}).get("value") == "乌龙茶" for row in result["evidence"])
            assert any(
                row["kind"] == "episode" and row["text"] == text for row in result["evidence"]
            )
            base = dict(
                scope=scope,
                origin=origin,
                scope_version=result["bundle"]["scope_version"],
                memory_snapshot={
                    k: result["bundle"][k] for k in ("association_version", "scope_checks")
                },
            )
            header = {k: sync.command()[k] for k in ("schema_version", "request_id", "origin")}
            checks = [
                legacy_check(
                    dict(base, version_domain="text-dialogue/v1", history_snapshot=None), header
                )
            ]
            for kind in ("history", "input_history"):
                snapshot = await getattr(memory, kind)(origin, scope)
                checks.append(
                    legacy_check(
                        dict(
                            base,
                            version_domain="text-input/v1"
                            if kind == "input_history"
                            else "text-dialogue/v1",
                            history_snapshot=snapshot,
                        ),
                        header,
                    )
                )
            profile = await memory.profiles(
                origin,
                scope,
                dict(kind="person", person_id=scope["person_id"]),
                "茶",
                ["interest"],
                dict(tokens=0, bytes=0),
            )
            checks.append(
                legacy_check(
                    dict(
                        base,
                        version_domain="profile-memory/v1",
                        target=profile["target"],
                        text="茶",
                        selection=["interest"],
                        scope_version=profile["scope_version"],
                    ),
                    header,
                )
            )
            sync.calls.clear()
            assert (await memory.verify_context(origin, scope, result["bundle"], checks))[
                "state"
            ] == "valid"
            assert [kind for kind, _, _ in sync.calls if kind != "origin"] == [
                "snapshot",
                "current",
                "head",
            ]
        finally:
            await wire.close()

    asyncio.run(run())


def test_real_producer_episode_only_withdrawal_rejects_consumer_bundle(sync):
    producer.setup(sync)
    text = "明天可能去松林，随身带雨伞。" * 30
    job, _ = producer.candidate(sync, text)
    producer.commit(sync, job, text)

    async def run():
        memory, wire = client(sync)
        try:
            origin, scope = sync.command()["origin"], sync.scope()
            result = await memory.query_context(
                origin, scope, "我明天去松林带什么", dict(tokens=8192, bytes=8192)
            )
            assert any(row["kind"] == "episode" for row in result["evidence"])
            sync.grants[-1]["state"] = "denied"
            sync.platform_head["sequence"] += 1
            with pytest.raises(Fault) as failure:
                await memory.verify_context(origin, scope, result["bundle"])
            assert failure.value.code == "scope_changed"
            with sync.store.transaction() as db:
                assert db.execute("SELECT count(*) FROM records").fetchone()[0] == 0
                assert db.execute("SELECT count(*) FROM memory_documents").fetchone()[0] == 0
                assert db.execute("SELECT count(*) FROM memory_episode_index").fetchone()[0] == 0
        finally:
            await wire.close()

    asyncio.run(run())


def test_real_producer_lost_commit_ack_recovers_same_typed_job_once(sync):
    producer.setup(sync)
    text = "我喜欢绿茶。"
    job, _ = producer.candidate(sync, text)

    class LostAck(httpx.AsyncBaseTransport):
        def __init__(self):
            self.inner = httpx.ASGITransport(app=sync.client.app)
            self.lost = False

        async def handle_async_request(self, request):
            response = await self.inner.handle_async_request(request)
            if (
                request.url.path.endswith("candidates/process")
                and json.loads(request.content)["output"] is not None
                and not self.lost
            ):
                self.lost = True
                await response.aread()
                await response.aclose()
                raise httpx.ReadError("synthetic lost committed acknowledgement")
            return response

        async def aclose(self):
            await self.inner.aclose()

    async def run():
        memory, wire = client(sync, transport=LostAck())
        try:
            output = dict(
                memories=[
                    dict(
                        conditions=[],
                        negations=[],
                        valid_time="当前",
                        evidence=[dict(source_index=0, quote=text)],
                        fact=producer.fact("绿茶"),
                    )
                ]
            )
            with pytest.raises(Fault) as failure:
                await memory.process_candidate(job, output, extraction_schema=4)
            assert failure.value.unknown
            receipt = await memory.process_candidate(job, None, extraction_schema=4)
            assert receipt["state"] == "completed" and receipt["extraction_schema"] == 4
            with sync.store.transaction() as db:
                assert (
                    db.execute(
                        "SELECT count(*) FROM write_ledger WHERE job_id=?", (job,)
                    ).fetchone()[0]
                    == 1
                )
        finally:
            await wire.close()

    asyncio.run(run())


def test_real_raw_source_proof_survives_unrelated_foreign_history_withdrawal(sync):
    from test_group_context import owner

    if sync.scope()["audience"] != "group":
        pytest.skip("Foreign group-history domain requires the group audience")
    producer.setup(sync)
    other = owner(sync)
    text = "我把随身杯放在桌上。"
    job, _ = producer.candidate(sync, text, serial=1)
    producer.commit(sync, job, text)
    original = copy.deepcopy(sync.turns[-1])
    foreign_text = "我喜欢乌龙茶。"
    foreign, _ = producer.candidate(sync, foreign_text, serial=2, actor=other)
    producer.commit(sync, foreign, foreign_text, producer.fact("乌龙茶"))
    foreign_physical = sync.physicals[-1]
    captured = dict(
        id=original["turn_id"],
        scope=original["scope"],
        bundle=dict(collection_revision=original["input_revision"]),
    )
    sources = copy.deepcopy(original["input_sources"])

    async def run():
        memory, wire = client(sync)
        try:
            origin, scope = sync.command()["origin"], sync.scope()
            selected = await memory.query_context(
                origin, scope, "群里谁喜欢乌龙茶", dict(tokens=8192, bytes=8192)
            )
            old = selected["bundle"]["input_history_snapshot"]
            assert sync.scope(other) in [entry["scope"] for entry in old["history_checks"]]
            assert await memory.check_sources(captured, sources) > 0

            # Withdraw only the other owner's original source. Its old complete
            # history is no longer a proof, while this human's raw input is.
            foreign_physical.update(
                state="withdrawn",
                kind="retract",
                revision=2,
                physical_receipt_id="physical:foreign-withdrawn",
                content_digest="c" * 64,
            )
            sync.core_head["sequence"] += 1
            with pytest.raises(Fault):
                await memory.input_history(origin, scope, known=old)
            assert await memory.check_sources(captured, sources) > 0
            assert sources == original["input_sources"]
            fresh = await memory.query_context(
                origin, scope, "随身杯放在哪里", dict(tokens=8192, bytes=8192)
            )
            assert await memory.prepare_context(origin, scope, fresh["bundle"], [], []) == {}
            # These are independently real source-sync and bundle proofs, not
            # a fabricated fifth wire kind or a single common database snapshot.
        finally:
            await wire.close()

    asyncio.run(run())


def test_real_producer_forget_episode_uses_receipt_and_blocks_reindex(sync):
    from support import Harness
    from test_memory_context import ProofIssuer
    from tianshu_companion.clients import epoch, utc
    from tianshu_companion.context import scope_check
    from tianshu_companion.context_preparation import _v2_variants
    from tianshu_companion.contracts import digest
    from tianshu_memory.memory_reindex import MemoryReindexer

    producer.setup(sync)
    text = "明天去松林随身带雨伞。" * 30
    job, _ = producer.candidate(sync, text)
    producer.commit(sync, job, text)
    original = copy.deepcopy(sync.turns[-1])
    captured = dict(
        id=original["turn_id"],
        scope=original["scope"],
        bundle=dict(collection_revision=original["input_revision"]),
    )
    sources = copy.deepcopy(original["input_sources"])

    async def run():
        memory, wire = client(sync)
        h = Harness(silence_ms=0)
        try:
            origin, scope = sync.command()["origin"], sync.scope()
            read = await memory.query_context(
                origin, scope, "我去松林带什么", dict(tokens=8192, bytes=8192)
            )
            episode = next(row for row in read["evidence"] if row["kind"] == "episode")
            target_source = read["bundle"]["source_dependencies"][0]["source_key"]
            assert await memory.check_sources(captured, sources) > 0
            request_text = "忘掉之前关于松林的原话。"
            request_job, _ = producer.candidate(sync, request_text, serial=2)
            producer.commit(sync, request_job, request_text)
            request = dict(
                command=sync.command(),
                scope=scope,
                batch_ref=None,
                item_id="forget:consumer",
                kind="forget_episode",
                target=dict(
                    episode_ref=episode["episode_id"], episode_version=episode["episode_version"]
                ),
                evidence_refs=[sync.admissions[-1]["source"]],
                proof_ref="proof:consumer",
            )
            # The approval issuer is explicitly synthetic in this component
            # joint; TS311 separately verifies the real Platform issuer rules.
            proofs = ProofIssuer()
            sync.service.memory_context.proofs = proofs
            proofs.issue(request, "revision", [sync.account], [scope])
            receipt = await memory.forget_episode(request)
            assert receipt["state"] == "tombstoned" and receipt["record_ids"] == []
            replay = await memory.forget_episode(request)
            assert replay == receipt
            restored = await memory.receipt(origin, scope, request["command"]["idempotency_key"])
            assert restored == receipt
            MemoryReindexer(sync.service).step(restart=True)
            with sync.store.transaction() as db:
                assert (
                    db.execute(
                        "SELECT count(*) FROM memory_documents WHERE source_key=?", (target_source,)
                    ).fetchone()[0]
                    == 0
                )
                assert (
                    db.execute(
                        "SELECT count(*) FROM suppression WHERE source_key=?", (target_source,)
                    ).fetchone()[0]
                    == 1
                )
                assert db.execute("SELECT count(*) FROM records").fetchone()[0] == 0
            with pytest.raises(Fault):
                await memory.verify_context(origin, scope, read["bundle"])
            # Memory's already committed source suppression must reject raw
            # reuse even though the original Core physical is still active.
            assert any(
                physical["state"] == "active"
                and physical["key"]
                == {
                    key: value
                    for key, value in sources[0]["message_key"].items()
                    if key != "revision"
                }
                for physical in sync.physicals
            )
            with pytest.raises(Fault) as negative:
                await memory.check_sources(captured, sources)
            assert negative.value.code == "version_conflict"
            assert negative.value.source_invalid and not negative.value.unknown
            assert negative.value.cause["http_status"] == 409

            # The real producer's known negative must also be usable by the
            # consumer's actual catalog planner. As in the existing candidate
            # owner joint, seed the same admitted facts on both sides rather
            # than bypassing Core's real sources_current/_context_source guard.
            h.core.memory = memory
            _, binding = await memory.identity(origin, sync.account, h.clock())
            channel = sources[0]["message_key"]["channel"]
            h.core.bindings[channel["binding_id"]] = dict(
                service="nonebot",
                namespace=channel["namespace"],
                audience=scope["audience"],
                actor_ids=[scope["actor_id"]],
            )
            h.origins.values[origin["assertion_ref"]] = dict(
                authenticated_service="nonebot",
                verified_account=sync.account,
                verified_channel=channel,
                allowed_scope=scope,
                expires_at=utc(h.clock() + 3600),
                revoked=False,
            )
            origin_deadline = epoch(h.origins.values[origin["assertion_ref"]]["expires_at"])

            def seed(source, content):
                messages = []
                for value in source["input_sources"]:
                    physical = next(
                        row
                        for row in sync.physicals
                        if dict(row["key"], revision=row["revision"]) == value["message_key"]
                    )
                    admission = next(row for row in sync.admissions if row["source"] == value)
                    message = dict(
                        message_key=value["message_key"],
                        source=value,
                        author=sync.account,
                        person_id=scope["person_id"],
                        parts=[dict(kind="text", text=content)],
                    )
                    messages.append(message)
                    h.core.store.put(
                        "physicals",
                        dict(
                            id=digest(message["message_key"]),
                            conversation_id=scope["conversation_id"],
                            base=digest(physical["key"]),
                            revision=physical["revision"],
                            fact=physical,
                        ),
                    )
                    h.core.store.put(
                        "admissions",
                        dict(
                            id=digest(admission["selector"]),
                            conversation_id=scope["conversation_id"],
                            fact=admission,
                        ),
                    )
                previous = dict(
                    id=source["turn_id"],
                    conversation_id=scope["conversation_id"],
                    phase="sent",
                    cancelled=False,
                    service="nonebot",
                    origin=origin,
                    scope=scope,
                    binding_version=binding,
                    scope_version=1,
                    source_deadline=origin_deadline,
                    bundle=dict(
                        collection_id="collection:" + digest(source["turn_id"]),
                        collection_revision=source["input_revision"],
                        collection_key=dict(channel=channel, author=sync.account),
                        messages=messages,
                    ),
                )
                h.core.store.put("turns", previous)
                group = dict(
                    turn_id=previous["id"],
                    actor_id=scope["actor_id"],
                    person_id=scope["person_id"],
                    author=sync.account,
                    collection_id=previous["bundle"]["collection_id"],
                    input_revision=source["input_revision"],
                    messages=messages,
                    input_only=True,
                    replies=[],
                )
                return previous, group

            forgotten, old_group = seed(original, text)
            healthy, good_group = seed(sync.turns[-1], request_text)
            forgotten["history_snapshot"] = read["bundle"]["history_snapshot"]
            h.core.store.put("turns", forgotten)
            old_group.update(
                input_only=False,
                replies=[dict(reply_id="reply:synthetic-old", text="合成已确认回复。")],
            )
            fresh = await memory.query_context(
                origin, scope, "继续一个不依赖旧原话的问题", dict(tokens=8192, bytes=8192)
            )
            current = dict(
                id="turn:joint-fresh-viewer",
                origin=origin,
                service="nonebot",
                scope=scope,
                binding_version=binding,
                source_deadline=origin_deadline,
                bundle=dict(collection_key=dict(channel=channel, author=sync.account)),
                memory_context=fresh,
            )
            assert (
                h.core._context_source(
                    scope_check(forgotten, input_only=True, raw_input=old_group)
                )["id"]
                == forgotten["id"]
            )
            assert await _v2_variants(h.core, current, [], [[old_group, good_group]]) == [[]]
            # Excluding the whole linked batch never leaves its healthy member
            # behind. That member can independently appear in a later batch.
            usable = await _v2_variants(h.core, current, [], [[good_group]])
            assert [g["turn_id"] for g in usable[0][0][0]] == [healthy["id"]]
            required = scope_check(forgotten, input_only=True, raw_input=old_group)
            with pytest.raises(Fault) as required_negative:
                await _v2_variants(h.core, current, [required], [])
            assert required_negative.value.code == "scope_changed"
            assert not getattr(required_negative.value, "memory_selection_invalid", False)
            assert required_negative.value.cause["http_status"] == 200
        finally:
            await h.core.close()
            await wire.close()

    asyncio.run(run())
