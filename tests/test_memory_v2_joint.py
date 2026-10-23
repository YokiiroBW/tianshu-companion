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
            bundle=dict(messages=[message]),
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


def test_real_producer_forget_episode_uses_receipt_and_blocks_reindex(sync):
    from test_memory_context import ProofIssuer
    from tianshu_memory.memory_reindex import MemoryReindexer

    producer.setup(sync)
    text = "明天去松林随身带雨伞。" * 30
    job, _ = producer.candidate(sync, text)
    producer.commit(sync, job, text)

    async def run():
        memory, wire = client(sync)
        try:
            origin, scope = sync.command()["origin"], sync.scope()
            read = await memory.query_context(
                origin, scope, "我去松林带什么", dict(tokens=8192, bytes=8192)
            )
            episode = next(row for row in read["evidence"] if row["kind"] == "episode")
            target_source = read["bundle"]["source_dependencies"][0]["source_key"]
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
        finally:
            await wire.close()

    asyncio.run(run())
