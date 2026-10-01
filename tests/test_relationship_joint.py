"""Actual TS-114 HTTP/SQLite + Companion Core, synthetic Platform/model/channel.

Only the pinned Memory Git archive and ephemeral loopback TLS are used. This is
local component acceptance, not QQ, NAS, or a production Platform verification.
"""

import asyncio
import copy
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import uvicorn
from support import Harness
from test_relationships import schema_path
from test_source_https import CERT_PROGRAM

from tianshu_companion.clients import JsonService, Memory, query
from tianshu_companion.contracts import canonical, digest
from tianshu_companion.relationships import assemble
from tianshu_companion.source_sync import read_facts

MEMORY_SHA = "0b82242590c35c5299721ad165bd5f5e9aa0f77e"


class OwnerTransport:
    """Actual Core facts; explicitly synthetic Platform origin/access decisions."""

    def __init__(self, fixture):
        self.fixture = fixture
        self.loop = asyncio.get_running_loop()
        self.platform_sequence = 1

    def facts(self, request):
        async def read():
            return read_facts(
                self.fixture.h.core.store, self.fixture.h.contracts, "memory", request
            )

        return asyncio.run_coroutine_threadsafe(read(), self.loop).result(timeout=5)

    def current(self, request):
        grants = []
        for admission in request["admissions"]:
            context = self.fixture.config["origins"][admission["accepted_origin"]["assertion_ref"]]
            grants.append(
                {
                    "selector": admission["selector"],
                    "admission_digest": digest(admission),
                    "entry_id": "synthetic-entry:" + digest(admission["selector"]),
                    "entry_digest": digest([admission["selector"], "synthetic-role-grant-v1"]),
                    "account": context["verified_account"],
                    "scope": admission["scope"],
                    "binding_version": admission["binding_version"],
                    "state": "revoked" if context["revoked"] else "allowed",
                }
            )
        viewer = request["viewer"]
        context = (
            copy.deepcopy(self.fixture.config["origins"][viewer["origin"]["assertion_ref"]])
            if viewer
            else None
        )
        return {
            "schema_version": 1,
            "request_id": request["request_id"],
            "request_digest": digest(request),
            "operation": "current",
            "head": {"generation": "synthetic-platform", "sequence": self.platform_sequence},
            "grants": grants,
            "viewer_context": context,
        }


class MemoryJoint:
    async def start(self):
        self.directory = tempfile.TemporaryDirectory(prefix="ts115-joint-")
        self.root = Path(self.directory.name).resolve()
        repo = Path(os.environ["TIANSHU_RELATIONSHIP_MEMORY_REPO"]).resolve()
        archive = (
            await asyncio.to_thread(
                subprocess.run,
                [
                    "git",
                    "-c",
                    f"safe.directory={repo}",
                    "-C",
                    str(repo),
                    "archive",
                    "--format=zip",
                    MEMORY_SHA,
                    "src",
                ],
                check=True,
                capture_output=True,
            )
        ).stdout
        with zipfile.ZipFile(io.BytesIO(archive)) as files:
            if any(
                not (self.root / name).resolve().is_relative_to(self.root)
                for name in files.namelist()
            ):
                raise ValueError("Invalid local archive")
            files.extractall(self.root)
        sys.path.insert(0, str(self.root / "src"))
        from tianshu_memory.app import create_app
        from tianshu_memory.auth import Authenticator
        from tianshu_memory.contracts import Contracts
        from tianshu_memory.relationships import Relationships
        from tianshu_memory.service import MemoryService
        from tianshu_memory.source_authority import SourceAuthority
        from tianshu_memory.store import Store

        module_path = Path(sys.modules["tianshu_memory.app"].__file__).resolve()
        if not module_path.is_relative_to(self.root / "src"):
            raise RuntimeError("Memory module did not come from the immutable archive")
        self.h = Harness(path=self.root / "companion.sqlite", silence_ms=0)
        contracts = Contracts(os.environ["TIANSHU_CONTRACTS"])
        contracts.load_profiles()
        contracts.load_sources()
        self.store = Store(self.root / "memory.sqlite")
        self.store.migrate_profiles(self.root / "before-profiles.sqlite")
        self.store.migrate_sources(self.root / "before-sources.sqlite", contracts)
        self.transport = OwnerTransport(self)
        self.service = MemoryService(
            self.store,
            contracts,
            source_authority=SourceAuthority(self.transport, contracts),
            clock=lambda: datetime.fromtimestamp(self.h.clock(), UTC),
        )
        self.store.migrate_relationships(
            self.root / "before-relationships.sqlite", clock=self.service.clock
        )
        self.config = {
            "mode": "local_fixture",
            "origins": {},
            "callers": {
                "companion": {
                    "token": "synthetic-ts115-joint-only",
                    "issuer": "platform",
                    "allowed_actors": ["actor:a", "actor:b"],
                    "role_admin": True,
                    "operations": [
                        "resolve",
                        "register",
                        "select",
                        "select_profiles",
                        "consume",
                        "check_sources",
                        "relationships.read",
                        "relationships.check",
                        "relationships.settle",
                        "relationships.manage",
                    ],
                    "event_scopes": [],
                }
            },
        }
        self.config_path = self.root / "synthetic-config.json"
        self.save()
        auth = Authenticator(self.config_path, contracts, self.service.clock)
        application = Relationships(self.service, auth=auth)
        app = create_app(service=self.service, auth=auth, relationships=application)
        await asyncio.to_thread(
            subprocess.run,
            [os.environ["TIANSHU_TLS_PYTHON"], "-c", CERT_PROGRAM, str(self.root)],
            check=True,
            capture_output=True,
            timeout=20,
        )
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=port,
                log_level="error",
                lifespan="off",
                ssl_certfile=str(self.root / "cert.pem"),
                ssl_keyfile=str(self.root / "key.pem"),
            )
        )
        self.thread = threading.Thread(
            target=self.server.run, kwargs={"sockets": [listener]}, daemon=True
        )
        self.thread.start()
        deadline = time.monotonic() + 5
        while not self.server.started and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        if not self.server.started:
            raise RuntimeError("Synthetic loopback TLS server did not start")
        self.http = JsonService(
            f"https://127.0.0.1:{port}",
            "synthetic-ts115-joint-only",
            ca_file=str(self.root / "cert.pem"),
        )
        self.h.core.memory = Memory(self.h.contracts, self.http)
        self.runtime = assemble(
            {"enabled": True, "candidate_schema_path": str(schema_path())}, self.http
        )
        self.h.core.relationships = self.runtime
        self.app = app
        return self

    def save(self):
        self.config_path.write_text(canonical(self.config), encoding="utf-8")

    async def accept(self, **fields):
        fields.setdefault("account", "10001")
        fields.setdefault(
            "channel", "group:20001" if fields.get("group") else "private:" + fields["account"]
        )
        request = self.h.request(**fields)
        ref = request["command"]["origin"]["assertion_ref"]
        context = copy.deepcopy(self.h.origins.values[ref])
        context.update(
            issuer="platform",
            authenticated_service="companion",
            audience_service="memory",
            principal_id="synthetic-console-admin",
            expires_at="2030-01-01T00:00:00Z",
        )
        self.config["origins"][ref] = context
        self.save()
        result = await self.h.core.ingest("nonebot", request)
        scope = dict(
            context["allowed_scope"],
            person_id=result["person_id"],
            conversation_id=result["conversation_id"],
        )
        context["allowed_scope"] = scope
        self.h.origins.values[ref]["allowed_scope"] = scope
        self.h.origins.values[ref]["expires_at"] = context["expires_at"]
        if scope not in self.config["callers"]["companion"]["event_scopes"]:
            self.config["callers"]["companion"]["event_scopes"].append(scope)
        self.save()
        self.last_collection = result["collection_id"]
        return request, scope

    async def drain(self, phase="sent"):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            await self.h.cycles(1)
            await asyncio.sleep(0.002)
            turns = self.h.turns()
            current = next(
                (t for t in turns if t["bundle"]["collection_id"] == self.last_collection), None
            )
            if current and current["phase"] == phase:
                return current
        raise AssertionError([(t["phase"], t.get("failure")) for t in self.h.turns()])

    async def complete(self, **fields):
        request, scope = await self.accept(**fields)
        turn = await self.drain()
        await self.h.core.flush_outbox()
        return request, scope, turn

    async def read(self, turn):
        return await self.runtime.client.read(turn["origin"], turn["scope"], self.h.clock)

    async def manage(self, turn, operation, **fields):
        value = await self.read(turn)
        request = query(turn["origin"])
        request["command"] = {
            "request_id": request["request_id"],
            "pair": value["pair"],
            "expected_version": value["version"],
            "operation": operation,
            **fields,
        }
        result = await self.http.call(
            "/internal/v1/relationships/manage", request, uncertain_write=True
        )
        return self.runtime.client.contract.check("PrivateProjection", result["projection"])

    async def close(self):
        await self.http.close()
        self.server.should_exit = True
        await asyncio.to_thread(self.thread.join, 5)
        if self.thread.is_alive():
            raise RuntimeError("Synthetic TLS server did not stop")
        await self.h.core.close()
        sys.path.remove(str(self.root / "src"))
        for name in list(sys.modules):
            if name == "tianshu_memory" or name.startswith("tianshu_memory."):
                del sys.modules[name]
        self.directory.cleanup()


@unittest.skipUnless(
    os.environ.get("TIANSHU_RELATIONSHIP_MEMORY_REPO") and os.environ.get("TIANSHU_TLS_PYTHON"),
    "Explicit pinned Memory repository and synthetic TLS runtime required",
)
class RelationshipJointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.j = await MemoryJoint().start()

    async def asyncTearDown(self):
        await self.j.close()

    async def test_actual_projection_settlement_duplicate_and_actor_person_isolation(self):
        j = self.j
        request, _, a = await j.complete()
        self.assertEqual(1, (await j.read(a))["score"])
        await j.manage(
            a, "set_binding", relationship_type="partner", display_label="synthetic-private"
        )
        retry = copy.deepcopy(request)
        retry["command"]["idempotency_key"] = "synthetic-duplicate"
        result = await j.h.core.ingest("nonebot", retry)
        self.assertTrue(result["deduplicated"])
        await j.h.core.flush_outbox()
        self.assertEqual(1, (await j.read(a))["score"])
        _, _, b = await j.complete(actor="actor:b")
        _, _, other = await j.complete(account="10002")
        values = [await j.read(t) for t in (a, b, other)]
        self.assertEqual(
            ["partner", "unspecified", "unspecified"], [p["relationship_type"] for p in values]
        )
        self.assertEqual(a["scope"]["person_id"], b["scope"]["person_id"])
        self.assertNotEqual(a["scope"]["person_id"], other["scope"]["person_id"])
        self.assertEqual([1, 1, 1], [p["score"] for p in values])

    async def test_group_projection_and_prompt_have_no_private_relationship_data(self):
        j = self.j
        _, _, private = await j.complete()
        await j.manage(
            private, "set_binding", relationship_type="partner", display_label="synthetic-private"
        )
        _, _, group = await j.complete(group=True)
        public = await j.read(group)
        self.assertEqual("public", public["view"])
        self.assertFalse({"score", "stage", "frozen", "relationship_type"} & public.keys())
        self.assertNotIn("synthetic-private", canonical(j.h.gateway.calls[-1][1]))
        self.assertEqual(1, (await j.read(private))["score"])

    async def test_failed_send_cannot_increment_actual_memory(self):
        j = self.j
        j.h.sender.states = ["failed"]
        await j.accept()
        turn = await j.drain("failed")
        await j.h.core.flush_outbox()
        self.assertEqual(0, (await j.read(turn))["score"])

    async def test_unknown_send_cannot_increment_actual_memory(self):
        j = self.j
        j.h.sender.states = ["unknown"]
        await j.accept()
        turn = await j.drain("reconciling")
        await j.h.core.flush_outbox()
        self.assertEqual(0, (await j.read(turn))["score"])

    async def test_freeze_rejects_new_events_and_unfreeze_never_replays_them(self):
        j = self.j
        _, _, first = await j.complete()
        await j.manage(first, "set_freeze", frozen=True)
        _, _, frozen_turn = await j.complete()
        old = next(
            r
            for r in j.h.core.store.list("outbox")
            if r["event"]["aggregate_id"] == frozen_turn["id"]
        )
        self.assertEqual("rejected_frozen", old["relationship_receipt"]["outcome"])
        j.h.clock.advance(30 * 86400)
        self.assertEqual(1, (await j.read(first))["score"])
        await j.manage(first, "set_freeze", frozen=False)
        # Core event timestamps use milliseconds; this is strictly after the microsecond freeze end.
        j.h.clock.advance(1)
        _, _, latest = await j.complete()
        self.assertEqual(2, (await j.read(latest))["score"])
        result = await j.runtime.client.settle(frozen_turn["origin"], old["relationship_candidate"])
        self.assertEqual("rejected_frozen", result["outcome"])
        self.assertEqual(2, (await j.read(latest))["score"])

    async def test_actual_version_change_blocks_reply_prepared_with_old_projection(self):
        j = self.j
        j.h.gateway.gates[1] = asyncio.Event()
        await j.accept()
        for _ in range(300):
            await j.h.cycles(1)
            await asyncio.sleep(0.002)
            if j.h.gateway.calls:
                break
        self.assertTrue(j.h.gateway.calls)
        turn = j.h.turns()[0]
        await j.manage(turn, "set_freeze", frozen=True)
        j.h.gateway.gates[1].set()
        await j.drain("failed")
        self.assertEqual("version_conflict", j.h.turns()[0]["failure"])
        self.assertFalse(j.h.sender.calls)
        self.assertEqual(0, (await j.read(turn))["score"])

    async def test_actual_relationship_grant_withdrawal_blocks_send(self):
        j = self.j
        j.h.gateway.gates[1] = asyncio.Event()
        await j.accept()
        for _ in range(300):
            await j.h.cycles(1)
            await asyncio.sleep(0.002)
            if j.h.gateway.calls:
                break
        j.config["callers"]["companion"]["operations"].remove("relationships.check")
        j.save()
        j.h.gateway.gates[1].set()
        await j.drain("failed")
        self.assertFalse(j.h.sender.calls)

    async def test_relationship_timeout_degrades_without_candidate_and_keeps_plain_chat(self):
        j = self.j
        original = j.http.call

        async def delayed(path, *args, **kwargs):
            if path.endswith("/relationships/read"):
                await asyncio.sleep(0.05)
            return await original(path, *args, **kwargs)

        j.http.call = delayed
        j.runtime.client.timeout_seconds = 0.01
        _, _, turn = await j.complete()
        prompt = json.loads(j.h.gateway.calls[-1][1][1]["content"])
        self.assertNotIn("relationship_expression", prompt)
        self.assertEqual("sent", turn["phase"])
        j.http.call = original
        j.runtime.client.timeout_seconds = 5
        self.assertEqual(0, (await j.read(turn))["score"])

    async def test_actual_source_withdrawal_removes_contribution_even_while_frozen(self):
        j = self.j
        request, _, turn = await j.complete(message="synthetic-source-withdrawal")
        await j.manage(turn, "set_freeze", frozen=True)
        await j.accept(message=request["message_key"]["message_id"], revision=2, kind="retract")
        value = await j.read(turn)
        self.assertEqual(0, value["score"])
        self.assertTrue(value["frozen"])

    async def test_restart_preserves_pending_candidate_and_unknown_intent_never_replays(self):
        j = self.j
        await j.accept()
        turn = await j.drain()
        await j.h.core._flush_outbox()
        self.assertEqual(1, len(j.h.core.store.list("outbox", states=["relationship_pending"])))
        memory = j.h.core.memory
        await j.h.core.close()
        j.h.core = j.h.new_core(relationships=j.runtime)
        j.h.core.memory = memory
        j.h.core.recover()
        await j.h.core.flush_outbox()
        self.assertEqual(1, (await j.read(turn))["score"])
        item = j.h.core.store.list("outbox", states=["delivered"])[0]
        item["state"] = "relationship_submitting"
        del item["relationship_receipt"]
        j.h.core.store.put("outbox", item)
        await j.h.core.close()
        j.h.core = j.h.new_core(relationships=j.runtime)
        j.h.core.memory = memory
        j.h.core.recover()
        await j.h.core.flush_outbox()
        self.assertEqual(1, len(j.h.core.store.list("outbox", states=["relationship_unknown"])))
        self.assertEqual(1, (await j.read(turn))["score"])
