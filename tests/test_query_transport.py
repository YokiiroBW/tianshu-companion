"""Query-only recovery; synthetic contracts and real loopback TLS, never L0."""

import asyncio
import json
import os
import socket
import ssl
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx
import uvicorn

from support import contracts, Harness
from test_source_https import CERT_PROGRAM
from tianshu_companion.clients import JsonService, Memory, Sender
from tianshu_companion.contracts import Fault


SELECT = "/internal/v1/memory/select"
IDENTITY = "/internal/v1/identity/resolve"
QUERIES = [
    ("/internal/v1/origins/resolve", {}),
    ("/internal/v1/identity/resolve", {}),
    (SELECT, {}),
    ("/internal/v1/memory/context/history", {}),
    ("/internal/v1/memory/context/input-history", {}),
    ("/internal/v1/memory/profiles/select", {}),
    ("/internal/v1/memory/source-sync/check", {}),
    ("/internal/v2/bot-delivery/context", {}),
    ("/internal/v1/memory/candidates/process", {"job_id": "candidate:fixture", "output": None}),
    ("/internal/v1/model-requests/model:" + "a" * 32, None),
]
WRITES = [
    "/internal/v1/source-access/read",
    "/internal/v1/identity/register",
    "/internal/v1/memory/turn-commits",
    "/internal/v1/memory/revise",
    "/internal/v1/conversation/ingest",
    "/internal/v1/conversation/ingest-actors",
    "/internal/v1/conversation/send",
    "/internal/v2/bot-delivery/send",
    "/internal/v2/bot-delivery/finalize",
    "/internal/v2/bot-delivery/cancel",
    "/v1/chat/completions",
]


class QueryTransportTests(unittest.IsolatedAsyncioTestCase):
    def service(self, handler, *, memory_dependency=False):
        service = JsonService(
            "https://synthetic.invalid",
            "synthetic-only",
            transport=httpx.MockTransport(handler),
            memory_dependency=memory_dependency,
        )
        self.addAsyncCleanup(service.close)
        return service

    async def test_memory_reads_and_writes_share_capacity_and_leave_identity_room(self):
        release, ordinary_entered, identity_entered = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        seen, tasks = [], []
        active = peak = 0

        async def handler(request):
            nonlocal active, peak
            seen.append(request.url.path)
            active += 1
            peak = max(peak, active)
            if len(seen) == 3:
                ordinary_entered.set()
            if request.url.path == IDENTITY:
                identity_entered.set()
            try:
                await release.wait()
                return httpx.Response(200, json={"ok": True})
            finally:
                active -= 1

        service = self.service(handler, memory_dependency=True)
        initial = [
            (SELECT, {}),
            ("/internal/v1/memory/candidates/materials", {"event": "frozen"}),
            ("/internal/v1/memory/candidates/process", {"job_id": "job:one", "output": {}}),
        ]
        queued = [
            "/internal/v1/memory/candidates/segments",
            "/internal/v1/identity/register",
            "/internal/v2/memory/observations",
            "/internal/v2/memory/observations/query",
            "/internal/v1/relationships/settle",
            "/synthetic/not-a-query",
        ]
        try:
            tasks.extend(asyncio.create_task(service.call(path, body)) for path, body in initial)
            await asyncio.wait_for(ordinary_entered.wait(), 1)
            tasks.extend(asyncio.create_task(service.call(path, {})) for path in queued)
            await asyncio.sleep(0)
            tasks.append(asyncio.create_task(service.call(IDENTITY, {})))
            await asyncio.wait_for(identity_entered.wait(), 1)
            self.assertEqual(seen, [path for path, _ in initial] + [IDENTITY])
            self.assertTrue(all(not task.done() for task in tasks[3:-1]))
            self.assertEqual(peak, 4)
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.assertCountEqual(seen, [path for path, _ in initial] + queued + [IDENTITY])
        self.assertEqual(peak, 4)

    async def test_runtime_marks_memory_dependency_without_url_or_label_inference(self):
        from tianshu_companion.app import build_runtime

        with mock.patch.dict(
            os.environ,
            {"SYNTHETIC_MEMORY": "synthetic-only", "SYNTHETIC_GATEWAY": "synthetic-only"},
        ):
            core, _, clients, _ = build_runtime(
                {
                    "contracts_path": os.environ["TIANSHU_CONTRACTS"],
                    "database_path": ":memory:",
                    "services": {
                        "memory": {
                            "url": "https://synthetic.invalid",
                            "token_env": "SYNTHETIC_MEMORY",
                        },
                        "gateway": {
                            "url": "https://synthetic.invalid",
                            "token_env": "SYNTHETIC_GATEWAY",
                        },
                    },
                }
            )
        release, entered = asyncio.Event(), asyncio.Event()
        seen, tasks = [], []

        async def handler(request):
            probe = json.loads(request.content)["probe"]
            seen.append(probe)
            if probe.startswith("hold:"):
                if len([value for value in seen if value.startswith("hold:")]) == 3:
                    entered.set()
                await release.wait()
            return httpx.Response(200, json={"ok": True})

        memory, gateway = core.memory.client, core.gateway.client
        try:
            self.assertIsNot(core.knowledge_client, memory)
            self.assertIn(core.knowledge_client, clients)
            self.assertEqual(core.knowledge_client.url, memory.url)
            self.assertEqual(core.knowledge_client.token, memory.token)
            for service in (memory, gateway):
                await service.client.aclose()
                service.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
                service.label = "gateway"
            tasks.extend(
                asyncio.create_task(memory.call(SELECT, {"probe": f"hold:{i}"})) for i in range(3)
            )
            await asyncio.wait_for(entered.wait(), 1)
            path = "/internal/v1/memory/candidates/materials"
            queued = asyncio.create_task(memory.call(path, {"probe": "memory-write"}))
            tasks.append(queued)
            await asyncio.sleep(0)
            self.assertEqual(
                {"ok": True},
                await asyncio.wait_for(gateway.call(path, {"probe": "other-peer-write"}), 1),
            )
            self.assertEqual(
                {"ok": True},
                await asyncio.wait_for(memory.call(IDENTITY, {"probe": "identity"}), 1),
            )
            self.assertNotIn("memory-write", seen)
            self.assertFalse(queued.done())
            release.set()
            await asyncio.gather(*tasks)
            self.assertEqual(seen.count("memory-write"), 1)
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            await core.close()
            for service in clients:
                await service.close()

    async def test_runtime_knowledge_callback_has_independent_bounded_memory_admission(self):
        from tianshu_companion.app import build_runtime

        settings = {
            "contracts_path": os.environ["TIANSHU_CONTRACTS"],
            "database_path": ":memory:",
            "services": {
                "memory": {"url": "https://synthetic.invalid", "token_env": "CALLBACK_MEMORY"},
            },
        }
        with mock.patch.dict(os.environ, {"CALLBACK_MEMORY": "same-owner-token"}):
            core, _, clients, _ = build_runtime(settings)
        memory, knowledge = core.memory.client, core.knowledge_client
        release, outer_full, full = asyncio.Event(), asyncio.Event(), asyncio.Event()
        tasks, seen = [], {"memory": [], "knowledge": []}
        active, peak = {"memory": 0, "knowledge": 0}, {"memory": 0, "knowledge": 0}
        read_path = "/internal/v1/knowledge/content/read"
        outer_path = "/internal/v1/actor-self-memory/operate"

        async def handler(request):
            owner = (
                "memory"
                if request.url.path == outer_path
                else "knowledge"
                if request.url.path == read_path
                else request.headers["x-fixture-owner"]
            )
            self.assertEqual(request.url.host, "synthetic.invalid")
            self.assertEqual(request.headers["authorization"], "Bearer same-owner-token")
            seen[owner].append(request.url.path)
            active[owner] += 1
            peak[owner] = max(peak[owner], active[owner])
            try:
                if request.url.path == IDENTITY:
                    return httpx.Response(200, json={"identity": owner})
                if owner == "memory":
                    self.assertEqual(request.url.path, outer_path)
                    if active[owner] == 3:
                        outer_full.set()
                    await outer_full.wait()
                    result = await knowledge.call(read_path, {"source": "exact-reading-leaf"})
                    return httpx.Response(200, json=result)
                self.assertEqual(request.url.path, read_path)
                if active[owner] == 3:
                    full.set()
                await release.wait()
                return httpx.Response(200, json={"original": "真实区间🍋"})
            finally:
                active[owner] -= 1

        try:
            self.assertIn(knowledge, clients)
            self.assertEqual(JsonService.QUERY_BUDGET_SECONDS, 15)
            self.assertEqual(JsonService.COMMAND_BUDGET_SECONDS, 15)
            for service in dict.fromkeys((memory, knowledge)):
                await service.client.aclose()
                service.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            body = dict(
                schema_version=1,
                actor_id="actor:actual",
                role_version=7,
                operation="get_episode",
                episode_id="episode:actual",
                expected_revision=1,
                context=dict(purpose="reflection", audience=dict(kind="actor_only")),
            )
            tasks.extend(
                asyncio.create_task(memory.call(outer_path, dict(body, request_id=f"outer:{i}")))
                for i in range(3)
            )
            await asyncio.wait_for(full.wait(), 1)
            tasks.extend(
                [
                    asyncio.create_task(
                        memory.call(outer_path, dict(body, request_id="outer:fourth"))
                    ),
                    asyncio.create_task(knowledge.call(read_path, {"source": "fourth-leaf"})),
                ]
            )
            await asyncio.sleep(0)
            self.assertEqual(len(seen["memory"]), 3)
            self.assertEqual(len(seen["knowledge"]), 3)
            self.assertTrue(all(not task.done() for task in tasks))
            for owner, service in (("memory", memory), ("knowledge", knowledge)):
                self.assertEqual(
                    {"identity": owner},
                    await asyncio.wait_for(
                        service.call(IDENTITY, {}, headers={"X-Fixture-Owner": owner}), 1
                    ),
                )
            self.assertEqual(peak, {"memory": 4, "knowledge": 4})
            release.set()
            self.assertEqual(
                await asyncio.wait_for(asyncio.gather(*tasks), 1),
                [{"original": "真实区间🍋"}] * 5,
            )
            self.assertEqual(seen["memory"].count(outer_path), 4)
            self.assertEqual(seen["knowledge"].count(read_path), 5)
            self.assertEqual(peak, {"memory": 4, "knowledge": 4})
        finally:
            release.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await core.close()
            for service in clients:
                await service.close()
            self.assertTrue(all(service.client.is_closed for service in clients))

        explicit = dict(
            settings,
            services=dict(
                settings["services"],
                knowledge={
                    "url": "https://knowledge.synthetic.invalid",
                    "token_env": "CALLBACK_KNOWLEDGE",
                },
            ),
        )
        with mock.patch.dict(
            os.environ, {"CALLBACK_MEMORY": "memory", "CALLBACK_KNOWLEDGE": "knowledge"}
        ):
            core, _, clients, _ = build_runtime(explicit)
        try:
            self.assertEqual(core.knowledge_client.url, "https://knowledge.synthetic.invalid")
            self.assertEqual(core.knowledge_client.token, "knowledge")
            self.assertIsNot(core.memory.client, core.knowledge_client)
            self.assertFalse(core.knowledge_client._memory_dependency)
            self.assertIn(core.knowledge_client, clients)
        finally:
            await core.close()
            for service in clients:
                await service.close()
            self.assertTrue(all(service.client.is_closed for service in clients))

    async def test_waiting_memory_write_timeout_and_cancellation_never_send_or_leak(self):
        for path in (
            "/internal/v1/memory/candidates/materials",
            "/internal/v1/memory/candidates/segments",
            "/internal/v1/memory/candidates/process",
            "/internal/v1/identity/register",
        ):
            with self.subTest(path=path):
                release, full = asyncio.Event(), asyncio.Event()
                seen, tasks = [], []
                active = 0

                async def handler(request):
                    nonlocal active
                    seen.append(request.url.path)
                    active += 1
                    if active == 4:
                        full.set()
                    try:
                        await release.wait()
                        return httpx.Response(200, json={"ok": True})
                    finally:
                        active -= 1

                service = self.service(handler, memory_dependency=True)
                try:
                    tasks.extend(asyncio.create_task(service.call(SELECT, {})) for _ in range(3))
                    tasks.append(asyncio.create_task(service.call(IDENTITY, {})))
                    await asyncio.wait_for(full.wait(), 1)
                    service.COMMAND_BUDGET_SECONDS = 0.04
                    with self.assertRaises(Fault) as caught:
                        await service.call(path, {}, uncertain_write=True)
                    self.assertFalse(caught.exception.unknown)
                    self.assertFalse(caught.exception.retryable_read)
                    self.assertEqual(caught.exception.cause["operation"], "command")
                    waiting = asyncio.create_task(service.call(path, {}, uncertain_write=True))
                    tasks.append(waiting)
                    await asyncio.sleep(0)
                    waiting.cancel("unsent memory write")
                    with self.assertRaises(asyncio.CancelledError) as cancelled:
                        await waiting
                    self.assertEqual(cancelled.exception.args, ("unsent memory write",))
                    self.assertEqual(len(seen), 4)
                    release.set()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    release, full = asyncio.Event(), asyncio.Event()
                    service.COMMAND_BUDGET_SECONDS = 15
                    tasks = [
                        asyncio.create_task(service.call(path, {}, uncertain_write=True))
                        for _ in range(3)
                    ]
                    tasks.append(asyncio.create_task(service.call(IDENTITY, {})))
                    await asyncio.wait_for(full.wait(), 1)
                    self.assertEqual(len(seen), 8)
                finally:
                    release.set()
                    await asyncio.gather(*tasks, return_exceptions=True)

    async def test_memory_write_budget_includes_capacity_wait_and_sent_body_wait(self):
        release, full, write_entered = asyncio.Event(), asyncio.Event(), asyncio.Event()
        seen, tasks = [], []

        class SlowBody(httpx.AsyncByteStream):
            async def __aiter__(self):
                write_entered.set()
                await asyncio.sleep(0.3)
                yield b'{"ok": true}'

        async def handler(request):
            seen.append(request.url.path)
            if request.url.path == SELECT:
                if seen.count(SELECT) == 3:
                    full.set()
                await release.wait()
                return httpx.Response(200, json={"ok": True})
            return httpx.Response(200, stream=SlowBody())

        service = self.service(handler, memory_dependency=True)
        service.COMMAND_BUDGET_SECONDS = 0.4
        try:
            tasks.extend(asyncio.create_task(service.call(SELECT, {})) for _ in range(3))
            await asyncio.wait_for(full.wait(), 1)
            path = "/internal/v1/memory/candidates/materials"
            write = asyncio.create_task(
                service.call(path, {"request_id": "req:frozen"}, uncertain_write=True)
            )
            tasks.append(write)
            await asyncio.sleep(0.2)
            self.assertFalse(write_entered.is_set())
            release.set()
            with self.assertRaises(Fault) as caught:
                await write
            self.assertTrue(write_entered.is_set())
            self.assertTrue(caught.exception.unknown)
            self.assertFalse(caught.exception.retryable_read)
            self.assertEqual(caught.exception.cause["operation"], "command")
            self.assertEqual(seen.count(path), 1)
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_memory_write_admission_does_not_reclassify_or_replay_failures(self):
        for failure in ("transport", "timeout", "not_started", "forbidden"):
            with self.subTest(failure=failure):
                seen = []

                async def handler(request):
                    seen.append(bytes(request.content))
                    if failure == "transport":
                        raise httpx.ReadError("synthetic response lost after acceptance")
                    if failure == "timeout":
                        await asyncio.Event().wait()
                    return httpx.Response(
                        503 if failure == "not_started" else 403,
                        json=dict(
                            code=(
                                "dependency_unavailable"
                                if failure == "not_started"
                                else "forbidden"
                            ),
                            execution_state="not_started",
                            retryable=failure == "not_started",
                        ),
                    )

                service = self.service(handler, memory_dependency=True)
                service.COMMAND_BUDGET_SECONDS = 0.04
                request = {"request_id": "req:stable", "material_refs": ["material:stable"]}
                with self.assertRaises(Fault) as caught:
                    await service.call(
                        "/internal/v1/memory/candidates/segments", request, uncertain_write=True
                    )
                self.assertEqual(caught.exception.unknown, failure in {"transport", "timeout"})
                self.assertFalse(caught.exception.retryable_read)
                self.assertEqual(caught.exception.cause["operation"], "command")
                self.assertEqual(len(seen), 1)
                self.assertEqual(json.loads(seen[0]), request)

    async def test_memory_query_retry_keeps_one_admission_with_mixed_writes(self):
        release, writes_entered, retry_entered, identity_entered = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        seen, queries, tasks = [], [], []

        async def handler(request):
            seen.append(request.url.path)
            if request.url.path == SELECT:
                queries.append((bytes(request.content), dict(request.headers)))
                if len(queries) == 1:
                    raise httpx.RemoteProtocolError("synthetic idle close")
                retry_entered.set()
            elif request.url.path == IDENTITY:
                identity_entered.set()
            elif seen.count("/internal/v1/memory/candidates/materials") == 2:
                writes_entered.set()
            await release.wait()
            return httpx.Response(200, json={"ok": True})

        service = self.service(handler, memory_dependency=True)
        path = "/internal/v1/memory/candidates/materials"
        try:
            tasks.extend(
                asyncio.create_task(service.call(path, {"probe": i}, uncertain_write=True))
                for i in range(2)
            )
            await asyncio.wait_for(writes_entered.wait(), 1)
            tasks.append(asyncio.create_task(service.call(SELECT, {"request_id": "req:one-read"})))
            await asyncio.wait_for(retry_entered.wait(), 1)
            queued = asyncio.create_task(
                service.call(path, {"probe": "queued"}, uncertain_write=True)
            )
            tasks.append(queued)
            await asyncio.sleep(0)
            tasks.append(asyncio.create_task(service.call(IDENTITY, {})))
            await asyncio.wait_for(identity_entered.wait(), 1)
            self.assertEqual(len(queries), 2)
            self.assertEqual(queries[0], queries[1])
            self.assertEqual(seen.count(path), 2)
            self.assertFalse(queued.done())
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(seen.count(path), 3)
        self.assertEqual(len(queries), 2)

    async def test_memory_writes_share_ordinary_cooldown_without_blocking_identity(self):
        for cooled in (SELECT, IDENTITY):
            with self.subTest(cooled=cooled):
                seen = []

                async def handler(request):
                    seen.append(request.url.path)
                    if request.url.path == cooled:
                        return httpx.Response(
                            429,
                            json=dict(
                                code="queue_full", execution_state="not_started", retryable=True
                            ),
                            headers={"Retry-After": "1"},
                        )
                    return httpx.Response(200, json={"ok": True})

                service = self.service(handler, memory_dependency=True)
                with self.assertRaises(Fault) as caught:
                    await service.call(cooled, {})
                self.assertTrue(caught.exception.retryable_read)
                write_path = "/internal/v1/memory/candidates/materials"
                waiting_path = write_path if cooled == SELECT else IDENTITY
                other_path = IDENTITY if cooled == SELECT else write_path
                waiting = asyncio.create_task(service.call(waiting_path, {}))
                try:
                    await asyncio.sleep(0)
                    self.assertEqual(
                        {"ok": True}, await asyncio.wait_for(service.call(other_path, {}), 0.5)
                    )
                    self.assertEqual(seen, [cooled, other_path])
                    self.assertFalse(waiting.done())
                finally:
                    waiting.cancel()
                    await asyncio.gather(waiting, return_exceptions=True)

    async def test_memory_reads_leave_one_shared_slot_for_identity(self):
        release, ordinary_entered, identity_entered = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        seen, tasks = [], []
        active = peak = 0

        async def handler(request):
            nonlocal active, peak
            seen.append(request.url.path)
            active += 1
            peak = max(peak, active)
            if len(seen) == 3:
                ordinary_entered.set()
            if request.url.path == IDENTITY:
                identity_entered.set()
            try:
                await release.wait()
                return httpx.Response(200, json={"ok": True})
            finally:
                active -= 1

        service = self.service(handler)
        reads = [
            (SELECT, {}),
            ("/internal/v2/memory/context/verify", {}),
            ("/internal/v1/memory/candidates/process", {"job_id": "job:fixture", "output": None}),
        ]
        try:
            tasks.extend(asyncio.create_task(service.call(path, body)) for path, body in reads)
            await asyncio.wait_for(ordinary_entered.wait(), 1)
            tasks.append(asyncio.create_task(service.call(SELECT, {})))
            await asyncio.sleep(0)
            tasks.append(asyncio.create_task(service.call(IDENTITY, {})))
            await asyncio.wait_for(identity_entered.wait(), 1)
            self.assertEqual(len(seen), 4)
            self.assertFalse(tasks[3].done())
            self.assertEqual(peak, 4)
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(len(seen), 5)
        self.assertEqual(peak, 4)

    async def test_identity_can_borrow_all_four_slots_without_exceeding_total(self):
        release, entered = asyncio.Event(), asyncio.Event()
        seen, tasks = [], []

        async def handler(request):
            seen.append(request.url.path)
            if len(seen) == 4:
                entered.set()
            await release.wait()
            return httpx.Response(200, json={"ok": True})

        service = self.service(handler)
        try:
            tasks.extend(asyncio.create_task(service.call(IDENTITY, {})) for _ in range(4))
            await asyncio.wait_for(entered.wait(), 1)
            tasks.append(asyncio.create_task(service.call(IDENTITY, {})))
            tasks.append(asyncio.create_task(service.call(SELECT, {})))
            await asyncio.sleep(0)
            self.assertEqual(seen, [IDENTITY] * 4)
            self.assertFalse(tasks[-1].done())
            self.assertFalse(tasks[-2].done())
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(len(seen), 6)

    async def test_other_peer_reads_keep_their_four_slots(self):
        for path, body in (
            ("/internal/v1/origins/resolve", {}),
            ("/internal/v2/bot-delivery/context", {}),
            ("/internal/v1/model-requests/model:" + "a" * 32, None),
        ):
            with self.subTest(path=path):
                release, entered = asyncio.Event(), asyncio.Event()
                seen, tasks = [], []

                async def handler(request):
                    seen.append(request)
                    if len(seen) == 4:
                        entered.set()
                    await release.wait()
                    return httpx.Response(200, json={"ok": True})

                service = self.service(handler)
                try:
                    tasks.extend(asyncio.create_task(service.call(path, body)) for _ in range(4))
                    await asyncio.wait_for(entered.wait(), 1)
                    self.assertEqual(len(seen), 4)
                finally:
                    release.set()
                    await asyncio.gather(*tasks, return_exceptions=True)

    async def test_commands_do_not_wait_for_query_capacity(self):
        release, entered = asyncio.Event(), asyncio.Event()
        reads, writes, tasks = [], [], []

        async def handler(request):
            if request.url.path == IDENTITY:
                reads.append(request)
                if len(reads) == 4:
                    entered.set()
                await release.wait()
            else:
                writes.append(request.url.path)
            return httpx.Response(200, json={"ok": True})

        service = self.service(handler)
        paths = [
            "/internal/v1/identity/register",
            "/internal/v1/memory/candidates/process",
            "/internal/v2/bot-delivery/send",
        ]
        try:
            tasks.extend(asyncio.create_task(service.call(IDENTITY, {})) for _ in range(4))
            await asyncio.wait_for(entered.wait(), 1)
            for path in paths:
                self.assertEqual({"ok": True}, await asyncio.wait_for(service.call(path, {}), 1))
            self.assertEqual(writes, paths)
            self.assertTrue(all(not task.done() for task in tasks))
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_memory_and_identity_cooldowns_are_isolated_both_ways(self):
        for cooled, other in ((SELECT, IDENTITY), (IDENTITY, SELECT)):
            with self.subTest(cooled=cooled):
                seen = []

                async def handler(request):
                    seen.append(request.url.path)
                    if request.url.path == cooled:
                        return httpx.Response(
                            429,
                            json=dict(
                                code="queue_full", execution_state="not_started", retryable=True
                            ),
                            headers={"Retry-After": "1"},
                        )
                    return httpx.Response(200, json={"ok": True})

                service = self.service(handler)
                with self.assertRaises(Fault) as caught:
                    await service.call(cooled, {})
                self.assertEqual(caught.exception.read_execution, "not_started")
                self.assertGreaterEqual(caught.exception.retry_after_seconds, 0.99)
                waiting = asyncio.create_task(service.call(cooled, {}))
                try:
                    await asyncio.sleep(0)
                    self.assertEqual(
                        {"ok": True}, await asyncio.wait_for(service.call(other, {}), 0.5)
                    )
                    self.assertEqual(seen, [cooled, other])
                    self.assertFalse(waiting.done())
                finally:
                    waiting.cancel()
                    await asyncio.gather(waiting, return_exceptions=True)

    async def test_cooldown_extended_during_capacity_wait_releases_shared_slot(self):
        release_identity, release_ordinary = asyncio.Event(), asyncio.Event()
        full, fourth_identity = asyncio.Event(), asyncio.Event()
        seen, tasks = [], []

        async def handler(request):
            body = json.loads(request.content)
            seen.append(body["probe"])
            if len(seen) == 4:
                full.set()
            if request.url.path == IDENTITY:
                if body["probe"] == "identity:4":
                    fourth_identity.set()
                await release_identity.wait()
                return httpx.Response(200, json={"ok": True})
            await release_ordinary.wait()
            return httpx.Response(
                429,
                json=dict(code="queue_full", execution_state="not_started", retryable=True),
                headers={"Retry-After": "1"},
            )

        service = self.service(handler)
        try:
            tasks.extend(
                asyncio.create_task(service.call(IDENTITY, {"probe": f"identity:{i}"}))
                for i in range(3)
            )
            source = asyncio.create_task(service.call(SELECT, {"probe": "source"}))
            tasks.append(source)
            await asyncio.wait_for(full.wait(), 1)
            queued = [
                asyncio.create_task(service.call(SELECT, {"probe": f"queued:{i}"}))
                for i in range(2)
            ]
            tasks.extend(queued)
            await asyncio.sleep(0)
            release_ordinary.set()
            with self.assertRaises(Fault):
                await source
            tasks.append(asyncio.create_task(service.call(IDENTITY, {"probe": "identity:4"})))
            await asyncio.wait_for(fourth_identity.wait(), 0.5)
            self.assertFalse(any(probe.startswith("queued:") for probe in seen))
            self.assertTrue(all(not task.done() for task in queued))
        finally:
            release_identity.set()
            release_ordinary.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_capacity_wait_timeout_and_cancellation_do_not_leak_either_limit(self):
        for waiting_path in (SELECT, IDENTITY):
            with self.subTest(waiting_path=waiting_path):
                release, full = asyncio.Event(), asyncio.Event()
                seen, tasks = [], []
                active = 0

                async def handler(request):
                    nonlocal active
                    seen.append(request.url.path)
                    active += 1
                    if active == 4:
                        full.set()
                    try:
                        await release.wait()
                        return httpx.Response(200, json={"ok": True})
                    finally:
                        active -= 1

                service = self.service(handler)
                try:
                    tasks.extend(asyncio.create_task(service.call(SELECT, {})) for _ in range(3))
                    tasks.append(asyncio.create_task(service.call(IDENTITY, {})))
                    await asyncio.wait_for(full.wait(), 1)
                    service.QUERY_BUDGET_SECONDS = 0.04
                    with self.assertRaises(Fault) as caught:
                        await service.call(waiting_path, {})
                    self.assertEqual(caught.exception.read_execution, "waiting")
                    waiting = asyncio.create_task(service.call(waiting_path, {}))
                    tasks.append(waiting)
                    await asyncio.sleep(0)
                    waiting.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await waiting
                    self.assertEqual(len(seen), 4)
                    release.set()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    # Refill all four positions after both abandoned waits.
                    release, full = asyncio.Event(), asyncio.Event()
                    service.QUERY_BUDGET_SECONDS = 15
                    tasks = [asyncio.create_task(service.call(SELECT, {})) for _ in range(3)]
                    tasks.append(asyncio.create_task(service.call(IDENTITY, {})))
                    await asyncio.wait_for(full.wait(), 1)
                    self.assertEqual(len(seen), 8)
                finally:
                    release.set()
                    await asyncio.gather(*tasks, return_exceptions=True)

    async def test_late_worker_is_not_replaced_during_peer_cooldown_or_local_wait_timeout(self):
        # The HTTP owner can stop while a separately owned peer worker lives.
        # A client retry must not pretend that cancelled HTTP released it.
        release, stopped = asyncio.Event(), asyncio.Event()
        workers, seen = [], []

        async def late_worker():
            try:
                await release.wait()
            finally:
                stopped.set()

        async def handler(request):
            seen.append(request)
            workers.append(asyncio.create_task(late_worker()))
            await asyncio.Event().wait()

        service = self.service(handler)
        service.QUERY_BUDGET_SECONDS = 0.04
        try:
            with self.assertRaises(Fault) as first:
                await service.call(SELECT, {})
            self.assertTrue(first.exception.retryable_read)
            self.assertEqual(first.exception.read_execution, "unconfirmed")
            self.assertEqual(len(seen), 1)
            self.assertFalse(stopped.is_set())
            with self.assertRaises(Fault) as second:
                await service.call(SELECT, {})
            self.assertEqual(second.exception.read_execution, "waiting")
            self.assertEqual(len(seen), 1)
            self.assertFalse(stopped.is_set())
            self.assertGreater(second.exception.retry_after_seconds, 0.8)
        finally:
            release.set()
            await asyncio.gather(*workers)
        self.assertTrue(stopped.is_set())

    async def test_retry_after_is_peer_wide_and_cancellation_during_cooldown_sends_nothing(self):
        seen = []

        async def handler(request):
            seen.append(request)
            return httpx.Response(
                429,
                json=dict(code="queue_full", execution_state="not_started", retryable=True),
                headers={"Retry-After": "1"},
            )

        service = self.service(handler)
        with self.assertRaises(Fault) as first:
            await service.call(SELECT, {})
        self.assertEqual(first.exception.read_execution, "not_started")
        self.assertGreaterEqual(first.exception.retry_after_seconds, 0.99)
        tasks = [asyncio.create_task(service.call(SELECT, {})) for _ in range(4)]
        await asyncio.sleep(0.02)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(len(seen), 1)
        self.assertTrue(all(task.cancelled() for task in tasks))

    async def test_transport_cause_is_internal_and_survives_unknown_write_wrapping(self):
        for exception, category in (
            (httpx.ConnectTimeout, "connect_error"),
            (httpx.ReadTimeout, "read_error"),
        ):
            calls = []

            async def handler(request):
                calls.append(request)
                raise exception("secret URL, token and peer text")

            with self.assertRaises(Fault) as caught:
                await self.service(handler).call(
                    "/internal/v1/memory/candidates/process",
                    {"job_id": "candidate:fixture", "output": {"memories": []}},
                    uncertain_write=True,
                )
            error = caught.exception
            self.assertEqual(len(calls), 1)
            self.assertTrue(error.unknown)
            self.assertEqual(error.cause["category"], category)
            self.assertEqual(error.cause["exception_type"], exception.__name__)
            self.assertEqual(error.cause["operation"], "command")
            self.assertNotIn("http_status", error.cause)
            wire = error.wire("request:synthetic")
            self.assertNotIn("cause", wire)
            self.assertNotIn("secret", json.dumps([wire, error.cause]))

    async def test_exact_queries_retry_once_with_identical_wire_request(self):
        for path, body in QUERIES:
            for error in (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError):
                with self.subTest(path=path, error=error):
                    seen = []

                    async def handler(request):
                        seen.append((request.method, request.url, request.content, request.headers))
                        if len(seen) == 1:
                            raise error("synthetic connection loss")
                        return httpx.Response(200, json={"ok": True})

                    service = self.service(handler)
                    self.assertEqual(
                        {"ok": True}, await service.call(path, body, {"X-Request-ID": "fixed"})
                    )
                    self.assertEqual(2, len(seen))
                    self.assertEqual(seen[0], seen[1])

    async def test_failure_limit_and_nonquery_paths_never_replay(self):
        cases = [(path, {}, 1) for path in WRITES] + [
            (SELECT, {}, 2),
            (SELECT, None, 1),
            (SELECT + "/", {}, 1),
            (SELECT + "?write=true", {}, 1),
            ("/internal/v1/model-requests/model:" + "a" * 32 + "/consume", None, 1),
            ("/internal/v1/model-requests/model:" + "a" * 32, {}, 1),
            ("/internal/v1/memory/candidates/process", {"job_id": "candidate:fixture"}, 1),
            (
                "/internal/v1/memory/candidates/process",
                {"job_id": "candidate:fixture", "output": {"memories": []}},
                1,
            ),
            (
                "/internal/v1/memory/candidates/process",
                {"job_id": "candidate:fixture", "output": {}},
                1,
            ),
            (
                "/internal/v1/memory/candidates/process",
                {"job_id": "candidate:fixture", "output": None, "extra": True},
                1,
            ),
        ]
        for path, body, expected in cases:
            with self.subTest(path=path, body=body):
                seen = []

                async def handler(request):
                    seen.append(request)
                    raise httpx.RemoteProtocolError("received command but lost response")

                with self.assertRaises(Fault):
                    await self.service(handler).call(path, body)
                self.assertEqual(expected, len(seen))

    async def test_http_json_transport_configuration_and_body_failures_do_not_retry(self):
        class BrokenBody(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'{"partial":'
                raise httpx.ReadError("lost body after response headers")

        outcomes = [
            httpx.Response(status, json={"code": code})
            for status, code in [
                (401, "unauthorized"),
                (403, "forbidden"),
                (409, "scope_changed"),
                (409, "version_conflict"),
                (503, "dependency_unavailable"),
                (302, "redirect"),
            ]
        ] + [
            httpx.Response(200, content=b"not-json"),
            httpx.Response(200, stream=BrokenBody()),
            httpx.ConnectError("certificate verify failed"),
            httpx.ConnectTimeout("connect timeout"),
            httpx.ReadTimeout("read timeout"),
            httpx.PoolTimeout("busy pool"),
            httpx.LocalProtocolError("invalid request"),
        ]
        for outcome in outcomes:
            with self.subTest(outcome=outcome):
                seen = []

                async def handler(request):
                    seen.append(request)
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome

                with self.assertRaises(Fault):
                    await self.service(handler).call(SELECT, {})
                self.assertEqual(1, len(seen))

    async def test_schema_failure_is_not_retried(self):
        seen = []

        async def handler(request):
            seen.append(request)
            return httpx.Response(200, json={"invalid_schema": True})

        memory = Memory(contracts(), self.service(handler))
        with self.assertRaises(Fault):
            await memory.identity({}, {}, 0)
        self.assertEqual(1, len(seen))

    async def test_total_budget_covers_both_attempts_and_response_body(self):
        for delay_first, body_delay, expected in [(0.04, False, 2), (0.2, False, 1), (0, True, 2)]:
            with self.subTest(delay_first=delay_first, body_delay=body_delay):
                seen = []
                cancelled = asyncio.Event()

                class SlowBody(httpx.AsyncByteStream):
                    async def __aiter__(self):
                        try:
                            await asyncio.sleep(1)
                            yield b"{}"
                        finally:
                            cancelled.set()

                async def handler(request):
                    seen.append(request)
                    if len(seen) == 1:
                        await asyncio.sleep(delay_first)
                        raise httpx.ReadError("first failure")
                    if body_delay:
                        return httpx.Response(200, stream=SlowBody())
                    try:
                        await asyncio.sleep(0.08)
                        return httpx.Response(200, json={"ok": True})
                    finally:
                        cancelled.set()

                service = self.service(handler)
                service.QUERY_BUDGET_SECONDS = 0.1
                start = asyncio.get_running_loop().time()
                with self.assertRaises(Fault):
                    await service.call(SELECT, {})
                elapsed = asyncio.get_running_loop().time() - start
                self.assertLess(elapsed, 0.18)  # A reset budget or no body budget fails.
                self.assertEqual(expected, len(seen))
                if expected == 2:
                    self.assertTrue(cancelled.is_set())

    async def test_cancellation_on_either_attempt_propagates_without_another_request(self):
        for cancel_attempt in (1, 2):
            seen, entered = [], asyncio.Event()

            async def handler(request):
                seen.append(request)
                if len(seen) < cancel_attempt:
                    raise httpx.ReadError("first failure")
                entered.set()
                await asyncio.Event().wait()

            service = self.service(handler)
            task = asyncio.create_task(service.call(SELECT, {}))
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel("caller cancelled")
            with self.assertRaises(asyncio.CancelledError) as raised:
                await task
            self.assertEqual(("caller cancelled",), raised.exception.args)
            self.assertEqual(cancel_attempt, len(seen))


@unittest.skipUnless(os.environ.get("TIANSHU_TLS_PYTHON"), "requires TLS certificate interpreter")
class QueryTLS(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        subprocess.run(
            [os.environ["TIANSHU_TLS_PYTHON"], "-c", CERT_PROGRAM, str(self.root)],
            check=True,
            capture_output=True,
        )

    def service(self, port, *, memory_dependency=False):
        service = JsonService(
            f"https://127.0.0.1:{port}",
            "synthetic-only",
            ca_file=str(self.root / "cert.pem"),
            memory_dependency=memory_dependency,
        )
        self.addAsyncCleanup(service.close)
        return service

    async def test_memory_lost_receipt_replays_same_candidate_after_restart(self):
        seen = []

        async def drop(reader, writer):
            try:
                headers = await reader.readuntil(b"\r\n\r\n")
                length = next(
                    int(line.split(b":", 1)[1])
                    for line in headers.split(b"\r\n")
                    if line.lower().startswith(b"content-length:")
                )
                seen.append(json.loads(await reader.readexactly(length)))
            finally:
                writer.close()
                await writer.wait_closed()

        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(self.root / "cert.pem", self.root / "key.pem")
        server = await asyncio.start_server(drop, "127.0.0.1", 0, ssl=tls)
        async with server:
            h = Harness(self.root / "synthetic.db", silence_ms=0)
            try:
                await h.ingest()
                await h.cycles()
                h.core.memory = Memory(
                    h.contracts,
                    self.service(server.sockets[0].getsockname()[1], memory_dependency=True),
                )
                await h.core.flush_outbox()
                self.assertEqual(1, len(seen))
                self.assertEqual("unknown", h.core.store.list("outbox")[0]["state"])
                remote = h.core.memory
                await h.core.close()
                h.core = h.new_core()
                h.core.memory = remote
                h.core.recover()
                h.clock.advance(600)
                await h.core.flush_outbox()
                self.assertEqual(2, len(seen))
                self.assertEqual(seen[0], seen[1])
                self.assertEqual("unknown", h.core.store.list("outbox")[0]["state"])
            finally:
                await h.core.close()

    async def test_uvicorn_idle_close_recovers_on_new_tls_connection_without_interrupting_t2(self):
        seen, traces = [], []
        t2_entered, release_t2, stalled = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def app(scope, receive, send):
            body = b""
            while True:
                message = await receive()
                body += message.get("body", b"")
                if not message.get("more_body"):
                    break
            value = json.loads(body)
            seen.append((value, scope["client"], dict(scope["headers"])))
            if value["round"] == "t2":
                t2_entered.set()
                await release_t2.wait()
            result = json.dumps(value).encode()
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-length", str(len(result)).encode())],
                }
            )
            await send({"type": "http.response.body", "body": result})

        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        config = uvicorn.Config(
            app,
            lifespan="off",
            log_level="critical",
            ssl_certfile=str(self.root / "cert.pem"),
            ssl_keyfile=str(self.root / "key.pem"),
        )
        self.assertEqual(5, config.timeout_keep_alive)
        server = uvicorn.Server(config)
        serving = asyncio.create_task(server.serve(sockets=[listener]))
        service = self.service(listener.getsockname()[1])
        t1 = t2 = None
        try:
            async with asyncio.timeout(5):
                while not server.started:
                    await asyncio.sleep(0.01)
            await service.call(SELECT, {"round": "warm"})
            armed = True

            async def trace(name, info):
                nonlocal armed
                traces.append((name, info))
                if name == "http11.send_request_headers.started" and armed:
                    armed = False
                    stalled.set()
                    await asyncio.sleep(5.2)

            async def attach_trace(request):
                if json.loads(request.content)["round"] == "t1":
                    request.extensions["trace"] = trace

            service.client.event_hooks["request"].append(attach_trace)
            t1 = asyncio.create_task(service.call(SELECT, {"round": "t1", "request_id": "fixed"}))
            await asyncio.wait_for(stalled.wait(), 1)
            t2 = asyncio.create_task(service.call(SELECT, {"round": "t2"}))
            await asyncio.wait_for(t2_entered.wait(), 2)
            self.assertEqual({"round": "t1", "request_id": "fixed"}, await asyncio.wait_for(t1, 8))
            self.assertFalse(t2.done())
            release_t2.set()
            self.assertEqual({"round": "t2"}, await asyncio.wait_for(t2, 2))
            names = [name for name, _ in traces]
            failures = [(name, info) for name, info in traces if name.endswith(".failed")]
            self.assertTrue(
                any(
                    type(info["exception"]).__name__ == "RemoteProtocolError"
                    for _, info in failures
                ),
                failures,
            )
            self.assertEqual(2, names.count("http11.send_request_headers.started"))
            self.assertEqual(1, names.count("http11.receive_response_headers.complete"))
            self.assertEqual(1, names.count("connection.start_tls.complete"))
            self.assertLess(
                names.index("http11.receive_response_headers.failed"),
                names.index("connection.start_tls.complete"),
            )
            by_round = {item[0]["round"]: item for item in seen}
            self.assertEqual(3, len(seen))
            self.assertNotEqual(by_round["warm"][1], by_round["t1"][1])
            self.assertEqual(b"Bearer synthetic-only", by_round["t1"][2][b"authorization"])
            self.assertFalse(service.client.is_closed)
            untrusted = JsonService(service.url, "synthetic-only")
            try:
                with self.assertRaises(Fault):
                    await untrusted.call(SELECT, {"round": "untrusted"})
                self.assertEqual(3, len(seen))
            finally:
                await untrusted.close()
            print(
                "TLS idle-close: RemoteProtocolError before headers; one new TLS connection; "
                "t1 received once; concurrent t2 completed on original connection"
            )
        finally:
            release_t2.set()
            for task in (t1, t2):
                if task and not task.done():
                    task.cancel()
            await asyncio.gather(*(task for task in (t1, t2) if task), return_exceptions=True)
            await service.close()
            server.should_exit = True
            await asyncio.wait_for(serving, 5)
            listener.close()

    async def test_received_writes_drop_response_without_replay_and_sender_stays_unknown(self):
        seen = []

        async def drop(reader, writer):
            try:
                headers = await reader.readuntil(b"\r\n\r\n")
                length = next(
                    int(line.split(b":", 1)[1])
                    for line in headers.split(b"\r\n")
                    if line.lower().startswith(b"content-length:")
                )
                body = await reader.readexactly(length)
                seen.append((headers.split(b" ")[1].decode(), json.loads(body)))
            finally:
                writer.close()
                await writer.wait_closed()

        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(self.root / "cert.pem", self.root / "key.pem")
        server = await asyncio.start_server(drop, "127.0.0.1", 0, ssl=tls)
        async with server:
            service = self.service(server.sockets[0].getsockname()[1])
            for path in WRITES:
                before = len(seen)
                with self.assertRaises(Fault):
                    await service.call(path, {"operation": "input", "idempotency_key": "fixed"})
                self.assertEqual(before + 1, len(seen))
                self.assertEqual(path, seen[-1][0])
            core_send_path = "/internal/v1/conversation/send"
            before_core_send = sum(path == core_send_path for path, _ in seen)
            self.assertEqual(1, before_core_send)
            h = Harness(silence_ms=0)
            try:
                h.core.sender = Sender(h.contracts, service)
                await h.core.ingest("nonebot", h.request())
                await h.cycles(40)
                self.assertEqual("reconciling", h.turns()[0]["phase"])
                self.assertEqual("unknown", h.turns()[0]["delivery_state"])
                self.assertEqual(
                    before_core_send + 1, sum(path == core_send_path for path, _ in seen)
                )
                self.assertEqual(
                    {path: 1 + (path == core_send_path) for path in WRITES},
                    {path: sum(seen_path == path for seen_path, _ in seen) for path in WRITES},
                )
                await h.cycles(40)
                h.clock.advance(h.core.policy.delivery_reconcile_timeout_ms / 1000 + 1)
                await h.cycles(40)
                self.assertEqual("closed_unknown", h.turns()[0]["phase"])
                self.assertTrue(h.turns()[0]["unresolved_delivery"])
                self.assertEqual(
                    {path: 1 + (path == core_send_path) for path in WRITES},
                    {path: sum(seen_path == path for seen_path, _ in seen) for path in WRITES},
                )
            finally:
                await h.core.close()
