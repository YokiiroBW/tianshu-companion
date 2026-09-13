"""Opt-in component joint acceptance against immutable Memory 69b29f3.

Real producer HTTP application + consumer client + Core. Issuers, SourceAuthority,
approval, model and sender are explicitly synthetic; this is not production L0.
Only a Git archive is read; all producer files/databases/config live in a temp directory.
"""

import asyncio
import copy
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx

from support import Harness
from tianshu_companion.clients import JsonService, Memory
from tianshu_companion.contracts import Fault, canonical, digest


@unittest.skipUnless(
    os.environ.get("TIANSHU_MEMORY_REPO"), "requires explicit immutable Memory repository reference"
)
class ProfileJointTests(unittest.IsolatedAsyncioTestCase):
    async def test_core_consumes_real_producer_profiles_and_stops_after_withdrawal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = subprocess.run(
                [
                    "git",
                    "-C",
                    os.environ["TIANSHU_MEMORY_REPO"],
                    "archive",
                    "--format=zip",
                    "69b29f3",
                    "src",
                ],
                check=True,
                capture_output=True,
            ).stdout
            with zipfile.ZipFile(io.BytesIO(archive)) as files:
                files.extractall(root)
            sys.path.insert(0, str(root / "src"))
            try:
                from tianshu_memory.app import create_app
                from tianshu_memory.auth import Authenticator
                from tianshu_memory.contracts import Contracts
                from tianshu_memory.service import MemoryService
                from tianshu_memory.sources import LocalFixtureSources
                from tianshu_memory.store import Store
                from tianshu_memory.workflow import LocalWorkflow

                h = Harness(silence_ms=0)
                workspace = json.loads(
                    (Path(__file__).parents[1] / ".runtime/workspace-context.json").read_text()
                )["workspace"]
                contracts = Contracts(Path(workspace) / "contracts/text-dialogue/v1")
                contracts.load_profiles()
                store = Store(root / "fixture.sqlite")
                store.migrate_profiles(root / "before-profile.sqlite")

                def now():
                    return datetime.now(timezone.utc)

                service = MemoryService(
                    store, contracts, source_authority=LocalFixtureSources(), clock=now
                )
                workflow = LocalWorkflow(service)
                config = dict(
                    mode="local_fixture",
                    origins={},
                    callers={
                        "companion": dict(
                            token="synthetic-joint-only",
                            issuer="nonebot",
                            allowed_actors=["actor:a"],
                            operations=["resolve", "register", "select", "select_profiles"],
                            event_scopes=[],
                        ),
                    },
                )
                config_path = root / "fixture.json"

                def save():
                    config_path.write_text(canonical(config), encoding="utf-8")

                save()
                app = create_app(service=service, auth=Authenticator(config_path, contracts, now))
                calls = []

                async def record(request):
                    calls.append((request.url.path, json.loads(request.content)))

                client = JsonService(
                    "https://memory-fixture.invalid",
                    "synthetic-joint-only",
                    transport=httpx.ASGITransport(app=app),
                )
                client.client.event_hooks["request"] = [record]
                h.core.memory = Memory(h.contracts, client)

                async def accept(account, text, *, targets=None):
                    request = h.request(
                        account=account,
                        text=text,
                        group=True,
                        channel="group:joint",
                        targets=targets,
                    )
                    ref = request["command"]["origin"]["assertion_ref"]
                    ctx = copy.deepcopy(h.origins.values[ref])
                    ctx.update(authenticated_service="companion", audience_service="memory")
                    config["origins"][ref] = ctx
                    save()
                    result = await h.core.ingest("nonebot", request)
                    scope = dict(
                        ctx["allowed_scope"],
                        person_id=result["person_id"],
                        conversation_id=result["conversation_id"],
                    )
                    config["origins"][ref]["allowed_scope"] = scope
                    h.origins.values[ref]["allowed_scope"] = scope
                    save()
                    return result, ctx, request

                try:
                    b, owner, request = await accept("b", "我叫小明，群里聊咖啡", targets=[])
                    await h.cycles(80)
                    self.assertEqual("observed", h.turns()[0]["phase"])
                    public_source = dict(
                        message_key=dict(
                            channel={
                                **request["message_key"]["channel"],
                                "channel_conversation_id": "private:b",
                            },
                            message_id="synthetic-interest-source",
                            revision=1,
                        ),
                        receipt_id="receipt:synthetic",
                        archive_state="pending",
                        locator=None,
                    )
                    private_scope = dict(
                        owner["allowed_scope"],
                        audience="self_private",
                        conversation_id="conv:private-b",
                    )
                    private_ctx = dict(owner, allowed_scope=private_scope)
                    workflow.observe_source(public_source, private_scope)

                    def publish(subject, scope, source, category, text, sharing, ctx):
                        draft = dict(
                            source_scope=scope,
                            subject=subject,
                            sharing=sharing,
                            conversation_id=scope["conversation_id"]
                            if sharing == "group_only"
                            else None,
                            category=category,
                            field_key=category + ".coffee",
                            units=[
                                dict(
                                    statement=text,
                                    conditions=["白天且不失眠"],
                                    negations=["晚上不喝"],
                                    valid_time="本月",
                                    uncertainty="confirmed",
                                    reality="real",
                                    sources=[source],
                                )
                            ],
                        )
                        approval = workflow.approve_profile(draft, ctx, "2030-01-01T00:00:00Z")
                        return workflow.publish_profile(draft, approval, ctx)

                    person = dict(kind="person", person_id=b["person_id"])
                    publish(
                        person,
                        private_scope,
                        public_source,
                        "interest",
                        "咖啡白天可喝，因为会失眠所以晚上不喝",
                        "public_preference",
                        private_ctx,
                    )
                    group_source = h.turns()[0]["bundle"]["messages"][0]["source"]
                    workflow.observe_source(group_source, owner["allowed_scope"])
                    group = dict(kind="group", conversation_id=b["conversation_id"])
                    publish(
                        group,
                        owner["allowed_scope"],
                        group_source,
                        "style",
                        "咖啡群喜欢简洁讨论",
                        "group_only",
                        owner,
                    )
                    publish(
                        group,
                        owner["allowed_scope"],
                        group_source,
                        "topic",
                        "咖啡主题交流",
                        "group_only",
                        owner,
                    )
                    h.gateway.gates[2] = asyncio.Event()
                    a, _, _ = await accept("a", "我也叫小明，咖啡怎么聊")
                    for _ in range(200):
                        await h.cycles(1)
                        if h.gateway.calls:
                            break
                        await asyncio.sleep(0.01)
                    self.assertTrue(h.gateway.calls, h.turns())
                    prompt = json.loads(h.gateway.calls[-1][1][1]["content"])
                    self.assertEqual(
                        {digest(person), digest(group)},
                        {digest(p["target"]) for p in prompt["profiles"]},
                    )
                    self.assertNotEqual(a["person_id"], b["person_id"])
                    self.assertIn("因为会失眠所以晚上不喝", canonical(prompt))
                    self.assertNotIn("private:b", canonical(prompt))
                    self.assertNotIn("synthetic-interest-source", canonical(prompt))
                    self.assertLessEqual(h.turns()[1]["context_budget_used"]["bytes"], 16384)
                    profile_calls = [r for path, r in calls if path.endswith("profiles/select")]
                    self.assertTrue(
                        any(
                            r["known_scope_version"] is not None
                            and r["budget"] == dict(tokens=0, bytes=0)
                            for r in profile_calls
                        )
                    )
                    workflow.observe_source(public_source, private_scope, state="withdrawn")
                    h.gateway.gates[2].set()
                    for _ in range(200):
                        await h.cycles(1)
                        if h.turns()[1]["phase"] == "failed":
                            break
                        await asyncio.sleep(0.01)
                    self.assertEqual("scope_changed", h.turns()[1]["failure"])
                    self.assertFalse(h.sender.calls)
                    # No configured real SourceAuthority: the producer must remain unavailable.
                    service.source_authority = None
                    with self.assertRaises(Fault) as error:
                        await h.core.memory.profiles(
                            request["command"]["origin"],
                            owner["allowed_scope"],
                            person,
                            "咖啡",
                            ["interest"],
                            dict(tokens=0, bytes=0),
                        )
                    self.assertEqual("dependency_unavailable", error.exception.code)
                finally:
                    await h.core.close()
                    await client.close()
            finally:
                sys.path.remove(str(root / "src"))
                for name in list(sys.modules):
                    if name == "tianshu_memory" or name.startswith("tianshu_memory."):
                        del sys.modules[name]
