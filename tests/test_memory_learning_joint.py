"""Actual Platform/Core/Memory/Gateway over loopback TLS; synthetic model/channel only.

Run explicitly with the existing TS050 integration fixture on PYTHONPATH.
"""

import json
import os

import pytest

SourceChain = pytest.importorskip("ts050_source_support").SourceChain
web = pytest.importorskip("aiohttp.web")

TEXT = "第一支钢笔用蓝黑墨水"


class MemoryLearningJoint(SourceChain):
    silence_ms = 0

    def make_platform_app(self):
        from services.platform.server import create_app

        # The current admission layer owns streaming body reads.
        return create_app(self.platform)

    def platform_settings(self):
        config = super().platform_settings()

        # This fixture exercises the trusted application channel, not QQ admission.
        def web_namespace(value):
            if isinstance(value, dict):
                if value.get("namespace") == "qq":
                    value["namespace"] = "web"
                for child in value.values():
                    web_namespace(child)
            elif isinstance(value, list):
                for child in value:
                    web_namespace(child)

        web_namespace(config)
        web_namespace(self.account)
        web_namespace(self.channels)
        for entry in config["entries"].values():
            route = dict(caller="companion", receiver="platform", purpose="dialogue")
            if entry.get("actor_id") and route not in entry["routes"]:
                entry["routes"].append(route)
        return config

    def make_core_config(self):
        config = super().make_core_config()
        for binding in config["bindings"].values():
            binding["namespace"] = "web"
        return config

    def make_memory_config(self):
        config = super().make_memory_config()
        config["contract_directory"] = os.environ.get(
            "TIANSHU_CONTRACT_DIRECTORY", config["contract_directory"]
        )
        config["callers"]["companion"]["operations"] += [
            "context_query",
            "context_propose",
            "context_receipt",
        ]
        return config

    async def start_asgi(self, app, owner, sock):
        if owner == "memory":
            self.memory.store.migrate_context(self.directory / "backups/context.sqlite")
            self.memory.contracts.load_context()
            self.memory.store.migrate_facts(self.directory / "backups/facts.sqlite")
        return await super().start_asgi(app, owner, sock)

    async def start_core(self):
        await super().start_core()
        # The synthetic web channel uses this fixture's delivery recorder.
        self.core.web_sender = self.core.sender
        for skill in ("life.manage", "memory.propose"):
            if self.core.skills.actor("actor:a")["skills"].get(skill, {}).get("enabled") is False:
                continue
            await self.core.skills.call(
                "platform",
                "manage",
                dict(
                    schema_version=1,
                    request_id="learning:disable:" + skill,
                    actor_id="actor:a",
                    operation="skill.disable",
                    expected_version=self.core.skills.actor("actor:a")["version"],
                    value=dict(skill_id=skill),
                ),
            )

    async def record_model(self, request):
        body = await request.json()
        self.model_requests.append(body)
        extraction = body["messages"][0]["content"].startswith("Extract only durable facts")
        if extraction:
            content = json.loads(body["messages"][-1]["content"])
            self.assertEqual(content["extraction_schema"], 3)
            memories = (
                [
                    dict(
                        statement=TEXT,
                        conditions=[],
                        negations=[],
                        valid_time="current",
                        evidence=[dict(source_index=0, quote=TEXT)],
                        fact=dict(
                            key="pen.ink",
                            value="蓝黑墨水",
                            state="current",
                            valid_from=None,
                            valid_to=None,
                            keywords=["钢笔", "墨水"],
                            entity=dict(id=None, kind="pen", mention="第一支钢笔"),
                        ),
                    )
                ]
                if TEXT in content["messages"][0]["text"]
                else []
            )
            text = json.dumps({"memories": memories}, ensure_ascii=False)
        else:
            text = "收到。"
        if body.get("stream"):
            chunks = [
                dict(
                    id="synthetic-memory-model",
                    object="chat.completion.chunk",
                    choices=[
                        dict(
                            index=0, delta=dict(role="assistant", content=text), finish_reason=None
                        )
                    ],
                ),
                dict(
                    id="synthetic-memory-model",
                    object="chat.completion.chunk",
                    choices=[dict(index=0, delta={}, finish_reason="stop")],
                ),
            ]
            return web.Response(
                text="".join("data: " + json.dumps(c, ensure_ascii=False) + "\n\n" for c in chunks)
                + "data: [DONE]\n\n",
                content_type="text/event-stream",
            )
        return web.json_response(
            dict(
                id="synthetic-memory-model",
                object="chat.completion",
                choices=[
                    dict(
                        index=0, message=dict(role="assistant", content=text), finish_reason="stop"
                    )
                ],
            )
        )

    async def test_chat_extract_persist_restart_and_recall_without_trigger_words(self):
        await self.submit(self.physical(TEXT, "learn-one"))
        await self.eventually(
            lambda: any(
                item["state"] == "completed"
                for item in self.core.store.list("metadata", "memory:learning")
            ),
            timeout=30,
        )
        tasks = self.core.store.list("metadata", "memory:learning")
        self.assertEqual(tasks[0]["receipt"]["state"], "committed")
        extraction_count = sum(
            body["messages"][0]["content"].startswith("Extract only durable facts")
            for body in self.model_requests
        )
        self.assertEqual(extraction_count, 1)
        # Discard short adjacency so the next model prompt can only get this fact from Memory.
        from tianshu_companion.short_context import ShortContextPolicy

        await self.restart_core()
        self.core.short_context_policy = ShortContextPolicy(max_turns=0)
        await self.submit(self.physical("第一支钢笔用什么墨水", "recall-two"))

        def recalled():
            for body in self.model_requests:
                if body["messages"][0]["content"].startswith("Extract only durable facts"):
                    continue
                content = json.loads(body["messages"][1]["content"])
                if any(unit["statement"] == TEXT for unit in content.get("evidence", [])):
                    return True
            return False

        await self.eventually(recalled, timeout=30)
        self.assertTrue(recalled())

    async def test_automatic_learning_preserves_recent_dialogue_after_restart(self):
        await self.submit(self.physical(TEXT, "context-first"))
        await self.eventually(
            lambda: any(
                item["state"] == "completed"
                for item in self.core.store.list("metadata", "memory:learning")
            ),
            timeout=30,
        )
        await self.eventually(
            lambda: any(turn["phase"] == "sent" for turn in self.core.store.list("turns")),
            timeout=30,
        )
        first = self.core.store.list("turns")[0]
        self.assertIsNotNone(first.get("history_snapshot"))
        await self.restart_core()
        count = len(self.model_requests)
        await self.submit(self.physical("刚才那支呢？", "context-next"))

        def followup():
            return [
                body
                for body in self.model_requests[count:]
                if not body["messages"][0]["content"].startswith("Extract only durable facts")
            ]

        await self.eventually(followup, timeout=30)
        prompt = json.loads(followup()[0]["messages"][1]["content"])
        self.assertIn(TEXT, json.dumps(prompt["recent_dialogue"], ensure_ascii=False))
        self.assertGreater(
            self.core.store.list("turns")[-1]["scope_version"], first["scope_version"]
        )
