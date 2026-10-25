"""Actual Platform/Core/Memory/Gateway over loopback TLS; synthetic model/channel only.

Run explicitly with the existing TS050 integration fixture on PYTHONPATH.
"""

import json
import os

import pytest

SourceChain = pytest.importorskip("ts050_source_support").SourceChain
web = pytest.importorskip("aiohttp.web")

TEXT = "第一支钢笔用蓝黑墨水"


class ConsolidationJointBase(SourceChain):
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
            self.memory.store.migrate_facts(self.directory / "backups/facts.sqlite")
            self.memory.contracts.load_context()
            self.memory.store.migrate_memory(self.directory / "backups/consolidation.sqlite")
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

    @staticmethod
    def model_material(body):
        return json.loads(body["messages"][-1]["content"])

    def consolidation_requests(self):
        return [
            body
            for body in self.model_requests
            if self.model_material(body).get("extraction_schema") == 5
        ]

    async def submit_and_learn(self, text, key):
        before = {item["id"] for item in self.core.store.list("metadata", "memory:learning")}
        await self.submit(self.physical(text, key))

        def completed():
            return [
                item
                for item in self.core.store.list("metadata", "memory:learning")
                if item["id"] not in before and item["state"] == "completed"
            ]

        await self.eventually(completed, timeout=30)
        result = completed()[0]
        self.assertEqual(5, result["extraction_schema"])
        self.assertEqual("saved", result["receipt"]["experience_state"])
        return result

    async def recall(self, text, intent="current"):
        turn = self.core.store.list("turns")[-1]
        return await self.core.memory.query_context(
            turn["origin"],
            turn["scope"],
            text,
            dict(tokens=8192, bytes=8192),
            queries=[
                dict(
                    query_id="joint",
                    text=text,
                    kinds=["facts", "episodes"],
                    intent=intent,
                    time_filter=None,
                )
            ],
        )

    def summary_output(self, material):
        summary = "\n".join(
            row["text"]
            for row in material["conversation"]
            if row["role"] == "user" and row["purpose"] == "new"
        )
        portrait = None
        if material["portrait_input"] is not None:
            portrait = "\n".join(
                [
                    *(
                        member["text"]
                        for row in material["portrait_input"]["summaries"]
                        for member in row["subject_material"]
                    ),
                    summary,
                ]
            )
        return dict(
            subject_person_id=material["subject"]["person_id"],
            summary=summary,
            portrait=portrait,
            importance="normal",
        )

    async def record_model(self, request):
        body = await request.json()
        self.model_requests.append(body)
        content = self.model_material(body)
        extraction = content.get("extraction_schema") == 5
        if extraction:
            text = json.dumps(self.summary_output(content), ensure_ascii=False)
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


class MemoryLearningJoint(ConsolidationJointBase):
    async def test_chat_extract_persist_restart_and_recall_without_trigger_words(self):
        result = await self.submit_and_learn(TEXT, "learn-one")
        self.assertEqual("committed", result["receipt"]["summary_state"])
        self.assertEqual(1, len(self.consolidation_requests()))
        # Discard adjacency so this supplied account must come from Memory.
        from tianshu_companion.short_context import ShortContextPolicy

        await self.restart_core()
        self.core.short_context_policy = ShortContextPolicy(max_turns=0)
        await self.submit(self.physical("第一支钢笔用什么墨水", "recall-two"))

        def recalled():
            for body in self.model_requests:
                if self.model_material(body).get("extraction_schema") == 5:
                    continue
                content = json.loads(body["messages"][1]["content"])
                if any(unit["statement"] == TEXT for unit in content.get("evidence", [])):
                    return True
            return False

        await self.eventually(recalled, timeout=30)
        self.assertTrue(recalled())

    async def test_automatic_learning_preserves_recent_dialogue_after_restart(self):
        await self.submit_and_learn(TEXT, "context-first")
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
                if self.model_material(body).get("extraction_schema") != 5
            ]

        await self.eventually(followup, timeout=30)
        prompt = json.loads(followup()[0]["messages"][1]["content"])
        self.assertIn(TEXT, json.dumps(prompt["recent_dialogue"], ensure_ascii=False))
        self.assertGreater(
            self.core.store.list("turns")[-1]["scope_version"], first["scope_version"]
        )
