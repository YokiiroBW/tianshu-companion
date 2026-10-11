"""Actual Platform/Core/Memory/Gateway over loopback TLS; synthetic model/channel only.

Run explicitly with the existing TS050 integration fixture on PYTHONPATH.
"""

import json

import pytest

SourceChain = pytest.importorskip("ts050_source_support").SourceChain
web = pytest.importorskip("aiohttp.web")

TEXT = "晚上不喝咖啡，白天偶尔可以"


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
        return config

    def make_core_config(self):
        config = super().make_core_config()
        for binding in config["bindings"].values():
            binding["namespace"] = "web"
        return config

    def make_memory_config(self):
        config = super().make_memory_config()
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
        return await super().start_asgi(app, owner, sock)

    async def record_model(self, request):
        body = await request.json()
        self.model_requests.append(body)
        extraction = body["messages"][0]["content"].startswith("Extract durable memories")
        if extraction:
            content = json.loads(body["messages"][-1]["content"])
            memories = (
                [
                    dict(
                        statement=TEXT,
                        conditions=["白天偶尔可以"],
                        negations=["晚上不喝咖啡"],
                        valid_time="current",
                        evidence=[dict(source_index=0, quote=TEXT)],
                    )
                ]
                if TEXT in content[0]["text"]
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
            body["messages"][0]["content"].startswith("Extract durable memories")
            for body in self.model_requests
        )
        self.assertEqual(extraction_count, 1)
        # Discard short adjacency so the next model prompt can only get this fact from Memory.
        from tianshu_companion.short_context import ShortContextPolicy

        await self.restart_core()
        self.core.short_context_policy = ShortContextPolicy(max_turns=0)
        await self.submit(self.physical("咖啡适合晚上喝吗", "recall-two"))

        def recalled():
            for body in self.model_requests:
                if body["messages"][0]["content"].startswith("Extract durable memories"):
                    continue
                content = json.loads(body["messages"][1]["content"])
                if any(unit["statement"] == TEXT for unit in content.get("evidence", [])):
                    return True
            return False

        await self.eventually(recalled, timeout=30)
        self.assertTrue(recalled())
