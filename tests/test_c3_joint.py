"""Five real TLS product owners; only the external paid model is a recording substitute.

Run explicitly with Platform tests/backend/run_c4_joint.ps1. Ordinary component tests
skip this module when the joint deployment fixture is not on their import path.
"""

import asyncio
import base64
import copy
import hashlib
import json
import uuid

import pytest
from aiohttp import web
from tianshu_companion.clients import utc

CompleteJoint = pytest.importorskip("test_c4_joint").CompleteJoint


class CompanionJoint(CompleteJoint):
    def make_core_config(self):
        config = super().make_core_config()
        config["bot_binding_management_enabled"] = True
        return config

    def configure_gateway(self, settings):
        # Native Chat JSON/SSE uses the existing ClientGrant, independently of Responses.
        settings.max_request_bytes = 4_194_304

    async def upload(self, raw, filename="joint.txt", media_type="text/plain"):
        descriptor = await self.web(
            "content/uploads",
            dict(
                actor_id=self.actor,
                client_id=str(uuid.uuid4()),
                value=dict(
                    filename=filename,
                    media_type=media_type,
                    size=len(raw),
                    sha256=hashlib.sha256(raw).hexdigest(),
                ),
            ),
        )
        response = await self.client.post(
            self.platform_url + "/api/web/content/upload/" + descriptor["upload_id"],
            content=raw,
            headers={
                **self.web_headers,
                "Content-Type": "application/octet-stream",
                "X-Tianshu-Actor-Id": self.actor,
                "X-Tianshu-Request-Id": "upload:" + uuid.uuid4().hex,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        acquired = await self.manage(
            "content.acquire",
            dict(
                query={},
                scope={},
                source=dict(kind="upload", upload_id=descriptor["upload_id"]),
                purpose="read",
            ),
        )
        return acquired["result"]["content_ref"]

    async def test_acquire_reading_progress_and_current_draft_over_http(self):
        await self.add_role()
        today = await self.web("life/today", dict(actor_id=self.actor))
        self.assertTrue(today["plan"]["entries"])
        raw = "这是实际原文。\n逐段共读应保存已经实际读到的位置。".encode()
        reference = await self.upload(raw)
        opened = await self.manage(
            "reading.open",
            dict(
                id="reading:joint",
                query={},
                scope={},
                content_ref=reference,
                mode="together",
                participants=[],
            ),
        )
        self.assertEqual(opened["result"]["state"], "open")
        result = await self.web(
            "life/runtime/content",
            dict(
                actor_id=self.actor,
                content_ref=reference,
                reading_id="reading:joint",
                range=dict(unit="characters", start=0, end=8),
            ),
        )
        self.assertEqual(result["text"], raw.decode()[:8])
        self.assertTrue(result["complete"])
        session = (await self.read("reading", "reading:joint"))["items"][0]
        self.assertEqual(session["position"]["end"], 8)
        self.assertEqual(session["version"], 2)
        await self.restart_core()
        again = (await self.read("reading", "reading:joint"))["items"][0]
        self.assertEqual(again["coverage"], session["coverage"])
        work = await self.manage(
            "writing.create",
            dict(
                id="writing:joint",
                title="真实原文续接",
                outline="沿章节原文继续",
                characters=[],
                recipe="chapter",
            ),
        )
        self.assertEqual(work["result"]["id"], "writing:joint")
        added = await self.manage(
            "chapter.add",
            dict(
                id="chapter:joint",
                work_id="writing:joint",
                title="第一章",
                goal="保留完整正文",
                order=None,
            ),
            work["result"]["version"],
        )
        draft = "一段未经发布的完整原文，不能用摘要替代。"
        await self.manage(
            "chapter.revise",
            dict(id="chapter:joint", content=draft, reason="作者审阅"),
            added["result"]["version"],
        )
        current = (await self.read("chapter", "chapter:joint", chapter_view="current"))["items"][0]
        self.assertEqual(current["content"], draft)
        await self.web(
            "life/runtime/read",
            dict(
                actor_id=self.actor,
                resource="chapter",
                object_id="chapter:joint",
                expected_version=None,
                limit=20,
                after=None,
            ),
            expected=404,
        )

    async def enable_dialogue(self):
        await self.add_role()
        body = {
            **self.role_body,
            "actor_id": self.actor,
            "client_id": str(uuid.uuid4()),
            "expected_version": 1,
            "capabilities": ["dialogue", "memory.read", "memory.write"],
            "provider_id": self.providers[0]["provider_id"],
            "provider_revision": 1,
        }
        await self.web("roles/apply", body)
        session = (await self.client.get(self.platform_url + "/api/web/session")).json()
        self.conversation = next(
            item["id"] for item in session["conversations"] if self.actor in item["actors"]
        )

    async def say(self, text):
        await self.web(
            "messages",
            dict(
                actor=self.actor,
                conversation=self.conversation,
                text=text,
                client_id=str(uuid.uuid4()),
            ),
        )
        async with asyncio.timeout(30):
            while True:
                result = await self.web(
                    "snapshot", dict(actor=self.actor, conversation=self.conversation, before=None)
                )
                snapshot = result["snapshot"]
                if (
                    snapshot["history"]
                    and snapshot["history"][0]["messages"][-1]["parts"][0]["text"] == text
                ):
                    return snapshot["history"][0]
                await asyncio.sleep(0.1)

    async def record_model(self, request):
        self.assertEqual(request.headers.get("Authorization"), self.bearer("MODEL"))
        body = await request.json()
        self.model_requests.append(dict(body=body, kind="native-chat"))
        messages = body["messages"]
        tool = next((m for m in reversed(messages) if m["role"] == "tool"), None)
        if tool:
            actual = json.loads(tool["content"])
            content = "这次写入的实际回执是 " + actual["result"].get("state", "rejected")
            message = dict(role="assistant", content=content)
            finish = "stop"
        else:
            material = json.loads(messages[1]["content"])
            prose = (material.get("messages") or [{}])[-1].get("parts", [{"text": ""}])[0]["text"]
            if "summary" in material:
                message = dict(
                    role="assistant",
                    content=json.dumps(
                        dict(send=True, text="刚完成这件事，想和你分享。"), ensure_ascii=False
                    ),
                )
                finish = "stop"
            elif prose == "请实际读这张原图":
                values = dict(
                    expected_version=1,
                    value=dict(
                        id="reading:vision", range=dict(unit="bytes", start=0, end=self.vision_size)
                    ),
                )
                message = dict(
                    role="assistant",
                    content=None,
                    tool_calls=[
                        dict(
                            id="tool:vision",
                            type="function",
                            function=dict(name="life_reading_read", arguments=json.dumps(values)),
                        )
                    ],
                )
                finish = "tool_calls"
            elif prose in {"记住：我白天喝咖啡", "纠正记忆：我只在白天喝咖啡"}:
                target = dict(
                    category="evidence",
                    field_key="coffee",
                    item_key=None,
                    record_id=None,
                    expected_version=None,
                )
                kind = "upsert"
                if prose.startswith("纠正"):
                    unit = material["evidence"][0]
                    target.update(
                        record_id=unit["record_id"], expected_version=unit["record_version"]
                    )
                    kind = "correct"
                values = dict(
                    kind=kind,
                    target=target,
                    units=[
                        dict(
                            statement="我只在白天喝咖啡" if kind == "correct" else "我白天喝咖啡",
                            conditions=["白天"],
                            negations=["晚上不喝"] if kind == "correct" else [],
                            valid_time="current",
                            uncertainty="confirmed",
                            reality="real",
                        )
                    ],
                    intent=dict(source_quote=prose, unambiguous_target=True),
                )
                message = dict(
                    role="assistant",
                    content=None,
                    tool_calls=[
                        dict(
                            id="tool:" + uuid.uuid4().hex,
                            type="function",
                            function=dict(
                                name="memory_propose",
                                arguments=json.dumps(values, ensure_ascii=False),
                            ),
                        )
                    ],
                )
                finish = "tool_calls"
            else:
                message = dict(
                    role="assistant",
                    content="你指的是哪一条？" if prose == "不是这个" else "记得下次带钥匙。",
                )
                finish = "stop"
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        if getattr(self, "held_model", False):
            chunk = dict(
                choices=[
                    dict(
                        index=0,
                        delta=dict(
                            content="已经实际受理的首段："
                            + "原文接续需要维持实际阅读位置" * 5
                            + "。"
                        ),
                        finish_reason=None,
                    )
                ]
            )
            await response.write(
                ("data: " + json.dumps(chunk, ensure_ascii=False) + "\n\n").encode()
            )
            # The gateway withholds a credential-sized byte tail. A second genuine
            # delta flushes the first complete SSE event while generation remains open.
            continuation = dict(
                choices=[
                    dict(index=0, delta=dict(content="续写尚在生成中，" * 5), finish_reason=None)
                ]
            )
            await response.write(
                ("data: " + json.dumps(continuation, ensure_ascii=False) + "\n\n").encode()
            )
            self.model_started.set()
            await self.model_release.wait()
            try:
                await response.write(b"data: [DONE]\n\n")
            except ConnectionError:
                pass
            return response
        delta = {key: value for key, value in message.items() if key != "role"}
        if delta.get("tool_calls"):
            for index, item in enumerate(delta["tool_calls"]):
                item["index"] = index
        for chunk in [
            dict(choices=[dict(index=0, delta=delta, finish_reason=None)]),
            dict(choices=[dict(index=0, delta={}, finish_reason=finish)]),
        ]:
            await response.write(
                ("data: " + json.dumps(chunk, ensure_ascii=False) + "\n\n").encode()
            )
        await response.write(b"data: [DONE]\n\n")
        await response.write_eof()
        return response

    async def test_native_dialogue_correction_receipts_and_semantic_negatives(self):
        await self.enable_dialogue()
        first = await self.say("记住：我白天喝咖啡")
        self.assertTrue(any("committed" in reply["text"] for reply in first["replies"]))
        second = await self.say("纠正记忆：我只在白天喝咖啡")
        self.assertTrue(any("corrected" in reply["text"] for reply in second["replies"]))
        self.assertTrue(
            any(
                m["role"] == "tool" and m.get("tool_call_id")
                for call in self.model_requests
                for m in call["body"]["messages"]
            )
        )
        count = sum(
            bool(m.get("tool_calls"))
            for call in self.model_requests
            for m in call["body"]["messages"]
        )
        await self.say("忘记带钥匙")
        await self.say("不是这个")
        newer = sum(
            bool(m.get("tool_calls"))
            for call in self.model_requests
            for m in call["body"]["messages"]
        )
        self.assertEqual(newer, count)

    async def test_actual_original_vision_reaches_native_tool_continuation(self):
        from test_images import png

        await self.enable_dialogue()
        raw = png()
        self.vision_size = len(raw)
        reference = await self.upload(raw, "original.png", "image/png")
        await self.manage(
            "reading.open",
            dict(
                id="reading:vision",
                query={},
                scope={},
                content_ref=reference,
                mode="together",
                participants=[],
            ),
        )
        await self.say("请实际读这张原图")
        images = [
            part["image_url"]["url"]
            for call in self.model_requests
            for message in call["body"]["messages"]
            if isinstance(message.get("content"), list)
            for part in message["content"]
            if part["type"] == "image_url"
        ]
        self.assertEqual(len(images), 1)
        self.assertEqual(base64.b64decode(images[0].split(",", 1)[1]), raw)
        tools = [
            message
            for call in self.model_requests
            for message in call["body"]["messages"]
            if message["role"] == "tool"
        ]
        actual = json.loads(tools[0]["content"])["result"]["actual_content"]
        self.assertTrue(actual["complete"])
        self.assertEqual(actual["gaps"], [])

    async def test_cancel_native_stream_preserves_accepted_segment_and_gateway_terminal(self):
        await self.enable_dialogue()
        self.held_model = True
        self.model_started = asyncio.Event()
        self.model_release = asyncio.Event()

        async def observe_request(request):
            if request.url.path == "/v1/chat/completions":
                self.held_request_id = request.headers["X-Request-ID"]

        self.core.gateway.client.client.event_hooks["request"].append(observe_request)
        await self.web(
            "messages",
            dict(
                actor=self.actor,
                conversation=self.conversation,
                text="请慢慢接续",
                client_id=str(uuid.uuid4()),
            ),
        )
        try:
            async with asyncio.timeout(30):
                await self.model_started.wait()
                while True:
                    turn = next(iter(self.core.store.list("turns")), None)
                    replies = self.core._replies(turn) if turn else []
                    if replies and replies[0]["state"] == "sent":
                        break
                    await asyncio.sleep(0.1)
            before = copy.deepcopy(replies[0])
            result = await self.web(
                "cancel",
                dict(
                    actor=self.actor,
                    conversation=self.conversation,
                    turn_id=turn["id"],
                    expected_version=self.core.store.get("turns", turn["id"])["version"],
                ),
            )
            self.assertIn(result["state"], {"partially_cancelled", "unknown"})
            async with asyncio.timeout(10):
                while True:
                    response = await self.client.get(
                        self.gateway_url + "/internal/v1/model-requests/" + self.held_request_id,
                        headers={"Authorization": self.bearer("GATEWAY_CORE")},
                    )
                    self.assertEqual(response.status_code, 200, response.text)
                    receipt = response.json()
                    if receipt["execution"]["state"] == "cancelled":
                        break
                    await asyncio.sleep(0.1)
            self.assertTrue(receipt["execution"]["cancel_requested"])
            self.assertEqual(receipt["outcome"], "unknown")
            self.assertEqual(self.core._replies(turn)[0]["receipt"], before["receipt"])
            self.assertEqual(len(self.core._replies(turn)), 1)
            self.assertEqual(self.core.store.get("turns", turn["id"])["phase"], "cancelled")
            await self.restart_core()
            await asyncio.sleep(0.2)
            self.assertEqual(len(self.core._replies(turn)), 1)
        finally:
            self.model_release.set()

    async def core_manage(self, operation, value, version=0):
        response = await self.client.post(
            self.core_url + "/internal/v2/life/manage",
            headers={"Authorization": self.bearer("INGRESS")},
            json=dict(
                schema_version=2,
                request_id="joint:" + uuid.uuid4().hex,
                actor_id=self.actor,
                operation=operation,
                expected_version=version,
                value=value,
            ),
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["result"]

    async def start_bot(self):
        session = next(item for item in self.console.sessions.values() if item["authenticated"])
        session["bot_management"] = self.console.clock() + 1800
        self.sdk_events = []
        self.sdk_deliveries = []
        self.sdk_binding = None
        plugin_key = "synthetic-only-proactive-sdk-key"

        async def plugin(request):
            self.assertEqual(request.headers["Authorization"], "Bearer " + plugin_key)
            body = await request.json()
            path = request.path.removeprefix("/tianshu/adapter/v1")
            if path == "/capabilities":
                result = dict(
                    protocol="tianshu.bot-adapter/v1",
                    adapter="nonebot",
                    instance_id="sdk:joint",
                    capabilities=["text"],
                    max_outbound_utf8_bytes=32768,
                    accounts=[dict(id="42", platform="qq", label="Synthetic bot")],
                )
            elif path == "/bindings/apply":
                self.sdk_binding = {
                    key: body[key] for key in ("connection_id", "revision", "enabled")
                }
                result = self.sdk_binding
            elif path == "/bindings/status":
                result = dict(found=self.sdk_binding is not None, binding=self.sdk_binding)
            elif path == "/events/poll":
                result = dict(events=self.sdk_events)
            elif path == "/events/ack":
                self.sdk_events = []
                result = dict(acknowledged=body["event_ids"])
            elif path == "/messages/send":
                delivery = body["delivery"]
                self.sdk_deliveries.append(delivery)
                result = dict(
                    reply_id=delivery["reply_id"],
                    attempt_id=delivery["attempt_id"],
                    state="sent",
                    channel_message_ids=["sdk:" + delivery["reply_id"]],
                )
            elif path == "/messages/status":
                result = dict(found=False, receipt=None)
            else:
                return web.json_response(dict(code="not_found"), status=404)
            return web.json_response(result)

        app = web.Application()
        app.router.add_post("/tianshu/adapter/v1/{tail:.*}", plugin)
        address = await self.start_aio(app)
        adapters = self.platform.bot_adapters
        draft = await adapters.probe(
            dict(
                adapter="nonebot",
                address=address,
                access_key=plugin_key,
                allow_private_http=True,
                ca_pem=self.ca.read_text(encoding="utf-8"),
            ),
            session,
        )
        row = await adapters.create(
            dict(
                draft_id=draft["draft_id"],
                name="Joint proactive bot",
                account_id="42",
                conversation=dict(kind="private", id="7"),
                allowed_authors=["7"],
                actor_id=self.actor,
                client_id=str(uuid.uuid4()),
            ),
            session,
        )
        row = await adapters.change(
            "enable",
            dict(id=row["id"], expected_revision=row["revision"], client_id=str(uuid.uuid4())),
        )
        self.assertEqual(row["state"], "ready")
        event = dict(
            schema_version=1,
            connection_id=row["id"],
            platform_id="sdk:joint",
            self_id="42",
            event_id="sdk:joint:inbound",
            revision=1,
            namespace="qq",
            conversation_id="private:7",
            thread_id=None,
            account_id="7",
            sent_at=utc(self.platform.origins.clock()),
            text="合成账号的实际入站",
        )
        self.sdk_events = [dict(id="event:joint", event=event)]
        async with asyncio.timeout(30):
            while not self.sdk_deliveries:
                await adapters.pump_once()
                await asyncio.sleep(0.1)
        return row

    async def test_proactive_uses_existing_queue_and_actual_adapter_ack(self):
        await self.enable_dialogue()
        row = await self.start_bot()
        turn = next(
            item
            for item in self.core.store.list("turns")
            if item["bundle"]["collection_key"]["channel"]["namespace"] == "qq"
        )
        scope = turn["scope"]
        channel = turn["bundle"]["collection_key"]["channel"]
        subscription = await self.core_manage(
            "proactive.subscription",
            dict(
                person_id=scope["person_id"],
                audience=scope["audience"],
                conversation_id=scope["conversation_id"],
                channel=channel,
                timezone_name="UTC",
                quiet_start="00:00",
                quiet_end="00:00",
                cooldown_seconds=0,
                daily_quota=2,
                unanswered_limit=2,
                expiry_seconds=3600,
                consent_basis="explicit_admin_registration",
                consent_ref="joint:requested",
            ),
        )
        saved = await self.core_manage(
            "activity.save",
            dict(
                id="activity:joint",
                title="完成窗边札记",
                state="completed",
                checkpoint=dict(step=1, position=1, unit="step", note="实际作者管理已完成"),
                next_due_at=None,
                resume_condition=None,
                sources=[],
                result_refs=[],
                scope=None,
            ),
        )
        await self.core_manage(
            "proactive.motive",
            dict(
                id="motive:joint",
                subscription_id=subscription["id"],
                summary="窗边札记已经完成",
                sources=[dict(owner="companion", object_id=saved["id"], version=saved["version"])],
                content_refs=[],
                due_at=self.core.clock(),
                expires_at=self.core.clock() + 3600,
                weight=1.0,
            ),
        )
        self.assertEqual(
            self.core.proactive._quota_used(
                self.core.store.get("proactive_subscriptions", subscription["id"]),
                self.core.clock(),
            ),
            0,
        )
        async with asyncio.timeout(30):
            while len(self.sdk_deliveries) < 2:
                await self.platform.bot_adapters.pump_once()
                await asyncio.sleep(0.1)
        async with asyncio.timeout(10):
            while True:
                candidate = next(
                    item
                    for item in self.core.store.list("proactive_candidates")
                    if item["subject_id"] == "motive:joint"
                )
                if candidate["delivered"]:
                    break
                await asyncio.sleep(0.1)
        self.assertEqual(candidate["state"], "sent")
        self.assertEqual(
            candidate["receipt"]["channel_message_ids"],
            ["sdk:" + self.sdk_deliveries[-1]["reply_id"]],
        )
        self.assertEqual(self.sdk_deliveries[-1]["text"], "刚完成这件事，想和你分享。")
        self.assertEqual(
            self.core.proactive._quota_used(
                self.core.store.get("proactive_subscriptions", subscription["id"]),
                self.core.clock(),
            ),
            1,
        )
        self.assertEqual(len(self.core.store.list("delivery_contacts")), 2)
        await self.restart_core()
        await self.platform.bot_adapters.pump_once()
        await asyncio.sleep(0.2)
        self.assertEqual(len(self.sdk_deliveries), 2)
        self.assertEqual(row["state"], "ready")


for name in dir(CompleteJoint):
    if name.startswith("test_") and name not in CompanionJoint.__dict__:
        setattr(CompanionJoint, name, None)
