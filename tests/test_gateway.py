from support import native_sse
import json
import unittest

import httpx

from support import Harness
from tianshu_companion.clients import Gateway, JsonService, utc
from tianshu_companion.contracts import Fault


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_generation_and_separate_route_receipt(self):
        h = Harness()
        requests = []
        receipt_override = {}
        native_override = {}

        def handler(request):
            requests.append(request)
            if request.url.path == "/v1/chat/completions":
                body = json.loads(request.content)
                self.assertNotIn("model", body)
                self.assertNotIn("turn_id", body)
                self.assertEqual("companion.text", request.headers["X-Tianshu-Workload"])
                self.assertEqual("7", request.headers["X-Tianshu-Config-Version"])
                response = dict(
                    id="synthetic:completion",
                    object="chat.completion",
                    choices=[
                        dict(
                            message=dict(role="assistant", content="合成网关回复"),
                            finish_reason="stop",
                        )
                    ],
                    usage=None,
                )
                response.update(native_override)
                return httpx.Response(200, json=response)
            response = dict(
                schema_version=1,
                request_id=request.url.path.rsplit("/", 1)[-1],
                config_version=7,
                provider_id="provider:synthetic",
                requested_model=None,
                resolved_model="synthetic-model",
                protocol="openai-chat-completions",
                outcome="succeeded",
                upstream_request_id="upstream:synthetic",
                usage=None,
                fallback_used=False,
                observed_at=utc(h.clock()),
                credential_namespace="fixture:namespace",
                caller_service="companion",
                requested_reasoning={},
                effective_reasoning={},
                applied_policies=[dict(field="model", mode="workload_binding", config_version=7)],
                usage_complete=False,
                native_usage=None,
            )
            response.update(receipt_override)
            return httpx.Response(200, json=response)

        client = JsonService(
            "https://gateway.synthetic.invalid",
            "synthetic-only",
            transport=httpx.MockTransport(native_sse(handler)),
        )
        try:
            gateway = Gateway(h.contracts, client)
            turn = dict(
                id="turn:synthetic",
                config_version=7,
                conversation_id="conversation:synthetic",
                scope={"actor_id": "actor:synthetic"},
            )
            messages = [dict(role="user", content="Hello")]
            segments, route = await gateway.generate(turn, messages)
            self.assertEqual(["合成网关回复"], segments)
            self.assertIsNone(route["usage"])
            self.assertEqual(2, len(requests))
            session = requests[0].headers["X-Tianshu-Provider-Session"]
            self.assertNotIn("actor:synthetic", session)
            await gateway.generate({**turn, "id": "turn:next"}, messages)
            self.assertEqual(requests[2].headers["X-Tianshu-Provider-Session"], session)
            await gateway.generate({**turn, "conversation_id": "conversation:other"}, messages)
            self.assertNotEqual(requests[4].headers["X-Tianshu-Provider-Session"], session)
            await gateway.generate({**turn, "scope": {"actor_id": "actor:other"}}, messages)
            self.assertNotEqual(requests[6].headers["X-Tianshu-Provider-Session"], session)
            receipt_override["config_version"] = 8
            with self.assertRaises(Fault):
                await gateway.generate(turn, messages)
            receipt_override.clear()
            native_override["choices"] = [
                dict(message=dict(content="partial"), finish_reason="length")
            ]
            with self.assertRaises(Fault):
                await gateway.generate(turn, messages)
            native_override["choices"] = [
                dict(message=dict(content="", tool_calls=[{}]), finish_reason="stop")
            ]
            with self.assertRaises(Fault):
                await gateway.generate(turn, messages)
        finally:
            await client.close()
            await h.core.close()
