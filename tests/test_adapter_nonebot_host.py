"""Real NoneBot 2.5/OneBot 2.4 plugin load and loopback ASGI server."""

import asyncio
import importlib
import importlib.util
import os
import socket
import tempfile
import unittest


@unittest.skipUnless(importlib.util.find_spec("nonebot") and importlib.util.find_spec("uvicorn")
                     and importlib.util.find_spec("tianshu_nonebot"),
                     "NoneBot host extras are not installed")
class NoneBotHostTests(unittest.TestCase):
    def test_framework_load_route_lifecycle_and_sdk_boundary(self):
        async def run():
            import httpx
            import nonebot
            import uvicorn
            from nonebot.adapters.onebot.v11 import Adapter, Bot, PrivateMessageEvent
            from nonebot.message import handle_event

            with tempfile.TemporaryDirectory() as directory:
                before = os.getcwd()
                os.chdir(directory)
                try:
                    nonebot.init(driver="~fastapi")
                    driver = nonebot.get_driver()
                    driver.register_adapter(Adapter)
                    plugin = nonebot.load_plugin("tianshu_nonebot.adapter_plugin")
                    self.assertIsNotNone(plugin)
                    with self.assertRaisesRegex(RuntimeError, "cannot share"):
                        importlib.import_module("tianshu_nonebot.plugin")
                    import tianshu_nonebot.adapter_plugin as host
                    self.assertEqual(sum(getattr(route, "path", "").startswith("/tianshu/adapter/v1")
                                         for route in nonebot.get_asgi().routes), 7)
                    self.assertEqual(await host._accounts(), [])
                    adapter = Adapter(driver)
                    bot = Bot(adapter, "42")
                    sent = []

                    async def login():
                        return {"user_id": 42, "nickname": "synthetic"}

                    async def native_send(**kwargs):
                        sent.append(kwargs)
                        return {"message_id": 88}

                    bot.get_login_info = login
                    bot.send_private_msg = native_send
                    adapter.bot_connect(bot)

                    sock = socket.socket()
                    sock.bind(("127.0.0.1", 0))
                    sock.listen(128)
                    port = sock.getsockname()[1]
                    server = uvicorn.Server(uvicorn.Config(nonebot.get_asgi(), log_level="error",
                                                           lifespan="on"))
                    task = asyncio.create_task(server.serve(sockets=[sock]))
                    for _ in range(100):
                        if server.started:
                            break
                        await asyncio.sleep(0.02)
                    self.assertTrue(server.started)
                    key = host.service.access_key
                    headers = {"Authorization": "Bearer " + key}
                    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=3) as client:
                        async def post(route, data, auth=headers):
                            return await client.post("/tianshu/adapter/v1" + route, json=data,
                                                     headers=auth)

                        self.assertEqual((await post("/capabilities", {}, {"Authorization": "Bearer wrong"})).status_code, 401)
                        caps = (await post("/capabilities", {})).json()
                        self.assertEqual(caps["adapter"], "nonebot")
                        self.assertEqual(caps["accounts"], [{"id": "42", "platform": "qq", "label": "synthetic"}])
                        self.assertEqual((await client.post("/tianshu/adapter/v2/capabilities", json={}, headers=headers)).status_code, 404)
                        binding = dict(request_id="a1", connection_id="c1", revision=1,
                            account_id="42", conversation={"kind": "private", "id": "7"},
                            allowed_authors=["7"], enabled=True)
                        self.assertEqual((await post("/bindings/apply", binding)).status_code, 200)
                        def event(user, mid):
                            return PrivateMessageEvent.model_validate({
                                "time": 1700000000, "self_id": 42, "post_type": "message",
                                "sub_type": "friend", "user_id": user, "message_type": "private",
                                "message_id": mid, "message": [{"type": "text", "data": {"text": "hello"}}],
                                "original_message": [{"type": "text", "data": {"text": "hello"}}],
                                "raw_message": "hello", "font": 0, "sender": {"user_id": user},
                                "to_me": True,
                            })
                        await handle_event(bot, event(8, 1))
                        self.assertEqual((await post("/events/poll", {"connection_id": "c1", "limit": 20})).json()["events"], [])
                        await handle_event(bot, event(7, 2))
                        polled = (await post("/events/poll", {"connection_id": "c1", "limit": 20})).json()
                        self.assertEqual(len(polled["events"]), 1)
                        self.assertEqual(polled["events"][0]["event"]["event_id"], "2")
                        event_id = polled["events"][0]["id"]
                        self.assertEqual((await post("/events/ack", {"connection_id": "c1", "event_ids": [event_id]})).json()["acknowledged"], [event_id])
                        delivery = dict(reply_id="reply", attempt_id="attempt", namespace="qq",
                                        conversation_id="private:7", thread_id=None, text="answer",
                                        turn_id="turn", segment_sequence=1)
                        receipt = (await post("/messages/send", {"connection_id": "c1", "delivery": delivery})).json()
                        self.assertEqual(receipt["state"], "sent")
                        self.assertEqual(receipt["channel_message_ids"], ["88"])
                        self.assertEqual((await post("/messages/send", {"connection_id": "c1", "delivery": delivery})).json(), receipt)
                        self.assertEqual(len(sent), 1)
                        failed_calls = []
                        async def uncertain_send(**kwargs):
                            failed_calls.append(kwargs)
                            raise TimeoutError()
                        bot.send_private_msg = uncertain_send
                        uncertain = {**delivery, "attempt_id": "uncertain"}
                        self.assertEqual((await post("/messages/send", {"connection_id": "c1", "delivery": uncertain})).json()["state"], "unknown")
                        self.assertEqual((await post("/messages/send", {"connection_id": "c1", "delivery": uncertain})).json()["state"], "unknown")
                        self.assertEqual(len(failed_calls), 1)
                        binding.update(request_id="a2", revision=2, enabled=False)
                        self.assertEqual((await post("/bindings/apply", binding)).status_code, 200)
                        await handle_event(bot, event(7, 3))
                        self.assertEqual((await post("/events/poll", {"connection_id": "c1", "limit": 20})).json()["events"], [])
                        self.assertEqual((await post("/messages/send", {"connection_id": "c1", "delivery": {**delivery, "attempt_id": "new"}})).status_code, 403)
                    server.should_exit = True
                    await asyncio.wait_for(task, 5)
                    self.assertFalse(any(getattr(route, "path", "").startswith("/tianshu/adapter/v1")
                                         for route in nonebot.get_asgi().routes))
                finally:
                    os.chdir(before)
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
