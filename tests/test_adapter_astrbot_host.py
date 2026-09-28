"""AstrBot 4.27.3 PluginManager load/reload/unload and loopback HTTP."""

import asyncio
import importlib.util
import json
import os
import socket
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace


@unittest.skipUnless(importlib.util.find_spec("astrbot") and importlib.util.find_spec("httpx"),
                     "AstrBot host extras are not installed")
class AstrBotHostTests(unittest.TestCase):
    def test_real_manager_lifecycle_and_native_sdk_boundary(self):
        async def run():
            import httpx

            artifact = Path(__file__).resolve().parents[1] / ".runtime" / "adapter-artifacts" / "astrbot_plugin_tianshu-0.2.0.zip"
            with tempfile.TemporaryDirectory() as root:
                before = os.getcwd()
                old_root = os.environ.get("ASTRBOT_ROOT")
                os.environ["ASTRBOT_ROOT"] = root
                os.chdir(root)
                sys.path.insert(0, root)
                manager = None
                metadata = None
                try:
                    from astrbot.core.star.context import Context
                    from astrbot.core.star.star_manager import PluginManager
                    with zipfile.ZipFile(artifact) as archive:
                        archive.extractall(Path(root) / "data" / "plugins")
                    config_dir = Path(root) / "data" / "config"
                    config_dir.mkdir(parents=True)
                    with socket.socket() as sock:
                        sock.bind(("127.0.0.1", 0))
                        port = sock.getsockname()[1]
                    (config_dir / "astrbot_plugin_tianshu_config.json").write_text(
                        json.dumps({"enabled": False, "adapter_port": port}), encoding="utf-8")
                    platforms = SimpleNamespace(platform_insts=[])
                    context = Context(asyncio.Queue(), {}, None, None, platforms,
                                      None, None, None, None, None, None)
                    manager = PluginManager(context, {})
                    loaded, error = await manager.load(specified_dir_name="astrbot_plugin_tianshu")
                    self.assertTrue(loaded, error)
                    metadata = next(item for item in context.get_all_stars()
                                    if item.root_dir_name == "astrbot_plugin_tianshu")
                    plugin = metadata.star_cls
                    self.assertIsNotNone(plugin._adapter)
                    await plugin.on_astrbot_loaded()
                    for _ in range(100):
                        if plugin._adapter.runner is not None:
                            break
                        await asyncio.sleep(0.02)
                    self.assertIsNotNone(plugin._adapter.runner)
                    self.assertEqual(await plugin._adapter.accounts(), [])

                    calls = []
                    class Client:
                        _wsr_api_clients = {"42": object()}
                        async def call_action(self, action, **params):
                            self_id = params.get("self_id")
                            return {"user_id": int(self_id), "nickname": "synthetic"}
                        async def send_private_msg(self, **kwargs):
                            calls.append(kwargs)
                            return {"message_id": 55}
                    class Platform:
                        def __init__(self):
                            self.client = Client()
                        def meta(self):
                            return SimpleNamespace(name="aiocqhttp", id="host1")
                        def get_client(self):
                            return self.client
                    platforms.platform_insts.append(Platform())
                    key = plugin._adapter.service.access_key
                    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=3) as client:
                        async def post(route, body, secret=key):
                            return await client.post("/tianshu/adapter/v1" + route, json=body,
                                headers={"Authorization": "Bearer " + secret})
                        self.assertEqual((await post("/capabilities", {}, "bad")).status_code, 401)
                        caps = (await post("/capabilities", {})).json()
                        self.assertEqual(caps["accounts"], [{"id": "42", "platform": "qq", "label": "synthetic"}])
                        platforms.platform_insts.append(Platform())
                        self.assertEqual((await post("/capabilities", {})).json()["accounts"], [])
                        platforms.platform_insts.pop()
                        binding = dict(request_id="one", connection_id="conn", revision=1,
                            account_id="42", conversation={"kind": "private", "id": "7"},
                            allowed_authors=["7"], enabled=True)
                        self.assertEqual((await post("/bindings/apply", binding)).status_code, 200)
                        class Event:
                            def __init__(self, author):
                                self.author = author
                                self.stopped = False
                                self.message_obj = SimpleNamespace(message_id="9", raw_message={
                                    "post_type": "message", "message_type": "private",
                                    "self_id": 42, "user_id": int(author), "message_id": 9,
                                    "time": 1700000000,
                                    "message": [{"type": "text", "data": {"text": "hello"}}],
                                })
                            def get_platform_name(self): return "aiocqhttp"
                            def get_platform_id(self): return "host1"
                            def get_self_id(self): return "42"
                            def get_sender_id(self): return self.author
                            def get_group_id(self): return None
                            def stop_event(self): self.stopped = True
                        outsider = Event("8")
                        await plugin.on_message(outsider)
                        self.assertFalse(outsider.stopped)
                        incoming = Event("7")
                        await plugin.on_message(incoming)
                        self.assertTrue(incoming.stopped)
                        events = (await post("/events/poll", {"connection_id": "conn", "limit": 20})).json()["events"]
                        self.assertEqual(len(events), 1)
                        self.assertEqual(events[0]["event"]["event_id"], "9")
                        delivery = dict(reply_id="reply", attempt_id="attempt", namespace="qq",
                            conversation_id="private:7", thread_id=None, text="answer",
                            turn_id="turn", segment_sequence=1)
                        sent = (await post("/messages/send", {"connection_id": "conn", "delivery": delivery})).json()
                        self.assertEqual(sent["channel_message_ids"], ["55"])
                        self.assertEqual(len(calls), 1)
                        async def uncertain_send(**kwargs):
                            calls.append(kwargs)
                            raise TimeoutError()
                        platforms.platform_insts[0].client.send_private_msg = uncertain_send
                        uncertain = {**delivery, "attempt_id": "uncertain"}
                        self.assertEqual((await post("/messages/send", {"connection_id": "conn", "delivery": uncertain})).json()["state"], "unknown")
                        self.assertEqual((await post("/messages/send", {"connection_id": "conn", "delivery": uncertain})).json()["state"], "unknown")
                        self.assertEqual(len(calls), 2)
                    reloaded, error = await manager.reload("astrbot_plugin_tianshu")
                    self.assertTrue(reloaded, error)
                    metadata = next(item for item in context.get_all_stars()
                                    if item.root_dir_name == "astrbot_plugin_tianshu")
                    plugin = metadata.star_cls
                    await plugin.on_astrbot_loaded()
                    for _ in range(100):
                        if plugin._adapter.runner is not None:
                            break
                        await asyncio.sleep(0.02)
                    self.assertEqual(plugin._adapter.service.access_key, key)
                    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=3) as client:
                        replay = await client.post("/tianshu/adapter/v1/messages/send",
                            json={"connection_id": "conn", "delivery": delivery},
                            headers={"Authorization": "Bearer " + key})
                        self.assertEqual(replay.json(), sent)
                    self.assertEqual(len(calls), 2)
                    await manager._terminate_plugin(metadata)
                    await manager._unbind_plugin(metadata.name, metadata.module_path)
                    metadata = None
                    with socket.socket() as sock:
                        self.assertNotEqual(sock.connect_ex(("127.0.0.1", port)), 0)
                finally:
                    if manager and metadata is not None:
                        await manager._terminate_plugin(metadata)
                        await manager._unbind_plugin(metadata.name, metadata.module_path)
                    if "astrbot.core" in sys.modules:
                        from astrbot.core import db_helper, sp
                        await sp.close()
                        await db_helper.engine.dispose()
                    sys.path.remove(root)
                    os.chdir(before)
                    if old_root is None:
                        os.environ.pop("ASTRBOT_ROOT", None)
                    else:
                        os.environ["ASTRBOT_ROOT"] = old_root
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
