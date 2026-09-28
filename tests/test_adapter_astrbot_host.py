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
                    context = Context(asyncio.Queue(), {"dashboard": {"username": "admin"}}, None, None, platforms,
                                      None, None, None, None, None, None)
                    manager = PluginManager(context, {})
                    loaded, error = await manager.load(specified_dir_name="astrbot_plugin_tianshu")
                    self.assertTrue(loaded, error)
                    metadata = next(item for item in context.get_all_stars()
                                    if item.root_dir_name == "astrbot_plugin_tianshu")
                    plugin = metadata.star_cls
                    self.assertIsNotNone(plugin._adapter)
                    self.assertEqual(plugin._adapter.listen_host, "127.0.0.1")
                    from astrbot.dashboard.services.plugin_page_service import PluginPageService
                    pages = PluginPageService(manager)
                    self.assertEqual([page.name for page in await pages.discover_plugin_pages(metadata)],
                                     ["connection"])
                    page_html = await pages.serve_page_content(
                        plugin_name=metadata.name, page_name="connection", asset_path="",
                        asset_token="", jwt_secret="synthetic-test-secret",
                        username="admin", locale="zh-CN", theme=None,
                    )
                    self.assertIn("/api/plugin/page/bridge-sdk.js", page_html.content)
                    self.assertIn("/api/plugin/page/content/", page_html.content)
                    page_script = await pages.serve_page_content(
                        plugin_name=metadata.name, page_name="connection", asset_path="app.js",
                        asset_token="", jwt_secret="synthetic-test-secret",
                        username="admin", locale="zh-CN", theme=None,
                    )
                    self.assertIn('bridge.apiGet("connection-info")', page_script.content)
                    from data.plugins.astrbot_plugin_tianshu.adapter import _listen_host, _private_address
                    self.assertEqual(_listen_host("lan", "192.168.31.210"), "192.168.31.210")
                    self.assertTrue(_private_address("127.0.0.1"))
                    self.assertTrue(_private_address("172.20.0.7"))
                    self.assertFalse(_private_address("8.8.8.8"))
                    self.assertFalse(_private_address("169.254.1.1"))
                    for mode, address in (("lan", "8.8.8.8"), ("lan", "127.0.0.2"),
                                          ("lan", "169.254.1.1"), ("public", "")):
                        with self.assertRaises(ValueError):
                            _listen_host(mode, address)
                    await plugin.on_astrbot_loaded()
                    for _ in range(100):
                        if plugin._adapter.runner is not None:
                            break
                        await asyncio.sleep(0.02)
                    self.assertIsNotNone(plugin._adapter.runner)
                    self.assertEqual(await plugin._adapter.accounts(), [])
                    from fastapi import FastAPI
                    from fastapi.responses import JSONResponse
                    from astrbot.dashboard.api.plugins import legacy_router
                    from astrbot.dashboard.responses import ApiError
                    import jwt
                    dashboard = FastAPI()
                    dashboard.include_router(legacy_router)
                    dashboard.state.core_lifecycle = SimpleNamespace(star_context=context)
                    dashboard.state.jwt_secret = "synthetic-test-secret"
                    dashboard.state.dashboard_app_adapter = None
                    @dashboard.exception_handler(ApiError)
                    async def api_error(_request, error):
                        return JSONResponse({"message": error.message}, status_code=error.status_code)
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=dashboard),
                                                 base_url="http://dashboard.test") as dashboard_client:
                        info_path = "/api/plug/astrbot_plugin_tianshu/connection-info"
                        self.assertEqual((await dashboard_client.get(info_path)).status_code, 401)
                        wrong_user = jwt.encode({"username": "viewer"}, "synthetic-test-secret", algorithm="HS256")
                        self.assertEqual((await dashboard_client.get(info_path,
                            headers={"Authorization": "Bearer " + wrong_user})).status_code, 403)
                        admin_token = jwt.encode({"username": "admin"}, "synthetic-test-secret", algorithm="HS256")
                        info_response = await dashboard_client.get(info_path,
                            headers={"Authorization": "Bearer " + admin_token})
                        self.assertEqual(info_response.status_code, 200)
                        self.assertEqual(info_response.json()["access_key"], plugin._adapter.service.access_key)
                        self.assertIn("no-store", info_response.headers["cache-control"])

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
                    (config_dir / "astrbot_plugin_tianshu_config.json").write_text(
                        json.dumps({"enabled": False, "adapter_port": port,
                                    "adapter_listen_mode": "container"}), encoding="utf-8")
                    reloaded, error = await manager.reload("astrbot_plugin_tianshu")
                    self.assertTrue(reloaded, error)
                    metadata = next(item for item in context.get_all_stars()
                                    if item.root_dir_name == "astrbot_plugin_tianshu")
                    plugin = metadata.star_cls
                    self.assertEqual(plugin._adapter.listen_host, "0.0.0.0")
                    await plugin.on_astrbot_loaded()
                    for _ in range(100):
                        if plugin._adapter.runner is not None:
                            break
                        await asyncio.sleep(0.02)
                    self.assertEqual(plugin._adapter.service.access_key, key)
                    sites = tuple(plugin._adapter.runner.sites)
                    self.assertEqual(sites[0]._server.sockets[0].getsockname()[0], "0.0.0.0")
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=dashboard),
                                                 base_url="http://dashboard.test") as dashboard_client:
                        reloaded_info = await dashboard_client.get(info_path,
                            headers={"Authorization": "Bearer " + admin_token})
                        self.assertEqual(reloaded_info.json()["access_key"], key)
                        self.assertEqual(reloaded_info.json()["listen_mode"], "container")
                    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=3) as client:
                        replay = await client.post("/tianshu/adapter/v1/messages/send",
                            json={"connection_id": "conn", "delivery": delivery},
                            headers={"Authorization": "Bearer " + key})
                        self.assertEqual(replay.json(), sent)
                    self.assertEqual(len(calls), 2)
                    import ipaddress
                    import psutil
                    from data.plugins.astrbot_plugin_tianshu.adapter import PRIVATE_NETWORKS
                    lan_hosts = sorted({addr.address for entries in psutil.net_if_addrs().values()
                        for addr in entries if addr.family == socket.AF_INET and
                        any(ipaddress.IPv4Address(addr.address) in network
                            for network in PRIVATE_NETWORKS)},
                        key=lambda address: (not address.startswith("192.168."), address))
                    if lan_hosts:
                        lan_host = lan_hosts[0]
                        (config_dir / "astrbot_plugin_tianshu_config.json").write_text(
                            json.dumps({"enabled": False, "adapter_port": port,
                                        "adapter_listen_mode": "lan",
                                        "adapter_lan_host": lan_host}), encoding="utf-8")
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
                        self.assertEqual(plugin._adapter.listen_host, lan_host)
                        async with httpx.AsyncClient(base_url=f"http://{lan_host}:{port}",
                                                     trust_env=False, timeout=3) as client:
                            lan_response = await client.post("/tianshu/adapter/v1/capabilities",
                                json={}, headers={"Authorization": "Bearer " + key})
                            self.assertEqual(lan_response.status_code, 200)
                    await manager._terminate_plugin(metadata)
                    await manager._unbind_plugin(metadata.name, metadata.module_path)
                    metadata = None
                    self.assertFalse(any(item[0] == "/astrbot_plugin_tianshu/connection-info"
                                         for item in context.registered_web_apis))
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
