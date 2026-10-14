"""Real standalone process, reverse WebSocket and SDK; synthetic local OneBot peer."""

import asyncio
from contextlib import closing
import json
import os
import socket
import site
import sqlite3
import subprocess
import sys
import time

import httpx
import pytest


def test_standalone_reverse_websocket_metadata_send_dedup_and_reconnect(tmp_path):
    pytest.importorskip("nonebot")
    websockets = pytest.importorskip("websockets.asyncio.client")

    async def run(directory):
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        token = "synthetic-onebot-token"
        env = dict(
            os.environ,
            TIANSHU_ONEBOT_TOKEN=token,
            PYTHONIOENCODING="utf-8",
            PYTHONPATH=os.pathsep.join([os.environ.get("PYTHONPATH", ""), *site.getsitepackages()]),
        )
        # Windows venv launchers can outlive wait() with a child still closing SQLite.
        executable = sys._base_executable if os.name == "nt" else sys.executable
        with (directory / "host.log").open("wb") as log:
            process = subprocess.Popen(
                [
                    executable,
                    "-B",
                    "-m",
                    "tianshu_nonebot.onebot",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--data-dir",
                    str(directory / "data"),
                ],
                cwd=directory,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                async with httpx.AsyncClient(
                    base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=5
                ) as client:
                    for _ in range(150):
                        if process.poll() is not None:
                            pytest.fail((directory / "host.log").read_text(encoding="utf-8"))
                        try:
                            if (await client.get("/health/live")).status_code == 200:
                                break
                        except httpx.HTTPError:
                            pass
                        await asyncio.sleep(0.05)
                    else:
                        pytest.fail("standalone host did not become live")
                    assert (await client.get("/health/ready")).status_code == 503
                    with closing(
                        sqlite3.connect(
                            (directory / "data/adapter.sqlite3").as_uri() + "?mode=ro", uri=True
                        )
                    ) as db:
                        key = db.execute(
                            "SELECT value FROM meta WHERE key='access_key'"
                        ).fetchone()[0]
                    assert key != token

                    async def rpc(path, body):
                        response = await client.post(
                            "/tianshu/adapter/v1" + path,
                            json=body,
                            headers={"Authorization": "Bearer " + key},
                        )
                        assert response.status_code == 200, response.text
                        return response.json()

                    sent = []

                    async def peer(ws):
                        async for raw in ws:
                            request = json.loads(raw)
                            action = request["action"]
                            if action == "get_login_info":
                                data = {"user_id": 42, "nickname": "synthetic"}
                            elif action == "get_msg":
                                data = {
                                    "time": int(time.time()),
                                    "message_type": "group",
                                    "message_id": 7,
                                    "real_id": 7,
                                    "sender": {"user_id": 123},
                                    "message": "earlier",
                                }
                            elif action in ("send_group_msg", "send_private_msg"):
                                sent.append(request["params"])
                                data = {"message_id": 88}
                            else:
                                data = {"online": True, "good": True}
                            await ws.send(
                                json.dumps(
                                    {
                                        "status": "ok",
                                        "retcode": 0,
                                        "data": data,
                                        "echo": request.get("echo"),
                                    }
                                )
                            )

                    headers = {
                        "X-Self-ID": "42",
                        "X-Client-Role": "Universal",
                        "Authorization": "Bearer " + token,
                    }
                    async with websockets.connect(
                        f"ws://127.0.0.1:{port}/onebot/v11/ws", additional_headers=headers
                    ) as ws:
                        pumping = asyncio.create_task(peer(ws))
                        try:
                            caps = await rpc("/capabilities", {})
                            assert caps["adapter"] == "onebot" and caps["accounts"][0]["id"] == "42"
                            await rpc(
                                "/bindings/apply",
                                dict(
                                    request_id="bind",
                                    connection_id="conn",
                                    revision=1,
                                    account_id="42",
                                    conversation=dict(kind="group", id="9"),
                                    allowed_authors=["123"],
                                    enabled=True,
                                ),
                            )
                            event = dict(
                                time=int(time.time()),
                                self_id=42,
                                post_type="message",
                                message_type="group",
                                sub_type="normal",
                                message_id=12,
                                user_id=123,
                                group_id=9,
                                anonymous=None,
                                message=[
                                    dict(type="reply", data={"id": "7"}),
                                    dict(type="at", data={"qq": "456"}),
                                    dict(type="text", data={"text": "刚才那本呢？"}),
                                    dict(type="at", data={"qq": "42"}),
                                ],
                                raw_message="synthetic",
                                font=0,
                                sender={"user_id": 123, "nickname": "合成用户"},
                            )
                            await ws.send(json.dumps(event, ensure_ascii=False))
                            for _ in range(100):
                                result = await rpc(
                                    "/events/poll", dict(connection_id="conn", limit=20)
                                )
                                if result["events"]:
                                    break
                                await asyncio.sleep(0.03)
                            else:
                                pytest.fail("SDK event was not forwarded")
                            forwarded = result["events"][0]["event"]
                            assert forwarded["text"] == "刚才那本呢？"
                            assert forwarded["message_context"] == {
                                "reply_ids": ["7"],
                                "mentioned_accounts": ["456", "42"],
                            }
                            delivery = dict(
                                reply_id="reply",
                                attempt_id="attempt",
                                namespace="qq",
                                conversation_id="group:9",
                                thread_id=None,
                                text="收到",
                                turn_id="turn",
                                segment_sequence=1,
                            )
                            first = await rpc(
                                "/messages/send", dict(connection_id="conn", delivery=delivery)
                            )
                            second = await rpc(
                                "/messages/send", dict(connection_id="conn", delivery=delivery)
                            )
                            assert (
                                first == second
                                and first["state"] == "sent"
                                and first["channel_message_ids"] == ["88"]
                            )
                            assert len(sent) == 1
                        finally:
                            pumping.cancel()
                            await asyncio.gather(pumping, return_exceptions=True)
                    for _ in range(100):
                        if (await client.get("/health/ready")).status_code == 503:
                            break
                        await asyncio.sleep(0.03)
                    assert (await client.get("/health/ready")).status_code == 503
                    async with websockets.connect(
                        f"ws://127.0.0.1:{port}/onebot/v11/ws", additional_headers=headers
                    ) as ws:
                        pumping = asyncio.create_task(peer(ws))
                        try:
                            assert (await rpc("/capabilities", {}))["accounts"][0]["id"] == "42"
                            await ws.send(json.dumps(event))
                            await asyncio.sleep(0.05)
                            assert (
                                len(
                                    (
                                        await rpc(
                                            "/events/poll", dict(connection_id="conn", limit=20)
                                        )
                                    )["events"]
                                )
                                == 1
                            )
                        finally:
                            pumping.cancel()
                            await asyncio.gather(pumping, return_exceptions=True)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                with socket.socket() as probe:
                    assert probe.connect_ex(("127.0.0.1", port)) != 0

    asyncio.run(run(tmp_path))
