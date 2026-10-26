"""Real fixed NoneBot SDK dispatch, isolated in a child process; no QQ connection."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import sqlite3
import zipfile

import pytest


@pytest.mark.skipif(
    importlib.util.find_spec("nonebot") is None, reason="fixed NoneBot SDK unavailable"
)
def test_owned_entry_receipts_unknown_graph_and_swallowed_rule_failure(tmp_path):
    script = textwrap.dedent(r"""
        import asyncio
        import json
        from pathlib import Path
        import sys
        import time

        import nonebot
        from nonebot.adapters.onebot.v11 import Adapter, Bot, GroupMessageEvent
        from nonebot.message import handle_event
        from tianshu_nonebot.onebot import create_host

        root = Path(sys.argv[1])
        create_host(host="127.0.0.1", port=18765, data_dir=root, access_token="synthetic-only")
        import tianshu_nonebot.adapter_plugin as host
        assert host.participation_host.available(), "owned fixed registration graph must be supported"

        async def main():
            adapter = nonebot.get_adapter(Adapter)
            bot = Bot(adapter, "10001")
            async def login():
                return {"user_id": 10001, "nickname": "synthetic"}
            bot.get_login_info = login
            adapter.bot_connect(bot)
            service = host.service
            async def call(op, request):
                status, result = await service.handle("/tianshu/adapter/v1/" + op,
                    "Bearer " + service.access_key, json.dumps(request).encode())
                assert status == 200, (status, result)
                return result
            await call("observation/apply", dict(request_id="apply:one", account_id="10001", revision=1, enabled=True,
                group_policy=dict(observe=True, mode="whitelist", list=["20001"]),
                private_policy=dict(observe=True, mode="observe_only", list=[])))
            def event(number, mention=False):
                segments = [{"type":"text", "data":{"text":"Synthetic ordinary message"}}]
                if mention:
                    segments.insert(0, {"type":"at", "data":{"qq":"10001"}})
                return GroupMessageEvent(time=int(time.time()), self_id=10001, post_type="message", sub_type="normal",
                    message_type="group", group_id=20001, user_id=10002, message_id=number, message=segments,
                    raw_message="Synthetic ordinary message", font=0, sender={"user_id":10002})
            def state(number):
                row = service.db.execute("SELECT p.document,p.receipts FROM participation_state p JOIN observations o ON o.id=p.event_id WHERE o.native_id=?", (str(number),)).fetchone()
                assert row is not None
                return json.loads(row[0]), json.loads(row[1])

            await handle_event(bot, event(1))
            saved, receipts = state(1)
            assert saved["state"] == "free", (saved, receipts)
            assert set(receipts) == {"capture", "explicit_reply", "scope", "closure"}
            assert saved["completion_evidence"]["closure_receipt_id"] == receipts["closure"]["id"]

            # A real foreign matcher still executes; ordinary observation does not block it.
            observed = []
            foreign = nonebot.on_message(priority=6, block=False)
            @foreign.handle()
            async def foreign_handler():
                observed.append("entered")
            await handle_event(bot, event(2))
            assert observed == ["entered"]
            assert state(2)[0]["state"] == "handled_elsewhere"
            foreign.destroy()
            assert host.participation_host.available()

            async def unrelated_rule():
                return False
            unknown = nonebot.on_message(rule=unrelated_rule, priority=6, block=False)
            await handle_event(bot, event(20))
            assert state(20)[0]["state"] == "ownership_unknown"
            unknown.destroy()

            # NoneBot catches this exception as rule=False; our owned rule records failure.
            original = service.capture_result
            async def broken(*args, **kwargs):
                raise RuntimeError("synthetic owned rule failure")
            service.capture_result = broken
            await handle_event(bot, event(3))
            assert state(3)[0]["state"] == "ownership_unknown"
            async def cancelled(*args, **kwargs):
                raise asyncio.CancelledError()
            service.capture_result = cancelled
            try:
                await handle_event(bot, event(4))
            except asyncio.CancelledError:
                pass
            assert state(4)[0]["state"] != "free"
            service.capture_result = original

            # Our old exact connection wins without changing its input or queue.
            await call("bindings/apply", dict(request_id="exact:one", connection_id="connection:legacy", revision=1,
                account_id="10001", conversation={"kind":"group", "id":"20001"}, allowed_authors=["10002"], enabled=True))
            await handle_event(bot, event(5))
            assert state(5)[0]["state"] == "handled_elsewhere"
            assert len((await call("events/poll", {"connection_id":"connection:legacy", "limit":20}))["events"]) == 1
            await handle_event(bot, event(6, True))
            original_row = service.db.execute("SELECT claimed FROM observations WHERE native_id='6'").fetchone()
            assert original_row[0] == 1
            host.unregister()
            print("actual-nonebot-owned-and-negative-paths-passed")
        asyncio.run(main())
    """)
    root = Path(__file__).resolve().parents[1]
    env = dict(
        os.environ,
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=os.pathsep.join(
            (
                str(root / "integrations" / "nonebot"),
                str(root / "src"),
                os.environ.get("PYTHONPATH", ""),
            )
        ),
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", script, str(tmp_path)],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "actual-nonebot-owned-and-negative-paths-passed" in result.stdout


@pytest.mark.skipif(
    importlib.util.find_spec("nonebot") is None, reason="fixed NoneBot SDK unavailable"
)
def test_built_wheels_start_without_workspace_and_keep_original_identity(tmp_path):
    root = Path(__file__).resolve().parents[1]
    artifacts = root / ".runtime" / "adapter-artifacts"
    wheels = [
        artifacts / "tianshu_nonebot_adapter-0.6.0-py3-none-any.whl",
        artifacts / "tianshu_storage-0.1.3-py3-none-any.whl",
    ]
    assert all(path.is_file() for path in wheels), (
        "run integrations/build_adapter_releases.py first"
    )
    installed = tmp_path / "released-packages"
    for path in wheels:
        with zipfile.ZipFile(path) as archive:
            archive.extractall(installed)
    database = tmp_path / "adapter.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        connection.executemany(
            "INSERT INTO meta VALUES(?,?)",
            [
                ("instance_id", "synthetic-original-instance"),
                ("access_key", "synthetic-original-key"),
            ],
        )
    script = textwrap.dedent("""
        from pathlib import Path
        import sys
        import tianshu_nonebot
        import tianshu_storage
        assert Path(tianshu_nonebot.__file__).is_relative_to(Path(sys.argv[2]))
        assert Path(tianshu_storage.__file__).is_relative_to(Path(sys.argv[2]))
        from tianshu_nonebot.onebot import create_host
        create_host(host="127.0.0.1",port=18765,data_dir=Path(sys.argv[1]),access_token="synthetic-only")
        import tianshu_nonebot.adapter_plugin as host
        assert host.participation_host.available()
        assert host.service.instance_id == "synthetic-original-instance"
        assert host.service.access_key == "synthetic-original-key"
        assert host.service.db.execute("SELECT COUNT(*) FROM participation_state").fetchone()[0] == 0
        host.unregister()
        print("released-wheels-and-original-identity-passed")
    """)
    result = subprocess.run(
        [sys.executable, "-B", "-c", script, str(tmp_path), str(installed)],
        cwd=tmp_path,
        env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(installed)),
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "released-wheels-and-original-identity-passed" in result.stdout
