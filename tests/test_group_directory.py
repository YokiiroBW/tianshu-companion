"""Real adapter metadata owner and SDK entry boundaries; no QQ connection or model."""

import asyncio
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "integrations" / "shared"))
sys.path.insert(0, str(ROOT / "integrations" / "astrbot"))
import tianshu_adapter_rpc as rpc  # noqa: E402
from astrbot_plugin_tianshu.adapter import AstrAdapter  # noqa: E402


async def accounts():
    return [dict(id="42", platform="qq", label="synthetic")]


async def send(*args):
    raise AssertionError("Directory discovery must not send")


async def groups(service, *, cursor=None, limit=100, account="42", status=200):
    code, result = await service.handle(
        rpc.PREFIX + "/observation/groups",
        "Bearer " + service.access_key,
        rpc._json(dict(account_id=account, cursor=cursor, limit=limit)).encode(),
    )
    assert code == status, result
    return result


def test_directory_without_observation_or_binding_is_metadata_only_and_survives_restart(tmp_path):
    async def run():
        path = tmp_path / "adapter.sqlite"
        service = rpc.AdapterService(path, "astrbot", accounts, send)
        try:
            await service.note_group("42", "9")
            await service.note_group("42", "10")
            await service.note_group("42", "9")
            first = await groups(service, limit=1)
            assert first["groups"][0]["group_id"] == "10"
            assert first["next_cursor"] == "10"
            second = await groups(service, cursor=first["next_cursor"], limit=1)
            assert second["next_cursor"] is None
            item = second["groups"][0]
            assert item == dict(
                group_id="9",
                name=None,
                member_count=None,
                max_member_count=None,
                first_seen_at=item["first_seen_at"],
                last_seen_at=item["last_seen_at"],
                updated_at=None,
            )
            assert item["last_seen_at"] >= item["first_seen_at"]
            for table in ("observations", "events", "observation_accounts", "bindings"):
                assert service.db.execute("SELECT count(*) FROM " + table).fetchone()[0] == 0
            original_instance = service.instance_id
            service.close()
            service = rpc.AdapterService(path, "astrbot", accounts, send)
            saved = await groups(service)
            assert saved["instance_id"] == original_instance
            assert len(saved["groups"]) == 2
            assert saved["groups"][1] == item
            await groups(service, account="999", status=503)
            for changes in (dict(limit=0), dict(limit=True), dict(cursor="group:9")):
                await groups(service, **changes, status=400)
            assert (await service.handle(rpc.PREFIX + "/observation/groups", "bad", b"{}"))[
                0
            ] == 401
            caps = await service._observation_capabilities({})
            assert caps["group_directory_version"] == 1
        finally:
            service.close()

    asyncio.run(run())


def test_refresh_is_async_throttled_and_failure_preserves_prior_metadata(tmp_path, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(rpc.time, "time", lambda: now[0])

    async def run():
        path = tmp_path / "adapter.sqlite"
        service = rpc.AdapterService(path, "astrbot", accounts, send)
        release, started = asyncio.Event(), asyncio.Event()
        calls = []

        async def fetch():
            calls.append("info")
            started.set()
            await release.wait()
            return dict(group_id=9, group_name="实际群\n名", member_count=3, max_member_count=200)

        try:
            await service.note_group("42", "9", fetch)
            await started.wait()
            assert (await groups(service))["groups"][0]["name"] is None
            await service.note_group("42", "9", fetch)
            assert calls == ["info"]
            release.set()
            await service.drain_group_refreshes()
            item = (await groups(service))["groups"][0]
            assert item["name"] == "实际群名"
            assert (item["member_count"], item["max_member_count"]) == (3, 200)
            service.close()
            service = rpc.AdapterService(path, "astrbot", accounts, send)
            await service.note_group("42", "9", fetch)
            assert calls == ["info"]  # Persisted throttle also survives restart.
            now[0] += rpc.GROUP_REFRESH_SECONDS + 1

            async def unavailable():
                calls.append("failed")
                raise OSError("synthetic SDK unavailable")

            await service.note_group("42", "9", unavailable)
            await service.drain_group_refreshes()
            after = (await groups(service))["groups"][0]
            assert after == {**item, "last_seen_at": now[0]}
            assert calls == ["info", "failed"]
        finally:
            release.set()
            await service.drain_group_refreshes()
            service.close()

    asyncio.run(run())


def test_refresh_work_is_bounded_and_drained_before_sql_close(tmp_path):
    async def run():
        service = rpc.AdapterService(tmp_path / "adapter.sqlite", "astrbot", accounts, send)
        release, active = asyncio.Event(), []

        def fetch(group):
            async def result():
                active.append(group)
                await release.wait()
                return dict(group_id=group, group_name="群" + group)

            return result

        try:
            for index in range(rpc.MAX_GROUP_REFRESHES + 1):
                group = str(index + 1)
                await service.note_group("42", group, fetch(group))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert len(active) == rpc.MAX_GROUP_REFRESHES
            assert len((await groups(service))["groups"]) == rpc.MAX_GROUP_REFRESHES + 1
            draining = asyncio.create_task(service.drain_group_refreshes())
            await asyncio.sleep(0)
            assert not draining.done()
            with pytest.raises(RuntimeError, match="Drain group"):
                service.close()
            release.set()
            await draining
            assert sum(
                item["name"] is not None for item in (await groups(service))["groups"]
            ) == len(active)
        finally:
            release.set()
            await service.drain_group_refreshes()
            service.close()

    asyncio.run(run())


def test_directory_pagination_fits_full_utf8_metadata_without_losing_rows(tmp_path):
    async def run():
        service = rpc.AdapterService(tmp_path / "adapter.sqlite", "astrbot", accounts, send)
        try:
            for index in range(1, 101):
                group = str(index)

                async def info(group=group):
                    return dict(group_id=group, group_name="😀" * 128, member_count=1)

                await service.note_group("42", group, info)
                await asyncio.gather(*service._group_refreshes.values())
                await asyncio.sleep(0)
            cursor, seen = None, []
            while True:
                page = await groups(service, cursor=cursor)
                assert len(rpc._json(page).encode()) <= rpc.MAX_RESPONSE
                seen.extend(item["group_id"] for item in page["groups"])
                assert all(item["name"] == "😀" * 128 for item in page["groups"])
                cursor = page["next_cursor"]
                if cursor is None:
                    break
            assert len(seen) == len(set(seen)) == 100
        finally:
            await service.drain_group_refreshes()
            service.close()

    asyncio.run(run())


@pytest.mark.parametrize("configured", [False, True], ids=["unconfigured", "disabled"])
def test_astrbot_group_discovery_precedes_disabled_body_and_author_access(tmp_path, configured):
    async def run():
        calls = []

        class Client:
            _wsr_api_clients = {"42": object()}

            async def call_action(self, operation, **kwargs):
                calls.append((operation, kwargs))
                if operation == "get_login_info":
                    return {"user_id": 42}
                return dict(group_id=9, group_name="SDK群", member_count=4)

        client = Client()
        platform = SimpleNamespace(
            meta=lambda: SimpleNamespace(name="aiocqhttp"), get_client=lambda: client
        )
        context = SimpleNamespace(
            platform_manager=SimpleNamespace(platform_insts=[platform]),
            get_platform_inst=lambda _: platform,
        )
        adapter = AstrAdapter(context, tmp_path, 8765)

        class Raw(dict):
            def get(self, key, *args):
                if key in {"message", "raw_message"}:
                    raise AssertionError("disabled body must not be read")
                return super().get(key, *args)

        class Event:
            def __init__(self, *, group="9", raw_group=9, self_id=42, kind="group"):
                self.group = group
                self.message_obj = SimpleNamespace(
                    raw_message=Raw(
                        post_type="message",
                        message_type=kind,
                        self_id=self_id,
                        group_id=raw_group,
                        user_id=7,
                    )
                )

            def get_platform_name(self):
                return "aiocqhttp"

            def get_platform_id(self):
                return "synthetic"

            def get_self_id(self):
                return "42"

            def get_group_id(self):
                return self.group

            def get_sender_id(self):
                if self.group is not None:
                    raise AssertionError("disabled author must not be read")
                return "7"

            def stop_event(self):
                raise AssertionError("discovery must not claim reply")

        try:
            if configured:
                adapter.service.db.execute(
                    "INSERT INTO observation_accounts VALUES (?,?,?,?,?,?,?)",
                    (
                        "42",
                        1,
                        1,
                        "synthetic",
                        rpc._json(
                            dict(
                                version=2,
                                default=dict(mode="disabled", actor_id=None, keywords=[]),
                                groups={},
                            )
                        ),
                        rpc._json(dict(observe=False, mode="observe_only", list=[])),
                        rpc.time.time(),
                    ),
                )
            assert not await adapter.capture(Event())
            assert (await groups(adapter.service))["groups"][0]["group_id"] == "9"
            assert not await adapter.capture(Event(group="10", raw_group=11))
            assert not await adapter.capture(Event(group="12", raw_group=12, self_id=43))
            assert not await adapter.capture(Event(group=None, raw_group=None, kind="private"))
            assert len((await groups(adapter.service))["groups"]) == 1
            for table in ("events", "observations"):
                assert (
                    adapter.service.db.execute("SELECT count(*) FROM " + table).fetchone()[0] == 0
                )
        finally:
            await adapter.close()
        assert sum(operation == "get_group_info" for operation, _ in calls) == 1

    asyncio.run(run())


@pytest.mark.skipif(
    importlib.util.find_spec("nonebot") is None, reason="NoneBot SDK is not installed"
)
def test_nonebot_actual_entry_discovers_disabled_group_without_reading_body(tmp_path):
    program = r"""
import asyncio
import nonebot
from nonebot.adapters.onebot.v11 import Adapter, Bot, GroupMessageEvent
nonebot.init(driver="~fastapi")
driver = nonebot.get_driver()
driver.register_adapter(Adapter)
assert nonebot.load_plugin("tianshu_nonebot.adapter_plugin") is not None
import tianshu_nonebot.adapter_plugin as host
adapter = Adapter(driver)
bot = Bot(adapter, "42")
async def info(**kwargs):
    return dict(group_id=9, group_name="SDK group", member_count=5)
bot.get_group_info = info
async def login():
    return dict(user_id=42)
bot.get_login_info = login
adapter.bot_connect(bot)
class Event(GroupMessageEvent):
    def __getattribute__(self, name):
        if name in {"original_message", "user_id", "sender"}:
            raise AssertionError("Disabled message body/author touched")
        return super().__getattribute__(name)
def event(self_id=42, group=9):
    return Event.model_validate(dict(time=1700000000, self_id=self_id, post_type="message",
        sub_type="normal", user_id=7, message_type="group", message_id=1,
        message=[dict(type="text", data=dict(text="must remain unread"))],
        original_message=[], raw_message="must remain unread", font=0,
        sender=dict(user_id=7), group_id=group, to_me=False))
async def run():
    try:
        assert not await host._observe(bot, event(), {})
        import json
        policy = dict(version=2, default=dict(mode="active", actor_id="actor:old", keywords=[]),
                      groups={"9": dict(mode="disabled", actor_id=None, keywords=[])})
        code, _ = await host.service.handle(host.PREFIX + "/observation/apply",
            "Bearer " + host.service.access_key, json.dumps(dict(request_id="disabled",
            account_id="42", revision=1, enabled=True, group_policy=policy,
            group_revisions={"9": 1}, group_default_revision=1,
            private_policy=dict(observe=False, mode="observe_only", list=[]))).encode())
        assert code == 200
        assert not await host._observe(bot, event(), {})
        assert host.service.db.execute("SELECT count(*) FROM meta WHERE key LIKE 'group-directory:%'").fetchone()[0] == 1
        assert not await host._observe(bot, event(self_id=43, group=10), {})
        assert not await host._observe(bot, event(group=0), {})
        assert host.service.db.execute("SELECT count(*) FROM meta WHERE key LIKE 'group-directory:%'").fetchone()[0] == 1
        assert host.service.db.execute("SELECT count(*) FROM observations").fetchone()[0] == 0
        assert host.service.db.execute("SELECT count(*) FROM events").fetchone()[0] == 0
        import httpx
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=host.app), base_url="http://test") as client:
            response = await client.post(host.PREFIX + "/observation/groups",
                json=dict(account_id="42", cursor=None, limit=10),
                headers={"Authorization": "Bearer " + host.service.access_key})
            assert response.status_code == 200, response.text
            assert [item["group_id"] for item in response.json()["groups"]] == ["9"]
        assert await host.lifecycle.drain()
    finally:
        await host.shutdown()
asyncio.run(run())
"""
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(program)],
        cwd=tmp_path,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
