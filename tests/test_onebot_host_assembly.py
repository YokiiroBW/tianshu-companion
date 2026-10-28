"""Owned OneBot ASGI lifecycle and storage isolation; no real QQ or model calls."""

import asyncio
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import textwrap
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tianshu_nonebot.adapter_lifecycle import AdapterLifecycle
from tianshu_nonebot.rpc import AdapterService
from tianshu_nonebot.storage import read_access_key


def test_completion_lease_survives_waiter_timeout_and_cancellation():
    async def run():
        owner = AdapterLifecycle(
            SimpleNamespace(instance_id="synthetic-instance", drain_group_refreshes=AsyncMock())
        )
        assert owner.instance_id == "synthetic-instance"
        with pytest.raises(AttributeError):
            owner.instance_id = "replacement"
        operation = owner.operation(event=True)
        assert await operation.__aenter__()
        assert not await owner.drain(timeout=0.001)
        assert len(owner.pending) == 1
        waiter = asyncio.create_task(owner.drain(timeout=None))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert len(owner.pending) == 1
        assert not next(iter(owner.pending)).done()
        async with owner.operation(event=True) as accepted:
            assert not accepted
        async with owner.operation() as accepted:
            assert accepted  # Legitimate receipts/sends remain admitted.
        # NoneBot exits its event dependency in a different task's exit stack.
        await asyncio.create_task(operation.__aexit__(None, None, None))
        assert await owner.drain(timeout=0.1)
        assert not owner.pending

    asyncio.run(run())


@pytest.mark.parametrize("environ", [{}, {"TIANSHU_STORAGE_NAMESPACE": "owned-onebot"}])
def test_storage_environment_does_not_inherit_platform(tmp_path, monkeypatch, environ):
    monkeypatch.setenv("TIANSHU_POSTGRES_URL", "invalid-platform-database-url")
    monkeypatch.setenv("TIANSHU_REDIS_URL", "invalid-platform-cache-url")
    monkeypatch.setenv("TIANSHU_STORAGE_NAMESPACE", "platform-only")

    async def unused(*args):
        raise AssertionError("No SDK call during construction")

    path = tmp_path / "adapter.sqlite3"
    service = AdapterService(
        path,
        "onebot",
        unused,
        unused,
        storage_backends={"namespace": "onebot-settings"},
        storage_environ=environ,
    )
    try:
        assert service.storage_runtime.backend == "sqlite"
        assert service.storage_runtime.options.namespace == environ.get(
            "TIANSHU_STORAGE_NAMESPACE", "onebot-settings"
        )
        assert read_access_key(path, environ=environ) == service.access_key
        assert os.environ["TIANSHU_STORAGE_NAMESPACE"] == "platform-only"
    finally:
        service.close()


def test_failed_adapter_construction_closes_open_database(tmp_path, monkeypatch):
    import tianshu_nonebot.rpc as rpc
    import tianshu_nonebot.storage as storage

    opened = []
    original = storage.open_database

    def remember(*args, **kwargs):
        result = original(*args, **kwargs)
        opened.append(result)
        return result

    def fail(*args):
        raise RuntimeError("synthetic construction failure")

    monkeypatch.setattr(storage, "open_database", remember)
    monkeypatch.setattr(rpc, "Participation", fail)
    with pytest.raises(RuntimeError, match="synthetic construction failure"):
        AdapterService(tmp_path / "adapter.sqlite3", "onebot", fail, fail, storage_environ={})
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0][1].execute("SELECT 1")


def _run_sdk(script, directory):
    pytest.importorskip("nonebot")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-B", "-c", textwrap.dedent(script), str(directory)],
        cwd=directory,
        env=dict(
            os.environ,
            PYTHONIOENCODING="utf-8",
            PYTHONDONTWRITEBYTECODE="1",
            PYTHONPATH=os.pathsep.join(
                [
                    str(root / "integrations/nonebot"),
                    str(root / "src"),
                    os.environ.get("PYTHONPATH", ""),
                ]
            ),
        ),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=40,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_owned_lifespan_waits_for_event_closure_and_native_send(tmp_path):
    _run_sdk(
        """
        import asyncio, json, os, sqlite3, sys, time
        from pathlib import Path
        import httpx, nonebot
        from nonebot.adapters.onebot.v11 import Adapter, Bot, GroupMessageEvent
        from nonebot.message import handle_event
        from tianshu_nonebot.onebot import create_host

        os.environ['TIANSHU_POSTGRES_URL'] = 'invalid-platform-database-url'
        os.environ['TIANSHU_STORAGE_NAMESPACE'] = 'platform-only'
        nonebot.run = lambda: (_ for _ in ()).throw(AssertionError('No internal server'))
        app = create_host(host='127.0.0.1', port=18765, data_dir=Path(sys.argv[1]),
            access_token='synthetic-only', storage_environ={})
        import tianshu_nonebot.adapter_plugin as host
        assert app.state.lifecycle is host.lifecycle
        assert host.service.storage_runtime.backend == 'sqlite'
        assert host.participation_host.available()
        routes = tuple(app.routes)
        try:
            create_host(host='127.0.0.1', port=18765, data_dir=Path(sys.argv[1]),
                access_token='synthetic-only', storage_environ={})
            raise AssertionError('A second global driver must not register again')
        except RuntimeError as error:
            assert 'initialized host' in str(error)
        assert tuple(app.routes) == routes
        hooks = []
        driver = nonebot.get_driver()
        driver.on_startup(lambda: hooks.append('startup'))
        driver.on_shutdown(lambda: hooks.append('shutdown'))

        async def main():
            adapter = nonebot.get_adapter(Adapter)
            bot = Bot(adapter, '10001')
            async def login():
                return {'user_id':10001, 'nickname':'synthetic'}
            bot.get_login_info = login
            adapter.bot_connect(bot)
            service, lifecycle = host.service, host.lifecycle
            capture_started, release_capture = asyncio.Event(), asyncio.Event()
            original = service.capture_observation_result
            async def paused_capture(*args, **kwargs):
                capture_started.set()
                await release_capture.wait()
                return await original(*args, **kwargs)
            service.capture_observation_result = paused_capture
            def event(number):
                return GroupMessageEvent(time=int(time.time()), self_id=10001,
                    post_type='message', sub_type='normal', message_type='group',
                    group_id=20001, user_id=10002, message_id=number,
                    message=[{'type':'text','data':{'text':'Synthetic event'}}],
                    raw_message='Synthetic event', font=0, sender={'user_id':10002})
            async with app.router.lifespan_context(app):
                assert hooks == ['startup']
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                    base_url='http://owned-onebot', trust_env=False) as client:
                    async def call(path, body):
                        response = await client.post('/tianshu/adapter/v1/' + path,
                            json=body, headers={'Authorization':'Bearer ' + service.access_key})
                        assert response.status_code == 200, response.text
                        return response.json()
                    await call('observation/apply', dict(request_id='observe:one',
                        account_id='10001', revision=1, enabled=True,
                        group_policy=dict(version=2, default=dict(mode='active',
                            actor_id='actor:a', keywords=[]), groups={}),
                        group_revisions={}, group_default_revision=1,
                        private_policy=dict(observe=True, mode='blacklist', list=[])))
                    event_task = asyncio.create_task(handle_event(bot, event(1)))
                    await asyncio.wait_for(capture_started.wait(), 2)
                    await lifecycle.quiesce()
                    assert not await lifecycle.drain(timeout=.01)
                    assert lifecycle.pending
                    try:
                        host.unregister()
                        raise AssertionError('An active event still needs its database')
                    except RuntimeError as error:
                        assert 'Drain' in str(error)
                    await handle_event(bot, event(2))
                    assert service.db.execute("SELECT 1 FROM observations WHERE native_id='2'").fetchone() is None
                    release_capture.set()
                    await asyncio.wait_for(event_task, 2)
                    assert await lifecycle.drain(timeout=1)
                    saved = service.db.execute('SELECT document,receipts FROM participation_state').fetchone()
                    assert saved is not None
                    assert json.loads(saved[0])['state'] == 'free'
                    assert 'closure' in json.loads(saved[1])

                    await call('bindings/apply', dict(request_id='bind:one', connection_id='conn',
                        revision=1, account_id='10001', conversation=dict(kind='private',id='10002'),
                        allowed_authors=['10002'], enabled=True))
                    send_started, release_send = asyncio.Event(), asyncio.Event()
                    sent = []
                    async def native_send(**kwargs):
                        sent.append(kwargs)
                        send_started.set()
                        await release_send.wait()
                        assert service.db.execute('SELECT 1').fetchone() == (1,)
                        return {'message_id':88}
                    bot.send_private_msg = native_send
                    delivery = dict(reply_id='reply',attempt_id='attempt',namespace='qq',
                        conversation_id='private:10002',thread_id=None,text='Synthetic send',
                        turn_id='turn',segment_sequence=1)
                    sending = asyncio.create_task(call('messages/send', dict(connection_id='conn',delivery=delivery)))
                    await asyncio.wait_for(send_started.wait(), 2)
                    assert not await lifecycle.drain(timeout=.01)
                    async def release():
                        await asyncio.sleep(.02)
                        assert not lifecycle.closed
                        release_send.set()
                    releaser = asyncio.create_task(release())
                # Exit closes only HTTPX's ASGI facade; the admitted route still owns
                # its send/receipt and the driver shutdown must wait for it.
            await releaser
            receipt = await sending
            assert receipt['state'] == 'sent' and len(sent) == 1
            assert hooks == ['startup','shutdown']
            assert lifecycle.closed and not lifecycle.pending
            with sqlite3.connect(Path(sys.argv[1]) / 'adapter.sqlite3') as db:
                assert json.loads(db.execute('SELECT receipt FROM deliveries').fetchone()[0])['state'] == 'sent'
            try:
                service.db.execute('SELECT 1')
                raise AssertionError('Final cleanup must close SQL')
            except sqlite3.ProgrammingError:
                pass
        asyncio.run(main())
        """,
        tmp_path,
    )


@pytest.mark.parametrize("mode", ["foreign", "failed-owned"])
def test_factory_does_not_reset_foreign_driver_and_closes_failed_owned_build(tmp_path, mode):
    _run_sdk(
        f"""
        from pathlib import Path
        import sqlite3, sys, nonebot
        from fastapi import FastAPI
        from tianshu_nonebot.onebot import create_host
        mode = {mode!r}
        if mode == 'foreign':
            nonebot.init(driver='~fastapi', _env_file=None)
            original = nonebot.get_driver()
        else:
            original_get = FastAPI.get
            def failing_get(self, path, *args, **kwargs):
                if path == '/health/live':
                    raise RuntimeError('synthetic route failure')
                return original_get(self, path, *args, **kwargs)
            FastAPI.get = failing_get
        try:
            create_host(host='127.0.0.1', port=18765, data_dir=Path(sys.argv[1]),
                access_token='synthetic-only', storage_environ={{}})
            raise AssertionError('Expected factory failure')
        except RuntimeError:
            pass
        if mode == 'foreign':
            assert nonebot.get_driver() is original
            assert 'tianshu_nonebot.adapter_plugin' not in sys.modules
        else:
            import tianshu_nonebot.adapter_plugin as host
            assert host.lifecycle.closed
            assert not any(getattr(route,'path','').startswith('/tianshu/adapter/v1') for route in nonebot.get_asgi().routes)
            try:
                host.service.db.execute('SELECT 1')
                raise AssertionError('Failed construction leaked SQL')
            except sqlite3.ProgrammingError:
                pass
        """,
        tmp_path,
    )
