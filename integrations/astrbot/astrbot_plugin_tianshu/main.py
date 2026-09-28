"""AstrBot 4.27.3 aiocqhttp/QQ text connector for Tianshu."""

from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import time
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools

from .http_port import PlatformHTTP
from .runtime import BoundaryError, Journal, Runner, Settings


class TianshuPlugin(Star):
    def __init__(self, context: Context, config: dict[str, Any] | None = None):
        super().__init__(context)
        self.context = context
        self._last_report: dict[str, float] = {}
        self._task: asyncio.Task | None = None
        self._journal: Journal | None = None
        self._runner: Runner | None = None
        if not config or config.get("enabled") is not True:
            logger.info("tianshu connector disabled")
            return
        try:
            settings = Settings.from_config(config)
            data_dir = Path(StarTools.get_data_dir("astrbot_plugin_tianshu"))
            journal_key = hashlib.sha256(
                f"{settings.connection_id}\0{settings.platform_id}\0{settings.self_id}".encode()
            ).hexdigest()[:24]
            self._journal = Journal(data_dir / f"connector-{journal_key}.sqlite3")
            self._runner = Runner(
                settings,
                self._journal,
                PlatformHTTP(settings),
                self._send_native,
                report=self._report,
            )
        except (BoundaryError, OSError, sqlite3.Error) as error:
            # BoundaryError carries a fixed code. Never log the config values.
            logger.warning(f"tianshu connector inactive: {type(error).__name__}")
            if self._journal is not None:
                self._journal.close()
                self._journal = None
            return
        self._start_background()

    def _start_background(self) -> None:
        if self._runner is None or self._task is not None:
            return
        try:
            self._task = asyncio.get_running_loop().create_task(self._background())
        except RuntimeError:
            # Some AstrBot loaders instantiate plugins before the main loop starts.
            # The loaded hook or first matching event starts the same one task.
            pass

    def _report(self, code: str) -> None:
        now = time.monotonic()
        if now - self._last_report.get(code, -1e9) >= 60:
            logger.warning(f"tianshu connector: {code}")
            self._last_report[code] = now

    @filter.event_message_type(filter.EventMessageType.ALL, priority=100)
    async def on_message(self, event: AstrMessageEvent) -> None:
        if self._runner is None:
            return
        self._start_background()
        try:
            await self._runner.on_event(event)
        except Exception:
            # Never print an SDK event or HTTP exception: both can contain text.
            self._report("event_handler_failed")

    @filter.on_astrbot_loaded()
    async def on_astrbot_loaded(self) -> None:
        self._start_background()

    def _client(self):
        if self._runner is None:
            raise BoundaryError("connector_inactive")
        settings = self._runner.settings
        platform = self.context.get_platform_inst(settings.platform_id)
        if (
            platform is None
            or platform.meta().name != "aiocqhttp"
            or platform.meta().id != settings.platform_id
        ):
            raise BoundaryError("platform_unavailable")
        client = platform.get_client()
        if client is None:
            raise BoundaryError("platform_unavailable")
        return client

    async def _send_native(self, conversation: str, text: str) -> str:
        if self._runner is None:
            raise BoundaryError("connector_inactive")
        client = self._client()
        settings = self._runner.settings
        kind, target = conversation.split(":", 1)
        if not target.isdecimal():
            raise BoundaryError("invalid_conversation")
        message = [{"type": "text", "data": {"text": text}}]
        if kind == "group":
            result = await client.send_group_msg(
                group_id=int(target), message=message, self_id=settings.self_id
            )
        elif kind == "private":
            result = await client.send_private_msg(
                user_id=int(target), message=message, self_id=settings.self_id
            )
        else:
            raise BoundaryError("invalid_conversation")
        if not isinstance(result, dict):
            raise BoundaryError("native_receipt_missing")
        value = result.get("message_id")
        if (
            isinstance(value, bool)
            or not isinstance(value, (str, int))
            or not str(value).isdecimal()
        ):
            raise BoundaryError("native_receipt_missing")
        return str(value)

    async def _background(self) -> None:
        assert self._runner is not None
        loop = asyncio.get_running_loop()
        next_heartbeat = 0.0
        while True:
            try:
                # A connection may be disabled while an already claimed native
                # send is in flight. Settlement remains allowed by Platform, so
                # flush its durable ACK even if heartbeat/claim are now refused.
                if not await self._runner.flush_acks():
                    await asyncio.sleep(self._runner.settings.poll_seconds)
                    continue
                # Do not claim a reply while its pinned SDK adapter is disconnected.
                self._client()
                if loop.time() >= next_heartbeat:
                    try:
                        await self._runner.heartbeat()
                    except Exception:
                        self._report("heartbeat_unavailable")
                    next_heartbeat = loop.time() + self._runner.settings.heartbeat_seconds
                await self._runner.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._report("poll_unavailable")
            await asyncio.sleep(self._runner.settings.poll_seconds)

    async def terminate(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._journal is not None:
            self._journal.close()
            self._journal = None
        self._runner = None
