"""AstrBot 4.27.3 adapter transport on loopback only.

AstrBot's supported plugin Web API is Dashboard-session protected, so it cannot
serve an independent platform Bearer client. A controlled TLS reverse proxy may
publish this loopback endpoint; this module never binds a public interface.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import web

from .rpc import AdapterService, PREFIX


class AstrAdapter:
    def __init__(self, context, data_dir: Path, port: int):
        if isinstance(port, bool) or not isinstance(port, int) or not 1024 <= port <= 65535:
            raise ValueError("invalid adapter port")
        self.context = context
        self.port = port
        self.service = AdapterService(data_dir / "adapter.sqlite3", "astrbot",
                                      self.accounts, self.send)
        self.runner = None
        self.semaphore = asyncio.Semaphore(8)

    def _client(self, self_id: str):
        matches = []
        for platform in self.context.platform_manager.platform_insts:
            if platform.meta().name != "aiocqhttp":
                continue
            client = platform.get_client()
            active = getattr(client, "_wsr_api_clients", {})
            if isinstance(active, dict) and self_id in active:
                matches.append(client)
        return matches[0] if len(matches) == 1 else None

    async def accounts(self):
        result = []
        candidates = set()
        for platform in self.context.platform_manager.platform_insts:
            if platform.meta().name != "aiocqhttp":
                continue
            client = platform.get_client()
            active = getattr(client, "_wsr_api_clients", {})
            if not isinstance(active, dict):
                continue
            for self_id in tuple(active):
                if str(self_id).isdecimal():
                    candidates.add(str(self_id))
        for self_id in sorted(candidates):
            client = self._client(self_id)
            if client is None:
                continue
            try:
                info = await asyncio.wait_for(
                    client.call_action("get_login_info", self_id=self_id), 3
                )
            except Exception:
                continue
            if not isinstance(info, dict) or str(info.get("user_id")) != self_id:
                continue
            result.append({"id": self_id, "platform": "qq",
                           "label": str(info.get("nickname") or self_id)[:128]})
        return result

    async def send(self, self_id: str, target: str, text: str):
        client = self._client(self_id)
        if client is None:
            raise RuntimeError("SDK offline")
        kind, value = target.split(":", 1)
        message = [{"type": "text", "data": {"text": text}}]
        if kind == "group":
            response = await client.send_group_msg(
                group_id=int(value), message=message, self_id=self_id)
        else:
            response = await client.send_private_msg(
                user_id=int(value), message=message, self_id=self_id)
        if not isinstance(response, dict):
            raise ValueError("SDK receipt missing")
        return response.get("message_id")

    async def capture(self, event) -> bool:
        if event.get_platform_name() != "aiocqhttp":
            return False
        platform = self.context.get_platform_inst(event.get_platform_id())
        if platform is None or platform.meta().name != "aiocqhttp":
            return False
        self_id = str(event.get_self_id())
        if self._client(self_id) is None:
            return False
        raw = getattr(event.message_obj, "raw_message", None)
        if not isinstance(raw, Mapping) or raw.get("post_type") != "message":
            return False
        author = str(event.get_sender_id())
        if not author.isdecimal() or author == self_id:
            return False
        group = str(event.get_group_id() or "")
        kind = "group" if group else "private"
        if raw.get("message_type") != kind or str(raw.get("self_id")) != self_id:
            return False
        if str(raw.get("user_id")) != author or (group and str(raw.get("group_id")) != group):
            return False
        native_id = str(raw.get("message_id"))
        if native_id != str(event.message_obj.message_id) or not native_id.isdecimal():
            return False
        segments = raw.get("message")
        if not isinstance(segments, list) or not segments:
            return False
        parts = []
        for segment in segments:
            if not isinstance(segment, Mapping) or not isinstance(segment.get("data"), Mapping):
                return False
            if segment.get("type") == "text" and isinstance(segment["data"].get("text"), str):
                parts.append(segment["data"]["text"])
            elif (segment.get("type") == "at" and group and
                  str(segment["data"].get("qq")) == self_id):
                continue
            else:
                return False
        text = "".join(parts).strip()
        if not text or text.startswith("/") or len(text) > 8000:
            return False
        try:
            when = datetime.fromtimestamp(int(raw["time"]), timezone.utc)
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            return False
        captured = await self.service.capture(
            self_id, f"{kind}:{group or author}", author, native_id,
            when.isoformat(timespec="milliseconds").replace("+00:00", "Z"), text,
        )
        if captured:
            event.stop_event()
        return captured

    async def start(self):
        if self.runner is not None:
            return
        app = web.Application(client_max_size=65536)
        app.router.add_post(PREFIX + "/{tail:.*}", self._http)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        try:
            await web.TCPSite(runner, "127.0.0.1", self.port).start()
        except Exception:
            await runner.cleanup()
            raise
        self.runner = runner

    async def _http(self, request):
        if request.content_type != "application/json":
            return web.json_response({"code": "invalid_input", "retryable": False}, status=400)
        if self.semaphore.locked():
            return web.json_response({"code": "busy", "retryable": True}, status=429)
        async with self.semaphore:
            try:
                body = await request.read()
                status, payload = await asyncio.wait_for(
                    self.service.handle(request.path, request.headers.get("Authorization"), body), 20
                )
            except web.HTTPRequestEntityTooLarge:
                status, payload = 400, {"code": "invalid_input", "retryable": False}
            except asyncio.TimeoutError:
                status, payload = 503, {"code": "dependency_unavailable", "retryable": True}
            return web.json_response(payload, status=status)

    async def close(self):
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None
        self.service.close()
