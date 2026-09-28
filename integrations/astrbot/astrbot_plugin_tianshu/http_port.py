"""Small HTTPS JSON port for the Platform bot connection API."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .runtime import BoundaryError, Settings


class PlatformHTTP:
    def __init__(self, settings: Settings):
        self.settings = settings

    async def __call__(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await asyncio.to_thread(self._post, path, payload)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if path not in {
            "/internal/v1/bot/events",
            "/internal/v1/bot/events/status",
            "/internal/v1/bot/heartbeat",
            "/internal/v1/bot/replies/claim",
            "/internal/v1/bot/replies/status",
            "/internal/v1/bot/replies/ack",
        }:
            raise BoundaryError("invalid_route")
        request = Request(
            self.settings.base_url + path,
            data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            headers={
                "Authorization": "Bearer " + self.settings.token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )

        # urlopen verifies TLS with the system trust store. Exceptions and response
        # bodies are never logged: they may contain credentials or user text.
        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self, *_args, **_kwargs):
                return None

        with build_opener(NoRedirect()).open(request, timeout=10) as response:
            raw = response.read(262145)
        if len(raw) > 262144:
            raise BoundaryError("response_too_large")
        if not raw:
            return {}
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise BoundaryError("invalid_response")
        return value
