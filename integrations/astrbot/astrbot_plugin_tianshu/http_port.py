"""Small HTTPS JSON port for the Platform bot connection API."""

from __future__ import annotations

import asyncio
import json
import ssl
from typing import Any
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

from .runtime import BoundaryError, Settings


class PlatformHTTP:
    def __init__(self, settings: Settings):
        self.settings = settings
        # A connection-specific CA bundle avoids modifying the AstrBot host's
        # trust store. create_default_context retains certificate and hostname
        # verification for both public and private platform certificates.
        context = ssl.create_default_context(cafile=settings.ca_file)

        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self, *_args, **_kwargs):
                return None

        self.opener = build_opener(NoRedirect(), HTTPSHandler(context=context))

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

        # Exceptions and response bodies are never logged: they may contain
        # credentials or user text.
        limit = 45 * 1024 * 1024 if path == "/internal/v1/bot/replies/claim" else 262144
        with self.opener.open(request, timeout=10) as response:
            raw = response.read(limit + 1)
        if len(raw) > limit:
            raise BoundaryError("response_too_large")
        if not raw:
            return {}
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise BoundaryError("invalid_response")
        return value
