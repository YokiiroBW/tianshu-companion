"""Connection-scoped Platform HTTP port; never accepts an admin or Core credential."""

import ssl
from pathlib import Path
from urllib.parse import urlsplit

import httpx


class PlatformError(RuntimeError):
    def __init__(self, status=None):
        self.status = status
        super().__init__("Platform bot request failed")


class PlatformPort:
    def __init__(self, base_url, token, *, transport=None, ca_file=None):
        url = urlsplit(base_url)
        if (
            (
                url.scheme != "https"
                and not (url.scheme == "http" and url.hostname in {"127.0.0.1", "localhost", "::1"})
            )
            or url.username
            or url.password
            or url.query
            or url.fragment
            or url.path not in {"", "/"}
        ):
            raise ValueError("Platform URL must use HTTPS (or loopback HTTP for local tests)")
        if not token or any(c.isspace() for c in token):
            raise ValueError("A connection-scoped bearer is required")
        if ca_file is not None and (not Path(ca_file).is_absolute() or not Path(ca_file).is_file()):
            raise ValueError("CA file must be an existing absolute path")
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(
            timeout=10,
            follow_redirects=False,
            trust_env=False,
            transport=transport,
            headers={"Authorization": "Bearer " + token, "Accept-Encoding": "identity"},
            verify=ssl.create_default_context(cafile=ca_file) if ca_file else True,
        )

    async def close(self):
        await self.client.aclose()

    async def _post(self, path, body):
        try:
            response = await self.client.post(self.base_url + path, json=body)
        except (httpx.HTTPError, TimeoutError) as error:
            raise PlatformError() from error
        if response.status_code != 200 or len(response.content) > 1_000_000:
            raise PlatformError(response.status_code)
        try:
            value = response.json()
        except ValueError as error:
            raise PlatformError(response.status_code) from error
        if not isinstance(value, dict):
            raise PlatformError(response.status_code)
        return value

    async def event(self, body):
        value = await self._post("/internal/v1/bot/events", body)
        if (
            value.get("event_id") != body["event_id"]
            or value.get("state") not in {"accepted", "not_started", "unknown"}
            or not isinstance(value.get("message_id"), str)
            or not isinstance(value.get("outcomes"), list)
        ):
            raise PlatformError()
        return value

    async def event_status(self, body):
        value = await self._post(
            "/internal/v1/bot/events/status",
            {k: body[k] for k in ("connection_id", "event_id", "account_id")},
        )
        if type(value.get("found")) is not bool or value.get("state") not in {
            "accepted",
            "not_started",
            "unknown",
            None,
        }:
            raise PlatformError()
        return value

    async def heartbeat(self, connection_id, instance_id):
        value = await self._post(
            "/internal/v1/bot/heartbeat",
            {"connection_id": connection_id, "instance_id": instance_id},
        )
        if value.get("state") != "online":
            raise PlatformError()
        return value

    async def claim(self, connection_id, instance_id, limit=8):
        value = await self._post(
            "/internal/v1/bot/replies/claim",
            {"connection_id": connection_id, "instance_id": instance_id, "limit": limit},
        )
        deliveries = value.get("deliveries")
        if not isinstance(deliveries, list) or len(deliveries) > limit:
            raise PlatformError()
        required = {
            "reply_id",
            "attempt_id",
            "namespace",
            "conversation_id",
            "thread_id",
            "text",
            "turn_id",
            "segment_sequence",
        }
        if any(
            not isinstance(d, dict)
            or not required <= d.keys()
            or d.get("namespace") != "qq"
            or not isinstance(d.get("text"), str)
            or not d["text"]
            or not isinstance(d.get("reply_id"), str)
            or not isinstance(d.get("attempt_id"), str)
            for d in deliveries
        ) or len({d["reply_id"] for d in deliveries}) != len(deliveries):
            raise PlatformError()
        return deliveries

    async def ack(self, body):
        value = await self._post("/internal/v1/bot/replies/ack", body)
        if value.get("reply_id") != body["reply_id"] or value.get("state") != body["state"]:
            raise PlatformError()
        return value
