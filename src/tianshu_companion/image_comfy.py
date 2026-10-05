"""ComfyUI provider transport; holds resolved credentials only in memory."""

import ipaddress
import json
import os
from urllib.parse import urlsplit
import httpx
from .contracts import digest
from .image_provider import ImageRejected, ImageResult


class ComfyUI:
    provider = "comfyui"

    def __init__(self, base_url, *, token_env=None, token=None, credential_ref=None, timeout=10):
        url = urlsplit(base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or url.path not in {"", "/"}
        ):
            raise ValueError("Fixed ComfyUI origin required")
        if url.scheme == "http" and url.hostname != "localhost":
            try:
                private = ipaddress.ip_address(url.hostname).is_private
            except ValueError:
                private = False
            if not private:
                raise ValueError("Public ComfyUI requires TLS")
        if not 0 < timeout <= 30:
            raise ValueError("Invalid network budget")
        token = os.environ.get(token_env) if token_env else token
        if token_env and not token:
            raise ValueError("Missing authentication reference")
        self.identity = digest([base_url.rstrip("/"), credential_ref or token_env])
        self.client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            headers={"Authorization": "Bearer " + token} if token else {},
        )

    async def json(self, method, path, body=None, *, maximum=2_000_000):
        async with self.client.stream(method, path, json=body) as response:
            response.raise_for_status()
            data = bytearray()
            async for chunk in response.aiter_bytes():
                data.extend(chunk)
                if len(data) > maximum:
                    raise ValueError("Response budget exceeded")
            return json.loads(data) if data else {}

    async def submit(self, submission_id, plan):
        try:
            result = await self.json(
                "POST",
                "/prompt",
                dict(prompt_id=submission_id, prompt=plan["graph"], client_id="tianshu-core"),
            )
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 400:
                raise ImageRejected("prompt_rejected") from None
            raise
        if result.get("prompt_id") != submission_id:
            raise ValueError("Server must preserve submitted prompt_id")
        return ImageResult("queued", handle=submission_id)

    async def poll(self, submission_id, plan):
        history = await self.json("GET", "/history/" + submission_id)
        record = history.get(submission_id)
        if record:
            if record.get("status", {}).get("status_str") == "error":
                return ImageResult("failed", error_code="prompt_rejected")
            if record.get("status", {}).get("completed") is not True:
                return ImageResult("unknown")
            descriptors = [
                item
                for node in plan["outputs"]
                for item in record.get("outputs", {}).get(node, {}).get("images", [])
            ]
            return ImageResult("completed", descriptors)
        queue = await self.json("GET", "/queue")
        running = any(item[1] == submission_id for item in queue["queue_running"])
        pending = any(item[1] == submission_id for item in queue["queue_pending"])
        return ImageResult("running" if running else "queued" if pending else "unknown")

    async def cancel_pending(self, submission_id, plan):
        observed = await self.poll(submission_id, plan)
        if observed.state != "queued":
            return observed
        await self.json("POST", "/queue", {"delete": [submission_id]})
        result = await self.poll(submission_id, plan)
        return ImageResult("cancelled") if result.state == "unknown" else result

    async def image(self, descriptor, maximum):
        if descriptor.get("type") != "output":
            raise ValueError("Only output artifacts allowed")
        for key in ("filename", "subfolder"):
            value = descriptor.get(key)
            if not isinstance(value, str) or len(value) > 240 or "\\" in value or ":" in value:
                raise ValueError("Unsafe artifact descriptor")
            if value.startswith("/") or any(p in {".", ".."} for p in value.split("/")):
                raise ValueError("Unsafe artifact path")
        if not descriptor["filename"] or "/" in descriptor["filename"]:
            raise ValueError("Invalid filename")
        # ComfyUI resolves these suffixes before query type, even without a space.
        if descriptor["filename"].endswith(("[input]", "[temp]", "[output]")):
            raise ValueError("Artifact directory annotations are not allowed")
        params = {key: descriptor[key] for key in ("filename", "subfolder", "type")}
        async with self.client.stream("GET", "/view", params=params) as response:
            response.raise_for_status()
            data = bytearray()
            async for chunk in response.aiter_bytes():
                data.extend(chunk)
                if len(data) > maximum:
                    raise ValueError("Artifact byte budget exceeded")
        from .image_formats import png

        return png(data, maximum)

    async def close(self):
        await self.client.aclose()

    async def upload(self, data, name):
        response = await self.client.post(
            "/upload/image",
            files={"image": (name, data, "image/png")},
            data={"type": "input", "overwrite": "true"},
        )
        response.raise_for_status()
        if len(response.content) > 16384:
            raise ValueError("Upload response budget")
        result = json.loads(response.content)
        if result.get("name") != name or result.get("type") != "input":
            raise ValueError("Unexpected uploaded image identity")
        return name
