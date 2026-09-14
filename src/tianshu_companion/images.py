"""Trusted, opt-in ComfyUI image ports. No public wire or automatic GPU submission."""

import asyncio
import copy
import hashlib
import json
import os
import re
import struct
import uuid
import zlib
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from .contracts import canonical, digest
from .life import expected_version, text

EDITABLE = {
    "positive": {("CLIPTextEncode", "text"), ("AnimaArtistPack", "base_prompt")},
    "negative": {("CLIPTextEncode", "text")},
    "reference": {("LoadImage", "image")},
    "seed": {("KSampler", "seed"), ("FLS_SamplerV4", "seed")},
    "steps": {("KSampler", "steps"), ("FLS_SamplerV4", "steps")},
    "width": {("EmptyLatentImage", "width")},
    "height": {("EmptyLatentImage", "height")},
}
TERMINAL = {"completed", "failed", "cancelled"}


class Workflow:
    """Reviewed API export plus explicit typed editable inputs, never UI conversion."""

    def __init__(self, graph, bindings, outputs):
        if not isinstance(graph, dict) or not graph or "nodes" in graph:
            raise ValueError("Reviewed API-format export required")
        if len(canonical(graph).encode()) > 1_000_000:
            raise ValueError("Workflow too large")
        for key, node in graph.items():
            if not isinstance(key, str) or not isinstance(node, dict):
                raise ValueError("Invalid API node")
            text(node.get("class_type"), 128)
            if not isinstance(node.get("inputs"), dict):
                raise ValueError("Invalid API inputs")
            for value in node["inputs"].values():
                if isinstance(value, list) and (
                    len(value) != 2
                    or value[0] not in graph
                    or type(value[1]) is not int
                    or value[1] < 0
                ):
                    raise ValueError("Invalid node link")
        if not outputs or any(graph.get(k, {}).get("class_type") != "SaveImage" for k in outputs):
            raise ValueError("Explicit SaveImage outputs required")
        seen = set()
        for name, spec in bindings.items():
            if name not in {
                "positive",
                "negative",
                "reference",
                "seed",
                "width",
                "height",
                "steps",
            }:
                raise ValueError("Unsupported binding")
            if (spec["class_type"], spec["input"]) not in EDITABLE[name]:
                raise ValueError("Input is not an approved editable field")
            node = graph.get(spec["node"], {})
            value = node.get("inputs", {}).get(spec["input"])
            pair = (spec["node"], spec["input"])
            if node.get("class_type") != spec["class_type"] or pair in seen:
                raise ValueError("Missing/type-mismatched/duplicate binding")
            seen.add(pair)
            kind = str if name in {"positive", "negative", "reference"} else int
            if type(value) is not kind:
                raise ValueError("Editable literal of correct type required")
            if kind is int and not (
                type(spec.get("min")) is int
                and type(spec.get("max")) is int
                and 0 <= spec["min"] <= value <= spec["max"] <= 2**63 - 1
            ):
                raise ValueError("Explicit bounded numeric constraint required")
        if "positive" not in bindings:
            raise ValueError("Positive binding required")
        self.graph, self.bindings, self.outputs = copy.deepcopy((graph, bindings, outputs))
        self.version = digest([graph, bindings, outputs])

    def render(self, values):
        graph = copy.deepcopy(self.graph)
        for name, value in values.items():
            spec = self.bindings.get(name)
            if spec is None:
                raise ValueError("Unbound input")
            if name in {"positive", "negative", "reference"}:
                text(value, 8000)
            elif type(value) is not int or not spec["min"] <= value <= spec["max"]:
                raise ValueError("Out of range")
            # Fixed style/artist prefix is retained even in the editable positive node.
            if name == "positive":
                value = graph[spec["node"]]["inputs"][spec["input"]] + "\n" + value
            graph[spec["node"]]["inputs"][spec["input"]] = value
        return graph


class ComfyUI:
    def __init__(self, base_url, *, token_env=None, timeout=10):
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
        if url.scheme == "http" and url.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("Remote ComfyUI requires TLS")
        if not 0 < timeout <= 30:
            raise ValueError("Invalid network budget")
        token = os.environ.get(token_env) if token_env else None
        if token_env and not token:
            raise ValueError("Missing authentication reference")
        self.identity = digest([base_url, token_env])
        self.client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            headers={"Authorization": "Bearer " + token} if token else {},
        )

    async def json(self, method, path, body=None):
        async with self.client.stream(method, path, json=body) as response:
            response.raise_for_status()
            data = bytearray()
            async for chunk in response.aiter_bytes():
                data.extend(chunk)
                if len(data) > 2_000_000:
                    raise ValueError("Response budget exceeded")
            return json.loads(data) if data else {}

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
        # First adapter deliberately accepts PNG only, with header and dimension checks.
        if len(data) < 33 or data[:16] != b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR":
            raise ValueError("Expected PNG")
        width, height = struct.unpack(">II", data[16:24])
        if not 0 < width <= 8192 or not 0 < height <= 8192 or width * height > 32_000_000:
            raise ValueError("Image pixel budget exceeded")
        offset, has_data, ended = 8, False, False
        while offset + 12 <= len(data):
            size = int.from_bytes(data[offset : offset + 4], "big")
            end = offset + 12 + size
            if end > len(data):
                raise ValueError("Truncated PNG")
            chunk = data[offset + 4 : end - 4]
            if zlib.crc32(chunk) != int.from_bytes(data[end - 4 : end], "big"):
                raise ValueError("PNG CRC mismatch")
            has_data |= chunk[:4] == b"IDAT"
            offset = end
            if chunk[:4] == b"IEND":
                ended = size == 0 and end == len(data)
                break
        if not has_data or not ended:
            raise ValueError("Incomplete PNG")
        return bytes(data), width, height

    async def close(self):
        await self.client.aclose()


class Images:
    """Host authorizes actor management before calling. One bounded background pass."""

    def __init__(
        self,
        life,
        *,
        transport=None,
        workflow=None,
        staging=None,
        max_pending=16,
        max_bytes=8_000_000,
        max_files=4,
        max_storage=128_000_000,
    ):
        self.life, self.store = life, life.store
        self.transport, self.workflow = transport, workflow
        self.staging = Path(staging).resolve() if staging is not None else None
        if any(
            type(x) is not int or x < 1 for x in (max_pending, max_bytes, max_files, max_storage)
        ):
            raise ValueError("Positive budgets required")
        self.max_pending, self.max_bytes, self.max_files = max_pending, max_bytes, max_files
        self.max_storage = max_storage
        self.lock = asyncio.Lock()

    def get(self, job_id):
        item = self.store.get("image_jobs", job_id)
        if item is None:
            raise KeyError(job_id)
        return item

    def put_outfit(
        self, outfit_id, *, description, prompt, reference=None, activities=(), expected=None
    ):
        text(outfit_id, 128)
        text(description, 1000)
        text(prompt, 4000)
        if reference is not None:
            # An explicitly pre-provisioned ComfyUI input name, never upload or arbitrary URL.
            if not isinstance(reference, str) or not re.fullmatch(
                r"[A-Za-z0-9_-]+\.(png|jpg|webp)", reference
            ):
                raise ValueError("Provisioned reference filename required")
        if len(activities) > 32:
            raise ValueError("Too many activities")
        for activity in activities:
            text(activity, 128)
        old = self.store.get("image_outfits", outfit_id)
        if old:
            expected_version(expected)
            if old["version"] != expected:
                raise ValueError("Stale outfit")
        item = dict(
            id=outfit_id,
            version=(old or {}).get("version", 0) + 1,
            description=description,
            prompt=prompt,
            reference=reference,
            activities=list(activities),
        )
        self.store.put("image_outfits", item)
        return item

    def select_outfit(self, actor_id, outfit_id, *, expected):
        expected_version(expected)
        if not self.store.get("image_outfits", outfit_id):
            raise KeyError(outfit_id)
        actor = self.life.snapshot(actor_id)["actor"]
        actor["outfit_ref"] = outfit_id
        return self.life._save("actors", actor, expected)

    def request(self, request_id, actor_id, *, parameters=None):
        text(request_id, 128)
        parameters = parameters or {}
        fingerprint = digest([actor_id, parameters])
        old = self.store.get("image_jobs", request_id)
        if old:
            if old["request_hash"] != fingerprint:
                raise ValueError("Idempotency conflict")
            return old
        if not self.transport or not self.workflow or self.staging is None:
            raise ValueError("Image service explicitly unconfigured")
        if set(parameters) - {"seed", "width", "height", "steps", "negative"}:
            raise ValueError("Caller cannot override life or reference binding")
        count = self.store.db.execute(
            "SELECT count(*) FROM image_jobs WHERE status NOT IN ('completed','failed','cancelled')"
        ).fetchone()[0]
        if count >= self.max_pending:
            raise ValueError("Pending budget exhausted")
        snapshot = self.life.snapshot(actor_id)
        outfit = self.store.get("image_outfits", snapshot["actor"]["outfit_ref"])
        if outfit is None:
            raise ValueError("Current outfit has no explicit generation binding")
        values = dict(
            parameters,
            positive=canonical(
                dict(
                    fictional=True,
                    outfit=outfit["prompt"],
                    activity=snapshot["actor"]["activity"],
                    room=snapshot["room"],
                    world=snapshot["world"],
                )
            ),
        )
        if outfit["reference"] is not None:
            values["reference"] = outfit["reference"]
        graph = self.workflow.render(values)
        item = dict(
            id=request_id,
            conversation_id=actor_id,
            request_hash=fingerprint,
            state="queued",
            submitted=False,
            prompt_id=str(uuid.uuid4()),
            snapshot=snapshot,
            outfit=outfit,
            graph=graph,
            workflow_version=self.workflow.version,
            outputs=self.workflow.outputs,
            endpoint=self.transport.identity,
            fictional=True,
            artifacts=[],
            cancel_requested=False,
            created_at=self.life.clock(),
        )
        self.store.put("image_jobs", item)
        return item

    def recover(self):
        for item in self.store.list("image_jobs", states=["running", "queued"]):
            if item["submitted"]:
                item["state"] = "unknown"
                self.store.put("image_jobs", item)

    def cancel(self, job_id):
        item = self.get(job_id)
        if item["state"] not in TERMINAL:
            item["cancel_requested"] = True
            if not item["submitted"]:
                item["state"] = "cancelled"
            self.store.put("image_jobs", item)
        return item

    async def work(self):
        if not self.transport or self.lock.locked():
            return
        async with self.lock:
            row = self.store.db.execute(
                "SELECT id FROM image_jobs WHERE status IN ('queued','running','unknown') "
                "AND json_extract(body,'$.endpoint')=? "
                "ORDER BY deadline IS NOT NULL, deadline, id LIMIT 1",
                (self.transport.identity,),
            ).fetchone()
            if row is None:
                return
            item = self.get(row[0])
            if item["endpoint"] != self.transport.identity:
                return  # Never query a different service for a persisted prompt.
            try:
                async with asyncio.timeout(30):
                    await self._work(item)
            except asyncio.CancelledError:
                item["state"] = "unknown" if item["submitted"] else "queued"
                raise
            except httpx.HTTPStatusError as exc:
                rejected = (
                    exc.request.method == "POST"
                    and exc.request.url.path == "/prompt"
                    and exc.response.status_code == 400
                )
                item.update(
                    state="failed" if rejected else "unknown",
                    failure="prompt_rejected" if rejected else "http_error",
                )
            except Exception as exc:
                item.update(
                    state="unknown" if item["submitted"] else "failed", failure=type(exc).__name__
                )
            finally:
                # Cancellation may arrive while HTTP awaits; preserve that durable intent.
                item["cancel_requested"] |= self.get(item["id"])["cancel_requested"]
                item["deadline"] = self.life.clock()
                self.store.put("image_jobs", item)

    async def _work(self, item):
        prompt_id = item["prompt_id"]
        if not item["submitted"]:
            # Persist intent before I/O: even crash/timeout cannot cause automatic resubmission.
            item.update(submitted=True, state="unknown")
            self.store.put("image_jobs", item)
            result = await self.transport.json(
                "POST",
                "/prompt",
                {
                    "prompt_id": prompt_id,
                    "prompt": item["graph"],
                    "client_id": "tianshu-core",
                },
            )
            if result.get("prompt_id") != prompt_id:
                raise ValueError("Server must support submitted prompt_id")
            item["state"] = "queued"
            return
        history = await self.transport.json("GET", "/history/" + prompt_id)
        record = history.get(prompt_id)
        if record:
            if record.get("status", {}).get("status_str") == "error":
                item["state"] = "failed"
                return
            if record.get("status", {}).get("completed") is not True:
                item["state"] = "unknown"
                return
            await self._artifacts(item, record)
            item["state"] = "completed"
            return
        queue = await self.transport.json("GET", "/queue")
        running = any(x[1] == prompt_id for x in queue["queue_running"])
        pending = any(x[1] == prompt_id for x in queue["queue_pending"])
        item["state"] = "running" if running else "queued" if pending else "unknown"
        item["cancel_requested"] |= self.get(item["id"])["cancel_requested"]
        if item["cancel_requested"] and pending:
            await self.transport.json("POST", "/queue", {"delete": [prompt_id]})
            # Deletion can race execution. Do not claim cancellation without reconciliation.
            queue = await self.transport.json("GET", "/queue")
            history = await self.transport.json("GET", "/history/" + prompt_id)
            if (
                not any(x[1] == prompt_id for x in queue["queue_running"] + queue["queue_pending"])
                and prompt_id not in history
            ):
                item["state"] = "cancelled"
        # Running GPU work is not interrupted. Intent remains visible until completion.

    async def _artifacts(self, item, record):
        descriptors = []
        for node in item["outputs"]:
            descriptors.extend(record.get("outputs", {}).get(node, {}).get("images", []))
        if not 0 < len(descriptors) <= self.max_files:
            raise ValueError("Artifact count budget")
        self.staging.mkdir(parents=True, exist_ok=True)
        used = sum(p.stat().st_size for p in self.staging.iterdir() if p.is_file())
        artifacts, created = [], []
        try:
            for descriptor in descriptors:
                data, width, height = await self.transport.image(descriptor, self.max_bytes)
                used += len(data)
                if used > self.max_storage:
                    raise ValueError("Staging storage budget")
                name = uuid.uuid4().hex + ".png"
                path = self.staging / name
                with path.open("xb") as handle:
                    created.append(path)
                    handle.write(data)
                artifacts.append(
                    dict(
                        staging_name=name,
                        sha256=hashlib.sha256(data).hexdigest(),
                        size=len(data),
                        media_type="image/png",
                        width=width,
                        height=height,
                        archived=False,
                    )
                )
        except BaseException:
            for path in created:
                path.unlink(missing_ok=True)
            raise
        item["artifacts"] = artifacts
