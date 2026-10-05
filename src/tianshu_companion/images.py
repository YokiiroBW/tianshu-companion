"""Trusted, opt-in ComfyUI image ports. No public wire or automatic GPU submission."""

import asyncio
import hashlib
import json
import os
import re
import uuid
from pathlib import Path


from .contracts import digest
from .life import expected_version, text

from .image_workflow import Workflow
from .image_comfy import ComfyUI as ComfyUI
from .image_provider import ImageRejected
from .image_prompt import intent_from_life, bound_prompt_values

TERMINAL = {"completed", "failed", "cancelled"}


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
        max_bytes=33554432,
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
        self.standard_checkpoint = None
        self.completion_port = None
        self.original_reader = None
        self.submission_guard = None

    def get(self, job_id):
        item = self.store.get("image_jobs", job_id)
        if item is None:
            raise KeyError(job_id)
        return item

    def put_outfit(
        self,
        outfit_id,
        *,
        description,
        prompt,
        reference=None,
        source_scope=None,
        activities=(),
        expected=None,
    ):
        text(outfit_id, 128)
        text(description, 1000)
        text(prompt, 4000)
        if reference is not None and not isinstance(reference, dict):
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
            source_scope=source_scope,
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

    def request(
        self,
        request_id,
        actor_id,
        *,
        parameters=None,
        outfit_id=None,
        scene=None,
        activity_id=None,
        edit_source_id=None,
        scope=None,
        reference_inputs=None,
        workflow=None,
        prompt_values=None,
        compile_evidence=None,
        intent=None,
        prepared_plan=None,
    ):
        text(request_id, 128)
        parameters = parameters or {}
        identity = [
            actor_id,
            parameters,
            outfit_id,
            scene,
            activity_id,
            edit_source_id,
            scope,
            [
                {key: value for key, value in item.items() if key != "query"}
                for item in (reference_inputs or [])
            ],
        ]
        if intent is not None:
            identity.append(intent)
        fingerprint = digest(identity)
        old = self.store.get("image_jobs", request_id)
        if old:
            if old["request_hash"] != fingerprint:
                raise ValueError("Idempotency conflict")
            return old
        if (
            not self.transport
            or not (prepared_plan or workflow or self.workflow)
            or self.staging is None
        ):
            raise ValueError("Image service explicitly unconfigured")
        if set(parameters) - {"seed", "width", "height", "steps", "negative"}:
            raise ValueError("Caller cannot override life or reference binding")
        count = self.store.db.execute(
            "SELECT count(*) FROM image_jobs WHERE status NOT IN ('completed','failed','cancelled')"
        ).fetchone()[0]
        if count >= self.max_pending:
            raise ValueError("Pending budget exhausted")
        snapshot = self.life.snapshot(actor_id)
        outfit = self.store.get("image_outfits", outfit_id or snapshot["actor"]["outfit_ref"])
        if activity_id is not None:
            process = self.life.activities.get(actor_id, activity_id)
            if process["state"] == "planned" or process.get("scope") != scope:
                raise ValueError("An actual authorized activity is required")
        if prepared_plan is not None:
            return self._admit(
                request_id,
                actor_id,
                fingerprint,
                snapshot,
                outfit,
                prepared_plan,
                activity_id,
                scene,
                scope,
                edit_source_id,
                reference_inputs,
                compile_evidence,
            )
        selected = workflow or self.workflow
        values = (
            dict(prompt_values)
            if prompt_values is not None
            else bound_prompt_values(
                selected, intent_from_life(snapshot, outfit, scene, intent=intent), parameters
            )
        )
        if outfit and isinstance(outfit["reference"], str):
            values["reference"] = outfit["reference"]
        for index, reference in enumerate(reference_inputs or []):
            values["reference" + (str(index + 1) if index else "")] = reference["filename"]
        workflow = workflow or (
            Workflow.standard(
                self.standard_checkpoint,
                edit=bool(edit_source_id or (outfit or {}).get("reference") or reference_inputs),
                references=max(1, len(reference_inputs or [])),
            )
            if self.standard_checkpoint
            else self.workflow
        )
        if edit_source_id is not None:
            _, source = self.life.album.read(actor_id, edit_source_id, scope)
            if "reference" not in workflow.bindings:
                raise ValueError("Configured workflow has no reference input")
            values["reference"] = "tianshu_" + source["sha256"] + ".png"
        if "seed" in workflow.bindings and "seed" not in values:
            spec = workflow.bindings["seed"]
            values["seed"] = spec["min"] + uuid.uuid4().int % (spec["max"] - spec["min"] + 1)
        graph = workflow.render(values)
        plan = dict(
            provider=getattr(self.transport, "provider", "comfyui"),
            submission=dict(graph=graph, outputs=workflow.outputs),
            graph=graph,
            outputs=workflow.outputs,
            workflow_version=workflow.version,
        )
        return self._admit(
            request_id,
            actor_id,
            fingerprint,
            snapshot,
            outfit,
            plan,
            activity_id,
            scene,
            scope,
            edit_source_id,
            reference_inputs,
            compile_evidence,
        )

    def _admit(
        self,
        request_id,
        actor_id,
        fingerprint,
        snapshot,
        outfit,
        plan,
        activity_id,
        scene,
        scope,
        edit_source_id,
        reference_inputs,
        compile_evidence,
    ):
        item = dict(
            id=request_id,
            conversation_id=actor_id,
            request_hash=fingerprint,
            state="queued",
            submitted=False,
            prompt_id=str(uuid.uuid4()),
            snapshot=snapshot,
            outfit=outfit,
            graph=plan.get("graph"),
            workflow_version=plan.get("workflow_version"),
            provider=plan["provider"],
            submission=plan["submission"],
            provider_handle=None,
            compile_evidence=compile_evidence,
            outputs=plan.get("outputs", []),
            endpoint=self.transport.identity,
            fictional=True,
            artifacts=[],
            cancel_requested=False,
            created_at=self.life.clock(),
            activity_id=activity_id,
            scene=scene,
            scope=scope,
            edit_source_id=edit_source_id,
            reference_uploaded=False,
            reference_inputs=reference_inputs or [],
            runtime_epoch=snapshot["actor"].get("life_runtime_epoch", 0),
            version=1,
        )
        self._save_job(item)
        return item

    def recover(self):
        self.reconcile_originals()
        for item in self.store.list("image_jobs", states=["running", "queued"]):
            if item["submitted"]:
                item["state"] = "unknown"
                self._save_job(item)

    def cancel(self, job_id):
        item = self.get(job_id)
        if item["state"] not in TERMINAL:
            item["cancel_requested"] = True
            if not item["submitted"]:
                item["state"] = "cancelled"
            self._save_job(item)
        return item

    async def work(self):
        self.reconcile_originals()
        if self.completion_port:
            await self.completion_port()
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
            except ImageRejected:
                item.update(state="failed", failure="prompt_rejected")
            except Exception as exc:
                item.update(
                    state="unknown" if item["submitted"] else "failed", failure=type(exc).__name__
                )
            finally:
                # Cancellation may arrive while HTTP awaits; preserve that durable intent.
                item["cancel_requested"] |= self.get(item["id"])["cancel_requested"]
                item["deadline"] = self.life.clock()
                self._save_job(item)

    async def _work(self, item):
        prompt_id = item["prompt_id"]
        if not item["submitted"]:
            if self.submission_guard and not await self.submission_guard(item):
                item["state"] = "cancelled"
                return
            actor = self.store.get("life_actors", item["conversation_id"])
            if (
                not actor
                or not actor.get("life_enabled", True)
                or item.get("runtime_epoch", actor.get("life_runtime_epoch", 0))
                != actor.get("life_runtime_epoch", 0)
            ):
                item["state"] = "cancelled"
                return
            for reference in item.get("reference_inputs", []):
                if self.original_reader is None:
                    raise ValueError("Reference owner reader unavailable")
                data, _ = await self.original_reader(
                    item["conversation_id"],
                    reference["scope"],
                    reference["content_ref"],
                    reference["query"],
                    actor_owned=True,
                )
                await self.transport.upload(data, reference["filename"])
            if (
                item.get("edit_source_id")
                and not item.get("reference_uploaded")
                and not item.get("reference_inputs")
            ):
                data, source = self.life.album.read(
                    item["conversation_id"], item["edit_source_id"], item.get("scope")
                )
                await self.transport.upload(data, "tianshu_" + source["sha256"] + ".png")
                item["reference_uploaded"] = True
                self._save_job(item)
            # Persist intent before I/O: even crash/timeout cannot cause automatic resubmission.
            item.update(submitted=True, state="unknown")
            self._save_job(item)
            result = await self.transport.submit(
                prompt_id,
                item.get("submission") or dict(graph=item["graph"], outputs=item["outputs"]),
            )
            item["provider_handle"] = result.handle
            item["state"] = "unknown" if result.state == "completed" else result.state
            if result.error_code:
                item["failure"] = result.error_code
            # A synchronous artifact may still need I/O to download. Persist the
            # provider's recovery handle before that I/O can be interrupted.
            self._save_job(item)
            if result.state == "completed":
                await self._complete(item, result.artifacts)
            return
        plan = item.get("submission") or dict(graph=item["graph"], outputs=item["outputs"])
        handle = item.get("provider_handle")
        if handle is None and item.get("provider", "comfyui") == "comfyui":
            handle = prompt_id
        result = await self.transport.poll(handle, plan)
        item["cancel_requested"] |= self.get(item["id"])["cancel_requested"]
        if item["cancel_requested"] and result.state == "queued":
            result = await self.transport.cancel_pending(handle, plan)
        item["state"] = result.state
        if result.error_code:
            item["failure"] = result.error_code
        if result.state == "completed":
            await self._complete(item, result.artifacts)

    async def _complete(self, item, descriptors):
        await self._artifacts(item, descriptors)
        item["state"] = "completed"
        item["failure"] = None
        item["completed_at"] = self.life.clock()
        for artifact in item["artifacts"]:
            self._record_original(item, artifact)
        self._save_job(item)
        if hasattr(self.life, "album"):
            self.life.album.reconcile_job(item)
        # Running GPU work is not interrupted. Intent remains visible until completion.

    async def _artifacts(self, item, descriptors):
        if not 0 < len(descriptors) <= self.max_files:
            raise ValueError("Artifact count budget")
        self.staging.mkdir(parents=True, exist_ok=True)
        used = sum(p.stat().st_size for p in self.staging.iterdir() if p.is_file())
        artifacts = []
        for descriptor in descriptors:
            data, width, height = await self.transport.image(descriptor, self.max_bytes)
            sha = hashlib.sha256(data).hexdigest()
            name = "original_" + sha + ".png"
            path = self.staging / name
            if not path.exists():
                used += len(data)
                if used > self.max_storage:
                    raise ValueError("Staging storage budget")
                temporary = self.staging / (name + "." + uuid.uuid4().hex + ".tmp")
                try:
                    with temporary.open("xb") as handle:
                        handle.write(data)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temporary, path)
                finally:
                    temporary.unlink(missing_ok=True)
            elif hashlib.sha256(path.read_bytes()).hexdigest() != sha:
                raise ValueError("Original media identity mismatch")
            media_id = "media:" + digest([item["id"], len(artifacts), sha])
            artifacts.append(
                dict(
                    staging_name=name,
                    sha256=sha,
                    size=len(data),
                    media_type="image/png",
                    width=width,
                    height=height,
                    archived=False,
                    media_id=media_id,
                )
            )
        item["artifacts"] = artifacts
        with self.store.transaction():
            for artifact in artifacts:
                self._record_original(item, artifact)

    def _record_original(self, job, artifact):
        reference = dict(
            owner="companion",
            object_id=artifact["media_id"],
            version=1,
            kind="image",
            sha256=artifact["sha256"],
            sources=[],
            coverage=dict(unit="bytes", start=0, end=artifact["size"], total=artifact["size"]),
        )
        media = dict(
            artifact,
            id=artifact["media_id"],
            actor_id=job["conversation_id"],
            conversation_id=job["conversation_id"],
            job_id=job["id"],
            state="available",
            version=1,
            content_ref=reference,
            scope=job.get("scope"),
            completed_at=job.get("completed_at"),
        )
        if not self.media_available(media):
            media["state"] = "unavailable"
        elif (
            hashlib.sha256((self.staging / artifact["staging_name"]).read_bytes()).hexdigest()
            != artifact["sha256"]
        ):
            media["state"] = "unavailable"
        self.store.put("image_media", media)

    def _save_job(self, item):
        previous = self.store.get("image_jobs", item["id"])
        if previous:
            item["cancel_requested"] = item.get("cancel_requested", False) or previous.get(
                "cancel_requested", False
            )
            fields = ("state", "cancel_requested", "artifacts", "failure")
            item["version"] = previous.get("version", 1) + int(
                any(previous.get(field) != item.get(field) for field in fields)
            )
        self.store.put("image_jobs", item)

    def reconcile_originals(self):
        """Adopt only persisted completed descriptors and verified existing files, four jobs per pass."""
        marker = self.store.get("metadata", "image-original-recovery") or dict(
            id="image-original-recovery", cursor="", state="pending"
        )
        if marker["state"] == "completed":
            return
        rows = self.store.db.execute(
            "SELECT body FROM image_jobs WHERE status='completed' AND id>? ORDER BY id LIMIT 4",
            (marker["cursor"],),
        ).fetchall()
        with self.store.transaction():
            for row in rows:
                job = json.loads(row[0])
                for index, artifact in enumerate(job["artifacts"]):
                    artifact.setdefault(
                        "media_id", "media:" + digest([job["id"], index, artifact["sha256"]])
                    )
                    if not self.store.get("image_media", artifact["media_id"]):
                        self._record_original(job, artifact)
                self._save_job(job)
                if hasattr(self.life, "album") and self.store.get(
                    "life_actors", job["conversation_id"]
                ):
                    self.life.album.reconcile_job(job)
                marker["cursor"] = job["id"]
            marker["state"] = "pending" if len(rows) == 4 else "completed"
            self.store.put("metadata", marker)

    def media_available(self, item):
        if self.staging is None or item["state"] != "available":
            return False
        path = (self.staging / item["staging_name"]).resolve()
        return (
            path.parent == self.staging and path.is_file() and path.stat().st_size == item["size"]
        )
