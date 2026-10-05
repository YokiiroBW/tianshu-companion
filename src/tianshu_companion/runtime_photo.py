"""A proactive photo stays attached to its original sourced candidate and job."""

from .clients import query
from .contracts import Fault, digest
import json


class RuntimePhotos:
    def __init__(self, execution):
        self.execution, self.core, self.store = execution, execution.core, execution.store

    def from_image_event(self, candidate):
        for source in candidate.get("sources", []):
            if source["owner"] == "companion":
                event = self.store.get("life_events", source["object_id"])
                if event and event.get("kind") == "image_completed":
                    return True
        return False

    def completion_subscription(self, event):
        if event.get("kind") != "image_completed":
            return None
        for reference in event.get("content_refs", []):
            media = self.store.get("image_media", reference["object_id"])
            job = self.store.get("image_jobs", media["job_id"]) if media else None
            if job and job.get("proactive_subscription_id"):
                return job["proactive_subscription_id"]
        return None

    async def work(self):
        marker = self.store.get("metadata", "photo-waiting:cursor") or dict(
            id="photo-waiting:cursor", cursor=""
        )
        query_ = "SELECT body FROM proactive_candidates WHERE status='ready' AND id>? AND json_extract(body,'$.photo') IS NOT NULL AND json_extract(body,'$.photo_state')!='completed' ORDER BY id LIMIT 4"
        rows = self.store.db.execute(query_, (marker["cursor"],)).fetchall()
        if not rows and marker["cursor"]:
            marker["cursor"] = ""
            rows = self.store.db.execute(query_, ("",)).fetchall()
        for row in rows:
            candidate = json.loads(row[0])
            marker["cursor"] = candidate["id"]
            await self.prepare(candidate)
        self.store.put("metadata", marker)

    async def before_submission(self, job):
        candidate_id = job.get("proactive_candidate_id")
        if not candidate_id:
            return True
        candidate = self.store.get("proactive_candidates", candidate_id)
        if not candidate or candidate["state"] != "ready":
            return False
        try:
            current = await self.execution.current_context(candidate)
            await self.execution.validate_sources(candidate, current["origin"])
            return True
        except Fault:
            return False

    async def prepare(self, candidate):
        """Returns true only when an exact completed original is ready to dispatch."""
        decision = candidate.get("photo")
        if not decision:
            return True
        job_id = candidate.get("photo_job_id") or "proactive-photo:" + digest(
            [candidate["id"], candidate["subject_version"]]
        )
        try:
            current = await self.execution.current_context(candidate)
            await self.execution.validate_sources(candidate, current["origin"])
            job = self.store.get("image_jobs", job_id)
            if job is None:
                value = dict(
                    id=job_id,
                    parameters=decision["parameters"],
                    outfit_id=None,
                    scene=candidate["summary"],
                    activity_id=None,
                    edit_source_id=None,
                    edit_source_ref=None,
                    scope=current["scope"],
                    query=query(current["origin"]),
                    intent=decision["intent"],
                )
                request = dict(
                    schema_version=2,
                    request_id="photo-admit:" + digest(job_id),
                    actor_id=candidate["actor_id"],
                    operation="image.request",
                    expected_version=0,
                    value=value,
                )
                self.core.contracts.check("life-runtime#manage_request", request)
                prepared = await self.core.life_runtime.prepare_action(
                    candidate["actor_id"], "image.request", value, 0
                )
                fresh = self.store.get("proactive_candidates", candidate["id"])
                if (
                    not fresh
                    or fresh["state"] != "ready"
                    or fresh["subject_version"] != candidate["subject_version"]
                ):
                    return False
                latest = await self.execution.current_context(fresh)
                await self.execution.validate_sources(fresh, latest["origin"])
                with self.store.transaction():
                    self.core.life.concerns.operation(
                        "actor:" + candidate["actor_id"],
                        request,
                        lambda: self.core.life_runtime._execute(
                            candidate["actor_id"],
                            "image.request",
                            value,
                            0,
                            request["request_id"],
                            candidate["actor_id"],
                            prepared=prepared,
                        ),
                    )
                    job = self.store.get("image_jobs", job_id)
                    job.update(
                        proactive_candidate_id=candidate["id"],
                        proactive_subscription_id=candidate["subscription_id"],
                    )
                    self.core.images._save_job(job)
                    fresh.update(
                        photo_job_id=job_id, photo_state="waiting", decision="photo_waiting"
                    )
                    self.core.proactive._save("candidates", fresh)
            fresh = self.store.get("proactive_candidates", candidate["id"])
            if job["state"] in {"failed", "cancelled"}:
                fresh["photo_state"] = job["state"]
                self.core.proactive._apply(
                    fresh, "cancelled", None, "photo_" + job["state"], self.core.clock()
                )
                return False
            if job["state"] != "completed":
                return False
            references = [
                self.store.get("image_media", artifact["media_id"])["content_ref"]
                for artifact in job["artifacts"]
            ]
            fresh.update(
                photo_state="completed", photo_job_id=job_id, photo_content_refs=references
            )
            self.core.proactive._save("candidates", fresh)
            return True
        except (Fault, ValueError, KeyError, OSError):
            job = self.store.get("image_jobs", job_id)
            if job and not job["submitted"]:
                self.core.images.cancel(job_id)
            fresh = self.store.get("proactive_candidates", candidate["id"])
            if fresh and fresh["state"] == "ready":
                fresh["photo_state"] = "unavailable"
                self.core.proactive._apply(
                    fresh, "cancelled", None, "photo_unavailable", self.core.clock()
                )
            return False
