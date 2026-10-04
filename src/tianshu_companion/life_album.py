"""Actor albums reference persistent originals; delivery owns sharing receipts."""

import hashlib
import json

from .contracts import Fault, digest
from .life import text
from .life_work import require_version, visible


class Album:
    def __init__(self, life, images):
        self.life, self.images, self.store = life, images, life.store

    def attach(self, actor_id, value, *, expected=0):
        self.life._get("actors", actor_id)
        job = self.images.get(value["job_id"])
        if job["conversation_id"] != actor_id or job["state"] != "completed":
            raise Fault("not_found")
        index = value["artifact_index"]
        if type(index) is not int or not 0 <= index < len(job["artifacts"]):
            raise Fault("invalid_input")
        artifact = job["artifacts"][index]
        media = self.store.get("image_media", artifact.get("media_id", ""))
        if not media or media["actor_id"] != actor_id:
            raise Fault("dependency_unavailable")
        if media.get("scope") is not None and media["scope"] != value["scope"]:
            raise Fault("forbidden")
        text(value["caption"], 4000)
        activity_id = value["activity_id"]
        if activity_id is not None:
            self.life.activities.get(actor_id, activity_id)
            if activity_id != job.get("activity_id"):
                raise Fault("invalid_input")
        with self.store.transaction():
            old = self.store.get("life_album", value["id"])
            if old and old["actor_id"] != actor_id:
                raise Fault("not_found")
            require_version(old, expected)
            item = dict(
                value,
                id=value["id"],
                actor_id=actor_id,
                conversation_id=actor_id,
                version=expected + 1,
                state="available",
                media_id=media["id"],
                scene_at=job["created_at"],
                completed_at=media["completed_at"],
                content_ref=media["content_ref"],
            )
            self.store.put("life_album", item)
            return item

    def remove(self, actor_id, entry_id, *, expected, reason):
        with self.store.transaction():
            item = self.store.get("life_album", entry_id)
            if not item or item["actor_id"] != actor_id:
                raise Fault("not_found")
            require_version(item, expected)
            item.update(state="removed", reason=text(reason, 4000), version=expected + 1)
            self.store.put("life_album", item)
            return item

    def page(self, actor_id, *, scope=None, after=None, limit=20, object_id=None):
        rows = self.store.db.execute(
            "SELECT body FROM life_album WHERE conversation_id=? AND ((? IS NULL AND id>?) OR id=?) ORDER BY id LIMIT ?",
            (actor_id, object_id, after or "", object_id, limit + 1),
        ).fetchall()
        items = [json.loads(row[0]) for row in rows]
        values = []
        for item in items[:limit]:
            if item["state"] == "removed" or not visible(item, scope):
                continue
            projected = dict(item)
            media = self.store.get("image_media", item["media_id"])
            if not media or not self.images.media_available(media):
                projected["state"] = "unavailable"
            values.append(projected)
        return values, items[limit - 1]["id"] if len(items) > limit else None

    def read(self, actor_id, media_id, scope=None):
        """Read the original only under its current album grant and actor ownership."""
        media = self.store.get("image_media", media_id)
        if not media or media["actor_id"] != actor_id or not visible(media, scope):
            raise Fault("not_found")
        rows = self.store.db.execute(
            "SELECT body FROM life_album WHERE conversation_id=? "
            "AND json_extract(body,'$.media_id')=? AND status='available' LIMIT 64",
            (actor_id, media_id),
        ).fetchall()
        if not any(visible(json.loads(row[0]), scope) for row in rows):
            raise Fault("not_found")
        if not self.images.media_available(media):
            raise Fault("dependency_unavailable")
        data = (self.images.staging / media["staging_name"]).read_bytes()
        if len(data) != media["size"] or hashlib.sha256(data).hexdigest() != media["sha256"]:
            raise Fault("dependency_unavailable")
        return data, media

    def reconcile_job(self, job):
        """Completion linkage retries do not issue another image request."""
        if job["state"] != "completed":
            return
        for index, artifact in enumerate(job["artifacts"]):
            key = "album:" + digest([job["id"], artifact["media_id"]])
            if self.store.get("life_album", key):
                continue
            self.attach(
                job["conversation_id"],
                dict(
                    id=key,
                    job_id=job["id"],
                    artifact_index=index,
                    activity_id=job.get("activity_id"),
                    caption=job.get("scene") or "生活留影",
                    scope=job.get("scope"),
                ),
                expected=0,
            )
        if job.get("completed_at") is None:
            # Adopting a verified old original does not date a new lived experience.
            return
        actor = self.life._get("actors", job["conversation_id"])
        self.life._event(
            dict(
                id="image-event:" + digest(job["id"]),
                world_id=actor["world_id"],
                participants=[actor["id"]],
                visible_to=[],
                summary=job.get("scene") or "完成一张生活留影",
                occurred_at=job["completed_at"],
                fictional=True,
                kind="image_completed",
                scope=job.get("scope"),
                content_refs=[
                    self.store.get("image_media", artifact["media_id"])["content_ref"]
                    for artifact in job["artifacts"]
                ],
            )
        )
