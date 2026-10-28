"""Subscription-scoped diary drafts through the existing proactive intention and context."""

from .contracts import Fault, digest


class DiarySharing:
    def __init__(self, materials):
        self.materials, self.life = materials, materials.life
        self.memory = self.life.actor_memory
        self.core, self.store = self.memory.core, self.life.store

    async def context(self, intent):
        motive = self.store.get("proactive_motives", intent["motive_id"])
        sub = self.store.get("proactive_subscriptions", intent["subscription_id"])
        if not motive or not sub or sub["state"] != "active" or motive["state"] == "cancelled":
            raise Fault("scope_changed")
        current = await self.core.runtime_execution.current_context(
            dict(
                actor_id=sub["actor_id"],
                person_id=sub["person_id"],
                audience=sub["audience"],
                conversation_id=sub["conversation_id"],
                subscription_id=sub["id"],
                subscription_version=sub["config_version"],
                subject_id=motive["id"],
                subject_version=motive["version"],
                sources=motive["sources"],
            )
        )
        scope, origin = current["scope"], current["origin"]
        if any(
            scope[key] != sub[key]
            for key in ("actor_id", "person_id", "audience", "conversation_id")
        ):
            raise Fault("scope_changed")
        audience = dict(
            kind="group" if scope["audience"] == "group" else "private",
            conversation_id=scope["conversation_id"],
            assertion_ref=origin["assertion_ref"],
        )
        if audience["kind"] == "private":
            audience["person_id"] = scope["person_id"]
        return dict(purpose="diary", audience=audience)

    async def prepare(self, diary, subscription):
        actor, day = diary["conversation_id"], diary["day"]
        key = "diary-share:" + digest([actor, day, subscription["id"]])
        motive = self.store.get("proactive_motives", key)
        if motive and motive.get("diary_id"):
            return
        if motive is None:
            concern_id = "diary-share-intent:" + digest([actor, day, subscription["id"]])
            concern = self.store.get("life_concerns", concern_id)
            if not concern:
                concern = self.life.concerns.save(
                    actor,
                    dict(
                        id=concern_id,
                        title="分享已获准的昨日感想",
                        goal="只向已有订阅的原受众分享获准材料",
                        state="completed",
                        scope=None,
                        sources=[],
                        fragments=[],
                        next_due_at=None,
                        expires_at=None,
                        result_refs=[],
                        actor_only=True,
                    ),
                    expected=0,
                )
            # The neutral intention is paused synchronously before any await/transport.
            self.core.proactive.register_motive(
                actor,
                dict(
                    id=key,
                    subscription_id=subscription["id"],
                    summary="准备一份仅含当前受众获准材料的日记分享",
                    sources=[
                        dict(owner="companion", object_id=concern_id, version=concern["version"])
                    ],
                    content_refs=[],
                    due_at=self.life.clock() + 86400,
                    expires_at=self.life.clock() + 172800,
                    weight=1,
                ),
            )
            self.core.proactive.pause_subject(
                "motive", key, expected=self.store.get("proactive_motives", key)["version"]
            )
        intent = dict(motive_id=key, subscription_id=subscription["id"])
        context = await self.context(intent)
        result = await self.materials.prepare(actor, day, context=context, share_intent=intent)
        if result["state"] == "material_incomplete":
            raise Fault("dependency_unavailable")
        motive = self.store.get("proactive_motives", key)
        motive["diary_id"] = result["id"]
        self.core.proactive._save("motives", motive)

    async def complete(self, diary):
        intent = diary["share_intent"]
        await self.materials.check(diary)
        motive = self.store.get("proactive_motives", intent["motive_id"])
        if motive.get("diary_revision") == diary["current_revision"]:
            return
        revision = self.life._get("revisions", diary["current_revision"])
        # A scope-only draft has no public publish pointer. Only its matching motive receives prose.
        with self.store.transaction():
            diary.pop("postprocess_result", None)
            diary["state"] = "draft"
            self.life._save("diaries", diary)
            self.core.proactive.register_motive(
                diary["conversation_id"],
                dict(
                    id=motive["id"],
                    subscription_id=intent["subscription_id"],
                    summary=revision["content"],
                    sources=[
                        dict(owner="companion", object_id=diary["id"], version=diary["version"])
                    ],
                    content_refs=[],
                    due_at=self.life.clock(),
                    expires_at=self.life.clock() + 86400,
                    weight=1,
                    diary_id=diary["id"],
                    diary_revision=revision["id"],
                ),
                expected=motive["version"],
            )
