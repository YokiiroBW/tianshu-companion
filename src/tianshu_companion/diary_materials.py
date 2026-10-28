"""Compose source-checked daily material; prose remains in Life's original revisions."""

import copy
from datetime import date, datetime, time, timedelta

from .clients import utc
from .contracts import Fault, canonical, digest
from .life import zone


def civil_window(day, timezone_name, now):
    tz = zone(timezone_name)
    if day == "yesterday":
        day = str(datetime.fromtimestamp(now, tz).date() - timedelta(days=1))
    try:
        civil = date.fromisoformat(day)
        if str(civil) != day:
            raise ValueError()
    except (TypeError, ValueError):
        raise Fault("invalid_input") from None
    return dict(
        start=utc(datetime.combine(civil, time.min, tz).timestamp()),
        end=utc(datetime.combine(civil + timedelta(days=1), time.min, tz).timestamp()),
    )


class DiaryMaterials:
    def __init__(self, life):
        self.life, self.store = life, life.store

    @property
    def available(self):
        return self.life.actor_memory is not None and self.life.actor_memory.available

    async def compose(self, actor_id, day, context):
        actor = self.life._get("actors", actor_id)
        world = self.life._get("worlds", actor["world_id"])
        window = civil_window(day, world["timezone"], self.life.clock())
        pages, material, cursor = [], [], None
        complete, omitted = False, False
        for _ in range(4):
            page = await self.life.actor_memory.query(
                actor_id, context, window=window, cursor=cursor, limit=16, budget_bytes=32768
            )
            pages.append(page)
            omitted = omitted or page["omitted"]
            for row in page["items"]:
                fact = row["fact"]
                # The owner supplies actual occurrence, never the diary's collection time.
                kind = (
                    "digital_experience"
                    if fact["relation"] == "participant" and fact["provenance"] == "digital_action"
                    else "observation"
                )
                if fact["provenance"] == "fictional":
                    kind = "fictional"
                material.append(
                    dict(
                        episode_refs=copy.deepcopy(row["episode_refs"]),
                        kind=kind,
                        provenance=fact["provenance"],
                        relation=fact["relation"],
                        outcome=fact["outcome"],
                        summary=fact["summary"],
                        occurred_at=fact["occurred_at"],
                        learned_at=fact["learned_at"],
                        coverage=copy.deepcopy(fact["content_reads"]),
                    )
                )
            complete = page["complete"]
            if complete or page["next_cursor"] is None or page["next_cursor"] == cursor:
                break
            cursor = page["next_cursor"]
        # Revalidate every earlier page after later remote reads; no cached text is trusted.
        for page in pages:
            await self.life.actor_memory.check(page)
        pending = self.store.db.execute(
            "SELECT 1 FROM outbox WHERE conversation_id=? AND status IN "
            "('actor_memory_pending','actor_memory_unknown','actor_memory_submitting') LIMIT 1",
            (actor_id,),
        ).fetchone()
        omitted = omitted or pending is not None
        kinds = sorted({row["kind"] for row in material})
        real = any(kind != "fictional" for kind in kinds)
        provenance = dict(
            kind="mixed" if real and "fictional" in kinds else "digital" if real else "fictional",
            source_kinds=kinds or ["fictional"],
            selection_complete=complete and not omitted,
            timezone=world["timezone"],
            occurred_from=window["start"],
            occurred_before=window["end"],
        )
        return dict(material=material, proofs=pages, context=context, provenance=provenance)

    async def check(self, diary, *, public=False, audience=None):
        if "source_capture" not in diary:
            return
        capture = diary["source_capture"]
        if "material_version" in diary:
            expected = dict(
                context=capture["context"],
                entries=capture["material"],
                provenance=capture["provenance"],
            )
            stored = self.store.get("life_materials", diary["id"])
            if (
                digest(expected) != diary["material_version"]
                or not stored
                or stored["material"] != expected
                or diary.get("provenance") != capture["provenance"]
            ):
                raise Fault("scope_changed")
        if public and capture["context"]["audience"]["kind"] != "public":
            raise Fault("forbidden")
        if not self.available:
            raise Fault("dependency_unavailable")
        context = capture["context"]
        if (
            audience is not None
            and audience.get("actor_id", diary["conversation_id"]) != diary["conversation_id"]
        ):
            raise Fault("forbidden")
        if (
            audience is not None
            and not diary.get("share_intent")
            and context["audience"]["kind"] != "public"
        ):
            raise Fault("forbidden")
        if diary.get("share_intent"):
            from .diary_sharing import DiarySharing

            context = await DiarySharing(self).context(diary["share_intent"])
            identity = lambda value: {
                key: val for key, val in value["audience"].items() if key != "assertion_ref"
            }
            if identity(context) != identity(capture["context"]):
                raise Fault("scope_changed")
            if audience is not None:
                actual = context["audience"]
                if (
                    actual["conversation_id"] != audience["conversation_id"]
                    or actual["kind"] != ("group" if audience["audience"] == "group" else "private")
                    or actual["kind"] == "private"
                    and actual["person_id"] != audience["person_id"]
                ):
                    raise Fault("forbidden")
        for page in capture["proofs"]:
            await self.life.actor_memory.check(dict(page, context=context))

    async def prepare(self, actor_id, day, *, public=False, context=None, share_intent=None):
        context = context or dict(
            purpose="diary", audience=dict(kind="public" if public else "actor_only")
        )
        capture = await self.compose(actor_id, day, context)
        actor = self.life._get("actors", actor_id)
        recipe = dict(self.life._recipe(actor["recipe"]))
        recipe.update(
            basis="Actual digital actions and observations in the fixed local-day window; subjective interpretations are not events.",
            prohibitions="Never invent physical actions, users' experiences or unseen content. Fictional scenes must be explicitly marked.",
        )
        material = dict(
            context=context, entries=capture["material"], provenance=capture["provenance"]
        )
        material_version = digest(material)
        key = digest([actor_id, day, recipe, material_version])
        old = self.store.get("life_diaries", key)
        if old:
            return self.life.diary_metadata(key)
        state = (
            "queued"
            if capture["material"]
            else "skipped"
            if capture["provenance"]["selection_complete"]
            else "material_incomplete"
        )
        if state == "queued" and not self.life._available():
            state = "unavailable"
        if len(canonical(material).encode()) > 65536:
            state = "material_overflow"
        await self.check(dict(source_capture=capture))
        with self.store.transaction():
            self.store.put("life_materials", dict(id=key, material=material))
            self.life._save(
                "diaries",
                dict(
                    id=key,
                    conversation_id=actor_id,
                    day=day,
                    fictional=capture["provenance"]["kind"] == "fictional",
                    provenance=capture["provenance"],
                    source_capture=capture,
                    **({"share_intent": share_intent} if share_intent else {}),
                    recipe=recipe,
                    material_version=material_version,
                    state=state,
                    config_version=self.life.config_version,
                    current_revision=None,
                    published_revision=None,
                    created_at=self.life.clock(),
                    real_chat_sources="sourced observations only; never the user's own memory",
                ),
            )
        return self.life.diary_metadata(key)

    async def publish(self, diary_id, *, reviewer, expected):
        item = self.life._get("diaries", diary_id)
        await self.check(item, public=True)
        return self.life._publish_diary(diary_id, reviewer=reviewer, expected=expected)

    def validate_result(self, diary, result):
        if not isinstance(result, dict) or set(result) != {"content", "reflections", "questions"}:
            raise Fault("invalid_input")
        if (
            not isinstance(result["reflections"], list)
            or len(result["reflections"]) > 4
            or not isinstance(result["questions"], list)
            or len(result["questions"]) > 3
        ):
            raise Fault("invalid_input")
        pages = diary["source_capture"]["proofs"]
        known = {
            canonical(ref) for page in pages for row in page["items"] for ref in row["episode_refs"]
        }
        for value in result["reflections"]:
            if isinstance(value, dict) and "interest" in value:
                if value.get("kind") != "preference":
                    raise Fault("invalid_input")
                self.life.actor_memory.core.contracts.check(
                    "actor-self-memory#interest", value["interest"]
                )
            if (
                not isinstance(value, dict)
                or set(value) - {"interest"}
                != {"kind", "text", "episode_refs", "importance", "retention"}
                or value["kind"] not in {"reflection", "understanding", "preference"}
                or not isinstance(value["text"], str)
                or not 1 <= len(value["text"]) <= 4000
                or value["importance"] not in {"low", "normal", "high"}
                or value["retention"] not in {"short", "medium", "long"}
                or not isinstance(value["episode_refs"], list)
                or not 1 <= len(value["episode_refs"]) <= 32
                or any(canonical(ref) not in known for ref in value["episode_refs"])
            ):
                raise Fault("invalid_input")
        if any(
            not isinstance(q, str) or not q.strip() or len(q) > 1000 for q in result["questions"]
        ):
            raise Fault("invalid_input")
        return known

    async def finish(self, diary):
        """Resume the same generated conclusions and receipt keys after a lost acknowledgement."""
        result = dict(content="", **diary["postprocess_result"])
        try:
            await self.check(diary)
            await self.completed(diary, result)
        except (Fault, OSError, TimeoutError) as error:
            current = self.life._get("diaries", diary["id"])
            current["postprocess_error"] = getattr(error, "code", "dependency_unavailable")
            # Already-published share sources must retain their registered version.
            if not current["published_revision"]:
                current["state"] = "postprocessing"
                self.life._save("diaries", current)
            return
        current = self.life._get("diaries", diary["id"])
        if current.get("share_intent"):
            return  # Its motive source pins the version saved by the sharing owner.
        if not current["published_revision"]:
            current.pop("postprocess_result", None)
            current.pop("postprocess_error", None)
            current["state"] = "draft"
            self.life._save("diaries", current)

    async def reflect(self, diary, result):
        """Interpretations cite original episodes; diary prose is never a new episode."""
        known = self.validate_result(diary, result)
        pages = diary["source_capture"]["proofs"]

        async def validate():
            await self.check(diary)

        for index, value in enumerate(result["reflections"]):
            await self.life.actor_memory.append_validated_derived(
                diary["conversation_id"],
                diary["id"],
                "diary-reflection:" + str(index),
                value,
                known,
                validate,
            )
        await validate()
        if not pages or not known:
            return
        proof = {
            key: copy.deepcopy(pages[0][key]) for key in ("actor_id", "role_version", "context")
        }
        proof["items"] = [
            dict(ref=copy.deepcopy(row["ref"])) for page in pages for row in page["items"]
        ]
        for question in result["questions"]:
            if not isinstance(question, str) or not question.strip() or len(question) > 1000:
                raise Fault("invalid_input")
            key = "autonomy:diary:" + digest([diary["id"], question])
            if self.store.get("life_concerns", key):
                continue
            self.life.concerns.save(
                diary["conversation_id"],
                dict(
                    id=key,
                    title=question,
                    goal=question,
                    state="open",
                    scope=None,
                    sources=[],
                    fragments=[],
                    next_due_at=self.life.clock(),
                    expires_at=None,
                    result_refs=[],
                    actor_only=True,
                    autonomy_basis=proof,
                ),
                expected=0,
            )

    async def completed(self, diary, result):
        if diary.get("share_intent"):
            from .diary_sharing import DiarySharing

            await DiarySharing(self).complete(diary)
            return
        public = diary["source_capture"]["context"]["audience"]["kind"] == "public"
        subscriptions = [
            row
            for row in self.store.list("proactive_subscriptions", states=["active"])
            if row["actor_id"] == diary["conversation_id"]
        ]
        if not public:
            await self.reflect(diary, result)
            if subscriptions:
                from .diary_disclosure import DiaryDisclosure

                await DiaryDisclosure(self.life.actor_memory).project(diary["source_capture"])
                await self.prepare(diary["conversation_id"], diary["day"], public=True)
                from .diary_sharing import DiarySharing

                sharing = DiarySharing(self)
                for subscription in subscriptions:
                    await sharing.prepare(diary, subscription)
            return
        if not subscriptions:
            return  # No subscription never publishes or sends an autonomous draft.
        await self.publish(
            diary["id"], reviewer="source-verified-subscription", expected=diary["version"]
        )
