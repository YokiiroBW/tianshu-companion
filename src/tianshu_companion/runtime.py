"""Production role-life use cases and authorized projections; app only transports."""

import json

from .contracts import Fault, canonical, digest
from .clients import uid
from .life_work import require_version, visible


class LifeRuntime:
    def __init__(self, core):
        self.core, self.life, self.store = core, core.life, core.store
        self.reader = None

    async def _read_origin(self, service, query):
        # Reader admission remains its own credential + life grant. A deployed read-only
        # caller may reuse the registered Platform resolver for a real Platform assertion.
        # The resolver proves the publisher/account/channel; it does not grant life reading.
        issuers = getattr(self.core.origins, "issuers", None)
        resolver = "platform" if issuers is not None and service not in issuers else service
        return await self.core.origins.resolve(resolver, query, self.core.clock())

    async def prepare_action(self, actor, operation, value, expected, *, model_slot_held=False):
        if operation == "image.request":
            if model_slot_held and "character" in (value.get("intent") or {}):
                raise Fault("forbidden")
            compiled = await self.core.image_backend.catalog.prepare_request(
                actor, value, model_slot_held=model_slot_held
            )
            references = await self.core.image_backend.prepare_job(actor, value)
            for reference in references:
                self.core.image_backend.remember_reference(
                    actor, reference["content_ref"], reference["scope"], reference["query"]
                )
            return dict(reference_inputs=references, **compiled)
        if operation == "actor.image-reference.configure":
            await self.core.image_backend.authorize_reference(
                actor, value["content_ref"], value["scope"], value["query"]
            )
        elif operation == "outfit.put" and isinstance(value["reference"], dict):
            await self.core.image_backend.authorize_reference(
                actor, value["reference"], value["source_scope"], value["query"]
            )
            self.core.image_backend.remember_reference(
                actor, value["reference"], value["source_scope"], value["query"]
            )
        return None

    def _actor(self, actor_id):
        actor = self.store.get("life_actors", actor_id)
        if not actor:
            raise Fault("not_found")
        return actor

    def _chapter(self, actor_id, chapter_id):
        chapter = self.core.writing._get("chapters", chapter_id)
        work = self.core.writing._work(chapter["work_id"])
        if work["actor_id"] != actor_id:
            raise Fault("not_found")
        return chapter

    async def manage(self, service, request):
        if service != "platform":
            raise Fault("forbidden")
        self.core.contracts.check("life-runtime#manage_request", request)
        actor_id = request["actor_id"]
        self._actor(actor_id)
        operation, value, expected = (
            request["operation"],
            request["value"],
            request["expected_version"],
        )
        replay = (
            self.life.concerns.replay(service, request)
            if not (operation.startswith("reading.") or operation == "content.acquire")
            else None
        )
        if replay:
            return dict(
                schema_version=2,
                request_id=request["request_id"],
                actor_id=actor_id,
                operation=operation,
                result=replay,
            )
        action_inputs = await self.prepare_action(actor_id, operation, value, expected)
        if operation.startswith("reading.") or operation == "content.acquire":
            result = await self.core.reading.action(service, request)
            return dict(
                schema_version=2,
                request_id=request["request_id"],
                actor_id=actor_id,
                operation=operation,
                result=result,
            )
        prepared = (
            await self.core.image_backend.prepare(value, expected)
            if operation == "image.backend.configure"
            else None
        )

        def execute():
            if prepared is not None:
                backend = self.core.image_backend.apply(prepared, expected)
                result = dict(
                    id=backend["id"],
                    version=backend["version"],
                    state=backend["state"],
                    operation_ref=request["request_id"],
                )
            else:
                result = self._execute(
                    actor_id,
                    operation,
                    value,
                    expected,
                    request["request_id"],
                    service,
                    prepared=action_inputs,
                )
            self.core.contracts.check(
                "life-runtime#manage_response",
                dict(
                    schema_version=2,
                    request_id=request["request_id"],
                    actor_id=actor_id,
                    operation=operation,
                    result=result,
                ),
            )
            return result

        try:
            result = self.life.concerns.operation(service, request, execute)
        except KeyError:
            raise Fault("not_found") from None
        except ValueError:
            raise Fault("invalid_input") from None
        response = dict(
            schema_version=2,
            request_id=request["request_id"],
            actor_id=actor_id,
            operation=operation,
            result=result,
        )
        self.core.contracts.check("life-runtime#manage_response", response)
        return response

    def _execute(
        self, actor_id, operation, value, expected, request_id, operator, *, prepared=None
    ):
        if operation == "concern.save":
            result = self.life.concerns.save(actor_id, value, expected=expected)
        elif operation == "concern.close":
            result = self.life.concerns.close(
                actor_id, value["id"], expected=expected, reason=value["reason"]
            )
        elif operation == "activity.save":
            result = self.life.activities.save(actor_id, value, expected=expected)
        elif operation in {"activity.pause", "activity.resume", "activity.cancel"}:
            result = self.life.activities.transition(
                actor_id,
                value["id"],
                {
                    "activity.pause": "paused",
                    "activity.resume": "running",
                    "activity.cancel": "cancelled",
                }[operation],
                expected=expected,
                reason=value.get("reason"),
            )
        elif operation == "affect.feedback":
            result = self.life.affect.feedback(actor_id, value)
        elif operation == "outfit.put":
            result = self.core.images.put_outfit(
                value["id"],
                description=value["description"],
                prompt=value["prompt"],
                reference=value["reference"],
                source_scope=value.get("source_scope"),
                activities=value["activities"],
                expected=expected if expected else None,
            )
        elif operation == "outfit.select":
            result = self.core.images.select_outfit(actor_id, value["id"], expected=expected)
        elif operation == "image.request":
            result = self.core.images.request(
                value["id"],
                actor_id,
                parameters=value["parameters"],
                outfit_id=value["outfit_id"],
                activity_id=value["activity_id"],
                scene=value["scene"],
                edit_source_id=value.get("edit_source_id"),
                scope=value.get("scope"),
                intent=value.get("intent"),
                **(prepared or {}),
            )
        elif operation == "actor.image-reference.configure":
            result = self.core.image_backend.configure_reference(actor_id, value, expected)
        elif operation == "image.cancel":
            job = self.core.images.get(value["id"])
            if job["conversation_id"] != actor_id:
                raise Fault("not_found")
            require_version(job, expected)
            self.core.images.cancel(value["id"])
            result = self.core.images.get(value["id"])
        elif operation == "album.attach":
            result = self.life.album.attach(actor_id, value, expected=expected)
        elif operation == "album.remove":
            result = self.life.album.remove(
                actor_id, value["id"], expected=expected, reason=value["reason"]
            )
        elif operation == "diary.request":
            result = self.life.request_diary(actor_id, value["day"])
        elif operation in {"diary.revise", "diary.publish"}:
            diary = self.life._get("diaries", value["id"])
            if diary["actor_id"] != actor_id:
                raise Fault("not_found")
            if operation == "diary.revise":
                self.life.revise_diary(
                    value["id"],
                    value["content"],
                    editor=operator,
                    reason=value["reason"],
                    expected=expected,
                )
            else:
                self.life.publish_diary(value["id"], reviewer=operator, expected=expected)
            result = self.life._get("diaries", value["id"])
        elif operation == "writing.create":
            require_version(self.store.get("write_works", value["id"]), expected)
            result = self.core.writing.create_work(
                value["id"],
                actor_id,
                title=value["title"],
                outline=value["outline"],
                characters=value["characters"],
                recipe=value["recipe"],
            )
        elif operation == "chapter.add":
            work = self.core.writing._work(value["work_id"])
            if work["actor_id"] != actor_id:
                raise Fault("not_found")
            require_version(work, expected)
            result = self.core.writing.add_chapter(
                value["id"],
                value["work_id"],
                title=value["title"],
                goal=value["goal"],
                order=value["order"],
            )
        elif operation.startswith("chapter."):
            chapter = self._chapter(actor_id, value["id"])
            require_version(chapter, expected)
            if operation == "chapter.generate":
                self.core.writing.request_chapter(
                    value["id"], request_id=request_id, expected=expected
                )
            elif operation == "chapter.revise":
                self.core.writing.revise_chapter(
                    value["id"],
                    value["content"],
                    editor=operator,
                    reason=value["reason"],
                    expected=expected,
                )
            elif operation == "chapter.review":
                self.core.writing.review_chapter(
                    value["id"],
                    chapter["current_revision"],
                    reviewer=operator,
                    decision=value["decision"],
                    notes=value["notes"],
                    expected=expected,
                    acknowledge_drift=value["acknowledge_drift"],
                )
            elif operation == "chapter.publish":
                self.core.writing.publish_chapter(value["id"], reviewer=operator, expected=expected)
            else:
                raise Fault("invalid_input")
            result = self._chapter(actor_id, value["id"])
        elif operation == "proactive.subscription":
            result = self.core.proactive.register_subscription(
                actor_id,
                person_id=value["person_id"],
                audience=value["audience"],
                conversation_id=value["conversation_id"],
                channel=value["channel"],
                timezone_name=value["timezone_name"],
                quiet=(value["quiet_start"], value["quiet_end"]),
                cooldown_seconds=value["cooldown_seconds"],
                daily_limit=value["daily_quota"],
                unanswered_limit=value["unanswered_limit"],
                expiry_seconds=value["expiry_seconds"],
                consent=dict(
                    registered_by=operator,
                    basis=value["consent_basis"],
                    evidence_ref=value["consent_ref"],
                ),
                expected=expected or None,
            )
        elif operation == "proactive.subscription.state":
            subscription = self.core.proactive._get("subscriptions", value["id"])
            if subscription["actor_id"] != actor_id:
                raise Fault("not_found")
            require_version(subscription, expected)
            if value["state"] == "paused":
                result = self.core.proactive.pause_subscription(value["id"], expected=expected)
            elif value["state"] == "active":
                result = self.core.proactive.resume_subscription(value["id"], expected=expected)
            else:
                result = self.core.proactive.revoke_subscription(
                    value["id"], expected=expected, reason="explicit_admin_revocation"
                )
        elif operation == "proactive.motive":
            result = self.core.proactive.register_motive(actor_id, value, expected=expected)
        elif operation == "proactive.cancel":
            candidate = self.core.proactive.candidate_view(value["id"])
            if candidate["actor_id"] != actor_id:
                raise Fault("not_found")
            self.core.proactive.cancel_candidate(
                value["id"], reason=value["reason"], expected=expected
            )
            result = self.store.get("proactive_candidates", value["id"])
        else:
            raise Fault("invalid_input")
        return dict(
            id=result["id"],
            version=result.get("version", 1),
            state=result.get("state", "active"),
            operation_ref=request_id,
        )

    async def authorize_read(self, service, request):
        if self.reader is None:
            raise Fault("dependency_unavailable")
        entry = self.reader.readers.get(service)
        if not entry:
            raise Fault("forbidden")
        actor_id = request["actor_id"]
        self.reader._require_grant(entry, actor_id)
        self._actor(actor_id)
        query = request["query"]
        self.core.contracts.check("common#query", query)
        context = await self._read_origin(service, query)
        allowed = context["allowed_scope"]
        if allowed["actor_id"] != actor_id:
            raise Fault("forbidden")
        scope = request["scope"]
        if scope is not None:
            identity = await self.core.memory.resolve_identity(
                query["origin"], context["verified_account"]
            )
            if identity is None or scope["actor_id"] != actor_id:
                raise Fault("forbidden")
            own_scope = dict(
                actor_id=actor_id,
                person_id=identity,
                audience=allowed["audience"],
                conversation_id=allowed["conversation_id"],
            )
            if scope != own_scope:
                if not own_scope["audience"] or not own_scope["conversation_id"]:
                    raise Fault("forbidden")
                selection = await self.core.memory.select(
                    query["origin"], own_scope, "scope validity", dict(tokens=0, bytes=0)
                )
                check = next(
                    (
                        item
                        for item in selection["scope_checks"]
                        if item["scope"] == scope and item["association_id"] is not None
                    ),
                    None,
                )
                if check is None:
                    raise Fault("forbidden")
                return entry, scope
            if any(
                scope[key] != allowed[key]
                for key in ("audience", "conversation_id")
                if allowed[key] is not None
            ):
                raise Fault("forbidden")
        return entry, scope

    async def ensure_conversation(self, service, request):
        self.core.contracts.check("life-runtime#conversation_ensure_request", request)
        await self.authorize_read(service, dict(request, scope=None))
        context = await self._read_origin(service, request["query"])
        actor, channel, account = (
            request["actor_id"],
            context["verified_channel"],
            context["verified_account"],
        )
        allowed = context["allowed_scope"]
        current = self._actor(actor)
        if not current.get("life_enabled", True) or not self.core.reading.actor_enabled(actor):
            raise Fault("forbidden")
        binding = self.core.bindings.get(channel["binding_id"])
        if (
            not binding
            or binding["service"] != context["authenticated_service"]
            or binding["namespace"] != channel["namespace"]
            or binding["audience"] != allowed["audience"]
            or actor not in binding["actor_ids"]
        ):
            raise Fault("forbidden")
        person, version = await self.core.memory.identity(
            request["query"]["origin"], account, self.core.clock()
        )
        if allowed["person_id"] not in (None, person):
            raise Fault("scope_changed")
        with self.store.transaction():
            conversation = self.store.get("conversations", digest(channel))
            if conversation is None:
                conversation = dict(
                    id=digest(channel),
                    conversation_id=uid("conv"),
                    channel=channel,
                    ingest_sequence=0,
                    turn_sequence=0,
                )
                self.store.put("conversations", conversation)
            if conversation.get("source_quarantined") or allowed["conversation_id"] not in (
                None,
                conversation["conversation_id"],
            ):
                raise Fault("scope_changed")
            scope = dict(
                actor_id=actor,
                person_id=person,
                audience=allowed["audience"],
                conversation_id=conversation["conversation_id"],
            )
            result = dict(
                schema_version=2,
                request_id=request["query"]["request_id"],
                actor_id=actor,
                scope=scope,
                channel=channel,
                binding_version=version,
            )
            self.core.contracts.check("life-runtime#conversation_ensure_response", result)
            return result

    def proactive_control(self, service, request):
        if service != "platform":
            raise Fault("forbidden")
        self.core.contracts.check("life-runtime#control_read_request", request)
        actor, person, channel = request["actor_id"], request["person_id"], request["channel"]
        self._actor(actor)
        binding = self.core.bindings.get(channel["binding_id"])
        if (
            not binding
            or binding["service"] != service
            or binding["namespace"] != channel["namespace"]
            or actor not in binding["actor_ids"]
        ):
            raise Fault("forbidden")
        rows = self.store.db.execute(
            "SELECT body FROM proactive_subscriptions WHERE json_extract(body,'$.actor_id')=? AND json_extract(body,'$.person_id')=? ORDER BY id LIMIT 51",
            (actor, person),
        ).fetchall()
        subscriptions = [
            json.loads(row[0]) for row in rows if json.loads(row[0])["channel"] == channel
        ]
        if len(subscriptions) > 50:
            raise Fault("budget_exceeded")
        projected, deliveries = [], []
        for item in subscriptions:
            projected.append(
                dict(
                    id=item["id"],
                    version=item["version"],
                    state=item["state"],
                    timezone_name=item["timezone"],
                    quiet_start=item["quiet"]["start_civil"],
                    quiet_end=item["quiet"]["end_civil"],
                    cooldown_seconds=item["cooldown_seconds"],
                    daily_quota=item["daily_limit"],
                    unanswered_limit=item["unanswered_limit"],
                    expiry_seconds=item["expiry_seconds"],
                    consent_basis=item["consent"]["basis"],
                )
            )
            candidates = self.store.db.execute(
                "SELECT body FROM proactive_candidates WHERE json_extract(body,'$.subscription_id')=? ORDER BY position DESC,id LIMIT ?",
                (item["id"], 50 - len(deliveries)),
            ).fetchall()
            for row in candidates:
                candidate = json.loads(row[0])
                deliveries.append(
                    dict(
                        id=candidate["id"],
                        subscription_id=item["id"],
                        state=candidate["state"],
                        created_at=candidate.get("created_at"),
                        updated_at=candidate.get("updated_at"),
                    )
                )
        response = dict(request, subscriptions=projected, deliveries=deliveries)
        self.core.contracts.check("life-runtime#control_read_response", response)
        return response

    def _page(self, table, actor_id, after, limit, object_id=None):
        rows = self.store.db.execute(
            f"SELECT body FROM {table} WHERE conversation_id=? AND ((? IS NULL AND id>?) OR id=?) ORDER BY id LIMIT ?",
            (actor_id, object_id, after or "", object_id, limit + 1),
        ).fetchall()
        items = [json.loads(row[0]) for row in rows]
        return items[:limit], items[limit - 1]["id"] if len(items) > limit else None

    async def read(self, service, request):
        self.core.contracts.check("life-runtime#read_request", request)
        entry, scope = await self.authorize_read(service, request)
        actor_id, resource = request["actor_id"], request["resource"]
        actor = self._actor(actor_id)
        limit, after = request["limit"], request["after"]
        next_cursor = None
        if resource == "state":
            summary = self.life.summary(actor_id, scope)
            items = [
                dict(
                    actor_id=actor_id,
                    actor_version=actor["version"],
                    activity=summary["activity"],
                    activity_id=(self.life.activities.current(actor_id, scope) or {}).get("id"),
                    timezone=summary["timezone"],
                    mood=summary["mood"],
                    outfit_ref=summary["outfit_ref"],
                    plan_id=actor.get("daily_plan_id"),
                    changed_at=summary["changed_at"],
                    fictional=True,
                )
            ]
        elif resource == "activities":
            items, next_cursor = self.life.activities.page(
                actor_id, scope=scope, after=after, limit=limit, object_id=request["object_id"]
            )
            keys = (
                "id",
                "version",
                "actor_id",
                "title",
                "state",
                "checkpoint",
                "next_due_at",
                "resume_condition",
                "sources",
                "result_refs",
                "scope",
                "started_at",
                "updated_at",
            )
            items = [{key: item[key] for key in keys} for item in items]
        elif resource == "open_work":
            items, next_cursor = self.life.concerns.page(
                actor_id, scope=scope, after=after, limit=limit, object_id=request["object_id"]
            )
            keys = (
                "id",
                "version",
                "actor_id",
                "title",
                "goal",
                "state",
                "scope",
                "sources",
                "fragments",
                "next_due_at",
                "expires_at",
                "result_refs",
                "intent_valid",
                "updated_at",
            )
            items = [{key: item[key] for key in keys} for item in items]
        elif resource == "affect":
            items = [self.life.affect.snapshot(actor_id, scope)]
        elif resource == "album":
            items, next_cursor = self.life.album.page(
                actor_id, scope=scope, after=after, limit=limit, object_id=request["object_id"]
            )
            keys = (
                "id",
                "version",
                "actor_id",
                "media_id",
                "activity_id",
                "caption",
                "scope",
                "scene_at",
                "completed_at",
                "state",
                "content_ref",
            )
            items = [{key: item[key] for key in keys} for item in items]
        elif resource == "outfits":
            rows = self.store.db.execute(
                "SELECT body FROM image_outfits WHERE ((? IS NULL AND id>?) OR id=?) ORDER BY id LIMIT ?",
                (request["object_id"], after or "", request["object_id"], limit + 1),
            ).fetchall()
            values = [json.loads(row[0]) for row in rows]
            items = [
                {
                    key: item[key]
                    for key in (
                        "id",
                        "version",
                        "description",
                        "prompt",
                        "reference",
                        "activities",
                        "source_scope",
                    )
                    if key in item
                }
                for item in values[:limit]
                if not isinstance(item["reference"], dict)
                or visible(dict(scope=item.get("source_scope")), scope)
            ]
            next_cursor = values[limit - 1]["id"] if len(values) > limit else None
        elif resource == "media":
            items, next_cursor = self._page(
                "image_media", actor_id, after, limit, request["object_id"]
            )
            keys = ("id", "version", "actor_id", "state", "media_type", "size", "content_ref")
            items = [{key: item[key] for key in keys} for item in items if visible(item, scope)]
        elif resource == "works":
            values, next_cursor = self._page(
                "write_works", actor_id, after, limit, request["object_id"]
            )
            items = [
                dict(
                    {
                        key: item[key]
                        for key in ("id", "version", "actor_id", "title", "outline", "state")
                    },
                    characters=[
                        character
                        if isinstance(character, dict)
                        else dict(name="角色" + str(index + 1), description=character)
                        for index, character in enumerate(item["characters"])
                    ],
                    chapter_ids=[
                        chapter["id"] for chapter in self.core.writing._ordered(item["id"])
                    ],
                )
                for item in values
                if visible(item, scope)
            ]
        elif resource == "chapter":
            if request["object_id"] is None:
                raise Fault("invalid_input")
            chapter = self._chapter(actor_id, request["object_id"])
            if not visible(self.core.writing._work(chapter["work_id"]), scope):
                raise Fault("not_found")
            if request["expected_version"] is not None:
                require_version(chapter, request["expected_version"])
            if request.get("chapter_view", "published") == "current":
                if service != "platform":
                    raise Fault("forbidden")
                revision_id = chapter["current_revision"]
                content = (
                    self.core.writing._get("revisions", revision_id)["content"]
                    if revision_id
                    else None
                )
            else:
                if chapter["published_revision"] is None:
                    raise Fault("not_found")
                try:
                    read = self.core.writing.read_chapter(chapter["id"], reader=entry["reader_id"])
                except ValueError:
                    raise Fault("not_found") from None
                revision_id, content = chapter["published_revision"], read["content"]
            items = [
                dict(
                    id=chapter["id"],
                    version=chapter["version"],
                    work_id=chapter["work_id"],
                    title=chapter["title"],
                    goal=chapter["goal"],
                    state=chapter["state"],
                    revision_id=revision_id,
                    content=content,
                )
            ]
        elif resource == "proactive":
            # Candidates are keyed by conversation, so query by actual actor and scope.
            rows = self.store.db.execute(
                "SELECT body FROM proactive_candidates WHERE json_extract(body,'$.actor_id')=? AND ((? IS NULL AND id>?) OR id=?) ORDER BY id LIMIT ?",
                (actor_id, request["object_id"], after or "", request["object_id"], limit + 1),
            ).fetchall()
            values = [json.loads(row[0]) for row in rows]
            next_cursor = values[limit - 1]["id"] if len(values) > limit else None
            items = [
                dict(
                    id=item["id"],
                    version=item["version"],
                    actor_id=actor_id,
                    person_id=item["person_id"],
                    audience=item["audience"],
                    conversation_id=item["conversation_id"],
                    state=item["state"],
                    summary=item.get("summary", item["content"]),
                    due_at=item["due_at"],
                    expires_at=item["expires_at"],
                    delivered=item["delivered"],
                    content_refs=item.get("content_refs", []),
                )
                for item in values[:limit]
                if scope is not None
                and all(
                    item[key] == scope[key]
                    for key in ("actor_id", "person_id", "audience", "conversation_id")
                )
            ]
        elif resource == "image_backend":
            if service != "platform":
                raise Fault("forbidden")
            items = [await self.core.image_backend.view()]
        elif resource == "image_reference":
            reference = self.core.image_backend.reference(actor_id)
            if reference and reference["content_ref"] and not visible(reference, scope):
                raise Fault("forbidden")
            items = [reference]
        elif resource == "image_jobs":
            values, next_cursor = self._page(
                "image_jobs", actor_id, after, limit, request["object_id"]
            )
            items = []
            for value in values:
                if not visible(value, scope):
                    continue
                artifacts = []
                for artifact in value["artifacts"]:
                    media = self.store.get("image_media", artifact.get("media_id", ""))
                    if media:
                        artifacts.append(
                            dict(media_id=media["id"], content_ref=media["content_ref"])
                        )
                items.append(
                    dict(
                        id=value["id"],
                        version=value.get("version", 1),
                        actor_id=actor_id,
                        state=value["state"],
                        error_code=value.get("failure"),
                        created_at=value["created_at"],
                        completed_at=value.get("completed_at"),
                        artifacts=artifacts,
                    )
                )
        elif resource == "reading":
            items, next_cursor = self.core.reading.page(
                actor_id, scope=scope, after=after, limit=limit, object_id=request["object_id"]
            )
        else:
            raise Fault("invalid_input")
        if request["object_id"] is not None and resource != "chapter":
            items = [item for item in items if item.get("id") == request["object_id"]]
            if not items:
                raise Fault("not_found")
            if request["expected_version"] is not None:
                require_version(items[0], request["expected_version"])
        result = dict(
            schema_version=2,
            request_id=request["query"]["request_id"],
            actor_id=actor_id,
            resource=resource,
            items=items,
            next_cursor=next_cursor,
        )
        if len(canonical(result).encode()) > 1048576:
            raise Fault("budget_exceeded")
        self.core.contracts.check("life-runtime#read_response", result)
        return result

    async def media(self, service, request):
        self.core.contracts.check("life-runtime#media_request", request)
        _, scope = await self.authorize_read(service, request)
        return self.life.album.read(request["actor_id"], request["media_id"], scope)

    async def content(self, service, request):
        self.core.contracts.check("life-runtime#content_read_request", request)
        await self.authorize_read(service, request)
        return await self.core.reading.read_content(request)
