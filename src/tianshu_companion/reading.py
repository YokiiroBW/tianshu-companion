"""References and reading progress. Knowledge remains the authority for original bytes."""

import json
import copy
import base64
import hashlib

from .clients import epoch

from .contracts import Fault, digest, canonical
from .life_work import require_version, visible


class Reading:
    def __init__(self, core):
        self.core, self.store = core, core.store

    def principal(self, actor, scope, query, operation_ref):
        if scope is not None:
            if query is None:
                raise Fault("forbidden")
            return dict(kind="user", query=query, scope=scope)
        current = self.store.get("life_actors", actor)
        if not current or not current.get("life_enabled", True) or not self.actor_enabled(actor):
            raise Fault("forbidden")
        self.actor_operation(actor, operation_ref)
        return dict(
            kind="actor",
            request_id="content-read:" + digest([operation_ref, self.core.clock()]),
            actor_id=actor,
            operation_ref=operation_ref,
        )

    def actor_enabled(self, actor):
        role = getattr(self.core, "role_runtime", None)
        registered = role.get(actor) if role else None
        return actor in self.core.roles and (registered is None or registered["enabled"])

    def actor_operation(self, actor, operation_ref):
        item = self.store.get("life_activities", operation_ref) or self.store.get(
            "life_reading", operation_ref
        )
        current = self.store.get("life_actors", actor)
        if (
            not item
            or item["actor_id"] != actor
            or not current
            or not self.actor_enabled(actor)
            or not current.get("life_enabled", True)
            or item["state"] in {"cancelled", "closed"}
        ):
            raise Fault("forbidden")
        if item.get("runtime_epoch", current.get("life_runtime_epoch", 0)) != current.get(
            "life_runtime_epoch", 0
        ):
            raise Fault("scope_changed")

    async def user_scope(self, actor, query):
        # An authorized actor-life view does not grant a Knowledge original to its reader.
        context = await self.core.origins.resolve("platform", query, self.core.clock())
        allowed = context["allowed_scope"]
        person = await self.core.memory.resolve_identity(
            query["origin"], context["verified_account"]
        )
        if (
            not person
            or allowed["actor_id"] != actor
            or not allowed.get("audience")
            or not allowed.get("conversation_id")
        ):
            raise Fault("forbidden")
        return dict(
            actor_id=actor,
            person_id=person,
            audience=allowed["audience"],
            conversation_id=allowed["conversation_id"],
        )

    def get(self, actor, reading_id, scope):
        item = self.store.get("life_reading", reading_id)
        if not item or item["actor_id"] != actor or not visible(item, scope):
            raise Fault("not_found")
        return item

    def _reference(self, actor, scope, reference):
        if reference["owner"] == "companion":
            media = self.store.get("image_media", reference["object_id"])
            if (
                media
                and media["actor_id"] == actor
                and visible(media, scope)
                and media["content_ref"] == reference
            ):
                return media
        item = self.store.get("life_content_refs", "content:" + digest(reference))
        if (
            not item
            or item["actor_id"] != actor
            or not visible(item, scope)
            or item["content_ref"] != reference
        ):
            raise Fault("not_found")
        return item

    async def owner(self, operation, request, *, uncertain=False):
        self.core.contracts.check("knowledge-content#" + operation + "_request", request)
        if self.core.knowledge_client is None:
            raise Fault("dependency_unavailable")
        response = await self.core.knowledge_client.call(
            "/internal/v1/knowledge/content/" + operation, request, uncertain_write=uncertain
        )
        self.core.contracts.check("knowledge-content#" + operation + "_response", response)
        principal = request["principal"]
        request_id = (
            principal["request_id"]
            if principal["kind"] == "actor"
            else principal["query"]["request_id"]
        )
        if (
            response["request_id"] != request_id
            or epoch(response["available_until"]) <= self.core.clock()
        ):
            raise Fault("scope_changed")
        if operation == "read" and response["content_ref"] != request["content_ref"]:
            raise Fault("version_conflict")
        if operation == "read":
            for representation in response["representations"]:
                data = base64.b64decode(representation["data_base64"], validate=True)
                if (
                    hashlib.sha256(data).hexdigest() != representation["sha256"]
                    or representation["source_sha256"] != request["content_ref"]["sha256"]
                ):
                    raise Fault("invalid_input")
        return response

    async def action(
        self, service, request, *, internal_scope=None, operation_ref=None, return_content=False
    ):
        actor, operation, value, expected = (
            request["actor_id"],
            request["operation"],
            request["value"],
            request["expected_version"],
        )
        prepared = None
        scope = value.get("scope", internal_scope)
        if operation in {"reading.read", "reading.pause", "reading.resume", "reading.close"}:
            existing = self.store.get("life_reading", value["id"])
            if existing is None or existing["actor_id"] != actor:
                raise Fault("not_found")
            scope = existing["scope"]
            if internal_scope is not None and not visible(existing, internal_scope):
                raise Fault("not_found")
        if service == "platform" and operation in {
            "content.acquire",
            "reading.open",
            "reading.read",
        }:
            await self.core.life_runtime.authorize_read(
                service, dict(actor_id=actor, query=value["query"], scope=scope)
            )
        replay = self.core.life.concerns.replay(service, request)
        if replay:
            return replay
        if operation == "content.acquire":
            if scope is None and operation_ref is None:
                # A management acquisition is itself a persisted real acquisition activity.
                operation_ref = "acquire-activity:" + digest(request["request_id"])
                if not self.store.get("life_activities", operation_ref):
                    self.core.life.activities.save(
                        actor,
                        dict(
                            id=operation_ref,
                            title="取得阅读原件",
                            state="planned",
                            checkpoint=dict(
                                step=0, position=0, unit="step", note="等待原件 owner 回执"
                            ),
                            next_due_at=None,
                            resume_condition=None,
                            sources=[],
                            result_refs=[],
                            scope=None,
                        ),
                        expected=0,
                    )
            principal = self.principal(
                actor, scope, copy.deepcopy(value["query"]), operation_ref or request["request_id"]
            )
            # Stable request ID is reused by the owner's acquisition receipt.
            if principal["kind"] == "actor":
                principal["request_id"] = request["request_id"]
            else:
                principal["query"]["request_id"] = request["request_id"]
            prepared = await self.owner(
                "acquire",
                dict(principal=principal, source=value["source"], purpose=value["purpose"]),
                uncertain=True,
            )
        elif operation == "reading.open":
            require_version(self.store.get("life_reading", value["id"]), expected)
            self._reference(actor, scope, value["content_ref"])
            participants = value["participants"]
            if (
                value["mode"] == "solo"
                and participants
                or value["mode"] == "together"
                and (scope is None or participants != [scope["person_id"]])
            ):
                # Additional people join through their own existing source authorization.
                raise Fault("forbidden")
        elif operation == "reading.read":
            require_version(existing, expected)
            if existing["state"] not in {"open", "reading"}:
                raise Fault("invalid_input")
            prepared = await self._read(
                actor,
                scope,
                existing["content_ref"],
                value["range"],
                value["query"],
                existing["id"],
            )

        def execute():
            now = self.core.clock()
            if operation == "content.acquire":
                reference = prepared["content_ref"]
                item = dict(
                    id="content:" + digest(reference),
                    actor_id=actor,
                    conversation_id=actor,
                    scope=scope,
                    version=1,
                    state="available",
                    content_ref=reference,
                    source=prepared["source"],
                    query=value["query"] if scope is not None else None,
                    operation_ref=operation_ref,
                    acquired_at=now,
                )
                self.store.put("life_content_refs", item)
            elif operation == "reading.open":
                require_version(self.store.get("life_reading", value["id"]), expected)
                coverage = value["content_ref"]["coverage"]
                item = dict(
                    value,
                    actor_id=actor,
                    conversation_id=actor,
                    version=expected + 1,
                    state="open",
                    coverage=[],
                    position=dict(
                        unit=coverage["unit"], start=coverage["start"], end=coverage["start"]
                    ),
                    updated_at=now,
                    runtime_epoch=self.store.get("life_actors", actor).get("life_runtime_epoch", 0),
                )
                self.store.put("life_reading", item)
            elif operation == "reading.read":
                item = self.get(actor, value["id"], scope)
                require_version(item, expected)
                self._advance(item, prepared)
            else:
                item = self.get(actor, value["id"], scope)
                require_version(item, expected)
                state = {
                    "reading.pause": "paused",
                    "reading.resume": "reading",
                    "reading.close": "closed",
                }.get(operation)
                if state is None or item["state"] in {"closed", "completed"}:
                    raise Fault("invalid_input")
                item.update(state=state, version=item["version"] + 1, updated_at=now)
                self.store.put("life_reading", item)
            result = dict(
                id=item["id"],
                version=item["version"],
                state=item["state"],
                operation_ref=request["request_id"],
            )
            if operation == "content.acquire":
                result["content_ref"] = prepared["content_ref"]
            return result

        result = self.core.life.concerns.operation(service, request, execute)
        if return_content and operation == "content.acquire":
            return dict(result, content_ref=prepared["content_ref"], source=prepared["source"])
        if return_content and prepared is not None and operation == "reading.read":
            return dict(
                result,
                actual_content={
                    field: prepared[field]
                    for field in ("text", "coverage", "complete", "representations", "gaps")
                },
            )
        return result

    async def _read(self, actor, scope, reference, range_, query, operation_ref, *, browser=False):
        stored = self._reference(actor, scope, reference)
        if reference["owner"] == "memory":
            if browser and scope is None:
                principal = dict(
                    kind="user", query=query, scope=await self.user_scope(actor, query)
                )
            else:
                principal = self.principal(
                    actor, scope, query or stored.get("query"), operation_ref
                )
            return await self.owner(
                "read",
                dict(
                    principal=principal, content_ref=reference, range=range_, budget_bytes=1000000
                ),
            )
        if reference["owner"] == "companion" and reference["kind"] == "image":
            data, media = self.core.life.album.read(actor, reference["object_id"], scope)
            if range_ != dict(unit="bytes", start=0, end=len(data)):
                raise Fault("invalid_input")
            import base64

            return dict(
                text=None,
                media_url=None,
                content_ref=reference,
                coverage=dict(unit="bytes", start=0, end=len(data)),
                complete=True,
                representations=[
                    dict(
                        kind="image",
                        media_type=media["media_type"],
                        sha256=media["sha256"],
                        source_sha256=media["sha256"],
                        data_base64=base64.b64encode(data).decode(),
                        at_seconds=None,
                    )
                ],
                gaps=[],
            )
        if reference["owner"] == "companion" and reference["kind"] == "text":
            revision = self.store.get("write_revisions", reference["object_id"])
            if not revision:
                raise Fault("not_found")
            self.core.life_runtime._chapter(actor, revision["chapter_id"])
            if range_["unit"] != "characters":
                raise Fault("invalid_input")
            start, end = range_["start"], range_["end"]
            if (
                type(start) is not int
                or type(end) is not int
                or not 0 <= start <= end <= len(revision["content"])
            ):
                raise Fault("invalid_input")
            return dict(
                text=revision["content"][start:end],
                media_url=None,
                content_ref=reference,
                coverage=range_,
                complete=True,
                representations=[],
                gaps=[],
            )
        raise Fault("dependency_unavailable")

    def _advance(self, item, result):
        actual = result["coverage"]
        coverage = item["content_ref"]["coverage"]
        if (
            actual["unit"] != coverage["unit"]
            or not 0 <= actual["start"] <= actual["end"]
            or (coverage["total"] is not None and actual["end"] > coverage["total"])
        ):
            raise Fault("invalid_input")
        # A video coarse interval does not assert all timestamps or audio were read.
        if not result.get("gaps"):
            intervals = sorted(item["coverage"] + [actual], key=lambda interval: interval["start"])
            merged = []
            for interval in intervals:
                if merged and interval["start"] <= merged[-1]["end"]:
                    merged[-1]["end"] = max(merged[-1]["end"], interval["end"])
                else:
                    merged.append(dict(interval))
            if len(merged) > 64:
                raise Fault("budget_exceeded")
            item["coverage"] = merged
        else:
            item.setdefault("sample_reads", []).append(
                dict(coverage=actual, gaps=result.get("gaps", []))
            )
            item["sample_reads"] = item["sample_reads"][-64:]
        complete = (
            coverage["total"] is not None
            and len(item["coverage"]) == 1
            and item["coverage"][0]["start"] <= coverage["start"]
            and item["coverage"][0]["end"] >= coverage["total"]
        )
        item.update(
            position=actual,
            version=item["version"] + 1,
            state="completed" if complete else "reading",
            updated_at=self.core.clock(),
        )
        self.store.put("life_reading", item)
        actor = self.store.get("life_actors", item["actor_id"])
        self.core.life._event(
            dict(
                id="reading-event:" + digest([item["id"], item["version"]]),
                world_id=actor["world_id"],
                participants=[item["actor_id"]],
                visible_to=[],
                summary="阅读原件的实际区间："
                + canonical(actual)
                + ("；仍有未覆盖内容" if result.get("gaps") else ""),
                occurred_at=self.core.clock(),
                fictional=True,
                kind="reading_progress",
                scope=item["scope"],
                content_refs=[item["content_ref"]],
                coverage=actual,
                complete=result["complete"],
                gaps=result.get("gaps", []),
            )
        )

    async def read_content(self, request):
        actor, scope, reading_id = request["actor_id"], request["scope"], request["reading_id"]
        session = self.get(actor, reading_id, scope) if reading_id else None
        if session and session["content_ref"] != request["content_ref"]:
            raise Fault("invalid_input")
        result = await self._read(
            actor,
            scope,
            request["content_ref"],
            request["range"],
            request["query"],
            reading_id or request["query"]["request_id"],
            browser=True,
        )
        response = dict(
            schema_version=2,
            request_id=request["query"]["request_id"],
            actor_id=actor,
            reading_id=reading_id,
            content_ref=request["content_ref"],
            **{
                field: result[field]
                for field in (
                    "text",
                    "media_url",
                    "coverage",
                    "complete",
                    "representations",
                    "gaps",
                )
            },
        )
        self.core.contracts.check("life-runtime#content_read_response", response)
        if len(canonical(response).encode()) > 2_000_000:
            raise Fault("budget_exceeded")
        if session:
            with self.store.transaction():
                fresh = self.get(actor, reading_id, scope)
                require_version(fresh, session["version"])
                self._advance(fresh, result)
        return response

    async def original(
        self, actor, scope, reference, query=None, *, actor_owned=False, allow_unregistered=False
    ):
        self.core.contracts.check("life-runtime#content_ref", reference)
        try:
            stored = self._reference(actor, scope, reference)
        except Fault:
            if not allow_unregistered or scope is None or reference["owner"] != "memory":
                raise
            stored = dict(scope=scope, id=reference["object_id"])
        if reference["owner"] == "companion" and reference["kind"] == "image":
            data, media = self.core.life.album.read(actor, reference["object_id"], scope)
            media_type = media["media_type"]
        elif reference["owner"] == "companion" and reference["kind"] == "text":
            revision = self.core.writing._get("revisions", reference["object_id"])
            data, media_type = revision["content"].encode("utf-8"), "text/plain"
        elif reference["owner"] == "memory":
            if stored.get("scope") is None and actor_owned:
                principal = self.principal(actor, None, None, stored["operation_ref"])
            elif query:
                principal = self.principal(actor, scope, query, stored["id"])
            else:
                raise Fault("forbidden")
            request = dict(principal=principal, content_ref=reference, range=None)
            self.core.contracts.check("knowledge-content#original_request", request)
            if self.core.knowledge_client is None:
                raise Fault("dependency_unavailable")
            data, headers = await self.core.knowledge_client.binary(
                "/internal/v1/knowledge/content/original", request
            )
            if headers.get("x-content-sha256") != reference["sha256"] or headers.get(
                "x-content-version"
            ) != str(reference["version"]):
                raise Fault("version_conflict")
            media_type = headers.get("content-type", "").split(";")[0].strip()
        else:
            raise Fault("dependency_unavailable")
        if hashlib.sha256(data).hexdigest() != reference["sha256"]:
            raise Fault("version_conflict")
        return data, media_type

    async def materialize(self, actor, scope, references, origin=None, *, actor_owned=False):
        from .clients import query

        records, total = [], 0
        for reference in references:
            data, media_type = await self.original(
                actor,
                scope,
                reference,
                query(origin) if origin else None,
                actor_owned=actor_owned
                or self._reference(actor, scope, reference).get("scope") is None,
            )
            if reference["kind"] == "text":
                # Verify the real source, without inventing a file delivery for a text citation.
                continue
            total += len(data)
            if total > 33554432 or len(records) >= 4:
                raise Fault("budget_exceeded")
            record = dict(
                content_ref=reference,
                media_type=media_type,
                encoding="base64",
                data=base64.b64encode(data).decode(),
                sha256=reference["sha256"],
            )
            self.core.contracts.check("bot-delivery#media_record", record)
            records.append(record)
        return records

    def page(self, actor, *, scope=None, after=None, limit=20, object_id=None):
        rows = self.store.db.execute(
            "SELECT body FROM life_reading WHERE conversation_id=? AND ((? IS NULL AND id>?) OR id=?) ORDER BY id LIMIT ?",
            (actor, object_id, after or "", object_id, limit + 1),
        ).fetchall()
        values = [json.loads(row[0]) for row in rows]
        fields = (
            "id",
            "version",
            "actor_id",
            "scope",
            "content_ref",
            "state",
            "mode",
            "participants",
            "coverage",
            "position",
            "updated_at",
        )
        return [
            {field: value[field] for field in fields}
            for value in values[:limit]
            if visible(value, scope)
        ], values[limit - 1]["id"] if len(values) > limit else None
