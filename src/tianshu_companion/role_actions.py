"""One actor's native tools. Transport identity and source ownership stay server-side."""

import base64
import copy

from .clients import command, query
from .contracts import Fault, canonical, digest, strict_json
from .life_work import visible


OPERATIONS = (
    "concern.save",
    "concern.close",
    "activity.save",
    "activity.pause",
    "activity.resume",
    "activity.cancel",
    "affect.feedback",
    "outfit.select",
    "image.request",
    "album.attach",
    "writing.create",
    "chapter.add",
    "chapter.generate",
    "chapter.revise",
    "content.acquire",
    "reading.open",
    "reading.read",
    "reading.pause",
    "reading.resume",
    "reading.close",
)
TRUSTED = {"scope", "sources", "query"}


class RoleActions:
    def __init__(self, core):
        self.core = core
        from .skills import SkillRegistry

        self.registry = SkillRegistry(self)

    def _expand(self, shape, schema):
        if isinstance(shape, list):
            return [self._expand(item, schema) for item in shape]
        if not isinstance(shape, dict):
            return shape
        if "$ref" in shape:
            reference = shape["$ref"]
            uri, _, pointer = reference.partition("#")
            target = (
                next(
                    (item for item in self.core.contracts.schemas.values() if item["$id"] == uri),
                    None,
                )
                if uri
                else schema
            )
            if target is None:
                raise Fault("dependency_unavailable")
            for name in pointer.strip("/").split("/"):
                target = target[name.replace("~1", "/").replace("~0", "~")]
            return self._expand(
                target,
                schema
                if not uri
                else next(
                    item for item in self.core.contracts.schemas.values() if item["$id"] == uri
                ),
            )
        value = {key: self._expand(item, schema) for key, item in shape.items()}
        if value.get("type") == "object":
            for field in TRUSTED - (
                {"sources"}
                if set(value.get("properties", {})) >= {"owner", "object_id", "version", "sha256"}
                else set()
            ):
                value.get("properties", {}).pop(field, None)
            if "required" in value:
                value["required"] = [
                    name
                    for name in value["required"]
                    if name not in TRUSTED
                    or (name == "sources" and "sha256" in value.get("properties", {}))
                ]
        return value

    def domain_tools(self, turn):
        schema = self.core.contracts.schemas["life-runtime"]
        tools = []
        if self.core._turn_allows(turn, "dialogue"):
            for operation in OPERATIONS:
                if operation == "image.request":
                    continue
                value = schema["$defs"][operation.replace(".", "_") + "_value"]
                parameters = dict(
                    type="object",
                    properties=dict(
                        expected_version={"type": "integer", "minimum": 0},
                        value=self._expand(value, schema),
                    ),
                    required=["expected_version", "value"],
                    additionalProperties=False,
                )
                tools.append(
                    dict(
                        type="function",
                        function=dict(
                            name="life_" + operation.replace(".", "_"),
                            description="Execute "
                            + operation
                            + " for this actor. Read current version before edits; 0 creates. Actual receipts determine success.",
                            parameters=parameters,
                        ),
                    )
                )
            tools.append(
                dict(
                    type="function",
                    function=dict(
                        name="life_send_content",
                        description="Send existing authorized original media to this conversation through the shared delivery queue. Return its actual queue/adapter receipt; queued is not sent.",
                        parameters=dict(
                            type="object",
                            properties=dict(
                                text=dict(type="string", maxLength=4000),
                                content_refs=dict(
                                    type="array",
                                    minItems=1,
                                    maxItems=4,
                                    items=self._expand(schema["$defs"]["content_ref"], schema),
                                ),
                            ),
                            required=["text", "content_refs"],
                            additionalProperties=False,
                        ),
                    ),
                )
            )
            tools.append(
                dict(
                    type="function",
                    function=dict(
                        name="life_read",
                        description="Read exact current actor-owned concern/activity/album/writing/chapter/reading/image and versions. A chapter read returns its original draft, never a summary.",
                        parameters=dict(
                            type="object",
                            properties=dict(
                                resource={
                                    "enum": [
                                        "concern",
                                        "activity",
                                        "album",
                                        "writing",
                                        "chapter",
                                        "reading",
                                        "image",
                                    ]
                                },
                                id={"type": "string"},
                            ),
                            required=["resource", "id"],
                            additionalProperties=False,
                        ),
                    ),
                )
            )
        if self.core._turn_allows(turn, "memory.write"):
            tools.append(
                dict(
                    type="function",
                    function=dict(
                        name="memory_propose",
                        description="Propose a sourced current user memory, correction or forgetting. Use real latest user input only. Correct/forget must target a recalled record/version. Report actual committed/corrected/tombstoned receipt, never claim rejected/unknown succeeded.",
                        parameters=dict(
                            type="object",
                            properties=dict(
                                kind={"enum": ["upsert", "correct", "forget", "no_op"]},
                                target={"type": "object"},
                                units={
                                    "type": "array",
                                    "maxItems": 32,
                                    "items": {"type": "object"},
                                },
                                intent=dict(
                                    type="object",
                                    properties=dict(
                                        source_quote={"type": "string", "minLength": 1},
                                        unambiguous_target={"type": "boolean"},
                                    ),
                                    required=["source_quote", "unambiguous_target"],
                                    additionalProperties=False,
                                ),
                            ),
                            required=["kind", "target", "units", "intent"],
                            additionalProperties=False,
                        ),
                    ),
                )
            )
        return tools

    def tools(self, turn):
        return self.registry.tools(turn)

    async def execute(self, turn_id, tool, *, model_slot_held=False):
        turn = self.core.store.get("turns", turn_id)
        await self.core._preflight(turn)
        return await self.registry.execute(turn, tool, model_slot_held=model_slot_held)

    async def execute_domain(self, turn_id, tool, *, model_slot_held=False):
        turn = self.core.store.get("turns", turn_id)
        name = tool["function"]["name"]
        supplied = strict_json(tool["function"]["arguments"])
        if not isinstance(supplied, dict):
            raise Fault("invalid_input")
        if name == "life_read":
            return await self.read(turn, supplied)
        if name == "life_send_content":
            if not self.core._turn_allows(turn, "dialogue") or set(supplied) != {
                "text",
                "content_refs",
            }:
                raise Fault("forbidden")
            for reference in supplied["content_refs"]:
                self.verify_reference(turn, reference)
            await self.core.stream_segment(turn_id, supplied["text"], supplied["content_refs"])
            fresh = self.core.store.get("turns", turn_id)
            return dict(
                state=fresh["expression_receipt"]["state"], expression_id=fresh["expression_id"]
            ), []
        if name == "memory_propose":
            return await self.memory_propose(turn, tool["id"], supplied), []
        operation = next(
            (item for item in OPERATIONS if name == "life_" + item.replace(".", "_")), None
        )
        if (
            operation is None
            or not self.core._turn_allows(turn, "dialogue")
            or set(supplied) != {"expected_version", "value"}
        ):
            raise Fault("forbidden")
        value = copy.deepcopy(supplied["value"])
        if not isinstance(value, dict) or TRUSTED & set(value):
            raise Fault("invalid_input")
        actor_id = turn["scope"]["actor_id"]
        schema = self.core.contracts.schemas["life-runtime"]["$defs"][
            operation.replace(".", "_") + "_value"
        ]
        properties = schema["properties"]
        if "scope" in properties:
            value["scope"] = turn["scope"]
        if "query" in properties:
            value["query"] = query(turn["origin"])
        if "sources" in properties:
            value["sources"] = self.core.life.concerns.captured_sources(turn)
        for fragment in value.get("fragments", []):
            if "sources" in fragment:
                raise Fault("invalid_input")
            fragment["sources"] = self.core.life.concerns.captured_sources(turn)
        if operation == "affect.feedback":
            value["event_id"] = "affect:" + digest([turn_id, tool["id"]])
        for reference in value.get("result_refs", []):
            self.verify_reference(turn, reference)
        if operation == "chapter.add":
            work = self.core.writing._work(value["work_id"])
            if not visible(work, turn["scope"]):
                raise Fault("not_found")
        if operation.startswith("chapter.") and operation != "chapter.add":
            chapter = self.core.life_runtime._chapter(actor_id, value["id"])
            if not visible(self.core.writing._work(chapter["work_id"]), turn["scope"]):
                raise Fault("not_found")
        request = dict(
            schema_version=2,
            request_id="actor-action:" + digest([turn_id, tool["id"]]),
            actor_id=actor_id,
            operation=operation,
            expected_version=supplied["expected_version"],
            value=value,
        )
        self.core.contracts.check("life-runtime#manage_request", request)
        if operation in {
            "content.acquire",
            "reading.open",
            "reading.read",
            "reading.pause",
            "reading.resume",
            "reading.close",
        }:
            result = await self.core.reading.action(
                turn["service"], request, internal_scope=turn["scope"], return_content=True
            )
        else:
            prepared = await self.core.life_runtime.prepare_action(
                actor_id,
                operation,
                value,
                supplied["expected_version"],
                model_slot_held=model_slot_held,
            )

            def execute():
                self.registry.check_selected(self.core.store.get("turns", turn_id), name)
                result = self.core.life_runtime._execute(
                    actor_id,
                    operation,
                    value,
                    supplied["expected_version"],
                    request["request_id"],
                    actor_id,
                    prepared=prepared,
                )
                if operation == "writing.create":
                    work = self.core.store.get("write_works", result["id"])
                    work["scope"] = turn["scope"]
                    self.core.store.put("write_works", work)
                return result

            result = self.core.life.concerns.operation("actor:" + actor_id, request, execute)
            if operation == "image.request":
                key = "image-notice:" + digest(result["id"])
                if not self.core.store.get("metadata", key):
                    self.core.store.put(
                        "metadata",
                        dict(
                            id=key,
                            state="waiting",
                            job_id=result["id"],
                            turn_id=turn_id,
                            actor_id=actor_id,
                            scope=turn["scope"],
                            origin=turn["origin"],
                            channel=turn["bundle"]["collection_key"]["channel"],
                        ),
                    )
        content = result.pop("actual_content", None)
        attachments = self.content_messages(content) if content else []
        if content:
            content = {key: value for key, value in content.items() if key != "representations"}
        return dict(result, actual_content=content) if content else result, attachments

    @staticmethod
    def content_messages(content):
        values = []
        for representation in content["representations"]:
            kind = representation["kind"]
            if kind in {"image", "video_frame"}:
                values.append(
                    dict(
                        type="text",
                        text="Actual original representation; at_seconds="
                        + str(representation["at_seconds"]),
                    )
                )
                values.append(
                    dict(
                        type="image_url",
                        image_url=dict(
                            url="data:"
                            + representation["media_type"]
                            + ";base64,"
                            + representation["data_base64"],
                            detail="auto",
                        ),
                    )
                )
            elif kind == "audio_clip":
                values.append(
                    dict(
                        type="input_audio",
                        input_audio=dict(data=representation["data_base64"], format="wav"),
                    )
                )
        return [dict(role="user", content=values)] if values else []

    def verify_reference(self, turn, reference):
        self.core.reading._reference(turn["scope"]["actor_id"], turn["scope"], reference)

    async def read(self, turn, value):
        if set(value) != {"resource", "id"} or not self.core._turn_allows(turn, "dialogue"):
            raise Fault("forbidden")
        actor, scope = turn["scope"]["actor_id"], turn["scope"]
        resource, key = value["resource"], value["id"]
        attachments = []
        if resource == "concern":
            item = self.core.life.concerns.get(actor, key)
            item["fragments"] = [
                fragment
                for fragment in item["fragments"]
                if fragment.get("valid", True)
                and (fragment["expires_at"] is None or fragment["expires_at"] > self.core.clock())
            ]
        elif resource == "activity":
            item = self.core.life.activities.get(actor, key)
        elif resource == "writing":
            item = self.core.writing._work(key)
            if item["actor_id"] != actor:
                raise Fault("not_found")
        elif resource == "chapter":
            chapter = self.core.life_runtime._chapter(actor, key)
            work = self.core.writing._work(chapter["work_id"])
            if not visible(work, scope):
                raise Fault("not_found")
            revision = (
                self.core.store.get("write_revisions", chapter["current_revision"])
                if chapter["current_revision"]
                else None
            )
            item = dict(chapter, content=revision["content"] if revision else None)
            if revision:
                row = self.core.store.db.execute(
                    "SELECT body FROM life_content_refs WHERE json_extract(body,'$.content_ref.object_id')=? LIMIT 1",
                    (revision["id"],),
                ).fetchone()
                item["content_ref"] = (
                    __import__("json").loads(row[0])["content_ref"] if row else None
                )
        elif resource == "reading":
            item = self.core.reading.get(actor, key, scope)
        elif resource in {"image", "album"}:
            album = self.core.store.get("life_album", key) if resource == "album" else None
            data, media = self.core.life.album.read(
                actor, album["media_id"] if album else key, scope
            )
            if len(data) > 2_000_000:
                raise Fault("budget_exceeded")
            item = dict(
                media_id=media["id"], content_ref=media["content_ref"], actual_bytes=len(data)
            )
            attachments = [
                dict(
                    role="user",
                    content=[
                        dict(type="text", text="Authorized original image " + media["id"]),
                        dict(
                            type="image_url",
                            image_url=dict(
                                url="data:"
                                + media["media_type"]
                                + ";base64,"
                                + base64.b64encode(data).decode(),
                                detail="auto",
                            ),
                        ),
                    ],
                )
            ]
        else:
            raise Fault("invalid_input")
        if not visible(item, scope):
            raise Fault("not_found")
        if len(canonical(item).encode()) > 200000:
            raise Fault("budget_exceeded")
        return item, attachments

    async def memory_propose(self, turn, tool_id, value):
        if not self.core._turn_allows(turn, "memory.write") or set(value) != {
            "kind",
            "target",
            "units",
            "intent",
        }:
            raise Fault("forbidden")
        latest = turn["bundle"]["messages"][-1]
        if (
            latest["person_id"] != turn["scope"]["person_id"]
            or self.core._event_reality(turn) != "real"
        ):
            raise Fault("forbidden")
        sources = [latest["source"]]
        intent = value["intent"]
        latest_text = "\n".join(part["text"] for part in latest["parts"] if part["kind"] == "text")
        if (
            not isinstance(intent, dict)
            or set(intent) != {"source_quote", "unambiguous_target"}
            or not isinstance(intent["source_quote"], str)
            or not intent["source_quote"]
            or intent["source_quote"] not in latest_text
            or (value["kind"] in {"correct", "forget"} and intent["unambiguous_target"] is not True)
        ):
            raise Fault("invalid_input")
        units = copy.deepcopy(value["units"])
        for unit in units:
            if "sources" in unit or unit.get("reality") != "real":
                raise Fault("invalid_input")
            unit["sources"] = sources
        target = value["target"]
        if value["kind"] in {"correct", "forget"}:
            recalled = next(
                (
                    unit
                    for unit in turn["preparation"]["selected_units"]
                    if unit["record_id"] == target.get("record_id")
                ),
                None,
            )
            if recalled is None or recalled.get(
                "version", recalled.get("record_version", recalled.get("revision"))
            ) != target.get("expected_version"):
                raise Fault("not_found")
        operation_id = "memory-op:" + digest([turn["id"], tool_id])
        key = "memory-action:" + digest(operation_id)
        prior = self.core.store.get("metadata", key)
        if prior:
            if prior.get("receipt"):
                return prior["receipt"]
            receipt = await self.core.memory.receipt(turn["origin"], turn["scope"], operation_id)
            if receipt is None:
                raise Fault("result_unknown", unknown=True)
            prior.update(state=receipt["state"], receipt=receipt)
            self.core.store.put("metadata", prior)
            return receipt
        proposal = dict(
            command=command(turn["origin"], operation_id, self.core.clock()),
            scope=turn["scope"],
            batch_ref=None,
            item_id="item:" + digest([turn["id"], tool_id]),
            kind=value["kind"],
            target=target,
            units=units,
            evidence_refs=sources if value["kind"] != "no_op" else [],
            proof_ref=None,
        )
        if value["kind"] in {"correct", "forget"}:
            physical = self.core.store.get("physicals", digest(latest["message_key"]))
            issuer = self.core.origins.issuers.get(turn["service"])
            if not physical or not issuer:
                raise Fault("dependency_unavailable")
            issue = dict(
                schema_version=1,
                request_id="proof:" + digest(operation_id),
                origin=turn["origin"],
                source=physical["request"],
                proposal=proposal,
            )
            self.core.contracts.check("memory-context#issue_request", issue)
            proof = await issuer[1].call(
                "/internal/v1/memory-context/proof/issue", issue, uncertain_write=True
            )
            self.core.contracts.check("memory-context#issue_response", proof)
            if (
                not isinstance(proof, dict)
                or set(proof)
                != {"schema_version", "request_id", "proof_ref", "operation_digest", "expires_at"}
                or proof["schema_version"] != 1
                or proof["request_id"] != "proof:" + digest(operation_id)
                or proof["operation_digest"]
                != digest(
                    {
                        key: item
                        for key, item in proposal.items()
                        if key not in {"query", "command", "proof_ref"}
                    }
                )
                or not isinstance(proof.get("proof_ref"), str)
                or __import__("tianshu_companion.clients", fromlist=["epoch"]).epoch(
                    proof["expires_at"]
                )
                <= self.core.clock()
            ):
                raise Fault("invalid_input")
            proposal["proof_ref"] = proof["proof_ref"]
        self.core.contracts.check("memory-context#proposal_request", proposal)
        key = "memory-action:" + digest(operation_id)
        old = self.core.store.get("metadata", key)
        if old and old.get("receipt"):
            return old["receipt"]
        self.core.store.put(
            "metadata",
            dict(
                id=key,
                operation_id=operation_id,
                proposal=proposal,
                state="submitted",
                receipt=None,
            ),
        )
        receipt = await self.core.memory.propose(proposal)
        self.core.store.put(
            "metadata",
            dict(
                id=key,
                operation_id=operation_id,
                proposal=proposal,
                state=receipt["state"],
                receipt=receipt,
            ),
        )
        if receipt["state"] in {"committed", "corrected", "tombstoned"}:
            current = await self.core.memory.select(
                turn["origin"],
                turn["scope"],
                self.core._input_text(turn),
                dict(tokens=2048, bytes=8192),
            )
            with self.core.store.transaction():
                fresh = self.core.store.get("turns", turn["id"])
                fresh.update(
                    preparation=current,
                    scope_version=current["scope_version"],
                    context_checks=[
                        check
                        for check in fresh.get("context_checks", [])
                        if check["version_domain"] == "role-relationship/v1"
                    ],
                    profile_checks=[],
                    short_context=None,
                )
                self.core._save_turn(fresh)
                if target.get("record_id") and value["kind"] in {"correct", "forget"}:
                    self.core.life.concerns.invalidate(
                        "memory", target["record_id"], target["expected_version"]
                    )
        return receipt
