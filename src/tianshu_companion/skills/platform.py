"""The existing Platform actor bridge, shared by dialogue and actor activities."""

import copy
import json

import jsonschema

from ..clients import uid
from ..contracts import Fault, canonical, digest, strict_json

PATH = "/internal/v1/actor-capabilities/operate"


class PlatformSkill:
    domain = "life"
    operations = {
        "platform." + operation: "platform_" + operation
        for operation in ("discover", "invoke", "get", "reconcile", "cancel")
    }

    def validate_config(self, config):
        if any(config.values()):
            raise Fault("invalid_input")

    def public_options(self, config):
        return {}

    def availability(self, registry, actor, config):
        authority = getattr(registry.core.actor_memory, "authority", None)
        ready = bool(
            authority
            and authority.url
            and authority.token
            and "actor-capabilities" in registry.core.contracts.schemas
        )
        return dict(
            state="available" if ready else "not_configured",
            can_execute=ready,
            reason_code="configured_authority_network_unverified"
            if ready
            else "actor_capability_authority_required",
        )

    def context(self, actions, turn):
        return dict(
            actor_id=turn["scope"]["actor_id"],
            credentials="server_owned",
            receipts="original_platform_ledger",
        )

    def instructions(self):
        return "Discover real actor bindings first, then invoke only available capabilities using their exact input schema. Search snippets and catalog metadata are not original reads. adapter_required means that original media transport is not connected. Queued/running is not success. An unknown operation is never retried with another intent: use the original invocation get/reconcile. Credentials and account selection remain server-owned. Actor-only exploration uses read capabilities only; use a public topic abstraction for external search, never send private quotations, personal identifiers or raw group discussion as a search query."

    def tools(self, actions, turn, definition):
        return (
            self._tools(actions, definition) if actions.core._turn_allows(turn, "dialogue") else []
        )

    def activity_tools(self, actions, context, definition):
        activity, _ = actions.core.runtime_execution.actions.check(context)
        # Independent actor bindings cannot borrow a user's private source context.
        return (
            self._tools(actions, definition)
            if activity["scope"] is None
            and actions.core.runtime_execution.actions.allows(context, "dialogue")
            else []
        )

    def _tools(self, actions, definition):
        schema = actions.core.contracts.schemas["actor-capabilities"]
        tools = []
        for named in definition["operations"]:
            operation = named.split(".", 1)[1]
            fields = actions._expand(schema["$defs"][operation], schema)["properties"]
            keys = (
                ("binding", "arguments", "intent")
                if operation == "invoke"
                else ()
                if operation == "discover"
                else ("invocation_id",)
            )
            fields["intent"] = dict(
                type="string",
                minLength=1,
                maxLength=128,
                description="Stable logical intention within this execution. Reuse it for the same action; never change it after unknown.",
            )
            tools.append(
                dict(
                    type="function",
                    function=dict(
                        name="platform_" + operation,
                        description=self.instructions()
                        if operation == "discover"
                        else "Use the original Platform actor capability "
                        + operation
                        + " receipt.",
                        parameters=dict(
                            type="object",
                            properties={key: fields[key] for key in keys},
                            required=list(keys),
                            additionalProperties=False,
                        ),
                    ),
                )
            )
        return tools

    async def execute(self, actions, turn_id, tool, *, model_slot_held=False):
        async def validate():
            turn = actions.core.store.get("turns", turn_id)
            await actions.core._preflight(turn)
            actions.registry.check_selected(turn, tool["function"]["name"])

        turn = actions.core.store.get("turns", turn_id)
        return await self._execute(actions, turn["scope"]["actor_id"], turn_id, tool, validate), []

    async def activity_execute(self, actions, context, tool, *, model_slot_held=False):
        activity, _ = actions.core.runtime_execution.actions.check(context)

        async def validate():
            actions.core.runtime_execution.actions.check(context)
            actions.registry.check_activity_selected(context, tool["function"]["name"])

        return await self._execute(
            actions,
            context["actor_id"],
            context["operation_id"],
            tool,
            validate,
            read_only=bool(activity.get("actor_only")),
        ), []

    async def wire(self, core, body):
        core.contracts.check("actor-capabilities#" + body["operation"], body)
        observed = {}

        def error_response(status, value, error):
            core.contracts.check("actor-capabilities#error", value)
            if value["request_id"] != body["request_id"]:
                raise Fault("invalid_input", unknown=body["operation"] != "discover")
            observed.update(value)

        try:
            result = await core.actor_memory.authority.call(
                PATH,
                body,
                uncertain_write=body["operation"] in {"invoke", "cancel", "reconcile"},
                error_response=error_response,
            )
        except Fault as error:
            if observed.get("execution") == "unknown":
                raise Fault("result_unknown", unknown=True) from None
            if observed.get("execution") == "not_started":
                error.platform_not_started = True
            raise
        core.contracts.check(
            "actor-capabilities#"
            + ("discovery_result" if body["operation"] == "discover" else "receipt"),
            result,
        )
        if result["request_id"] != body["request_id"] or result["actor_id"] != body["actor_id"]:
            raise Fault("invalid_input", unknown=body["operation"] != "discover")
        if len(canonical(result).encode()) > 1_048_576:
            raise Fault("budget_exceeded")
        return result

    async def _execute(self, actions, actor, execution_id, tool, validate, *, read_only=False):
        core, store = actions.core, actions.core.store
        operation = tool["function"]["name"].removeprefix("platform_")
        value = strict_json(tool["function"]["arguments"])
        if not isinstance(value, dict):
            raise Fault("invalid_input")
        await validate()
        discovery_key = "platform-catalog:" + digest([actor, execution_id])
        if operation == "discover":
            if value:
                raise Fault("invalid_input")
            body = dict(
                schema_version=1,
                request_id=uid("capabilities"),
                actor_id=actor,
                operation="discover",
            )
            result = await self.wire(core, body)
            await validate()
            if read_only:
                result = copy.deepcopy(result)
                for capability in result["capabilities"]:
                    if capability["effect"] != "read":
                        capability.update(
                            availability="unavailable",
                            reason="activity_disclosure_context_required",
                        )
            store.put("metadata", dict(id=discovery_key, catalog=result))
            return result
        catalog = store.get("metadata", discovery_key)
        if catalog is None:
            raise Fault("invalid_input")
        version = catalog["catalog"]["role_version"]
        if operation == "invoke":
            if (
                set(value) != {"binding", "arguments", "intent"}
                or not isinstance(value["intent"], str)
                or not 1 <= len(value["intent"]) <= 128
            ):
                raise Fault("invalid_input")
            capability = next(
                (
                    item
                    for item in catalog["catalog"]["capabilities"]
                    if item["binding"] == value["binding"]
                ),
                None,
            )
            if capability is None or capability["availability"] != "ready":
                raise Fault("forbidden")
            try:
                jsonschema.Draft202012Validator(capability["input_schema"]).validate(
                    value["arguments"]
                )
            except (jsonschema.ValidationError, jsonschema.SchemaError):
                raise Fault("invalid_input") from None
            key = "platform-action:" + digest([actor, execution_id, value["intent"]])
            prior = store.get("metadata", key)
            if prior:
                if prior["input"] != value:
                    raise Fault("idempotency_conflict")
                if prior.get("receipt"):
                    return await self._lookup(core, prior, "get", version, validate)
                if prior["state"] != "not_started":
                    raise Fault("result_unknown", unknown=True)
            else:
                # A renamed intent cannot bypass a still-unknown identical action.
                semantic = digest([actor, execution_id, value["binding"], value["arguments"]])
                unknown = store.db.execute(
                    "SELECT 1 FROM metadata WHERE conversation_id=? AND (status='unknown' OR json_extract(body,'$.receipt.effect')='unknown') AND json_extract(body,'$.semantic')=? LIMIT 1",
                    ("platform:actor:" + actor, semantic),
                ).fetchone()
                if unknown:
                    raise Fault("result_unknown", unknown=True)
                prior = dict(
                    id=key,
                    conversation_id="platform:actor:" + actor,
                    state="not_started",
                    input=value,
                    semantic=semantic,
                    request=dict(
                        schema_version=1,
                        request_id=uid("actor-action"),
                        actor_id=actor,
                        role_version=version,
                        operation="invoke",
                        binding=value["binding"],
                        intent_key=digest([actor, execution_id, value["intent"]]),
                        arguments=value["arguments"],
                    ),
                )
            prior["state"] = "unknown"
            store.put("metadata", prior)
            try:
                receipt = await self.wire(core, prior["request"])
            except Fault as error:
                if getattr(error, "platform_not_started", False):
                    prior["state"] = "not_started"
                    store.put("metadata", prior)
                raise
            prior.update(state="known", receipt=receipt)
            store.put("metadata", prior)
            await validate()
            return receipt
        if operation not in {"get", "cancel", "reconcile"} or set(value) != {"invocation_id"}:
            raise Fault("invalid_input")
        row = store.db.execute(
            "SELECT body FROM metadata WHERE conversation_id=? AND json_extract(body,'$.receipt.invocation_id')=? ORDER BY id LIMIT 1",
            ("platform:actor:" + actor, value["invocation_id"]),
        ).fetchone()
        if row is None:
            raise Fault("forbidden")
        prior = json.loads(row[0])
        if read_only and not any(
            item["binding"] == prior["receipt"]["binding"] and item["effect"] == "read"
            for item in catalog["catalog"]["capabilities"]
        ):
            raise Fault("forbidden")
        return await self._lookup(core, prior, operation, version, validate)

    async def _lookup(self, core, prior, operation, version, validate):
        await validate()
        original = prior["receipt"]
        body = dict(
            schema_version=1,
            request_id=uid("actor-receipt"),
            actor_id=prior["request"]["actor_id"],
            role_version=version,
            operation=operation,
            invocation_id=original["invocation_id"],
        )
        result = await self.wire(core, body)
        if (
            any(result[key] != original[key] for key in ("invocation_id", "intent_key", "binding"))
            or result["revision"] < original["revision"]
        ):
            raise Fault("invalid_input", unknown=True)
        prior.update(receipt=copy.deepcopy(result), state="known")
        core.store.put("metadata", prior)
        await validate()
        return result
