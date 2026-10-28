"""Actor experiences and interpretations, separate from user memory."""

from ..contracts import Fault, strict_json


class ActorMemorySkill:
    domain = "memory"
    operations = {
        "actor-memory.recall": "actor_memory_recall",
        "actor-memory.remember": "actor_memory_remember",
    }

    def validate_config(self, config):
        if any(config.values()):
            raise Fault("invalid_input")

    def public_options(self, config):
        return {}

    def availability(self, registry, actor, config):
        ready = registry.core.actor_memory.available
        return dict(
            state="available" if ready else "not_configured",
            can_execute=ready,
            reason_code="configured_authority_network_unverified"
            if ready
            else "actor_memory_authority_required",
        )

    def context(self, actions, turn):
        return dict(
            owner="this_actor",
            user_memory="separate",
            evidence="actual_owner_sources",
            current_audience="inherited",
        )

    def instructions(self):
        return (
            "actor_memory_recall retrieves this BOT actor's actual sourced experiences and revisable "
            "interpretations. It never retrieves experiences as a user's own history. Preserve digital "
            "actions, observations, reports and fictional material as distinct provenance. Reading an "
            "interval is not completing or understanding a whole work. With actor_memory_remember, "
            "derive a concise understanding, interest or reflection only from actual episode references "
            "returned by recall. These interpretations are not new physical experiences. Use actual "
            "receipts; an unknown command must be reconciled rather than submitted under another key."
        )

    def tools(self, actions, turn, definition):
        return self._tools(actions, lambda permission: actions.core._turn_allows(turn, permission))

    def activity_tools(self, actions, context, definition):
        activity, _ = actions.core.runtime_execution.actions.check(context)
        if activity["scope"] is not None:
            return []
        return self._tools(
            actions,
            lambda permission: actions.core.runtime_execution.actions.allows(context, permission),
        )

    def _tools(self, actions, allows):
        tools = []
        schema = actions.core.contracts.schemas["actor-self-memory"]
        if allows("memory.read"):
            query = actions._expand(schema["$defs"]["query"], schema)["properties"]
            tools.append(
                dict(
                    type="function",
                    function=dict(
                        name="actor_memory_recall",
                        description="Recall this actor's own sourced experiences and interpretations visible in the current real audience; user memories remain separate.",
                        parameters=dict(
                            type="object",
                            properties={key: query[key] for key in ("text", "kinds", "limit")},
                            required=["text"],
                            additionalProperties=False,
                        ),
                    ),
                )
            )
        if allows("memory.write"):
            derived = actions._expand(schema["$defs"]["append_derived"], schema)["properties"]
            keys = ("kind", "text", "episode_refs", "importance", "retention")
            tools.append(
                dict(
                    type="function",
                    function=dict(
                        name="actor_memory_remember",
                        description="Save this actor's revisable interpretation based only on real episode references returned by actor_memory_recall. Does not create a new experience or write user memory.",
                        parameters=dict(
                            type="object",
                            properties={key: derived[key] for key in keys},
                            required=list(keys),
                            additionalProperties=False,
                        ),
                    ),
                )
            )
        return tools

    async def activity_execute(self, actions, context, tool, *, model_slot_held=False):
        value = strict_json(tool["function"]["arguments"])
        if tool["function"]["name"] == "actor_memory_recall":
            return await actions.core.actor_memory.activity_recall(context, value)
        return await actions.core.actor_memory.activity_derive(context, tool["id"], value)

    async def execute(self, actions, turn_id, tool, *, model_slot_held=False):
        value = strict_json(tool["function"]["arguments"])
        if tool["function"]["name"] == "actor_memory_recall":
            return await actions.core.actor_memory.recall(turn_id, value)
        return await actions.core.actor_memory.derive(turn_id, tool["id"], value)
