"""Explicit actor activity execution context; never a synthetic dialogue turn."""

import copy

from jsonschema import Draft202012Validator

from .contracts import Fault, digest, strict_json
from .role_actions import OPERATIONS, TRUSTED


class ActivityActions:
    def __init__(self, core):
        self.core, self.store = core, core.store

    def context(self, activity, operation):
        marker = self.store.get("metadata", operation)
        return dict(
            kind="activity",
            actor_id=activity["actor_id"],
            activity_id=activity["id"],
            operation_id=operation,
            runtime_epoch=activity.get("runtime_epoch", 0),
            skill_pins=copy.deepcopy(marker.get("skill_pins", {})),
            skill_tool_owners=copy.deepcopy(marker.get("skill_tool_owners", {})),
        )

    def check(self, context):
        if context.get("kind") != "activity":
            raise Fault("forbidden")
        actor = context["actor_id"]
        self.core.reading.actor_operation(actor, context["activity_id"])
        activity = self.core.life.activities.get(actor, context["activity_id"])
        if activity.get("runtime_epoch", 0) != context["runtime_epoch"]:
            raise Fault("scope_changed")
        marker = self.store.get("metadata", context["operation_id"])
        if not marker or marker["activity_id"] != activity["id"]:
            raise Fault("scope_changed")
        return activity, marker

    def allows(self, context, permission):
        self.check(context)
        return self.core._role_allows(context["actor_id"], permission)

    async def execute(self, context, tool, *, model_slot_held=False):
        activity, marker = self.check(context)
        actor = context["actor_id"]
        name = tool["function"]["name"]
        operation = next((op for op in OPERATIONS if name == "life_" + op.replace(".", "_")), None)
        supplied = strict_json(tool["function"]["arguments"])
        if operation is None or not self.allows(context, "dialogue"):
            raise Fault("forbidden")
        if not isinstance(supplied, dict) or set(supplied) != {"expected_version", "value"}:
            raise Fault("invalid_input")
        value = copy.deepcopy(supplied["value"])
        if not isinstance(value, dict):
            raise Fault("invalid_input")
        for field in TRUSTED:
            value.pop(field, None)
        for fragment in value.get("fragments", []):
            fragment.pop("sources", None)
        parameters = self.core.role_actions.life_tools([operation])[0]["function"]["parameters"]
        if not Draft202012Validator(parameters).is_valid(dict(supplied, value=value)):
            raise Fault("invalid_input")
        schema = self.core.contracts.schemas["life-runtime"]["$defs"][
            operation.replace(".", "_") + "_value"
        ]
        sources = [dict(owner="companion", object_id=activity["id"], version=activity["version"])]
        for field, trusted in (("sources", sources), ("scope", activity["scope"]), ("query", None)):
            if field in schema["properties"]:
                if field == "query" and activity["scope"] is not None:
                    raise Fault("dependency_unavailable")
                value[field] = trusted
        for fragment in value.get("fragments", []):
            if "sources" in fragment:
                raise Fault("invalid_input")
            fragment["sources"] = sources
        if operation == "affect.feedback":
            value["event_id"] = "activity-affect:" + digest([context["operation_id"], tool["id"]])
        for ref in value.get("result_refs", []):
            self.core.reading._reference(actor, activity["scope"], ref)
        if (
            operation == "activity.save"
            and value.get("id") == activity["id"]
            and value.get("state") == "completed"
            and activity.get("autonomous_goal_id")
            and not (marker.get("reads") or marker.get("actual_work"))
        ):
            raise Fault("invalid_input")
        request = dict(
            schema_version=2,
            request_id="activity-action:" + digest([context["operation_id"], tool["id"]]),
            actor_id=actor,
            operation=operation,
            expected_version=supplied["expected_version"],
            value=value,
        )
        if not (operation.startswith("reading.") or operation == "content.acquire"):
            self.core.contracts.check("life-runtime#manage_request", request)
        marker["active_request"] = copy.deepcopy(request)
        self.store.put("metadata", marker)
        if operation.startswith("reading.") or operation == "content.acquire":
            result = await self.core.reading.action(
                "actor:" + actor,
                request,
                operation_ref=activity["id"],
                return_content=True,
            )
        else:
            prepared = await self.core.life_runtime.prepare_action(
                actor,
                operation,
                value,
                supplied["expected_version"],
                model_slot_held=model_slot_held,
                background=True,
            )
            self.check(context)
            self.core.skills.check_activity_selected(context, name)
            runtime_value = (
                dict(value, actor_only=True)
                if activity.get("actor_only") and operation in {"activity.save", "concern.save"}
                else value
            )
            result = self.core.life.concerns.operation(
                "actor:" + actor,
                request,
                lambda: self.core.life_runtime._execute(
                    actor,
                    operation,
                    runtime_value,
                    supplied["expected_version"],
                    request["request_id"],
                    actor,
                    prepared=prepared,
                ),
            )
        actual = result.pop("actual_content", None)
        if activity.get("actor_only"):
            table = (
                "life_content_refs"
                if operation == "content.acquire"
                else "life_reading"
                if operation == "reading.open"
                else "life_concerns"
                if operation == "concern.save"
                else "life_activities"
                if operation == "activity.save"
                else None
            )
            if table:
                record = self.store.get(table, result["id"])
                record["actor_only"] = True
                self.store.put(table, record)
        marker = self.store.get("metadata", context["operation_id"])
        reads = marker.get("reads", [])
        if actual:
            session = self.core.reading.get(actor, value["id"], activity["scope"])
            observed = dict(
                operation_ref=session["id"],
                scope=session["scope"],
                content_ref=session["content_ref"],
                coverage=actual["coverage"],
                content_digest=self.core.reading.read_signature(actual),
                requested_range=copy.deepcopy(value["range"]),
            )
            if observed not in reads:
                reads.append(observed)
            result["actual_content"] = {
                key: actual[key] for key in ("text", "coverage", "complete", "gaps")
            }
            result["reading_id"] = session["id"]
        marker["reads"] = reads
        if operation.startswith("chapter.") and result.get("state") == "completed":
            marker["actual_work"] = True
        self.store.put("metadata", marker)
        if (
            operation == "activity.save"
            and value.get("id") == activity["id"]
            and value.get("state") == "completed"
            and activity.get("autonomous_goal_id")
        ):
            concern = self.core.life.concerns.get(actor, activity["autonomous_goal_id"])
            if concern["state"] == "open":
                self.core.life.concerns.close(
                    actor,
                    concern["id"],
                    expected=concern["version"],
                    reason="实际活动及原件工具回执已保留",
                )
            actor_state = self.store.get("life_actors", actor)
            actor_state["autonomy_due_at"] = self.core.clock()
            actor_state["autonomy_last_activity"] = activity["id"]
            self.core.life._save("actors", actor_state)
        return result, self.core.role_actions.content_messages(actual) if actual else []
