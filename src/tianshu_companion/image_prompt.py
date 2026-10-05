"""Structured fictional image intent, translated to prompt text rather than JSON."""

from .life import text
from .image_workflow import TEXT_FIELDS


def intent_from_life(snapshot, outfit, scene=None, *, character="", defaults=None, intent=None):
    defaults = defaults or {}
    room, world = snapshot["room"], snapshot["world"]
    background = []
    for owner, fields in ((room, ("name", "description")), (world, ("weather", "season"))):
        for field in fields:
            value = owner.get(field)
            if isinstance(value, str) and value.strip():
                background.append(value)
    result = dict(
        positive="Fictional character illustration",
        outfit=outfit["prompt"] if outfit else "",
        pose=scene or snapshot["actor"]["activity"],
        background=", ".join(background),
        camera="",
        negative="",
    )
    if character:
        result["character"] = character
    result.update({key: value for key, value in defaults.items() if key in TEXT_FIELDS})
    result.update(intent or {})
    for key, value in result.items():
        if key not in TEXT_FIELDS or not isinstance(value, str) or len(value) > 8000:
            raise ValueError("Invalid semantic intent")
        if value:
            text(value, 8000)
    return result


def bound_prompt_values(workflow, intent, parameters=None):
    values = dict(parameters or {})
    for key, value in intent.items():
        if key in workflow.bindings and value:
            values[key] = value
    unbound = [key for key, value in intent.items() if value and key not in workflow.bindings]
    if unbound:
        # Standard text workflows receive clear prose with semantic labels. Multi-section
        # graphs receive their mapped fields directly; no canonical structured JSON leaks.
        destination = "positive" if "positive" in workflow.bindings else "camera"
        if destination not in workflow.bindings:
            raise ValueError("No general prompt binding")
        parts = [values.get(destination, "")]
        parts.extend(key.capitalize() + ": " + intent[key] for key in unbound if key != "negative")
        if intent.get("negative") and "negative" not in workflow.bindings:
            raise ValueError("Negative prompt has no binding")
        values[destination] = "\n".join(part for part in parts if part)
    return values


def preview(workflow, values, *, workflow_id=None, model_receipt=None):
    from .contracts import digest

    graph = workflow.render(values)
    changes = []
    modified = set()
    for semantic in values:
        spec = workflow.bindings[semantic]
        node, field = spec["node"], spec["input"]
        before = workflow.graph[node]["inputs"][field]
        after = graph[node]["inputs"][field]
        if before != after:
            modified.add(node)
            changes.append(
                dict(semantic=semantic, node_id=node, input=field, before=before, after=after)
            )
    dimensions = {}
    for field in ("width", "height"):
        spec = workflow.bindings.get(field)
        dimensions[field] = graph[spec["node"]]["inputs"][spec["input"]] if spec else None
    return dict(
        state="ready",
        provider="comfyui",
        workflow_id=workflow_id,
        workflow_version=workflow.version,
        graph=graph,
        graph_sha256=digest(graph),
        prompts={key: value for key, value in values.items() if key in TEXT_FIELDS},
        dimensions=dimensions,
        changes=changes,
        preserved_nodes=[node for node in graph if node not in modified],
        warnings=[],
        model_receipt=model_receipt,
    )
