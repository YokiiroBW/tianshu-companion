"""Shared proposal diagnostics and resolved schemas for current memory tools."""

from .contracts import Fault


class ProposalStructureError(Fault):
    """A newly generated proposal failed locally, before any Memory submission."""

    def __init__(self, code, diagnostics):
        super().__init__(code)
        self.diagnostics = diagnostics


def output_schema(contracts):
    """Resolve the published schema for a model that cannot access our Registry."""
    schema = contracts.schemas["memory-extraction"]

    def inline(value, resolver, active=()):
        if isinstance(value, list):
            return [inline(item, resolver, active) for item in value]
        if not isinstance(value, dict):
            return value
        if "$ref" in value:
            ref = value["$ref"]
            resolved = resolver.lookup(ref)
            identity = (ref, id(resolved.contents))
            if identity in active:
                raise ValueError("Recursive extraction output schema")
            target = inline(resolved.contents, resolved.resolver, (*active, identity))
            siblings = inline(
                {key: item for key, item in value.items() if key != "$ref"}, resolver, active
            )
            return dict(allOf=[target, siblings]) if siblings else target
        return {
            key: inline(item, resolver, active)
            for key, item in value.items()
            if key not in {"$id", "$schema", "$defs"}
        }

    return inline({"$ref": schema["$id"] + "#/$defs/segment_output"}, contracts.registry.resolver())
