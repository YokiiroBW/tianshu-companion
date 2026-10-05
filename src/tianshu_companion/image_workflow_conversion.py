"""Evidence-backed UI serializers and live ComfyUI semantic input discovery."""

import copy

from .contracts import canonical, digest
from .image_workflow import EDITABLE, Workflow


class UnsupportedWorkflow(ValueError):
    def __init__(self, code, node_id=None):
        super().__init__(code)
        self.code, self.node_id = code, node_id


# These nodes use the standard ComfyUI widget serializer. Custom list widgets are
# accepted only through a node-specific serializer below or a named widget export.
STANDARD_SERIALIZERS = {
    "CheckpointLoaderSimple",
    "CLIPLoader",
    "UNETLoader",
    "VAELoader",
    "LoraLoader",
    "LoraLoaderModelOnly",
    "CLIPTextEncode",
    "EmptyLatentImage",
    "EmptySD3LatentImage",
    "VAEDecode",
    "VAEEncode",
    "SaveImage",
    "LoadImage",
    "ImageScale",
    "ImageBlend",
    "KSampler",
    "KSamplerAdvanced",
    "Simple String",
    "AstrBot Prompt Router",
    "AnimaPromptPlus",
    "AnimaPromptPlusClipEncode",
    "AnimaArtistTagSelector",
    "AnimaArtistTagSelectorPlus",
    "AnimaCharacterTagSelector",
    "AnimaCharacterTagSelectorPlus",
    "AnimaClothingTagSelector",
    "AnimaClothingTagSelectorPlus",
    "AnimaPoseTagSelector",
    "AnimaPoseTagSelectorPlus",
    "AnimaBackgroundTagSelector",
    "AnimaBackgroundTagSelectorPlus",
}
RESOLUTION_PROPERTIES = {
    "mode": "mode",
    "width": "valueX",
    "height": "valueY",
    "batch_size": "batch_size",
    "auto_detect": "autoDetect",
    "auto_detect_source": "autoDetectSource",
    "auto_detect_width": "autoDetectWidth",
    "auto_detect_height": "autoDetectHeight",
    "auto_fit_on_change": "autoFitOnChange",
    "auto_resize_on_change": "autoResizeOnChange",
    "auto_snap_on_change": "autoSnapOnChange",
    "smart_fit": "smartFit",
    "use_custom_calc": "useCustomCalc",
    "preserve_scaling_ratio": "preserveScalingRatio",
    "selected_category": "selectedCategory",
    "snap_value": "snapValue",
    "upscale_value": "upscaleValue",
    "target_resolution": "targetResolution",
    "target_megapixels": "targetMegapixels",
    "auto_detect_presets_json": "autoDetectPresetsJSON",
    "rescale_mode": "rescaleMode",
    "rescale_value": "rescaleValue",
}


def definitions(info):
    return dict(
        info.get("input", {}).get("required", {}), **info.get("input", {}).get("optional", {})
    )


def scalar(spec):
    return isinstance(spec[0], list) or spec[0] in {"STRING", "INT", "FLOAT", "BOOLEAN", "COMBO"}


def validate_literal(value, spec, node):
    kind, options = spec[0], spec[1] if len(spec) > 1 else {}
    choices = (
        kind if isinstance(kind, list) else options.get("options") if kind == "COMBO" else None
    )
    if choices is not None:
        valid = value in choices
    elif kind == "INT":
        valid = type(value) is int and options.get("min", -(2**63)) <= value <= options.get(
            "max", 2**64 - 1
        )
    elif kind == "FLOAT":
        valid = type(value) in {int, float} and options.get("min", -1e100) <= value <= options.get(
            "max", 1e100
        )
    elif kind == "BOOLEAN":
        valid = type(value) is bool
    elif kind == "STRING":
        valid = isinstance(value, str)
    else:
        valid = False
    if not valid:
        raise UnsupportedWorkflow("invalid_literal:" + str(kind), node)


def ui_literals(node, info):
    kind, node_id = node["type"], str(node["id"])
    fields = definitions(info)
    widgets = node.get("widgets_values", [])
    if isinstance(widgets, dict):
        if set(widgets) - fields.keys():
            raise UnsupportedWorkflow("unknown_named_widget", node_id)
        return copy.deepcopy(widgets), "named_widgets"
    if kind == "ResolutionMaster":
        # This extension's front-end hides/reorders backend widgets. It restores
        # named backend widgets from properties in setupNode. Use that actual
        # property serialization, not saved positional values 5..7 (which in the
        # observed version are rescale mode/value/batch, not auto-detect fields).
        properties = node.get("properties", {})
        if (
            not isinstance(widgets, list)
            or len(widgets) < 4
            or any(value not in properties for value in RESOLUTION_PROPERTIES.values())
        ):
            raise UnsupportedWorkflow("resolution_master_properties_required", node_id)
        if widgets[0] != properties["mode"] or widgets[2:4] != [
            properties["valueX"],
            properties["valueY"],
        ]:
            raise UnsupportedWorkflow("resolution_master_dimension_conflict", node_id)
        values = {field: properties[name] for field, name in RESOLUTION_PROPERTIES.items()}
        values["latent_type"] = widgets[1]
        if set(values) != {field for field, spec in fields.items() if scalar(spec)}:
            raise UnsupportedWorkflow("resolution_master_schema_changed", node_id)
        return values, "resolution_master_named_properties_v1"
    if kind not in STANDARD_SERIALIZERS or not isinstance(widgets, list):
        raise UnsupportedWorkflow("unknown_widget_serializer", node_id)
    order = info.get("input_order", {})
    names = order.get("required", list(info.get("input", {}).get("required", {}))) + order.get(
        "optional", list(info.get("input", {}).get("optional", {}))
    )
    declared = {
        item.get("widget", {}).get("name") for item in node.get("inputs", []) if item.get("widget")
    }
    values, index = {}, 0
    for name in names:
        spec = fields[name]
        if not scalar(spec):
            continue
        if (
            declared
            and name not in declared
            and name not in info.get("input", {}).get("optional", {})
        ):
            raise UnsupportedWorkflow("widget_declaration_mismatch", node_id)
        if index >= len(widgets):
            if name in info.get("input", {}).get("optional", {}):
                continue
            raise UnsupportedWorkflow("missing_widget", node_id)
        values[name] = widgets[index]
        index += 1
        options = spec[1] if len(spec) > 1 else {}
        if options.get("control_after_generate"):
            if index >= len(widgets) or widgets[index] not in {
                "fixed",
                "increment",
                "decrement",
                "randomize",
            }:
                raise UnsupportedWorkflow("seed_control_widget_required", node_id)
            index += 1
    if index != len(widgets):
        raise UnsupportedWorkflow("extra_widget_values", node_id)
    return values, "reviewed_standard_widget_serializer"


def api_graph(source, object_info):
    if len(canonical(source).encode()) > 1_000_000:
        raise UnsupportedWorkflow("workflow_budget")
    evidence = []
    if "nodes" not in source:
        graph = source.get("prompt", source)
        if not isinstance(graph, dict):
            raise UnsupportedWorkflow("invalid_api_graph")
        graph = copy.deepcopy(graph)
    else:
        graph = {}
        links = {link[0]: link for link in source.get("links", [])}
        for node in source["nodes"]:
            node_id, kind = str(node["id"]), node["type"]
            if kind in {"MarkdownNote", "Note"}:
                continue
            if node.get("mode", 0) != 0:
                raise UnsupportedWorkflow("bypass_or_muted_node", node_id)
            if kind not in object_info:
                raise UnsupportedWorkflow("node_unavailable", node_id)
            inputs, strategy = ui_literals(node, object_info[kind])
            for slot, item in enumerate(node.get("inputs", [])):
                if item.get("link") is not None:
                    link = links.get(item["link"])
                    if not link or str(link[3]) != node_id or link[4] != slot:
                        raise UnsupportedWorkflow("unresolved_ui_link", node_id)
                    inputs[item["name"]] = [str(link[1]), link[2]]
            graph[node_id] = dict(
                class_type=kind, inputs=inputs, _meta=dict(title=node.get("title") or kind)
            )
            evidence.append(dict(node_id=node_id, strategy=strategy))
    validate_graph(graph, object_info)
    return graph, dict(
        strategy="node_specific_ui_serializers" if "nodes" in source else "api_export",
        evidence=evidence,
    )


def validate_graph(graph, object_info):
    for node_id, node in graph.items():
        info = object_info.get(node.get("class_type"))
        if info is None:
            raise UnsupportedWorkflow("node_unavailable", node_id)
        fields, inputs = definitions(info), node.get("inputs", {})
        if set(inputs) - fields.keys():
            raise UnsupportedWorkflow("unknown_input", node_id)
        if set(info.get("input", {}).get("required", {})) - inputs.keys():
            raise UnsupportedWorkflow("missing_required_input", node_id)
        for field, value in inputs.items():
            spec = fields[field]
            if (
                isinstance(value, list)
                and len(value) == 2
                and isinstance(value[0], str)
                and type(value[1]) is int
            ):
                origin = graph.get(value[0])
                outputs = object_info.get((origin or {}).get("class_type"), {}).get("output", [])
                if not 0 <= value[1] < len(outputs):
                    raise UnsupportedWorkflow("unresolved_api_link", node_id)
                expected = spec[0]
                if (
                    isinstance(expected, str)
                    and expected not in {"*", "COMBO"}
                    and outputs[value[1]] not in {expected, "*"}
                ):
                    raise UnsupportedWorkflow("link_type_mismatch", node_id)
            else:
                validate_literal(value, spec, node_id)
    # ComfyUI rejects cycles; report them at compile time instead of submitting.
    visiting, visited = set(), set()

    def visit(key):
        if key in visiting:
            raise UnsupportedWorkflow("graph_cycle", key)
        if key in visited:
            return
        visiting.add(key)
        for value in graph[key]["inputs"].values():
            if isinstance(value, list):
                visit(value[0])
        visiting.remove(key)
        visited.add(key)

    for key in graph:
        visit(key)


def discover_bindings(graph, object_info):
    candidates = {semantic: [] for semantic in EDITABLE}
    for node_id, node in graph.items():
        kind = node["class_type"]
        for semantic, allowed in EDITABLE.items():
            for field, value in node["inputs"].items():
                if (kind, field) not in allowed or isinstance(value, list):
                    continue
                spec = dict(
                    node=node_id,
                    class_type=kind,
                    input=field,
                    mode="append" if semantic in {"positive", "negative"} else "replace",
                )
                if type(value) is int:
                    live = definitions(object_info[kind])[field][1]
                    limits = {
                        "width": (64, 4096),
                        "height": (64, 4096),
                        "steps": (1, 150),
                        "seed": (0, 2**63 - 1),
                    }[semantic]
                    spec.update(
                        min=max(limits[0], live.get("min", 0)),
                        max=min(limits[1], live.get("max", 2**63 - 1)),
                    )
                candidates[semantic].append(spec)
    inferred = {}

    def choose(semantic, values):
        if len(values) == 1:
            inferred[semantic] = values[0]

    # Use actual sampler polarity and composer connections instead of choosing
    # the first CLIP node or overwriting a linked input.
    for semantic in ("positive", "negative"):
        targets = set()
        for node in graph.values():
            value = node["inputs"].get(semantic)
            if isinstance(value, list):
                targets.add(value[0])
        matches = [item for item in candidates[semantic] if item["node"] in targets]
        choose(semantic, matches)
    for semantic, field in {
        "character": "character_tags",
        "outfit": "clothing_tags",
        "pose": "pose_tags",
        "background": "background_tags",
        "camera": "extra_prompt",
    }.items():
        targets = set()
        router = set()
        for node in graph.values():
            if node["class_type"] not in {"AnimaPromptPlus", "AnimaPromptPlusClipEncode"}:
                continue
            value = node["inputs"].get(field)
            if not isinstance(value, list):
                continue
            origin = graph[value[0]]
            if origin["class_type"] == "AstrBot Prompt Router":
                prompt = origin["inputs"].get("prompt")
                if isinstance(prompt, list) and graph[prompt[0]]["class_type"] == "Simple String":
                    router.add(prompt[0])
            else:
                targets.add(value[0])
        matches = [
            item
            for item in candidates[semantic]
            if item["node"] in router or (item["node"] in targets and item["input"] != "extra_text")
        ]
        if not matches:
            matches = [
                item
                for item in candidates[semantic]
                if item["class_type"].startswith("Anima") and item["input"] == field
            ]
        choose(semantic, matches)
        if router and semantic == "camera":
            for item in candidates["positive"]:
                if item["node"] in router:
                    item["mode"] = "replace"
            choose(
                "positive",
                [item for item in candidates["positive"] if item["node"] in router],
            )
    for semantic in ("width", "height", "seed", "steps", "reference", "reference2", "reference3"):
        options = candidates[semantic]
        if semantic.startswith("reference"):
            index = int(semantic[-1]) - 1 if semantic[-1].isdigit() else 0
            if index < len(options):
                inferred[semantic] = options[index]
        else:
            choose(semantic, options)
    if not any(key in inferred for key in ("positive", "camera", "character")):
        choose("positive", candidates["positive"])
    return inferred, candidates


def inspect(source, object_info, workflow_id):
    try:
        graph, conversion = api_graph(source, object_info)
        bindings, candidates = discover_bindings(graph, object_info)
        outputs = [key for key, node in graph.items() if node["class_type"] == "SaveImage"]
        unresolved = []
        try:
            Workflow(graph, bindings, outputs)
        except ValueError:
            unresolved.append(
                dict(
                    code="binding_selection_required",
                    node_id=None,
                    detail="Select unambiguous prompt/output bindings",
                )
            )
        nodes = []
        for node_id, node in graph.items():
            fields = definitions(object_info[node["class_type"]])
            nodes.append(
                dict(
                    node_id=node_id,
                    class_type=node["class_type"],
                    title=node.get("_meta", {}).get("title", node["class_type"]),
                    inputs=[
                        dict(
                            name=name,
                            type=fields[name][0] if isinstance(fields[name][0], str) else "COMBO",
                            value=value,
                            linked=isinstance(value, list),
                        )
                        for name, value in node["inputs"].items()
                    ],
                )
            )
        mutable = {item["node"] for item in bindings.values()}
        result = dict(
            workflow_id=workflow_id,
            workflow_version=digest(source),
            format="ui" if "nodes" in source else "api",
            state="ready" if not unresolved else "unsupported",
            nodes=nodes,
            bindings=bindings,
            candidates=candidates,
            unresolved=unresolved,
            protected_nodes=[
                dict(node_id=key, class_type=node["class_type"])
                for key, node in graph.items()
                if key not in mutable
            ],
            outputs=outputs,
            conversion=conversion,
            model_receipt=None,
            selection_reason="semantic_node_and_connection_inference",
        )
        return result, graph
    except (UnsupportedWorkflow, KeyError, TypeError, IndexError) as error:
        return dict(
            workflow_id=workflow_id,
            workflow_version=digest(source),
            format="ui" if "nodes" in source else "api",
            state="unsupported",
            nodes=[],
            bindings={},
            candidates={},
            unresolved=[
                dict(
                    code=getattr(error, "code", "invalid_workflow"),
                    node_id=getattr(error, "node_id", None),
                    detail="Export an API-format graph or use a supported named serializer",
                )
            ],
            protected_nodes=[],
            outputs=[],
            conversion=dict(strategy="rejected", evidence=[]),
            model_receipt=None,
            selection_reason="unsupported_graph",
        ), None
