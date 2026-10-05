"""Reviewed immutable ComfyUI graph and typed semantic bindings."""

import copy
import re
from .contracts import canonical, digest
from .life import text

EDITABLE = {
    "positive": {
        ("CLIPTextEncode", "text"),
        ("AnimaArtistPack", "base_prompt"),
        ("AnimaPromptPlusClipEncode", "extra_prompt"),
        ("AnimaPromptPlus", "extra_prompt"),
        ("Simple String", "text"),
        ("AstrBot Prompt Router", "prompt"),
    },
    "negative": {("CLIPTextEncode", "text")},
    "reference": {("LoadImage", "image")},
    "reference2": {("LoadImage", "image")},
    "reference3": {("LoadImage", "image")},
    "seed": {("KSampler", "seed"), ("FLS_SamplerV4", "seed")},
    "steps": {("KSampler", "steps"), ("FLS_SamplerV4", "steps")},
    "width": {
        ("EmptyLatentImage", "width"),
        ("ImageScale", "width"),
        ("ResolutionMaster", "width"),
    },
    "height": {
        ("EmptyLatentImage", "height"),
        ("ImageScale", "height"),
        ("ResolutionMaster", "height"),
    },
}
for semantic, selector, field in (
    ("character", "AnimaCharacterTagSelectorPlus", "character_tags"),
    ("outfit", "AnimaClothingTagSelectorPlus", "clothing_tags"),
    ("pose", "AnimaPoseTagSelectorPlus", "pose_tags"),
    ("background", "AnimaBackgroundTagSelectorPlus", "background_tags"),
):
    EDITABLE[semantic] = {
        (selector, field),
        (selector, "extra_text"),
        (selector.removesuffix("Plus"), field),
        ("AnimaPromptPlusClipEncode", field),
        ("AnimaPromptPlus", field),
        ("Simple String", "text"),
    }
EDITABLE["camera"] = {
    ("Simple String", "text"),
    ("AnimaPromptPlusClipEncode", "extra_prompt"),
    ("AnimaPromptPlus", "extra_prompt"),
}
TEXT_FIELDS = {"positive", "negative", "character", "outfit", "pose", "background", "camera"}


class Workflow:
    """Reviewed API export plus explicit typed editable inputs, never UI conversion."""

    @classmethod
    def standard(cls, checkpoint, *, edit=False, references=1):
        text(checkpoint, 128)
        graph = {
            "1": {"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": checkpoint}},
            "2": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": "Fictional character illustration", "clip": ["1", 1]},
            },
            "3": {"class_type": "CLIPTextEncode", "inputs": {"text": "", "clip": ["1", 1]}},
            "4": {
                "class_type": "EmptyLatentImage",
                "inputs": {"width": 768, "height": 768, "batch_size": 1},
            },
            "5": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": 0,
                    "steps": 24,
                    "cfg": 7.0,
                    "sampler_name": "euler",
                    "scheduler": "normal",
                    "denoise": 0.65 if edit else 1.0,
                    "model": ["1", 0],
                    "positive": ["2", 0],
                    "negative": ["3", 0],
                    "latent_image": ["8", 0] if edit else ["4", 0],
                },
            },
            "6": {"class_type": "VAEDecode", "inputs": {"samples": ["5", 0], "vae": ["1", 2]}},
            "7": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "tianshu", "images": ["6", 0]},
            },
        }
        bindings = {
            "positive": dict(node="2", class_type="CLIPTextEncode", input="text"),
            "negative": dict(node="3", class_type="CLIPTextEncode", input="text"),
            "seed": dict(node="5", class_type="KSampler", input="seed", min=0, max=2**63 - 1),
            "steps": dict(node="5", class_type="KSampler", input="steps", min=1, max=150),
            "width": dict(node="4", class_type="EmptyLatentImage", input="width", min=64, max=4096),
            "height": dict(
                node="4", class_type="EmptyLatentImage", input="height", min=64, max=4096
            ),
        }
        if edit:
            graph.update(
                {
                    "8": {
                        "class_type": "VAEEncode",
                        "inputs": {"pixels": ["10", 0], "vae": ["1", 2]},
                    },
                    "9": {"class_type": "LoadImage", "inputs": {"image": "input.png"}},
                    "10": {
                        "class_type": "ImageScale",
                        "inputs": {
                            "image": ["9", 0],
                            "upscale_method": "lanczos",
                            "width": 768,
                            "height": 768,
                            "crop": "disabled",
                        },
                    },
                }
            )
            bindings["reference"] = dict(node="9", class_type="LoadImage", input="image")
            previous = "9"
            for number in range(2, references + 1):
                loader, blend = str(7 + number * 2), str(8 + number * 2)
                graph[loader] = {"class_type": "LoadImage", "inputs": {"image": "input.png"}}
                graph[blend] = {
                    "class_type": "ImageBlend",
                    "inputs": {
                        "image1": [previous, 0],
                        "image2": [loader, 0],
                        "blend_factor": 0.3,
                        "blend_mode": "normal",
                    },
                }
                bindings["reference" + str(number)] = dict(
                    node=loader, class_type="LoadImage", input="image"
                )
                previous = blend
            graph["10"]["inputs"]["image"] = [previous, 0]
            for field in ("width", "height"):
                bindings[field] = dict(
                    node="10", class_type="ImageScale", input=field, min=64, max=4096
                )
        return cls(graph, bindings, ["7"])

    def __init__(self, graph, bindings, outputs):
        if not isinstance(graph, dict) or not graph or "nodes" in graph:
            raise ValueError("Reviewed API-format export required")
        if len(canonical(graph).encode()) > 1_000_000:
            raise ValueError("Workflow too large")
        for key, node in graph.items():
            if not isinstance(key, str) or not isinstance(node, dict):
                raise ValueError("Invalid API node")
            text(node.get("class_type"), 128)
            if not isinstance(node.get("inputs"), dict):
                raise ValueError("Invalid API inputs")
            for value in node["inputs"].values():
                if isinstance(value, list) and (
                    len(value) != 2
                    or value[0] not in graph
                    or type(value[1]) is not int
                    or value[1] < 0
                ):
                    raise ValueError("Invalid node link")
        if not outputs or any(graph.get(k, {}).get("class_type") != "SaveImage" for k in outputs):
            raise ValueError("Explicit SaveImage outputs required")
        seen = set()
        self.router_inputs = {
            tuple(node["inputs"]["prompt"][:1])
            for node in graph.values()
            if node["class_type"] == "AstrBot Prompt Router"
            and isinstance(node["inputs"].get("prompt"), list)
        }
        for name, spec in bindings.items():
            if name not in EDITABLE:
                raise ValueError("Unsupported binding")
            if (spec["class_type"], spec["input"]) not in EDITABLE[name]:
                raise ValueError("Input is not an approved editable field")
            node = graph.get(spec["node"], {})
            value = node.get("inputs", {}).get(spec["input"])
            pair = (spec["node"], spec["input"])
            routed = (
                (spec["node"],) in self.router_inputs
                and spec["class_type"] == "Simple String"
                and name in {"positive", "camera", "outfit", "pose", "background"}
            )
            if node.get("class_type") != spec["class_type"] or (pair in seen and not routed):
                raise ValueError("Missing/type-mismatched/duplicate binding")
            seen.add(pair)
            if spec.get("mode", "append" if name == "positive" else "replace") not in {
                "append",
                "replace",
            }:
                raise ValueError("Invalid binding mode")
            kind = str if name in TEXT_FIELDS or name.startswith("reference") else int
            if type(value) is not kind:
                raise ValueError("Editable literal of correct type required")
            if kind is int and not (
                type(spec.get("min")) is int
                and type(spec.get("max")) is int
                and 0 <= spec["min"] <= value <= spec["max"] <= 2**63 - 1
            ):
                raise ValueError("Explicit bounded numeric constraint required")
        if not ({"positive", "camera"} & bindings.keys()):
            raise ValueError("Prompt binding required")
        self.graph, self.bindings, self.outputs = copy.deepcopy((graph, bindings, outputs))
        self.version = digest([graph, bindings, outputs])

    def render(self, values):
        graph = copy.deepcopy(self.graph)
        routed = {}
        for name, value in values.items():
            spec = self.bindings.get(name)
            if spec is None:
                raise ValueError("Unbound input")
            if name in TEXT_FIELDS or name.startswith("reference"):
                text(value, 8000)
            elif type(value) is not int or not spec["min"] <= value <= spec["max"]:
                raise ValueError("Out of range")
            if (
                (spec["node"],) in self.router_inputs
                and spec["class_type"] == "Simple String"
                and name in {"positive", "camera", "outfit", "pose", "background"}
            ):
                routed.setdefault((spec["node"], spec["input"]), {})[name] = value
                continue
            # Fixed style/artist prefix is retained even in the editable positive node.
            if (
                name in TEXT_FIELDS
                and spec.get("mode", "append" if name == "positive" else "replace") == "append"
            ):
                value = graph[spec["node"]]["inputs"][spec["input"]] + "\n" + value
            graph[spec["node"]]["inputs"][spec["input"]] = value
        for (node, field), parts in routed.items():
            original = self.graph[node]["inputs"][field].replace("\r\n", "\n").strip()
            inherited = {}
            for semantic, label in {
                "user": "User image request",
                "outfit": "Reference and wardrobe ruling",
                "pose": "Composition and continuity",
                "background": "Scene, style and final preset",
            }.items():
                # This is the router's documented envelope parser. Missing fields
                # inherit their existing section; unsectioned template text is
                # retained as context when no clothing override was supplied.
                pattern = (
                    rf"(?:^|\n)\s*\[{re.escape(label)}\]\s*\n?(.*?)"
                    r"(?=\n\s*\[[^\]]+\]\s*\n|\n\s*Negative prompt\s*:|\Z)"
                )
                match = re.search(pattern, original, re.IGNORECASE | re.DOTALL)
                inherited[semantic] = match.group(1).strip() if match else ""
            if not any(inherited.values()) and not parts.get("outfit"):
                inherited["user"] = original
            sections = [
                (
                    "User image request",
                    "\n".join(
                        value
                        for value in (inherited["user"], parts.get("positive"), parts.get("camera"))
                        if value
                    ),
                ),
                ("Reference and wardrobe ruling", parts.get("outfit", inherited["outfit"])),
                ("Composition and continuity", parts.get("pose", inherited["pose"])),
                ("Scene, style and final preset", parts.get("background", inherited["background"])),
            ]
            graph[node]["inputs"][field] = "\n\n".join(
                "[" + label + "]\n" + value for label, value in sections
            )
        sizes = {}
        for field in ("width", "height"):
            spec = self.bindings.get(field)
            if spec:
                sizes[field] = graph[spec["node"]]["inputs"][spec["input"]]
        if sizes.get("width", 0) * sizes.get("height", 0) > 4_194_304:
            raise ValueError("Image pixel budget exceeded")
        return graph
