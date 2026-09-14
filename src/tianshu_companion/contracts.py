"""Load the coordinator's immutable release, never a second schema copy."""

import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

MANIFEST_HASH = "81e6cc4ddef7c6f82e055d4cb04b090db036dd5c52763473ce697aa02db478a1"
PROFILE_MANIFEST_HASH = "488d05438dd5b5abaa43a66a7eab0eb5cf615d5af01a964a7286cd23e68f7eb7"
PROFILE_DOMAIN = "profile-memory/v1"
WEB_MANIFEST_HASH = "e493a1b5d0f4cec8d55995553faf84042f4c33a59365d15423e57f4dc70a6c09"
SOURCE_MANIFEST_HASH = "178d0ce66210bdfad4cfb85d8b5f0905b0b67f834e2a530efe5636ff0373633d"


class Fault(Exception):
    def __init__(self, code, *, unknown=False, current_version=None):
        super().__init__(code)
        self.code = code
        self.unknown = unknown
        self.current_version = current_version

    @property
    def status(self):
        return {
            "unauthorized": 401,
            "forbidden": 403,
            "not_found": 404,
            "timeout": 408,
            "version_conflict": 409,
            "scope_changed": 409,
            "idempotency_conflict": 409,
            "result_unknown": 409,
            "queue_full": 429,
            "budget_exceeded": 429,
            "dependency_unavailable": 503,
        }.get(self.code, 400)

    def wire(self, request_id):
        result = dict(
            schema_version=1,
            request_id=request_id,
            code=self.code,
            execution_state="unknown" if self.unknown else "not_started",
            retryable=self.code in {"queue_full", "dependency_unavailable", "timeout"},
        )
        if self.current_version is not None:
            result["current_version"] = self.current_version
        return result


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def constant(value):
        raise ValueError("Non-finite JSON value")

    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


class Contracts:
    def __init__(self, directory):
        root = Path(directory).resolve()

        def read(path):
            return path.read_bytes().replace(b"\r\n", b"\n")

        manifest = read(root / "manifest.json")
        if hashlib.sha256(manifest).hexdigest() != MANIFEST_HASH:
            raise ValueError("Unrecognized contract release")
        release = json.loads(manifest)
        if release["version"] != "1.0.0":
            raise ValueError("Unsupported contract version")
        for name, expected in release["sha256"].items():
            path = (root.parent.parent / name).resolve()
            if not path.is_relative_to(root) or hashlib.sha256(read(path)).hexdigest() != expected:
                raise ValueError(f"Contract content mismatch: {name}")
        self.schemas = {p.stem: json.loads(read(p)) for p in (root / "schemas").glob("*.json")}
        self.registry = Registry().with_resources(
            (s["$id"], Resource.from_contents(s)) for s in self.schemas.values()
        )
        profile_root = root.parent.parent / PROFILE_DOMAIN
        manifest = read(profile_root / "manifest.json")
        if hashlib.sha256(manifest).hexdigest() != PROFILE_MANIFEST_HASH:
            raise ValueError("Unrecognized profile contract release")
        release = json.loads(manifest)
        if (
            release["version"] != "1.0.0"
            or release["version_domain"] != PROFILE_DOMAIN
            or release["dependency"]["manifest_sha256"] != MANIFEST_HASH
        ):
            raise ValueError("Unsupported profile contract version")
        for name, expected in release["sha256"].items():
            path = (profile_root / name).resolve()
            if (
                not path.is_relative_to(profile_root)
                or hashlib.sha256(read(path)).hexdigest() != expected
            ):
                raise ValueError(f"Profile contract content mismatch: {name}")
        schema = json.loads(read(profile_root / "schemas/profiles.json"))
        self.schemas["profiles"] = schema
        self.registry = self.registry.with_resource(schema["$id"], Resource.from_contents(schema))
        source_root = root.parent.parent / "source-sync/v1"
        manifest = read(source_root / "manifest.json")
        if hashlib.sha256(manifest).hexdigest() != SOURCE_MANIFEST_HASH:
            raise ValueError("Unrecognized source contract release")
        release = json.loads(manifest)
        if release["version"] != "1.0.0" or {
            d["package"]: d["manifest_sha256"] for d in release["dependencies"]
        } != {"text-dialogue/v1": MANIFEST_HASH, PROFILE_DOMAIN: PROFILE_MANIFEST_HASH}:
            raise ValueError("Unsupported source contract dependencies")
        for name, expected in release["sha256"].items():
            path = (source_root / name).resolve()
            if (
                not path.is_relative_to(source_root)
                or hashlib.sha256(read(path)).hexdigest() != expected
            ):
                raise ValueError(f"Source contract content mismatch: {name}")
        for path in (source_root / "schemas").glob("*.json"):
            schema = json.loads(read(path))
            self.schemas[path.stem] = schema
            self.registry = self.registry.with_resource(
                schema["$id"], Resource.from_contents(schema)
            )

        web_root = root.parent.parent / "web-conversation/v1"
        manifest = read(web_root / "manifest.json")
        if hashlib.sha256(manifest).hexdigest() != WEB_MANIFEST_HASH:
            raise ValueError("Unrecognized web conversation release")
        release = json.loads(manifest)
        if (
            release["version"] != "1.0.0"
            or release["package"] != "web-conversation/v1"
            or release["dependency"]["manifest_sha256"] != MANIFEST_HASH
        ):
            raise ValueError("Unsupported web conversation dependencies")
        for name, expected in release["sha256"].items():
            path = (web_root / name).resolve()
            if (
                not path.is_relative_to(web_root)
                or hashlib.sha256(read(path)).hexdigest() != expected
            ):
                raise ValueError(f"Web conversation content mismatch: {name}")
        schema = json.loads(read(web_root / "schema.json"))
        self.schemas["web-conversation"] = schema
        self.registry = self.registry.with_resource(schema["$id"], Resource.from_contents(schema))

    def check(self, name, value):
        file, definition = name.split("#")
        schema = {"$ref": self.schemas[file]["$id"] + "#/$defs/" + definition}
        validator = Draft202012Validator(
            schema, registry=self.registry, format_checker=FormatChecker()
        )
        if not validator.is_valid(value):
            raise Fault("invalid_input")
        return value
