"""Load the coordinator's immutable release, never a second schema copy."""

import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

MANIFEST_HASH = "81e6cc4ddef7c6f82e055d4cb04b090db036dd5c52763473ce697aa02db478a1"


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
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


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

    def check(self, name, value):
        file, definition = name.split("#")
        schema = {"$ref": self.schemas[file]["$id"] + "#/$defs/" + definition}
        validator = Draft202012Validator(
            schema, registry=self.registry, format_checker=FormatChecker()
        )
        if not validator.is_valid(value):
            raise Fault("invalid_input")
        return value
