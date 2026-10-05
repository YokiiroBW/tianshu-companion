"""Load the coordinator's immutable release, never a second schema copy."""

import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from .contract_releases import RELEASES


MANIFEST_HASH = RELEASES["text-dialogue/v1"]
PROFILE_DOMAIN = "profile-memory/v1"
PROFILE_MANIFEST_HASH = RELEASES[PROFILE_DOMAIN]
WEB_MANIFEST_HASH = RELEASES["web-conversation/v1"]
SOURCE_MANIFEST_HASH = RELEASES["source-sync/v1"]
LIFE_READ_MANIFEST_HASH = RELEASES["life-read/v1"]


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
        packages = root.parent.parent
        self.schemas = {}
        self.registry = Registry()

        def read(path):
            return path.read_bytes().replace(b"\r\n", b"\n")

        def dependency(package, expected):
            if package not in RELEASES:
                matches = [name for name in RELEASES if name.split("/")[0] == package]
                if len(matches) != 1:
                    raise ValueError("Unknown contract dependency")
                package = matches[0]
            if expected != RELEASES[package]:
                raise ValueError("Unsupported contract dependency")

        aliases = {
            "profile-memory/v1": "profiles",
            "life-read/v1": "life-read",
            "life-runtime/v2": "life-runtime",
            "bot-delivery/v2": "bot-delivery",
            "web-conversation/v1": "web-conversation",
            "memory-context/v1": "memory-context",
            "knowledge-content/v1": "knowledge-content",
            "image-backend/v1": "image-backend",
        }
        for package in (
            "text-dialogue/v1",
            "profile-memory/v1",
            "source-sync/v1",
            "web-conversation/v1",
            "life-read/v1",
            "memory-context/v1",
            "life-runtime/v2",
            "bot-delivery/v2",
            "knowledge-content/v1",
            "image-backend/v1",
        ):
            package_root = (packages / package).resolve()
            manifest = read(package_root / "manifest.json")
            if hashlib.sha256(manifest).hexdigest() != RELEASES[package]:
                raise ValueError("Unrecognized contract release: " + package)
            release = json.loads(manifest)
            expected_version = "2.0.0" if package.endswith("/v2") else "1.0.0"
            if release.get("version") != expected_version:
                raise ValueError("Unsupported contract version")
            dependencies = release.get("dependencies", [])
            if isinstance(dependencies, dict):
                for name, expected in dependencies.items():
                    dependency(name, expected)
            else:
                for item in dependencies:
                    dependency(item["package"], item["manifest_sha256"])
            if release.get("dependency"):
                item = release["dependency"]
                dependency(item["package"], item["manifest_sha256"])
            for name, expected in release["sha256"].items():
                path = (
                    packages / name if name.startswith(package + "/") else package_root / name
                ).resolve()
                if (
                    not path.is_relative_to(package_root)
                    or hashlib.sha256(read(path)).hexdigest() != expected
                ):
                    raise ValueError("Contract content mismatch: " + name)
                if path.suffix != ".json":
                    continue
                schema = json.loads(read(path))
                if not isinstance(schema, dict) or "$id" not in schema:
                    continue
                key = path.stem if "dependencies" in path.parts else aliases.get(package, path.stem)
                if key in self.schemas and self.schemas[key]["$id"] != schema["$id"]:
                    raise ValueError("Contract alias collision")
                self.schemas[key] = schema
                self.registry = self.registry.with_resource(
                    schema["$id"], Resource.from_contents(schema)
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
