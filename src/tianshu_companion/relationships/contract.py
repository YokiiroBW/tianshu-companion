"""Pinned local candidate validation; no schema copy or release claim."""

import hashlib
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from ..contracts import Fault, strict_json

DOMAIN = "role-relationship/candidate-v1"
SCHEMA_HASH = "f3b588591411f1ed4b8aa7c9003d201530644d4dfc02294bdd9e9d7f847214a3"


class CandidateContract:
    def __init__(self, path):
        path = Path(path)
        if not path.is_absolute() or not path.is_file():
            raise ValueError("Relationship candidate schema requires an explicit absolute path")
        data = path.read_bytes().replace(b"\r\n", b"\n")
        if len(data) > 65536 or hashlib.sha256(data).hexdigest() != SCHEMA_HASH:
            raise ValueError("Unrecognized relationship candidate schema")
        schema = strict_json(data)
        self.validators = {
            name: Draft202012Validator(
                {"$defs": schema["$defs"], "$ref": "#/$defs/" + name},
                format_checker=FormatChecker(),
            )
            for name in schema["$defs"]
        }

    def check(self, name, value):
        if not self.validators[name].is_valid(value):
            raise Fault("dependency_unavailable")
        return value
