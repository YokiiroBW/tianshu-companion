"""Pinned published relationship v1 validation without a local schema copy."""

import hashlib
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from ..contracts import Fault, strict_json

DOMAIN = "role-relationship/v1"
SCHEMA_HASH = "e96397bac2b6ad8ff9d23c023d7d3c5ba0701734b27053a05b9d0f65a7ff8ee6"


class CandidateContract:
    def __init__(self, path):
        path = Path(path)
        if not path.is_absolute() or not path.is_file():
            raise ValueError("Relationship schema requires an explicit absolute path")
        data = path.read_bytes().replace(b"\r\n", b"\n")
        if len(data) > 65536 or hashlib.sha256(data).hexdigest() != SCHEMA_HASH:
            raise ValueError("Unrecognized published relationship schema")
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
