"""The dependency-free released checker agrees with the exact candidate examples."""

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "integrations" / "shared"))
import participation_protocol as wire  # noqa: E402


def package():
    context = json.loads((ROOT / ".runtime" / "workspace-context.json").read_text(encoding="utf-8"))
    path = Path(context["contract_root"]) / "observation-participation" / "candidate-v1"
    return path


def test_pinned_examples_and_release_checker():
    source = package()
    import hashlib

    assert (
        hashlib.sha256((source / "manifest.json").read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        == wire.MANIFEST_SHA256
    )
    examples = json.loads((source / "examples.json").read_text(encoding="utf-8"))
    for case in examples["positive"] + examples["runtime_scenarios"]:
        document = case["document"]
        wire.document(document, document["kind"])
    for case in examples["negative"]:
        document = case["document"]
        with pytest.raises((ValueError, TypeError)):
            wire.document(document, document["kind"])


def test_shared_release_bytes_and_no_workspace_dependency():
    for directory in ("nonebot/tianshu_nonebot", "astrbot/astrbot_plugin_tianshu"):
        for original, released in (
            ("tianshu_adapter_rpc.py", "rpc.py"),
            ("participation.py", "participation.py"),
            ("participation_protocol.py", "participation_protocol.py"),
        ):
            expected = (ROOT / "integrations" / "shared" / original).read_bytes()
            assert (ROOT / "integrations" / directory / released).read_bytes() == expected
            assert b"C:/YOKI" not in expected and b"workspace-context" not in expected
