"""Build isolated AstrBot ZIP and NoneBot wheel without modifying the host."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUT = ROOT.parent / ".runtime" / "adapter-artifacts"
VERSION = "0.5.0"
ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


def _files(folder: Path):
    for path in sorted(folder.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
            yield path


def _wheel_record(entries: dict[str, bytes], record_name: str) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    for name, data in sorted(entries.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        writer.writerow((name, "sha256=" + digest, len(data)))
    writer.writerow((record_name, "", ""))
    return stream.getvalue().encode()


def _write_entry(archive: zipfile.ZipFile, name: str, data: bytes) -> None:
    info = zipfile.ZipInfo(name, date_time=ZIP_TIMESTAMP)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    archive.writestr(info, data)


def main() -> None:
    source = (ROOT / "shared" / "tianshu_adapter_rpc.py").read_bytes()
    astr = ROOT / "astrbot" / "astrbot_plugin_tianshu"
    none = ROOT / "nonebot" / "tianshu_nonebot"
    for folder in (astr, none):
        if (folder / "rpc.py").read_bytes() != source:
            raise SystemExit(f"stale vendored RPC module: {folder}")
    OUT.mkdir(parents=True, exist_ok=True)
    astr_zip = OUT / f"astrbot_plugin_tianshu-{VERSION}.zip"
    with zipfile.ZipFile(astr_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in _files(astr):
            _write_entry(
                archive,
                f"astrbot_plugin_tianshu/{path.relative_to(astr).as_posix()}",
                path.read_bytes(),
            )
        _write_entry(
            archive,
            "astrbot_plugin_tianshu/README.md",
            (ROOT / "astrbot" / "ADAPTER.md").read_bytes(),
        )

    dist = f"tianshu_nonebot_adapter-{VERSION}.dist-info"
    entries = {
        f"tianshu_nonebot/{path.relative_to(none).as_posix()}": path.read_bytes()
        for path in _files(none)
    }
    entries[dist + "/WHEEL"] = (
        "Wheel-Version: 1.0\nGenerator: tianshu-adapter-builder\n"
        "Root-Is-Purelib: true\nTag: py3-none-any\n"
    ).encode()
    entries[dist + "/METADATA"] = (
        "Metadata-Version: 2.3\nName: tianshu-nonebot-adapter\n"
        f"Version: {VERSION}\nRequires-Python: >=3.12\n"
        "Requires-Dist: nonebot2[fastapi]>=2.5,<2.6\n"
        "Requires-Dist: nonebot-adapter-onebot>=2.4,<2.5\n"
    ).encode()
    entries[dist + "/README.md"] = (ROOT / "nonebot" / "ADAPTER.md").read_bytes()
    record = dist + "/RECORD"
    entries[record] = _wheel_record(entries, record)
    wheel = OUT / f"tianshu_nonebot_adapter-{VERSION}-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(entries.items()):
            _write_entry(archive, name, data)
    for path in (astr_zip, wheel):
        print(f"{path} sha256={hashlib.sha256(path.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    sys.exit(main())
