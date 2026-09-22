"""Explicit opt-in fresh-venv installation evidence. Does not claim a Docker build.

Run with --output <new task-local directory>. Network use is limited to pinned packages.
The runtime pin list is read from the actual Dockerfile, not duplicated here.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib
import venv


def verify(output):
    root = Path(__file__).resolve().parents[1]
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    runtime_pins = re.findall(r'"([\w.-]+==[^"\s]+)"', dockerfile.split("WORKDIR", 1)[0])
    assert runtime_pins and len(runtime_pins) == len(set(runtime_pins))
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    build_pins = project["build-system"]["requires"]
    assert all(pin in dockerfile for pin in build_pins)
    environment = output / "venv"
    venv.EnvBuilder(with_pip=True).create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    env = {
        key: value for key, value in os.environ.items() if key not in {"PYTHONPATH", "PYTHONHOME"}
    }
    env.update(PIP_DISABLE_PIP_VERSION_CHECK="1", PYTHONNOUSERSITE="1")
    steps = []

    def run(*args):
        completed = subprocess.run(
            [str(python), *args],
            cwd=output,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        steps.append({"args": list(args), "returncode": completed.returncode})
        with (output / "commands.log").open("a", encoding="utf-8") as log:
            log.write(json.dumps(steps[-1]) + "\n" + completed.stdout + completed.stderr + "\n")
        if completed.returncode:
            raise RuntimeError(f"Installation step failed; see {output / 'commands.log'}")
        return completed.stdout

    initial = json.loads(run("-m", "pip", "list", "--format=json"))
    assert "setuptools" not in {item["name"].lower() for item in initial}
    run("-m", "pip", "install", "--no-cache-dir", "--only-binary=:all:", "--no-deps", *runtime_pins)
    stage = output / "source"
    stage.mkdir()
    for name in ("pyproject.toml", "README.md"):
        shutil.copyfile(root / name, stage / name)
    for name in ("src", "integrations"):
        shutil.copytree(
            root / name, stage / name, ignore=shutil.ignore_patterns("__pycache__", "*.egg-info")
        )
    run("-m", "pip", "install", "--no-cache-dir", "--only-binary=:all:", "--no-deps", *build_pins)
    wheels = output / "wheels"
    run(
        "-m",
        "pip",
        "wheel",
        "--no-cache-dir",
        "--no-deps",
        "--no-build-isolation",
        "--wheel-dir",
        str(wheels),
        str(stage),
    )
    wheel = next(wheels.glob("tianshu_companion-*.whl"))
    run("-m", "pip", "install", "--no-cache-dir", "--no-index", "--no-deps", str(wheel))
    run("-m", "pip", "uninstall", "-y", "setuptools")
    run("-m", "pip", "check")
    run(
        "-c",
        "import tianshu_companion.app, tianshu_nonebot; from pathlib import Path; "
        "p=Path(tianshu_companion.app.__file__).resolve(); assert 'site-packages' in p.parts; print(p)",
    )
    run("-m", "tianshu_companion.runtime_cli", "--print-config")
    installed = json.loads(run("-m", "pip", "list", "--format=json"))

    def normalize(name):
        return re.sub(r"[-_.]+", "-", name).lower()

    versions = {normalize(item["name"]): item["version"] for item in installed}
    expected = dict(pin.split("==", 1) for pin in runtime_pins)
    assert all(versions[normalize(name)] == version for name, version in expected.items())
    assert set(versions) == {normalize(name) for name in expected} | {"pip", "tianshu-companion"}
    report = {
        "python": sys.version,
        "platform": sys.platform,
        "docker_build_executed": False,
        "linux_runtime_executed": False,
        "initial_packages": initial,
        "installed_packages": installed,
        "dockerfile_sha256": hashlib.sha256((root / "Dockerfile").read_bytes()).hexdigest(),
        "pyproject_sha256": hashlib.sha256((root / "pyproject.toml").read_bytes()).hexdigest(),
        "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "steps": steps,
        "status": "passed",
    }
    (output / "result.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "passed", "report": str(output / "result.json")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    verify(parser.parse_args().output)
