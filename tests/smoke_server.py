"""Start an unconfigured loopback server, verify rejection, then stop it."""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

with socket.socket() as socket_:
    socket_.bind(("127.0.0.1", 0))
    port = socket_.getsockname()[1]
env = dict(os.environ)
env.pop("TIANSHU_COMPANION_CONFIG", None)
process = subprocess.Popen(
    [
        sys.executable,
        "-m",
        "uvicorn",
        "tianshu_companion.app:create_app",
        "--factory",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--workers",
        "1",
    ],
    env=env,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
)
try:
    health = None
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError("Local server stopped before becoming available")
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/healthz", timeout=0.2
            ) as response:
                health = json.load(response)
            break
        except (OSError, urllib.error.URLError):
            time.sleep(0.05)
    assert health == dict(alive=True, configured=False, external_dependencies="not_verified")
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/internal/v1/conversation/ingest",
        data=b"{}",
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=1)
        raise AssertionError("Unconfigured ingest must reject")
    except urllib.error.HTTPError as error:
        assert error.code == 503
        body = json.load(error)
        assert body["code"] == "dependency_unavailable"
    print(json.dumps(dict(health=health, ingest_status=503, external_connections=0)))
finally:
    process.terminate()
    try:
        process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=5)
    assert process.poll() is not None
    print("Local server stopped")
