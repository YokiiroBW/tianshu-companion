"""Real processes for persona management: a CLI subprocess and a live management port.

Nothing here is a mocked function boundary. The offline command is a separate interpreter
that opens the SQLite file itself and takes the single owner lock; the online command is a
separate interpreter that speaks HTTP to a real loopback server built from the deployment
configuration. Both drive the same application entry point, so this is where a drift between
the two adapters would show up.
"""

import asyncio
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from support import Harness, persona_config
from tianshu_companion.personas import Personas
from tianshu_companion.store import Store

ROOT = Path(__file__).resolve().parents[1]
A = "actor:a"
ADMIN_TOKEN_ENV = "TIANSHU_PERSONA_ADMIN_TOKEN"
ADMIN_TOKEN = "synthetic-persona-admin-credential"
NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0


def cli(*arguments, env=None):
    """Run the installed command surface in its own interpreter."""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(ROOT / "src"), environment.get("PYTHONPATH", "")]
    ).strip(os.pathsep)
    environment.update(env or {})
    process = subprocess.run(
        [sys.executable, "-m", "tianshu_companion.persona_cli", *arguments],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=str(ROOT),
        env=environment,
        timeout=120,
        creationflags=NO_WINDOW,
    )
    assert process.returncode in (0, 1), process.stderr
    payload = json.loads(process.stdout)
    return process.returncode, payload


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def write_json(path, document):
    path.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    return path


def draft_command(target, *, request_id, expected, content):
    """One `draft` command line, so a retry reuses the very same arguments."""
    return [
        *target,
        "--subject",
        A,
        "--operator",
        "operator:cli",
        "--reason",
        "authoring with a retry",
        "--request-id",
        request_id,
        "--expected",
        str(expected),
        "--content",
        str(content),
        "draft",
    ]


def test_offline_cli_runs_the_whole_lifecycle_as_a_real_process(tmp_path):
    path = tmp_path / "personas.db"
    import_config(tmp_path, path)
    document = write_json(tmp_path / "content.json", {"persona": "CLI authored persona"})

    code, listed = cli("--database", str(path), "list")
    assert code == 0 and listed["ok"] is True
    assert listed["result"]["subjects"] == [A]

    # Every writer names the version it saw; the command reads the current one first.
    code, before = cli("--database", str(path), "--subject", A, "get")
    assert code == 0, before
    imported_version = str(before["result"]["persona"]["version"])

    code, drafted = cli(
        "--database",
        str(path),
        "--subject",
        A,
        "--operator",
        "operator:cli",
        "--reason",
        "first authored revision",
        "--expected",
        imported_version,
        "--content",
        str(document),
        "draft",
    )
    assert code == 0, drafted
    revision = drafted["result"]["revision"]["revision_id"]
    assert drafted["result"]["revision"]["content"] == {"persona": "CLI authored persona"}
    drafted_version = str(drafted["result"]["persona"]["version"])

    # An unapproved revision never reaches a publication, and the refusal is explicit.
    code, refused = cli(
        "--database",
        str(path),
        "--subject",
        A,
        "--operator",
        "operator:cli",
        "--reason",
        "publish without review",
        "--expected",
        drafted_version,
        "--revision",
        revision,
        "publish",
    )
    assert code == 1
    assert refused["ok"] is False and refused["code"] == "invalid_input"

    code, approved = cli(
        "--database",
        str(path),
        "--subject",
        A,
        "--operator",
        "reviewer:cli",
        "--reason",
        "reviewed",
        "--expected",
        drafted_version,
        "--revision",
        revision,
        "approve",
    )
    assert code == 0, approved

    code, published = cli(
        "--database",
        str(path),
        "--subject",
        A,
        "--operator",
        "operator:cli",
        "--reason",
        "release",
        "--expected",
        str(approved["result"]["persona"]["version"]),
        "--revision",
        revision,
        "publish",
    )
    assert code == 0, published
    assert published["result"]["persona"]["published_revision"] == revision

    code, history = cli("--database", str(path), "--subject", A, "history")
    assert code == 0
    ids = [r["revision_id"] for r in history["result"]["revisions"]]
    assert ids[0] == before["result"]["persona"]["published_revision"]  # the seed
    assert ids[-1] == revision  # the revision this run published
    assert history["result"]["persona"]["published_revision"] == revision

    # A later command run sees the durable pointer, not a fresh in-memory state.
    store = Store(path)
    try:
        assert Personas(store, time.time).get(A)["published_revision"] == revision
    finally:
        store.close()


def test_offline_cli_replays_a_repeated_request_after_the_response_is_lost(tmp_path):
    """A real process, a real file: a resent command must not execute the write twice."""
    path = tmp_path / "retry.db"
    import_config(tmp_path, path)
    document = write_json(tmp_path / "retry.json", {"persona": "Retried persona"})
    target = ["--database", str(path)]
    request = draft_command(target, request_id="cli-retry:1", expected=2, content=document)
    code, first = cli(*request)
    assert code == 0, first
    revision = first["result"]["revision"]["revision_id"]

    # The caller never saw the answer and sends exactly the same command again.
    code, replay = cli(*request)
    assert code == 0, replay
    assert replay == first
    assert replay["result"]["revision"]["revision_id"] == revision

    store = Store(path)
    try:
        personas = Personas(store, time.time)
        assert len(personas.revisions(A)) == 2  # the seed and one draft, not two
        assert len(personas.approvals(A)) == 0
        assert len(store.list("persona_operations")) == 1
        assert personas.get(A)["version"] == first["result"]["persona"]["version"]
    finally:
        store.close()

    # The same identity carrying a different request is refused, in a second process.
    other = write_json(tmp_path / "other.json", {"persona": "Other persona"})
    code, reused = cli(*draft_command(target, request_id="cli-retry:1", expected=3, content=other))
    assert code == 1 and reused["code"] == "invalid_input"
    store = Store(path)
    try:
        assert len(Personas(store, time.time).revisions(A)) == 2
    finally:
        store.close()


def test_online_management_port_replays_and_refuses_reused_identities(tmp_path):
    """The same operation identity rules hold on the authenticated port, over real HTTP."""
    path = tmp_path / "served-retry.db"
    config = write_json(
        tmp_path / "retry-config.json",
        {
            "contracts_path": str(workspace() / "contracts/text-dialogue/v1"),
            "database_path": str(path),
            "config_version": None,
            "policy": {"silence_ms": 5000},
            "roles": {A: {"version": 1, "persona": "Served persona"}},
            "bindings": {},
            "callers": {},
            "services": {},
            "personas": {"admin_token_env": ADMIN_TOKEN_ENV},
        },
    )
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    environment = dict(os.environ)
    environment["TIANSHU_COMPANION_CONFIG"] = str(config)
    environment[ADMIN_TOKEN_ENV] = ADMIN_TOKEN
    server = subprocess.Popen(
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
        cwd=str(ROOT),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=NO_WINDOW,
    )
    try:
        wait_for_health(port, server)
        first = write_json(tmp_path / "port-one.json", {"persona": "Port persona"})
        second = write_json(tmp_path / "port-two.json", {"persona": "Other persona"})
        target = ["--url", url, "--token-env", ADMIN_TOKEN_ENV]
        request = draft_command(target, request_id="http-retry:1", expected=2, content=first)
        code, drafted = cli(*request)
        assert code == 0, drafted
        revision = drafted["result"]["revision"]["revision_id"]

        code, replay = cli(*request)
        assert code == 0, replay
        assert replay == drafted

        code, reused = cli(
            *draft_command(
                target,
                request_id="http-retry:1",
                expected=drafted["result"]["persona"]["version"],
                content=second,
            )
        )
        assert code == 1 and reused["code"] == "invalid_input"

        # The approval carries its own identity, and resending it adds no second decision.
        approved = [
            "--url",
            url,
            "--token-env",
            ADMIN_TOKEN_ENV,
            "--subject",
            A,
            "--operator",
            "reviewer:online",
            "--reason",
            "reviewed",
            "--request-id",
            "http-retry:approve",
            "--expected",
            str(drafted["result"]["persona"]["version"]),
            "--revision",
            revision,
            "approve",
        ]
        code, approval = cli(*approved)
        assert code == 0, approval
        code, again = cli(*approved)
        assert code == 0, again
        assert again == approval
    finally:
        stop(server)

    # A third interpreter owns the file and sees one draft, one approval and two identities.
    store = open_owned(path)
    try:
        personas = Personas(store, time.time)
        assert len(personas.revisions(A)) == 2
        assert len(personas.approvals(A)) == 1
        assert len(store.list("persona_operations")) == 2
    finally:
        store.close()


def test_offline_cli_refuses_to_write_while_a_service_owns_the_database(tmp_path):
    path = tmp_path / "owned.db"
    import_config(tmp_path, path)
    content = write_json(tmp_path / "content.json", {"persona": "must not land"})

    async def scenario():
        harness = Harness(path, personas=persona_config())
        try:
            code, refused = cli(
                "--database",
                str(path),
                "--subject",
                A,
                "--operator",
                "operator:cli",
                "--reason",
                "offline write",
                "--expected",
                "1",
                "--content",
                str(content),
                "draft",
            )
            assert code == 1
            assert refused["ok"] is False
            assert refused["code"] == "service_running"
            assert "remedy" in refused
            # The live service still owns the only writer, and nothing was written behind it.
            assert Personas(harness.core.store, time.time).get(A)["draft_revision"] is None
        finally:
            await harness.core.close()

    asyncio.run(scenario())


def test_offline_cli_reports_a_missing_character_without_touching_the_file(tmp_path):
    path = tmp_path / "empty.db"
    Store(path).close()
    code, unknown = cli("--database", str(path), "--subject", A, "get")
    assert code == 1
    assert unknown["code"] == "not_found"


def test_online_cli_uses_the_management_port_and_its_own_credential(tmp_path):
    path = tmp_path / "served.db"
    config = write_json(
        tmp_path / "config.json",
        {
            "contracts_path": str(workspace() / "contracts/text-dialogue/v1"),
            "database_path": str(path),
            "config_version": None,
            "policy": {"silence_ms": 5000},
            "roles": {A: {"version": 1, "persona": "Served persona"}},
            "bindings": {},
            "callers": {},
            "services": {},
            "personas": {"admin_token_env": ADMIN_TOKEN_ENV},
        },
    )
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    environment = dict(os.environ)
    environment["TIANSHU_COMPANION_CONFIG"] = str(config)
    environment[ADMIN_TOKEN_ENV] = ADMIN_TOKEN
    server = subprocess.Popen(
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
        cwd=str(ROOT),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=NO_WINDOW,
    )
    try:
        wait_for_health(port, server)
        # Without the dedicated credential the command refuses before it sends anything.
        code, refused = cli("--url", url, "--token-env", "TIANSHU_ABSENT_PERSONA_TOKEN", "list")
        assert code == 1 and refused["code"] == "management_credential_missing"

        code, listed = cli("--url", url, "--token-env", ADMIN_TOKEN_ENV, "list")
        assert code == 0, listed
        assert listed["result"]["subjects"] == [A]
        draft = write_json(tmp_path / "online.json", {"persona": "Online persona"})
        code, current = cli("--url", url, "--token-env", ADMIN_TOKEN_ENV, "--subject", A, "get")
        assert code == 0, current
        assert current["result"]["persona"]["published"]["content"] == {"persona": "Served persona"}
        version = current["result"]["persona"]["version"]
        code, drafted = cli(
            "--url",
            url,
            "--token-env",
            ADMIN_TOKEN_ENV,
            "--subject",
            A,
            "--operator",
            "operator:online",
            "--reason",
            "online authoring",
            "--expected",
            str(version),
            "--content",
            str(draft),
            "draft",
        )
        assert code == 0, drafted
        assert drafted["result"]["revision"]["content"] == {"persona": "Online persona"}
        # A stale writer is told the version moved instead of overwriting the newer draft.
        code, conflict = cli(
            "--url",
            url,
            "--token-env",
            ADMIN_TOKEN_ENV,
            "--subject",
            A,
            "--operator",
            "operator:online",
            "--reason",
            "stale writer",
            "--expected",
            str(version),
            "--content",
            str(draft),
            "draft",
        )
        assert code == 1 and conflict["code"] == "version_conflict"
    finally:
        stop(server)

    # The management port is gone, so the maintenance command owns the file again and reads
    # back exactly what the online command wrote: the two adapters share one state.
    code, afters = cli("--database", str(path), "--subject", A, "get")
    assert code == 0, afters
    persona = afters["result"]["persona"]
    assert persona["draft_revision"] is not None
    assert persona["published_revision"] is not None  # the seed; a draft is not a publication
    assert persona["draft"]["content"] == {"persona": "Online persona"}
    assert persona["published"]["content"] == {"persona": "Served persona"}


def wait_for_health(port, server):
    for _ in range(200):
        if server.poll() is not None:
            raise RuntimeError("Management server stopped before becoming available")
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/healthz", timeout=0.2
            ) as response:
                assert json.load(response)["alive"] is True
            return
        except (OSError, urllib.error.URLError):
            time.sleep(0.05)
    raise AssertionError("Management server never became available")


def stop(process):
    process.terminate()
    try:
        process.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=10)
    assert process.poll() is not None


def open_owned(path):
    """Take the owner lock after another process released it.

    The released database keeps its WAL and shared-memory files, and on Windows the OS can
    still hold a handle on them for a moment after the process is gone, so the first open
    may fail with a transient I/O error. Nothing about the database is wrong in that case;
    the retry is bounded and the failure is re-raised if it persists.
    """
    for _ in range(100):
        try:
            return Store(path)
        except sqlite3.OperationalError:
            time.sleep(0.05)
    return Store(path)


def workspace():
    context = json.loads((ROOT / ".runtime/workspace-context.json").read_text(encoding="utf-8"))
    return Path(context["workspace"])


def import_config(tmp_path, path):
    """A real file database seeded through the CLI's own import command."""
    config = write_json(
        tmp_path / "deployment.json",
        {"config_version": 4, "roles": {A: {"version": 2, "persona": "Role A"}}},
    )
    Store(path).close()
    code, imported = cli("--database", str(path), "--config", str(config), "import")
    assert code == 0, imported
    assert imported["result"]["imported"] == [A]
    return config


@pytest.fixture(autouse=True)
def _synthetic_credential():
    """One synthetic management credential, set by the test rather than inherited."""
    previous = os.environ.get(ADMIN_TOKEN_ENV)
    os.environ[ADMIN_TOKEN_ENV] = ADMIN_TOKEN
    yield
    if previous is None:
        os.environ.pop(ADMIN_TOKEN_ENV, None)
    else:
        os.environ[ADMIN_TOKEN_ENV] = previous
