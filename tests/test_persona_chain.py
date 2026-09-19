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
    """Run the installed command surface in its own interpreter.

    The command prints its answer as UTF-8 JSON, so the child is told to encode its standard
    output as UTF-8 too. A captured pipe has no console, and the platform locale would
    otherwise encode an answer that contains Chinese text in the local codepage - the answer
    would then be unreadable to any consumer that expects the JSON it documented.
    """
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(ROOT / "src"), environment.get("PYTHONPATH", "")]
    ).strip(os.pathsep)
    environment["PYTHONIOENCODING"] = "utf-8"
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


def test_the_real_management_port_serves_the_whole_browse_and_compare_chain(tmp_path):
    """One real loopback server, one synthetic management credential, the whole read chain.

    Nothing here is a mocked function boundary: every step is a separate interpreter speaking
    HTTP to a live Core built from a deployment document, and the same state is read back
    afterwards by a third interpreter that owns the file. This is the chain a future web
    surface would use, and it is also the proof that browsing writes nothing: the ledger and
    the revision count are checked at the end against the writes alone.
    """
    path = tmp_path / "browse.db"
    other = "actor:b"
    third = "actor:c"
    config = write_json(
        tmp_path / "browse-config.json",
        {
            "contracts_path": str(workspace() / "contracts/text-dialogue/v1"),
            "database_path": str(path),
            "config_version": None,
            "policy": {"silence_ms": 5000},
            "roles": {
                A: {"version": 1, "persona": "Served persona"},
                other: {"version": 1, "persona": "Second persona"},
                third: {"version": 1, "persona": "Third persona"},
            },
            "bindings": {},
            "callers": {
                "platform": {
                    "token_env": "TIANSHU_SYNTHETIC_PLATFORM_TOKEN",
                    "issuer": "synthetic-issuer",
                    "origin_service": "platform_origin",
                }
            },
            "services": {},
            "personas": {"admin_token_env": ADMIN_TOKEN_ENV},
        },
    )
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    environment = dict(os.environ)
    environment["TIANSHU_COMPANION_CONFIG"] = str(config)
    environment[ADMIN_TOKEN_ENV] = ADMIN_TOKEN
    environment["TIANSHU_SYNTHETIC_PLATFORM_TOKEN"] = "synthetic-platform-credential"
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
        target = ["--url", url, "--token-env", ADMIN_TOKEN_ENV]

        # 1. The live keyset directory, one record at a time, following its own cursor.
        seen, cursor = [], None
        while True:
            arguments = [*target, "--limit", "1", "catalog"]
            if cursor is not None:
                arguments += ["--cursor", cursor]
            code, page = cli(*arguments)
            assert code == 0, page
            assert page["result"]["consistency"] == "live_keyset"
            seen.extend(entry["subject"] for entry in page["result"]["entries"])
            cursor = page["result"]["next_cursor"]
            assert len(seen) < 10, "catalog paging did not terminate"
            if not cursor:
                break
        assert seen == [A, other, third]

        # 2. One history kind of one character, and the seed revision it names.
        code, history = cli(*target, "--subject", A, "--kind", "revisions", "history_page")
        assert code == 0, history
        assert history["result"]["count"] == 1 and history["result"]["has_more"] is False
        seed = history["result"]["entries"][0]["revision_id"]
        assert "content" not in history["result"]["entries"][0]

        # 3. The existing write chain, unchanged, over the same port.
        code, current = cli(*target, "--subject", A, "get")
        assert code == 0, current
        authored = write_json(tmp_path / "authored.json", {"persona": "温和\n简洁", "tone": "温和"})
        code, drafted = cli(
            *target,
            "--subject",
            A,
            "--operator",
            "operator:chain",
            "--reason",
            "authored over the port",
            "--request-id",
            "chain:draft",
            "--expected",
            str(current["result"]["persona"]["version"]),
            "--content",
            str(authored),
            "draft",
        )
        assert code == 0, drafted
        revision = drafted["result"]["revision"]["revision_id"]
        code, approved = cli(
            *target,
            "--subject",
            A,
            "--operator",
            "reviewer:chain",
            "--reason",
            "reviewed over the port",
            "--request-id",
            "chain:approve",
            "--expected",
            str(drafted["result"]["persona"]["version"]),
            "--revision",
            revision,
            "approve",
        )
        assert code == 0, approved
        code, published = cli(
            *target,
            "--subject",
            A,
            "--operator",
            "operator:chain",
            "--reason",
            "released over the port",
            "--request-id",
            "chain:publish",
            "--expected",
            str(approved["result"]["persona"]["version"]),
            "--revision",
            revision,
            "publish",
        )
        assert code == 0, published

        # 4. The newly published version, read on its own, and compared with the seed.
        code, one = cli(*target, "--subject", A, "--revision", revision, "revision")
        assert code == 0, one
        assert one["result"]["revision"]["content"] == {"persona": "温和\n简洁", "tone": "温和"}
        assert one["result"]["is_published"] is True and one["result"]["is_draft"] is False
        assert one["result"]["published_revision"] == revision
        code, diff = cli(*target, "--subject", A, "--left", seed, "--right", revision, "compare")
        assert code == 0, diff
        assert diff["result"]["comparison_scope"] == ["persona", "tone", "style", "address"]
        assert diff["result"]["fields"]["persona"]["change"] == "modified"
        assert diff["result"]["fields"]["persona"]["left"] == "Served persona"
        assert diff["result"]["fields"]["persona"]["right"] == "温和\n简洁"
        assert diff["result"]["fields"]["tone"]["change"] == "added"
        assert diff["result"]["content_identical"] is False
        assert diff["result"]["identical_revision"] is False

        # 5. A page cursor taken here, then a rollback: the cursor is refused, not spliced.
        code, opened = cli(
            *target, "--subject", A, "--kind", "revisions", "--limit", "1", "history_page"
        )
        assert code == 0, opened
        stale = opened["result"]["next_cursor"]
        assert stale
        code, latest = cli(*target, "--subject", A, "get")
        assert code == 0, latest
        code, rolled = cli(
            *target,
            "--subject",
            A,
            "--operator",
            "operator:chain",
            "--reason",
            "rolled back over the port",
            "--request-id",
            "chain:rollback",
            "--expected",
            str(latest["result"]["persona"]["version"]),
            "--revision",
            seed,
            "rollback",
        )
        assert code == 0, rolled
        restored = rolled["result"]["restored_revision"]
        code, refused = cli(
            *target,
            "--subject",
            A,
            "--kind",
            "revisions",
            "--limit",
            "1",
            "--cursor",
            stale,
            "history_page",
        )
        assert code == 1 and refused["code"] == "version_conflict"
        # Reopening the first page is the documented way forward, and it now shows the
        # rollback as a new revision that replays the older text.
        code, reopened = cli(
            *target, "--subject", A, "--kind", "revisions", "--limit", "1", "history_page"
        )
        assert code == 0, reopened
        code, back = cli(
            *target, "--subject", A, "--left", revision, "--right", restored, "compare"
        )
        assert code == 0, back
        assert back["result"]["fields"]["persona"]["left"] == "温和\n简洁"
        assert back["result"]["fields"]["persona"]["right"] == "Served persona"
        assert back["result"]["fields"]["persona"]["change"] == "modified"
        code, replayed = cli(
            *target, "--subject", A, "--left", seed, "--right", restored, "compare"
        )
        assert code == 0, replayed
        assert replayed["result"]["content_identical"] is True
        assert replayed["result"]["identical_revision"] is False
        assert all(
            field["change"] == "unchanged" for field in replayed["result"]["fields"].values()
        )

        # 6. Another character's revision and another character's cursor stay unreachable.
        code, foreign = cli(*target, "--subject", other, "get")
        assert code == 0, foreign
        other_seed = foreign["result"]["persona"]["published_revision"]
        code, crossed = cli(*target, "--subject", A, "--revision", other_seed, "revision")
        assert code == 1 and crossed["code"] == "invalid_input"
        code, wrong = cli(
            *target,
            "--subject",
            A,
            "--kind",
            "revisions",
            "--limit",
            "1",
            "--cursor",
            stale,
            "catalog",
        )
        assert code == 1 and wrong["code"] == "invalid_input"
    finally:
        stop(server)

    # A third interpreter owns the file and sees exactly the four writes: browsing left no
    # trace at all, and every revision the chain read is durable.
    store = open_owned(path)
    try:
        personas = Personas(store, time.time)
        assert len(personas.revisions(A)) == 3  # the seed, the release, the rollback
        assert len(personas.publications(A)) == 3
        assert len(personas.approvals(A)) == 2
        assert len(personas.rollbacks(A)) == 1
        assert len(store.list("persona_operations")) == 4  # draft, approve, publish, rollback
    finally:
        store.close()


def test_the_management_port_serves_no_browse_without_its_own_credential(tmp_path):
    """One credential opens the browse surface, and nothing else does. Real HTTP, real 401."""
    path = tmp_path / "credentials.db"
    config = write_json(
        tmp_path / "credentials-config.json",
        {
            "contracts_path": str(workspace() / "contracts/text-dialogue/v1"),
            "database_path": str(path),
            "config_version": None,
            "policy": {"silence_ms": 5000},
            "roles": {A: {"version": 1, "persona": "Served persona"}},
            "bindings": {},
            "callers": {
                "platform": {
                    "token_env": "TIANSHU_SYNTHETIC_PLATFORM_TOKEN",
                    "issuer": "synthetic-issuer",
                    "origin_service": "platform_origin",
                },
                "nonebot": {
                    "token_env": "TIANSHU_SYNTHETIC_BRIDGE_TOKEN",
                    "issuer": "synthetic-issuer",
                    "origin_service": "bridge_origin",
                },
            },
            "services": {},
            "personas": {"admin_token_env": ADMIN_TOKEN_ENV},
        },
    )
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    environment = dict(os.environ)
    environment["TIANSHU_COMPANION_CONFIG"] = str(config)
    environment[ADMIN_TOKEN_ENV] = ADMIN_TOKEN
    environment["TIANSHU_SYNTHETIC_PLATFORM_TOKEN"] = "synthetic-platform-credential"
    environment["TIANSHU_SYNTHETIC_BRIDGE_TOKEN"] = "synthetic-bridge-credential"
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
        # The host authenticates the platform and bridge credentials - they are real callers -
        # but neither is the persona-management service, so a browse is forbidden, not served.
        foreign = {
            "TIANSHU_SYNTHETIC_PLATFORM_TOKEN": "synthetic-platform-credential",
            "TIANSHU_SYNTHETIC_BRIDGE_TOKEN": "synthetic-bridge-credential",
        }
        for token_env, credential in foreign.items():
            environment_of_cli = {token_env: credential}
            code, refused = cli(
                "--url", url, "--token-env", token_env, "catalog", env=environment_of_cli
            )
            assert code == 1, refused
            assert refused["code"] == "forbidden" and refused["status"] == 403
            code, refused_write = cli(
                "--url",
                url,
                "--token-env",
                token_env,
                "--subject",
                A,
                "--operator",
                "operator:foreign",
                "--reason",
                "not my surface",
                "--expected",
                "99",
                "--content",
                str(write_json(tmp_path / "foreign.json", {"persona": "foreign"})),
                "draft",
                env=environment_of_cli,
            )
            assert code == 1 and refused_write["code"] == "forbidden"
        # No credential at all, and one that is simply wrong: unauthenticated, nothing served.
        for authorization in (None, "Bearer not-a-credential"):
            headers = {"Content-Type": "application/json"}
            if authorization:
                headers["Authorization"] = authorization
            request = urllib.request.Request(
                url + "/internal/v1/persona/manage",
                data=json.dumps({"operation": "catalog"}).encode("utf-8"),
                headers=headers,
            )
            with pytest.raises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(request, timeout=10)
            assert error.value.code == 401
            assert json.load(error.value)["code"] == "unauthorized"
        # The management credential itself still browses.
        code, served = cli("--url", url, "--token-env", ADMIN_TOKEN_ENV, "catalog")
        assert code == 0, served
        assert [entry["subject"] for entry in served["result"]["entries"]] == [A]
    finally:
        stop(server)

    # Nothing above wrote a fact: only the seed import exists, and no operation was recorded.
    store = open_owned(path)
    try:
        personas = Personas(store, time.time)
        assert len(personas.revisions(A)) == 1
        assert len(personas.approvals(A)) == 0 and len(personas.publications(A)) == 1
        assert store.list("persona_operations") == []
    finally:
        store.close()


def test_the_real_management_port_refuses_every_malformed_cursor_without_failing(tmp_path):
    """A malformed cursor is a refusal on the wire, never a server error.

    A cursor that is not ASCII used to reach `hmac` (`'中.x'` raised `UnicodeEncodeError`) or
    the signature comparison (`'abc.中'` raised `TypeError`). Neither is a refusal the domain
    can map, so the port would have answered 500 for a cursor the caller simply got wrong.
    Every shape now answers 400 with `invalid_input`, the same server keeps serving, and the
    valid cursor still continues the page it was issued for.
    """
    path = tmp_path / "cursors.db"
    config = write_json(
        tmp_path / "cursors-config.json",
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
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=NO_WINDOW,
    )
    try:
        wait_for_health(port, server)

        def post(document):
            request = urllib.request.Request(
                url + "/internal/v1/persona/manage",
                data=json.dumps(document, ensure_ascii=False).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + ADMIN_TOKEN,
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    return response.status, json.load(response)
            except urllib.error.HTTPError as error:
                return error.code, json.load(error)

        # Two authored revisions over the same port, so one history has three records.
        target = ["--url", url, "--token-env", ADMIN_TOKEN_ENV]
        for index in range(2):
            code, current = cli(*target, "--subject", A, "get")
            assert code == 0, current
            code, drafted = cli(
                *target,
                "--subject",
                A,
                "--operator",
                "operator:cursor",
                "--reason",
                "authoring before the cursor check",
                "--request-id",
                f"cursor:draft:{index}",
                "--expected",
                str(current["result"]["persona"]["version"]),
                "--content",
                str(
                    write_json(
                        tmp_path / f"cursor-{index}.json",
                        {"persona": f"合成版本 {index}", "tone": "温和"},
                    )
                ),
                "draft",
            )
            assert code == 0, drafted

        status, first = post(
            {"operation": "history_page", "subject": A, "kind": "revisions", "limit": 2}
        )
        assert status == 200, first
        valid = first["next_cursor"]
        assert valid and first["has_more"] is True
        payload, _, tag = valid.partition(".")
        assert payload and tag
        malformed = (
            "中.x",  # a non-ASCII payload used to raise inside the signer
            "abc.中",  # a non-ASCII tag used to raise inside the comparison
            payload + ".中",
            "中" * 4,
            "a\tb.c",
            "\x00.x",
            "abc",
            ".x",
            "x.",
            payload + ".tag",  # the right shape with the wrong signature
            payload + "..tag",
            payload + ".tag==",
            payload + ".ta+g",
            "x" * 3000,  # over the bounded cursor length
            12345,
            True,
            ["list"],
            {"payload": payload},
        )
        for cursor in malformed:
            status, body = post({"operation": "catalog", "limit": 5, "cursor": cursor})
            assert status == 400, (cursor, status, body)
            assert body["code"] == "invalid_input", (cursor, body)
        # A valid cursor presented to another request is refused, not reinterpreted: another
        # operation, another kind and another page size are all different requests.
        for document in (
            {"operation": "catalog", "limit": 2, "cursor": valid},
            {
                "operation": "history_page",
                "subject": A,
                "kind": "publications",
                "limit": 2,
                "cursor": valid,
            },
            {
                "operation": "history_page",
                "subject": A,
                "kind": "revisions",
                "limit": 3,
                "cursor": valid,
            },
        ):
            status, body = post(document)
            assert status == 400 and body["code"] == "invalid_input", (document, status, body)
        # Nothing above took the server down, and the cursor still continues its own page.
        assert server.poll() is None
        status, rest = post(
            {
                "operation": "history_page",
                "subject": A,
                "kind": "revisions",
                "limit": 2,
                "cursor": valid,
            }
        )
        assert status == 200, rest
        assert rest["count"] == 1 and rest["next_cursor"] is None
        assert rest["subject"] == A and rest["kind"] == "revisions"
        assert rest["consistency"] == "version_bound"
        assert rest["persona_version"] == first["persona_version"]
    finally:
        stop(server)

    store = open_owned(path)
    try:
        personas = Personas(store, time.time)
        assert len(personas.revisions(A)) == 3  # the seed and the two authored revisions
        assert len(store.list("persona_operations")) == 2  # the two drafts, and no read
    finally:
        store.close()


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
    still hold a handle on them - and on the owner lock file - for a moment after the process
    is gone, so the first open may fail with a transient I/O error or with the owner lock
    still reported as taken. Nothing about the database is wrong in that case; the retry is
    bounded (five seconds) and the failure is re-raised if it persists.
    """
    for _ in range(100):
        try:
            return Store(path)
        except (RuntimeError, sqlite3.OperationalError):
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
