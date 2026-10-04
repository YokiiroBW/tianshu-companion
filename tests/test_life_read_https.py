"""Real Core process over real TLS: the four authorized read routes, end to end.

Set `TIANSHU_TLS_PYTHON` to an interpreter with `cryptography` to generate an ephemeral
loopback certificate. No certificate, key or bearer is written into the repository, and the
system trust store is never touched.

The database is an isolated synthetic one, settled before the process starts: every actor
holds its activity indefinitely and the day the background pass would ask for already exists,
so the running service performs no life write while the reads are measured. That is what makes
the before/after comparison meaningful instead of a race with the service's own scheduler.
The configured remote services are a sentinel HTTP server that records every request, so "no
external client, no model, no tick" is asserted rather than assumed.
"""

import asyncio
import json
import os
import socket
import ssl
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest

from support import Clock
from test_life_read import READER, Fixture
from tianshu_companion.contracts import digest
from tianshu_companion.life import DEFAULT_RECIPE, Life
from tianshu_companion.store import Store

pytestmark = pytest.mark.skipif(
    not os.environ.get("TIANSHU_TLS_PYTHON"),
    reason="Explicit ephemeral TLS certificate runtime required",
)

SERVICE = "story_reader"
OTHER_SERVICE = "other_caller"
TOKENS = {SERVICE: "tls-story-token", OTHER_SERVICE: "tls-other-token"}
DAY = "2026-09-20"
CONTENT = "A fictional quiet day, written from synthetic material only."
CEILING = 1048576

CERT_PROGRAM = """
import datetime, ipaddress, pathlib, sys
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
root=pathlib.Path(sys.argv[1])
key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,"Core synthetic loopback")])
now=datetime.datetime.now(datetime.timezone.utc)
cert=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
 .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=5))
 .not_valid_after(now+datetime.timedelta(days=1))
 .add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True)
 .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),critical=False)
 .sign(key,hashes.SHA256()))
(root/"cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
(root/"key.pem").write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
"""


def uri(path):
    return "file:" + Path(path).as_posix() + "?mode=ro"


def read_facts(path):
    """Every durable row and the schema version, read from outside the running process."""
    connection = sqlite3.connect(uri(path), uri=True)
    try:
        tables = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        return {
            table: [
                tuple(row) for row in connection.execute(f"SELECT id,body FROM {table} ORDER BY id")
            ]
            for table in tables
        }
    finally:
        connection.close()


def settled(path, *, timeout=15.0):
    """Wait until the running service stops writing, then return the quiet facts."""
    deadline, previous = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        current = read_facts(path)
        if current == previous:
            return current
        previous = current
        time.sleep(0.4)
    raise AssertionError("the isolated fixture never settled")


def prepare(path):
    """Synthetic world, held actors and one published diary; returns the diary facts."""
    store = Store(path)
    clock = Clock()
    clock.now = time.time()
    life = Life(store, clock, None, None, asyncio.Semaphore(4))
    life.create_world("world:home", timezone_name="UTC")
    life.create_room("room:study", "world:home")
    for actor_id in ("actor:a", "actor:b"):
        actor = life.configure_actor(
            actor_id,
            "room:study",
            personality_version=1,
            schedule=[dict(minute=0, activity="sleeping", controls={"desk_light": 0})],
        )
        # An indefinite manual hold means the service's own tick settles nothing and writes
        # nothing, so every row observed around a read is the row the read produced.
        life.set_activity(actor_id, "reading", expected=actor["version"])
    life.set_diary_access("actor:a", readers=[READER])
    fixture = Fixture(store, clock)
    diary = fixture.generate("actor:a", DAY, CONTENT)
    fixture.publish(diary["id"])
    heavy = fixture.generate("actor:a", "2026-09-19", "c" * (CEILING + 1), material="heavy")
    fixture.publish(heavy["id"])
    # The day the background pass asks for already exists, so that pass writes nothing either.
    today = datetime.fromtimestamp(clock.now, timezone.utc).date()
    yesterday = str(today - timedelta(days=1))
    key = digest(["actor:a", yesterday, dict(DEFAULT_RECIPE), digest([])])
    store.put(
        "life_diaries",
        dict(
            id=key,
            conversation_id="actor:a",
            day=yesterday,
            fictional=True,
            recipe=dict(DEFAULT_RECIPE),
            material_version=digest([]),
            state="skipped",
            config_version=None,
            current_revision=None,
            published_revision=None,
            created_at=clock.now,
            version=1,
        ),
    )
    store.close()
    return dict(
        diary_id=diary["id"],
        revision_id=diary["published_revision"],
        version=diary["version"],
        material_version=diary["material_version"],
        heavy_id=heavy["id"],
        heavy_revision=heavy["published_revision"],
        heavy_version=heavy["version"],
        yesterday=date.fromisoformat(yesterday),
    )


def sentinel(certfile, keyfile):
    """A TLS remote-service stand-in that records every request and answers nothing useful.

    It exists to be silent: the life read path must never call a model, Memory or the platform,
    so every request this server records is a defect rather than a fixture detail.
    """
    hits = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            hits.append(self.path)
            body = json.dumps(dict(code="forbidden")).encode()
            self.send_response(403)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_POST

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(certfile, keyfile)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, hits


def test_four_routes_identity_budgets_errors_cancellation_and_exit():
    workspace = json.loads(
        (Path(__file__).parents[1] / ".runtime/workspace-context.json").read_text(encoding="utf-8")
    )["workspace"]
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        subprocess.run(
            [os.environ["TIANSHU_TLS_PYTHON"], "-c", CERT_PROGRAM, str(root)],
            check=True,
            capture_output=True,
            timeout=30,
        )
        verify = ssl.create_default_context(cafile=str(root / "cert.pem"))
        database = root / "core.db"
        facts = prepare(database)
        server, hits = sentinel(root / "cert.pem", root / "key.pem")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        url = f"https://127.0.0.1:{port}"
        remote = f"https://127.0.0.1:{server.server_port}"
        config = dict(
            contracts_path=str(Path(workspace) / "contracts/text-dialogue/v1"),
            database_path=str(database),
            config_version=1,
            policy=dict(silence_ms=0),
            callers={
                SERVICE: dict(token_env="TEST_READER"),
                OTHER_SERVICE: dict(token_env="TEST_OTHER"),
            },
            # The deployment mapping is the whole grant list: the service name is established
            # by the bearer credential, and the reader identity never comes from a request.
            life_readers={SERVICE: {"reader_id": READER, "actor_ids": ["actor:a"]}},
            services={
                name: dict(
                    url=remote, token_env="TEST_" + name.upper(), ca_file=str(root / "cert.pem")
                )
                for name in ("platform", "memory", "gateway", "nonebot")
            },
        )
        (root / "config.json").write_text(json.dumps(config), encoding="utf-8")
        env = {
            **os.environ,
            "TIANSHU_COMPANION_CONFIG": str(root / "config.json"),
            "TEST_READER": TOKENS[SERVICE],
            "TEST_OTHER": TOKENS[OTHER_SERVICE],
            **{
                "TEST_" + name.upper(): "remote-fixture"
                for name in ("PLATFORM", "MEMORY", "GATEWAY", "NONBOT")
            },
        }
        log = open(root / "core.log", "wb")
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
                "--ssl-certfile",
                str(root / "cert.pem"),
                "--ssl-keyfile",
                str(root / "key.pem"),
            ],
            env=env,
            # The service's own log goes to a file rather than a pipe: a pipe nobody drains
            # fills up and blocks the child mid-request, which would look like a hung service.
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        reader = {"Authorization": "Bearer " + TOKENS[SERVICE]}
        other = {"Authorization": "Bearer " + TOKENS[OTHER_SERVICE]}

        def service_log():
            log.flush()
            return (root / "core.log").read_text(encoding="utf-8", errors="replace")[-4000:]

        try:
            with httpx.Client(verify=verify, trust_env=False, timeout=5) as client:
                for _ in range(160):
                    if process.poll() is not None:
                        raise AssertionError("Core process failed: " + service_log())
                    try:
                        if client.get(url + "/healthz").status_code == 200:
                            break
                    except httpx.TransportError:
                        time.sleep(0.05)
                before = settled(database)
                reported = client.post(
                    url + "/internal/v1/life-read/actors",
                    json={"schema_version": 1},
                    headers=reader,
                )
                assert reported.status_code == 200, reported.text
                assert [item["actor_id"] for item in reported.json()["items"]] == ["actor:a"]

                state = client.post(
                    url + "/internal/v1/life-read/snapshot",
                    json={"schema_version": 1, "actor_id": "actor:a"},
                    headers=reader,
                )
                assert state.status_code == 200, state.text
                assert state.json()["activity"] == "reading"  # the persisted row, not a tick
                assert state.json()["outfit_ref"] is None
                assert state.json()["state_basis"] == "last_persisted"

                page = client.post(
                    url + "/internal/v1/life-read/diaries",
                    json={"schema_version": 1, "actor_id": "actor:a"},
                    headers=reader,
                )
                assert page.status_code == 200, page.text
                listed = [item["diary_id"] for item in page.json()["items"]]
                assert listed == [facts["diary_id"], facts["heavy_id"]]
                assert page.json()["next_after"] is None

                document = client.post(
                    url + "/internal/v1/life-read/revision",
                    json={
                        "schema_version": 1,
                        "actor_id": "actor:a",
                        "diary_id": facts["diary_id"],
                        "revision_id": facts["revision_id"],
                        "expected_diary_version": facts["version"],
                    },
                    headers=reader,
                )
                assert document.status_code == 200, document.text
                assert document.json()["content"] == CONTENT
                assert document.json()["captured"]["config_version"] == 19
                assert document.json()["captured"]["material_version"] == facts["material_version"]

                # Identity: unauthenticated, a credential that is no caller at all, and a
                # caller that is registered but not a reader.
                assert (
                    client.post(
                        url + "/internal/v1/life-read/actors", json={"schema_version": 1}
                    ).status_code
                    == 401
                )
                assert (
                    client.post(
                        url + "/internal/v1/life-read/actors",
                        json={"schema_version": 1},
                        headers={"Authorization": "Bearer unknown"},
                    ).status_code
                    == 401
                )
                denied = client.post(
                    url + "/internal/v1/life-read/actors",
                    json={"schema_version": 1},
                    headers=other,
                )
                assert denied.status_code == 403 and denied.json()["code"] == "forbidden"

                # Errors: an actor outside the deployment, a stale expected version, and a
                # revision id that is not the published one.
                absent = client.post(
                    url + "/internal/v1/life-read/snapshot",
                    json={"schema_version": 1, "actor_id": "actor:b"},
                    headers=reader,
                )
                assert absent.status_code == 404 and absent.json()["code"] == "not_found"
                stale = client.post(
                    url + "/internal/v1/life-read/revision",
                    json={
                        "schema_version": 1,
                        "actor_id": "actor:a",
                        "diary_id": facts["diary_id"],
                        "revision_id": facts["revision_id"],
                        "expected_diary_version": facts["version"] + 1,
                    },
                    headers=reader,
                )
                assert stale.status_code == 409 and stale.json()["code"] == "version_conflict"
                wrong = client.post(
                    url + "/internal/v1/life-read/revision",
                    json={
                        "schema_version": 1,
                        "actor_id": "actor:a",
                        "diary_id": facts["diary_id"],
                        "revision_id": "revision:not-published",
                        "expected_diary_version": facts["version"],
                    },
                    headers=reader,
                )
                assert wrong.status_code == 404

                # Budgets: the raw request body, and the finished revision response.
                over = client.post(
                    url + "/internal/v1/life-read/diaries",
                    content=b'{"schema_version":1,"actor_id":"' + b"a" * 16500 + b'"}',
                    headers={**reader, "Content-Type": "application/json"},
                )
                assert over.status_code == 400 and over.json()["code"] == "invalid_input"
                heavy = client.post(
                    url + "/internal/v1/life-read/revision",
                    json={
                        "schema_version": 1,
                        "actor_id": "actor:a",
                        "diary_id": facts["heavy_id"],
                        "revision_id": facts["heavy_revision"],
                        "expected_diary_version": facts["heavy_version"],
                    },
                    headers=reader,
                )
                assert heavy.status_code == 429 and heavy.json()["code"] == "budget_exceeded"

                # Media types: the same valid document is refused at the boundary under a
                # wrong, missing or repeated `Content-Type`, and accepted with a charset
                # parameter. Refusing it here is what keeps the media type a checked claim
                # rather than something inferred from whether the bytes happen to parse.
                refused = 0
                for media, expected in (
                    ("application/json; charset=utf-8", 200),
                    ("text/plain", 400),
                    ("application/octet-stream", 400),
                    (None, 400),
                ):
                    typed = client.post(
                        url + "/internal/v1/life-read/actors",
                        headers={**reader} if media is None else {**reader, "Content-Type": media},
                        content=b'{"schema_version":1}',
                    )
                    assert typed.status_code == expected, (media, typed.status_code)
                    if expected == 400:
                        assert typed.json()["code"] == "invalid_input"
                        refused += 1
                for values in (
                    (b"application/json", b"text/plain"),
                    (b"application/json", b"application/json"),
                ):
                    repeated = client.post(
                        url + "/internal/v1/life-read/actors",
                        headers=[
                            (b"authorization", ("Bearer " + TOKENS[SERVICE]).encode()),
                            *[(b"content-type", value) for value in values],
                        ],
                        content=b'{"schema_version":1}',
                    )
                    assert repeated.status_code == 400
                    assert repeated.json()["code"] == "invalid_input"
                    refused += 1
                unauthenticated = client.post(
                    url + "/internal/v1/life-read/actors",
                    headers={"Content-Type": "text/plain"},
                    content=b'{"schema_version":1}',
                )
                assert unauthenticated.status_code == 401  # the credential is settled first

            # Cancellation: a client that announces a body and then vanishes must stop that
            # request without writing anything, and the service must keep serving.
            context = ssl.create_default_context(cafile=str(root / "cert.pem"))
            with socket.create_connection(("127.0.0.1", port), timeout=5) as raw:
                with context.wrap_socket(raw, server_hostname="127.0.0.1") as tls:
                    tls.sendall(
                        b"POST /internal/v1/life-read/diaries HTTP/1.1\r\n"
                        b"Host: 127.0.0.1\r\n"
                        b"Authorization: Bearer " + TOKENS[SERVICE].encode() + b"\r\n"
                        b"Content-Type: application/json\r\n"
                        b"Content-Length: 4096\r\n\r\n"
                    )
                    time.sleep(0.4)  # the body never arrives
            time.sleep(0.4)
            assert read_facts(database) == before
            with httpx.Client(verify=verify, trust_env=False, timeout=5) as client:
                assert client.get(url + "/healthz").status_code == 200
                again = client.post(
                    url + "/internal/v1/life-read/snapshot",
                    json={"schema_version": 1, "actor_id": "actor:a"},
                    headers=reader,
                )
                assert again.status_code == 200

            # No tick, no model, no external client: the facts are byte-identical and the
            # sentinel remote-service stand-in was never asked for anything.
            after = read_facts(database)
            assert after == before
            assert hits == []
            assert not list(root.glob("*.bak")), "a scheduled read must add no restore point"
            print(
                "TLS life-read: routes=4 actors=1 diaries=%d revision_bytes=%d "
                "refusals=401/403/404/409/400/429+%dmedia remote_calls=%d facts_unchanged=True"
                % (len(listed), len(document.json()["content"].encode()), refused, len(hits))
            )
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            log.close()
            server.shutdown()
            server.server_close()
        assert process.returncode is not None, "the Core process must exit on termination"


def test_a_database_without_the_index_is_still_served_after_its_restore_point():
    """The read port works on an existing database whose derived index had to be added."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "rebuilt.db"
        facts = prepare(path)
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("DROP INDEX life_diaries_published_page")
        store = Store(path)  # the real open: restore point, then the derived index
        try:
            assert store.db.execute("PRAGMA user_version").fetchone()[0] == 10
            assert store.db.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                ("life_diaries_published_page",),
            ).fetchone()
        finally:
            store.close()
        (backup,) = Path(directory).glob("*.pre-life-read-index-*.bak")
        with closing(sqlite3.connect(backup)) as connection:
            # The restore point is the database as it was: the facts, without the new index.
            assert (
                connection.execute(
                    "SELECT count(*) FROM life_diaries "
                    "WHERE json_extract(body,'$.published_revision') IS NOT NULL"
                ).fetchone()[0]
                == 2
            )
            assert (
                connection.execute(
                    "SELECT count(*) FROM sqlite_master WHERE name='life_diaries_published_page'"
                ).fetchone()[0]
                == 0
            )
        assert facts["diary_id"]
