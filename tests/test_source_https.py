"""Real Core process + TLS sockets; Platform/Memory are synthetic HTTP fixtures.

Set TIANSHU_TLS_PYTHON to an interpreter with cryptography solely to generate
ephemeral loopback certificates. No test certificate, key or bearer is committed.
"""

import copy
import json
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx

from support import contracts
from tianshu_companion.clients import command, uid, utc
from tianshu_companion.contracts import digest


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


@unittest.skipUnless(
    os.environ.get("TIANSHU_TLS_PYTHON"), "Explicit ephemeral TLS certificate runtime required"
)
class HttpsSourceTests(unittest.TestCase):
    def test_core_https_first_identity_fanout_facts_and_background_check(self):
        contract = contracts()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(
                [os.environ["TIANSHU_TLS_PYTHON"], "-c", CERT_PROGRAM, str(root)],
                check=True,
                capture_output=True,
                timeout=20,
            )
            verify = ssl.create_default_context(cafile=str(root / "cert.pem"))
            channel = dict(
                namespace="qq",
                binding_id="qq-fixture",
                channel_conversation_id="fixture:group",
                thread_id=None,
            )
            author = dict(namespace="qq", immutable_account_id="synthetic-only")
            physical = dict(
                message_key=dict(channel=channel, message_id="p:tls", revision=1),
                author=author,
                sent_at=utc(),
                kind="message",
                parts=[dict(kind="text", text="仅合成TLS输入")],
                reply_refs=[],
                mentioned_accounts=[],
            )
            request = dict(
                schema_version=1,
                command=command(dict(assertion_ref="input:fixture"), "tls:first", time.time(), 30),
                input=physical,
                target_actor_ids=["actor:a", "actor:b"],
            )
            tokens = {
                name: uid("synthetic-token")
                for name in ("platform", "memory", "nonebot", "ingress", "reader", "gateway")
            }
            state = dict(registered=False, contexts={}, paths=[], commits=[], checks=0)
            core_url = None

            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *args):
                    pass

                def do_POST(self):
                    body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                    state["paths"].append(self.path)
                    status = 200
                    role = (
                        "platform"
                        if self.path
                        in ("/internal/v1/source-access/read", "/internal/v1/origins/resolve")
                        else "memory"
                    )
                    if self.headers.get("Authorization") != "Bearer " + tokens[role]:
                        status, response = 401, dict(code="unauthorized")
                    elif self.path == "/internal/v1/source-access/read":
                        if body["ingest"]["input"] != physical:
                            status, response = 403, dict(code="forbidden")
                        else:
                            contexts = []
                            for actor in ("actor:a", "actor:b"):
                                ref = "origin:" + actor
                                ctx = state["contexts"].setdefault(
                                    ref,
                                    dict(
                                        issuer="platform",
                                        authenticated_service="nonebot",
                                        audience_service="companion",
                                        assertion_ref=ref,
                                        verified_account=author,
                                        verified_channel=channel,
                                        principal_id=None,
                                        revoked=False,
                                        expires_at=utc(time.time() + 30),
                                        allowed_scope=dict(
                                            actor_id=actor,
                                            person_id=None,
                                            conversation_id=None,
                                            audience="group",
                                        ),
                                    ),
                                )
                                contexts.append(ctx)
                            response = dict(
                                schema_version=1,
                                request_id=body["request_id"],
                                operation="input",
                                request_digest=digest(body),
                                ingest_digest=digest(body["ingest"]),
                                verified_account=author,
                                verified_channel=channel,
                                input_digest=digest(physical),
                                origin_ref="input:fixture",
                                expires_at=utc(time.time() + 30),
                                default_actor_ids=[],
                                routing_version=1,
                                actor_contexts=contexts,
                                audience="group",
                            )
                    elif self.path.endswith("/identity/resolve"):
                        assert body["query"]["origin"]["assertion_ref"].startswith("origin:actor:")
                        response = dict(
                            schema_version=1,
                            request_id=body["query"]["request_id"],
                            state="found" if state["registered"] else "unregistered",
                            person_id="person:tls" if state["registered"] else None,
                            binding_version=1 if state["registered"] else 0,
                        )
                    elif self.path.endswith("/identity/register"):
                        state["registered"] = True
                        response = dict(
                            schema_version=1,
                            request_id=body["command"]["request_id"],
                            person_id="person:tls",
                            binding_version=1,
                            created=True,
                        )
                    elif self.path == "/internal/v1/origins/resolve":
                        response = dict(
                            schema_version=1,
                            request_id=body["request_id"],
                            context=state["contexts"][body["assertion_ref"]],
                        )
                    elif self.path == "/internal/v1/memory/source-sync/check":
                        # A synthetic Memory checker calls the real Core facts endpoint.
                        selectors = [
                            dict(
                                key={k: v for k, v in s["message_key"].items() if k != "revision"},
                                actor_id=body["scope"]["actor_id"],
                            )
                            for s in body["sources"]
                        ]
                        with httpx.Client(verify=verify, trust_env=False) as client:
                            facts = client.post(
                                core_url + "/internal/v1/source-facts/read",
                                headers={"Authorization": "Bearer " + tokens["reader"]},
                                json=dict(
                                    schema_version=1,
                                    request_id=uid("check-read"),
                                    mode="snapshot",
                                    selectors=selectors,
                                    turn_ids=[body["turn_id"]],
                                    include_content=False,
                                ),
                            )
                        facts.raise_for_status()
                        snapshot = facts.json()
                        assert snapshot["turns"][0]["input_sources"] == body["sources"]
                        assert all(a["scope"] == body["scope"] for a in snapshot["admissions"])
                        state["checks"] += 1
                        response = dict(
                            schema_version=1,
                            request_id=body["request_id"],
                            request_digest=digest(body),
                            scope=body["scope"],
                            version_domain="text-dialogue/v1",
                            scope_version=3,
                            checked_at=utc(),
                        )
                    elif self.path == "/internal/v1/memory/turn-commits":
                        contract.check("conversation#committed_event", body)
                        state["commits"].append(body)
                        response = dict(
                            schema_version=1,
                            event_id=body["event_id"],
                            turn_id=body["aggregate_id"],
                            input_revision=body["input_revision"],
                            state="accepted",
                            candidate_job_ref=None,
                            confirmed_memory_written=False,
                        )
                    else:
                        # No model or private-memory success is fabricated. This
                        # forces the real runtime's blocked_scope repair path.
                        status, response = 503, dict(code="dependency_unavailable")
                    encoded = json.dumps(response).encode()
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(root / "cert.pem", root / "key.pem")
            server.socket = tls.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            core_url = f"https://127.0.0.1:{port}"
            service_url = f"https://127.0.0.1:{server.server_port}"
            context = json.loads(
                (Path(__file__).parents[1] / ".runtime/workspace-context.json").read_text(
                    encoding="utf-8"
                )
            )
            config = dict(
                contracts_path=str(Path(context["workspace"]) / "contracts/text-dialogue/v1"),
                database_path=str(root / "core.db"),
                config_version=1,
                policy=dict(silence_ms=0),
                roles={
                    actor: dict(version=1, persona="synthetic") for actor in ("actor:a", "actor:b")
                },
                bindings={
                    "qq-fixture": dict(
                        service="nonebot",
                        namespace="qq",
                        audience="group",
                        actor_ids=["actor:a", "actor:b"],
                        classification=dict(
                            value="real",
                            basis="registered_input_mode",
                            policy_ref="fixture:mode",
                            policy_version=1,
                        ),
                    )
                },
                callers={
                    "nonebot": dict(
                        token_env="TEST_INGRESS", issuer="platform", origin_service="platform"
                    ),
                    "memory": dict(token_env="TEST_READER"),
                },
                services={
                    name: dict(
                        url=service_url,
                        token_env="TEST_" + name.upper(),
                        ca_file=str(root / "cert.pem"),
                    )
                    for name in ("platform", "memory", "gateway", "nonebot")
                },
            )
            (root / "config.json").write_text(json.dumps(config), encoding="utf-8")
            env = {
                **os.environ,
                "TIANSHU_COMPANION_CONFIG": str(root / "config.json"),
                **{"TEST_" + name.upper(): value for name, value in tokens.items()},
            }
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
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            try:
                with httpx.Client(verify=verify, trust_env=False, timeout=3) as client:
                    for _ in range(80):
                        if process.poll() is not None:
                            raise AssertionError(
                                "Core process failed: "
                                + process.communicate()[1].decode(errors="replace")
                            )
                        try:
                            if client.get(core_url + "/healthz").status_code == 200:
                                break
                        except httpx.TransportError:
                            time.sleep(0.05)
                    result = client.post(
                        core_url + "/internal/v1/conversation/ingest-actors",
                        json=request,
                        headers={"Authorization": "Bearer " + tokens["ingress"]},
                    )
                    self.assertEqual(200, result.status_code, result.text)
                    result = result.json()
                    contract.check("sources#fanout_response", result)
                    self.assertEqual(
                        ["accepted", "accepted"], [o["state"] for o in result["outcomes"]]
                    )
                    self.assertEqual(1, state["paths"].count("/internal/v1/identity/register"))
                    for outcome in result["outcomes"]:
                        admission = outcome["admission"]
                        state["contexts"][admission["accepted_origin"]["assertion_ref"]][
                            "allowed_scope"
                        ] = copy.deepcopy(admission["scope"])
                    query = dict(
                        schema_version=1,
                        request_id=uid("snapshot"),
                        mode="snapshot",
                        selectors=[o["admission"]["selector"] for o in result["outcomes"]],
                        turn_ids=[],
                        include_content=True,
                    )
                    facts = client.post(
                        core_url + "/internal/v1/source-facts/read",
                        json=query,
                        headers={"Authorization": "Bearer " + tokens["reader"]},
                    )
                    self.assertEqual(200, facts.status_code, facts.text)
                    self.assertEqual(physical, facts.json()["physicals"][0]["content"])
                    self.assertEqual(2, len(facts.json()["admissions"]))
                    denied = client.post(
                        core_url + "/internal/v1/source-facts/read",
                        json=query,
                        headers={"Authorization": "Bearer " + tokens["ingress"]},
                    )
                    self.assertEqual(403, denied.status_code)
                    deadline = time.monotonic() + 10
                    while len(state["commits"]) < 2 and time.monotonic() < deadline:
                        time.sleep(0.05)
                    self.assertEqual(2, len(state["commits"]))
                    self.assertEqual(2, state["checks"])
                    self.assertEqual({3}, {e["scope_version"] for e in state["commits"]})
                    self.assertEqual(
                        {"actor:a", "actor:b"}, {e["scope"]["actor_id"] for e in state["commits"]}
                    )
                    print(
                        "TLS Core process: physical=1 admissions=2 identity_register=1 checks=2 commits=2; remote fixtures only; L0 unverified"
                    )
            finally:
                process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
