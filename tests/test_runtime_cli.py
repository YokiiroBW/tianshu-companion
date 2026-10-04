"""The runtime entry point: explicit paths, a refused unsafe binding, and one owner released.

The refusal rules are checked without opening a socket, by handing `main` a server factory that
records whether it was ever called: a binding that must be refused has to be refused *before*
anything listens, and a fake factory is how that ordering is proven rather than assumed.

The owner lock is checked for real, in a separate interpreter: the CLI starts a loopback
server, the test refuses to take the lock while it runs, asks the process to stop, and then
takes the lock itself. A lock that is released only by a crash is not released.

The container health check is exercised the same way, against a running runtime and against a
dead port, because its only job is to tell those two apart without writing anything.

Everything runs on loopback with synthetic paths in a temporary directory. No container image is
built, no deployment happens, and no external service or real credential is involved.
"""

import asyncio
import contextlib
import importlib.util
import io
import json
import os
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from tianshu_companion import app as app_module
from tianshu_companion import observability as obs
from tianshu_companion import runtime_cli
from tianshu_companion.app import create_app
from tianshu_companion.store import Store

ROOT = Path(__file__).resolve().parents[1]
NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
NEW_PROCESS_GROUP = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0


def _load_healthcheck():
    """Import the container health check by path: it is a script, not part of the package."""
    path = ROOT / "scripts" / "container_healthcheck.py"
    spec = importlib.util.spec_from_file_location("tianshu_container_healthcheck", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _LiveAnswer:
    """A urlopen stand-in that answers the documented liveness payload."""

    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *arguments):
        return False

    def read(self):
        return json.dumps({"status": "alive"}).encode()


class _Recorder:
    """A server stand-in that records whether it was asked to serve and how it was built."""

    def __init__(self, should_exit_after=None):
        self.served = 0
        self.config = None
        self.should_exit = False
        self.should_exit_after = should_exit_after

    def __call__(self, config):
        self.config = config
        return self

    async def serve(self):
        self.served += 1
        if self.should_exit_after is not None:
            self.should_exit = True


class _RangeTests(unittest.TestCase):
    """The address rules are pure functions, so they are checked directly and exhaustively."""

    def test_loopback_is_recognised_by_address_not_by_spelling(self):
        for host in ("127.0.0.1", "127.1.2.3", "::1", "[::1]", "localhost", "LOCALHOST"):
            self.assertTrue(runtime_cli.is_loopback(host), host)
        for host in ("0.0.0.0", "192.168.31.210", "10.0.0.5", "::", "example.invalid", "", "  "):
            self.assertFalse(runtime_cli.is_loopback(host), host)

    def test_a_port_outside_the_range_is_refused(self):
        for port in (0, -1, 65536, 100000, True, "8765", None):
            self.assertEqual(
                "port_out_of_range",
                runtime_cli.bind_refusal("127.0.0.1", port, None, None),
                port,
            )

    def test_a_missing_host_is_refused(self):
        self.assertEqual("host_missing", runtime_cli.bind_refusal("", 8765, None, None))


class _BindingTests(unittest.TestCase):
    """Transport protection is not optional outside loopback, and it cannot be switched off."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="tianshu-runtime-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.certificate = self.root / "server.crt"
        self.key = self.root / "server.key"
        self.certificate.write_text("-----BEGIN CERTIFICATE-----\n", encoding="utf-8")
        self.key.write_text("-----BEGIN PRIVATE KEY-----\n", encoding="utf-8")

    def test_loopback_needs_no_tls(self):
        self.assertIsNone(runtime_cli.bind_refusal("127.0.0.1", 8765, None, None))
        self.assertIsNone(runtime_cli.bind_refusal("::1", 8765, None, None))

    def test_a_remote_bind_without_tls_material_is_refused(self):
        for certificate, key in (
            (None, None),
            (str(self.certificate), None),
            (None, str(self.key)),
        ):
            self.assertEqual(
                "tls_required_for_non_loopback",
                runtime_cli.bind_refusal("0.0.0.0", 8765, certificate, key),
            )

    def test_tls_material_that_is_missing_empty_or_relative_is_refused(self):
        relative = self.root / "relative.crt"
        relative.write_text("x", encoding="utf-8")
        empty = self.root / "empty.crt"
        empty.write_text("", encoding="utf-8")
        for certificate, key in (
            (str(self.root / "absent.crt"), str(self.key)),
            (str(empty), str(self.key)),
            ("relative.crt", str(self.key)),
            (str(self.certificate), str(empty)),
        ):
            self.assertEqual(
                "tls_material_unreadable",
                runtime_cli.bind_refusal("0.0.0.0", 8765, certificate, key),
            )

    def test_a_remote_bind_with_a_complete_pair_is_allowed(self):
        self.assertIsNone(
            runtime_cli.bind_refusal("0.0.0.0", 8765, str(self.certificate), str(self.key))
        )


class _ResolutionTests(unittest.TestCase):
    """Paths are explicit, then from their documented variable, then the platform default."""

    def parse(self, *argv):
        return runtime_cli.build_parser().parse_args(list(argv))

    def test_the_development_defaults_are_obviously_development_paths(self):
        paths = runtime_cli.resolve(self.parse(), environ={}, platform="win32")
        self.assertEqual(Path(".runtime/companion.json"), paths.config)
        self.assertIsNone(paths.log_dir)
        self.assertEqual("127.0.0.1", paths.host)
        self.assertEqual(8765, paths.port)
        self.assertTrue(paths.loopback)

    def test_the_container_defaults_are_the_documented_mounts(self):
        paths = runtime_cli.resolve(self.parse(), environ={}, platform="linux")
        self.assertEqual(Path("/config/companion.json"), paths.config)
        self.assertEqual(Path("/contracts"), paths.contracts)
        self.assertEqual(Path("/data/companion.db"), paths.database)
        self.assertEqual(Path("/var/log/tianshu"), paths.log_dir)
        self.assertTrue(paths.loopback)

    def test_a_remote_container_binding_is_not_treated_as_loopback(self):
        paths = runtime_cli.resolve(self.parse("--host", "0.0.0.0"), environ={}, platform="linux")
        self.assertFalse(paths.loopback)
        self.assertEqual(
            1,
            runtime_cli.bind_refusal(paths.host, paths.port, None, None)
            == "tls_required_for_non_loopback",
        )

    def test_an_explicit_argument_wins_over_the_environment(self):
        paths = runtime_cli.resolve(
            self.parse("--database", "/tmp/one.db"),
            environ={"TIANSHU_COMPANION_DATABASE": "/tmp/two.db"},
            platform="linux",
        )
        self.assertEqual(Path("/tmp/one.db"), paths.database)

    def test_an_explicit_port_of_zero_is_kept_and_refused_not_replaced(self):
        """`--port 0` is a value the operator typed, and it is out of range.

        Treating a falsy port as "unset" would silently bind the default 8765 instead, which is
        how a deployment ends up listening somewhere nobody asked for.
        """
        paths = runtime_cli.resolve(self.parse("--port", "0"), environ={}, platform="linux")
        self.assertEqual(0, paths.port)
        self.assertEqual(
            "port_out_of_range", runtime_cli.bind_refusal(paths.host, paths.port, None, None)
        )
        recorder = _Recorder()
        self.assertEqual(
            2,
            runtime_cli.main(
                ["--port", "0"], environ={}, platform="linux", server_factory=recorder
            ),
        )
        self.assertEqual(0, recorder.served)
        # An omitted port still resolves to the documented default.
        self.assertEqual(
            runtime_cli.DEFAULT_PORT,
            runtime_cli.resolve(self.parse(), environ={}, platform="linux").port,
        )

    def test_the_environment_wins_over_the_platform_default(self):
        paths = runtime_cli.resolve(
            self.parse(),
            environ={"TIANSHU_LOG_DIR": "/tmp/logs", "TIANSHU_CONTRACTS": "/tmp/contracts"},
            platform="linux",
        )
        self.assertEqual(Path("/tmp/logs"), paths.log_dir)
        self.assertEqual(Path("/tmp/contracts"), paths.contracts)

    def test_the_summary_never_contains_a_credential(self):
        paths = runtime_cli.RuntimePaths(
            Path("/config/companion.json"),
            Path("/contracts"),
            Path("/data/companion.db"),
            Path("/var/log/tianshu"),
            "0.0.0.0",
            8765,
            "/tls/server.crt",
            "/tls/server.key",
        )
        summary = paths.summary()
        self.assertEqual(
            {"config", "contracts", "database", "log_dir", "host", "port", "tls", "workers"},
            set(summary),
        )
        self.assertEqual(1, summary["workers"])
        self.assertTrue(summary["tls"])
        text = json.dumps(summary)
        for value in ("token", "secret", "password", "authorization", "bearer"):
            self.assertNotIn(value, text.lower())

    def test_the_environment_handed_to_the_factory_keeps_one_entry_point(self):
        paths = runtime_cli.resolve(self.parse(), environ={}, platform="linux")
        environment = runtime_cli.environment_for(paths, environ={"PATH": "x"})
        # Compared as paths, because the resolved value is native to the machine running this.
        self.assertEqual(paths.config, Path(environment["TIANSHU_COMPANION_CONFIG"]))
        self.assertEqual(paths.contracts, Path(environment["TIANSHU_CONTRACTS"]))
        self.assertEqual(paths.database, Path(environment["TIANSHU_COMPANION_DATABASE"]))
        self.assertEqual(paths.log_dir, Path(environment["TIANSHU_LOG_DIR"]))
        self.assertEqual("x", environment["PATH"])
        # The development default has no log directory at all, and no empty variable is left
        # behind for the adapter to mistake for a configured one.
        development = runtime_cli.environment_for(
            runtime_cli.resolve(self.parse(), environ={}, platform="win32"), environ={}
        )
        self.assertNotIn("TIANSHU_LOG_DIR", development)

    def test_workers_are_fixed_and_not_configurable(self):
        self.assertEqual(1, runtime_cli.FIXED_WORKERS)
        with self.assertRaises(SystemExit):
            self.parse("--workers", "4")


class _MainTests(unittest.TestCase):
    """`main` refuses before listening, prints without listening, and starts exactly once."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="tianshu-runtime-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def test_a_remote_binding_without_tls_exits_two_and_never_builds_a_server(self):
        recorder = _Recorder()
        code = runtime_cli.main(
            ["--host", "0.0.0.0", "--port", "8765"],
            environ={},
            platform="linux",
            server_factory=recorder,
        )
        self.assertEqual(2, code)
        self.assertEqual(0, recorder.served)
        self.assertIsNone(recorder.config)

    def test_a_port_out_of_range_exits_two_and_never_builds_a_server(self):
        recorder = _Recorder()
        code = runtime_cli.main(
            ["--port", "70000"], environ={}, platform="linux", server_factory=recorder
        )
        self.assertEqual(2, code)
        self.assertEqual(0, recorder.served)

    def test_print_config_writes_the_paths_and_never_builds_a_server(self):
        recorder = _Recorder()
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            code = runtime_cli.main(
                ["--print-config", "--database", str(self.root / "companion.db")],
                environ={},
                platform="linux",
                server_factory=recorder,
            )
        self.assertEqual(0, code)
        document = json.loads(captured.getvalue())
        self.assertEqual(str(self.root / "companion.db"), document["database"])
        self.assertEqual(str(Path("/var/log/tianshu")), document["log_dir"])
        self.assertEqual("127.0.0.1", document["host"])
        self.assertEqual(8765, document["port"])
        self.assertFalse(document["tls"])
        self.assertEqual(1, document["workers"])
        self.assertEqual(0, recorder.served)
        self.assertIsNone(recorder.config)

    def test_a_missing_configuration_document_exits_two_and_never_builds_a_server(self):
        recorder = _Recorder()
        code = runtime_cli.main(
            ["--config", str(self.root / "absent.json")],
            environ={},
            platform="linux",
            server_factory=recorder,
        )
        self.assertEqual(2, code)
        self.assertEqual(0, recorder.served)

    def test_a_loopback_binding_builds_the_server_with_the_fixed_settings(self):
        config = self.root / "companion.json"
        config.write_text(
            json.dumps({"roles": {}, "bindings": {}, "services": {}}), encoding="utf-8"
        )
        recorder = _Recorder(should_exit_after=True)
        code = runtime_cli.main(
            ["--config", str(config), "--port", "8765"],
            environ={},
            platform="linux",
            server_factory=recorder,
        )
        self.assertEqual(0, code)
        self.assertEqual(1, recorder.served)
        self.assertEqual(1, recorder.config.workers)
        self.assertEqual("127.0.0.1", recorder.config.host)
        self.assertEqual(8765, recorder.config.port)
        self.assertIsNone(recorder.config.ssl_certfile)
        self.assertFalse(recorder.config.access_log)


class _DeploymentPathTests(unittest.TestCase):
    """The explicit deployment paths reach the real runtime, not just the environment.

    A `--database` flag that only sets a variable nothing reads is worse than no flag: the
    operator believes the process opened the mounted volume while it quietly opened whatever a
    stale configuration document still named. These cases drive the real assembly point
    (`create_app` -> `build_runtime` -> `Store`/`Contracts`) with synthetic paths in a temporary
    directory, and check which path was actually used and which was left untouched.
    """

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(
            prefix="tianshu-deploy-", ignore_cleanup_errors=True
        )
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.stale_database = self.root / "stale.db"
        self.deployed_database = self.root / "mounted" / "companion.db"
        # A deployed pack keeps the published layout: the manifest hashes its own files by
        # their path relative to the contracts root, so the copy must be the whole tree.
        self.deployed_root = self.root / "mounted-contracts"
        self.deployed_contracts = self.deployed_root / "text-dialogue" / "v1"
        shutil.copytree(_contracts_path().parents[1], self.deployed_root)
        self.config = self.root / "companion.json"
        # The document names the *old* paths: the deployment variables must win over it.
        self.config.write_text(
            json.dumps(
                {
                    "contracts_path": str(_contracts_path()),
                    "database_path": str(self.stale_database),
                    "config_version": None,
                    "policy": {"silence_ms": 5000},
                    "roles": {},
                    "bindings": {},
                    "callers": {},
                    "services": {},
                }
            ),
            encoding="utf-8",
        )
        self.environment = {
            "TIANSHU_COMPANION_CONFIG": str(self.config),
            "TIANSHU_COMPANION_DATABASE": str(self.deployed_database),
            "TIANSHU_CONTRACTS": str(self.deployed_contracts),
        }
        self.addCleanup(self._close)

    def _close(self):
        if getattr(self, "core", None) is not None:
            asyncio.run(self.core.close())

    def build(self, environment=None):
        """Assemble the real application factory under exactly the given deployment variables.

        Every `TIANSHU_*` variable of the ambient environment is dropped first, so "which path
        was used" is decided by the variables under test rather than by whatever this machine
        happens to export - while the platform's own variables (certificate stores, and the
        like) are left alone.
        """
        wanted = self.environment if environment is None else environment
        cleaned = {
            key: value for key, value in os.environ.items() if not key.startswith("TIANSHU_")
        }
        cleaned.update(wanted)
        with mock.patch.dict(os.environ, cleaned, clear=True):
            app = create_app()
        self.core = app.state.core
        return app

    def opened_database(self):
        """The file the store actually opened, read from SQLite rather than from a flag."""
        return Path(self.core.store.db.execute("PRAGMA database_list").fetchone()[2])

    def test_the_deployment_database_and_contracts_are_the_ones_actually_used(self):
        app = self.build()
        # The real store opened the deployed path, and it is a real database file.
        self.assertEqual(self.deployed_database.resolve(), self.opened_database())
        self.assertTrue(self.deployed_database.is_file())
        self.assertEqual(10, self.core.store.db.execute("PRAGMA user_version").fetchone()[0])
        # The path the document named was never touched: no file, no owner lock, no side file.
        self.assertFalse(self.stale_database.exists())
        self.assertEqual([], sorted(self.root.glob("stale.db*")))
        # The contract pack that was loaded is the deployed one, byte for byte.
        self.assertEqual(
            (self.deployed_contracts / "manifest.json").read_bytes(),
            (_contracts_path() / "manifest.json").read_bytes(),
        )
        self.assertIsNotNone(app.state.log)

    def test_an_explicit_path_wins_over_the_environment_and_the_document(self):
        """Explicit argument, then its variable, then the document: the documented order."""
        explicit_database = self.root / "explicit" / "explicit.db"
        paths = runtime_cli.resolve(
            runtime_cli.build_parser().parse_args(
                [
                    "--config",
                    str(self.config),
                    "--database",
                    str(explicit_database),
                    "--contracts",
                    str(self.deployed_contracts),
                ]
            ),
            environ={"TIANSHU_COMPANION_DATABASE": str(self.root / "from-env.db")},
            platform="win32",
        )
        self.assertEqual(explicit_database, paths.database)
        self.assertEqual(self.deployed_contracts, paths.contracts)
        # The CLI's own environment handed to the factory carries those same resolved paths.
        environment = runtime_cli.environment_for(paths, environ=dict(self.environment))
        self.build(environment)
        self.assertEqual(explicit_database.resolve(), self.opened_database())
        self.assertTrue(explicit_database.is_file())
        self.assertFalse((self.root / "from-env.db").exists())
        self.assertFalse(self.stale_database.exists())

    def test_a_document_with_no_deployment_variables_still_works(self):
        """The factory stays compatible: no override, no change in behaviour."""
        self.build({"TIANSHU_COMPANION_CONFIG": str(self.config)})
        self.assertEqual(self.stale_database.resolve(), self.opened_database())
        self.assertTrue(self.stale_database.is_file())
        self.assertFalse(self.deployed_database.exists())

    def test_the_override_never_mutates_the_document_the_caller_loaded(self):
        document = {"contracts_path": "old-contracts", "database_path": "old-database"}
        resolved = app_module.apply_deployment_overrides(
            document,
            {
                "TIANSHU_CONTRACTS": "new-contracts",
                "TIANSHU_COMPANION_DATABASE": "new-database",
            },
        )
        self.assertEqual("new-contracts", resolved["contracts_path"])
        self.assertEqual("new-database", resolved["database_path"])
        # The caller's own mapping is untouched, and an absent variable changes nothing.
        self.assertEqual(
            {"contracts_path": "old-contracts", "database_path": "old-database"}, document
        )
        self.assertEqual(document, app_module.apply_deployment_overrides(document, {"PATH": "x"}))


class _ProcessTests(unittest.TestCase):
    """The owner lock is held while the process runs and released by a termination signal."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(
            prefix="tianshu-runtime-", ignore_cleanup_errors=True
        )
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.database = self.root / "companion.db"
        self.logs = self.root / "logs"
        self.config = self.root / "companion.json"
        self.config.write_text(
            json.dumps(
                {
                    "contracts_path": str(self.contracts()),
                    "database_path": str(self.database),
                    "config_version": None,
                    "policy": {"silence_ms": 5000},
                    "roles": {},
                    "bindings": {},
                    "callers": {},
                    "services": {},
                }
            ),
            encoding="utf-8",
        )
        self.environment = dict(os.environ)
        self.environment["TIANSHU_LOG_DIR"] = str(self.logs)

    def contracts(self):
        """The published contract pack this checkout validates against."""
        return _contracts_path()

    def start(self):
        port = _free_port()
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tianshu_companion.runtime_cli",
                "--config",
                str(self.config),
                "--contracts",
                str(self.contracts()),
                "--database",
                str(self.database),
                "--log-dir",
                str(self.logs),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=str(ROOT),
            env=self.environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=NEW_PROCESS_GROUP,
        )
        self.addCleanup(lambda: process.poll() is None and process.kill())
        return process, port

    def stop(self, process, *, expect=(0, 3)):
        """Ask the running process to shut down, the way its platform delivers a request to.

        On POSIX that is SIGTERM. On Windows SIGTERM cannot be delivered to another process at
        all - `terminate` would hard-kill it, which would prove nothing about a graceful
        shutdown - so the console control event a service manager sends is used instead.

        The accepted exit codes are "finished normally" and the code the server uses for "a
        termination signal was received", which is an orderly stop, not a crash.
        """
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            self.fail("the runtime did not stop when asked")
        self.assertIn(process.returncode, expect)

    def reachable(self, process, port):
        """Wait for the port, and report what the process said if it never opens."""
        if _wait_for_port(port, process):
            return True
        if process.poll() is None:
            process.kill()
        out, err = process.communicate(timeout=30)
        self.fail(
            f"the runtime never became reachable: returncode={process.returncode} "
            f"stdout={out.decode('utf-8', 'replace')[-800:]} "
            f"stderr={err.decode('utf-8', 'replace')[-2000:]}"
        )

    def test_the_process_takes_the_owner_lock_and_a_request_to_stop_releases_it(self):
        process, port = self.start()
        self.reachable(process, port)
        # While it runs, a second opener must be refused: one owner, enforced for real.
        with self.assertRaises(RuntimeError):
            Store(self.database).close()
        self.stop(process)
        # A released lock is one another process can take, which is what a restart does.
        store = Store(self.database)
        self.addCleanup(store.close)

    def test_actual_startup_exposes_disabled_candidates_and_unintegrated_chat_audit(self):
        import urllib.request

        config = json.loads(self.config.read_text(encoding="utf-8"))
        config["automatic_memory_candidates"] = False
        self.config.write_text(json.dumps(config), encoding="utf-8")
        self.environment["TIANSHU_DIAGNOSTICS_TOKEN"] = "synthetic-diagnostics"
        for _ in range(2):
            process, port = self.start()
            self.reachable(process, port)
            request = urllib.request.Request(
                f"http://127.0.0.1:{port}/internal/v1/runtime/capabilities",
                headers={"Authorization": "Bearer synthetic-diagnostics"},
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                self.assertEqual(200, response.status)
                state = json.load(response)
            self.assertFalse(state["automatic_memory_candidates"]["enabled"])
            self.assertEqual("disabled", state["automatic_memory_candidates"]["generation"])
            self.assertEqual("paused", state["automatic_memory_candidates"]["submission"])
            self.assertEqual({"enabled": False, "state": "not_integrated"}, state["chat_audit"])
            self.stop(process)

    def test_shutdown_is_an_orderly_transition_not_a_crash(self):
        process, port = self.start()
        self.reachable(process, port)
        self.stop(process)
        records = [
            json.loads(line)
            for path in sorted(self.logs.iterdir())
            for line in path.read_bytes().splitlines()
            if line.strip()
        ]
        events = [record["event"] for record in records]
        self.assertIn("runtime.started", events)
        self.assertIn("runtime.stopping", events)
        self.assertIn("runtime.stopped", events)
        self.assertLess(events.index("runtime.stopping"), events.index("runtime.stopped"))
        # The stream is the contract's, end to end, on a real process.
        for record in records:
            self.assertEqual("companion", record["service"])
            self.assertEqual("1.0.0", record["schema_version"])
            self.assertEqual(
                {
                    "schema_version",
                    "timestamp",
                    "service",
                    "instance_id",
                    "sequence",
                    "event_id",
                    "level",
                    "event",
                    "outcome",
                    "correlation_id",
                    "duration_ms",
                    "error_code",
                },
                set(record),
            )

    def test_a_second_process_on_the_same_database_refuses_to_start(self):
        process, port = self.start()
        self.reachable(process, port)
        second = subprocess.run(
            [
                sys.executable,
                "-m",
                "tianshu_companion.runtime_cli",
                "--config",
                str(self.config),
                "--contracts",
                str(self.contracts()),
                "--database",
                str(self.database),
                "--log-dir",
                str(self.logs),
            ],
            cwd=str(ROOT),
            env=self.environment,
            capture_output=True,
            timeout=60,
            creationflags=NO_WINDOW,
        )
        self.stop(process)
        self.assertNotEqual(0, second.returncode)

    def test_a_real_process_never_writes_a_caller_supplied_value_anywhere(self):
        """The canary travels through a real process: headers, a body, and the query string.

        Everything the process writes is inspected - the whole event stream and both of its
        standard streams - because "the record is closed" is only worth something if the real
        deployment also keeps it that way.
        """
        canary = "CANARY-7b21-not-a-real-secret"
        process, port = self.start()
        self.reachable(process, port)
        connection = socket.create_connection(("127.0.0.1", port), timeout=10)
        try:
            body = json.dumps({"secret": canary, "text": canary}).encode()
            request = (
                b"POST /internal/v1/conversation/ingest?" + canary.encode() + b" HTTP/1.1\r\n"
                b"Host: 127.0.0.1\r\n"
                b"Authorization: Bearer " + canary.encode() + b"\r\n"
                b"X-Tianshu-Correlation-Id: " + canary.encode() + b"\r\n"
                b"X-Canary: " + canary.encode() + b"\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + body
            )
            connection.sendall(request)
            answered = b""
            while True:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                answered += chunk
        finally:
            connection.close()
        self.assertIn(b"HTTP/1.1", answered)
        self.stop(process)
        out, err = process.communicate(timeout=30)
        blobs = [path.read_bytes() for path in sorted(self.logs.iterdir()) if path.is_file()] + [
            out,
            err,
        ]
        self.assertTrue(any(blob for blob in blobs))
        for blob in blobs:
            self.assertNotIn(canary.encode(), blob)
        # And what the process did write is still the frozen record, on a real process.
        records = [
            json.loads(line)
            for path in sorted(self.logs.iterdir())
            for line in path.read_bytes().splitlines()
            if line.strip()
        ]
        self.assertTrue(records)
        for record in records:
            self.assertIn(record["outcome"], obs.OUTCOMES)
            self.assertIn(record["event"], obs.EVENTS)
            if record["error_code"] is not None:
                self.assertIn(record["error_code"], obs.ERROR_CODES)
            self.assertLessEqual(len(json.dumps(record).encode()) + 1, obs.MAX_RECORD_BYTES)

    def test_a_request_that_cannot_be_accounted_for_is_refused_by_the_real_process(self):
        """An unwritable destination refuses the work, in the deployment, not just in a test.

        The refusal is visible to the caller as 503, the process stays alive (a log failure is
        not a crash loop), and readiness reports the log as failed rather than pretending.
        """
        process, port = self.start()
        self.reachable(process, port)
        # Make the destination unusable from outside the process: the directory is replaced by a
        # file, so no further segment can be created in it.
        self.stop(process)
        log_root = self.logs
        if log_root.exists():
            shutil.rmtree(log_root)
        log_root.write_bytes(b"not-a-directory")
        process, port = self.start()
        try:
            self.assertTrue(
                _wait_for_port(port, process),
                "the runtime refused to start instead of serving with an unusable log",
            )
            connection = socket.create_connection(("127.0.0.1", port), timeout=10)
            try:
                connection.sendall(
                    b"POST /internal/v1/conversation/ingest HTTP/1.1\r\n"
                    b"Host: 127.0.0.1\r\n"
                    b"Content-Length: 2\r\n"
                    b"Connection: close\r\n\r\n{}"
                )
                answered = b""
                while True:
                    chunk = connection.recv(4096)
                    if not chunk:
                        break
                    answered += chunk
            finally:
                connection.close()
            self.assertIn(b" 503 ", answered)
        finally:
            if process.poll() is None:
                self.stop(process)

    def test_a_probe_writes_nothing_to_the_log_directory(self):
        import urllib.request

        process, port = self.start()
        self.reachable(process, port)
        # Measured while the process is up, so startup and shutdown records are not counted
        # against the probe: the question is only what a probe itself writes.
        before = _digest(self.logs)
        for path in ("/health/live", "/healthz"):
            with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as answer:
                self.assertEqual(200, answer.status)
        self.assertEqual(before, _digest(self.logs))
        self.stop(process)


def _free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class _HealthcheckTests(unittest.TestCase):
    """The container health check distinguishes a live process from a dead one, read-only."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(
            prefix="tianshu-healthcheck-", ignore_cleanup_errors=True
        )
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.database = self.root / "companion.db"
        self.logs = self.root / "logs"
        self.config = self.root / "companion.json"
        self.config.write_text(
            json.dumps(
                {
                    "contracts_path": str(_contracts_path()),
                    "database_path": str(self.database),
                    "config_version": None,
                    "policy": {"silence_ms": 5000},
                    "roles": {},
                    "bindings": {},
                    "callers": {},
                    "services": {},
                }
            ),
            encoding="utf-8",
        )
        self.environment = dict(os.environ)
        self.environment["TIANSHU_LOG_DIR"] = str(self.logs)

    def check(self, url):
        return subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "container_healthcheck.py"), "--url", url],
            cwd=str(ROOT),
            capture_output=True,
            timeout=30,
            creationflags=NO_WINDOW,
        )

    def test_a_dead_port_is_unhealthy_and_the_reason_is_named(self):
        result = self.check(f"http://127.0.0.1:{_free_port()}/health/live")
        self.assertNotEqual(0, result.returncode)
        self.assertIn(b"unhealthy", result.stderr)
        self.assertEqual(b"", result.stdout)

    def test_a_running_runtime_is_healthy_and_the_check_writes_nothing(self):
        port = _free_port()
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tianshu_companion.runtime_cli",
                "--config",
                str(self.config),
                "--contracts",
                str(_contracts_path()),
                "--database",
                str(self.database),
                "--log-dir",
                str(self.logs),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=str(ROOT),
            env=self.environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=NEW_PROCESS_GROUP,
        )
        self.addCleanup(lambda: process.poll() is None and process.kill())
        self.assertTrue(_wait_for_port(port, process), "the runtime never became reachable")
        before = _digest(self.logs)
        result = self.check(f"http://127.0.0.1:{port}/health/live")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(b"healthy", result.stdout.strip())
        self.assertEqual(before, _digest(self.logs))
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.terminate()
        process.wait(timeout=30)

    def test_the_check_never_reproduces_a_failure_or_a_payload(self):
        """The probe's own output is a closed vocabulary, not a copy of what it saw.

        A container runtime collects this stderr and an operator reads it, so anything the check
        echoes lands in a log: an exception message, a response body, an address or a path can
        each carry a credential or a hostname that has nothing to do with liveness. A synthetic
        secret is pushed through every failing branch of the real `main` - a broken payload, a
        network error, a TLS rejection and a malformed timeout - and must appear nowhere.
        """
        module = _load_healthcheck()
        canary = "REVIEW_CANARY_4f9a1c"

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *arguments):
                return False

            def read(self):
                return json.dumps({"synthetic_secret": canary}).encode()

        branches = {
            "payload": (
                {"return_value": Response()},
                {},
                module.BAD_PAYLOAD,
            ),
            "network": (
                {"side_effect": OSError(canary)},
                {},
                module.UNREACHABLE,
            ),
            "tls": (
                {"side_effect": ssl.SSLError(canary)},
                {},
                module.TLS_REFUSED,
            ),
            "timeout": (
                {"side_effect": OSError(canary)},
                {"TIANSHU_HEALTHCHECK_TIMEOUT": canary},
                module.BAD_TIMEOUT,
            ),
        }
        for name, (stub, environment, category) in branches.items():
            captured_out, captured_err = io.StringIO(), io.StringIO()
            with (
                mock.patch.object(module.urllib.request, "urlopen", **stub),
                contextlib.redirect_stdout(captured_out),
                contextlib.redirect_stderr(captured_err),
            ):
                code = module.main([], environ=environment)
            written = captured_out.getvalue() + captured_err.getvalue()
            self.assertEqual(1, code, name)
            self.assertEqual(f"unhealthy: {category}", written.strip(), name)
            self.assertNotIn(canary, written, name)
            self.assertNotIn("OSError", written, name)
            self.assertNotIn("SSLError", written, name)
        # The healthy answer is the same closed vocabulary, so a passing probe cannot leak a
        # payload either.
        captured_out, captured_err = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(
                module.urllib.request,
                "urlopen",
                return_value=_LiveAnswer(),
            ),
            contextlib.redirect_stdout(captured_out),
            contextlib.redirect_stderr(captured_err),
        ):
            self.assertEqual(0, module.main([], environ={}))
        self.assertEqual("healthy", captured_out.getvalue().strip())
        self.assertEqual("", captured_err.getvalue())
        # A certificate rejection is its own category, however `urllib` wraps it: the real library
        # reports a handshake failure as `URLError(reason=SSLCertVerificationError)`, and calling
        # that "unreachable" would send an operator to the network instead of the trust store.
        wrapped = urllib.error.URLError(ssl.SSLCertVerificationError(canary))
        with mock.patch.object(module.urllib.request, "urlopen", side_effect=wrapped):
            ok, category = module.check("https://127.0.0.1:1/health/live", timeout=1, environ={})
        self.assertFalse(ok)
        self.assertEqual(module.TLS_REFUSED, category)
        self.assertNotIn(canary, category)

    def test_the_check_never_asks_the_authenticated_readiness_question(self):
        # The image's health check must not be able to reach readiness at all: it sends no
        # credential, and the address it actually uses is the public liveness one. The module
        # docstring explains the rule, so only the executable text is inspected.
        source = (ROOT / "scripts" / "container_healthcheck.py").read_text(encoding="utf-8")
        code = source.split('"""', 2)[2]
        self.assertIn('DEFAULT_PATH = "/health/live"', code)
        self.assertNotIn("/health/ready", code)
        self.assertNotIn("TIANSHU_DIAGNOSTICS_TOKEN", code)
        self.assertNotIn("Authorization", code)

    def test_the_check_follows_the_deployment_address_and_scheme(self):
        """A TLS listener on a custom port must be probed as such, not as plaintext 8765.

        A hard-coded address would report a healthy HTTPS process as unhealthy forever, so the
        address is assembled from the deployment's own configuration - and verification is
        never relaxed to make a private CA work.
        """
        module = _load_healthcheck()
        self.assertEqual("http://127.0.0.1:8765/health/live", module.probe_url({}))
        self.assertEqual(
            "https://127.0.0.1:9443/health/live",
            module.probe_url(
                {
                    "TIANSHU_HEALTHCHECK_SCHEME": "https",
                    "TIANSHU_HEALTHCHECK_PORT": "9443",
                }
            ),
        )
        self.assertEqual(
            "https://companion.internal:8443/health/live",
            module.probe_url(
                {"TIANSHU_HEALTHCHECK_URL": "https://companion.internal:8443/health/live"}
            ),
        )
        # Plaintext needs no TLS context at all.
        self.assertIsNone(module.ssl_context("http://127.0.0.1:8765/health/live", None, {}))
        # An HTTPS address always gets a verifying context, with a CA added when one is named.
        context = module.ssl_context("https://127.0.0.1:9443/health/live", None, {})
        self.assertTrue(context.check_hostname)
        self.assertEqual(ssl.CERT_REQUIRED, context.verify_mode)
        self.assertTrue(
            module.ssl_context("https://127.0.0.1:9443/health/live", None, {}).verify_mode
        )

    def test_a_trusted_tls_listener_is_healthy_and_an_untrusted_one_is_not(self):
        """Real TLS: a private CA is added, and nothing is ever switched off.

        The listener is a real `https` socket on a loopback port with an ephemeral certificate
        generated for `127.0.0.1` - the same shape a deployment uses. The probe must answer
        `healthy` when it is given that CA, and must *fail* when it is not, because a check that
        accepted any certificate would report a machine-in-the-middle as a healthy runtime.
        """
        pem = _ephemeral_loopback_certificate(self.root)
        server = _LivenessTlsServer(str(pem), str(self.root / "key.pem"))
        self.addCleanup(server.shutdown)
        url = f"https://127.0.0.1:{server.port}/health/live"
        # Untrusted: the certificate is not in the default trust store, so verification fails
        # and the probe says so instead of accepting it.
        untrusted = self.check(url)
        self.assertNotEqual(0, untrusted.returncode, untrusted.stdout)
        self.assertEqual(b"unhealthy: tls_verification_failed", untrusted.stderr.strip())
        # Trusted: the same address, with the deployment's own CA named explicitly.
        trusted = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "container_healthcheck.py"),
                "--url",
                url,
                "--ca",
                str(self.root / "cert.pem"),
            ],
            cwd=str(ROOT),
            capture_output=True,
            timeout=30,
            creationflags=NO_WINDOW,
        )
        self.assertEqual(0, trusted.returncode, trusted.stderr)
        self.assertEqual(b"healthy", trusted.stdout.strip())
        # The certificate covers the address, not the name: a name-based URL must still be
        # verified, so the check cannot be fooled by a hostname that resolves to loopback.
        mismatched = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "container_healthcheck.py"),
                "--url",
                f"https://localhost:{server.port}/health/live",
                "--ca",
                str(self.root / "cert.pem"),
            ],
            cwd=str(ROOT),
            capture_output=True,
            timeout=30,
            creationflags=NO_WINDOW,
        )
        self.assertNotEqual(0, mismatched.returncode)
        self.assertIn(b"unhealthy", mismatched.stderr)
        # Whatever the answer, the probe asked liveness and only liveness: the handshake
        # failures never reach the application, so the served path is the one check that
        # completed - and no other path was ever requested.
        self.assertIn("/health/live", server.paths)
        self.assertEqual({"/health/live"}, set(server.paths))

    def test_the_image_declares_the_health_check_and_a_non_root_identity(self):
        text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("HEALTHCHECK", text)
        self.assertIn('CMD ["python", "/app/scripts/container_healthcheck.py"]', text)
        self.assertIn("USER 10001:10001", text)
        self.assertIn('ENTRYPOINT ["python", "-m", "tianshu_companion.runtime_cli"]', text)
        # The health check it declares reads liveness, and the readiness view is not named
        # anywhere in the image definition.
        self.assertIn("/health/live", (ROOT / "scripts" / "container_healthcheck.py").read_text())
        self.assertNotIn("/health/ready", text)
        # A baked credential is a defect regardless of how the rest of the file looks.
        for forbidden in ("TOKEN=", "PASSWORD=", "SECRET=", "PRIVATE KEY"):
            self.assertNotIn(forbidden, text)
        # No model download and no build step that reaches the network beyond the pinned index.
        for forbidden in ("huggingface", "torch", "git clone", "curl http", "wget http"):
            self.assertNotIn(forbidden, text.lower())


def _contracts_path():
    """The published contract pack this checkout validates against."""
    if os.environ.get("TIANSHU_CONTRACTS"):
        return Path(os.environ["TIANSHU_CONTRACTS"])
    context = json.loads((ROOT / ".runtime/workspace-context.json").read_text(encoding="utf-8"))
    return Path(context["workspace"]) / "contracts/text-dialogue/v1"


CERT_PROGRAM = """
import datetime, ipaddress, pathlib, sys
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
root=pathlib.Path(sys.argv[1])
key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,"Companion synthetic loopback")])
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


def _ephemeral_loopback_certificate(root):
    """A one-day certificate for `127.0.0.1`, generated by an interpreter with `cryptography`.

    Nothing is committed: the certificate and its key exist only in this case's temporary
    directory, which is why the TLS cases are skipped rather than faked when the explicit
    certificate runtime is not configured. The project's own environment is never extended with
    a certificate dependency, and the listener under test is still the project's interpreter.
    """
    interpreter = os.environ.get("TIANSHU_TLS_PYTHON")
    if not interpreter:
        raise unittest.SkipTest(
            "Set TIANSHU_TLS_PYTHON to an interpreter with cryptography to exercise real TLS"
        )
    subprocess.run(
        [interpreter, "-c", CERT_PROGRAM, str(root)],
        check=True,
        capture_output=True,
        timeout=120,
    )
    return root / "cert.pem"


class _LivenessTlsServer:
    """A real `https` listener that serves the liveness payload and records what was asked."""

    def __init__(self, certificate, key):
        import http.server

        self.paths = []
        self.certificate, self.key = certificate, key
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                server.paths.append(self.path)
                if self.path == "/health/live":
                    body = b'{"status":"alive"}'
                    self.send_response(200)
                else:
                    body = b'{"status":"not_here"}'
                    self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *arguments):
                """Silence: this listener's output is not the subject of the case."""

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.httpd.server_address[1]
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certificate, key)
        self.httpd.socket = context.wrap_socket(self.httpd.socket, server_side=True)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def shutdown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=10)


def _wait_for_port(port, process, attempts=200):
    """Wait until the loopback port answers a plain TCP connection, or the process stops."""
    for _ in range(attempts):
        if process.poll() is not None:
            return False
        probe = socket.socket()
        probe.settimeout(1.0)
        try:
            probe.connect(("127.0.0.1", port))
            return True
        except OSError:
            time.sleep(0.05)
        finally:
            probe.close()
    return False


def _digest(directory):
    import hashlib

    digest = hashlib.sha256()
    for path in sorted(Path(directory).iterdir()):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


if __name__ == "__main__":
    unittest.main()
