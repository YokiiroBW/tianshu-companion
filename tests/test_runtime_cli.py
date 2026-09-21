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

import contextlib
import io
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from tianshu_companion import runtime_cli
from tianshu_companion.store import Store

ROOT = Path(__file__).resolve().parents[1]
NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
NEW_PROCESS_GROUP = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0


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
        self.assertIn(b"owner", (second.stdout + second.stderr).lower())

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

    def test_the_check_never_asks_the_authenticated_readiness_question(self):
        # The image's health check must not be able to reach readiness at all: it sends no
        # credential, and the address it actually uses is the public liveness one. The module
        # docstring explains the rule, so only the executable text is inspected.
        source = (ROOT / "scripts" / "container_healthcheck.py").read_text(encoding="utf-8")
        code = source.split('"""', 2)[2]
        self.assertIn('DEFAULT_URL = "http://127.0.0.1:8765/health/live"', code)
        self.assertNotIn("/health/ready", code)
        self.assertNotIn("TIANSHU_DIAGNOSTICS_TOKEN", code)
        self.assertNotIn("Authorization", code)

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
