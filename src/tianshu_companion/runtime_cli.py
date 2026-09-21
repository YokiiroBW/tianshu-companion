"""Production entry point: explicit paths, explicit binding, TLS outside loopback, one owner.

This module decides nothing about the companion's behaviour. It resolves the deployment
paths, refuses an unsafe binding before a socket exists, assembles the application through
the existing factory, and turns a termination signal into a bounded graceful shutdown that
releases the single SQLite owner lock.

Deliberate rules, all of them checkable before anything listens:

* Configuration, contracts, database and log directory are **explicit**. The container
  defaults are `/config`, `/contracts`, `/data/companion.db` and `/var/log/tianshu`; the
  development defaults are relative paths inside this checkout. Nothing here reads the
  Windows coordination repository, and no path is guessed from the current directory of a
  different machine.
* The default port is 8765 and the default bind is the IPv4 loopback address.
* A binding that is **not** loopback requires an explicit TLS certificate and key. There is
  no flag that turns TLS off for a remote binding, because "temporarily plaintext" is how a
  deployment quietly loses its transport protection.
* `workers` is fixed at 1 for the whole process. SQLite has one owner and the scheduler has
  one in-process state, so a second worker is refused rather than silently corrupting both.
"""

import argparse
import asyncio
import ipaddress
import json
import os
import signal
import sys
from pathlib import Path

DEFAULT_PORT = 8765
DEFAULT_HOST = "127.0.0.1"
FIXED_WORKERS = 1

CONTAINER_PATHS = {
    "config": "/config/companion.json",
    "contracts": "/contracts",
    "database": "/data/companion.db",
    "log_dir": "/var/log/tianshu",
}
DEVELOPMENT_PATHS = {
    "config": ".runtime/companion.json",
    "contracts": ".runtime/contracts",
    "database": ".runtime/companion.db",
    "log_dir": None,
}

ENVIRONMENT = {
    "config": "TIANSHU_COMPANION_CONFIG",
    "contracts": "TIANSHU_CONTRACTS",
    "database": "TIANSHU_COMPANION_DATABASE",
    "log_dir": "TIANSHU_LOG_DIR",
    "log_segment_bytes": "TIANSHU_LOG_SEGMENT_BYTES",
    "log_directory_bytes": "TIANSHU_LOG_DIRECTORY_BYTES",
}

ASCII_HOST = frozenset({"localhost"})


def is_loopback(host):
    """Only a real loopback address counts; a hostname that merely looks local does not."""
    if not isinstance(host, str) or not host.strip():
        return False
    candidate = host.strip().strip("[]")
    if candidate.lower() in ASCII_HOST:
        return True
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


def bind_refusal(host, port, certificate, key):
    """The reason this binding may not be used, or None when it may.

    A non-loopback bind without a complete certificate/key pair is refused before the
    process opens a socket, so remote plaintext is not reachable by forgetting a flag.
    """
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        return "port_out_of_range"
    if not isinstance(host, str) or not host.strip():
        return "host_missing"
    if is_loopback(host):
        return None
    if not certificate or not key:
        return "tls_required_for_non_loopback"
    for value in (certificate, key):
        path = Path(value)
        if not path.is_absolute() or not path.is_file() or path.stat().st_size == 0:
            return "tls_material_unreadable"
    return None


class RuntimePaths:
    """Every path and binding the process uses, resolved once and printed on request."""

    def __init__(self, config, contracts, database, log_dir, host, port, certificate, key):
        self.config = config
        self.contracts = contracts
        self.database = database
        self.log_dir = log_dir
        self.host = host
        self.port = port
        self.certificate = certificate
        self.key = key

    @property
    def loopback(self):
        return is_loopback(self.host)

    def summary(self):
        """Safe to print: paths and binding only, never a credential or a token value."""
        return {
            "config": str(self.config),
            "contracts": str(self.contracts),
            "database": str(self.database),
            "log_dir": str(self.log_dir) if self.log_dir else None,
            "host": self.host,
            "port": self.port,
            "tls": bool(self.certificate and self.key),
            "workers": FIXED_WORKERS,
        }


def build_parser():
    parser = argparse.ArgumentParser(
        prog="tianshu-companion",
        description="Companion runtime: explicit deployment paths, one SQLite owner, TLS outside loopback.",
    )
    parser.add_argument("--config", help="deployment configuration document")
    parser.add_argument("--contracts", help="published contracts directory (read-only mount)")
    parser.add_argument("--database", help="explicit SQLite database path, e.g. /data/companion.db")
    parser.add_argument("--log-dir", help="runtime event log directory, e.g. /var/log/tianshu")
    parser.add_argument("--host", help=f"bind address (default {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, help=f"bind port (default {DEFAULT_PORT})")
    parser.add_argument("--tls-cert", help="TLS certificate chain, required outside loopback")
    parser.add_argument("--tls-key", help="TLS private key, required outside loopback")
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="print the resolved paths and binding, then exit without listening",
    )
    return parser


def resolve(arguments, environ=None, platform=None):
    """Explicit argument, then its documented environment variable, then the platform default.

    The development defaults are relative to this checkout, so an unconfigured `--print-config`
    on a workstation is obviously a development binding. The container defaults are the
    documented absolute mounts, so the image does not need a flag to be correct.
    """
    environ = os.environ if environ is None else environ
    platform = sys.platform if platform is None else platform
    defaults = CONTAINER_PATHS if platform.startswith("linux") else DEVELOPMENT_PATHS

    def value(name, explicit):
        if explicit:
            return explicit
        from_environment = environ.get(ENVIRONMENT[name])
        if from_environment:
            return from_environment
        return defaults[name]

    config = value("config", arguments.config)
    contracts = value("contracts", arguments.contracts)
    database = value("database", arguments.database)
    log_dir = value("log_dir", arguments.log_dir)
    return RuntimePaths(
        config=Path(config),
        contracts=Path(contracts),
        database=Path(database),
        log_dir=Path(log_dir) if log_dir else None,
        host=arguments.host or DEFAULT_HOST,
        # `is None` rather than `or`: an explicit `--port 0` is a real value the operator typed,
        # and it is out of range. Treating it as "unset" would silently bind the default port
        # instead of refusing, which is how a deployment ends up listening where nobody expects.
        port=DEFAULT_PORT if arguments.port is None else arguments.port,
        certificate=arguments.tls_cert,
        key=arguments.tls_key,
    )


def environment_for(paths, environ=None):
    """The deployment document the application factory already understands.

    `TIANSHU_COMPANION_CONFIG` stays the one entry point, so the original loopback
    development path and the published factory keep working unchanged.

    `TIANSHU_CONTRACTS` and `TIANSHU_COMPANION_DATABASE` are the *deployment* paths, and the
    application factory gives them precedence over the values written in the configuration
    document (see `app.apply_deployment_overrides`). Writing them here is therefore not
    decoration: it is what makes `--database /data/companion.db` actually open that database
    instead of whichever path a stale document still names.
    """
    values = dict(os.environ if environ is None else environ)
    values[ENVIRONMENT["config"]] = str(paths.config)
    values[ENVIRONMENT["contracts"]] = str(paths.contracts)
    values[ENVIRONMENT["database"]] = str(paths.database)
    if paths.log_dir is None:
        values.pop(ENVIRONMENT["log_dir"], None)
    else:
        values[ENVIRONMENT["log_dir"]] = str(paths.log_dir)
    return values


async def serve(paths, *, factory=None, server_factory=None, shutdown=None):
    """Serve until a termination signal arrives, then shut down in an orderly way.

    The application lifespan already cancels the background workers and closes the store,
    which releases the single-owner lock, so this function only has to translate the signal.
    """
    import uvicorn

    from .app import create_app

    factory = factory or create_app
    shutdown = shutdown or asyncio.Event()
    server = (server_factory or uvicorn.Server)(
        uvicorn.Config(
            factory,
            host=paths.host,
            port=paths.port,
            workers=FIXED_WORKERS,
            ssl_certfile=str(paths.certificate) if paths.certificate else None,
            ssl_keyfile=str(paths.key) if paths.key else None,
            # The service's own JSON event stream is the log; uvicorn's plain access log
            # would be a second, unrelated format in the same operational view.
            access_log=False,
            log_level="info",
        )
    )

    def request_shutdown(*_):
        server.should_exit = True

    installed = []
    for name in ("SIGTERM", "SIGINT"):
        number = getattr(signal, name, None)
        if number is None:
            continue
        try:
            installed.append((number, signal.getsignal(number)))
            signal.signal(number, request_shutdown)
        except (ValueError, OSError, RuntimeError, NotImplementedError):
            continue
    try:
        await server.serve()
    finally:
        for number, previous in installed:
            try:
                signal.signal(number, previous)
            except (ValueError, OSError, RuntimeError, NotImplementedError):
                pass
    return server


def main(argv=None, environ=None, platform=None, server_factory=None):
    arguments = build_parser().parse_args(argv)
    paths = resolve(arguments, environ=environ, platform=platform)
    refusal = bind_refusal(paths.host, paths.port, paths.certificate, paths.key)
    if refusal is not None:
        print(
            f"refusing to start: {refusal}. A non-loopback bind needs --tls-cert and --tls-key.",
            file=sys.stderr,
        )
        return 2
    if arguments.print_config:
        print(json.dumps(paths.summary(), indent=2, sort_keys=True))
        return 0
    if not Path(paths.config).is_file():
        print(
            f"refusing to start: configuration document not found at {paths.config}",
            file=sys.stderr,
        )
        return 2
    saved = {key: os.environ.get(key) for key in ENVIRONMENT.values()}
    os.environ.update(environment_for(paths, environ=environ))
    try:
        asyncio.run(serve(paths, server_factory=server_factory))
    except KeyboardInterrupt:
        return 0
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
