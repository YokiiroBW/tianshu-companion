"""Container liveness check: one plain read of the public liveness address.

This is what the image's `HEALTHCHECK` runs. It asks one question - is the process answering -
and nothing else:

* It reads `/health/live`, never `/health/ready`. A restart policy driven by readiness would
  restart a healthy process whose log directory filled up or whose dependencies are not yet
  verified, turning a capacity problem into a restart loop that loses the in-flight work the
  log was supposed to record.
* It never sends a readiness credential, so it cannot be used to reach the readiness view.
* It writes nothing: no log record, no database row, no file. It has no dependency on this
  package at all, so it also works when the application is failing to import.
* It exits 0 when the process answered the documented payload, and non-zero otherwise, which
  is the only vocabulary a container health check has.

Usage: `python scripts/container_healthcheck.py [--url URL] [--timeout SECONDS]`.
"""

import argparse
import json
import sys
import urllib.error
import urllib.request

DEFAULT_URL = "http://127.0.0.1:8765/health/live"
DEFAULT_TIMEOUT = 3.0
EXPECTED = {"status": "alive"}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Container liveness check (read-only).")
    parser.add_argument(
        "--url", default=DEFAULT_URL, help=f"liveness address (default {DEFAULT_URL})"
    )
    parser.add_argument(
        "--timeout", type=float, default=DEFAULT_TIMEOUT, help="seconds to wait for an answer"
    )
    arguments = parser.parse_args(argv)
    try:
        with urllib.request.urlopen(arguments.url, timeout=arguments.timeout) as response:
            if response.status != 200:
                print(f"unhealthy: liveness answered {response.status}", file=sys.stderr)
                return 1
            payload = json.loads(response.read())
    except (OSError, urllib.error.URLError, ValueError) as error:
        # The message is the container runtime's only diagnostic, so it names the failure
        # rather than only the address: a timeout and a refused connection read differently.
        print(f"unhealthy: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    if payload != EXPECTED:
        print(f"unhealthy: unexpected liveness payload {payload!r}", file=sys.stderr)
        return 1
    print("healthy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
