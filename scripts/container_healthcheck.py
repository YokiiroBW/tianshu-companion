"""Container liveness check: one read of the public liveness address, over the real scheme.

This is what the image's `HEALTHCHECK` runs. It asks one question - is the process answering -
and nothing else:

* It reads `/health/live`, never `/health/ready`. A restart policy driven by readiness would
  restart a healthy process whose log directory filled up or whose dependencies are not yet
  verified, turning a capacity problem into a restart loop that loses the in-flight work the
  log was supposed to record.
* It follows the deployment's own binding. A service started as `--host 0.0.0.0 --tls-cert ...
  --tls-key ...` speaks HTTPS, so a probe hard-coded to plaintext `127.0.0.1:8765` would report
  a perfectly healthy process as unhealthy forever. The address, the port, the scheme and the
  CA all come from explicit variables, so the probe can always be made to match the listener.
* It never disables certificate or hostname verification. `TIANSHU_HEALTHCHECK_CA` *adds* a
  trust anchor for a private CA; it is never a switch that turns verification off.
* It sends no credential of any kind: no business token, no bridge token, no diagnostics token.
  It therefore cannot reach the authenticated readiness view even by accident.
* It writes nothing: no log record, no database row, no file. It has no dependency on this
  package at all, so it also works when the application is failing to import.
* It exits 0 when the process answered the documented payload, and non-zero otherwise, which
  is the only vocabulary a container health check has.
* Its output is a **closed set of static categories**, never text from the failure. An exception
  message, a response body, an address or a path can carry a credential, a hostname or a
  payload that has nothing to do with liveness - and this script's stderr is collected by the
  container runtime and read by whoever is debugging. The category says what class of failure
  happened; the detail is deliberately not reproduced here.

Configuration, all optional, all read from the environment of the container:

* `TIANSHU_HEALTHCHECK_URL` - the full liveness URL. Highest precedence: it carries the scheme
  and the port together, so a TLS listener on a custom port is one variable.
* `TIANSHU_HEALTHCHECK_HOST` / `TIANSHU_HEALTHCHECK_PORT` / `TIANSHU_HEALTHCHECK_SCHEME` -
  assembled into a URL when no full URL is given.
* `TIANSHU_HEALTHCHECK_CA` - path to a PEM bundle for a private CA.
* `TIANSHU_HEALTHCHECK_TIMEOUT` - seconds to wait for an answer.

Usage: `python scripts/container_healthcheck.py [--url URL] [--ca PEM] [--timeout SECONDS]`.
"""

import argparse
import json
import os
import ssl
import sys
import urllib.error
import urllib.request

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = "8765"
DEFAULT_SCHEME = "http"
DEFAULT_PATH = "/health/live"
DEFAULT_TIMEOUT = 3.0
EXPECTED = {"status": "alive"}

# The closed vocabulary this check reports in. Each entry names a class of failure, never the
# failure's own text: the runtime may print these, so they must stay true for every input.
HEALTHY = "healthy"
TLS_REFUSED = "tls_verification_failed"
UNREACHABLE = "unreachable"
BAD_ANSWER = "unexpected_status"
BAD_PAYLOAD = "unexpected_payload"
BAD_TIMEOUT = "invalid_timeout"

ENVIRONMENT_URL = "TIANSHU_HEALTHCHECK_URL"
ENVIRONMENT_HOST = "TIANSHU_HEALTHCHECK_HOST"
ENVIRONMENT_PORT = "TIANSHU_HEALTHCHECK_PORT"
ENVIRONMENT_SCHEME = "TIANSHU_HEALTHCHECK_SCHEME"
ENVIRONMENT_CA = "TIANSHU_HEALTHCHECK_CA"
ENVIRONMENT_TIMEOUT = "TIANSHU_HEALTHCHECK_TIMEOUT"


def probe_url(environ=None):
    """The liveness URL this deployment actually serves, from its own configuration."""
    environ = os.environ if environ is None else environ
    explicit = environ.get(ENVIRONMENT_URL)
    if explicit:
        return explicit
    scheme = environ.get(ENVIRONMENT_SCHEME) or DEFAULT_SCHEME
    host = environ.get(ENVIRONMENT_HOST) or DEFAULT_HOST
    port = environ.get(ENVIRONMENT_PORT) or DEFAULT_PORT
    return f"{scheme}://{host}:{port}{DEFAULT_PATH}"


def ssl_context(url, ca, environ=None):
    """A verifying context for an `https` URL, or None for plaintext.

    Verification is never relaxed: the default context checks the certificate chain and the
    hostname, and a configured CA is *added* to the trust store rather than replacing the
    checks.
    """
    if not url.lower().startswith("https://"):
        return None
    environ = os.environ if environ is None else environ
    bundle = ca or environ.get(ENVIRONMENT_CA)
    context = ssl.create_default_context(cafile=bundle if bundle else None)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


def _tls_failure(error):
    """Whether this failure is a TLS/certificate rejection, however it was wrapped.

    `urllib` reports a handshake problem as `URLError(reason=SSLCertVerificationError)`, so the
    top-level exception type alone would call every certificate failure "unreachable" - and an
    operator reading the category would go looking at the network instead of the trust store.
    """
    seen = set()
    while error is not None and id(error) not in seen:
        if isinstance(error, ssl.SSLError):
            return True
        seen.add(id(error))
        error = getattr(error, "reason", None)
    return False


def check(url, *, ca=None, timeout=DEFAULT_TIMEOUT, environ=None):
    """Return (ok, category). Never raises, and never reproduces the failure's own text.

    The categories are the whole diagnostic vocabulary: a TLS rejection and a refused connection
    read differently - which is what an operator needs - without either one carrying a
    certificate subject, a response body, an address or a path into the container log.
    """
    try:
        with urllib.request.urlopen(
            url, timeout=timeout, context=ssl_context(url, ca, environ)
        ) as response:
            if response.status != 200:
                return False, BAD_ANSWER
            payload = json.loads(response.read())
    except (OSError, urllib.error.URLError, ValueError) as error:
        # A certificate this process will not trust, or a hostname that does not match it, is its
        # own category; everything else that could not be reached is "unreachable".
        return False, TLS_REFUSED if _tls_failure(error) else UNREACHABLE
    if payload != EXPECTED:
        return False, BAD_PAYLOAD
    return True, HEALTHY


def main(argv=None, environ=None):
    environ = os.environ if environ is None else environ
    parser = argparse.ArgumentParser(description="Container liveness check (read-only).")
    parser.add_argument("--url", default=None, help="liveness address, including scheme and port")
    parser.add_argument(
        "--ca", default=None, help=f"PEM bundle for a private CA ({ENVIRONMENT_CA})"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help=f"seconds to wait for an answer ({ENVIRONMENT_TIMEOUT})",
    )
    arguments = parser.parse_args(argv)
    configured = environ.get(ENVIRONMENT_TIMEOUT)
    timeout = arguments.timeout
    if timeout is None:
        try:
            timeout = float(configured) if configured else DEFAULT_TIMEOUT
        except ValueError:
            # A malformed value is reported as a category too: it must not be echoed back, and
            # it must not be silently replaced by the default either.
            print(f"unhealthy: {BAD_TIMEOUT}", file=sys.stderr)
            return 1
    url = arguments.url or probe_url(environ)
    ok, category = check(url, ca=arguments.ca, timeout=timeout, environ=environ)
    print(category if ok else f"unhealthy: {category}", file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
