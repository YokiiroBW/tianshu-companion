"""The two health endpoints: public liveness, and an authenticated closed readiness view.

Readiness answers one question - may this process serve business traffic *now* - and it
answers it from local facts only. It never ticks the scheduler, never snapshots life state,
never inspects turn content, never migrates, never creates a directory, never writes a probe
file, never sends a message and never calls a paid model. A reader of `/health/ready` cannot
change one byte of the database, the `source_head` or the log directory.

`/health/live` is the public minimum: `{"status": "alive"}` and nothing else. It says the
process is running, not that any dependency works, and it is deliberately not what a
container restart policy should be driven by business state.

The readiness credential is its own configuration (`TIANSHU_DIAGNOSTICS_TOKEN`). Chat,
bridge, ingest and persona-management credentials never reach this endpoint, and neither
endpoint can fall through to a SPA handler or any other application route.
"""

import hmac
import time

SERVICE = "companion"

# The closed check vocabulary of contracts/diagnostics/v1.
CHECK_VALUES = frozenset({"ok", "failed", "not_configured", "not_verified", "non_durable"})

# Static keys, in the order the contract lists them. A key never appears or disappears
# depending on the deployment: a missing section reports `not_configured`, not silence.
CHECK_KEYS = ("configuration", "logs", "runtime", "dependencies")

# Values that mean "this local necessary condition holds". `not_verified` counts for the
# dependency check, which this batch deliberately never claims to have exercised: treating an
# unverifiable remote as a failure would make the process permanently unready, which is a
# different lie from calling it verified.
PASSING_VALUES = frozenset({"ok", "not_verified"})

# The database probe is a bounded read: one trivial read-only statement, no transaction, no
# content read, no write. A database that cannot answer it inside the budget is not ready.
DB_PROBE_BUDGET_SECONDS = 0.5

ENDPOINTS = ("/health/live", "/health/ready")

# Everything that answers a probe rather than a caller. `/healthz` is the pre-existing
# liveness address, so it is a probe too: a probe must be purely read-only, which includes
# leaving no record behind, and the request middleware skips exactly this set.
PROBE_PATHS = frozenset(ENDPOINTS) | {"/healthz"}

TOKEN_ENV = "TIANSHU_DIAGNOSTICS_TOKEN"


def live_payload():
    """The public minimum. No configuration, no dependency, no environment value."""
    return {"status": "alive"}


class Health:
    """Read-only health view over an assembled runtime and its log port."""

    def __init__(self, token, *, core=None, log=None, clock=time.monotonic):
        self.token = token or None
        self.core = core
        self.log = log
        self.clock = clock

    @property
    def configured(self):
        """A readiness endpoint without its own credential is not deployed at all."""
        return bool(self.token)

    def authorized(self, header):
        """Constant-time comparison against the dedicated diagnostics credential."""
        if not self.token:
            return False
        return hmac.compare_digest(str(header or "").encode(), b"Bearer " + self.token.encode())

    def checks(self):
        return {
            "configuration": "ok" if self.configured else "not_configured",
            "logs": self._logs(),
            "runtime": self._runtime(),
            # No real Memory, gateway or channel was exercised in this batch, so no
            # dependency may be reported as verified merely because a section exists.
            "dependencies": "not_verified",
        }

    def ready(self):
        """`ready` only when every local necessary condition holds. Closed keys, no paths."""
        checks = self.checks()
        return {
            "status": "ready" if all(v in PASSING_VALUES for v in checks.values()) else "not_ready",
            "service": SERVICE,
            "checks": checks,
        }

    def _logs(self):
        if self.log is None:
            return "non_durable"
        state = self.log.health()
        return state if state in CHECK_VALUES else "failed"

    def _runtime(self):
        """Assembled, owner still valid, still able to answer a bounded read."""
        core = self.core
        if core is None:
            return "not_configured"
        if getattr(core, "closed", False):
            return "failed"
        store = getattr(core, "store", None)
        if store is None:
            return "not_configured"
        lock = getattr(store, "_lock", None)
        if lock is None:
            # `:memory:` has no owner file, so a missing lock is not a lost lock; a file-backed
            # store that lost its handle is reported below, because the check is skipped only
            # when the connection itself is an in-memory database.
            if not self._file_backed(store):
                return self._readable(store)
            return "failed"
        if getattr(lock, "closed", False):
            # The single-owner advisory lock has been released: this process no longer owns
            # the database, so it must not be reported as able to serve.
            return "failed"
        return self._readable(store)

    @staticmethod
    def _file_backed(store):
        """Is this store a database file, or the in-process `:memory:` one?"""
        database = getattr(store, "db", None)
        if database is None:
            return True
        try:
            rows = database.execute("PRAGMA database_list").fetchall()
        except Exception:
            # An unreadable list is answered by the readable check itself, not here.
            return True
        # `:memory:` is listed with an empty file name; a real database names its file.
        return any(str(row[2] or "") for row in rows)

    def _readable(self, store):
        database = getattr(store, "db", None)
        if database is None:
            return "failed"
        deadline = self.clock() + DB_PROBE_BUDGET_SECONDS
        try:
            if hasattr(database, "set_progress_handler"):
                database.set_progress_handler(lambda: 1 if self.clock() > deadline else 0, 1000)
            try:
                row = database.execute("SELECT 1").fetchone()
            finally:
                if hasattr(database, "set_progress_handler"):
                    database.set_progress_handler(None, 0)
        except Exception:
            return "failed"
        return "ok" if row is not None else "failed"
