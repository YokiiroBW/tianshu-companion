"""The two health endpoints: a public minimum, and an authenticated closed readiness view.

Readiness is exercised as a real HTTP surface over the application's own ASGI entry point, and
its promise is checked the only way that promise can be checked: by comparing the bytes of the
database, the source head and the log directory before and after the request. A probe that
answers correctly but changes one of those is not a read-only probe.

Nothing here touches a network, a container, a real account or a paid model. Everything is a
synthetic in-process application over the test harness's own stores.
"""

import asyncio
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx

from support import Harness
from tianshu_companion import health as health_module
from tianshu_companion import observability as obs
from tianshu_companion.app import create_app

TOKEN = "synthetic-diagnostics-credential"
CHAT_TOKEN = "synthetic-bridge-credential"
PERSONA_TOKEN = "synthetic-persona-admin-credential"


class _LogDirectory:
    """A real log directory, configured the way a deployment configures one."""

    def __init__(self, case):
        self.case = case
        self.holder = tempfile.TemporaryDirectory(
            prefix="tianshu-health-", ignore_cleanup_errors=True
        )
        self.path = self.holder.name
        self.adapters = []
        case.addCleanup(self._release)
        patcher = mock.patch.dict(os.environ, {obs.ENVIRONMENT_LOG_DIR: self.path})
        patcher.start()
        case.addCleanup(patcher.stop)

    def _release(self):
        # Every adapter this case assembled is closed, including the ones built by an extra
        # `create_app` call: an open segment file would outlive the test and leak its handle.
        for adapter in self.adapters:
            adapter.close()

    def attach(self, app):
        """Remember the log port this application assembled, so the case can close it."""
        self.adapters.append(app.state.log)
        return app

    @property
    def adapter(self):
        return self.adapters[-1]

    def digest(self):
        """A content digest of every file under the directory, at this instant."""
        digest = hashlib.sha256()
        for path in sorted(Path(self.path).iterdir()):
            digest.update(path.name.encode())
            digest.update(path.read_bytes())
        return digest.hexdigest()

    def names(self):
        return sorted(path.name for path in Path(self.path).iterdir())


class _Response:
    def __init__(self, status, body, headers=None):
        self.status_code = status
        self.body = body
        self.headers = headers or {}

    def json(self):
        return json.loads(self.body)

    def __repr__(self):
        return f"<{self.status_code} {self.body[:120]!r}>"


def call(app, path, *, token=None, method="GET"):
    """One request against the application's ASGI surface, without a network."""

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://core"
        ) as client:
            headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
            response = await client.request(method, path, headers=headers)
            return _Response(response.status_code, response.content, dict(response.headers))

    return asyncio.run(scenario())


def database_digest(store):
    """Every fact row and the source head, serialized, for a before/after comparison."""
    digest = hashlib.sha256()
    names = [
        str(row[0]) for row in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    ]
    for name in sorted(names):
        for row in store.db.execute(f"SELECT id, body FROM {name} ORDER BY id"):
            digest.update(name.encode())
            digest.update(str(row[0]).encode())
            digest.update((row[1] or "").encode())
    return digest.hexdigest()


def head(store):
    return json.dumps(store.get("metadata", "source_head"), sort_keys=True)


class LiveTests(unittest.TestCase):
    """The public liveness answer is minimal, and it is not a readiness claim."""

    def setUp(self):
        self.logs = _LogDirectory(self)

    def test_liveness_is_the_public_minimum(self):
        self.assertEqual({"status": "alive"}, health_module.live_payload())

    def test_live_answers_without_a_credential_and_says_nothing_about_dependencies(self):
        h = Harness(silence_ms=0)
        self.addCleanup(lambda: asyncio.run(h.core.close()))
        app = self.logs.attach(create_app(h.core, {"nonebot": CHAT_TOKEN}, None))
        response = call(app, "/health/live")
        self.assertEqual(200, response.status_code)
        # Exactly one key with one value: liveness is not where readiness leaks out.
        self.assertEqual({"status": "alive"}, response.json())
        self.assertNotIn("ready", response.body.decode())

    def test_live_ignores_a_credential_rather_than_requiring_one(self):
        h = Harness(silence_ms=0)
        self.addCleanup(lambda: asyncio.run(h.core.close()))
        app = self.logs.attach(create_app(h.core, {"nonebot": CHAT_TOKEN}, None))
        self.assertEqual(
            call(app, "/health/live").body, call(app, "/health/live", token=TOKEN).body
        )


class ReadinessCredentialTests(unittest.TestCase):
    """Readiness has its own credential, and nothing else opens it."""

    def setUp(self):
        self.logs = _LogDirectory(self)
        self.h = Harness(silence_ms=0)
        self.addCleanup(lambda: asyncio.run(self.h.core.close()))
        self.environment = mock.patch.dict(os.environ, {health_module.TOKEN_ENV: TOKEN})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.app = self.logs.attach(
            create_app(self.h.core, {"nonebot": CHAT_TOKEN, "persona_admin": PERSONA_TOKEN}, None)
        )

    def test_a_missing_or_wrong_credential_is_unauthorized(self):
        for token in (None, "", "wrong", TOKEN + "x", "bearer " + TOKEN):
            response = call(self.app, "/health/ready", token=token)
            self.assertEqual(401, response.status_code, token)
            self.assertEqual({"code": "unauthorized"}, response.json())

    def test_another_service_credential_never_opens_readiness(self):
        # The two other credentials this deployment holds are not this endpoint's.
        for token in (CHAT_TOKEN, PERSONA_TOKEN):
            response = call(self.app, "/health/ready", token=token)
            self.assertEqual(401, response.status_code, token)
            self.assertEqual({"code": "unauthorized"}, response.json())

    def test_without_its_own_credential_the_endpoint_is_not_deployed(self):
        with mock.patch.dict(os.environ, {health_module.TOKEN_ENV: ""}):
            h = Harness(silence_ms=0)
            self.addCleanup(lambda: asyncio.run(h.core.close()))
            app = self.logs.attach(create_app(h.core, {"nonebot": CHAT_TOKEN}, None))
        response = call(app, "/health/ready", token=TOKEN)
        self.assertEqual(503, response.status_code)
        self.assertEqual(
            {"code": "dependency_unavailable", "detail": "diagnostics_not_configured"},
            response.json(),
        )

    def test_the_ready_body_is_closed(self):
        response = call(self.app, "/health/ready", token=TOKEN)
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual({"status", "service", "checks"}, set(body))
        self.assertEqual(health_module.SERVICE, body["service"])
        self.assertEqual(set(health_module.CHECK_KEYS), set(body["checks"]))
        for value in body["checks"].values():
            self.assertIn(value, health_module.CHECK_VALUES)
        # No path, no version, no credential and no dependency address leaks out.
        text = response.body.decode()
        for secret in (TOKEN, CHAT_TOKEN, PERSONA_TOKEN, self.logs.path):
            self.assertNotIn(secret, text)

    def test_readiness_reports_ready_only_when_every_local_check_passed(self):
        body = call(self.app, "/health/ready", token=TOKEN).json()
        self.assertEqual("ok", body["checks"]["configuration"])
        self.assertEqual("ok", body["checks"]["logs"])
        self.assertEqual("ok", body["checks"]["runtime"])
        # Nothing remote was exercised in this batch, so no dependency may claim verification.
        self.assertEqual("not_verified", body["checks"]["dependencies"])
        self.assertEqual("ready", body["status"])

    def test_an_unauthenticated_probe_changes_nothing_at_all(self):
        before_logs, before_db, before_head = (
            self.logs.digest(),
            database_digest(self.h.core.store),
            head(self.h.core.store),
        )
        for token in (None, "wrong", CHAT_TOKEN):
            call(self.app, "/health/ready", token=token)
        self.assertEqual(before_logs, self.logs.digest())
        self.assertEqual(before_db, database_digest(self.h.core.store))
        self.assertEqual(before_head, head(self.h.core.store))


class ReadinessTruthTests(unittest.TestCase):
    """Readiness tells the truth about the three local conditions it can actually see."""

    def setUp(self):
        self.logs = _LogDirectory(self)
        self.h = Harness(silence_ms=0)
        self.addCleanup(lambda: asyncio.run(self.h.core.close()))
        self.environment = mock.patch.dict(os.environ, {health_module.TOKEN_ENV: TOKEN})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.app = self.logs.attach(create_app(self.h.core, {"nonebot": CHAT_TOKEN}, None))

    def test_a_non_durable_log_channel_makes_the_process_not_ready(self):
        # No configured log directory means no durable channel, which is its own verdict -
        # never silently `ok` just because records happen to reach standard error.
        with mock.patch.dict(os.environ, {obs.ENVIRONMENT_LOG_DIR: ""}):
            app = self.logs.attach(create_app(self.h.core, {"nonebot": CHAT_TOKEN}, None))
        body = call(app, "/health/ready", token=TOKEN).json()
        self.assertEqual("non_durable", body["checks"]["logs"])
        self.assertEqual("not_ready", body["status"])

    def test_a_capacity_exhausted_log_makes_the_process_not_ready(self):
        self.logs.adapter.capacity_exhausted = True
        body = call(self.app, "/health/ready", token=TOKEN).json()
        self.assertEqual("failed", body["checks"]["logs"])
        self.assertEqual("not_ready", body["status"])

    def test_an_exhausted_log_also_refuses_new_business_without_resending_anything(self):
        self.logs.adapter.capacity_exhausted = True
        response = call(
            self.app, "/internal/v1/conversation/ingest", token=CHAT_TOKEN, method="POST"
        )
        self.assertEqual(503, response.status_code)
        self.assertEqual("exhausted", response.headers.get("x-tianshu-log-capacity"))
        self.assertEqual("close", response.headers.get("connection"))
        # The refusal happened before admission: nothing was queued, so nothing can be retried.
        self.assertEqual([], self.h.turns())

    def _file_harness(self):
        """A file-backed store, because an owner lock only exists for a real database file."""
        holder = tempfile.TemporaryDirectory(prefix="tianshu-health-db-")
        self.addCleanup(holder.cleanup)
        h = Harness(path=str(Path(holder.name) / "health.db"), silence_ms=0)
        self.addCleanup(lambda: asyncio.run(h.core.close()))
        return h, self.logs.attach(create_app(h.core, {"nonebot": CHAT_TOKEN}, None))

    def test_a_file_store_that_lost_its_owner_lock_is_reported_as_failed(self):
        # A file-backed store holds its single-owner lock in an open handle; losing that handle
        # means another process may own the database, so this one may not claim to serve.
        h, app = self._file_harness()
        self.assertEqual("ok", call(app, "/health/ready", token=TOKEN).json()["checks"]["runtime"])
        h.core.store._lock = None
        body = call(app, "/health/ready", token=TOKEN).json()
        self.assertEqual("failed", body["checks"]["runtime"])
        self.assertEqual("not_ready", body["status"])

    def test_a_released_lock_file_handle_is_reported_as_failed(self):
        h, app = self._file_harness()
        released = h.core.store._lock
        released.close()

        body = call(app, "/health/ready", token=TOKEN).json()
        self.assertEqual("failed", body["checks"]["runtime"])
        self.assertEqual("not_ready", body["status"])

    def test_an_unreadable_database_is_reported_as_failed(self):
        original = self.h.core.store.db

        class Broken:
            def execute(self, *args, **kwargs):
                raise RuntimeError("database is gone")

        self.h.core.store.db = Broken()
        self.addCleanup(setattr, self.h.core.store, "db", original)
        body = call(self.app, "/health/ready", token=TOKEN).json()
        self.assertEqual("failed", body["checks"]["runtime"])
        self.assertEqual("not_ready", body["status"])

    def test_the_probe_reads_the_database_without_writing_a_log_record(self):
        before = self.logs.digest()
        for _ in range(3):
            call(self.app, "/health/ready", token=TOKEN)
            call(self.app, "/health/live")
        self.assertEqual(before, self.logs.digest())
        self.assertEqual([], self.h.turns())

    def test_a_probe_never_creates_the_log_directory_it_reports_on(self):
        missing = Path(tempfile.mkdtemp(prefix="tianshu-absent-")) / "not-created"
        with mock.patch.dict(os.environ, {obs.ENVIRONMENT_LOG_DIR: str(missing)}):
            h = Harness(silence_ms=0)
            self.addCleanup(lambda: asyncio.run(h.core.close()))
            app = self.logs.attach(create_app(h.core, {"nonebot": CHAT_TOKEN}, None))
            body = call(app, "/health/ready", token=TOKEN).json()
        self.assertEqual("ok", body["checks"]["logs"])
        self.assertFalse(missing.exists(), "a probe must not create the directory it reports on")
        app.state.log.close()

    def test_the_probe_never_ticks_the_scheduler_or_snapshots_life_state(self):
        before = database_digest(self.h.core.store)
        before_head = head(self.h.core.store)
        for _ in range(5):
            call(self.app, "/health/ready", token=TOKEN)
        self.assertEqual(before, database_digest(self.h.core.store))
        self.assertEqual(before_head, head(self.h.core.store))
        # A tick would have admitted work or advanced a watermark; neither happened.
        self.assertEqual([], self.h.core.store.list("turns"))

    def test_no_probe_path_falls_through_to_an_application_route(self):
        # Trailing slashes, query strings and other verbs must not reach a business handler.
        # A redirect is the framework normalising the path, never a business answer.
        for path in ("/health/live/", "/health/ready/", "/health/live?x=1", "/health", "/healthz"):
            response = call(self.app, path, token=TOKEN)
            self.assertIn(response.status_code, (200, 307, 404, 405), path)
            body = response.body.decode()
            self.assertNotIn(TOKEN, body)
            self.assertNotIn("checks", body)
        for method in ("POST", "PUT", "DELETE"):
            response = call(self.app, "/health/live", method=method)
            self.assertEqual(405, response.status_code, method)
        # Following the normalising redirect lands on the probe itself, not on a business route.
        followed = call(self.app, "/health/live/", token=TOKEN)
        self.assertLessEqual(len(followed.body), len('{"status":"alive"}'))

    def test_the_contract_check_vocabulary_is_the_frozen_one(self):
        self.assertEqual(
            ("configuration", "logs", "runtime", "dependencies"), health_module.CHECK_KEYS
        )
        self.assertEqual(
            {"ok", "failed", "not_configured", "not_verified", "non_durable"},
            set(health_module.CHECK_VALUES),
        )
        self.assertEqual(("/health/live", "/health/ready"), health_module.ENDPOINTS)
        self.assertEqual("TIANSHU_DIAGNOSTICS_TOKEN", health_module.TOKEN_ENV)


class ReadinessIsolationTests(unittest.TestCase):
    """Readiness must not be reachable through, or reachable by, another service's credential."""

    def test_each_credential_is_distinct_or_startup_fails(self):
        h = Harness(silence_ms=0)
        self.addCleanup(lambda: asyncio.run(h.core.close()))
        with self.assertRaises(ValueError):
            create_app(h.core, {"nonebot": TOKEN, "persona_admin": TOKEN}, None)


if __name__ == "__main__":
    unittest.main()
