import ast
import copy
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from support import Harness
from tianshu_companion.app import create_app
from tianshu_companion.clients import JsonService, Memory, Origins, Sender, command, utc
from tianshu_companion.contracts import Fault
from tianshu_companion.store import Store
from tianshu_nonebot import Bridge, normalize_onebot, normalize_telegram


class BoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness(silence_ms=0)
        self.clients = []

    async def asyncTearDown(self):
        await self.h.core.close()
        for client in self.clients:
            await client.close()

    def service(self, handler):
        client = JsonService(
            "https://synthetic.invalid",
            "synthetic-service-credential",
            transport=httpx.MockTransport(handler),
        )
        self.clients.append(client)
        return client

    async def test_http_unauthorized_extra_fields_and_unconfigured(self):
        h = self.h
        with self.assertRaises(ValueError):
            create_app(h.core, {"nonebot": "same", "platform": "same"})
        app = create_app(h.core, {"nonebot": "fixture-only"})
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://fixture"
        ) as client:
            body = h.request()
            response = await client.post("/internal/v1/conversation/ingest", json=body)
            self.assertEqual(401, response.status_code)
            body["trusted_context"] = {"admin": True}
            response = await client.post(
                "/internal/v1/conversation/ingest",
                json=body,
                headers={"Authorization": "Bearer fixture-only"},
            )
            self.assertEqual(400, response.status_code)
            del body["trusted_context"]
            response = await client.post(
                "/internal/v1/conversation/ingest",
                json=body,
                headers={"Authorization": "Bearer fixture-only"},
            )
            self.assertEqual(200, response.status_code)
            h.contracts.check("conversation#ingest_response", response.json())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app()), base_url="http://fixture"
        ) as client:
            response = await client.post("/internal/v1/conversation/ingest", json={})
            self.assertEqual(503, response.status_code)

    async def test_real_origin_adapter_checks_issuer_audience_expiration_and_correlation(self):
        h = self.h
        request = h.request()
        ctx = h.origins.values[request["command"]["origin"]["assertion_ref"]]

        def handler(incoming):
            body = json.loads(incoming.content)
            self.assertEqual("/internal/v1/origins/resolve", incoming.url.path)
            return httpx.Response(
                200, json=dict(schema_version=1, request_id=body["request_id"], context=ctx)
            )

        adapter = Origins(h.contracts, {"nonebot": ("nonebot", self.service(handler))})
        value = await adapter.resolve("nonebot", request["command"], h.clock())
        self.assertEqual(ctx, value)
        for key, wrong in [
            ("issuer", "platform"),
            ("audience_service", "memory"),
            ("assertion_ref", "wrong:ref"),
        ]:
            original = ctx[key]
            ctx[key] = wrong
            with self.assertRaises(Fault):
                await adapter.resolve("nonebot", request["command"], h.clock())
            ctx[key] = original
        ctx["expires_at"] = utc(h.clock() - 1)
        with self.assertRaises(Fault):
            await adapter.resolve("nonebot", request["command"], h.clock())

    async def test_memory_registration_and_zero_budget_version_validation(self):
        h = self.h
        origin = h.request()["command"]["origin"]
        calls = []
        scope = dict(
            actor_id="actor:a",
            person_id="person:issued",
            audience="self_private",
            conversation_id="conv:issued",
        )
        response_override = {}

        def handler(incoming):
            body = json.loads(incoming.content)
            calls.append((incoming.url.path, body))
            if incoming.url.path.endswith("/resolve"):
                result = dict(
                    schema_version=1,
                    request_id=body["query"]["request_id"],
                    state="unregistered",
                    person_id=None,
                    binding_version=0,
                )
            elif incoming.url.path.endswith("/register"):
                result = dict(
                    schema_version=1,
                    request_id=body["command"]["request_id"],
                    person_id="person:issued",
                    binding_version=1,
                    created=True,
                )
            else:
                result = dict(
                    schema_version=1,
                    request_id=body["query"]["request_id"],
                    effective_scope=scope,
                    scope_version=1,
                    verified_at=utc(h.clock()),
                    valid_until=utc(h.clock() + 3600),
                    selected_units=[],
                    dependency_groups=[],
                    budget_used=dict(tokens=0, bytes=0),
                    omissions=["no_match"],
                )
                result.update(response_override)
            return httpx.Response(200, json=result)

        memory = Memory(h.contracts, self.service(handler))
        identity = await memory.identity(
            origin, dict(namespace="qq", immutable_account_id="first"), h.clock()
        )
        self.assertEqual(("person:issued", 1), identity)
        self.assertEqual(2, len(calls))
        await memory.select(origin, scope, "hello", dict(tokens=0, bytes=0), 1)
        response_override["scope_version"] = 2
        with self.assertRaises(Fault) as error:
            await memory.select(origin, scope, "hello", dict(tokens=0, bytes=0), 1)
        self.assertEqual("scope_changed", error.exception.code)
        response_override.clear()
        response_override["effective_scope"] = {**scope, "person_id": "person:other"}
        with self.assertRaises(Fault):
            await memory.select(origin, scope, "hello", dict(tokens=0, bytes=0))

    async def test_unconfigured_sender_is_failed_without_false_attempt(self):
        h = self.h
        client = JsonService()
        self.clients.append(client)
        h.core.sender = Sender(h.contracts, client)
        await h.ingest()
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertTrue(all(r["request"] is None for r in h.core.store.list("replies")))

    async def test_memory_full_groups_budget_and_group_visibility(self):
        h = self.h
        origin = h.request()["command"]["origin"]
        scope = dict(
            actor_id="actor:a",
            person_id="person:test",
            audience="group",
            conversation_id="conv:test",
        )
        unit = dict(
            record_id="record:one",
            record_version=1,
            semantic_group_id="semantic:one",
            subject_person_id="person:test",
            statement="白天偶尔喝咖啡，晚上不喝",
            conditions=["白天"],
            negations=["晚上不喝"],
            valid_time="current",
            uncertainty="confirmed",
            reality="real",
            visibility="shared_projection",
            sources=[
                dict(
                    kind="shareable_projection",
                    owner="memory",
                    projection_ref="projection:one",
                    projection_version=1,
                )
            ],
        )
        response = dict(
            schema_version=1,
            effective_scope=scope,
            scope_version=1,
            verified_at=utc(h.clock()),
            valid_until=utc(h.clock() + 3600),
            selected_units=[unit],
            dependency_groups=[
                dict(semantic_group_id="semantic:one", record_ids=["record:one"], complete=True)
            ],
            budget_used=dict(tokens=80, bytes=800),
            omissions=[],
        )

        def handler(request):
            return httpx.Response(
                200,
                json={**response, "request_id": json.loads(request.content)["query"]["request_id"]},
            )

        memory = Memory(h.contracts, self.service(handler))
        selected = await memory.select(origin, scope, "coffee", dict(tokens=2048, bytes=8192))
        self.assertEqual(["晚上不喝"], selected["selected_units"][0]["negations"])
        response["dependency_groups"][0]["record_ids"].append("record:missing")
        with self.assertRaises(Fault):
            await memory.select(origin, scope, "coffee", dict(tokens=2048, bytes=8192))
        response["dependency_groups"][0]["record_ids"].pop()
        with self.assertRaises(Fault) as error:
            await memory.select(origin, scope, "coffee", dict(tokens=10, bytes=8192))
        self.assertEqual("budget_exceeded", error.exception.code)
        unit["visibility"] = "self_private"
        with self.assertRaises(Fault) as error:
            await memory.select(origin, scope, "coffee", dict(tokens=2048, bytes=8192))
        self.assertEqual("forbidden", error.exception.code)

    async def test_blocked_scope_early_failure_releases_slot_and_requires_memory_check(self):
        h = self.h
        await h.ingest()
        h.core.config_version = None
        await h.cycles()
        self.assertEqual("failed", h.turns()[0]["phase"])
        self.assertEqual("blocked_scope", h.core.store.list("outbox")[0]["state"])
        h.memory.unavailable = True
        await h.core.flush_outbox()
        self.assertFalse(h.memory.commits)

        async def validated(turn, sources):
            self.assertEqual(1, len(sources))
            return 7

        h.memory.check_sources = validated
        h.clock.advance(2)
        await h.core.flush_outbox()
        self.assertEqual(7, h.memory.commits[0]["scope_version"])

    def test_store_single_owner_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "owned.db"
            store = Store(path)
            try:
                with self.assertRaises(RuntimeError):
                    Store(path)
            finally:
                store.close()


class PersonaBoundaryTests(unittest.TestCase):
    """Module boundaries for persona management, checked against the source itself.

    These are structural assertions, not behaviour: a wrong import direction or a table name
    spelled out in the wrong module is the kind of change that silently re-creates a second
    owner of one rule. The behaviour of each rule is covered in `test_personas.py`.
    """

    def source(self, name):
        return (Path(__file__).parents[1] / "src/tianshu_companion" / name).read_text(
            encoding="utf-8"
        )

    def imports(self, name):
        tree = ast.parse(self.source(name))
        found = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                found.add(node.module)
            elif isinstance(node, ast.Import):
                found.update(alias.name for alias in node.names)
        return found

    def test_persona_domain_owns_its_tables_and_knows_no_adapter(self):
        source = self.source("personas.py")
        # The domain never reaches back up into the orchestrator or the HTTP layer.
        self.assertFalse(
            {module for module in self.imports("personas.py") if module.endswith(("core", "app"))}
        )
        self.assertIn("persona_personas", source)
        # The only place that builds a persona table name from a caller's word.
        self.assertIn('"persona_" + table', source)

    def test_no_other_module_names_a_persona_table(self):
        tables = (
            "persona_personas",
            "persona_revisions",
            "persona_publications",
            "persona_approvals",
            "persona_rollbacks",
            "persona_imports",
            "persona_access",
            "persona_operations",
        )
        owners = {"personas.py": None, "store.py": None}
        for path in sorted(
            Path(__file__).parents[1].joinpath("src/tianshu_companion").glob("*.py")
        ):
            if path.name in owners:
                continue
            source = path.read_text(encoding="utf-8")
            for table in tables:
                self.assertNotIn(
                    f'"{table}"',
                    source,
                    f"{path.name} reaches into {table}; persona tables have one owner",
                )

    def test_core_only_takes_and_verifies_the_pinned_snapshot(self):
        source = self.source("core.py")
        # Exactly one module-level import of the persona module, and no operator vocabulary.
        self.assertIn("from .personas import PersonaError, Personas", source)
        self.assertEqual(1, source.count("from .personas import"))
        for word in (
            "published_revision",
            "draft_revision",
            "expected",
            "operator",
            "approve",
            "publish",
        ):
            self.assertNotIn(f'"{word}"', source, f"core.py re-states the persona rule {word}")

    def test_persona_cli_never_touches_a_table_and_shares_the_use_case(self):
        source = self.source("persona_cli.py")
        self.assertNotIn("store.get", source)
        self.assertNotIn("store.put", source)
        self.assertNotIn("store.list", source)
        self.assertEqual(1, source.count(".manage("))
        self.assertIn("from .store import Store", source)

    def test_only_the_persona_domain_owns_operation_idempotence(self):
        """One module decides what "the same request" means; the adapters only carry it."""
        domain = self.source("personas.py")
        self.assertIn("persona_operations", domain)
        self.assertIn("operation_key", domain)
        # The digest is what binds an identity to a request, and the ledger row is written
        # in the same transaction as the business write rather than beside it.
        self.assertIn("operation_digest", domain)
        self.assertEqual(1, domain.count("self._record_operation("))
        for name in ("core.py", "app.py", "persona_cli.py"):
            source = self.source(name)
            for word in ("request_digest", "operation_key", "persona_operations"):
                self.assertNotIn(word, source, f"{name} re-implements operation identity")
        # The HTTP adapter records which authorization surface authenticated the caller;
        # the CLI carries an identity through and never inspects it.
        self.assertIn("dict(request, scope=service)", self.source("core.py"))
        self.assertIn("request_id", self.source("persona_cli.py"))
        self.assertNotIn("request_digest", self.source("persona_cli.py"))

    def test_store_transactions_are_reentrant_and_commit_once(self):
        """A composed write - business rows plus its ledger row - is one atomic step."""
        source = self.source("store.py")
        self.assertIn("if self._in_transaction:", source)
        self.assertEqual(1, source.count('"BEGIN IMMEDIATE"'))

    def test_direct_send_path_has_no_persona_dependency(self):
        source = self.source("direct.py")
        # Functional commands keep their ordering and their own authorization facts; the
        # persona module is not part of that path at all.
        self.assertNotIn("personas", self.imports("direct.py"))
        self.assertNotIn("role =", source)
        self.assertNotIn("persona =", source)


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness()
        self.store = Store(":memory:")

    async def asyncTearDown(self):
        self.store.close()
        await self.h.core.close()

    def test_qq_and_tg_normalization_preserves_identity_targets_and_reply(self):
        h = self.h
        origin = dict(assertion_ref="origin:synthetic")
        event = dict(
            post_type="message",
            message_type="group",
            self_id=999,
            user_id=11,
            group_id=22,
            message_id=33,
            time=h.clock(),
            message=[
                dict(type="reply", data=dict(id="32")),
                dict(type="text", data=dict(text="hi")),
                dict(type="at", data=dict(qq="999")),
            ],
        )
        result = normalize_onebot(event, "qq-group", origin, targets=["actor:a"])
        h.contracts.check("conversation#ingest_request", result)
        self.assertEqual("11", result["author"]["immutable_account_id"])
        self.assertEqual("group:22", result["message_key"]["channel"]["channel_conversation_id"])
        self.assertEqual("32", result["reply_refs"][0]["message_id"])
        telegram = {
            "message": {
                "from": {"id": 11, "is_bot": False},
                "chat": {"id": -22},
                "message_id": 33,
                "message_thread_id": 9,
                "text": "TG",
                "date": h.clock(),
                "reply_to_message": {"message_id": 32},
            }
        }
        result = normalize_telegram(telegram, "tg-binding", origin)
        h.contracts.check("conversation#ingest_request", result)
        self.assertEqual("9", result["message_key"]["channel"]["thread_id"])
        event["message"].append(dict(type="image", data=dict(url="https://private.invalid")))
        with self.assertRaises(Fault):
            normalize_onebot(event, "qq-group", origin)

    async def test_unique_responsibility_and_durable_backpressure(self):
        h = self.h

        class CoreClient:
            failed = True

            async def call(self, path, request):
                if self.failed:
                    raise Fault("queue_full")
                return await h.core.ingest("nonebot", request)

        client = CoreClient()
        bridge = Bridge(self.store, h.contracts, client, destinations={}, clock=h.clock)
        direct = h.request(text="/help argument")
        self.assertEqual("direct", bridge.capture(direct, direct_commands=["/help"]))
        self.assertEqual("direct", bridge.capture(direct, direct_commands=[]))
        ordinary = h.request(text="ordinary")
        self.assertEqual("companion", bridge.capture(ordinary, direct_commands=["/help"]))
        await bridge.flush()
        self.assertEqual(1, len(self.store.list("inbox", states=["pending"])))
        self.assertFalse(h.core.store.list("inbox"))
        client.failed = False
        h.clock.advance(2)
        await bridge.flush()
        self.assertEqual(1, len(h.core.store.list("inbox")))
        self.assertEqual(1, len(self.store.list("inbox", states=["accepted"])))

    async def test_unknown_send_is_never_blindly_replayed_and_destination_is_verified(self):
        h = self.h
        destination = h.request()["message_key"]["channel"]
        native_calls = []

        async def verify(service, request):
            if service != "companion":
                raise Fault("forbidden")

        async def native(channel, text):
            native_calls.append((channel, text))
            raise OSError("connection lost after accepted")

        bridge = Bridge(
            self.store,
            h.contracts,
            None,
            destinations={"conv:test": destination},
            send_native=native,
            verify_send=verify,
            clock=h.clock,
        )
        request = dict(
            command=command(dict(assertion_ref="origin:send"), "send:key", h.clock()),
            conversation_id="conv:test",
            turn_id="turn:test",
            turn_sequence=1,
            reply_id="reply:test",
            actor_id="actor:a",
            destination=destination,
            segment_sequence=1,
            segment_count=1,
            text="text",
        )
        receipt = await bridge.send("companion", request)
        self.assertEqual("unknown", receipt["state"])
        self.assertFalse(receipt["retry_safe"])
        self.assertEqual(receipt, await bridge.send("companion", request))
        self.assertEqual(1, len(native_calls))
        alias = copy.deepcopy(request)
        alias["command"]["idempotency_key"] = "send:alias"
        self.assertEqual(receipt, await bridge.send("companion", alias))
        alias["reply_id"] = "reply:changed"
        with self.assertRaises(Fault):
            await bridge.send("companion", alias)
        conflict = copy.deepcopy(request)
        conflict["reply_id"] = "reply:other"
        with self.assertRaises(Fault):
            await bridge.send("companion", conflict)
        wrong = copy.deepcopy(request)
        wrong["destination"]["channel_conversation_id"] = "private:other"
        with self.assertRaises(Fault):
            await bridge.send("companion", wrong)

    async def test_native_telegram_receipt_does_not_invent_missing_message_id(self):
        from tianshu_nonebot.bridge import send_telegram_text

        class Bot:
            message_id = None

            async def call_api(self, method, **args):
                self.last_args = args
                return dict(message_id=self.message_id)

        bot = Bot()
        destination = dict(
            namespace="tg", binding_id="tg:fixture", channel_conversation_id="-1", thread_id="2"
        )
        with self.assertRaises(Fault):
            await send_telegram_text(bot, destination, "literal <text>")
        bot.message_id = 123
        self.assertEqual(["123"], await send_telegram_text(bot, destination, "literal <text>"))
        self.assertNotIn("parse_mode", bot.last_args)
