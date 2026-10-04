"""Published web snapshot contract, synthetic origins and isolated Core stores."""

import asyncio
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from support import FakeSender, Harness, contracts
from tianshu_companion.app import create_app
from tianshu_companion.clients import JsonService, Sender, command, uid, utc
from tianshu_companion.contracts import Fault, canonical, digest


class WebHarness(Harness):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.web_sender = FakeSender(self.clock)
        self.core.web_sender = self.web_sender
        self.core.bindings["web-self"] = dict(
            service="platform",
            namespace="web",
            audience="self_private",
            actor_ids=["actor:a", "actor:b"],
            classification=dict(
                value="real",
                basis="registered_input_mode",
                policy_ref="fixture:real",
                policy_version=1,
            ),
        )
        self.options["bindings"]["web-self"] = copy.deepcopy(self.core.bindings["web-self"])

    def web_request(self, **kwargs):
        value = self.request(**kwargs)
        channel = value["message_key"]["channel"]
        channel.update(namespace="web", binding_id="web-self")
        value["author"]["namespace"] = "web"
        context = self.origins.values[value["command"]["origin"]["assertion_ref"]]
        context.update(
            issuer="platform", authenticated_service="platform", principal_id="user:fixture"
        )
        return value

    async def submit(self, **kwargs):
        request = self.web_request(**kwargs)
        response = await self.core.ingest("platform", request)
        return request, response

    def viewer(self, request, response, **changes):
        context = copy.deepcopy(self.origins.values[request["command"]["origin"]["assertion_ref"]])
        context.update(assertion_ref=uid("viewer"), expires_at=utc(self.clock() + 60))
        context["allowed_scope"].update(
            conversation_id=response["conversation_id"], person_id=response["person_id"]
        )
        self.origins.values[context["assertion_ref"]] = context
        result = dict(
            schema_version=1,
            query=dict(
                schema_version=1,
                request_id=uid("snapshot"),
                origin=dict(assertion_ref=context["assertion_ref"]),
            ),
            deadline_at=utc(self.clock() + 30),
            actor_id=context["allowed_scope"]["actor_id"],
            conversation_id=response["conversation_id"],
            before_turn_sequence=None,
            limit=20,
        )
        result.update(changes)
        return result

    async def finish(self):
        self.clock.advance(6)
        await self.cycles(70)


class WebSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = WebHarness()

    async def asyncTearDown(self):
        await self.h.core.close()

    async def read(self, request, response, **changes):
        return await self.h.core.web_snapshot(
            "platform", self.h.viewer(request, response, **changes)
        )

    async def test_collector_then_confirmed_history_and_namespace_sender(self):
        h = self.h
        request, receipt = await h.submit(text="web private message")
        collecting = await self.read(request, receipt)
        self.assertEqual(
            "web private message", collecting["collectors"][0]["messages"][0]["parts"][0]["text"]
        )
        self.assertEqual([], collecting["history"])
        await h.finish()
        final = await self.read(request, receipt)
        self.assertEqual([], final["collectors"])
        self.assertEqual([], final["active_turns"])
        self.assertTrue(
            all(r["content_state"] == "available" for r in final["history"][0]["replies"])
        )
        self.assertEqual(2, len(h.web_sender.calls))
        self.assertEqual([], h.sender.calls)
        encoded = canonical(final)
        for secret in ("assertion_ref", "source", "route_receipt", "person_id", "credential"):
            self.assertNotIn(secret, encoded)

    async def test_history_pagination_and_live_group_independent(self):
        h = self.h
        for n in range(3):
            request, receipt = await h.submit(text=f"message {n}")
            await h.finish()
        await h.submit(text="still collecting")
        page = await self.read(request, receipt, limit=2)
        self.assertEqual([3, 2], [x["turn"]["turn_sequence"] for x in page["history"]])
        self.assertEqual(2, page["next_before_turn_sequence"])
        older = await self.read(request, receipt, limit=2, before_turn_sequence=2)
        self.assertEqual([1], [x["turn"]["turn_sequence"] for x in older["history"]])
        self.assertIsNone(older["next_before_turn_sequence"])
        self.assertEqual(page["collectors"], older["collectors"])

    async def test_actor_account_channel_scope_isolation(self):
        h = self.h
        request, receipt = await h.submit(text="actor A")
        other, other_receipt = await h.submit(actor="actor:b", text="actor B")
        await h.submit(account="other", channel="private:other", text="other user")
        await h.finish()
        result = await self.read(request, receipt)
        self.assertEqual(1, len(result["history"]))
        self.assertEqual("actor A", result["history"][0]["messages"][0]["parts"][0]["text"])
        self.assertEqual(1, len((await self.read(other, other_receipt))["history"]))
        for change in ({"actor_id": "actor:b"}, {"conversation_id": "conv:wrong"}):
            with self.assertRaises(Fault) as error:
                await self.read(request, receipt, **change)
            self.assertEqual("forbidden", error.exception.code)
        query = h.viewer(request, receipt)
        ctx = h.origins.values[query["query"]["origin"]["assertion_ref"]]
        ctx["verified_account"]["immutable_account_id"] = "other"
        ctx["allowed_scope"]["person_id"] = None
        with self.assertRaises(Fault) as error:
            await h.core.web_snapshot("platform", query)
        self.assertEqual("forbidden", error.exception.code)

    async def test_current_origin_twice_checked_but_old_expiry_and_global_memory_irrelevant(self):
        h = self.h
        request, receipt = await h.submit()
        await h.finish()
        h.origins.values[request["command"]["origin"]["assertion_ref"]]["expires_at"] = utc(
            h.clock() - 1
        )
        h.memory.scope_version += 10
        result = await self.read(request, receipt)
        self.assertEqual("available", result["history"][0]["replies"][0]["content_state"])
        query = h.viewer(request, receipt)
        original = h.origins.resolve
        calls = 0

        async def resolve(service, envelope, now):
            nonlocal calls
            calls += 1
            result = await original(service, envelope, now)
            if calls == 2:
                result["expires_at"] = utc(h.clock() - 1)
            return result

        with patch.object(h.origins, "resolve", resolve), self.assertRaises(Fault):
            await h.core.web_snapshot("platform", query)
        self.assertEqual(2, calls)

    async def test_source_edit_retraction_preserve_sent_fact_and_clear_derived_text(self):
        h = self.h
        request, receipt = await h.submit(message="msg:one", text="old secret")
        await h.finish()
        # A second delivered reply used first turn as local short context.
        second, second_receipt = await h.submit(text="follow up")
        await h.finish()
        await h.submit(message="msg:one", revision=2, kind="edit", text="replacement")
        result = await self.read(second, second_receipt)
        first = result["history"][-1]
        self.assertEqual("edited", first["messages"][0]["state"])
        self.assertEqual([], first["messages"][0]["parts"])
        for turn in result["history"]:
            for reply in turn["replies"]:
                self.assertEqual("sent", reply["state"])
                self.assertEqual("unavailable", reply["content_state"])
                self.assertIsNone(reply["text"])
        await h.submit(message="msg:one", revision=3, kind="retract")
        result = await self.read(request, receipt)
        self.assertEqual("retracted", result["history"][-1]["messages"][0]["state"])

    async def test_cancelled_unsent_drafts_omitted_partial_sent_preserved(self):
        h = self.h
        h.web_sender.states = ["sent", "failed"]
        request, receipt = await h.submit()
        await h.finish()
        result = await self.read(request, receipt)
        self.assertEqual("partial", result["history"][0]["turn"]["delivery_state"])
        self.assertEqual(
            ["available", "unavailable"],
            [r["content_state"] for r in result["history"][0]["replies"]],
        )
        turn = h.turns()[0]
        # Exact never-attempted cancellation fixture; no delivery fact is synthesized.
        replies = h.core._replies(turn)
        extra = dict(
            replies[-1],
            id="reply:cancelled",
            state="cancelled",
            attempted_at=None,
            request=None,
            receipt=None,
        )
        h.core.store.put("replies", extra)
        self.assertEqual(2, len((await self.read(request, receipt))["history"][0]["replies"]))
        extra["attempted_at"] = h.clock()
        h.core.store.put("replies", extra)
        with self.assertRaises(Fault) as error:
            await self.read(request, receipt)
        self.assertEqual("dependency_unavailable", error.exception.code)

    async def test_unknown_sending_never_expose_generated_text(self):
        h = self.h
        h.web_sender.states = ["unknown"]
        request, receipt = await h.submit()
        await h.finish()
        result = await self.read(request, receipt)
        active = result["active_turns"][0]
        self.assertEqual("unknown", active["replies"][0]["state"])
        self.assertTrue(all(r["text"] is None for r in active["replies"]))
        reply = h.core._replies(h.turns()[0])[0]
        reply["state"] = "sending"
        h.core.store.put("replies", reply)
        result = await self.read(request, receipt)
        self.assertEqual("sending", result["active_turns"][0]["replies"][0]["state"])
        self.assertIsNone(result["active_turns"][0]["replies"][0]["text"])

    async def test_missing_web_sender_does_not_fallback_to_nonebot(self):
        h = self.h
        h.core.web_sender = None
        request, receipt = await h.submit()
        await h.finish()
        self.assertEqual([], h.sender.calls)
        self.assertEqual([], h.web_sender.calls)
        self.assertEqual(
            "failed", (await self.read(request, receipt))["history"][0]["turn"]["phase"]
        )

    async def test_quarantine_deadline_nonweb_and_binding_change_fail_closed(self):
        h = self.h
        request, receipt = await h.submit()
        with self.assertRaises(Fault) as error:
            await self.read(request, receipt, deadline_at=utc(h.clock()))
        self.assertEqual("timeout", error.exception.code)
        original_identity = h.memory.identity

        async def changed(*args):
            person, version = await original_identity(*args)
            return person, version + 1

        with patch.object(h.memory, "identity", changed), self.assertRaises(Fault) as error:
            await self.read(request, receipt)
        self.assertEqual("scope_changed", error.exception.code)
        conv = h.core.store.get("conversations", digest(request["message_key"]["channel"]))
        conv["source_quarantined"] = True
        h.core.store.put("conversations", conv)
        with self.assertRaises(Fault) as error:
            await self.read(request, receipt)
        self.assertEqual("dependency_unavailable", error.exception.code)
        qq = h.request(channel="qq:conversation")
        qq_receipt = await h.core.ingest("nonebot", qq)
        with self.assertRaises(Fault):
            await self.read(qq, qq_receipt)

    async def test_http_only_platform_credentials_and_no_scope_injection(self):
        h = self.h
        request, receipt = await h.submit()
        transport = httpx.ASGITransport(
            app=create_app(h.core, {"platform": "platform-fixture", "nonebot": "qq-fixture"})
        )
        async with httpx.AsyncClient(transport=transport, base_url="http://fixture") as client:
            query = h.viewer(request, receipt)
            for token, code in [(None, 401), ("qq-fixture", 403), ("platform-fixture", 200)]:
                response = await client.post(
                    "/internal/v1/conversation/web-snapshot",
                    json=query,
                    headers={"Authorization": "Bearer " + token} if token else {},
                )
                self.assertEqual(code, response.status_code, response.text)
            query["scope"] = {"person_id": "other"}
            response = await client.post(
                "/internal/v1/conversation/web-snapshot",
                json=query,
                headers={"Authorization": "Bearer platform-fixture"},
            )
            self.assertEqual(400, response.status_code)

    async def test_full_group_bounds_no_silent_truncation(self):
        h = self.h
        request, receipt = await h.submit()
        collection = h.core.store.list("collections")[0]
        original = copy.deepcopy(collection)
        # Same message expanded for boundary fixture; no source content is fabricated as current.
        collection["messages"] *= 257
        h.core.store.put("collections", collection)
        with self.assertRaises(Fault) as error:
            await self.read(request, receipt)
        self.assertEqual("budget_exceeded", error.exception.code)
        h.core.store.put("collections", original)
        with (
            patch("tianshu_companion.web_snapshot.MAX_BYTES", 1),
            self.assertRaises(Fault) as error,
        ):
            await self.read(request, receipt)
        self.assertEqual("budget_exceeded", error.exception.code)

    async def test_restart_preserves_web_delivery_and_current_viewer(self):
        with tempfile.TemporaryDirectory() as directory:
            h = WebHarness(Path(directory) / "isolated.db")
            try:
                request, receipt = await h.submit()
                await h.finish()
                await h.core.close()
                h.core = h.new_core()
                h.core.web_sender = h.web_sender
                h.core.recover()
                result = await h.core.web_snapshot("platform", h.viewer(request, receipt))
                self.assertEqual("sent", result["history"][0]["replies"][0]["state"])
                self.assertEqual(2, len(h.web_sender.calls))
            finally:
                await h.core.close()

    async def test_sender_real_adapter_is_separately_service_authenticated(self):
        h = self.h
        received = []

        def handler(request):
            self.assertEqual("Bearer companion-web-fixture", request.headers["Authorization"])
            body = json.loads(request.content)
            h.contracts.check("conversation#send_request", body)
            self.assertEqual("web", body["destination"]["namespace"])
            received.append(body)
            return httpx.Response(200, json=h.web_sender.receipt(body))

        client = JsonService(
            "https://platform.synthetic.invalid",
            "companion-web-fixture",
            transport=httpx.MockTransport(handler),
        )
        h.core.web_sender = Sender(contracts(), client)
        try:
            request, receipt = await h.submit()
            await h.finish()
            self.assertEqual(2, len(received))
            self.assertEqual(
                "available",
                (await self.read(request, receipt))["history"][0]["replies"][0]["content_state"],
            )
        finally:
            await client.close()

    async def test_cancel_during_real_send_keeps_late_sent_fact(self):
        h = self.h
        h.web_sender.gate = asyncio.Event()
        request, receipt = await h.submit()
        await h.finish()
        self.assertEqual(1, len(h.web_sender.calls))
        turn = h.turns()[0]
        await h.core.cancel(
            "platform",
            dict(
                command=command(turn["origin"], uid("cancel"), h.clock()),
                conversation_id=turn["conversation_id"],
                turn_id=turn["id"],
                expected_version=turn["version"],
                reason="explicit_user_cancel",
            ),
        )
        active = (await self.read(request, receipt))["active_turns"][0]
        self.assertEqual(1, len(active["replies"]))
        self.assertEqual("sending", active["replies"][0]["state"])
        self.assertIsNone(active["replies"][0]["text"])
        h.web_sender.gate.set()
        await h.cycles(60)
        history = (await self.read(request, receipt))["history"][0]
        self.assertEqual("partial", history["turn"]["delivery_state"])
        self.assertEqual("cancelled", history["turn"]["phase"])
        self.assertEqual("sent", history["replies"][0]["state"])
        self.assertEqual("available", history["replies"][0]["content_state"])
        self.assertEqual(1, len(h.web_sender.calls))

    async def test_permission_revocation_after_delivery_invalidates_only_content(self):
        h = self.h
        request, receipt = await h.submit()
        await h.finish()
        turn = h.turns()[0]
        await h.core.cancel(
            "platform",
            dict(
                command=command(turn["origin"], uid("revoke"), h.clock()),
                conversation_id=turn["conversation_id"],
                turn_id=turn["id"],
                expected_version=turn["version"],
                reason="permission_revoked",
            ),
        )
        result = await self.read(request, receipt)
        self.assertEqual("sent", result["history"][0]["turn"]["phase"])
        self.assertEqual([], result["history"][0]["messages"][0]["parts"])
        for reply in result["history"][0]["replies"]:
            self.assertEqual("sent", reply["state"])
            self.assertIsNone(reply["text"])

    async def test_unrelated_source_edit_does_not_hide_other_actor_history(self):
        h = self.h
        request, receipt = await h.submit(text="actor A")
        await h.finish()
        await h.submit(actor="actor:b", message="msg:b", text="actor B")
        await h.finish()
        await h.submit(actor="actor:b", message="msg:b", revision=2, kind="edit", text="B edit")
        result = await self.read(request, receipt)
        self.assertEqual("available", result["history"][0]["replies"][0]["content_state"])

    async def test_full_256_message_group_is_complete(self):
        h = self.h
        for i in range(256):
            request, receipt = await h.submit(message=f"msg:{i}", text=f"entry {i}")
        result = await self.read(request, receipt)
        self.assertEqual(256, len(result["collectors"][0]["messages"]))
        self.assertEqual("entry 255", result["collectors"][0]["messages"][-1]["parts"][0]["text"])

    async def test_current_identity_changes_between_checks(self):
        h = self.h
        request, receipt = await h.submit()
        original = h.memory.identity
        calls = 0

        async def identity(*args):
            nonlocal calls
            calls += 1
            person, version = await original(*args)
            return person, version if calls == 1 else version + 1

        with patch.object(h.memory, "identity", identity), self.assertRaises(Fault) as error:
            await self.read(request, receipt)
        self.assertEqual("scope_changed", error.exception.code)

    async def test_nonweb_tg_keeps_existing_sender_path(self):
        h = self.h
        request = h.request()
        request["message_key"]["channel"]["namespace"] = "tg"
        request["author"]["namespace"] = "tg"
        h.core.bindings["qq-private"]["namespace"] = "tg"
        await h.core.ingest("nonebot", request)
        await h.finish()
        self.assertEqual(2, len(h.sender.calls))
        self.assertEqual([], h.web_sender.calls)
        self.assertEqual("tg", h.sender.calls[0]["destination"]["namespace"])
