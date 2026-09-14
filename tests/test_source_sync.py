"""Synthetic product regression tests; remote services here are explicit fixtures."""

import asyncio
import copy
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from support import Harness
from tianshu_companion.app import create_app
from tianshu_companion.clients import JsonService, Memory, Origins, uid, utc
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.source_sync import INPUT_FIELDS, physical_key, selector


class SourceHarness(Harness):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.authorities = {}
        self.origins.input_access = self.input_access

    def fanout(
        self, text="你好", *, targets=("actor:a", "actor:b"), defaults=(), grants=None, **kw
    ):
        old = self.request(text=text, **kw)
        data = {k: old[k] for k in INPUT_FIELDS}
        request = dict(
            schema_version=1, command=old["command"], input=data, target_actor_ids=list(targets)
        )
        ref = request["command"]["origin"]["assertion_ref"]
        original = self.origins.values[ref]
        contexts = []
        for actor in grants if grants is not None else ["actor:a", "actor:b"]:
            context = copy.deepcopy(original)
            context.update(issuer="platform", assertion_ref=uid("actor-origin"))
            context["allowed_scope"]["actor_id"] = actor
            self.origins.values[context["assertion_ref"]] = context
            contexts.append(context)
        self.authorities[ref] = dict(
            expires_at=utc(self.clock() + 3600),
            default_actor_ids=list(defaults),
            routing_version=1,
            actor_contexts=contexts,
            audience=original["allowed_scope"]["audience"],
        )
        return request

    async def input_access(self, service, request):
        assert not self.core.store.db.in_transaction
        return copy.deepcopy(self.authorities[request["command"]["origin"]["assertion_ref"]])

    async def submit(self, request, defer=False):
        return await self.core.ingest_actors("nonebot", request, defer_processing=defer)

    def facts(
        self, requests=(), actors=("actor:a", "actor:b"), *, content=True, turns=(), head=False
    ):
        request = dict(
            schema_version=1,
            request_id=uid("read"),
            mode="head" if head else "snapshot",
            selectors=[] if head else [selector(r["input"], a) for r in requests for a in actors],
            turn_ids=list(turns),
            include_content=False if head else content,
        )
        return self.core.source_facts("memory", request)


class SourceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = SourceHarness()

    async def asyncTearDown(self):
        await self.h.core.close()

    async def test_same_physical_fanout_and_other_messages_isolate_actor_collectors(self):
        h = self.h
        first = h.fanout(group=True, channel="group:synthetic")
        response = await h.submit(first)
        second = h.fanout(text="第二条", targets=["actor:a"], group=True, channel="group:synthetic")
        await h.submit(second)
        self.assertEqual(2, len(h.core.store.list("physicals")))
        self.assertEqual(3, len(h.core.store.list("inbox")))
        self.assertEqual(["actor:a", "actor:b"], response["effective_actor_ids"])
        a, b = response["outcomes"]
        self.assertNotEqual(a["receipt"]["receipt_id"], b["receipt"]["receipt_id"])
        self.assertNotEqual(a["receipt"]["collection_id"], b["receipt"]["collection_id"])
        self.assertEqual(
            a["admission"]["physical_receipt_id"], b["admission"]["physical_receipt_id"]
        )
        self.assertEqual(a["receipt"]["person_id"], b["receipt"]["person_id"])
        self.assertEqual([1, 2], [o["receipt"]["ingest_sequence"] for o in response["outcomes"]])
        h.clock.advance(5)
        await h.cycles(60)
        self.assertEqual([2, 1], [len(t["bundle"]["messages"]) for t in h.turns()])
        self.assertEqual(
            ["actor:a", "actor:a", "actor:b", "actor:b"], [r["actor_id"] for r in h.sender.calls]
        )
        self.assertEqual(1, len({t["conversation_id"] for t in h.turns()}))

    async def test_actor_dedup_does_not_swallow_first_b_or_alias_receipt(self):
        h = self.h
        request = h.fanout(targets=["actor:a"])
        first = await h.submit(request)
        request = copy.deepcopy(request)
        request["command"]["idempotency_key"] = uid("fanout")
        request["target_actor_ids"] = ["actor:b", "actor:a"]
        second = await h.submit(request)
        self.assertTrue(second["physical_deduplicated"])
        self.assertEqual(["duplicate", "accepted"], [o["state"] for o in second["outcomes"]])
        self.assertEqual(
            first["outcomes"][0]["receipt"]["receipt_id"],
            second["outcomes"][0]["receipt"]["receipt_id"],
        )
        self.assertEqual(2, len(h.core.store.list("inbox")))
        self.assertEqual(1, len(h.core.store.list("physicals")))

    async def test_empty_defaults_frozen_retry_and_revoked_receipt_is_hidden(self):
        h = self.h
        request = h.fanout(targets=[], defaults=["actor:a"])
        first = await h.submit(request)
        authority = h.authorities[request["command"]["origin"]["assertion_ref"]]
        authority.update(default_actor_ids=["actor:a", "actor:b"], routing_version=2)
        request["command"]["request_id"] = uid("retry")
        second = await h.submit(request)
        self.assertEqual(["actor:a"], second["effective_actor_ids"])
        self.assertEqual(1, second["routing_version"])
        self.assertEqual(
            first["outcomes"][0]["receipt"]["receipt_id"],
            second["outcomes"][0]["receipt"]["receipt_id"],
        )
        self.assertEqual(digest(request), second["request_digest"])
        authority["actor_contexts"] = [
            c for c in authority["actor_contexts"] if c["allowed_scope"]["actor_id"] == "actor:b"
        ]
        third = await h.submit(request)
        self.assertEqual(
            [dict(actor_id="actor:a", state="forbidden", receipt=None, admission=None)],
            third["outcomes"],
        )
        self.assertIsNone(third["person_id"])
        self.assertEqual(1, len(h.core.store.list("inbox")))

    async def test_no_routes_and_partial_permission_do_not_invent_identity(self):
        h = self.h
        unassigned = await h.submit(h.fanout(targets=[], defaults=[], grants=[]))
        self.assertEqual("unrouted", unassigned["routing_state"])
        self.assertIsNone(unassigned["person_id"])
        self.assertFalse(h.memory.accounts)
        response = await h.submit(h.fanout(grants=["actor:a"]))
        self.assertEqual(["accepted", "forbidden"], [o["state"] for o in response["outcomes"]])
        self.assertIsNone(response["outcomes"][1]["admission"])

    async def test_actor_context_binding_and_identity_deadline_are_rechecked(self):
        h = self.h
        for field, value in [
            ("issuer", "nonebot"),
            ("audience_service", "memory"),
            ("authenticated_service", "platform"),
            ("revoked", True),
        ]:
            with self.subTest(field=field):
                request = h.fanout(targets=["actor:a"], grants=["actor:a"])
                authority = h.authorities[request["command"]["origin"]["assertion_ref"]]
                authority["actor_contexts"][0][field] = value
                response = await h.submit(request)
                self.assertEqual("forbidden", response["outcomes"][0]["state"])
        request = h.fanout()
        request["command"]["deadline_at"] = utc(h.clock() + 1)
        identity = h.memory.identity

        async def delayed(*args):
            self.assertFalse(h.core.store.db.in_transaction)
            h.clock.advance(2)
            return await identity(*args)

        h.memory.identity = delayed
        before = h.facts(head=True)["head"]
        with self.assertRaises(Fault) as error:
            await h.submit(request)
        self.assertEqual("timeout", error.exception.code)
        self.assertEqual(before, h.facts(head=True)["head"])

    async def test_capacity_and_same_revision_conflict_roll_back_whole_fanout(self):
        h = self.h
        from dataclasses import replace

        h.core.policy = replace(h.core.policy, max_collectors_per_conversation=1)
        request = h.fanout()
        head = h.facts(head=True)["head"]
        with self.assertRaises(Fault) as error:
            await h.submit(request)
        self.assertEqual("queue_full", error.exception.code)
        self.assertEqual(head, h.facts(head=True)["head"])
        self.assertFalse(h.core.store.list("physicals"))
        self.assertFalse(h.core.store.list("inbox"))
        request["target_actor_ids"] = ["actor:a"]
        await h.submit(request)
        changed = copy.deepcopy(request)
        changed["command"]["idempotency_key"] = uid("new")
        changed["input"]["parts"][0]["text"] = "伪造同版本"
        with self.assertRaises(Fault) as error:
            await h.submit(changed)
        self.assertEqual("idempotency_conflict", error.exception.code)

    async def test_edit_revokes_all_actors_and_regrant_only_current_authorized_actor(self):
        h = self.h
        request = h.fanout(message="p:edit")
        accepted = await h.submit(request)
        edit = h.fanout(text="编辑", message="p:edit", revision=2, kind="edit", targets=["actor:a"])
        await h.submit(edit)
        facts = h.facts([edit])
        self.assertEqual(2, facts["physicals"][0]["revision"])
        self.assertEqual(
            [2, 1], [a["source"]["message_key"]["revision"] for a in facts["admissions"]]
        )
        self.assertEqual(
            accepted["outcomes"][1]["receipt"]["receipt_id"],
            facts["admissions"][1]["source"]["receipt_id"],
        )
        self.assertEqual(
            ["actor:a"],
            [
                c["scope"]["actor_id"]
                for c in h.core.store.list("collections", states=["collecting"])
            ],
        )
        edit["command"]["idempotency_key"] = uid("regrant")
        edit["target_actor_ids"] = ["actor:b"]
        result = await h.submit(edit)
        self.assertEqual("accepted", result["outcomes"][0]["state"])
        self.assertEqual(
            [2, 2], [a["source"]["message_key"]["revision"] for a in h.facts([edit])["admissions"]]
        )

    async def test_retraction_creates_no_actor_receipt_and_tombstone_cannot_revive(self):
        h = self.h
        request = h.fanout(message="p:retract")
        await h.submit(request)
        retract = h.fanout(message="p:retract", revision=2, kind="retract", targets=[], grants=[])
        result = await h.submit(retract)
        self.assertEqual([], result["outcomes"])
        self.assertIsNone(result["person_id"])
        self.assertEqual(2, len(h.core.store.list("inbox")))
        self.assertEqual("withdrawn", h.facts([request])["physicals"][0]["state"])
        for kind in ("edit", "message"):
            with self.assertRaises(Fault) as error:
                await h.submit(h.fanout(message="p:retract", revision=3, kind=kind))
            self.assertEqual("version_conflict", error.exception.code)
        ambiguous = copy.deepcopy(retract)
        ambiguous["target_actor_ids"] = ["actor:a"]
        with self.assertRaises(Fault):
            await h.submit(ambiguous)

    async def test_cross_author_mutation_and_changed_idempotent_targets_rejected(self):
        h = self.h
        request = h.fanout(message="p:owner")
        await h.submit(request)
        other = h.fanout(message="p:owner", revision=2, kind="edit", account="b")
        with self.assertRaises(Fault) as error:
            await h.submit(other)
        self.assertEqual("forbidden", error.exception.code)
        request["target_actor_ids"] = ["actor:a"]
        with self.assertRaises(Fault) as error:
            await h.submit(request)
        self.assertEqual("idempotency_conflict", error.exception.code)

    async def test_snapshot_actor_coverage_metadata_missing_and_owner_event(self):
        h = self.h
        request = h.fanout(targets=["actor:a"])
        await h.submit(request)
        h.clock.advance(5)
        await h.cycles()
        facts = h.facts([request], content=False, turns=[h.turns()[0]["id"], "turn:absent"])
        self.assertIsNone(facts["physicals"][0]["content"])
        self.assertEqual("missing", facts["admissions"][1]["state"])
        self.assertEqual("missing", facts["turns"][1]["state"])
        event = h.core.store.list("outbox")[0]["event"]
        self.assertEqual(event, facts["turns"][0]["committed_event"])
        self.assertEqual(event["sources"], facts["turns"][0]["input_sources"])
        self.assertGreaterEqual(facts["turns"][0]["aggregate_version"], event["aggregate_version"])
        self.assertEqual([], h.facts(head=True)["physicals"])
        self.assertEqual(facts["head"], h.facts(head=True)["head"])

    async def test_classification_real_fictional_mixed_and_unknown_never_default_real(self):
        h = self.h
        real = h.fanout(targets=["actor:a"])
        await h.submit(real)
        classification = h.core.bindings["qq-private"]["classification"]
        classification["value"] = "fictional"
        await h.submit(h.fanout(targets=["actor:a"]))
        h.clock.advance(5)
        await h.cycles()
        self.assertEqual("mixed", h.core.store.list("outbox")[0]["event"]["reality"])
        del h.core.bindings["qq-private"]["classification"]
        unknown = h.fanout(targets=["actor:a"], channel="private:unknown")
        unknown_result = await h.submit(unknown)
        h.clock.advance(5)
        await h.cycles()
        self.assertEqual(
            "unclassified", h.facts([unknown])["physicals"][0]["classification"]["value"]
        )
        self.assertEqual(
            "discarded_source",
            h.core.store.list("outbox", unknown_result["conversation_id"])[0]["state"],
        )
        before = h.facts(head=True)["head"]
        h.core.reclassify_source(
            physical_key(real["input"]),
            1,
            dict(
                value="fictional",
                basis="reviewed_source",
                policy_ref="fixture:review",
                policy_version=1,
            ),
        )
        self.assertGreater(h.facts(head=True)["head"]["sequence"], before["sequence"])

    async def test_global_two_slots_and_order_hold_when_b_generation_finishes_first(self):
        h = self.h
        from dataclasses import replace

        h.core.policy = replace(h.core.policy, silence_ms=0)
        h.gateway.gates[1] = asyncio.Event()
        await h.submit(h.fanout())
        await h.submit(h.fanout(targets=["actor:a"]))
        await h.cycles()
        self.assertEqual([1, 2], [seq for seq, _ in h.gateway.calls])
        self.assertEqual("queued", h.turns()[2]["phase"])
        self.assertFalse(h.sender.calls)
        h.gateway.gates[1].set()
        await h.cycles(70)
        self.assertEqual([1, 1, 2, 2, 3, 3], [r["turn_sequence"] for r in h.sender.calls])

    async def test_response_gate_holds_all_actors_until_receipts_released(self):
        h = self.h
        from dataclasses import replace

        h.core.policy = replace(h.core.policy, silence_ms=0)
        result = await h.submit(h.fanout(), defer=True)
        await h.cycles()
        self.assertTrue(all(t["phase"] == "queued" for t in h.turns()))
        for outcome in result["outcomes"]:
            await h.core.acknowledge_ingest(outcome["receipt"]["receipt_id"])
        await h.cycles(60)
        self.assertTrue(all(t["phase"] == "sent" for t in h.turns()))

    async def test_restart_head_receipts_default_route_and_tombstone_survive(self):
        await self.h.core.close()
        with tempfile.TemporaryDirectory() as directory:
            h = self.h = SourceHarness(Path(directory) / "fixture.db")
            request = h.fanout(targets=[], defaults=["actor:a"], message="persisted")
            first = await h.submit(request)
            head = h.facts(head=True)["head"]
            await h.core.close()
            h.core = h.new_core()
            self.assertEqual(head, h.facts(head=True)["head"])
            self.assertEqual(
                first["outcomes"][0]["receipt"]["receipt_id"],
                (await h.submit(request))["outcomes"][0]["receipt"]["receipt_id"],
            )
            await h.submit(h.fanout(message="persisted", revision=2, kind="retract", targets=[]))
            await h.core.close()
            h.core = h.new_core()
            with self.assertRaises(Fault):
                await h.submit(h.fanout(message="persisted", revision=3, kind="edit"))
            await h.core.close()
            self.h = SourceHarness()

    async def test_http_service_acl_strict_json_and_no_memory_callback_for_facts(self):
        h = self.h
        app = create_app(h.core, {"nonebot": "fixture-n", "memory": "fixture-m"})
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://fixture"
        ) as client:
            request = h.fanout()
            response = await client.post(
                "/internal/v1/conversation/ingest-actors",
                json=request,
                headers={"Authorization": "Bearer fixture-m"},
            )
            self.assertEqual(403, response.status_code)
            response = await client.post(
                "/internal/v1/conversation/ingest-actors",
                json=request,
                headers={"Authorization": "Bearer fixture-n"},
            )
            self.assertEqual(200, response.status_code)
            h.contracts.check("sources#fanout_response", response.json())
            h.memory.unavailable = True
            query = dict(
                schema_version=1,
                request_id="read:fixture",
                mode="head",
                selectors=[],
                turn_ids=[],
                include_content=False,
            )
            response = await client.post(
                "/internal/v1/source-facts/read",
                json=query,
                headers={"Authorization": "Bearer fixture-m"},
            )
            self.assertEqual(200, response.status_code)
            response = await client.post(
                "/internal/v1/source-facts/read",
                json=query,
                headers={"Authorization": "Bearer fixture-n"},
            )
            self.assertEqual(403, response.status_code)
            for body in ('{"schema_version":1,"schema_version":1}', '{"schema_version":NaN}'):
                response = await client.post(
                    "/internal/v1/source-facts/read",
                    content=body,
                    headers={"Authorization": "Bearer fixture-m"},
                )
                self.assertEqual(400, response.status_code)

    async def test_real_client_input_authority_correlation_and_memory_check_scope(self):
        h = self.h
        request = h.fanout(targets=["actor:a"])
        stored = h.authorities[request["command"]["origin"]["assertion_ref"]]
        mutation = {}

        def handler(incoming):
            body = json.loads(incoming.content)
            self.assertEqual("Bearer fixture", incoming.headers["Authorization"])
            if incoming.url.path == "/internal/v1/source-access/read":
                result = dict(
                    **stored,
                    schema_version=1,
                    request_id=body["request_id"],
                    operation="input",
                    request_digest=digest(body),
                    ingest_digest=digest(body["ingest"]),
                    input_digest=digest(body["ingest"]["input"]),
                    verified_account=request["input"]["author"],
                    verified_channel=request["input"]["message_key"]["channel"],
                    origin_ref=request["command"]["origin"]["assertion_ref"],
                )
            else:
                self.assertEqual("/internal/v1/memory/source-sync/check", incoming.url.path)
                self.assertNotIn("origin", body)
                result = dict(
                    schema_version=1,
                    request_id=body["request_id"],
                    request_digest=digest(body),
                    scope=body["scope"],
                    version_domain="text-dialogue/v1",
                    scope_version=7,
                    checked_at=utc(h.clock()),
                )
            result.update(mutation)
            return httpx.Response(200, json=result)

        service = JsonService(
            "https://fixture.invalid", "fixture", transport=httpx.MockTransport(handler)
        )
        try:
            origins = Origins(h.contracts, {"nonebot": ("platform", service)})
            value = await origins.input_access("nonebot", request)
            self.assertEqual(digest(request), value["ingest_digest"])
            mutation["request_digest"] = "0" * 64
            with self.assertRaises(Fault):
                await origins.input_access("nonebot", request)
            mutation.clear()
            await h.submit(request)
            h.memory.unavailable = True
            h.clock.advance(10)
            await h.cycles()
            h.clock.advance(5)
            await h.cycles()
            memory = Memory(h.contracts, service)
            turn = h.turns()[0]
            self.assertEqual("blocked_scope", h.core.store.list("outbox")[0]["state"])
            await h.core.repair_blocked_scope(turn["id"], memory.check_sources)
            self.assertEqual(7, h.core.store.list("outbox")[0]["event"]["scope_version"])
        finally:
            await service.close()
