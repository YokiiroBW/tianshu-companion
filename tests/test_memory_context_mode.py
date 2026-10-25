"""Positive selected context, lazy whole-unit fallback and actual recall adoption."""

import copy
import json
import time
import unittest
from dataclasses import replace
from unittest.mock import patch

import httpx
import test_memory_v2 as evidence_support

from support import contracts
from tianshu_companion.clients import JsonService, Memory, utc
from tianshu_companion.context import scope_check
from tianshu_companion.context_preparation import _v2_variants
from tianshu_companion.contracts import Fault, canonical
from tianshu_companion.memory_context import query_request


class ContextModeProtocolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.contracts = contracts()
        self.fixture = evidence_support.example("memory-context/v2", "context-positive")
        self.requests = []

    def contextual(self, response):
        response["bundle"]["retrieval_mode"] = "context"
        response["omissions"] = ["context_only"]
        for result, constraint in zip(
            response["query_results"], response["bundle"]["selection_constraints"]
        ):
            result.update(status="incomplete", omissions=["context_only"])
            result["coverage"]["candidate_complete"] = False
            constraint.update(
                result_status="incomplete",
                proof_kinds=["positive_sources"],
                candidate_domain_complete=False,
                owner_resolution=dict(mode="unresolved", targets=[]),
            )
        return response

    def client(self, change=None):
        async def handler(request):
            body = json.loads(request.content)
            self.requests.append((request.url.path, copy.deepcopy(body)))
            now = time.time()
            if request.url.path.endswith("/query"):
                response = copy.deepcopy(self.fixture["query_response"])
                response["request_id"] = body["query"]["request_id"]
                response["bundle"].update(
                    {
                        key: body[key]
                        for key in (
                            "queries",
                            "selection",
                            "include_associated",
                            "budget",
                            "limits",
                        )
                    }
                )
                response["bundle"]["valid_until"] = utc(now + 60)
                if body.get("retrieval_mode") == "context":
                    self.contextual(response)
            else:
                response = dict(
                    schema_version=2,
                    version_domain="memory-context/v2",
                    request_id=body["query"]["request_id"],
                    effective_scope=body["requested_scope"],
                    state="valid",
                    bundle=copy.deepcopy(body["bundle"]),
                    checks=[
                        dict(check_id=c["check_id"], kind=c["kind"], status="valid")
                        for c in body["checks"]
                    ],
                    checked_at=utc(now),
                    valid_until=utc(now + 30),
                )
                response["bundle"].update(verified_at=utc(now), valid_until=utc(now + 60))
            if change:
                change(body, response)
            return httpx.Response(200, json=response)

        wire = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        return Memory(self.contracts, wire)

    async def query(self, client, mode=None):
        request = self.fixture["query_request"]
        return await client.query_context(
            request["query"]["origin"],
            request["requested_scope"],
            "那次经历",
            request["budget"],
            queries=request["queries"],
            **({"retrieval_mode": mode} if mode is not None else {}),
        )

    async def test_context_is_explicit_and_standalone_factual_omits_the_new_field(self):
        memory = self.client()
        for mode in (None, "factual", "context"):
            with self.subTest(mode=mode):
                result = await self.query(memory, mode)
                request = self.requests[-1][1]
                self.assertEqual(mode == "context", "retrieval_mode" in request)
                if mode == "context":
                    self.assertEqual("context", result["bundle"]["retrieval_mode"])
                    self.assertEqual("incomplete", result["query_results"][0]["status"])
                    self.assertIn("context_only", result["omissions"])
        with self.assertRaises(Fault):
            query_request({}, {}, "x", {}, retrieval_mode="unknown")

    async def test_response_cannot_upgrade_context_to_absence_or_unique_owner_guarantees(self):
        def mode(body, response):
            response["bundle"].pop("retrieval_mode")

        def matched(body, response):
            response["query_results"][0]["status"] = "matched"
            response["bundle"]["selection_constraints"][0]["result_status"] = "matched"

        def completeness(body, response):
            response["bundle"]["selection_constraints"][0]["candidate_domain_complete"] = True

        def owners(body, response):
            response["bundle"]["selection_constraints"][0]["owner_resolution"] = dict(
                mode="self", targets=[]
            )

        def omission(body, response):
            response["omissions"] = []

        for change in (mode, matched, completeness, owners, omission):
            with self.subTest(change=change.__name__), self.assertRaises(Fault):
                await self.query(self.client(change), "context")

    async def test_context_refreshes_observations_but_factual_keeps_its_original_identity(self):
        def refresh(body, response):
            bundle = response["bundle"]
            bundle["scope_version"] += 1
            bundle["association_version"] += 1
            for check in bundle["scope_checks"]:
                check["scope_version"] += 1
            for key in ("history_snapshot", "input_history_snapshot"):
                bundle[key]["association_version"] += 1
                for check in bundle[key]["history_checks"]:
                    check["history_version"] += 1

        request = self.fixture["query_request"]
        for mode in ("context", "factual"):
            original = copy.deepcopy(self.fixture["query_response"])
            if mode == "context":
                self.contextual(original)
            bundle = original["bundle"]
            memory = self.client(refresh)
            if mode == "context":
                verified = await memory.verify_context(
                    request["query"]["origin"], request["requested_scope"], bundle
                )
                self.assertGreater(verified["bundle"]["scope_version"], bundle["scope_version"])
                self.assertEqual(
                    {},
                    await memory.prepare_context(
                        request["query"]["origin"], request["requested_scope"], bundle, [], []
                    ),
                )
            else:
                with self.assertRaises(Fault):
                    await memory.prepare_context(
                        request["query"]["origin"], request["requested_scope"], bundle, [], []
                    )

    async def test_context_never_changes_selected_sources_records_or_mode_after_a_read(self):
        response = self.contextual(copy.deepcopy(self.fixture["query_response"]))
        bundle, request = response["bundle"], self.fixture["query_request"]
        for changed in ("source_dependencies", "group_dependencies", "retrieval_mode"):
            with self.subTest(changed=changed):

                def change(body, response):
                    current = response["bundle"]
                    if changed == "source_dependencies":
                        current[changed][0]["epoch"] += 1
                    elif changed == "group_dependencies":
                        current[changed][0]["records"][0]["record_version"] += 1
                    else:
                        current[changed] = "factual"

                memory = self.client(change)
                with self.assertRaises(Fault):
                    await memory.verify_context(
                        request["query"]["origin"], request["requested_scope"], bundle
                    )
                with self.assertRaises(Fault):
                    await memory.prepare_context(
                        request["query"]["origin"], request["requested_scope"], bundle, [], []
                    )


class SelectedContextTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = evidence_support.CoreEvidenceTests.asyncSetUp

    async def turns(self, count=2):
        for index in range(count):
            await self.h.ingest(text=f"第{index}段有源会话")
            await self.h.cycles()
        turns = self.h.turns()
        self.assertTrue(all(turn["phase"] == "sent" for turn in turns))
        return turns

    async def test_healthy_full_unit_has_no_unused_raw_source_rpc(self):
        previous, current = await self.turns()
        calls, raw = [], []
        original = self.h.memory.prepare_context

        async def prepare(*args):
            calls.append(copy.deepcopy(args[3]))
            return await original(*args)

        async def sources(*args):
            raw.append(args)
            raise AssertionError("a healthy full unit must not prevalidate unused raw input")

        self.h.memory.prepare_context = prepare
        self.h.memory.check_sources = sources
        context, *_ = await self.h.core._prepare_context(current, [])
        self.assertEqual(1, len(calls))
        self.assertTrue(context.data["recent_dialogue"][0]["replies"])
        self.assertEqual([], raw)
        self.assertEqual([previous["id"]], [g["turn_id"] for g in context.data["recent_dialogue"]])
        self.assertTrue(all(row["retrieval_mode"] == "context" for row in self.context_queries))

    async def test_only_a_definite_optional_rejection_builds_one_whole_input_fallback(self):
        first, second, current = await self.turns(3)
        from tianshu_companion.short_context import select_recent

        selected, _ = select_recent(
            self.h.core.store, current, self.h.core.short_context_policy, self.h.clock()
        )
        selected = [
            next(g for g in selected if g["turn_id"] == turn["id"]) for turn in (first, second)
        ]
        calls, raw = [], []

        async def prepare(origin, scope, bundle, checks, required):
            calls.append(copy.deepcopy(checks))
            raw.extend(
                c["request"]["source_check"]["turn_id"]
                for c in checks
                if c["kind"] == "input_sources"
            )
            return {
                c["check_id"]: "rejected" if c["kind"] == "history" else "valid" for c in checks
            }

        async def sources(*args):
            raise AssertionError("unused standalone source check")

        self.h.memory.prepare_context, self.h.memory.check_sources = prepare, sources
        result = await _v2_variants(self.h.core, current, [], [selected])
        self.assertEqual(2, len(calls))
        self.assertTrue(calls[0])
        self.assertEqual({"input_sources"}, {c["kind"] for c in calls[1]})
        self.assertCountEqual([first["id"], second["id"]], raw)
        self.assertEqual([first["id"], second["id"]], [g["turn_id"] for g in result[0][0][0]])
        self.assertTrue(all(not g["replies"] for g in result[0][0][0]))

    async def test_unavailable_origin_never_triggers_a_raw_alternative(self):
        _, current = await self.turns()
        calls, raw = [], []

        async def prepare(origin, scope, bundle, checks, required):
            calls.append(checks)
            return {c["check_id"]: "unavailable" for c in checks}

        async def sources(*args):
            raw.append(args)

        self.h.memory.prepare_context, self.h.memory.check_sources = prepare, sources
        context, *_ = await self.h.core._prepare_context(current, [])
        self.assertEqual(1, len(calls))
        self.assertEqual([], raw)
        self.assertEqual([], context.data["recent_dialogue"])

    async def test_unavailable_linked_member_prevents_the_whole_raw_fallback(self):
        first, second, current = await self.turns(3)
        from tianshu_companion.short_context import select_recent

        selected, _ = select_recent(
            self.h.core.store, current, self.h.core.short_context_policy, self.h.clock()
        )
        groups = {group["turn_id"]: group for group in selected}
        calls, raw = [], []

        async def prepare(origin, scope, bundle, checks, required):
            calls.append(copy.deepcopy(checks))
            return {
                check["check_id"]: "unavailable"
                if check["request"]["query"]["origin"] == first["origin"]
                else "valid"
                for check in checks
            }

        async def sources(*args):
            raw.append(args)
            raise AssertionError("a linked raw unit still contains its unavailable origin")

        self.h.memory.prepare_context, self.h.memory.check_sources = prepare, sources
        result = await _v2_variants(
            self.h.core, current, [], [[groups[first["id"]], groups[second["id"]]]]
        )
        self.assertEqual([[]], result)
        self.assertEqual(1, len(calls))
        self.assertEqual([], raw)

    async def test_fallback_closes_the_whole_final_set_after_new_origin_unavailability(self):
        first, second, current = await self.turns(3)
        from tianshu_companion.short_context import select_recent

        selected, _ = select_recent(
            self.h.core.store, current, self.h.core.short_context_policy, self.h.clock()
        )
        groups = {group["turn_id"]: group for group in selected}
        calls = []

        async def prepare(origin, scope, bundle, checks, required):
            calls.append(copy.deepcopy(checks))
            result = {}
            for check in checks:
                source = check["request"]["query"]["origin"]
                result[check["check_id"]] = (
                    "rejected"
                    if len(calls) == 1 and source == second["origin"]
                    else "unavailable"
                    if len(calls) == 2 and source == first["origin"]
                    else "valid"
                )
            return result

        self.h.memory.prepare_context = prepare
        result = await _v2_variants(
            self.h.core, current, [], [[groups[first["id"]]], [groups[second["id"]]]]
        )
        self.assertEqual(2, len(calls))
        self.assertEqual([], result[0])
        self.assertEqual([second["id"]], [g["turn_id"] for g in result[1][0][0]])
        self.assertTrue(all(not group["replies"] for group in result[1][0][0]))

    async def test_unselected_turns_do_not_expand_the_catalog_or_v2_profile_fanout(self):
        turns = await self.turns(4)
        current = turns[-1]
        self.h.core.short_context_policy = replace(self.h.core.short_context_policy, max_turns=1)
        seen = []
        original = self.h.core._verify_context_check

        async def verify(turn, check, *args, **kwargs):
            seen.append(check.get("source_turn_id"))
            return await original(turn, check, *args, **kwargs)

        self.h.core._verify_context_check = verify
        context, metadata, _, profiles, *_ = await self.h.core._prepare_context(current, [])
        self.assertEqual(1, len(metadata["turn_ids"]))
        self.assertTrue(set(seen) <= set(metadata["turn_ids"]))
        self.assertEqual([], profiles)
        self.assertEqual([], context.data["profiles"])
        self.assertEqual([], self.h.memory.profile_selections)

    async def test_required_source_failure_is_never_replaced_by_an_input_projection(self):
        previous, current = await self.turns()
        original = self.h.origins.resolve

        async def revoked(service, envelope, now):
            if envelope["origin"] == previous["origin"]:
                error = Fault("forbidden")
                error.origin_read_unavailable = True
                raise error
            return await original(service, envelope, now)

        self.h.origins.resolve = revoked
        with self.assertRaises(Fault):
            await _v2_variants(self.h.core, current, [scope_check(previous)], [])

    async def test_recall_is_a_read_skill_and_adopts_exact_history_before_fresh_checks(self):
        turn = (await self.turns(1))[0]
        allowed = self.h.core._turn_allows
        with patch.object(
            self.h.core,
            "_turn_allows",
            side_effect=lambda t, p: p != "memory.write" and allowed(t, p),
        ):
            names = {item["function"]["name"] for item in self.h.core.role_actions.tools(turn)}
            self.assertIn("memory_recall", names)
            self.assertNotIn("memory_propose", names)
            tool = dict(
                id="recall:history",
                function=dict(
                    name="memory_recall",
                    arguments=canonical(dict(query="以前讨论的经历", intent="history")),
                ),
            )
            result, attachments = await self.h.core.role_actions.execute(turn["id"], tool)
        self.assertEqual([], attachments)
        self.assertEqual("not_integrated", result["chat_archive"])
        query = self.context_queries[-1]
        self.assertEqual(
            (turn["origin"], turn["scope"], "context"),
            (query["origin"], query["scope"], query["retrieval_mode"]),
        )
        self.assertEqual("history", query["queries"][0]["intent"])
        fresh = self.h.core.store.get("turns", turn["id"])
        self.assertEqual(
            result["material"]["episodes"][0]["text"],
            fresh["memory_context"]["evidence"][0]["text"],
        )
        self.invalid = True
        with self.assertRaises(Fault):
            await self.h.core._preflight(fresh)

    async def test_recall_cannot_supply_identity_or_survive_live_read_revocation(self):
        turn = (await self.turns(1))[0]
        for values in (dict(query="x", intent="history", scope={}), dict(query="x", intent=[])):
            with self.subTest(values=values), self.assertRaises(Fault):
                await self.h.core.role_actions.memory_recall(turn, values)
        original = self.h.memory.query_context
        before = copy.deepcopy(turn["memory_context"])

        async def revoke(*args, **kwargs):
            value = await original(*args, **kwargs)
            allowed = self.h.core._turn_allows
            self.h.core._turn_allows = lambda t, p: p != "memory.read" and allowed(t, p)
            return value

        self.h.memory.query_context = revoke
        with self.assertRaises(Fault):
            await self.h.core.role_actions.memory_recall(turn, dict(query="过去", intent="history"))
        self.assertEqual(before, self.h.core.store.get("turns", turn["id"])["memory_context"])

    async def test_native_recall_rebuilds_prompt_and_its_adopted_proof_before_the_next_model(self):
        await self.turns(1)
        original = self.h.memory.query_context

        async def recall(*args, **kwargs):
            value = await original(*args, **kwargs)
            if kwargs.get("queries"):
                value["evidence"][0]["text"] = "实际历史检索得到的有源经历"
            return value

        self.h.memory.query_context = recall
        seen = []

        async def complete(turn, messages, *, tools=None, on_delta=None):
            seen.append(copy.deepcopy(messages))
            if len(seen) == 1:
                self.assertIn("memory_recall", {tool["function"]["name"] for tool in tools})
                message = dict(
                    role="assistant",
                    content=None,
                    tool_calls=[
                        dict(
                            id="native:recall",
                            type="function",
                            function=dict(
                                name="memory_recall",
                                arguments=canonical(dict(query="以前的事", intent="mixed")),
                            ),
                        )
                    ],
                )
            else:
                message = dict(role="assistant", content="按本次可见的历史资料回答。")
            return message, dict(config_version=1, fixture_only=True)

        self.h.gateway.complete = complete
        await self.h.ingest(text="请翻看过去的记忆")
        await self.h.cycles(80)
        self.assertEqual(2, len(seen))
        current = self.h.turns()[-1]
        self.assertEqual("sent", current["phase"])
        prompt = json.loads(seen[-1][1]["content"])
        self.assertEqual("实际历史检索得到的有源经历", prompt["episodes"][0]["text"])
        self.assertTrue(current["context_checks"])
        self.assertEqual("mixed", self.context_queries[-1]["queries"][0]["intent"])
        self.assertTrue(self.verifications[-1])
