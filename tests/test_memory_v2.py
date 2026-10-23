"""Published producer fixtures + real consumer; synthetic transport, no live authority."""

import copy
import json
import time
import unittest

import httpx
from jsonschema import Draft202012Validator

from support import Harness, contracts
from tianshu_companion.clients import JsonService, Memory, epoch, utc
from tianshu_companion.context import TurnContext, merge_checks, scope_check
from tianshu_companion.contracts import Fault, canonical, digest
from tianshu_companion.memory_context import selection, validate_response
from tianshu_companion.memory_extraction import output_schema


def example(package, name):
    result = copy.deepcopy(
        next(
            row for row in contracts().documents[package + "/examples.json"] if row["name"] == name
        )
    )
    if "query_response" in result:
        # Published examples illustrate shape and use placeholder usage. The
        # consumer also enforces actual serialized UTF-8 bytes at runtime.
        response = result["query_response"]
        size = len(canonical(response["evidence"]).encode())
        response["budget_used"] = dict(bytes=size, tokens=(size + 3) // 4)
    return result


class EvidenceProtocolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.contracts = contracts()

    async def test_all_published_query_examples_validate_and_preserve_episodes(self):
        for fixture in self.contracts.documents["memory-context/v2/examples.json"]:
            with self.subTest(name=fixture["name"]):
                fixture = example("memory-context/v2", fixture["name"])
                request, response = fixture["query_request"], fixture["query_response"]
                validate_response(
                    request, response, epoch(response["bundle"]["verified_at"]), epoch
                )
                prompt = TurnContext(selection(response), [], [], memory=response)
                self.assertEqual(
                    [row["text"] for row in response["evidence"] if row["kind"] == "episode"],
                    [row["text"] for row in prompt.data["episodes"]],
                )
                self.assertEqual(
                    len(response["evidence"]),
                    len(prompt.data["memory_state"]) + len(prompt.data["episodes"]),
                )

    async def test_client_query_preserves_full_contract_and_validates_relationships(self):
        fixture = example("memory-context/v2", "context-positive")
        supplied = fixture["query_response"]
        requests = []

        async def handler(request):
            body = json.loads(request.content)
            requests.append(body)
            response = copy.deepcopy(supplied)
            response["request_id"] = body["query"]["request_id"]
            response["bundle"].update(
                {
                    key: body[key]
                    for key in ("queries", "selection", "include_associated", "budget", "limits")
                }
            )
            response["bundle"]["valid_until"] = utc(time.time() + 60)
            return httpx.Response(200, json=response)

        client = JsonService(
            "https://synthetic.invalid", "synthetic-token", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(client.close)
        memory = Memory(self.contracts, client)
        request = fixture["query_request"]
        response = await memory.query_context(
            request["query"]["origin"],
            request["requested_scope"],
            "合成查询",
            request["budget"],
            queries=request["queries"],
        )
        self.assertEqual(response["evidence"], supplied["evidence"])
        self.assertEqual(["facts", "episodes"], requests[0]["queries"][0]["kinds"])

        mutations = [
            lambda r: r["bundle"].update(evidence_digest="0" * 64),
            lambda r: r["evidence"][0]["owner_scope"].update(conversation_id="room:other"),
            lambda r: r["bundle"]["group_dependencies"][0]["records"][0].update(record_version=999),
            lambda r: r["bundle"]["episode_dependencies"][0].update(text_hash="0" * 64),
            lambda r: r["bundle"]["source_dependencies"][0]["owner_scope"].update(
                actor_id="actor:other"
            ),
            lambda r: r["query_results"][0].update(status="no_match"),
            lambda r: r["evidence"][0]["unit"].update(visibility="private"),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                supplied = copy.deepcopy(fixture["query_response"])
                mutate(supplied)
                if supplied["bundle"]["evidence_digest"] != "0" * 64:
                    supplied["bundle"]["evidence_digest"] = digest(supplied["evidence"])
                with self.assertRaises(Fault):
                    await memory.query_context(
                        request["query"]["origin"],
                        request["requested_scope"],
                        "合成查询",
                        request["budget"],
                        queries=request["queries"],
                    )

    async def test_verify_splits_over_64_without_accepting_failed_later_batch(self):
        fixture = example("memory-context/v2", "context-positive")
        request = fixture["verify_request"]
        checks = [
            dict(copy.deepcopy(request["checks"][0]), check_id=f"check:{index}")
            for index in range(65)
        ]
        calls, fail = [], False

        async def handler(wire):
            body = json.loads(wire.content)
            calls.append(body)
            if fail and len(calls) % 2 == 0:
                return httpx.Response(
                    409, json=dict(code="scope_changed", execution_state="not_started")
                )
            response = copy.deepcopy(fixture["verify_response"])
            response.update(
                request_id=body["query"]["request_id"],
                checks=[
                    dict(check_id=c["check_id"], kind=c["kind"], status="valid")
                    for c in body["checks"]
                ],
                valid_until=utc(time.time() + 60),
            )
            response["bundle"]["valid_until"] = response["valid_until"]
            return httpx.Response(200, json=response)

        client = JsonService(
            "https://synthetic.invalid", "synthetic-token", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(client.close)
        memory = Memory(self.contracts, client)
        await memory.verify_context(
            request["query"]["origin"], request["requested_scope"], request["bundle"], checks
        )
        self.assertEqual([64, 1], [len(c["checks"]) for c in calls])
        self.assertEqual(65, len(merge_checks(checks)))
        fail = True
        with self.assertRaises(Fault):
            await memory.verify_context(
                request["query"]["origin"], request["requested_scope"], request["bundle"], checks
            )
        self.assertEqual(4, len(calls))

    def test_prompt_schema_resolves_published_external_refs_without_copying_rules(self):
        schema = output_schema(self.contracts)
        self.assertNotIn('"$ref"', canonical(schema))
        validator = Draft202012Validator(schema)
        fixture = example("memory-extraction/v1", "extraction-positive")
        self.assertTrue(validator.is_valid(fixture["submit_request"]["output"]))
        invalid = copy.deepcopy(fixture["submit_request"]["output"])
        invalid["memories"][0]["fact"].pop("modality")
        self.assertFalse(validator.is_valid(invalid))


class ExtractionNegotiationTests(unittest.IsolatedAsyncioTestCase):
    async def test_negotiated_legacy_read_and_submit_are_explicit(self):
        fixture = example("memory-extraction/v1", "extraction-positive-legacy-negotiation")
        response = fixture["legacy_pending_response"]
        calls = []

        async def handler(request):
            calls.append(json.loads(request.content))
            return httpx.Response(200, json=response)

        client = JsonService(
            "https://synthetic.invalid", "synthetic-token", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(client.close)
        memory = Memory(contracts(), client)
        read = await memory.process_candidate("job:legacy", None, extraction_schema=4)
        self.assertEqual(3, read["extraction_schema"])
        self.assertEqual([3, 4], calls[0]["supported_extraction_schemas"])
        response = dict(
            state="completed",
            extraction_schema=3,
            content=[],
            result=dict(state="skipped", record_ids=[], group_ids=[]),
        )
        output = example("memory-extraction/v1", "extraction-positive-legacy-empty-time")[
            "legacy_submit_request"
        ]["output"]
        await memory.process_candidate("job:legacy", output, extraction_schema=3)
        self.assertEqual(dict(job_id="job:legacy", output=output, extraction_schema=3), calls[1])

    async def test_read_malformed_and_write_lost_response_keep_distinct_outcomes(self):
        calls = []
        fixture = example("memory-extraction/v1", "extraction-positive")

        async def handler(request):
            calls.append(json.loads(request.content))
            if calls[-1]["output"] is None:
                return httpx.Response(200, json=dict(state="pending", content=[], result=None))
            raise httpx.ReadError("synthetic lost acknowledgement")

        client = JsonService(
            "https://synthetic.invalid", "synthetic-token", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(client.close)
        memory = Memory(contracts(), client)
        with self.assertRaises(Fault) as rejected:
            await memory.process_candidate("job:synthetic", None, extraction_schema=4)
        self.assertFalse(rejected.exception.unknown)
        with self.assertRaises(Fault) as uncertain:
            await memory.process_candidate(
                "job:synthetic", fixture["submit_request"]["output"], extraction_schema=4
            )
        self.assertTrue(uncertain.exception.unknown)
        self.assertEqual(2, len(calls))

    async def test_learning_persists_negotiated_schema_before_generating_and_old_jobs_keep_wire(
        self,
    ):
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        calls = []

        async def process(job, output, **kwargs):
            calls.append((job, output, kwargs))
            return dict(
                state="pending", content=[], result=None, extraction_schema=3, known_facts=[]
            )

        h.memory.process_candidate = process
        item = dict(
            id="memory:learning:negotiated",
            kind="memory:learning",
            job_id="job:legacy-ack",
            extraction_schema=4,
        )
        await h.core.memory_learning._candidate(item, None)
        saved = h.core.store.get("metadata", item["id"])
        self.assertEqual(3, saved["extraction_schema"])
        self.assertTrue(saved["extraction_negotiated"])
        await h.core.memory_learning._candidate(saved, {"memories": []})
        self.assertEqual(dict(extraction_schema=3), calls[-1][2])
        old = dict(id="memory:learning:retained", job_id="job:old")
        await h.core.memory_learning._candidate(old, {"memories": []})
        self.assertEqual({}, calls[-1][2])


class CoreEvidenceTests(unittest.IsolatedAsyncioTestCase):
    """Routing/assembly tests use explicit ports, not a pretend source authority."""

    async def asyncSetUp(self):
        self.h = Harness(silence_ms=0)
        self.addAsyncCleanup(self.h.core.close)
        self.verifications = []
        self.invalid = False
        old_select, history = self.h.memory.select, self.h.memory.history

        async def query_context(origin, scope, text, budget):
            old = await old_select(origin, scope, text, budget)
            snapshot = await history(origin, scope)
            source = dict(
                kind="source_episode", owner="memory", episode_ref="episode:old", episode_version=1
            )
            episode = dict(
                kind="episode",
                evidence_id="evidence:old",
                subject_person_id=scope["person_id"],
                owner_scope=scope,
                episode_id="episode:old",
                episode_version=1,
                source_refs=[source],
                text="当时我只是计划周五出发。",
                observed_at=utc(self.h.clock() - 86400),
                event_time=None,
            )
            return dict(
                schema_version=2,
                request_id=old["request_id"],
                effective_scope=scope,
                evidence=[episode],
                query_results=[
                    dict(
                        query_id="current",
                        status="matched",
                        evidence_ids=["evidence:old"],
                        omissions=[],
                    )
                ],
                bundle=dict(
                    **{
                        key: old[key]
                        for key in (
                            "scope_version",
                            "association_version",
                            "scope_checks",
                            "verified_at",
                            "valid_until",
                        )
                    },
                    group_dependencies=[],
                    history_snapshot=snapshot,
                    input_history_snapshot=snapshot,
                ),
                budget_used=dict(bytes=500, tokens=500),
                omissions=[],
            )

        async def verify_context(origin, scope, bundle, checks=()):
            self.verifications.append(copy.deepcopy(checks))
            if self.invalid:
                raise Fault("scope_changed")

        self.h.memory.query_context = query_context
        self.h.memory.verify_context = verify_context

    async def test_recent_human_context_coexists_with_long_term_episode_and_checks_batch(self):
        for text in ("我们的旅行先保留计划", "后来提到的行程是什么？"):
            await self.h.ingest(text=text)
            await self.h.cycles()
        self.assertEqual("sent", self.h.turns()[-1]["phase"])
        prompt = json.loads(self.h.gateway.calls[-1][1][1]["content"])
        self.assertEqual("当时我只是计划周五出发。", prompt["episodes"][0]["text"])
        self.assertIn("我们的旅行先保留计划", canonical(prompt["recent_dialogue"]))
        self.assertTrue(
            any(checks and checks[0]["kind"] == "history" for checks in self.verifications)
        )
        before = len(self.verifications)
        await self.h.core._preflight(self.h.turns()[-1])
        self.assertEqual(before + 1, len(self.verifications))

    async def test_each_stream_boundary_reverifies_bundle_and_stops_after_withdrawal(self):
        from test_delivery_v2 import NativeQueue, source_turn

        turn = await source_turn(self.h)
        response = await self.h.memory.query_context(
            turn["origin"], turn["scope"], "合成", dict(tokens=8192, bytes=8192)
        )
        turn.update(memory_context=response, preparation=selection(response))
        self.h.core._save_turn(turn)
        queue = NativeQueue(self.h.clock)
        self.h.core.sender = queue
        await self.h.core.stream_segment(turn["id"], "第一段已送出。")
        self.invalid = True
        with self.assertRaises(Fault):
            await self.h.core.stream_segment(turn["id"], "撤回后不得送出。")
        self.assertEqual(1, len(queue.requests))
        self.assertEqual(2, len(self.verifications))

    async def test_renewed_history_lease_gets_fresh_memory_proof_inside_preparation(self):
        from test_delivery_v2 import NativeQueue
        from tianshu_companion.context_checks import VerificationCheckpoint

        for text in ("第一条合成人话。", "另一条合成人话。"):
            await self.h.ingest(text=text)
            await self.h.cycles()
        previous, current = self.h.turns()
        issuer = self.h.origins.values[previous["origin"]["assertion_ref"]]
        issuer["expires_at"] = utc(self.h.clock() + 60)
        previous["source_deadline"] = epoch(issuer["expires_at"])
        self.h.core._save_turn(previous)
        queue = NativeQueue(self.h.clock)
        renewals = []

        async def context(source, scope, channel):
            self.assertEqual(previous["origin"], source["origin"])
            renewals.append(source["origin"])
            issuer["expires_at"] = utc(self.h.clock() + 60)
            return dict(
                origin=source["origin"],
                scope=scope,
                channel=channel,
                expires_at=issuer["expires_at"],
            )

        queue.expression_context = context
        self.h.core.sender = queue
        before = len(self.verifications)
        async with VerificationCheckpoint() as checkpoint:
            await self.h.core._verify_checks(
                current, [scope_check(previous)], checkpoint=checkpoint
            )
            self.h.clock.advance(61)
            await self.h.core._verify_checks(
                current, [scope_check(previous)], checkpoint=checkpoint
            )
        self.assertEqual(1, len(renewals))
        self.assertEqual(before + 2, len(self.verifications))

    async def test_memory_tool_reassembles_raw_context_and_its_checks_before_next_model(self):
        await self.h.ingest(text="我们先讨论杯子的名字。")
        await self.h.cycles()
        verify = self.h.memory.verify_context
        messages_seen = []

        async def verify_context(origin, scope, bundle, checks=()):
            await verify(origin, scope, bundle, checks)
            if self.h.memory.scope_version > 1 and any(c["kind"] == "history" for c in checks):
                raise Fault("scope_changed")

        async def propose(request):
            self.h.memory.scope_version += 1
            return dict(state="committed", record_ids=["record:new"])

        async def complete(turn, messages, *, tools=None, on_delta=None):
            messages_seen.append(copy.deepcopy(messages))
            if len(messages_seen) == 1:
                value = dict(
                    kind="upsert",
                    target=dict(
                        category="evidence",
                        field_key="cup",
                        item_key=None,
                        record_id=None,
                        expected_version=None,
                    ),
                    units=[
                        dict(
                            statement="我的杯子叫晚星",
                            conditions=[],
                            negations=[],
                            valid_time="current",
                            uncertainty="confirmed",
                            reality="real",
                        )
                    ],
                    intent=dict(source_quote="记住我的杯子叫晚星", unambiguous_target=True),
                )
                message = dict(
                    role="assistant",
                    content=None,
                    tool_calls=[
                        dict(
                            id="tool:memory",
                            type="function",
                            function=dict(name="memory_propose", arguments=canonical(value)),
                        )
                    ],
                )
            else:
                message = dict(role="assistant", content="已按原话记下。")
            return message, dict(config_version=1, fixture_only=True)

        self.h.memory.verify_context = verify_context
        self.h.memory.propose = propose
        self.h.gateway.complete = complete
        await self.h.ingest(text="记住我的杯子叫晚星")
        await self.h.cycles(80)
        self.assertEqual(2, len(messages_seen))
        current = self.h.turns()[-1]
        self.assertEqual("sent", current["phase"])
        prompt = json.loads(messages_seen[-1][1]["content"])
        self.assertIn("我们先讨论杯子的名字", canonical(prompt["recent_dialogue"]))
        self.assertTrue(all(not group["replies"] for group in prompt["recent_dialogue"]))
        self.assertTrue(current["context_checks"])
        self.assertTrue(
            all(check["version_domain"] == "text-input/v1" for check in current["context_checks"])
        )

    async def test_300_turns_bound_optional_reply_proof_without_losing_human_inputs(self):
        omitted = []
        counts = []
        durations = []
        for index in range(300):
            started = time.perf_counter()
            await self.h.ingest(text=f"这轮合成花盆观察编号{index}。")
            await self.h.cycles()
            durations.append(time.perf_counter() - started)
            turn = self.h.turns()[-1]
            self.assertEqual("sent", turn["phase"], index)
            checks = turn["context_checks"]
            counts.append(len(checks))
            referenced = {c["source_turn_id"] for c in checks if c.get("source_turn_id")}
            self.assertLessEqual(len(referenced), 4)
            omitted.extend(turn["short_context"]["reply_dependency_omissions"])
            prompt = json.loads(self.h.gateway.calls[-1][1][1]["content"])
            if index == 1:
                self.assertTrue(prompt["recent_dialogue"][0]["replies"])
            if turn["short_context"]["reply_dependency_omissions"]:
                for group in prompt["recent_dialogue"]:
                    if group["turn_id"] in turn["short_context"]["reply_dependency_omissions"]:
                        self.assertEqual([], group["replies"])
                        self.assertEqual("omitted", group["reply_text_visibility"])
                        self.assertTrue(group["messages"])
        self.assertTrue(omitted)
        self.assertLessEqual(max(counts[100:]), max(counts[:100]))
        mandatory = [scope_check(turn) for turn in self.h.turns()[:65]]
        await self.h.core._verify_checks(self.h.turns()[-1], mandatory)
        self.assertEqual(65, len(self.verifications[-1]))
        # Report observations, not an environment-sensitive latency threshold.
        print(
            "v2-300-turns",
            dict(
                max_checks=max(counts),
                first50_seconds=sum(durations[:50]),
                last50_seconds=sum(durations[-50:]),
                total_seconds=sum(durations),
            ),
        )
