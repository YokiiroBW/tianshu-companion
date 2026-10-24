"""Rev9 protocol relations and real Core planning; no external services."""

import asyncio
import copy
import json
import ssl
import time
import unittest

import httpx

from support import contracts
from test_memory_v2 import CoreEvidenceTests, example
from tianshu_companion.clients import JsonService, Memory, utc
from tianshu_companion.context import scope_check
from tianshu_companion.contracts import Fault, canonical
from tianshu_companion.memory_preparation import packages
from tianshu_companion.preparation_retry import retry_preparation


class PreparationProtocolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = example("memory-context/v2", "prepare-optional-independent-rejection")
        self.calls = []

    async def client(self, change=None):
        async def handler(request):
            body = json.loads(request.content)
            self.calls.append((request.url.path, body))
            result = copy.deepcopy(self.fixture["prepare_response"])
            result.update(
                request_id=body["query"]["request_id"],
                effective_scope=body["requested_scope"],
                checked_at=utc(time.time()),
                valid_until=utc(time.time() + 30),
            )
            result["bundle"] = dict(
                body["bundle"], verified_at=utc(time.time()), valid_until=utc(time.time() + 60)
            )
            result["checks"] = [
                dict(check_id=c["check_id"], kind=c["kind"], status="valid") for c in body["checks"]
            ]
            if change:
                result = change(body, result)
            return httpx.Response(200, json=result)

        wire = JsonService(
            "https://synthetic.invalid", "synthetic-token", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        return Memory(contracts(), wire)

    async def prepare(self, memory, request=None):
        r = request or self.fixture["prepare_request"]
        return await memory.prepare_context(
            r["query"]["origin"],
            r["requested_scope"],
            r["bundle"],
            r["checks"],
            r["required_check_ids"],
        )

    async def test_all_packages_required_and_optional_results_are_collected(self):
        request = copy.deepcopy(self.fixture["prepare_request"])
        seed = request["checks"][0]
        request["checks"] = [dict(copy.deepcopy(seed), check_id=f"check:{i}") for i in range(65)]
        request["required_check_ids"] = ["check:64"]

        def reject(body, response):
            for c in response["checks"]:
                if c["check_id"] == "check:0":
                    c.update(status="rejected", reason="dependency_changed")
            return response

        results = await self.prepare(await self.client(reject), request)
        self.assertEqual([64, 1], [len(r["checks"]) for _, r in self.calls])
        self.assertEqual([[], ["check:64"]], [r["required_check_ids"] for _, r in self.calls])
        self.assertEqual(65, len(results))
        self.assertEqual("rejected", results["check:0"])
        self.assertEqual("valid", results["check:64"])

    async def test_typed_invalid_preserves_http200_and_only_selection_reselects(self):
        for component in ["current_selection", "required_check"]:
            with self.subTest(component=component):
                fixture = example("memory-context/v2", "prepare-invalid-" + component)
                request = fixture["prepare_request"]

                def invalid(body, response):
                    value = copy.deepcopy(fixture["prepare_response"])
                    value["request_id"] = value["failure"]["request_id"] = body["query"][
                        "request_id"
                    ]
                    return value

                before = len(self.calls)
                with self.assertRaises(Fault) as caught:
                    await self.prepare(await self.client(invalid), request)
                error = caught.exception
                self.assertEqual(200, error.cause["http_status"])
                self.assertEqual(
                    component == "current_selection",
                    getattr(error, "memory_selection_invalid", False),
                )
                self.assertEqual(before + 1, len(self.calls))
                turn = dict(model_calls=0, source_deadline=time.time() + 60)
                self.assertEqual(
                    component == "current_selection", retry_preparation(turn, error, time.time())
                )

    async def test_invalid_relations_never_become_optional_success(self):
        def missing(b, r):
            r["checks"].pop()
            return r

        def wrong_order(b, r):
            r["checks"].reverse()
            return r

        def wrong_bundle(b, r):
            r["bundle"]["association_version"] += 1
            return r

        def required_rejected(b, r):
            r["checks"][0].update(status="rejected", reason="origin_denied")
            return r

        request = copy.deepcopy(self.fixture["prepare_request"])
        request["required_check_ids"] = [request["checks"][0]["check_id"]]
        for change in [missing, wrong_order, wrong_bundle, required_rejected]:
            with self.subTest(change=change.__name__), self.assertRaises(Fault):
                await self.prepare(await self.client(change), request)

    def test_id_collision_and_utf8_size_are_not_silently_truncated(self):
        r = self.fixture["prepare_request"]
        check = copy.deepcopy(r["checks"][0])
        changed = copy.deepcopy(check)
        changed["expected"] = {}
        with self.assertRaises(Fault):
            packages(r["query"]["origin"], r["requested_scope"], r["bundle"], [check, changed], [])
        # Exercise byte partitioning independently of schema field limits.
        large = [dict(check, check_id=f"check:{i}", test_padding="汉" * 30000) for i in range(4)]
        batches = packages(r["query"]["origin"], r["requested_scope"], r["bundle"], large, [])
        self.assertGreater(len(batches), 1)
        self.assertTrue(all(len(canonical(b).encode()) <= 262144 for b in batches))
        self.assertEqual(4, sum(len(b["checks"]) for b in batches))

    async def test_unknown_failure_does_not_turn_into_typed_selection_invalid(self):
        async def handler(request):
            return httpx.Response(
                503,
                json=dict(
                    schema_version=1,
                    request_id=json.loads(request.content)["query"]["request_id"],
                    code="dependency_unavailable",
                    execution_state="unknown",
                    retryable=False,
                ),
            )

        wire = JsonService(
            "https://synthetic.invalid", "token", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        with self.assertRaises(Fault) as caught:
            await self.prepare(Memory(contracts(), wire))
        self.assertFalse(getattr(caught.exception, "memory_selection_invalid", False))

    async def test_later_required_failure_discards_previous_package_results(self):
        request = copy.deepcopy(self.fixture["prepare_request"])
        request["checks"] = [
            dict(copy.deepcopy(request["checks"][0]), check_id=f"check:{i}") for i in range(65)
        ]
        request["required_check_ids"] = ["check:64"]

        def failure(body, response):
            if body["required_check_ids"]:
                return dict(
                    schema_version=2,
                    version_domain="memory-context/v2",
                    request_id=body["query"]["request_id"],
                    effective_scope=body["requested_scope"],
                    state="invalid",
                    component="required_check",
                    check_id="check:64",
                    failure=dict(
                        schema_version=1,
                        request_id=body["query"]["request_id"],
                        code="scope_changed",
                        execution_state="not_started",
                        retryable=False,
                    ),
                )
            return response

        with self.assertRaises(Fault) as caught:
            await self.prepare(await self.client(failure), request)
        self.assertFalse(getattr(caught.exception, "memory_selection_invalid", False))
        self.assertEqual(2, len(self.calls))

    async def test_later_package_does_not_extend_an_expired_earlier_proof(self):
        from unittest.mock import patch

        request = copy.deepcopy(self.fixture["prepare_request"])
        request["checks"] = [
            dict(copy.deepcopy(request["checks"][0]), check_id=f"check:{i}") for i in range(65)
        ]
        request["required_check_ids"] = []
        clock = [time.time()]

        def expiry(body, response):
            if len(self.calls) == 1:
                response["valid_until"] = utc(clock[0] + 0.5)
            else:
                clock[0] += 1
                response.update(checked_at=utc(clock[0]), valid_until=utc(clock[0] + 30))
            return response

        memory = await self.client(expiry)
        with patch("tianshu_companion.memory_preparation.time.time", side_effect=lambda: clock[0]):
            with self.assertRaises(Fault) as caught:
                await self.prepare(memory, request)
        self.assertEqual("scope_changed", caught.exception.code)
        self.assertEqual(2, len(self.calls))

    async def test_profile_batch_preserves_target_order_and_rejects_missing_response(self):
        fixture = example("memory-context/v2", "profile-batch-empty-is-valid")
        request = fixture["profile_batch_request"]
        request["requests"].append(copy.deepcopy(request["requests"][0]))
        request["requests"][1]["query"]["request_id"] = "request:second"
        request["requests"][1]["target"] = dict(kind="person", person_id="person:second")
        request["requests"][1]["selection"] = ["interest"]
        missing = False

        async def handler(http_request):
            body = json.loads(http_request.content)
            self.calls.append((http_request.url.path, body))
            values = []
            for item in body["requests"]:
                value = copy.deepcopy(fixture["profile_batch_response"]["responses"][0])
                value.update(
                    request_id=item["query"]["request_id"],
                    target=item["target"],
                    verified_at=utc(time.time()),
                    valid_until=utc(time.time() + 60),
                )
                values.append(value)
            return httpx.Response(
                200,
                json=dict(
                    schema_version=2,
                    version_domain="memory-context/v2",
                    request_id=body["query"]["request_id"],
                    effective_scope=body["requested_scope"],
                    state="valid",
                    responses=values[:-1] if missing else values,
                    checked_at=utc(time.time()),
                    valid_until=utc(time.time() + 30),
                ),
            )

        wire = JsonService(
            "https://synthetic.invalid", "token", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        memory = Memory(contracts(), wire)
        values = await memory.profiles_batch(
            request["query"]["origin"], request["requested_scope"], request["requests"]
        )
        self.assertEqual([r["target"] for r in request["requests"]], [r["target"] for r in values])
        self.assertEqual(["/internal/v2/memory/context/profiles"], [p for p, _ in self.calls])
        missing = True
        with self.assertRaises(Fault):
            await memory.profiles_batch(
                request["query"]["origin"], request["requested_scope"], request["requests"]
            )

    async def test_independent_unavailable_is_not_valid_and_global_required_origin_wins(self):
        request = copy.deepcopy(self.fixture["prepare_request"])
        seed = request["checks"][0]
        request["checks"] = [dict(copy.deepcopy(seed), check_id=f"check:{i}") for i in range(65)]
        for index in [0, 64]:
            request["checks"][index]["request"]["query"]["origin"] = dict(
                assertion_ref="origin:independent"
            )
        request["required_check_ids"] = []

        def unavailable(body, response):
            for item in response["checks"]:
                if item["check_id"] == "check:0":
                    item.update(status="unavailable", reason="origin_unavailable")
            return response

        result = await self.prepare(await self.client(unavailable), request)
        self.assertEqual("unavailable", result["check:0"])
        self.assertEqual("unavailable", result["check:64"])
        request["required_check_ids"] = ["check:64"]
        before = len(self.calls)
        with self.assertRaises(Fault) as caught:
            await self.prepare(await self.client(unavailable), request)
        self.assertEqual("dependency_unavailable", caught.exception.code)
        self.assertEqual(200, caught.exception.cause["http_status"])
        self.assertEqual(before + 1, len(self.calls))
        self.assertFalse(getattr(caught.exception, "memory_selection_invalid", False))

    async def test_pure_timeout_marker_excludes_tls_protocol_and_unclassified_connection(self):
        for kind in ["timeout", "reset", "tls", "protocol", "connect", "pool"]:
            with self.subTest(kind=kind):

                async def handler(request):
                    if kind == "timeout":
                        raise httpx.ReadTimeout("synthetic", request=request)
                    if kind == "reset":
                        raise httpx.ReadError(
                            "synthetic", request=request
                        ) from ConnectionResetError()
                    if kind == "tls":
                        raise httpx.ReadError(
                            "synthetic", request=request
                        ) from ssl.SSLCertVerificationError()
                    if kind == "protocol":
                        raise httpx.RemoteProtocolError("synthetic", request=request)
                    if kind == "pool":
                        raise httpx.PoolTimeout("synthetic", request=request)
                    raise httpx.ConnectError("synthetic", request=request)

                wire = JsonService(
                    "https://synthetic.invalid", "token", transport=httpx.MockTransport(handler)
                )
                self.addAsyncCleanup(wire.close)
                with self.assertRaises(Fault) as caught:
                    await wire.call("/internal/v1/origins/resolve", dict(request_id="request:test"))
                self.assertEqual(
                    kind in {"timeout", "reset"},
                    getattr(caught.exception, "optional_read_unavailable", False),
                )
                # The pre-existing query recovery policy is deliberately untouched.
                self.assertTrue(caught.exception.retryable_read)


class CorePreparationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = CoreEvidenceTests.asyncSetUp

    async def turns(self):
        for text in ["合成旧计划。", "合成后续问题。"]:
            await self.h.ingest(text=text)
            await self.h.cycles()
        self.assertTrue(all(t["phase"] == "sent" for t in self.h.turns()))
        return self.h.turns()

    async def test_one_catalog_selects_input_variant_and_preserves_mandatory(self):
        previous, current = await self.turns()
        calls = []

        async def prepare(origin, scope, bundle, checks, required):
            calls.append((copy.deepcopy(checks), list(required)))
            return {
                c["check_id"]: "rejected" if c["kind"] == "history" else "valid" for c in checks
            }

        self.h.memory.prepare_context = prepare
        context, *_ = await self.h.core._prepare_context(current, [])
        self.assertEqual(1, len(calls))
        self.assertEqual({"history", "input_history"}, {c["kind"] for c in calls[0][0]})
        self.assertTrue(context.data["recent_dialogue"][0]["input_only"])
        self.assertFalse(context.data["recent_dialogue"][0]["replies"])

    async def test_external_unknown_and_cancel_cannot_become_an_empty_valid_variant(self):
        previous, current = await self.turns()
        calls = []

        async def prepare(*args):
            calls.append(args)
            return {}

        self.h.memory.prepare_context = prepare
        self.h.origins.unavailable = True
        with self.assertRaises(Fault):
            await self.h.core._prepare_context(current, [])
        self.assertFalse(calls)
        self.h.origins.unavailable = False
        entered = asyncio.Event()

        async def blocked(*args):
            entered.set()
            await asyncio.Event().wait()

        self.h.memory.prepare_context = blocked
        job = asyncio.create_task(self.h.core._prepare_context(current, []))
        await asyncio.wait_for(entered.wait(), 1)
        job.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await job

    async def test_renewal_during_batch_does_not_extend_old_authority(self):
        previous, current = await self.turns()

        async def prepare(origin, scope, bundle, checks, required):
            fresh = self.h.core.store.get("turns", previous["id"])
            fresh["source_deadline"] += 10
            self.h.core._save_turn(fresh)
            return {c["check_id"]: "valid" for c in checks}

        self.h.memory.prepare_context = prepare
        with self.assertRaises(Fault) as caught:
            await self.h.core._prepare_context(current, [])
        self.assertTrue(caught.exception.retryable_read)
        self.assertFalse(getattr(caught.exception, "memory_selection_invalid", False))

    async def test_local_source_withdrawal_rejects_full_and_input_variants(self):
        previous, current = await self.turns()
        check = scope_check(previous)
        check["binding_version"] += 1
        # Both variants must fail the same proved local authority comparison.
        from unittest.mock import patch

        with patch("tianshu_companion.context_preparation.recent_checks", return_value=[check]):
            context, *_ = await self.h.core._prepare_context(current, [])
        self.assertEqual([], context.data["recent_dialogue"])

    async def test_required_reference_wins_over_optional_shared_check(self):
        previous, current = await self.turns()
        from tianshu_companion.context_preparation import _v2_variants
        from tianshu_companion.context import recent_checks
        from tianshu_companion.short_context import select_recent

        selected, _ = select_recent(
            self.h.core.store, current, self.h.core.short_context_policy, self.h.clock()
        )
        mandatory = recent_checks(self.h.core.store, selected)
        calls = []

        async def prepare(origin, scope, bundle, checks, required):
            calls.append((checks, required))
            return {c["check_id"]: "valid" for c in checks}

        self.h.memory.prepare_context = prepare
        await _v2_variants(self.h.core, current, mandatory, [selected])
        self.assertEqual(1, len(calls))
        ids = [c["check_id"] for c in calls[0][0]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(calls[0][1])
        self.assertTrue(set(calls[0][1]) <= set(ids))

    async def test_optional_author_transport_unavailable_omits_both_variants(self):
        previous, current = await self.turns()
        original = self.h.origins.resolve

        async def unavailable(service, envelope, now):
            if envelope["origin"] == previous["origin"]:
                error = Fault("dependency_unavailable", retryable_read=True)
                error.origin_read_unavailable = True
                raise error
            return await original(service, envelope, now)

        self.h.origins.resolve = unavailable
        context, *_ = await self.h.core._prepare_context(current, [])
        self.assertEqual([], context.data["recent_dialogue"])
        from tianshu_companion.context_preparation import _v2_variants

        with self.assertRaises(Fault):
            await _v2_variants(self.h.core, current, [scope_check(previous)], [])

    async def test_renewal_transport_unavailable_still_fails_whole_preparation(self):
        previous, current = await self.turns()
        previous["source_deadline"] = self.h.clock() + 10
        self.h.core._save_turn(previous)

        async def unavailable(turn, **kwargs):
            error = Fault("dependency_unavailable", retryable_read=True)
            error.optional_read_unavailable = True
            raise error

        self.h.core.continue_delivery_origin = unavailable
        with self.assertRaises(Fault) as caught:
            await self.h.core._prepare_context(current, [])
        self.assertTrue(caught.exception.context_renewal_failure)

    async def test_unavailable_origin_lease_is_not_used_to_fail_valid_current_material(self):
        previous, current = await self.turns()
        previous["source_deadline"] = self.h.clock() + 30
        self.h.core._save_turn(previous)
        issuer = self.h.origins.values[previous["origin"]["assertion_ref"]]
        issuer["expires_at"] = utc(previous["source_deadline"])

        async def prepare(origin, scope, bundle, checks, required):
            self.h.clock.advance(31)
            return {check["check_id"]: "unavailable" for check in checks}

        self.h.memory.prepare_context = prepare
        context, *_ = await self.h.core._prepare_context(current, [])
        self.assertEqual([], context.data["recent_dialogue"])


del CoreEvidenceTests  # Reuse only setup, not the imported full test class.
