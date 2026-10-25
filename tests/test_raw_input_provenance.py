"""Raw human inputs use exact source proofs, not their old query's history domain."""

import asyncio
import copy
import json
import unittest

import httpx

from test_memory_v2 import CoreEvidenceTests
from tianshu_companion.clients import JsonService, Memory, epoch, utc
from tianshu_companion.context import profile_check, recent_checks, scope_check
from tianshu_companion.context_preparation import _v2_variants
from tianshu_companion.context_relevance import input_only
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.short_context import select_recent


class RawInputProvenanceTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = CoreEvidenceTests.asyncSetUp

    async def turns(self, count=2):
        for index in range(count):
            await self.h.ingest(
                text=f"合成第{index}位作者的原始输入。",
                account=f"raw-owner-{index}",
                group=True,
                channel="group:raw-proof",
            )
            await self.h.cycles()
        turns = self.h.turns()
        self.assertEqual(["sent"] * count, [turn["phase"] for turn in turns])
        self.source_calls, self.independent_calls, self.preparations = [], [], []
        original_verify = self.h.memory.verify_context
        original_source = self.h.memory.check_sources

        async def valid(request):
            return "valid"

        self.raw_result = valid

        async def statuses(checks):
            result = {}
            for check in checks:
                value = "valid"
                if check["kind"] == "input_sources":
                    request = copy.deepcopy(check["request"]["source_check"])
                    self.source_calls.append(request)
                    value = await self.raw_result(request)
                result[check["check_id"]] = value
            return result

        async def verify(origin, scope, bundle, checks=()):
            await original_verify(origin, scope, bundle, checks)
            if any(value != "valid" for value in (await statuses(checks)).values()):
                raise Fault(
                    "scope_changed", cause=dict(peer="memory", operation="query", http_status=409)
                )

        async def prepare(origin, scope, bundle, checks, required):
            self.preparations.append(copy.deepcopy(checks))
            await original_verify(origin, scope, bundle, checks)
            result = await statuses(checks)
            if any(result[key] != "valid" for key in required):
                raise Fault(
                    "scope_changed", cause=dict(peer="memory", operation="query", http_status=200)
                )
            return result

        async def independent(turn, selected):
            self.independent_calls.append((turn["id"], copy.deepcopy(selected)))
            return await original_source(turn, selected)

        self.h.memory.prepare_context = prepare
        self.h.memory.verify_context = verify
        self.h.memory.check_sources = independent
        self.verifications.clear()
        return turns

    def groups(self, current):
        selected, _ = select_recent(
            self.h.core.store, current, self.h.core.short_context_policy, self.h.clock()
        )
        return {group["turn_id"]: group for group in selected}

    def raw(self, previous):
        return scope_check(
            previous,
            input_only=True,
            raw_input=dict(
                actor_id=previous["scope"]["actor_id"],
                person_id=previous["scope"]["person_id"],
                author=previous["bundle"]["collection_key"]["author"],
                collection_id=previous["bundle"]["collection_id"],
                input_revision=previous["bundle"]["collection_revision"],
                messages=previous["bundle"]["messages"],
            ),
        )

    def adopt(self, current, checks):
        current.update(context_checks=checks, profile_checks=[], model_calls=0)
        self.h.core._save_turn(current)

    def lease(self, turn, seconds):
        expiry = utc(self.h.clock() + seconds)
        self.h.origins.values[turn["origin"]["assertion_ref"]]["expires_at"] = expiry
        fresh = self.h.core.store.get("turns", turn["id"])
        fresh["source_deadline"] = epoch(expiry)
        self.h.core.store.put("turns", fresh)
        turn["source_deadline"] = fresh["source_deadline"]

    def withdraw(self, turn):
        key = digest(turn["bundle"]["messages"][0]["message_key"])
        physical = self.h.core.store.get("physicals", key)
        physical["fact"]["state"] = "retracted"
        self.h.core.store.put("physicals", physical)

    def source_client(self, handler):
        def respond(incoming):
            self.assertEqual("/internal/v1/memory/source-sync/check", incoming.url.path)
            return handler(json.loads(incoming.content))

        wire = JsonService(
            "https://synthetic-memory.invalid",
            "synthetic-only",
            label="memory",
            transport=httpx.MockTransport(respond),
        )
        self.addAsyncCleanup(wire.close)
        return Memory(self.h.contracts, wire)

    def source_response(self, request, *, invalid=False):
        if invalid:
            return httpx.Response(409, json=Fault("version_conflict").wire(request["request_id"]))
        return httpx.Response(
            200,
            json=dict(
                schema_version=1,
                request_id=request["request_id"],
                request_digest=digest(request),
                scope=request["scope"],
                version_domain="text-dialogue/v1",
                scope_version=1,
                checked_at=utc(self.h.clock()),
            ),
        )

    async def test_source_invalid_requires_bound_complete_common_error_response(self):
        previous, _ = await self.turns()
        sources = [m["source"] for m in previous["bundle"]["messages"]]
        cases = [
            ("confirmed", 409, {}, None),
            ("wrong_request", 409, dict(request_id="req:other"), None),
            ("wrong_schema", 409, dict(schema_version=2), None),
            ("missing_field", 409, {}, "schema_version"),
            ("extra_field", 409, dict(peer_detail="untrusted"), None),
            ("unknown", 409, dict(execution_state="unknown"), None),
            ("retryable", 409, dict(retryable=True), None),
            ("wrong_code", 409, dict(code="scope_changed"), None),
            ("permission", 403, dict(code="forbidden"), None),
            ("barrier", 503, dict(code="dependency_unavailable", retryable=True), None),
            ("wrong_status", 400, {}, None),
            ("malformed_json", 409, {}, None),
            ("encoded_body", 409, {}, None),
            ("partial_body", 409, {}, None),
        ]
        for name, status, changes, missing in cases:
            with self.subTest(case=name):

                def response(request):
                    value = Fault("version_conflict").wire(request["request_id"])
                    value.update(changes)
                    if missing:
                        del value[missing]
                    if name == "malformed_json":
                        return httpx.Response(status, content=b'{"code":"version_conflict"')
                    if name == "encoded_body":
                        return httpx.Response(
                            status,
                            headers={"Content-Encoding": "gzip"},
                            stream=httpx.ByteStream(json.dumps(value).encode()),
                        )
                    if name == "partial_body":

                        class Partial(httpx.AsyncByteStream):
                            async def __aiter__(self):
                                yield json.dumps(value).encode()
                                raise httpx.ReadError("synthetic incomplete response")

                        return httpx.Response(status, stream=Partial())
                    return httpx.Response(status, json=value)

                memory = self.source_client(response)
                with self.assertRaises(Fault) as caught:
                    await memory.check_sources(previous, sources)
                self.assertEqual(
                    name == "confirmed", getattr(caught.exception, "source_invalid", False)
                )
                self.assertEqual(status, caught.exception.cause["http_status"])
                if name == "confirmed":
                    # Non-context callers still receive their original409 Fault.
                    self.assertEqual("version_conflict", caught.exception.code)
                    self.assertFalse(caught.exception.unknown)
                    self.assertFalse(caught.exception.retryable_read)

    async def test_confirmed_memory_negative_excludes_whole_linked_raw_batch_only(self):
        first, second, current = await self.turns(3)
        groups = self.groups(current)

        async def result(request):
            return "rejected" if request["turn_id"] == first["id"] else "valid"

        self.raw_result = result
        required = profile_check(
            first,
            dict(kind="person", person_id=first["scope"]["person_id"]),
            "当前画像",
            ["interest"],
            dict(scope_version=self.h.memory.profile_version),
        )
        batch = [input_only(groups[t["id"]]) for t in (first, second)]
        self.assertEqual([[]], await _v2_variants(self.h.core, current, [required], [batch]))
        self.assertCountEqual(
            [first["id"], second["id"]], [r["turn_id"] for r in self.source_calls]
        )
        self.assertEqual({"profile", "input_sources"}, {c["kind"] for c in self.preparations[-1]})
        self.assertEqual(first["id"], self.h.core._context_source(self.raw(first))["id"])
        # Same origin is not the same source dependency; no negative cache or
        # physical withdrawal was installed by the optional negative result.
        result = await _v2_variants(self.h.core, current, [required], [[batch[1]]])
        self.assertEqual([second["id"]], [g["turn_id"] for g in result[0][0][0]])
        self.assertEqual([], self.independent_calls)

    async def test_confirmed_negative_remains_fault_for_required_main_and_adopted_raw(self):
        previous, current = await self.turns()

        async def rejected(request):
            return "rejected"

        self.raw_result = rejected
        for stage in ("required", "main", "adopted"):
            with self.subTest(stage=stage):
                check = self.raw(current if stage == "main" else previous)
                with self.assertRaises(Fault) as caught:
                    if stage == "adopted":
                        self.adopt(current, [check])
                        await self.h.core._preflight_once(current)
                    else:
                        await _v2_variants(self.h.core, current, [check], [])
                self.assertEqual("scope_changed", caught.exception.code)
                self.assertEqual(
                    409 if stage == "adopted" else 200, caught.exception.cause["http_status"]
                )
                self.assertFalse(caught.exception.unknown)
                self.assertFalse(getattr(caught.exception, "memory_selection_invalid", False))
        self.assertEqual([], self.independent_calls)

    async def test_unproved_source_errors_still_fail_optional_preparation(self):
        previous, current = await self.turns()
        group = input_only(self.groups(current)[previous["id"]])
        for code, unknown in (
            ("version_conflict", False),
            ("forbidden", False),
            ("dependency_unavailable", False),
            ("result_unknown", True),
        ):
            with self.subTest(code=code, unknown=unknown):

                async def failed(request):
                    raise Fault(code, unknown=unknown)

                self.raw_result = failed
                with self.assertRaises(Fault) as caught:
                    await _v2_variants(self.h.core, current, [], [[group]])
                self.assertEqual(code, caught.exception.code)
                self.assertEqual(unknown, caught.exception.unknown)
                self.assertFalse(getattr(caught.exception, "context_dependency_invalid", False))
        self.assertEqual([], self.independent_calls)

    async def test_forget_tool_refresh_and_next_turn_omit_still_locally_active_raw(self):
        await self.h.ingest(text="这段原始输入稍后会被检索遗忘。")
        await self.h.cycles()
        previous = self.h.turns()[-1]
        current, value, _, submitted, _ = await CoreEvidenceTests.episode_forget(self)
        forgotten, calls = False, []
        original_forget = self.h.memory.forget_episode
        original_query = self.h.memory.query_context

        async def forget(request):
            nonlocal forgotten
            result = await original_forget(request)
            forgotten = True
            return result

        async def query(*args, **kwargs):
            result = await original_query(*args, **kwargs)
            if forgotten:
                result["evidence"] = []
                result["query_results"] = []
            return result

        async def prepare(origin, scope, bundle, checks, required):
            calls.extend(
                c["request"]["source_check"]["turn_id"]
                for c in checks
                if c["kind"] == "input_sources"
            )
            return {c["check_id"]: "rejected" for c in checks}

        self.h.memory.forget_episode = forget
        self.h.memory.query_context = query
        self.h.memory.prepare_context = prepare
        receipt = await self.h.core.role_actions.memory_propose(current, "tool:forget-raw", value)
        self.assertEqual("tombstoned", receipt["state"])
        self.assertEqual(1, len(submitted))
        refreshed = self.h.core.store.get("turns", current["id"])
        context, *_ = await self.h.core._prepare_context(refreshed, [])
        self.assertEqual([], context.data["recent_dialogue"])
        self.assertIn(previous["id"], calls)
        self.assertEqual(previous["id"], self.h.core._context_source(self.raw(previous))["id"])
        await self.h.ingest(text="继续一个不依赖那段原话的问题。")
        await self.h.cycles()
        self.assertEqual("sent", self.h.turns()[-1]["phase"])
        prompt = json.loads(self.h.gateway.calls[-1][1][1]["content"])
        self.assertNotIn(previous["id"], [g["turn_id"] for g in prompt["recent_dialogue"]])
        self.assertEqual(previous["id"], self.h.core._context_source(self.raw(previous))["id"])

    async def test_linked_raw_batch_survives_unrelated_old_history_without_splitting(self):
        *previous, current = await self.turns(4)
        groups = self.groups(current)
        old_snapshots = []
        for source in previous:
            old = copy.deepcopy(source["input_history_snapshot"])
            old["history_checks"].append(
                dict(
                    scope=dict(source["scope"], person_id="person:unrelated-old-owner"),
                    history_version=1,
                    association_id=None,
                    association_version=1,
                )
            )
            source["history_snapshot"] = source["input_history_snapshot"] = old
            self.h.core._save_turn(source)
            old_snapshots.append(copy.deepcopy(old))
        calls = []

        async def prepare(origin, scope, bundle, checks, required):
            calls.append(copy.deepcopy(checks))
            return {
                check["check_id"]: "valid" if check["kind"] == "input_sources" else "rejected"
                for check in checks
            }

        self.h.memory.prepare_context = prepare
        batch = [groups[source["id"]] for source in previous]
        variants = await _v2_variants(self.h.core, current, [], [batch])
        self.assertEqual(1, len(variants[0]))
        adopted, checks = variants[0][0]
        self.assertEqual([source["id"] for source in previous], [g["turn_id"] for g in adopted])
        self.assertTrue(all(g["input_only"] and not g["replies"] for g in adopted))
        self.assertEqual([], self.independent_calls)
        self.assertTrue(all("input_sources" in check for check in checks))
        self.assertEqual(2, len(calls))
        self.assertTrue(calls[0])
        self.assertEqual(3, len(calls[1]))
        self.assertEqual({"input_sources"}, {c["kind"] for c in calls[1]})
        self.assertNotIn("input_history", {check["kind"] for check in calls[0]})
        self.assertEqual(old_snapshots, [source["input_history_snapshot"] for source in previous])

    async def test_exact_payload_is_immutable_and_does_not_reinterpret_old_checks(self):
        previous, current = await self.turns()
        group = input_only(self.groups(current)[previous["id"]])
        old = recent_checks(self.h.core.store, [group])[0]
        raw = recent_checks(self.h.core.store, [group], exact_inputs=True)[0]
        self.assertIn("history_snapshot", old)
        self.assertNotIn("input_sources", old)
        self.assertNotIn("history_snapshot", raw)
        expected = copy.deepcopy(raw["input_sources"])
        previous["bundle"]["messages"][0]["source"]["receipt_id"] = "receipt:changed-copy"
        self.assertEqual(expected, raw["input_sources"])
        # Old persisted checks still have the exact four-kind history semantics.
        from tianshu_companion.clients import query
        from tianshu_companion.memory_context import legacy_check

        self.assertEqual("input_history", legacy_check(old, query(old["origin"]))["kind"])
        serialized = legacy_check(raw, query(raw["origin"]))
        self.assertEqual("input_sources", serialized["kind"])
        self.assertEqual({}, serialized["expected"])
        self.assertEqual(raw["source_turn_id"], serialized["request"]["source_check"]["turn_id"])
        self.assertEqual(raw["input_sources"], serialized["request"]["source_check"]["sources"])
        raw["input_sources"].clear()
        self.assertEqual(expected, serialized["request"]["source_check"]["sources"])

    async def test_raw_success_and_current_memory_bundle_are_both_required(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        self.invalid = True
        with self.assertRaises(Fault):
            await self.h.core._preflight_once(current)
        self.assertEqual(
            2, len(self.verifications)
        )  # Complete verify, then existing bundle-only probe.
        self.assertEqual("input_sources", self.verifications[0][0]["kind"])
        self.assertEqual([], self.verifications[1])
        self.assertEqual([], self.independent_calls)

    async def test_input_only_label_cannot_prove_visible_reply_text(self):
        previous, current = await self.turns()
        group = self.groups(current)[previous["id"]]
        self.assertTrue(group["replies"])
        group["input_only"] = True
        checks = recent_checks(self.h.core.store, [group], exact_inputs=True)
        self.assertTrue(checks)
        self.assertTrue(all("input_sources" not in check for check in checks))
        self.assertTrue(any("history_snapshot" in check for check in checks))

    async def test_selected_raw_payload_cannot_be_replaced_by_a_later_source_turn(self):
        previous, current = await self.turns()
        group = input_only(self.groups(current)[previous["id"]])
        saved = copy.deepcopy(previous)
        for change in (
            "input_revision",
            "input_sources",
            "actor",
            "person",
            "author",
            "collection",
        ):
            with self.subTest(change=change):
                fresh = copy.deepcopy(saved)
                if change == "input_revision":
                    fresh["bundle"]["collection_revision"] += 1
                elif change == "input_sources":
                    fresh["bundle"]["messages"].append(
                        copy.deepcopy(fresh["bundle"]["messages"][0])
                    )
                elif change in {"actor", "person"}:
                    fresh["scope"][change + "_id"] = change + ":changed"
                elif change == "author":
                    fresh["bundle"]["collection_key"]["author"]["immutable_account_id"] = "changed"
                else:
                    fresh["bundle"]["collection_id"] = "collection:changed"
                self.h.core.store.put("turns", fresh)
                try:
                    result = await _v2_variants(self.h.core, current, [], [[group]])
                    self.assertEqual([[]], result)
                    self.assertEqual([], self.source_calls)
                finally:
                    self.h.core.store.put("turns", saved)

    async def test_raw_only_catalog_is_verified_at_each_fresh_checkpoint(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        await self.h.core._preflight_once(current)
        await self.h.core._preflight_once(current)
        self.assertEqual(2, len(self.source_calls))
        self.assertEqual([1, 1], [len(c) for c in self.verifications])
        for request in self.source_calls:
            self.assertEqual(previous["id"], request["turn_id"])
            self.assertEqual(previous["bundle"]["collection_revision"], request["input_revision"])
            self.assertEqual(
                [m["source"] for m in previous["bundle"]["messages"]], request["sources"]
            )
            self.assertEqual(previous["scope"], request["scope"])
        self.assertNotEqual(self.source_calls[0]["request_id"], self.source_calls[1]["request_id"])
        self.assertEqual([], self.independent_calls)

    async def test_current_source_version_is_not_compared_to_old_history_version(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        await self.h.core._preflight_once(current)
        check = self.verifications[0][0]
        self.assertEqual({}, check["expected"])
        self.assertEqual({"query", "requested_scope", "source_check"}, set(check["request"]))
        self.assertEqual(1, len(self.source_calls))
        self.assertEqual([], self.independent_calls)

    async def test_memory_negative_or_unclassified_failure_never_becomes_optional_valid(self):
        previous, current = await self.turns()
        group = input_only(self.groups(current)[previous["id"]])
        for code, unknown in (
            ("version_conflict", False),
            ("dependency_unavailable", False),
            ("forbidden", False),
            ("result_unknown", True),
        ):
            with self.subTest(code=code):

                async def failed(*args):
                    raise Fault(code, unknown=unknown)

                self.h.memory.prepare_context = failed
                with self.assertRaises(Fault) as caught:
                    await _v2_variants(self.h.core, current, [], [[group]])
                self.assertEqual(code, caught.exception.code)
                self.assertEqual(unknown, caught.exception.unknown)
                self.assertEqual([], self.independent_calls)

    async def test_one_invalid_linked_member_cannot_leave_an_isolated_valid_anchor(self):
        first, second, current = await self.turns(3)
        groups = self.groups(current)
        batch = [input_only(groups[source["id"]]) for source in (first, second)]
        self.withdraw(second)
        result = await _v2_variants(self.h.core, current, [], [batch])
        self.assertEqual([[]], result)
        self.assertEqual([first["id"]], [request["turn_id"] for request in self.source_calls])

    async def test_required_raw_source_cannot_be_discarded_with_an_optional_batch(self):
        previous, current = await self.turns()
        check = self.raw(previous)
        self.withdraw(previous)
        with self.assertRaises(Fault):
            await _v2_variants(self.h.core, current, [check], [])
        self.assertEqual([], self.source_calls)

    async def test_memory_origin_unavailability_excludes_raw_aliases_but_rejection_does_not(self):
        previous, current = await self.turns()
        group = self.groups(current)[previous["id"]]
        self.assertTrue(group["replies"])
        for status in ("unavailable", "rejected"):
            with self.subTest(status=status):
                self.source_calls.clear()
                calls = []

                async def prepare(origin, scope, bundle, checks, required):
                    calls.append(copy.deepcopy(checks))
                    return {
                        check["check_id"]: "valid" if check["kind"] == "input_sources" else status
                        for check in checks
                    }

                self.h.memory.prepare_context = prepare
                variants = await _v2_variants(self.h.core, current, [], [[group]])
                self.assertEqual({"history"}, {check["kind"] for check in calls[0]})
                self.assertGreater(previous["source_deadline"], self.h.clock() + 35)
                if status == "unavailable":
                    self.assertEqual([], self.source_calls)
                    self.assertEqual(1, len(calls))
                    self.assertEqual([[]], variants)
                else:
                    self.assertEqual([], self.independent_calls)
                    self.assertEqual(2, len(calls))
                    self.assertEqual({"input_sources"}, {c["kind"] for c in calls[1]})
                    self.assertEqual(1, len(variants[0]))
                    selected, checks = variants[0][0]
                    self.assertEqual([previous["id"]], [group["turn_id"] for group in selected])
                    self.assertTrue(all(group["input_only"] for group in selected))
                    self.assertTrue(all("input_sources" in check for check in checks))

    async def test_required_raw_origin_cannot_be_discarded_by_optional_memory_check(self):
        previous, current = await self.turns()
        group = self.groups(current)[previous["id"]]
        calls = []

        async def prepare(origin, scope, bundle, checks, required):
            self.assertEqual(["check:" + digest(self.raw(previous))], required)
            calls.append(copy.deepcopy(checks))
            return {check["check_id"]: "unavailable" for check in checks}

        self.h.memory.prepare_context = prepare
        with self.assertRaises(Fault) as caught:
            await _v2_variants(self.h.core, current, [self.raw(previous)], [[group]])
        self.assertEqual("dependency_unavailable", caught.exception.code)
        self.assertTrue(caught.exception.retryable_read)
        self.assertFalse(caught.exception.unknown)
        self.assertEqual("memory", caught.exception.cause["peer"])
        self.assertEqual(200, caught.exception.cause["http_status"])
        self.assertEqual([], self.independent_calls)
        self.assertEqual(1, len(calls))
        self.assertEqual({"history", "input_sources"}, {check["kind"] for check in calls[0]})

    async def test_raw_batch_await_closes_captured_revision_sources_identity_and_deadline(self):
        previous, current = await self.turns()
        check = self.raw(previous)
        self.adopt(current, [check])
        saved = copy.deepcopy(previous)
        for field in (
            "input_revision",
            "input_sources",
            "binding",
            "channel",
            "person",
            "deadline",
        ):
            with self.subTest(field=field):
                self.verifications.clear()

                async def changed(*args):
                    fresh = copy.deepcopy(saved)
                    if field == "input_revision":
                        fresh["bundle"]["collection_revision"] += 1
                    elif field == "input_sources":
                        # Every physical source is still current; the captured list is not.
                        fresh["bundle"]["messages"].append(
                            copy.deepcopy(fresh["bundle"]["messages"][0])
                        )
                    elif field == "binding":
                        fresh["binding_version"] += 1
                    elif field == "channel":
                        fresh["bundle"]["collection_key"]["channel"]["binding_id"] = "binding:other"
                    elif field == "deadline":
                        fresh["source_deadline"] -= 1
                    else:
                        fresh["scope"]["person_id"] = "person:other"
                    self.h.core.store.put("turns", fresh)
                    return "valid"

                self.raw_result = changed
                try:
                    with self.assertRaises(Fault):
                        await self.h.core._preflight_once(current)
                    self.assertEqual(1, len(self.verifications))
                finally:
                    self.h.core.store.put("turns", saved)

    async def test_core_withdrawal_after_complete_raw_batch_rejects(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        original = self.h.memory.verify_context

        async def barrier(*args):
            self.assertEqual([], self.source_calls)
            result = await original(*args)
            self.assertEqual(1, len(self.source_calls))
            self.withdraw(previous)
            return result

        self.h.memory.verify_context = barrier
        with self.assertRaises(Fault):
            await self.h.core._preflight_once(current)
        self.assertEqual(
            [["input_sources"]], [[c["kind"] for c in batch] for batch in self.verifications]
        )

    async def test_preparation_closing_rejects_withdrawal_after_raw_batch(self):
        previous, current = await self.turns()
        group = input_only(self.groups(current)[previous["id"]])
        calls = []

        async def prepare(origin, scope, bundle, checks, required):
            calls.append(checks)
            self.assertEqual({"input_sources"}, {c["kind"] for c in checks})
            self.assertEqual([], self.independent_calls)
            self.withdraw(previous)
            return {check["check_id"]: "valid" for check in checks}

        self.h.memory.prepare_context = prepare
        with self.assertRaises(Fault):
            await _v2_variants(self.h.core, current, [], [[group]])
        self.assertEqual([["input_sources"]], [[c["kind"] for c in batch] for batch in calls])

    async def test_duplicate_raw_proofs_share_only_the_same_fullkey_checkpoint(self):
        previous, current = await self.turns()
        check = self.raw(previous)
        self.adopt(current, [check, copy.deepcopy(check)])
        await self.h.core._preflight_once(current)
        self.assertEqual(1, len(self.source_calls))
        await self.h.core._preflight_once(current)
        self.assertEqual(2, len(self.source_calls))

    async def test_raw_batch_near_margin_uses_renewed_authority_and_captured_key(self):
        from test_context_lease import lease_sender

        previous, current = await self.turns()
        self.lease(previous, 35)
        old = previous["source_deadline"]
        self.adopt(current, [self.raw(previous)])
        lease_sender(self.h)
        proofs = []
        original = self.h.core._authorize_context

        async def authorize(check, checkpoint, captured):
            if check.get("source_turn_id") == previous["id"]:
                proofs.append(captured)
            return await original(check, checkpoint, captured)

        self.h.core._authorize_context = authorize
        await self.h.core._preflight_once(current)
        captured = proofs[-1]
        self.assertGreater(captured, old)
        self.assertEqual(
            captured, self.h.core.store.get("turns", previous["id"])["source_deadline"]
        )

    async def test_extension_keeps_original_cap_and_next_checkpoint_uses_new_fullkey(self):
        previous, current = await self.turns()
        self.lease(previous, 60)
        old = previous["source_deadline"]
        self.adopt(current, [self.raw(previous)])
        proofs = []
        original = self.h.core._authorize_context

        async def authorize(check, checkpoint, captured):
            if check.get("source_turn_id") == previous["id"]:
                proofs.append(captured)
            return await original(check, checkpoint, captured)

        self.h.core._authorize_context = authorize

        async def extended(request):
            self.lease(previous, 180)
            return "valid"

        self.raw_result = extended
        await self.h.core._preflight_once(current)
        new = previous["source_deadline"]
        self.assertGreater(new, old)
        await self.h.core._preflight_once(current)
        self.assertEqual([old, new], proofs)
        self.assertEqual(2, len(self.source_calls))

    async def test_original_raw_cap_expiry_cannot_borrow_later_durable_extension(self):
        previous, current = await self.turns()
        self.lease(previous, 60)
        self.lease(current, 300)
        old = previous["source_deadline"]
        self.adopt(current, [self.raw(previous)])
        original = self.h.memory.verify_context

        async def barrier(*args):
            result = await original(*args)
            self.lease(previous, 180)
            self.h.clock.advance(61)
            self.assertLessEqual(old, self.h.clock())
            self.assertLess(self.h.clock(), previous["source_deadline"])
            return result

        self.h.memory.verify_context = barrier
        with self.assertRaises(Fault) as caught:
            await self.h.core._preflight_once(current)
        self.assertTrue(caught.exception.retryable_read)
        self.assertEqual(1, len(self.source_calls))
        self.assertEqual(
            [["input_sources"]], [[c["kind"] for c in batch] for batch in self.verifications]
        )

    async def test_cancellation_during_complete_raw_batch_drains_the_actual_read(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        entered, stopped = asyncio.Event(), asyncio.Event()

        async def blocked(*args):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        self.raw_result = blocked
        job = asyncio.create_task(self.h.core._preflight_once(current))
        await asyncio.wait_for(entered.wait(), 1)
        job.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await job
        self.assertTrue(stopped.is_set())
        self.assertEqual(
            [["input_sources"]], [[c["kind"] for c in batch] for batch in self.verifications]
        )

    async def test_four_authors_raw_and_required_legacy_share_complete_batch(self):
        *previous, current = await self.turns(5)
        groups = self.groups(current)
        legacy = scope_check(previous[0])
        batches = [[input_only(groups[t["id"]])] for t in previous]
        selected = await _v2_variants(self.h.core, current, [legacy], batches)
        self.assertTrue(all(len(variants) == 1 for variants in selected))
        self.assertEqual(1, len(self.preparations))
        checks = self.preparations[0]
        self.assertEqual(5, len(checks))
        self.assertEqual(4, sum(c["kind"] == "input_sources" for c in checks))
        self.assertEqual(1, sum(c["kind"] == "history" for c in checks))
        self.assertEqual(4, len({r["scope"]["person_id"] for r in self.source_calls}))
        self.assertEqual({t["id"] for t in previous}, {r["turn_id"] for r in self.source_calls})
        self.adopt(current, [legacy, *(self.raw(t) for t in previous)])
        self.verifications.clear()
        await self.h.core._preflight_once(current)
        await self.h.core._preflight_once(current)
        self.assertEqual([5, 5], [len(batch) for batch in self.verifications])
        self.assertEqual(12, len(self.source_calls))  # Four sources at three fresh boundaries.
        self.assertEqual([], self.independent_calls)

    async def test_standalone_raw_check_still_uses_real_source_sync(self):
        from tianshu_companion.context_checks import VerificationCheckpoint

        previous, current = await self.turns()
        check = self.raw(previous)
        async with VerificationCheckpoint() as checkpoint:
            await self.h.core._verify_context_check(current, check, checkpoint)
        self.assertEqual([(previous["id"], check["input_sources"])], self.independent_calls)
        self.assertEqual([], self.source_calls)
        self.assertEqual([], self.verifications)
