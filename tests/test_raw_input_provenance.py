"""Raw human inputs use exact source proofs, not their old query's history domain."""

import asyncio
import copy
import unittest

from test_memory_v2 import CoreEvidenceTests
from tianshu_companion.clients import epoch, utc
from tianshu_companion.context import recent_checks, scope_check
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
        self.source_calls = []
        original = self.h.memory.check_sources

        async def sources(turn, selected):
            self.source_calls.append((copy.deepcopy(turn), copy.deepcopy(selected)))
            return await original(turn, selected)

        self.h.memory.check_sources = sources
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
            return {check["check_id"]: "rejected" for check in checks}

        self.h.memory.prepare_context = prepare
        batch = [groups[source["id"]] for source in previous]
        variants = await _v2_variants(self.h.core, current, [], [batch])
        self.assertEqual(1, len(variants[0]))
        adopted, checks = variants[0][0]
        self.assertEqual([source["id"] for source in previous], [g["turn_id"] for g in adopted])
        self.assertTrue(all(g["input_only"] and not g["replies"] for g in adopted))
        self.assertEqual(3, len(self.source_calls))
        self.assertTrue(all("input_sources" in check for check in checks))
        self.assertEqual(1, len(calls))
        self.assertTrue(calls[0])
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
        self.assertIsNone(legacy_check(raw, query(raw["origin"])))

    async def test_raw_success_and_current_memory_bundle_are_both_required(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        self.invalid = True
        with self.assertRaises(Fault):
            await self.h.core._preflight_once(current)
        self.assertEqual(1, len(self.source_calls))
        self.assertEqual([[]], self.verifications)

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

    async def test_empty_legacy_catalog_still_reads_raw_sources_at_each_fresh_checkpoint(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        await self.h.core._preflight_once(current)
        await self.h.core._preflight_once(current)
        self.assertEqual(2, len(self.source_calls))
        self.assertEqual([[], []], self.verifications)
        for captured, sources in self.source_calls:
            self.assertEqual(previous["id"], captured["id"])
            self.assertEqual(
                previous["bundle"]["collection_revision"], captured["bundle"]["collection_revision"]
            )
            self.assertEqual([m["source"] for m in previous["bundle"]["messages"]], sources)

    async def test_current_source_version_is_not_compared_to_old_history_version(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        original = self.h.memory.check_sources

        async def newer(*args):
            await original(*args)
            return previous["scope_version"] + 100

        self.h.memory.check_sources = newer
        await self.h.core._preflight_once(current)
        self.assertEqual(1, len(self.source_calls))
        self.assertEqual([[]], self.verifications)

    async def test_memory_negative_or_unclassified_failure_never_becomes_optional_valid(self):
        previous, current = await self.turns()
        group = input_only(self.groups(current)[previous["id"]])
        calls = []

        async def prepare(*args):
            calls.append(args)
            return {}

        self.h.memory.prepare_context = prepare
        for code, unknown in (
            ("version_conflict", False),
            ("dependency_unavailable", False),
            ("forbidden", False),
            ("result_unknown", True),
        ):
            with self.subTest(code=code):

                async def failed(*args):
                    raise Fault(code, unknown=unknown)

                self.h.memory.check_sources = failed
                with self.assertRaises(Fault) as caught:
                    await _v2_variants(self.h.core, current, [], [[group]])
                self.assertEqual(code, caught.exception.code)
                self.assertEqual(unknown, caught.exception.unknown)
                self.assertEqual([], calls)

    async def test_one_invalid_linked_member_cannot_leave_an_isolated_valid_anchor(self):
        first, second, current = await self.turns(3)
        groups = self.groups(current)
        batch = [input_only(groups[source["id"]]) for source in (first, second)]
        self.withdraw(second)
        result = await _v2_variants(self.h.core, current, [], [batch])
        self.assertEqual([[]], result)
        self.assertEqual([first["id"]], [turn["id"] for turn, _ in self.source_calls])

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
                    return {check["check_id"]: status for check in checks}

                self.h.memory.prepare_context = prepare
                variants = await _v2_variants(self.h.core, current, [], [[group]])
                self.assertEqual(1, len(self.source_calls))
                self.assertEqual(1, len(calls))
                self.assertEqual({"history"}, {check["kind"] for check in calls[0]})
                self.assertGreater(previous["source_deadline"], self.h.clock() + 35)
                if status == "unavailable":
                    self.assertEqual([[]], variants)
                else:
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
            self.assertEqual([], required)
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
        self.assertEqual(1, len(self.source_calls))
        self.assertEqual(1, len(calls))
        self.assertEqual({"history"}, {check["kind"] for check in calls[0]})

    async def test_raw_rpc_await_closes_captured_revision_sources_and_identity(self):
        previous, current = await self.turns()
        check = self.raw(previous)
        self.adopt(current, [check])
        saved = copy.deepcopy(previous)
        for field in ("input_revision", "input_sources", "binding", "channel", "person"):
            with self.subTest(field=field):

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
                    else:
                        fresh["scope"]["person_id"] = "person:other"
                    self.h.core.store.put("turns", fresh)
                    return 1

                self.h.memory.check_sources = changed
                try:
                    with self.assertRaises(Fault):
                        await self.h.core._preflight_once(current)
                    self.assertEqual([], self.verifications)
                finally:
                    self.h.core.store.put("turns", saved)

    async def test_core_withdrawal_between_raw_rpc_and_memory_closing_rejects(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        original = self.h.memory.verify_context

        async def barrier(*args):
            self.assertEqual(1, len(self.source_calls))
            result = await original(*args)
            self.withdraw(previous)
            return result

        self.h.memory.verify_context = barrier
        with self.assertRaises(Fault):
            await self.h.core._preflight_once(current)
        self.assertEqual([[]], self.verifications)

    async def test_preparation_closing_rejects_withdrawal_after_raw_rpc(self):
        previous, current = await self.turns()
        group = input_only(self.groups(current)[previous["id"]])
        calls = []

        async def prepare(origin, scope, bundle, checks, required):
            calls.append(checks)
            self.assertEqual(1, len(self.source_calls))
            self.withdraw(previous)
            return {check["check_id"]: "valid" for check in checks}

        self.h.memory.prepare_context = prepare
        with self.assertRaises(Fault):
            await _v2_variants(self.h.core, current, [], [[group]])
        self.assertEqual([[]], calls)

    async def test_duplicate_raw_proofs_share_only_the_same_fullkey_checkpoint(self):
        previous, current = await self.turns()
        check = self.raw(previous)
        self.adopt(current, [check, copy.deepcopy(check)])
        await self.h.core._preflight_once(current)
        self.assertEqual(1, len(self.source_calls))
        await self.h.core._preflight_once(current)
        self.assertEqual(2, len(self.source_calls))

    async def test_raw_rpc_near_margin_uses_renewed_authority_and_captured_key(self):
        from test_context_lease import lease_sender

        previous, current = await self.turns()
        self.lease(previous, 35)
        old = previous["source_deadline"]
        self.adopt(current, [self.raw(previous)])
        lease_sender(self.h)
        await self.h.core._preflight_once(current)
        captured = self.source_calls[0][0]["source_deadline"]
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
        sources = self.h.memory.check_sources

        async def extended(*args):
            result = await sources(*args)
            self.lease(previous, 180)
            return result

        self.h.memory.check_sources = extended
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
        self.assertEqual([[]], self.verifications)

    async def test_cancellation_during_raw_rpc_drains_the_actual_read(self):
        previous, current = await self.turns()
        self.adopt(current, [self.raw(previous)])
        entered, stopped = asyncio.Event(), asyncio.Event()

        async def blocked(*args):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        self.h.memory.check_sources = blocked
        job = asyncio.create_task(self.h.core._preflight_once(current))
        await asyncio.wait_for(entered.wait(), 1)
        job.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await job
        self.assertTrue(stopped.is_set())
        self.assertEqual([], self.verifications)
