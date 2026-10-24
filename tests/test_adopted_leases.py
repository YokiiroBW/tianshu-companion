"""Adopted v2 proofs retain their captured bound across same-source extensions."""

import asyncio
import copy
import unittest

from test_delivery_v2 import NativeQueue
from test_memory_v2 import CoreEvidenceTests
from tianshu_companion.clients import epoch, utc
from tianshu_companion.context import scope_check
from tianshu_companion.contracts import Fault


class AdoptedLeaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = CoreEvidenceTests.asyncSetUp

    async def prepare(self):
        for index, text in enumerate(("先记住这段合成计划。", "随后再问这段计划。")):
            await self.h.ingest(
                text=text, account=f"lease-owner-{index}", group=True, channel="group:lease"
            )
            await self.h.cycles()
        previous, current = self.h.turns()
        self.assertEqual(["sent", "sent"], [previous["phase"], current["phase"]])
        self.assertNotEqual(previous["scope"]["person_id"], current["scope"]["person_id"])
        current.update(
            context_checks=[scope_check(previous), scope_check(previous, input_only=True)],
            profile_checks=[],
            model_calls=0,
        )
        self.h.core._save_turn(current)
        self.verifications.clear()
        self.renewals = []
        self.authorizations = []
        original = self.h.origins.resolve

        async def resolve(service, envelope, now):
            source = previous if envelope["origin"] == previous["origin"] else current
            durable = self.h.core.store.get("turns", source["id"])
            self.authorizations.append((source["id"], durable["source_deadline"]))
            return await original(service, envelope, now)

        async def context(source, scope, channel):
            origin = source["origin"]
            self.renewals.append(copy.deepcopy(origin))
            expiry = utc(self.h.clock() + 60)
            self.h.origins.values[origin["assertion_ref"]]["expires_at"] = expiry
            return dict(origin=origin, scope=scope, channel=channel, expires_at=expiry)

        queue = NativeQueue(self.h.clock)
        queue.expression_context = context
        self.h.core.sender = queue
        self.h.origins.resolve = resolve
        return previous, current

    def lease(self, turn, seconds):
        expiry = utc(self.h.clock() + seconds)
        self.h.origins.values[turn["origin"]["assertion_ref"]]["expires_at"] = expiry
        fresh = self.h.core.store.get("turns", turn["id"])
        fresh["source_deadline"] = epoch(expiry)
        self.h.core.store.put("turns", fresh)
        turn["source_deadline"] = fresh["source_deadline"]

    def before_authority(self, action):
        async def book(turn):
            action()

        self.h.core.book_reading.verify = book

    async def test_expired_early_source_is_renewed_and_reauthorized_before_complete_barrier(self):
        previous, current = await self.prepare()
        self.lease(previous, 25)
        self.before_authority(lambda: self.h.clock.advance(30))
        old = previous["source_deadline"]
        original = self.h.memory.verify_context

        async def barrier(origin, scope, bundle, checks):
            self.assertEqual({"history", "input_history"}, {c["kind"] for c in checks})
            self.assertEqual(2, len(checks))
            durable = self.h.core.store.get("turns", previous["id"])
            self.assertGreater(durable["source_deadline"], self.h.clock())
            self.assertIn((previous["id"], durable["source_deadline"]), self.authorizations)
            return await original(origin, scope, bundle, checks)

        self.h.memory.verify_context = barrier
        await self.h.core._preflight_once(current)
        self.assertEqual([current["origin"], current["origin"], previous["origin"]], self.renewals)
        self.assertEqual(1, len(self.verifications))
        source_proofs = [deadline for key, deadline in self.authorizations if key == previous["id"]]
        self.assertEqual(1, len(source_proofs))
        self.assertGreater(source_proofs[0], old)

    async def test_main_renewed_elsewhere_gets_new_authority_even_when_refresh_needs_no_renewal(
        self,
    ):
        previous, current = await self.prepare()
        self.lease(current, 60)
        self.before_authority(lambda: self.lease(current, 180))
        await self.h.core._preflight_once(current)
        proofs = [deadline for key, deadline in self.authorizations if key == current["id"]]
        self.assertEqual([current["source_deadline"]], proofs)
        self.assertEqual([current["origin"]], self.renewals)
        self.assertEqual(1, len(self.verifications))

    async def renewal_failure(self, failure):
        previous, current = await self.prepare()
        self.lease(previous, 25)
        self.before_authority(lambda: self.h.clock.advance(30))
        context = self.h.core.sender.expression_context

        async def renew(source, scope, channel):
            if source["origin"] == previous["origin"]:
                if failure == "cancelled":
                    raise asyncio.CancelledError()
                if failure == "expired":
                    return dict(
                        origin=source["origin"],
                        scope=scope,
                        channel=channel,
                        expires_at=utc(self.h.clock() - 1),
                    )
                error = Fault(
                    "result_unknown" if failure == "unknown" else "forbidden",
                    unknown=failure == "unknown",
                )
                error.origin_read_unavailable = True
                raise error
            return await context(source, scope, channel)

        self.h.core.sender.expression_context = renew
        with self.assertRaises(
            asyncio.CancelledError if failure == "cancelled" else Fault
        ) as caught:
            await self.h.core._preflight_once(current)
        if failure != "cancelled":
            self.assertTrue(caught.exception.context_renewal_failure)
            self.assertEqual(failure == "unknown", caught.exception.unknown)
        self.assertEqual([], self.verifications)

    async def test_source_renewal_lost_ack_is_global_unknown(self):
        await self.renewal_failure("unknown")

    async def test_source_renewal_denial_cannot_become_optional_unavailability(self):
        await self.renewal_failure("forbidden")

    async def test_expired_source_renewal_cannot_enter_memory_verify(self):
        await self.renewal_failure("expired")

    async def test_cancellation_of_source_renewal_drains_checkpoint(self):
        await self.renewal_failure("cancelled")

    async def test_verify_return_with_less_than_twenty_seconds_remains_valid(self):
        previous, current = await self.prepare()
        self.lease(previous, 60)
        original = self.h.memory.verify_context

        async def barrier(*args):
            self.h.clock.advance(45)
            return await original(*args)

        self.h.memory.verify_context = barrier
        await self.h.core._preflight_once(current)
        self.assertEqual(1, len(self.verifications))
        self.assertEqual([current["origin"]], self.renewals)

    async def test_pure_extension_while_verifying_keeps_the_original_checkpoint_deadline(self):
        previous, current = await self.prepare()
        self.lease(previous, 60)
        original = self.h.memory.verify_context

        async def barrier(*args):
            self.lease(previous, 180)
            return await original(*args)

        self.h.memory.verify_context = barrier
        old = previous["source_deadline"]
        await self.h.core._preflight_once(current)
        self.assertEqual([old], [d for key, d in self.authorizations if key == previous["id"]])
        self.assertGreater(previous["source_deadline"], old)
        self.assertEqual(1, len(self.verifications))

    async def test_lease_expired_while_verifying_rejects_old_proof(self):
        previous, current = await self.prepare()
        self.lease(previous, 60)
        original = self.h.memory.verify_context

        async def barrier(*args):
            self.h.clock.advance(61)
            return await original(*args)

        self.h.memory.verify_context = barrier
        with self.assertRaises(Fault) as caught:
            await self.h.core._preflight_once(current)
        self.assertTrue(caught.exception.retryable_read)
        self.assertEqual(1, len(self.verifications))

    async def test_extension_during_authorization_does_not_relabel_its_original_proof(self):
        previous, current = await self.prepare()
        self.lease(previous, 60)
        old = previous["source_deadline"]
        original = self.h.origins.resolve
        changed = False

        async def resolve(service, envelope, now):
            nonlocal changed
            result = await original(service, envelope, now)
            if not changed and envelope["origin"] == previous["origin"]:
                changed = True
                self.lease(previous, 180)
            return result

        self.h.origins.resolve = resolve
        await self.h.core._prepare_context(current, [])
        self.assertTrue(changed)
        self.assertEqual(
            [old, previous["source_deadline"]],
            [deadline for key, deadline in self.authorizations if key == previous["id"]],
        )

    async def test_next_checkpoint_reauthorizes_the_extended_source_with_a_new_key(self):
        previous, current = await self.prepare()
        self.lease(previous, 60)
        old = previous["source_deadline"]
        original = self.h.memory.verify_context

        async def extended(*args):
            result = await original(*args)
            self.lease(previous, 180)
            return result

        self.h.memory.verify_context = extended
        await self.h.core._preflight_once(current)
        self.h.memory.verify_context = original
        await self.h.core._preflight_once(current)
        self.assertEqual(
            [old, previous["source_deadline"]],
            [d for key, d in self.authorizations if key == previous["id"]],
        )
        self.assertGreater(previous["source_deadline"], old)
        self.assertEqual(2, len(self.verifications))

    async def test_main_extension_during_authorization_keeps_its_captured_key(self):
        _, current = await self.prepare()
        self.lease(current, 60)
        old = current["source_deadline"]
        original = self.h.origins.resolve

        async def extended(service, envelope, now):
            result = await original(service, envelope, now)
            if envelope["origin"] == current["origin"]:
                self.lease(current, 180)
            return result

        self.h.origins.resolve = extended
        await self.h.core._preflight_once(current)
        self.assertEqual([old], [d for key, d in self.authorizations if key == current["id"]])
        self.assertGreater(current["source_deadline"], old)
        self.assertEqual(1, len(self.verifications))

    async def test_main_and_source_extension_at_the_post_memory_guard_preserve_old_proofs(self):
        previous, current = await self.prepare()
        original = self.h.memory.verify_context
        source = self.h.core._context_source
        for target in (current, previous):
            with self.subTest(target=target["id"]):
                self.lease(previous, 60)
                self.lease(current, 60)
                old = target["source_deadline"]
                returned = False

                async def barrier(*args):
                    nonlocal returned
                    result = await original(*args)
                    returned = True
                    return result

                def extended(check):
                    nonlocal returned
                    if returned and check["source_turn_id"] == target["id"]:
                        returned = False
                        self.lease(target, 180)
                    return source(check)

                self.h.memory.verify_context = barrier
                self.h.core._context_source = extended
                try:
                    await self.h.core._preflight_once(current)
                    self.assertGreater(target["source_deadline"], old)
                finally:
                    self.h.core._context_source = source

    async def test_main_extension_during_memory_cannot_outlive_its_captured_deadline(self):
        _, current = await self.prepare()
        self.lease(current, 60)
        current.update(context_checks=[], profile_checks=[])
        old = current["source_deadline"]
        original = self.h.memory.verify_context

        async def extended(*args):
            result = await original(*args)
            self.lease(current, 180)
            self.h.clock.advance(61)
            return result

        self.h.memory.verify_context = extended
        with self.assertRaises(Fault) as caught:
            await self.h.core._preflight_once(current)
        self.assertTrue(caught.exception.retryable_read)
        self.assertGreater(current["source_deadline"], old)
        self.assertLessEqual(old, self.h.clock())
        self.assertGreater(current["source_deadline"], self.h.clock())
        self.assertEqual([[]], self.verifications)

    def long_main_authority(self, current):
        self.lease(current, 300)
        original = self.h.core.sender.expression_context

        async def context(source, scope, channel):
            if source["origin"] == current["origin"]:
                return dict(
                    origin=source["origin"],
                    scope=scope,
                    channel=channel,
                    expires_at=utc(current["source_deadline"]),
                )
            return await original(source, scope, channel)

        self.h.core.sender.expression_context = context

    async def test_historical_extension_during_memory_cannot_outlive_its_captured_deadline(self):
        previous, current = await self.prepare()
        self.lease(previous, 60)
        old = previous["source_deadline"]
        self.long_main_authority(current)
        original = self.h.memory.verify_context

        async def extended(*args):
            result = await original(*args)
            self.lease(previous, 180)
            self.h.clock.advance(61)
            return result

        self.h.memory.verify_context = extended
        with self.assertRaises(Fault) as caught:
            await self.h.core._preflight_once(current)
        self.assertTrue(caught.exception.retryable_read)
        self.assertGreater(previous["source_deadline"], old)
        self.assertLessEqual(old, self.h.clock())
        self.assertGreater(previous["source_deadline"], self.h.clock())
        self.assertEqual(1, len(self.verifications))

    async def test_prepare_extension_during_authority_cannot_replace_the_earliest_cap(self):
        previous, current = await self.prepare()
        self.lease(previous, 60)
        self.long_main_authority(current)
        original = self.h.origins.resolve
        prepare = self.h.memory.prepare_context
        old = previous["source_deadline"]

        async def extended(service, envelope, now):
            result = await original(service, envelope, now)
            if envelope["origin"] == previous["origin"]:
                self.lease(previous, 180)
            return result

        async def expired(*args):
            result = await prepare(*args)
            self.h.clock.advance(61)
            return result

        self.h.origins.resolve = extended
        self.h.memory.prepare_context = expired
        with self.assertRaises(Fault) as caught:
            await self.h.core._prepare_context(current, [])
        self.assertEqual("forbidden", caught.exception.code)
        self.assertIn((previous["id"], old), self.authorizations)
        self.assertGreater(previous["source_deadline"], old)
        self.assertLessEqual(old, self.h.clock())
        self.assertGreater(previous["source_deadline"], self.h.clock())

    async def test_shorter_main_and_source_deadlines_reject_after_complete_memory_verify(self):
        previous, current = await self.prepare()
        original = self.h.memory.verify_context
        for target in (current, previous):
            with self.subTest(target=target["id"]):
                self.lease(previous, 60)
                self.lease(current, 60)
                before = len(self.verifications)

                async def shortened(*args):
                    result = await original(*args)
                    self.lease(target, 25)
                    return result

                self.h.memory.verify_context = shortened
                with self.assertRaises(Fault) as caught:
                    await self.h.core._preflight_once(current)
                self.assertTrue(caught.exception.retryable_read)
                self.assertEqual(before + 1, len(self.verifications))

    async def test_same_deadline_full_source_identity_and_withdrawal_are_still_checked(self):
        from tianshu_companion.contracts import digest

        previous, current = await self.prepare()
        original = self.h.memory.verify_context
        for target in (previous, current):
            for change in (
                "origin",
                "service",
                "person",
                "actor",
                "audience",
                "conversation",
                "binding",
                "channel",
                "withdrawal",
                "revision",
                "receipt",
            ):
                with self.subTest(target=target["id"], change=change):
                    saved = self.h.core.store.get("turns", target["id"])
                    physical_key = digest(saved["bundle"]["messages"][0]["message_key"])
                    physical = self.h.core.store.get("physicals", physical_key)
                    before = len(self.verifications)

                    async def changed(*args):
                        result = await original(*args)
                        fresh = copy.deepcopy(saved)
                        if change == "origin":
                            fresh["origin"] = dict(assertion_ref="origin:different")
                        elif change == "service":
                            fresh["service"] = "different"
                        elif change == "person":
                            fresh["scope"]["person_id"] = "person:different"
                        elif change == "actor":
                            fresh["scope"]["actor_id"] = "actor:different"
                        elif change == "audience":
                            fresh["scope"]["audience"] = "self_private"
                        elif change == "conversation":
                            fresh["scope"]["conversation_id"] = "conversation:different"
                        elif change == "binding":
                            fresh["binding_version"] += 1
                        elif change == "channel":
                            fresh["bundle"]["collection_key"]["channel"]["binding_id"] = "different"
                        else:
                            withdrawn = copy.deepcopy(physical)
                            if change == "withdrawal":
                                withdrawn["fact"]["state"] = "retracted"
                            elif change == "revision":
                                withdrawn["revision"] += 1
                            else:
                                withdrawn["fact"]["physical_receipt_id"] = "receipt:different"
                            self.h.core.store.put("physicals", withdrawn)
                        self.assertEqual(saved["source_deadline"], fresh["source_deadline"])
                        self.h.core.store.put("turns", fresh)
                        return result

                    self.h.memory.verify_context = changed
                    try:
                        with self.assertRaises(Fault):
                            await self.h.core._preflight_once(current)
                        self.assertEqual(before + 1, len(self.verifications))
                    finally:
                        self.h.core.store.put("turns", saved)
                        self.h.core.store.put("physicals", physical)

    async def test_main_cancel_and_live_role_revocation_still_reject_after_memory(self):
        _, current = await self.prepare()
        original = self.h.memory.verify_context
        allowed = self.h.core._role_allows
        for change in ("cancel", "role", "capability"):
            with self.subTest(change=change):
                saved = self.h.core.store.get("turns", current["id"])
                actor = current["scope"]["actor_id"]
                role = self.h.core.roles[actor]

                async def revoked(*args):
                    result = await original(*args)
                    if change == "cancel":
                        fresh = copy.deepcopy(saved)
                        fresh["cancelled"] = True
                        self.h.core.store.put("turns", fresh)
                    elif change == "role":
                        self.h.core.roles.pop(actor)
                    else:
                        self.h.core._role_allows = lambda key, cap: (
                            cap != "dialogue" and allowed(key, cap)
                        )
                    return result

                self.h.memory.verify_context = revoked
                try:
                    with self.assertRaises(Fault):
                        await self.h.core._preflight_once(current)
                finally:
                    self.h.core.roles[actor] = role
                    self.h.core._role_allows = allowed
                    self.h.core.store.put("turns", saved)

    async def test_foreign_none_lease_cannot_borrow_main_and_still_requires_its_own_authority(self):
        from tianshu_companion.context_preparation import _lease, _proof_lease

        previous, current = await self.prepare()
        foreign = scope_check(previous)
        foreign.pop("source_turn_id")
        self.assertIsNone(_lease(self.h.core, current, foreign))
        self.assertIsNone(_proof_lease(self.h.core, current, foreign, None))
        with self.assertRaises(Fault):
            _proof_lease(self.h.core, current, foreign, current["source_deadline"])
        main = scope_check(current)
        main.pop("source_turn_id")
        self.assertEqual(current["source_deadline"], _lease(self.h.core, current, main))
        with self.assertRaises(Fault):
            _proof_lease(self.h.core, current, main, None)
        other_person = dict(main, scope=previous["scope"])
        with self.assertRaises(Fault):
            _lease(self.h.core, current, other_person)
        authorize = self.h.core._authorize_context
        keys = []

        async def recorded(check, point, deadline):
            keys.append((check["origin"], deadline))
            return await authorize(check, point, deadline)

        self.h.core._authorize_context = recorded
        await self.h.core._verify_checks(current, [foreign])
        self.assertIn((previous["origin"], None), keys)
        self.assertEqual(["history"], [c["kind"] for c in self.verifications[-1]])

    async def test_empty_catalog_still_authorizes_main_and_checks_its_controls_after_memory(self):
        _, current = await self.prepare()
        current.update(context_checks=[], profile_checks=[])
        await self.h.core._preflight_once(current)
        self.assertEqual([[]], self.verifications)
        self.assertEqual([current["id"]], [key for key, _ in self.authorizations])
        original = self.h.memory.verify_context

        async def changed(*args):
            result = await original(*args)
            fresh = self.h.core.store.get("turns", current["id"])
            fresh["binding_version"] += 1
            self.h.core.store.put("turns", fresh)
            return result

        self.h.memory.verify_context = changed
        with self.assertRaises(Fault):
            await self.h.core._preflight_once(current)
        self.assertEqual([[], []], self.verifications)

    async def test_final_authority_runs_once_and_relationship_check_is_not_a_memory_batch(self):
        from types import SimpleNamespace

        from tianshu_companion.relationships.contract import DOMAIN, SCHEMA_HASH

        previous, current = await self.prepare()
        local_done, relationship_calls = [], []
        pin = dict(
            scope_check(current),
            version_domain=DOMAIN,
            schema_hash=SCHEMA_HASH,
            relationship_view="public",
            relationship_version=1,
            checked_at=utc(self.h.clock()),
        )
        current["context_checks"].append(pin)
        self.before_authority(lambda: local_done.append(True))
        authorize = self.h.core._authorize_context

        async def authorized(*args):
            self.assertTrue(local_done)
            return await authorize(*args)

        async def relationship(origin, scope, version):
            self.assertTrue(local_done)
            relationship_calls.append((origin, scope, version))

        self.h.core._authorize_context = authorized
        self.h.core.relationships = SimpleNamespace(client=SimpleNamespace(check=relationship))
        await self.h.core._preflight_once(current)
        self.assertEqual([(current["origin"], current["scope"], 1)], relationship_calls)
        self.assertEqual(1, len([key for key, _ in self.authorizations if key == previous["id"]]))
        self.assertEqual(1, len([key for key, _ in self.authorizations if key == current["id"]]))
        self.assertEqual({"history", "input_history"}, {c["kind"] for c in self.verifications[-1]})

        async def unknown(*args):
            raise Fault("result_unknown", unknown=True)

        self.h.core.relationships.client.check = unknown
        before = len(self.verifications)
        with self.assertRaises(Fault) as caught:
            await self.h.core._preflight_once(current)
        self.assertTrue(caught.exception.unknown)
        self.assertEqual(before, len(self.verifications))

    async def catalog_coordination(self, *, concurrent=False, failure=None):
        from tianshu_companion.clients import JsonService
        from tianshu_companion.context_preparation import _v2_variants
        from tianshu_companion.contracts import digest
        from tianshu_companion.short_context import select_recent

        previous, current = await self.prepare()
        self.lease(previous, 40)
        self.lease(current, 120)
        old = previous["source_deadline"]
        selected, _ = select_recent(
            self.h.core.store, current, self.h.core.short_context_policy, self.h.clock()
        )
        required = [scope_check(previous)]
        ready, release = asyncio.Event(), asyncio.Event()
        verify = self.h.core._verify_context_check
        initial, active = 0, 0

        async def checked(turn, check, checkpoint, **kwargs):
            nonlocal initial, active
            active += 1
            try:
                lease = await verify(turn, check, checkpoint, **kwargs)
                if check.get("source_turn_id") == previous["id"] and initial < 2:
                    initial += 1
                    if initial == 2:
                        ready.set()
                        await release.wait()
                return lease
            finally:
                active -= 1

        self.h.core._verify_context_check = checked
        prepare = self.h.memory.prepare_context
        calls, raw_proofs = [], []
        source_check = self.h.memory.check_sources

        async def checked_sources(source, sources):
            raw_proofs.append((source["source_deadline"], copy.deepcopy(sources)))
            return await source_check(source, sources)

        self.h.memory.check_sources = checked_sources

        async def prepared(origin, scope, bundle, checks, required_ids):
            fresh = self.h.core.store.get("turns", previous["id"])
            self.assertGreater(fresh["source_deadline"], old)
            self.assertIn((previous["id"], fresh["source_deadline"]), self.authorizations)
            self.assertEqual({"history"}, {c["kind"] for c in checks})
            self.assertEqual(1, len(checks))
            self.assertEqual([old, fresh["source_deadline"]], [cap for cap, _ in raw_proofs])
            self.assertTrue(
                all(
                    sources == [message["source"] for message in previous["bundle"]["messages"]]
                    for _, sources in raw_proofs
                )
            )
            self.assertEqual(["check:" + digest(required[0])], required_ids)
            calls.append(copy.deepcopy(checks))
            return await prepare(origin, scope, bundle, checks, required_ids)

        self.h.memory.prepare_context = prepared
        task = asyncio.create_task(_v2_variants(self.h.core, current, required, [selected]))
        try:
            await asyncio.wait_for(ready.wait(), 1)
            self.assertFalse(calls)
            self.h.clock.advance(6)
            if concurrent:
                other = self.h.core.store.get("turns", previous["id"])
                await asyncio.create_task(
                    self.h.core.continue_delivery_origin(
                        other, read_budget=JsonService.QUERY_BUDGET_SECONDS
                    )
                )
                self.assertGreater(other["source_deadline"], old)
            if failure == "binding":
                identity = self.h.memory.identity

                async def changed_identity(origin, *args):
                    person, binding = await identity(origin, *args)
                    return person, binding + (origin == previous["origin"])

                self.h.memory.identity = changed_identity
            elif failure is not None:
                context = self.h.core.sender.expression_context

                async def failed_context(source, scope, channel):
                    if source["origin"] == previous["origin"]:
                        if failure == "cancelled":
                            raise asyncio.CancelledError()
                        error = Fault(
                            "result_unknown" if failure == "unknown" else "forbidden",
                            unknown=failure == "unknown",
                        )
                        error.origin_read_unavailable = True
                        raise error
                    return await context(source, scope, channel)

                self.h.core.sender.expression_context = failed_context
            release.set()
            if failure is not None:
                with self.assertRaises(
                    asyncio.CancelledError if failure == "cancelled" else Fault
                ) as caught:
                    await task
                if failure not in {"binding", "cancelled"}:
                    self.assertTrue(caught.exception.context_renewal_failure)
                    self.assertEqual(failure == "unknown", caught.exception.unknown)
                self.assertFalse(calls)
            else:
                variants = await task
                self.assertEqual(2, len(variants[0]))
                self.assertEqual(1, len(calls))
                fresh = self.h.core.store.get("turns", previous["id"])
                self.assertEqual(
                    [old, fresh["source_deadline"]],
                    [deadline for key, deadline in self.authorizations if key == previous["id"]],
                )
                self.assertEqual([previous["origin"]], self.renewals)
            self.assertEqual(0, active)
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_prepare_coordinates_source_renewed_by_another_turn_before_its_memory_read(self):
        await self.catalog_coordination(concurrent=True)

    async def test_prepare_coordinates_sources_that_cross_the_existing_margin_during_attestation(
        self,
    ):
        await self.catalog_coordination()

    async def test_prepare_coordination_renewal_unknown_cannot_become_optional(self):
        await self.catalog_coordination(failure="unknown")

    async def test_prepare_coordination_renewal_denial_cannot_become_optional(self):
        await self.catalog_coordination(failure="forbidden")

    async def test_prepare_coordination_cancellation_drains_authority_tasks(self):
        await self.catalog_coordination(failure="cancelled")

    async def test_prepare_coordination_rechecks_required_binding(self):
        await self.catalog_coordination(failure="binding")

    async def test_prepare_shortening_after_one_coordination_still_rejects(self):
        from collections import Counter

        from tianshu_companion.contracts import digest

        previous, current = await self.prepare()
        self.lease(previous, 40)
        self.lease(current, 120)
        verify = self.h.core._verify_context_check
        ready, release = asyncio.Event(), asyncio.Event()
        calls, attempts = [], []

        async def checked(turn, check, checkpoint, **kwargs):
            attempts.append(digest(check))
            result = await verify(turn, check, checkpoint, **kwargs)
            if len(attempts) == 2:
                ready.set()
                await release.wait()
            return result

        async def prepared(origin, scope, bundle, checks, required):
            calls.append(checks)
            self.lease(previous, 20)
            return {check["check_id"]: "valid" for check in checks}

        self.h.core._verify_context_check = checked
        self.h.memory.prepare_context = prepared
        task = asyncio.create_task(self.h.core._prepare_context(current, []))
        try:
            await asyncio.wait_for(ready.wait(), 1)
            self.h.clock.advance(6)
            release.set()
            with self.assertRaises(Fault) as caught:
                await task
            self.assertTrue(caught.exception.retryable_read)
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual([2, 2], sorted(Counter(attempts).values()))
        self.assertEqual(2, len(self.authorizations))
        self.assertEqual(1, len(calls))

    async def concurrent_continuation(self, operation, *, near=False):
        previous, current = await self.prepare()
        self.lease(previous, 36 if near else 60)
        deadline = previous["source_deadline"]
        entered, release = asyncio.Event(), asyncio.Event()
        name = "prepare_context" if operation == "prepare" else "verify_context"
        original = getattr(self.h.memory, name)

        async def blocked(*args):
            result = await original(*args)
            entered.set()
            await release.wait()
            return result

        setattr(self.h.memory, name, blocked)
        read = (
            self.h.core._prepare_context(current, [])
            if operation == "prepare"
            else self.h.core._preflight_once(current)
        )
        task = asyncio.create_task(read)
        try:
            await asyncio.wait_for(entered.wait(), 1)
            self.h.clock.advance(17 if near else 1)
            # A different live turn/checkpoint continues this human's source
            # while the successful Memory read is still in flight.
            await asyncio.create_task(self.h.core.continue_delivery_origin(previous))
            release.set()
            await task
            if near:
                self.assertGreater(previous["source_deadline"], deadline)
            else:
                self.assertEqual(deadline, previous["source_deadline"])
            self.assertIn(previous["origin"], self.renewals)
            self.assertEqual(
                previous["source_deadline"],
                self.h.core.store.get("turns", previous["id"])["source_deadline"],
            )
        finally:
            release.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_healthy_other_checkpoint_does_not_invalidate_successful_prepare(self):
        await self.concurrent_continuation("prepare")

    async def test_healthy_other_checkpoint_does_not_invalidate_successful_verify(self):
        await self.concurrent_continuation("verify")

    async def test_real_extension_preserves_prepare_inside_its_original_deadline(self):
        await self.concurrent_continuation("prepare", near=True)

    async def test_real_extension_preserves_verify_inside_its_original_deadline(self):
        await self.concurrent_continuation("verify", near=True)

    async def test_preflight_reserves_existing_read_budget_before_local_checks(self):
        previous, current = await self.prepare()
        self.lease(previous, 120)
        self.lease(current, 32)
        old = current["source_deadline"]
        self.before_authority(lambda: self.h.clock.advance(14))
        await self.h.core._preflight_once(current)
        self.assertGreater(current["source_deadline"], old)
        self.assertEqual([current["origin"]], self.renewals)
        self.assertEqual(1, len(self.verifications))
        self.assertEqual(
            [current["source_deadline"]],
            [deadline for key, deadline in self.authorizations if key == current["id"]],
        )

    async def test_final_source_coordination_reserves_budget_and_reproves_exact_new_lease(self):
        previous, current = await self.prepare()
        self.lease(previous, 40)
        old = previous["source_deadline"]
        self.before_authority(lambda: self.h.clock.advance(10))
        await self.h.core._preflight_once(current)
        fresh = self.h.core.store.get("turns", previous["id"])
        self.assertGreater(fresh["source_deadline"], old)
        self.assertEqual(
            [fresh["source_deadline"]],
            [deadline for key, deadline in self.authorizations if key == previous["id"]],
        )
        self.assertEqual([current["origin"], previous["origin"]], self.renewals)
        self.assertEqual(1, len(self.verifications))

    async def test_final_main_refresh_joins_its_exact_checkpoint_authorization(self):
        previous, current = await self.prepare()
        self.lease(previous, 120)
        self.before_authority(lambda: self.h.clock.advance(30))
        await self.h.core._preflight_once(current)
        proofs = [deadline for key, deadline in self.authorizations if key == current["id"]]
        self.assertEqual([current["source_deadline"]], proofs)
        self.assertEqual([current["origin"], current["origin"]], self.renewals)
        self.assertEqual(1, len(self.verifications))

    async def test_prepare_catalog_renews_before_proof_and_survives_another_bounded_reader(self):
        from tianshu_companion.clients import JsonService

        previous, current = await self.prepare()
        self.lease(previous, 30)
        self.lease(current, 120)
        old = previous["source_deadline"]
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.h.memory.prepare_context
        proved = []

        async def prepare(*args):
            result = await original(*args)
            lease = self.h.core.store.get("turns", previous["id"])["source_deadline"]
            proved.append(lease)
            self.assertGreater(lease, old)
            self.assertIn((previous["id"], lease), self.authorizations)
            entered.set()
            await release.wait()
            return result

        self.h.memory.prepare_context = prepare
        task = asyncio.create_task(self.h.core._prepare_context(current, []))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            self.h.clock.advance(1)
            other = self.h.core.store.get("turns", previous["id"])
            await asyncio.create_task(
                self.h.core.continue_delivery_origin(
                    other, read_budget=JsonService.QUERY_BUDGET_SECONDS
                )
            )
            self.assertEqual(proved[0], other["source_deadline"])
            release.set()
            context, _, checks, *_ = await task
            self.assertTrue(context.data["recent_dialogue"])
            self.assertTrue(any(c["source_turn_id"] == previous["id"] for c in checks))
            self.assertEqual(
                [proved[0]],
                [deadline for key, deadline in self.authorizations if key == previous["id"]],
            )
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_v2_local_snapshot_keeps_default_then_recent_reserves_its_actual_read(self):
        previous, current = await self.prepare()
        self.lease(previous, 120)
        self.lease(current, 32)
        old = current["source_deadline"]
        refresh = self.h.core._refresh_preparation_origin
        transitions = []

        async def refreshed(turn, **kwargs):
            before = self.h.core.store.get("turns", turn["id"])["source_deadline"]
            result = await refresh(turn, **kwargs)
            transitions.append((before, turn["source_deadline"]))
            return result

        self.h.core._refresh_preparation_origin = refreshed
        prepare = self.h.memory.prepare_context

        async def bounded(*args):
            self.assertGreater(current["source_deadline"], old)
            self.h.clock.advance(14)
            result = await prepare(*args)
            self.assertGreater(current["source_deadline"], self.h.clock() + 20)
            return result

        self.h.memory.prepare_context = bounded
        await self.h.core._prepare_context(current, [])
        self.assertEqual((old, old), transitions[0])
        self.assertEqual(old, transitions[1][0])
        self.assertGreater(transitions[1][1], old)

    async def test_profile_batch_reserves_main_lease_before_its_bounded_read(self):
        from tianshu_companion.context import TurnContext
        from tianshu_companion.context_preparation import profiles

        _, current = await self.prepare()
        self.lease(current, 32)
        old = current["source_deadline"]
        original = self.h.memory.profiles_batch
        calls = []

        async def bounded(*args):
            calls.append(current["source_deadline"])
            self.assertGreater(current["source_deadline"], old)
            self.h.clock.advance(14)
            result = await original(*args)
            self.assertGreater(current["source_deadline"], self.h.clock() + 20)
            return result

        self.h.memory.profiles_batch = bounded
        context = TurnContext(current["preparation"], [], [], memory=current["memory_context"])
        await profiles(self.h.core, current, context, [])
        self.assertEqual([current["source_deadline"]], calls)

    async def test_midrange_catalog_renewal_unknown_still_aborts_the_entire_preparation(self):
        previous, current = await self.prepare()
        self.lease(previous, 32)
        self.lease(current, 120)
        context = self.h.core.sender.expression_context
        calls = []

        async def unknown(source, scope, channel):
            if source["origin"] == previous["origin"]:
                raise Fault("result_unknown", unknown=True)
            return await context(source, scope, channel)

        async def prepare(*args):
            calls.append(args)
            return {}

        self.h.core.sender.expression_context = unknown
        self.h.memory.prepare_context = prepare
        with self.assertRaises(Fault) as caught:
            await self.h.core._prepare_context(current, [])
        self.assertTrue(caught.exception.unknown)
        self.assertTrue(caught.exception.context_renewal_failure)
        self.assertFalse(calls)

    async def test_read_budget_is_not_a_guarantee_after_unbounded_other_checks(self):
        previous, current = await self.prepare()
        self.lease(previous, 120)
        self.lease(current, 32)
        self.before_authority(lambda: self.h.clock.advance(61))
        context = self.h.core.sender.expression_context

        async def unchanged(source, scope, channel):
            # The port cannot supply a fresh grant after the first continuation.
            if self.renewals:
                return dict(
                    origin=source["origin"],
                    scope=scope,
                    channel=channel,
                    expires_at=utc(current["source_deadline"]),
                )
            return await context(source, scope, channel)

        self.h.core.sender.expression_context = unchanged
        with self.assertRaises(Fault):
            await self.h.core._preflight_once(current)
        self.assertEqual([], self.verifications)


del CoreEvidenceTests
