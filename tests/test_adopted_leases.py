"""Adopted v2 source proofs survive peer waits only with the same live lease."""

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
        for text in ("先记住这段合成计划。", "随后再问这段计划。"):
            await self.h.ingest(text=text, group=True, channel="group:lease")
            await self.h.cycles()
        previous, current = self.h.turns()
        self.assertEqual(["sent", "sent"], [previous["phase"], current["phase"]])
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

    def wait_after_source(self, previous, action):
        ready = asyncio.Event()
        verify = self.h.core._verify_context_check

        async def checked(turn, check, checkpoint, **kwargs):
            result = await verify(turn, check, checkpoint, **kwargs)
            if check["origin"] == previous["origin"]:
                ready.set()
            return result

        async def book(turn):
            await ready.wait()
            action()

        self.h.core._verify_context_check = checked
        self.h.core.book_reading.verify = book

    async def test_expired_early_source_is_renewed_and_reauthorized_before_complete_barrier(self):
        previous, current = await self.prepare()
        self.lease(previous, 25)
        self.wait_after_source(previous, lambda: self.h.clock.advance(30))
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
        self.assertEqual(2, len(source_proofs))
        self.assertEqual(old, source_proofs[0])
        self.assertGreater(source_proofs[1], old)

    async def test_main_renewed_elsewhere_gets_new_authority_even_when_refresh_needs_no_renewal(
        self,
    ):
        previous, current = await self.prepare()
        self.lease(current, 60)
        self.wait_after_source(previous, lambda: self.lease(current, 180))
        await self.h.core._preflight_once(current)
        proofs = [deadline for key, deadline in self.authorizations if key == current["id"]]
        self.assertEqual(2, len(proofs))
        self.assertGreater(proofs[1], proofs[0])
        self.assertEqual([current["origin"]], self.renewals)
        self.assertEqual(1, len(self.verifications))

    async def renewal_failure(self, failure):
        previous, current = await self.prepare()
        self.lease(previous, 25)
        self.wait_after_source(previous, lambda: self.h.clock.advance(30))
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

    async def test_lease_changed_while_verifying_rejects_old_proof_without_replaying_barrier(self):
        previous, current = await self.prepare()
        original = self.h.memory.verify_context

        async def barrier(*args):
            self.lease(previous, 180)
            return await original(*args)

        self.h.memory.verify_context = barrier
        with self.assertRaises(Fault) as caught:
            await self.h.core._preflight_once(current)
        self.assertTrue(caught.exception.retryable_read)
        self.assertFalse(caught.exception.unknown)
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

    async def test_renewal_during_authorization_cannot_label_a_new_lease_as_the_old_proof(self):
        previous, current = await self.prepare()
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
        with self.assertRaises(Fault) as caught:
            await self.h.core._prepare_context(current, [])
        self.assertTrue(caught.exception.retryable_read)
        self.assertTrue(changed)

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
            if near:
                with self.assertRaises(Fault) as caught:
                    await task
                self.assertTrue(caught.exception.retryable_read)
                self.assertGreater(previous["source_deadline"], deadline)
            else:
                await task
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

    async def test_real_near_expiry_renewal_still_invalidates_old_prepare_proof(self):
        await self.concurrent_continuation("prepare", near=True)

    async def test_real_near_expiry_renewal_still_invalidates_old_verify_proof(self):
        await self.concurrent_continuation("verify", near=True)

    async def test_preflight_reserves_existing_read_budget_before_local_checks(self):
        previous, current = await self.prepare()
        self.lease(previous, 120)
        self.lease(current, 32)
        old = current["source_deadline"]
        self.wait_after_source(previous, lambda: self.h.clock.advance(14))
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
        self.wait_after_source(previous, lambda: self.h.clock.advance(10))
        await self.h.core._preflight_once(current)
        fresh = self.h.core.store.get("turns", previous["id"])
        self.assertGreater(fresh["source_deadline"], old)
        self.assertEqual(
            [old, fresh["source_deadline"]],
            [deadline for key, deadline in self.authorizations if key == previous["id"]],
        )
        self.assertEqual([current["origin"], previous["origin"]], self.renewals)
        self.assertEqual(1, len(self.verifications))

    async def test_final_main_refresh_joins_its_exact_checkpoint_authorization(self):
        previous, current = await self.prepare()
        self.lease(previous, 120)
        self.wait_after_source(previous, lambda: self.h.clock.advance(30))
        await self.h.core._preflight_once(current)
        proofs = [deadline for key, deadline in self.authorizations if key == current["id"]]
        self.assertEqual(2, len(proofs))
        self.assertGreater(proofs[1], proofs[0])
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
        self.wait_after_source(previous, lambda: self.h.clock.advance(61))
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
