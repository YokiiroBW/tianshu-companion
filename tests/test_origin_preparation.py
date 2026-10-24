"""Real origin read classification and dependency-closed optional history selection."""

import copy
import json
import ssl
import unittest
from unittest.mock import patch

import httpx

from test_memory_v2 import CoreEvidenceTests
from tianshu_companion.clients import JsonService, Memory, Origins
from tianshu_companion.context import scope_check
from tianshu_companion.context_preparation import _v2_variants
from tianshu_companion.contracts import Fault


class OriginPreparationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = CoreEvidenceTests.asyncSetUp

    async def turns(self, count=2):
        for index in range(count):
            await self.h.ingest(text=f"合成第{index}轮计划。")
            await self.h.cycles()
        self.assertTrue(all(t["phase"] == "sent" for t in self.h.turns()))
        return self.h.turns()

    def origins(self, modes):
        async def handler(request):
            body = json.loads(request.content)
            mode = modes(body["assertion_ref"])
            if mode in {"timeout", "pool", "protocol", "tls", "connect"}:
                if mode == "timeout":
                    raise httpx.ReadTimeout("synthetic", request=request)
                if mode == "pool":
                    raise httpx.PoolTimeout("synthetic", request=request)
                if mode == "protocol":
                    raise httpx.RemoteProtocolError("synthetic", request=request)
                if mode == "tls":
                    raise httpx.ReadError("synthetic", request=request) from ssl.SSLError()
                raise httpx.ConnectError("synthetic", request=request)
            if mode in {"401", "403", "404", "500"}:
                return httpx.Response(
                    int(mode),
                    json=dict(code="forbidden", execution_state="not_started", retryable=False),
                )
            if mode == "json":
                return httpx.Response(200, content=b"{bad-json")
            if mode == "unicode":
                return httpx.Response(200, content=b'"\xff"')
            if mode == "encoded":
                return httpx.Response(200, content=b"{}", headers={"content-encoding": "br"})
            value = dict(
                schema_version=1,
                request_id=body["request_id"],
                context=copy.deepcopy(self.h.origins.values[body["assertion_ref"]]),
            )
            if mode == "request_id":
                value["request_id"] = "request:wrong"
            elif mode == "schema":
                value["context"].pop("verified_account")
            elif mode == "revoked":
                value["context"]["revoked"] = True
            elif mode in {"issuer", "authenticated_service", "audience_service", "assertion_ref"}:
                value["context"][mode] = {
                    "issuer": "platform",
                    "authenticated_service": "platform",
                    "audience_service": "memory",
                    "assertion_ref": "origin:wrong",
                }[mode]
            return httpx.Response(200, json=value)

        wire = JsonService(
            "https://synthetic.invalid",
            "synthetic-token",
            transport=httpx.MockTransport(handler),
        )
        self.addAsyncCleanup(wire.close)
        value = Origins(self.h.contracts, {"nonebot": ("nonebot", wire)})
        self.h.core.origins = value
        return value, wire

    async def test_independent_unproved_origin_omits_full_and_input_without_authority(self):
        previous, current = await self.turns()
        ref = previous["origin"]["assertion_ref"]
        for mode in [
            "403",
            "404",
            "json",
            "schema",
            "revoked",
            "request_id",
            "issuer",
            "authenticated_service",
            "audience_service",
            "assertion_ref",
            "timeout",
        ]:
            with self.subTest(mode=mode):
                origins, _ = self.origins(lambda key: mode if key == ref else "valid")
                with self.assertRaises(Fault) as caught:
                    await origins.resolve(
                        previous["service"],
                        dict(request_id="request:direct", origin=previous["origin"]),
                        self.h.clock(),
                    )
                self.assertTrue(caught.exception.origin_read_unavailable)
                self.assertFalse(getattr(caught.exception, "context_dependency_invalid", False))
                context, _, checks, profiles, *_ = await self.h.core._prepare_context(current, [])
                self.assertEqual([], context.data["recent_dialogue"])
                self.assertEqual([], checks)
                self.assertEqual([], profiles)

    async def test_shared_auth_transport_and_config_faults_remain_global(self):
        previous, current = await self.turns()
        ref = previous["origin"]["assertion_ref"]
        for mode in ["401", "500", "pool", "protocol", "tls", "connect", "unicode", "encoded"]:
            with self.subTest(mode=mode):
                self.origins(lambda key: mode if key == ref else "valid")
                with self.assertRaises(Fault) as caught:
                    await self.h.core._prepare_context(current, [])
                self.assertFalse(getattr(caught.exception, "origin_read_unavailable", False))
        origins, wire = self.origins(lambda _: "valid")
        wire.token = None
        with self.assertRaises(Fault) as caught:
            await self.h.core._prepare_context(current, [])
        self.assertFalse(getattr(caught.exception, "origin_read_unavailable", False))
        origins.issuers.clear()
        with self.assertRaises(Fault) as caught:
            await self.h.core._prepare_context(current, [])
        self.assertEqual("unauthorized", caught.exception.code)

    async def test_main_and_other_required_check_close_the_whole_origin(self):
        previous, current = await self.turns()
        for source, required in [
            (previous, [scope_check(previous, input_only=True)]),
            (current, []),
        ]:
            with self.subTest(required=bool(required)):
                ref = source["origin"]["assertion_ref"]
                self.origins(lambda key: "403" if key == ref else "valid")
                candidate = [dict(turn_id=source["id"], replies=[])]
                with (
                    patch(
                        "tianshu_companion.context_preparation.recent_checks",
                        return_value=[scope_check(source)],
                    ),
                    self.assertRaises(Fault) as caught,
                ):
                    await _v2_variants(self.h.core, current, required, [candidate])
                self.assertEqual("forbidden", caught.exception.code)
                self.assertEqual(403, caught.exception.cause["http_status"])

    async def test_missing_or_invalid_issuer_config_fails_before_remote_read(self):
        _, current = await self.turns()
        for issuer in (None, "unknown", {}):
            with self.subTest(issuer=issuer):
                origins, wire = self.origins(lambda _: self.fail("invalid config reached issuer"))
                origins.issuers["nonebot"] = (issuer, wire)
                with self.assertRaises(Fault) as caught:
                    await self.h.core._prepare_context(current, [])
                self.assertEqual("dependency_unavailable", caught.exception.code)
                self.assertFalse(getattr(caught.exception, "origin_read_unavailable", False))

    async def test_one_unproved_check_excludes_same_origin_with_another_valid_lease(self):
        previous, current = await self.turns()
        clone = dict(copy.deepcopy(previous), id="turn:another-lease")
        clone["source_deadline"] = self.h.clock() + 36
        self.h.core._save_turn(clone)
        ref, reads = previous["origin"]["assertion_ref"], []

        def mode(key):
            if key != ref:
                return "valid"
            reads.append(key)
            if len(reads) == 2:
                # The apparently valid sibling enters the coordination margin,
                # but an unavailable check already excluded their whole origin.
                self.h.clock.advance(2)
            return "403" if len(reads) == 1 else "valid"

        self.origins(mode)
        batches = [[dict(turn_id=t["id"], replies=[])] for t in (previous, clone)]
        checks = {t["id"]: [scope_check(t)] for t in (previous, clone)}
        with patch(
            "tianshu_companion.context_preparation.recent_checks",
            side_effect=lambda store, candidate, **kwargs: checks[candidate[0]["turn_id"]],
        ):
            result = await _v2_variants(self.h.core, current, [], batches)
        self.assertEqual(2, len(reads))
        self.assertEqual([[], []], result)

    async def test_initial_rejected_check_is_not_retried_by_prepare_coordination(self):
        previous, current = await self.turns()
        previous["source_deadline"] = self.h.clock() + 10
        self.h.core._save_turn(previous)
        rejected = dict(scope_check(previous), binding_version=previous["binding_version"] + 1)
        verify, calls = self.h.core._verify_context_check, []

        async def checked(*args, **kwargs):
            calls.append(args[1])
            return await verify(*args, **kwargs)

        self.h.core._verify_context_check = checked
        with patch("tianshu_companion.context_preparation.recent_checks", return_value=[rejected]):
            result = await _v2_variants(
                self.h.core, current, [], [[dict(turn_id=previous["id"], replies=[])]]
            )
        self.assertEqual([[]], result)
        self.assertEqual([rejected], calls)

    async def test_reply_only_unproved_origin_keeps_only_independent_input_projection(self):
        old, previous, current = await self.turns(3)
        previous["context_checks"] = [scope_check(old)]
        self.h.core._save_turn(previous)
        ref = old["origin"]["assertion_ref"]
        self.origins(lambda key: "403" if key == ref else "valid")
        context, *_ = await self.h.core._prepare_context(current, [])
        self.assertEqual([previous["id"]], [g["turn_id"] for g in context.data["recent_dialogue"]])
        self.assertTrue(context.data["recent_dialogue"][0]["input_only"])
        self.assertEqual([], context.data["recent_dialogue"][0]["replies"])

    async def test_identity_read_cannot_borrow_origin_transport_marker(self):
        previous, current = await self.turns()
        self.origins(lambda _: "valid")
        original = self.h.memory.identity

        async def identity(origin, *args):
            if origin == previous["origin"]:
                error = Fault("dependency_unavailable", retryable_read=True)
                error.optional_read_unavailable = True
                raise error
            return await original(origin, *args)

        self.h.memory.identity = identity
        with self.assertRaises(Fault) as caught:
            await self.h.core._prepare_context(current, [])
        self.assertFalse(getattr(caught.exception, "origin_read_unavailable", False))

    async def test_lost_identity_register_ack_remains_unknown_and_is_not_replayed(self):
        _, current = await self.turns()
        self.origins(lambda _: "valid")
        calls = []

        async def handler(request):
            body = json.loads(request.content)
            calls.append(request.url.path)
            if request.url.path.endswith("/register"):
                raise httpx.ReadTimeout("synthetic lost ack", request=request)
            return httpx.Response(
                200,
                json=dict(
                    schema_version=1,
                    request_id=body["query"]["request_id"],
                    state="unregistered",
                    person_id=None,
                    binding_version=0,
                ),
            )

        wire = JsonService(
            "https://synthetic.invalid", "token", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        self.h.memory.identity = Memory(self.h.contracts, wire).identity
        with self.assertRaises(Fault) as caught:
            await self.h.core._prepare_context(current, [])
        self.assertTrue(caught.exception.unknown)
        self.assertFalse(getattr(caught.exception, "origin_read_unavailable", False))
        self.assertEqual(1, calls.count("/internal/v1/identity/register"))


del CoreEvidenceTests
