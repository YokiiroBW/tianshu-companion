"""Real consumer adapter; explicitly synthetic HTTP responses, origins and delivery."""

import asyncio
import copy
import json
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import httpx

from support import Harness, contracts
from tianshu_companion.clients import JsonService, Memory, utc
from tianshu_companion.context import CONTEXT_LIMIT, TurnContext
from tianshu_companion.contracts import Contracts, Fault, canonical, digest


def profile_unit(target, category="interest", text="咖啡白天可喝，因为晚上影响睡眠，所以晚上不喝"):
    key = digest([target, category, text])
    return dict(
        record_id="record:" + key,
        record_version=1,
        semantic_group_id="group:" + key,
        subject=target,
        category=category,
        field_key=category + ".coffee",
        statement=text,
        conditions=["白天且没有失眠"],
        negations=["晚上不喝"],
        valid_time="本月",
        uncertainty="inferred",
        reality="real",
        visibility="shared_projection",
        sharing="public_preference" if category == "interest" else "group_only",
        sources=[
            dict(
                kind="shareable_projection",
                owner="memory",
                projection_ref="projection:" + key,
                projection_version=1,
            )
        ],
    )


def response_for(request, units, version=1):
    groups = [
        dict(semantic_group_id=u["semantic_group_id"], record_ids=[u["record_id"]], complete=True)
        for u in units
    ]
    size = (
        len(canonical(dict(selected_units=units, dependency_groups=groups)).encode())
        if units
        else 0
    )
    return dict(
        schema_version=1,
        version_domain="profile-memory/v1",
        request_id=request["query"]["request_id"],
        requester_scope=request["requester_scope"],
        target=request["target"],
        scope_version=version,
        verified_at=utc(),
        valid_until=utc(time.time() + 3600),
        selected_units=units,
        dependency_groups=groups,
        budget_used=dict(tokens=size, bytes=size),
        omissions=[] if units else ["no_match"],
    )


class ProfileContextTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness(silence_ms=0)
        self.units, self.calls, self.override = {}, [], None
        self.version = 7
        self.client = JsonService(
            "https://fixture.invalid", "fixture-only", transport=httpx.MockTransport(self.handle)
        )
        self.adapter = Memory(self.h.contracts, self.client)
        self.h.memory.profiles = self.adapter.profiles

    async def asyncTearDown(self):
        await self.h.core.close()
        await self.client.close()

    def handle(self, incoming):
        self.assertEqual("/internal/v1/memory/profiles/select", incoming.url.path)
        request = json.loads(incoming.content)
        self.h.contracts.check("profiles#select_request", request)
        self.calls.append(request)
        if request["known_scope_version"] not in (None, self.version):
            return httpx.Response(409, json=dict(code="scope_changed"))
        available = copy.deepcopy(self.units.get(digest(request["target"]), []))
        if request["requester_scope"]["actor_id"] != "actor:a":
            available = []
        available = [u for u in available if u["category"] in request["selection"]]
        selected = []
        for unit in available:
            candidate = response_for(request, selected + [unit], self.version)
            if all(candidate["budget_used"][k] <= request["budget"][k] for k in request["budget"]):
                selected.append(unit)
        response = response_for(request, selected, self.version)
        if self.override:
            self.override(response)
        return httpx.Response(200, json=response, headers={"Cache-Control": "no-store"})

    async def say(self, text="咖啡", **kwargs):
        result = await self.h.ingest(text=text, group=True, channel="group:one", **kwargs)
        await self.h.cycles(70)
        return result

    def prompt(self):
        return json.loads(self.h.gateway.calls[-1][1][1]["content"])

    async def seed(self):
        b = await self.say("我叫小明，白天聊咖啡", account="b", targets=[])
        person = dict(kind="person", person_id=b["person_id"])
        group = dict(kind="group", conversation_id=b["conversation_id"])
        self.units[digest(person)] = [profile_unit(person)]
        self.units[digest(group)] = [
            profile_unit(group, "style", "本群咖啡讨论简洁"),
            profile_unit(group, "topic", "本群主题是咖啡"),
        ]
        return person, group

    async def test_other_person_current_group_style_and_topic_are_consumed_with_stable_authorship(
        self,
    ):
        person, group = await self.seed()
        a = await self.say("我也叫小明，聊聊咖啡")
        prompt = self.prompt()
        self.assertEqual("b", prompt["recent_dialogue"][0]["author"]["immutable_account_id"])
        self.assertEqual(person["person_id"], prompt["recent_dialogue"][0]["person_id"])
        self.assertEqual([group, person], [p["target"] for p in prompt["profiles"]])
        unit = prompt["profiles"][1]["selected_units"][0]
        self.assertEqual(["白天且没有失眠"], unit["conditions"])
        self.assertEqual(["晚上不喝"], unit["negations"])
        self.assertIn("因为晚上影响睡眠", unit["statement"])
        self.assertEqual("inferred", unit["uncertainty"])
        queries = [c for c in self.calls if c["budget"]["tokens"]]
        self.assertTrue(all(c["requester_scope"]["person_id"] == a["person_id"] for c in queries))
        self.assertNotEqual(person["person_id"], a["person_id"])
        self.assertTrue(
            any(c["target"] == person and c["known_scope_version"] == 7 for c in self.calls)
        )
        self.assertTrue(all(c["known_version"] in (None, 1) for c in self.h.memory.selections))
        self.assertEqual("sent", self.h.turns()[-1]["phase"])

    async def test_unseen_account_mention_and_nickname_do_not_resolve_or_link_an_identity(self):
        await self.seed()
        before = copy.deepcopy(self.h.memory.accounts)
        request = self.h.request(text="小明 @unknown 咖啡", group=True, channel="group:one")
        request["mentioned_accounts"] = [dict(namespace="tg", immutable_account_id="unknown")]
        await self.h.core.ingest("nonebot", request)
        await self.h.cycles(70)
        self.assertTrue(all(c["target"].get("person_id") != "unknown" for c in self.calls))
        self.assertTrue(all(dict(k)["namespace"] == "qq" for k in self.h.memory.accounts))
        self.assertEqual(len(before) + 1, len(self.h.memory.accounts))

    async def test_other_group_thread_actor_and_private_dialogue_are_isolated(self):
        person, _ = await self.seed()
        await self.say()
        for extra in [
            dict(channel="group:other"),
            dict(actor="actor:b"),
            dict(group=False, channel="private:a"),
        ]:
            args = dict(group=True, channel="group:one", text="咖啡")
            args.update(extra)
            await self.h.ingest(**args)
            await self.h.cycles(70)
            prompt = self.prompt()
            self.assertNotIn("本群咖啡讨论简洁", canonical(prompt))
            self.assertNotIn(person["person_id"], canonical(prompt["profiles"]))
            self.assertNotIn("我叫小明", canonical(prompt["recent_dialogue"]))
        request = self.h.request(text="咖啡", group=True, channel="group:one")
        request["message_key"]["channel"]["thread_id"] = "thread:other"
        await self.h.core.ingest("nonebot", request)
        await self.h.cycles(70)
        self.assertEqual([], self.prompt()["recent_dialogue"])

    async def test_different_groups_can_use_their_own_style(self):
        _, first = await self.seed()
        other = await self.h.ingest(
            text="群说明咖啡", group=True, channel="group:other", targets=[]
        )
        await self.h.cycles()
        second = dict(kind="group", conversation_id=other["conversation_id"])
        self.units[digest(second)] = [profile_unit(second, "style", "另一个群咖啡讨论详细")]
        await self.h.ingest(text="咖啡", group=True, channel="group:other")
        await self.h.cycles(70)
        self.assertIn("另一个群咖啡讨论详细", canonical(self.prompt()))
        self.assertNotIn(first["conversation_id"], canonical(self.prompt()))

    async def test_profile_revision_stops_unsent_segments_and_history_does_not_restore_it(self):
        self.h.memory.scope_version = 7  # Equal integers must still use separate version domains.
        await self.seed()
        h = self.h
        h.gateway.segments = ["画像派生回复一", "画像派生回复二"]
        h.sender.gate = asyncio.Event()
        await self.say()
        self.assertEqual(1, len(h.sender.calls))
        self.version += 1
        self.units.clear()  # Synthetic correction/forget/withdrawal of the published projections.
        h.sender.gate.set()
        await h.cycles(70)
        self.assertEqual(1, len(h.sender.calls))
        self.assertEqual("partial", h.turns()[-1]["delivery_state"])
        self.assertEqual("scope_changed", h.turns()[-1]["failure"])
        h.sender.gate = None
        await self.say("咖啡后来呢")
        self.assertNotIn("画像派生回复", canonical(self.prompt()))
        self.assertEqual("sent", h.core.store.list("replies")[0]["state"])

    async def test_sent_profile_derived_history_is_invalidated_after_forget_and_restart(self):
        await self.h.core.close()
        with tempfile.TemporaryDirectory() as directory:
            self.h = Harness(Path(directory) / "companion.db", silence_ms=0)
            self.h.memory.profiles = self.adapter.profiles
            await self.seed()
            self.h.gateway.segments = ["旧画像派生语句"]
            await self.say()
            await self.say("咖啡继续")
            self.assertIn("旧画像派生语句", canonical(self.prompt()["recent_dialogue"]))
            await self.h.core.close()
            self.h.core = self.h.new_core()
            self.h.core.recover()
            self.version += 1
            self.units.clear()
            await self.say()
            self.assertNotIn("旧画像派生语句", canonical(self.prompt()))
            await self.h.core.close()
            self.h = Harness(silence_ms=0)

    async def test_other_author_revision_retraction_and_rebinding_stop_dependent_output(self):
        await self.seed()
        h = self.h
        h.gateway.gates[2] = asyncio.Event()
        await self.say()
        first = h.turns()[0]["bundle"]["messages"][0]
        await h.ingest(
            account="b",
            group=True,
            channel="group:one",
            kind="retract",
            revision=2,
            message=first["message_key"]["message_id"],
        )
        h.gateway.gates[2].set()
        await h.cycles(70)
        self.assertFalse(h.sender.calls)
        self.assertEqual("scope_changed", h.turns()[1]["failure"])

    async def test_rebinding_or_forgetting_another_author_omits_the_entire_context_group(self):
        await self.seed()
        original = self.h.memory.identity

        async def rebound(origin, account, now):
            person, version = await original(origin, account, now)
            return person, 2 if account["immutable_account_id"] == "b" else version

        self.h.memory.identity = rebound
        await self.say()
        self.assertEqual([], self.prompt()["recent_dialogue"])
        self.assertTrue(all(p["target"]["kind"] == "group" for p in self.prompt()["profiles"]))

    async def test_legacy_collector_without_revision_seals_without_promoting_group_history(self):
        h = self.h
        h.core.policy = replace(h.core.policy, silence_ms=5000)
        result = await h.ingest(
            text="旧版无标记公开消息", account="b", group=True, channel="group:one", targets=[]
        )
        with h.core.store.transaction():
            collection = h.core.store.get("collections", result["collection_id"])
            collection.pop("source_context_revision")
            h.core.store.put("collections", collection)
        h.clock.advance(5)
        await h.cycles()
        self.assertEqual("observed", h.turns()[0]["phase"])
        self.assertIsNone(h.turns()[0]["context_revision"])
        h.core.policy = replace(h.core.policy, silence_ms=0)
        await self.say()
        self.assertEqual([], self.prompt()["recent_dialogue"])

    async def test_group_two_prepares_and_ordered_send_never_expose_a_draft(self):
        await self.seed()
        h = self.h
        h.gateway.gates[2] = asyncio.Event()
        h.gateway.segments = ["尚未发出的草稿"]
        await self.say(account="a")
        await self.say(account="c")
        self.assertEqual([2, 3], [seq for seq, _ in h.gateway.calls])
        self.assertNotIn("尚未发出的草稿", canonical(self.prompt()))
        self.assertFalse(h.sender.calls)
        h.gateway.gates[2].set()
        await h.cycles(100)
        self.assertEqual([2, 3], [c["turn_sequence"] for c in h.sender.calls])

    async def test_combined_budget_counts_memory_profiles_and_short_context_without_slicing(self):
        await self.seed()
        original = self.h.memory.select

        async def ordinary(origin, scope, text, budget, known_version=None, **kwargs):
            result = await original(origin, scope, text, budget, known_version)
            if budget["tokens"]:
                unit = profile_unit(dict(kind="person", person_id=scope["person_id"]))
                for key in ("subject", "category", "field_key", "sharing"):
                    unit.pop(key)
                unit["subject_person_id"] = scope["person_id"]
                result["selected_units"] = [unit]
                result["dependency_groups"] = [
                    dict(
                        semantic_group_id=unit["semantic_group_id"],
                        record_ids=[unit["record_id"]],
                        complete=True,
                    )
                ]
            return result

        self.h.memory.select = ordinary
        await self.say("之前咖啡")
        prompt = self.prompt()
        context = {
            k: prompt[k]
            for k in (
                "evidence",
                "dependency_groups",
                "profiles",
                "recent_dialogue",
                "earlier_fragment",
                "delivered_dependencies",
                "open_concerns",
                "current_activity",
                "short_affect",
            )
        }
        self.assertTrue(all(context[k] for k in ("evidence", "profiles", "recent_dialogue")))
        used = len(canonical(context).encode())
        self.assertEqual(dict(tokens=used, bytes=used), self.h.turns()[-1]["context_budget_used"])
        self.assertLessEqual(used, CONTEXT_LIMIT)
        # Exact aggregate boundary: the whole profile response fits, one byte less omits it whole.
        selection = dict(
            selected_units=context["evidence"], dependency_groups=context["dependency_groups"]
        )
        value = TurnContext(selection, [], [])
        block = context["profiles"][0]
        exact = value.used + len(canonical(block).encode())
        with patch("tianshu_companion.context.CONTEXT_LIMIT", exact):
            self.assertTrue(value.append("profiles", block))
        value = TurnContext(selection, [], [])
        with patch("tianshu_companion.context.CONTEXT_LIMIT", exact - 1):
            self.assertFalse(value.append("profiles", block))
            self.assertEqual([], value.data["profiles"])

    async def test_profile_adapter_rejects_untrusted_relations_fields_and_partial_groups(self):
        target = dict(kind="person", person_id="person:target")
        self.units[digest(target)] = [profile_unit(target)]
        scope = dict(
            actor_id="actor:a",
            person_id="person:requester",
            audience="group",
            conversation_id="conv:group",
        )
        origin = dict(assertion_ref="origin:requester")

        async def select():
            return await self.adapter.profiles(
                origin, scope, target, "咖啡", ["interest"], dict(tokens=8192, bytes=8192)
            )

        result = await select()
        self.assertTrue(result["selected_units"])
        mutations = [
            lambda r: r.update(request_id="wrong"),
            lambda r: r.update(target=dict(kind="person", person_id="wrong")),
            lambda r: r.update(requester_scope={**scope, "actor_id": "other"}),
            lambda r: r.update(version_domain="text-dialogue/v1"),
            lambda r: r.update(valid_until="2000-01-01T00:00:00Z"),
            lambda r: r["selected_units"][0].update(subject=dict(kind="person", person_id="wrong")),
            lambda r: r["selected_units"][0].update(relationship_score=99),
            lambda r: r["selected_units"][0].update(visibility="self_private"),
            lambda r: r["selected_units"][0]["sources"][0].update(raw_text="私人原文"),
            lambda r: r["dependency_groups"][0]["record_ids"].append("missing"),
            lambda r: r["dependency_groups"][0].update(complete=False),
            lambda r: r["selected_units"].append(copy.deepcopy(r["selected_units"][0])),
            lambda r: r.update(budget_used=dict(tokens=0, bytes=0)),
        ]
        for mutation in mutations:
            self.override = mutation
            with self.subTest(mutation=mutations.index(mutation)), self.assertRaises(Fault):
                await select()
        self.override = None
        with self.assertRaises(Fault):
            await self.adapter.profiles(
                origin,
                scope,
                dict(kind="group", conversation_id="other"),
                "咖啡",
                ["topic"],
                dict(tokens=0, bytes=0),
            )

    async def test_zero_budget_no_match_does_not_prove_existence_and_versions_are_independent(self):
        target = dict(kind="person", person_id="unknown")
        scope = dict(
            actor_id="actor:a",
            person_id="person:requester",
            audience="group",
            conversation_id="conv:group",
        )
        self.units[digest(target)] = [profile_unit(target)]
        origin = dict(assertion_ref="origin:requester")
        for person in (target, dict(kind="person", person_id="private-only")):
            response = await self.adapter.profiles(
                origin, scope, person, "咖啡", ["interest"], dict(tokens=0, bytes=0), 7
            )
            self.assertEqual([], response["selected_units"])
            self.assertEqual(7, response["scope_version"])
        with self.assertRaises(Fault) as error:
            await self.adapter.profiles(
                origin, scope, target, "咖啡", ["interest"], dict(tokens=0, bytes=0), 1
            )
        self.assertEqual("scope_changed", error.exception.code)


class ProfileContractTests(unittest.TestCase):
    def test_both_published_releases_are_loaded_and_profile_schema_tampering_is_rejected(self):
        release = contracts()
        self.assertIn("profiles", release.schemas)
        # Intercept a read, leaving the shared release untouched.
        original = Path.read_bytes

        def changed(path):
            data = original(path)
            return data + b" " if path.name == "profiles.json" else data

        root = Path(
            json.loads((Path(__file__).parents[1] / ".runtime/workspace-context.json").read_text())[
                "workspace"
            ]
        )
        with patch.object(Path, "read_bytes", changed), self.assertRaises(ValueError):
            Contracts(root / "contracts/text-dialogue/v1")
