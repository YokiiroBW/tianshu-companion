"""Context windows, per-job receipts and bounded schema5 recovery."""

import asyncio
import copy
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from support import Harness, contracts
from tianshu_companion.app import create_app
from tianshu_companion.clients import JsonService, Memory, utc
from tianshu_companion.context import model_input_checks, record_model_dispatch, scope_check
from tianshu_companion.contracts import Fault, canonical, digest
from tianshu_companion.memory_summaries import messages, parse
from tianshu_companion.memory_context import prompt_material, query_request
from tianshu_companion.runtime_capabilities import read_capabilities
from tianshu_companion.summary_windows import sent_digest


def legacy_window(h, turn, turns=None):
    """Use the real compatibility builder for a previously capturable full window."""
    windows = h.core.summary_windows
    window = windows.read([turn["id"]])
    if turns is None:
        turns = [
            row
            for row in h.turns()
            if all(
                row["scope"][key] == turn["scope"][key]
                for key in ("actor_id", "audience", "conversation_id")
            )
        ]
    current = {row["id"]: h.core.store.get("turns", row["id"]) for row in turns}
    pairs = [
        pair
        for row in sorted(current.values(), key=lambda row: row["sequence"])
        for pair in windows._members(
            row, "new" if row["id"] == turn["id"] else "context", turns=current
        )
    ]
    manifest = window["manifest"]
    manifest["members"] = [member for member, _ in pairs]
    manifest["window_id"] = digest(
        {key: value for key, value in manifest.items() if key != "window_id"}
    )
    window["members"] = [dict(member_id=member["member_id"], text=value) for member, value in pairs]
    checked = windows.check(
        "memory", dict(schema_version=1, request_id="legacy-window:check", manifest=manifest)
    )
    if checked["state"] != "valid":
        raise AssertionError("Real compatibility reconstruction did not validate")
    return window


class WindowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.h = Harness(Path(self.temp.name) / "window.db", silence_ms=0)

    async def asyncTearDown(self):
        await self.h.core.close()

    async def chat(self, text="我周六想去看看", **kwargs):
        await self.h.ingest(text=text, **kwargs)
        await self.h.cycles()
        turn = self.h.turns()[-1]
        self.assertEqual("sent", turn["phase"])
        return turn

    def check(self, window):
        return self.h.core.summary_windows.check(
            "memory",
            dict(
                schema_version=1,
                request_id="window:check",
                manifest=window["manifest"],
            ),
        )

    def allow_empty_memory_provenance(self):
        # The synthetic dialogue used no Memory evidence or profile units.
        # Record its complete, empty v2 dependency set explicitly.
        for turn in self.h.turns():
            self.assertFalse(turn.get("profile_checks"))
            turn.pop("reply_context_provenance", None)  # Actual pre-journal frozen reply fixture.
            turn["memory_context"] = dict(
                bundle=dict(
                    group_dependencies=[],
                    episode_dependencies=[],
                    source_dependencies=[],
                )
            )
            self.h.core.store.put("turns", turn)

    def assert_projection_matches_window(self, material, window):
        self.assertNotIn("conversation", material)
        self.assertNotIn("conversation_context", material)
        subject = window["manifest"]["scope"]
        original = [
            (position, member)
            for position, member in enumerate(window["manifest"]["members"])
            if member["role"] == "user"
            and member["speaker"]["person_id"] == subject["person_id"]
            and member["source_scope"] == subject
        ]
        projected = sorted(
            [*material["subject_messages"], *material["subject_history"]],
            key=lambda member: member["position"],
        )
        self.assertEqual(
            [position for position, _ in original], [member["position"] for member in projected]
        )
        texts = {row["member_id"]: row["text"] for row in window["members"]}
        for actual, (_, expected) in zip(projected, original, strict=True):
            for key in (
                "member_id",
                "turn_id",
                "turn_sequence",
                "speaker",
                "observed_at",
                "role",
                "purpose",
            ):
                self.assertEqual(expected[key], actual[key])
            self.assertEqual(texts[expected["member_id"]], actual["text"])
        self.assertTrue(all(member["purpose"] == "new" for member in material["subject_messages"]))
        self.assertTrue(
            all(member["purpose"] == "context" for member in material["subject_history"])
        )

    async def test_group_window_preserves_speakers_bot_and_later_correction(self):
        first = await self.chat("我周六想去看看", group=True, channel="group:window")
        other = await self.chat("我可能周日去", account="b", group=True, channel="group:window")
        last = await self.chat("刚才只是设想，我还没决定", group=True, channel="group:window")
        self.allow_empty_memory_provenance()
        window = self.h.core.summary_windows.read([last["id"]])
        members = window["manifest"]["members"]
        people = [row for row in members if row["role"] == "user"]
        self.assertEqual([first["id"], last["id"]], [row["turn_id"] for row in people])
        self.assertEqual(["context", "new"], [row["purpose"] for row in people])
        self.assertTrue(all(row["role"] == "user" for row in members))
        legacy = legacy_window(self.h, last, [first, other, last])
        self.assertTrue(any(row["role"] == "assistant" for row in legacy["manifest"]["members"]))
        self.assertTrue(
            any(row["source_scope"] == other["scope"] for row in legacy["manifest"]["members"])
        )
        old_model = json.loads(messages(self.h.contracts, legacy, None)[1]["content"])
        self.assert_projection_matches_window(old_model, legacy)
        self.assertEqual("valid", self.check(window)["state"])
        original = copy.deepcopy(window)
        prompt = json.loads(messages(self.h.contracts, window, None)[1]["content"])
        self.assert_projection_matches_window(prompt, window)
        self.assertEqual(original, window)
        self.assertEqual(window["manifest"]["coverage"], prompt["coverage"])
        new_human = prompt["subject_messages"]
        self.assertEqual(1, len(new_human))
        self.assertIn("刚才只是设想", new_human[0]["text"])
        self.assertEqual(
            [first["id"]],
            [row["turn_id"] for row in prompt["subject_history"]],
        )

    async def test_append_does_not_replace_or_invalidate_captured_members(self):
        first = await self.chat()
        window = self.h.core.summary_windows.read([first["id"]])
        await self.chat("又补一句")
        self.assertEqual("valid", self.check(window)["state"])
        self.assertEqual([first["id"]], [row["turn_id"] for row in window["manifest"]["new_turns"]])

    async def test_model_subject_distinguishes_owner_history_from_other_speakers(self):
        await self.chat("我准备去拿书", group=True, channel="group:subjects")
        owner = await self.chat("我已经退了包裹", account="b", group=True, channel="group:subjects")
        last = await self.chat(
            "他说的时间是哪儿的时间？", account="b", group=True, channel="group:subjects"
        )
        self.allow_empty_memory_provenance()
        window = self.h.core.summary_windows.read([last["id"]])
        human = next(
            member
            for member in window["manifest"]["members"]
            if member["turn_id"] == owner["id"] and member["role"] == "user"
        )
        text = next(
            row["text"] for row in window["members"] if row["member_id"] == human["member_id"]
        )
        portrait = dict(
            subject_person_id=last["scope"]["person_id"],
            expected_revision=0,
            summary_refs=[dict(record_id="record:owner", record_version=1)],
            summaries=[
                dict(
                    record_id="record:owner",
                    record_version=1,
                    subject_person_id=last["scope"]["person_id"],
                    subject_material=[
                        {
                            **{key: human[key] for key in ("member_id", "speaker", "observed_at")},
                            "text": text,
                        }
                    ],
                )
            ],
            current_text="旧版画像的解释，不应反馈为原始依据。",
        )
        original_window, original_portrait = copy.deepcopy(window), copy.deepcopy(portrait)
        material = json.loads(messages(self.h.contracts, window, portrait)[1]["content"])
        self.assertEqual(last["scope"], {key: material["subject"][key] for key in last["scope"]})
        self.assertEqual(last["bundle"]["collection_key"]["author"], material["subject"]["account"])
        self.assertTrue(material["subject"]["new_member_ids"])
        self.assert_projection_matches_window(material, window)
        self.assertEqual(
            material["subject"]["new_member_ids"],
            [member["member_id"] for member in material["subject_messages"]],
        )
        for member in [*material["subject_messages"], *material["subject_history"]]:
            self.assertEqual("subject", member["relation_to_subject"])
        earlier_owner = next(
            row for row in material["subject_history"] if row["member_id"] == human["member_id"]
        )
        self.assertEqual("context", earlier_owner["purpose"])
        self.assertEqual(
            [text],
            [row["text"] for row in material["portrait_input"]["summaries"][0]["subject_material"]],
        )
        self.assertNotIn("text", material["portrait_input"]["summaries"][0])
        self.assertEqual(dict(portrait, current_text=""), material["portrait_input"])
        self.assertEqual(original_portrait, portrait)
        self.assertEqual(original_window, window)
        for level in ("portrait", "summary", "member", "refs", "size", "schema"):
            with self.subTest(level=level):
                wrong = copy.deepcopy(portrait)
                if level == "portrait":
                    wrong["subject_person_id"] = "person:other"
                elif level == "summary":
                    wrong["summaries"][0]["subject_person_id"] = "person:other"
                elif level == "member":
                    wrong["summaries"][0]["subject_material"][0]["speaker"]["person_id"] = (
                        "person:other"
                    )
                elif level == "refs":
                    wrong["summary_refs"][0]["record_id"] = "record:other"
                elif level == "size":
                    raw = wrong["summaries"][0]["subject_material"][0]
                    raw["text"] = "甲" * 3001
                    wrong["summaries"][0]["subject_material"].append(
                        dict(raw, member_id="input:more")
                    )
                else:
                    wrong["current_text"] = []
                with self.assertRaises(Fault):
                    messages(self.h.contracts, window, wrong)

    async def test_owner_history_keeps_original_unicode_and_linked_account_identity(self):
        first_text = "我之前说‘🍋’还没决定。\n原话不改。"
        first = await self.chat(first_text, group=True, channel="group:accounts")
        # The existing identity port double explicitly links both real input
        # accounts to one person. Core still builds and checks the full window.
        self.h.memory.accounts[
            tuple(sorted(dict(namespace="qq", immutable_account_id="b").items()))
        ] = first["scope"]["person_id"]
        last = await self.chat(
            "后来想了想，仍没确定。🪶", account="b", group=True, channel="group:accounts"
        )
        self.allow_empty_memory_provenance()
        window = self.h.core.summary_windows.read([last["id"]])
        self.assertEqual("valid", self.check(window)["state"])
        original = copy.deepcopy(window)
        material = json.loads(messages(self.h.contracts, window, None)[1]["content"])
        self.assert_projection_matches_window(material, window)
        history = material["subject_history"]
        self.assertEqual(1, len(history))
        self.assertEqual(first_text, history[0]["text"])
        self.assertNotEqual(material["subject"]["account"], history[0]["speaker"]["account"])
        self.assertEqual(material["subject"]["person_id"], history[0]["speaker"]["person_id"])
        self.assertIsNone(material["portrait_input"])
        self.assertEqual(original, window)
        self.assertEqual(1, material["subject_messages"][0]["position"])
        legacy = legacy_window(self.h, last, [first, last])
        old_model = json.loads(messages(self.h.contracts, legacy, None)[1]["content"])
        self.assert_projection_matches_window(old_model, legacy)
        self.assertGreater(
            old_model["subject_messages"][0]["position"],
            old_model["subject_history"][0]["position"] + 1,
        )

    async def test_model_history_does_not_cross_the_original_exact_source_scope(self):
        first = await self.chat("之前的本人原话")
        last = await self.chat("本轮的本人原话")
        self.allow_empty_memory_provenance()
        window = self.h.core.summary_windows.read([last["id"]])
        self.assertEqual("valid", self.check(window)["state"])
        # Projection-only copies isolate scope filtering; they are not claimed
        # to pass the real owner check above after their metadata is altered.
        for key, value in (
            ("actor_id", "actor:other"),
            ("person_id", "person:other"),
            ("audience", "group"),
            ("conversation_id", "conv:other"),
        ):
            with self.subTest(key=key):
                changed = copy.deepcopy(window)
                human = next(
                    member
                    for member in changed["manifest"]["members"]
                    if member["role"] == "user" and member["turn_id"] == first["id"]
                )
                human["source_scope"][key] = value
                material = json.loads(messages(self.h.contracts, changed, None)[1]["content"])
                self.assertEqual([], material["subject_history"])
                self.assertEqual(
                    [last["id"]], [member["turn_id"] for member in material["subject_messages"]]
                )
                new_human = next(
                    member
                    for member in changed["manifest"]["members"]
                    if member["role"] == "user" and member["purpose"] == "new"
                )
                new_human["source_scope"][key] = value
                with self.assertRaises(Fault):
                    messages(self.h.contracts, changed, None)

    async def test_window_check_reuses_reconstruction_only_inside_one_call(self):
        await self.chat()
        last = await self.chat("补充一句")
        self.allow_empty_memory_provenance()
        windows = self.h.core.summary_windows
        window = legacy_window(self.h, last)
        count = len({member["turn_id"] for member in window["manifest"]["members"]})
        with (
            patch.object(windows, "_current", wraps=windows._current) as current,
            patch.object(windows, "_members", wraps=windows._members) as members,
        ):
            self.assertEqual("valid", self.check(window)["state"])
            self.assertEqual((count, count), (current.call_count, members.call_count))
            reply = self.h.core.store.turn_replies(last["id"])[0]
            reply["text"] += " 篡改正文"
            self.h.core.store.put("replies", reply)
            self.assertEqual("invalid", self.check(window)["state"])
            self.assertEqual((2 * count, 2 * count), (current.call_count, members.call_count))

    async def test_original_edit_or_binding_change_cannot_validate_old_text(self):
        first = await self.chat()
        window = self.h.core.summary_windows.read([first["id"]])
        for key, replacement in (
            ("binding_version", first["binding_version"] + 1),
            ("cancelled", True),
        ):
            with self.subTest(field=key):
                changed = copy.deepcopy(first)
                changed[key] = replacement
                self.h.core.store.put("turns", changed)
                with self.assertRaises(Fault):
                    self.check(window)
                self.h.core.store.put("turns", first)
        message = first["bundle"]["messages"][0]
        await self.h.ingest(
            text="改成周日", message=message["message_key"]["message_id"], revision=2, kind="edit"
        )
        with self.assertRaises(Fault):
            self.check(window)

    async def test_forged_hash_speaker_or_partial_new_input_is_not_valid(self):
        turn = await self.chat()
        window = self.h.core.summary_windows.read([turn["id"]])
        for field, value in (
            ("content_sha256", "0" * 64),
            ("speaker", dict(kind="actor", actor_id="actor:a")),
        ):
            with self.subTest(field=field):
                forged = copy.deepcopy(window)
                forged["manifest"]["members"][0][field] = value
                manifest = forged["manifest"]
                manifest["window_id"] = digest(
                    {k: v for k, v in manifest.items() if k != "window_id"}
                )
                try:
                    checked = self.check(forged)
                except Fault:
                    pass
                else:
                    self.assertEqual("invalid", checked["state"])

    async def test_new_capture_does_not_require_old_bot_provenance(self):
        turn = await self.chat()
        window = self.h.core.summary_windows.read([turn["id"]])
        self.assertEqual(["user"], [row["role"] for row in window["manifest"]["members"]])
        self.assertIn("我周六想去看看", window["members"][0]["text"])
        self.assertTrue(window["manifest"]["coverage"]["complete"])

    async def test_bot_context_cannot_be_separated_from_its_complete_human_input(self):
        first = await self.chat()
        last = await self.chat("还没决定")
        self.allow_empty_memory_provenance()
        window = legacy_window(self.h, last)
        manifest = window["manifest"]
        self.assertTrue(
            any(
                row["role"] == "assistant" and row["turn_id"] == first["id"]
                for row in manifest["members"]
            )
        )
        manifest["members"] = [
            row
            for row in manifest["members"]
            if not (row["role"] == "user" and row["turn_id"] == first["id"])
        ]
        manifest["window_id"] = digest({k: v for k, v in manifest.items() if k != "window_id"})
        self.assertEqual("invalid", self.check(window)["state"])

    async def test_sent_receipt_poll_metadata_does_not_change_stable_reference(self):
        turn = await self.chat()
        self.allow_empty_memory_provenance()
        window = legacy_window(self.h, turn)
        reply = self.h.core.store.turn_replies(turn["id"])[0]
        original = sent_digest(reply)
        reply["receipt"].update(observed_at="2026-10-24T01:00:00Z", attempt_id="attempt:later-poll")
        self.h.core.store.put("replies", reply)
        self.assertEqual(original, sent_digest(reply))
        self.assertEqual("valid", self.check(window)["state"])
        reply["text"] += " changed"
        self.h.core.store.put("replies", reply)
        self.assertEqual("invalid", self.check(window)["state"])

    async def test_episode_dependency_uses_its_source_owner_and_private_leaf_stays_private(self):
        turn = await self.chat()
        self.allow_empty_memory_provenance()
        fixture = next(
            row
            for row in self.h.contracts.documents["memory-context/v2/examples.json"]
            if row["name"] == "context-positive"
        )["query_response"]["bundle"]
        episode = copy.deepcopy(fixture["episode_dependencies"][0])
        source = copy.deepcopy(
            next(
                row
                for row in fixture["source_dependencies"]
                if row["dependency_id"] == episode["source_dependency_id"]
            )
        )
        source["owner_scope"] = copy.deepcopy(turn["scope"])
        turn = self.h.core.store.get("turns", turn["id"])
        turn["memory_context"]["bundle"].update(
            episode_dependencies=[episode], source_dependencies=[source]
        )
        self.h.core.store.put("turns", turn)
        window = legacy_window(self.h, turn)
        bot = next(row for row in window["manifest"]["members"] if row["role"] == "assistant")
        self.assertEqual([episode], bot["memory_dependencies"]["episode_dependencies"])
        source["owner_scope"]["conversation_id"] = "conversation:another-private"
        self.h.core.store.put("turns", turn)
        private = legacy_window(self.h, turn)
        self.assertEqual(["user"], [row["role"] for row in private["manifest"]["members"]])

    async def test_new_capture_ignores_unused_bot_source_dependencies(self):
        turn = await self.chat()
        self.allow_empty_memory_provenance()
        fixture = next(
            row
            for row in self.h.contracts.documents["memory-context/v2/examples.json"]
            if row["name"] == "context-positive"
        )["query_response"]["bundle"]
        template = fixture["source_dependencies"][0]
        dependencies = [
            dict(
                copy.deepcopy(template),
                dependency_id=f"dependency:{index}",
                source_key=f"source:synthetic:{index}",
                owner_scope=copy.deepcopy(turn["scope"]),
            )
            for index in range(256)
        ]
        turn = self.h.core.store.get("turns", turn["id"])
        turn["memory_context"]["bundle"]["source_dependencies"] = dependencies
        self.h.core.store.put("turns", turn)
        window = self.h.core.summary_windows.read([turn["id"]])
        self.assertEqual(["user"], [row["role"] for row in window["manifest"]["members"]])
        self.assertEqual("我周六想去看看", window["members"][0]["text"])
        self.assertTrue(window["manifest"]["coverage"]["complete"])
        self.assertEqual("valid", self.check(window)["state"])

    def source_dependencies(self, turn, start, count):
        fixture = next(
            row
            for row in self.h.contracts.documents["memory-context/v2/examples.json"]
            if row["name"] == "context-positive"
        )["query_response"]["bundle"]
        return [
            dict(
                copy.deepcopy(fixture["source_dependencies"][0]),
                dependency_id=f"dependency:wire:{index}",
                source_key=f"source:wire:{index}",
                owner_scope=copy.deepcopy(turn["scope"]),
            )
            for index in range(start, start + count)
        ]

    async def test_bot_dependency_wire_bound_keeps_64_and_omits_complete_65(self):
        turn = await self.chat()
        self.allow_empty_memory_provenance()
        for count in (64, 65):
            with self.subTest(count=count):
                turn = self.h.core.store.get("turns", turn["id"])
                turn["memory_context"]["bundle"]["source_dependencies"] = self.source_dependencies(
                    turn, 0, count
                )
                self.h.core.store.put("turns", turn)
                window = legacy_window(self.h, turn)
                bots = [row for row in window["manifest"]["members"] if row["role"] == "assistant"]
                self.assertEqual(count == 64, bool(bots))
                if bots:
                    self.assertEqual(
                        count, len(bots[0]["memory_dependencies"]["source_dependencies"])
                    )
                self.assertEqual("我周六想去看看", window["members"][0]["text"])
                self.assertEqual("valid", self.check(window)["state"])

    async def test_inherited_reply_dependencies_are_bounded_after_complete_deduplication(self):
        first = await self.chat()
        last = await self.chat("补充一次")
        self.allow_empty_memory_provenance()
        first, last = [self.h.core.store.get("turns", turn["id"]) for turn in (first, last)]
        first["memory_context"]["bundle"]["source_dependencies"] = self.source_dependencies(
            first, 0, 32
        )
        last["context_checks"] = [scope_check(first)]
        self.h.core.store.put("turns", first)
        for start, count, expected in ((0, 33, True), (32, 32, True), (32, 33, False)):
            with self.subTest(start=start, count=count):
                last["memory_context"]["bundle"]["source_dependencies"] = self.source_dependencies(
                    last, start, count
                )
                self.h.core.store.put("turns", last)
                window = legacy_window(self.h, last)
                bots = [
                    row
                    for row in window["manifest"]["members"]
                    if row["role"] == "assistant" and row["turn_id"] == last["id"]
                ]
                self.assertEqual(expected, bool(bots))
                self.assertTrue(
                    any(
                        row["role"] == "user" and row["turn_id"] == last["id"]
                        for row in window["manifest"]["members"]
                    )
                )
                self.assertEqual("valid", self.check(window)["state"])

    async def test_dispatch_journal_keeps_each_used_bundle_and_foreign_leaf_in_bot_window(self):
        first = await self.chat(
            "合成作者甲的发言", account="journal-a", group=True, channel="group:journal"
        )
        other = await self.chat(
            "合成作者乙的发言", account="journal-b", group=True, channel="group:journal"
        )
        self.allow_empty_memory_provenance()
        first = self.h.core.store.get("turns", first["id"])
        first["model_calls"] = 0
        bundle = first["memory_context"]["bundle"]
        foreign = self.source_dependencies(first, 0, 1)[0]
        foreign["owner_scope"] = copy.deepcopy(other["scope"])
        bundle.update(
            retrieval_mode="context",
            effective_scope=copy.deepcopy(first["scope"]),
            bundle_id="bundle:used:first",
            source_dependencies=[foreign],
        )
        record_model_dispatch(first, model_input_checks(first))
        own = self.source_dependencies(first, 1, 1)[0]
        bundle.update(bundle_id="bundle:used:second", source_dependencies=[own])
        record_model_dispatch(first, model_input_checks(first))
        self.h.core.store.put("turns", first)
        window = legacy_window(self.h, other)
        bot = next(
            m
            for m in window["manifest"]["members"]
            if m["role"] == "assistant" and m["turn_id"] == first["id"]
        )
        self.assertCountEqual([foreign, own], bot["memory_dependencies"]["source_dependencies"])
        self.assertEqual("valid", self.check(window)["state"])

    async def test_mixed_dispatch_journal_cannot_borrow_latest_bundle_for_bot_window(self):
        turn = await self.chat()
        self.allow_empty_memory_provenance()
        turn = self.h.core.store.get("turns", turn["id"])
        turn["model_calls"] = 0
        bundle = turn["memory_context"]["bundle"]
        bundle.update(
            retrieval_mode="context",
            effective_scope=copy.deepcopy(turn["scope"]),
            bundle_id="bundle:before",
        )
        record_model_dispatch(turn, model_input_checks(turn))
        bundle["retrieval_mode"] = "factual"
        record_model_dispatch(turn, model_input_checks(turn))
        bundle.update(retrieval_mode="context", bundle_id="bundle:after")
        record_model_dispatch(turn, model_input_checks(turn))
        self.h.core.store.put("turns", turn)
        window = legacy_window(self.h, turn)
        self.assertEqual(["user"], [m["role"] for m in window["manifest"]["members"]])
        self.assertEqual("我周六想去看看", window["members"][0]["text"])

    async def test_known_context_invalidation_discards_old_turn_and_dependent_bot_only(self):
        first = await self.chat()
        last = await self.chat("刚才还没决定")
        self.allow_empty_memory_provenance()
        window = legacy_window(self.h, last)
        invalid = next(
            row["member_id"]
            for row in window["manifest"]["members"]
            if row["turn_id"] == first["id"] and row["role"] == "user"
        )
        reduced = self.h.core.summary_windows.without_context(window, [invalid])
        self.assertFalse(
            any(row["turn_id"] == first["id"] for row in reduced["manifest"]["members"])
        )
        self.assertEqual(["user"], [row["role"] for row in reduced["manifest"]["members"]])
        self.assertEqual(window["manifest"]["new_turns"], reduced["manifest"]["new_turns"])
        self.assertEqual("valid", self.check(reduced)["state"])

    async def test_invalid_new_turn_bot_keeps_its_new_human_and_does_not_refill(self):
        turn = await self.chat()
        self.allow_empty_memory_provenance()
        window = legacy_window(self.h, turn)
        invalid = next(
            row["member_id"] for row in window["manifest"]["members"] if row["role"] == "assistant"
        )
        reduced = self.h.core.summary_windows.without_context(window, [invalid])
        self.assertEqual(["user"], [row["role"] for row in reduced["manifest"]["members"]])
        self.assertEqual("我周六想去看看", reduced["members"][0]["text"])

    async def test_owner_history_is_filtered_before_the_original_candidate_limit(self):
        first = await self.chat("本人较早的原话", group=True, channel="group:crowded")
        for index in range(14):
            await self.chat(
                f"他人的第{index}次发言", account="b", group=True, channel="group:crowded"
            )
        last = await self.chat("本人后来补充", group=True, channel="group:crowded")
        store, windows = self.h.core.store, self.h.core.summary_windows
        unfiltered = store.recent_turns(last["conversation_id"], last["sequence"] + 1, 13)
        self.assertNotIn(first["id"], [row["id"] for row in unfiltered])
        exact = store.recent_turns(
            last["conversation_id"], last["sequence"] + 1, 13, scope=last["scope"]
        )
        self.assertEqual([last["id"], first["id"]], [row["id"] for row in exact])
        with patch.object(store, "recent_turns", wraps=store.recent_turns) as recent:
            window = windows.read([last["id"]])
        self.assertEqual(1, recent.call_count)
        self.assertEqual(last["scope"], recent.call_args.kwargs["scope"])
        self.assertEqual(
            [first["id"], last["id"]], [row["turn_id"] for row in window["manifest"]["members"]]
        )
        self.assertTrue(window["manifest"]["coverage"]["complete"])
        self.assertEqual("valid", self.check(window)["state"])

    async def test_new_capture_and_check_never_read_unused_reply_material(self):
        first = await self.chat("本人第一轮")
        last = await self.chat("本人第二轮")
        windows = self.h.core.summary_windows
        with (
            patch.object(
                windows,
                "_reply_dependencies",
                side_effect=AssertionError("unused reply dependency"),
            ),
            patch.object(
                self.h.core.store,
                "replies_for_turns",
                side_effect=AssertionError("unused sent reply"),
            ),
            patch.object(
                windows, "_members", side_effect=AssertionError("legacy reply reconstruction")
            ),
        ):
            window = windows.read([last["id"]])
            self.assertEqual("valid", self.check(window)["state"])
        self.assertEqual(
            [first["id"], last["id"]], [row["turn_id"] for row in window["manifest"]["members"]]
        )

    async def test_foreign_and_bot_changes_leave_the_new_owner_capture_unchanged(self):
        first = await self.chat("本人原话", group=True, channel="group:isolated")
        foreign = await self.chat("他人原话", account="b", group=True, channel="group:isolated")
        last = await self.chat("本人补充", group=True, channel="group:isolated")
        self.allow_empty_memory_provenance()
        old = legacy_window(self.h, last, [first, foreign, last])
        fresh = self.h.core.summary_windows.read([last["id"]])
        reply = self.h.core.store.turn_replies(last["id"])[0]
        original_reply = copy.deepcopy(reply)
        reply["receipt"] = dict(reply["receipt"], state="failed")
        self.h.core.store.put("replies", reply)
        self.assertEqual(fresh, self.h.core.summary_windows.read([last["id"]]))
        self.assertEqual("valid", self.check(fresh)["state"])
        self.assertEqual("invalid", self.check(old)["state"])
        self.h.core.store.put("replies", original_reply)
        self.assertEqual("valid", self.check(old)["state"])
        foreign["cancelled"] = True
        self.h.core.store.put("turns", foreign)
        self.assertEqual(fresh, self.h.core.summary_windows.read([last["id"]]))
        self.assertEqual("valid", self.check(fresh)["state"])
        self.assertEqual("invalid", self.check(old)["state"])

    async def test_private_window_cannot_include_other_person_or_actor(self):
        first = await self.chat()
        second = await self.chat("另一人", account="b", channel="private:b")
        with self.assertRaises(Fault):
            self.h.core.summary_windows.read([first["id"], second["id"]])
        third = await self.chat("另一角色", actor="actor:b")
        with self.assertRaises(Fault):
            self.h.core.summary_windows.read([first["id"], third["id"]])

    async def test_owner_check_http_is_memory_only_and_does_not_write(self):
        turn = await self.chat()
        window = self.h.core.summary_windows.read([turn["id"]])
        request = dict(schema_version=1, request_id="window:http", manifest=window["manifest"])
        app = create_app(
            self.h.core, {"memory": "synthetic-memory", "platform": "synthetic-platform"}
        )
        self.addCleanup(app.state.log.close)
        before = self.h.core.store.db.total_changes
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            for token, status in (
                ("wrong", 401),
                ("synthetic-platform", 403),
                ("synthetic-memory", 200),
            ):
                response = await client.post(
                    "/internal/v1/memory/consolidation/window/check",
                    json=request,
                    headers={"Authorization": "Bearer " + token},
                )
                self.assertEqual(status, response.status_code)
        self.assertEqual(before, self.h.core.store.db.total_changes)


class SummaryLearningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.h = Harness(Path(self.temp.name) / "summary.db", silence_ms=0)
        self.h.memory.extraction_schema = 5
        original_commit = self.h.memory.commit

        async def commit(event):
            receipt = await original_commit(event)
            # The old shared double intentionally has one fixture candidate.
            # These multi-turn scenarios need the producer's per-input identity,
            # retaining that same identity on an original input's receipt retry.
            return dict(
                receipt,
                candidate_job_ref="candidate:"
                + digest([event["aggregate_id"], event["input_revision"]]),
            )

        self.h.memory.commit = commit
        self.calls, self.generated = [], []
        self.window, self.completed = None, None
        self.windows, self.receipts = {}, {}
        self.lost_ack, self.portrait_pending = False, False
        self.original_generate = self.h.gateway.generate
        self.h.memory.process_candidate = self.process
        self.h.gateway.generate = self.generate

    async def asyncTearDown(self):
        await self.h.core.close()

    async def chat(self, text="我周六想去看看"):
        await self.h.ingest(text=text)
        await self.h.cycles()
        self.assertEqual("sent", self.h.turns()[-1]["phase"])

    def tasks(self):
        return self.h.core.store.list("metadata", "memory:learning")

    async def flush(self):
        await self.h.core.flush_outbox()
        await self.h.core.memory_learning.work()

    async def process(self, job, output, **request):
        self.calls.append(dict(job_id=job, output=copy.deepcopy(output), **copy.deepcopy(request)))
        self.assertEqual(5, request["extraction_schema"])
        if self.completed:
            return copy.deepcopy(self.completed)
        if job in self.receipts:
            return copy.deepcopy(self.receipts[job])
        if request["window"] is not None:
            self.window = copy.deepcopy(request["window"])
            self.windows[job] = self.window
        ids = request["job_ids"]
        self.assertEqual([job], ids)
        window = self.windows[job]
        subject = window["manifest"]["scope"]["person_id"]
        if output is None:
            return dict(
                state="pending",
                extraction_schema=5,
                batch_ref="batch:one",
                job_ids=ids,
                window_id=window["manifest"]["window_id"],
                subject_person_id=subject,
                portrait_input=None,
            )
        result = dict(
            subject_person_id=subject,
            state="committed" if output["summary"] else "skipped",
            record_ids=["record:summary"],
            group_ids=["group:summary"],
            experience_state="saved",
            summary_state="committed" if output["summary"] else "skipped",
            summary_record_refs=[dict(record_id="record:summary", record_version=1)]
            if output["summary"]
            else [],
            portrait=dict(
                state="pending" if self.portrait_pending else "not_requested", record_refs=[]
            ),
            covered_job_ids=ids,
            cursor=dict(window_id=window["manifest"]["window_id"], job_ids=ids),
        )
        self.receipts[job] = dict(
            state="completed",
            extraction_schema=5,
            batch_ref="batch:one",
            job_ids=ids,
            result=result,
        )
        if self.lost_ack:
            self.lost_ack = False
            raise Fault("result_unknown", unknown=True)
        return copy.deepcopy(self.receipts[job])

    async def generate(self, turn, material):
        if not turn["id"].startswith("memory-generation:"):
            return await self.original_generate(turn, material)
        self.generated.append(copy.deepcopy(material))
        subject = json.loads(material[1]["content"])["subject"]["person_id"]
        return [
            canonical(
                dict(
                    subject_person_id=subject,
                    summary="对方谈到周六的想法，后来仍有待决定。",
                    portrait=None,
                )
            )
        ], {}

    async def test_scope_queue_keeps_per_job_receipts_and_real_conversation_context(self):
        await self.chat("我周六想去看看")
        await self.chat("还没决定，刚才只是设想")
        await self.flush()
        self.assertEqual(["completed", "pending"], [row["state"] for row in self.tasks()])
        await self.flush()
        tasks = self.tasks()
        self.assertEqual(2, len(tasks))
        self.assertTrue(all(row["state"] == "completed" for row in tasks))
        self.assertEqual(2, len(self.generated))
        self.assertTrue(all(row["job_ids"] == [row["job_id"]] for row in self.calls))
        self.assertEqual(
            [None, "对方谈到周六的想法，后来仍有待决定。"] * 2,
            [row["output"]["summary"] if row["output"] else None for row in self.calls],
        )
        material = json.loads(self.generated[1][1]["content"])
        self.assertEqual(1, len(material["subject_messages"]))
        self.assertTrue(all(row["purpose"] == "new" for row in material["subject_messages"]))
        self.assertTrue(any(row["purpose"] == "context" for row in material["subject_history"]))
        self.assertNotIn("conversation", material)
        self.assertNotIn("source_hints", material)
        self.assertTrue(all(row["cursor"]["job_ids"] == [row["job_id"]] for row in tasks))
        self.assertNotEqual(tasks[0]["cursor"], tasks[1]["cursor"])

    async def test_lost_commit_ack_restarts_with_receipt_not_new_window_or_model(self):
        await self.chat()
        self.lost_ack = True
        await self.flush()
        original = self.tasks()[0]
        self.assertEqual("pending", original["state"])
        self.assertIsNotNone(original["output"])
        await self.h.core.close()
        self.h.core = self.h.new_core()
        self.h.core.recover()
        self.h.clock.advance(10)
        await self.h.core.memory_learning.work()
        self.assertEqual("completed", self.tasks()[0]["state"])
        self.assertEqual(1, len(self.generated))
        self.assertIsNone(self.calls[-1]["output"])
        self.assertIsNone(self.calls[-1]["window"])
        self.assertEqual(original["window"], self.tasks()[0]["window"])

    async def test_old_frozen_bot_window_survives_unknown_commit_recovery_unchanged(self):
        await self.chat()
        await self.h.core.flush_outbox()
        item = self.tasks()[0]
        turn = self.h.core.store.get("turns", item["turn_id"])
        self.assertFalse(turn.get("profile_checks"))
        turn.pop("reply_context_provenance", None)  # Frozen before dispatch journals existed.
        turn["memory_context"] = dict(
            bundle=dict(group_dependencies=[], episode_dependencies=[], source_dependencies=[])
        )
        self.h.core.store.put("turns", turn)
        frozen = legacy_window(self.h, turn)
        self.assertTrue(
            any(member["role"] == "assistant" for member in frozen["manifest"]["members"])
        )
        # The real capture boundary persists these together. A retained window
        # without its job_ids is not a valid captured task.
        item.update(job_ids=[item["job_id"]], window=copy.deepcopy(frozen))
        self.h.core.store.put("metadata", item)
        self.lost_ack = True
        await self.h.core.memory_learning.work()
        retained = self.tasks()[0]
        self.assertEqual("pending", retained["state"])
        self.assertIsNotNone(retained["output"])
        self.assertEqual(
            ("result_unknown", "commit"),
            (retained["first_failure"]["code"], retained["first_failure"]["stage"]),
        )
        self.assertEqual(1, len(self.generated))
        self.assertEqual(frozen, retained["window"])
        await self.h.core.close()
        self.h.core = self.h.new_core()
        self.h.core.recover()
        self.h.clock.advance(10)
        await self.h.core.memory_learning.work()
        final = self.tasks()[0]
        self.assertEqual("completed", final["state"])
        self.assertEqual(frozen, final["window"])
        self.assertEqual(1, len(self.generated))
        self.assertIsNone(self.calls[-1]["window"])
        self.assertIsNone(self.calls[-1]["output"])

    async def test_uncertain_predecessor_cannot_complete_or_drop_the_next_queued_job(self):
        await self.chat("第一段")
        await self.chat("第二段补充")
        self.lost_ack = True
        await self.flush()
        self.assertEqual(["pending", "pending"], [row["state"] for row in self.tasks()])
        self.assertEqual(1, len(self.generated))
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual(["completed", "pending"], [row["state"] for row in self.tasks()])
        self.assertEqual(1, len(self.generated))
        await self.flush()
        self.assertEqual(["completed", "completed"], [row["state"] for row in self.tasks()])
        self.assertEqual(2, len(self.generated))
        for item in self.tasks():
            self.assertEqual([item["job_id"]], item["receipt"]["covered_job_ids"])

    async def test_optional_portrait_pending_does_not_rollback_saved_experience(self):
        await self.chat()
        self.portrait_pending = True
        await self.flush()
        task = self.tasks()[0]
        self.assertEqual(
            ("completed", "saved", "committed", "pending"),
            (
                task["state"],
                task["experience_state"],
                task["summary_state"],
                task["portrait_state"],
            ),
        )
        status = read_capabilities(self.h.core)["automatic_memory_candidates"][
            "consolidation_receipts"
        ]
        self.assertTrue(status["experience_saved"])
        self.assertTrue(status["portrait_pending"])

    async def test_wrong_model_subject_cannot_submit_another_persons_portrait(self):
        await self.chat()

        async def wrong(turn, material):
            self.generated.append(copy.deepcopy(material))
            return [
                canonical(
                    dict(
                        subject_person_id="person:other",
                        summary="别人的经历",
                        portrait="别人的画像",
                    )
                )
            ], {}

        self.h.gateway.generate = wrong
        await self.flush()
        first = self.tasks()[0]
        self.assertEqual(("proposal", 1), (first["stage"], first["proposal_failures"]))
        self.assertTrue(all(call["output"] is None for call in self.calls))
        frozen = copy.deepcopy(first["window"])
        self.h.gateway.generate = self.generate
        self.h.clock.advance(10)
        await self.flush()
        final = self.tasks()[0]
        self.assertEqual("completed", final["state"])
        self.assertEqual(frozen, final["window"])
        self.assertEqual(2, len(self.generated))
        self.assertEqual(final["scope"]["person_id"], final["receipt"]["subject_person_id"])

    async def test_pending_subject_mismatch_never_reaches_the_model(self):
        await self.chat()

        async def wrong(job, output, **request):
            response = await self.process(job, output, **request)
            response["subject_person_id"] = "person:other"
            return response

        self.h.memory.process_candidate = wrong
        await self.flush()
        self.assertEqual([], self.generated)
        self.assertEqual("pending", self.tasks()[0]["state"])
        self.assertTrue(self.tasks()[0]["capture_uncertain"])
        frozen = copy.deepcopy(self.tasks()[0]["window"])
        self.h.memory.process_candidate = self.process
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual("completed", self.tasks()[0]["state"])
        self.assertEqual(frozen, self.tasks()[0]["window"])
        self.assertEqual(1, len(self.generated))

    async def test_wrong_receipt_subject_keeps_output_for_exact_receipt_recovery(self):
        await self.chat()

        async def wrong(job, output, **request):
            response = await self.process(job, output, **request)
            if response["state"] == "completed":
                response["result"]["subject_person_id"] = "person:other"
            return response

        self.h.memory.process_candidate = wrong
        await self.flush()
        original = self.tasks()[0]
        self.assertEqual("pending", original["state"])
        self.assertIsNotNone(original["output"])
        self.assertEqual(1, len(self.generated))
        self.h.memory.process_candidate = self.process
        self.h.clock.advance(10)
        await self.flush()
        final = self.tasks()[0]
        self.assertEqual("completed", final["state"])
        self.assertEqual(original["window"], final["window"])
        self.assertIsNone(self.calls[-1]["output"])
        self.assertEqual(final["scope"]["person_id"], final["receipt"]["subject_person_id"])
        self.assertEqual(1, len(self.generated))

    async def test_retained_unbound_job_invalidates_without_rewriting_its_proposal(self):
        await self.chat()
        await self.h.core.flush_outbox()
        item = self.tasks()[0]
        learner = self.h.core.memory_learning
        learner._capture_window(item)
        old_output = dict(summary="旧的多人叙述", portrait="旧画像")
        item.update(batch_ref="batch:legacy", output=copy.deepcopy(old_output), stage="commit")
        self.h.core.store.put("metadata", item)
        seen = []

        async def invalidated(job, output, **request):
            seen.append(dict(output=copy.deepcopy(output), **request))
            return dict(state="invalidated", extraction_schema=5, job_ids=[job], result=None)

        self.h.memory.process_candidate = invalidated
        await learner._work(item)
        final = self.tasks()[0]
        self.assertEqual("invalidated", final["state"])
        self.assertEqual(item["window"], final["window"])
        self.assertEqual([None], [call["output"] for call in seen])
        self.assertEqual([None], [call["window"] for call in seen])
        self.assertEqual(dict(summary="旧的多人叙述", portrait="旧画像"), old_output)
        self.assertEqual([], self.generated)

    async def test_receipt_reconcile_before_capture_keeps_the_job_without_a_new_window(self):
        await self.chat()
        await self.h.core.flush_outbox()
        item = self.tasks()[0]
        seen = []

        async def unclaimed(job, output, **request):
            seen.append(dict(output=output, **request))
            return dict(state="unclaimed", extraction_schema=5, job_ids=[job])

        self.h.memory.process_candidate = unclaimed
        response = await self.h.core.memory_learning.reconcile(item["job_id"])
        self.assertEqual("unclaimed", response["state"])
        self.assertIsNone(self.tasks()[0].get("window"))
        self.assertEqual(item["scope"]["person_id"], seen[0]["subject_person_id"])
        self.assertIsNone(seen[0]["window"])
        self.assertIsNone(seen[0]["output"])
        self.assertEqual([], self.generated)

    async def test_empty_summary_is_single_call_without_lexical_rereview(self):
        await self.chat("嗯，知道了")

        async def empty(turn, material):
            if not turn["id"].startswith("memory-generation:"):
                return await self.original_generate(turn, material)
            self.generated.append(material)
            subject = json.loads(material[1]["content"])["subject"]["person_id"]
            return [canonical(dict(subject_person_id=subject, summary="", portrait=None))], {}

        self.h.gateway.generate = empty
        with patch(
            "tianshu_companion.memory_admission.extraction_review_hints",
            side_effect=AssertionError("legacy path"),
        ):
            await self.flush()
        self.assertEqual(1, len(self.generated))
        self.assertEqual(
            ("completed", "saved", "skipped"),
            (
                self.tasks()[0]["state"],
                self.tasks()[0]["experience_state"],
                self.tasks()[0]["summary_state"],
            ),
        )

    async def test_empty_optional_portrait_commits_summary_with_or_without_portrait_input(self):
        basis = None

        async def process(job, output, **request):
            response = await self.process(job, output, **request)
            if response["state"] == "pending":
                response["portrait_input"] = copy.deepcopy(basis)
            return response

        async def generate(turn, material):
            if not turn["id"].startswith("memory-generation:"):
                return await self.original_generate(turn, material)
            self.generated.append(copy.deepcopy(material))
            subject = json.loads(material[1]["content"])["subject"]["person_id"]
            return [
                canonical(
                    dict(subject_person_id=subject, summary="保留这段非空经历。", portrait="")
                )
            ], {}

        self.h.memory.process_candidate = process
        self.h.gateway.generate = generate
        for index, text in enumerate(("我周六想去看看", "后来仍没有决定")):
            with self.subTest(has_portrait_input=basis is not None):
                prior_jobs = set(self.receipts)
                await self.chat(text)
                await self.flush()
                job = next(job for job in self.receipts if job not in prior_jobs)
                task = next(row for row in self.tasks() if row["job_id"] == job)
                self.assertEqual(
                    ("completed", "committed", 0),
                    (task["state"], task["summary_state"], task["proposal_failures"]),
                )
                self.assertEqual(index + 1, len(self.generated))
                material = json.loads(self.generated[-1][1]["content"])
                self.assertEqual(
                    None if basis is None else dict(basis, current_text=""),
                    material["portrait_input"],
                )
                submitted = [
                    call["output"]
                    for call in self.calls
                    if call["job_id"] == job and call["output"] is not None
                ]
                self.assertEqual(1, len(submitted))
                self.assertEqual("保留这段非空经历。", submitted[0]["summary"])
                self.assertIsNone(submitted[0]["portrait"])
                window = self.windows[job]
                human = next(
                    member
                    for member in window["manifest"]["members"]
                    if member["role"] == "user" and member["purpose"] == "new"
                )
                basis = dict(
                    subject_person_id=window["manifest"]["scope"]["person_id"],
                    expected_revision=0,
                    current_text="",
                    summary_refs=task["receipt"]["summary_record_refs"],
                    summaries=[
                        dict(
                            **task["receipt"]["summary_record_refs"][0],
                            subject_person_id=window["manifest"]["scope"]["person_id"],
                            subject_material=[
                                dict(
                                    **{
                                        key: human[key]
                                        for key in ("member_id", "speaker", "observed_at")
                                    },
                                    text=text,
                                )
                            ],
                        )
                    ],
                )

    async def test_parse_failure_then_lock_wait_does_not_spend_another_proposal(self):
        await self.chat()

        async def malformed(turn, material):
            self.generated.append(material)
            return ['{"summary":'], {}

        self.h.gateway.generate = malformed
        await self.flush()
        first = self.tasks()[0]
        self.assertEqual(("proposal", 1), (first["stage"], first["proposal_failures"]))
        calls, attempts = len(self.calls), first["attempts"]
        self.h.clock.advance(10)
        timeout = asyncio.timeout
        learner = self.h.core.memory_learning
        async with learner.lock:
            with patch(
                "tianshu_companion.memory_learning.asyncio.timeout",
                side_effect=lambda seconds: timeout(0.01 if seconds == 45 else seconds),
            ):
                await learner._work(self.tasks()[0])
        final = self.tasks()[0]
        self.assertEqual(1, final["proposal_failures"])
        self.assertEqual(
            (calls, attempts, 1), (len(self.calls), final["attempts"], len(self.generated))
        )
        self.assertEqual("pending", final["state"])
        self.assertEqual("receipt", final["stage"])

    async def test_unclaimed_known_context_change_recaptures_once_before_one_model(self):
        await self.chat("旧内容")
        await self.flush()
        self.completed, self.window = None, None
        await self.chat("新的健康输入")
        original, changed = self.process, []

        async def drop_context(job, output, **request):
            if output is None and not changed:
                window = request["window"]
                invalid = next(
                    row["member_id"]
                    for row in window["manifest"]["members"]
                    if row["purpose"] == "context"
                )
                changed.append(copy.deepcopy(window))
                return dict(
                    state="context_changed",
                    extraction_schema=5,
                    job_ids=request["job_ids"],
                    window_id=window["manifest"]["window_id"],
                    invalid_context_member_ids=[invalid],
                )
            return await original(job, output, **request)

        self.h.memory.process_candidate = drop_context
        await self.flush()
        self.assertEqual(2, len(self.generated))
        item = self.tasks()[-1]
        self.assertEqual("completed", item["state"])
        self.assertTrue(item["context_recaptured"])
        self.assertNotEqual(
            changed[0]["manifest"]["window_id"], item["window"]["manifest"]["window_id"]
        )
        self.assertEqual(
            changed[0]["manifest"]["new_turns"], item["window"]["manifest"]["new_turns"]
        )
        self.assertTrue(
            all(row["purpose"] == "new" for row in item["window"]["manifest"]["members"])
        )

    async def test_unknown_capture_restores_receipt_without_altering_frozen_window(self):
        await self.chat()

        async def unknown(job, output, **request):
            raise Fault("result_unknown", unknown=True)

        self.h.memory.process_candidate = unknown
        await self.flush()
        item = self.tasks()[0]
        self.assertTrue(item["capture_uncertain"])
        frozen = copy.deepcopy(item["window"])
        self.h.memory.process_candidate = self.process
        self.h.clock.advance(10)
        # A terminal receipt is the only successful recovery in this fixture.
        self.completed = dict(
            state="invalidated", extraction_schema=5, job_ids=item["job_ids"], result=None
        )
        await self.flush()
        self.assertIsNone(self.calls[-1]["window"])
        self.assertEqual(frozen, self.tasks()[0]["window"])
        self.assertEqual([], self.generated)

    async def test_unknown_http_capture_before_claim_retries_the_identical_window_after_lookup(
        self,
    ):
        await self.chat()
        requests = []

        async def handler(request):
            body = json.loads(request.content)
            requests.append(copy.deepcopy(body))
            if len(requests) == 1:
                # The real candidate HTTP envelope after business entry can be
                # unknown even when a temporary owner failure prevented claim.
                return httpx.Response(
                    503, json=dict(code="dependency_unavailable", execution_state="unknown")
                )
            if body["output"] is None and body["window"] is None:
                self.assertIsNone(self.window)
                return httpx.Response(
                    200, json=dict(state="unclaimed", extraction_schema=5, job_ids=body["job_ids"])
                )
            result = await self.process(
                body["job_id"],
                body["output"],
                extraction_schema=5,
                job_ids=body["job_ids"],
                window=body.get("window"),
                batch_ref=body.get("batch_ref"),
            )
            return httpx.Response(200, json=result)

        wire = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        self.h.memory.process_candidate = Memory(self.h.contracts, wire).process_candidate
        await self.flush()
        item = self.tasks()[0]
        self.assertTrue(item["capture_uncertain"])
        frozen = copy.deepcopy(item["window"])
        self.assertIsNone(self.window)
        self.assertEqual([], self.generated)
        self.h.clock.advance(10)
        await self.flush()
        self.assertEqual("completed", self.tasks()[0]["state"])
        self.assertEqual(1, len(self.generated))
        self.assertEqual(1, sum(row["output"] is None for row in self.calls))
        self.assertEqual(1, sum(row["output"] is not None for row in self.calls))
        self.assertEqual([frozen, None, frozen, None], [row.get("window") for row in requests])
        self.assertEqual(frozen, self.tasks()[0]["window"])

    async def test_waiting_for_model_rechecks_full_captured_window(self):
        await self.chat()
        entered = asyncio.Event()
        original = self.h.core.summary_windows.check
        count = 0

        def revoke(service, request):
            nonlocal count
            count += 1
            if count == 2:
                turn = self.h.turns()[0]
                turn["cancelled"] = True
                self.h.core.store.put("turns", turn)
                entered.set()
            return original(service, request)

        self.h.core.summary_windows.check = revoke
        await self.flush()
        self.assertTrue(entered.is_set())
        self.assertEqual([], self.generated)
        self.assertFalse(any(row["output"] is not None for row in self.calls))
        self.assertEqual("blocked", self.tasks()[0]["state"])

    async def test_large_new_input_preserves_its_text_and_omits_only_old_context(self):
        await self.chat("甲" * 9000)
        await self.chat("乙" * 9000)
        await self.flush()
        await self.flush()
        self.assertEqual(1, len(self.calls[0]["job_ids"]))
        new_texts = [row["text"] for row in self.window["members"]]
        self.assertEqual(["乙" * 9000], new_texts)
        self.assertFalse(self.window["manifest"]["coverage"]["complete"])
        tasks = self.tasks()
        self.assertEqual(["completed", "completed"], [row["state"] for row in tasks])

    async def test_explicit_reconcile_only_reads_and_projects_real_batch_receipt(self):
        await self.chat()
        self.lost_ack = True
        await self.flush()
        await self.h.core.memory_learning.reconcile(self.tasks()[0]["job_id"])
        self.assertEqual("completed", self.tasks()[0]["state"])
        self.assertEqual(1, len(self.generated))
        self.assertIsNone(self.calls[-1]["output"])

    async def legacy_backfill_receipt(self, locked_schema):
        await self.chat()
        await self.h.core.flush_outbox()
        old = self.tasks()[0]
        self.h.core.store.delete("metadata", old["id"])
        # Simulate a real pre-schema5 database: delivered outbox, no local job
        # metadata and no upgrade boundary. The producer already locked a pin.
        self.h.core.store.delete("metadata", "memory-learning-schema5-boundary")
        await self.h.core.close()
        requests = []

        async def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(
                200,
                json=dict(
                    state="completed",
                    extraction_schema=locked_schema,
                    content=[],
                    result=dict(
                        state="committed", record_ids=["record:old"], group_ids=["group:old"]
                    ),
                ),
            )

        wire = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        self.h.memory.process_candidate = Memory(self.h.contracts, wire).process_candidate
        self.h.core = self.h.new_core()
        self.h.core.recover()
        await self.h.core.memory_learning.work()
        restored = self.tasks()[0]
        self.assertEqual(
            ("completed", locked_schema), (restored["state"], restored["extraction_schema"])
        )
        self.assertEqual(
            [dict(job_id=old["job_id"], output=None, supported_extraction_schemas=[3, 4])], requests
        )
        self.assertEqual([], self.generated)
        await self.chat("升级后新内容")
        await self.h.core.flush_outbox()
        self.assertEqual(5, self.tasks()[-1]["extraction_schema"])
        self.assertEqual(1, len(requests))

    async def test_backfill_missing_metadata_recovers_locked_schema3_receipt(self):
        await self.legacy_backfill_receipt(3)

    async def test_backfill_missing_metadata_recovers_locked_schema4_receipt(self):
        await self.legacy_backfill_receipt(4)


class SummaryProtocolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.contracts = contracts()

    def contextual_response(self, kind):
        fixture = copy.deepcopy(
            next(
                row
                for row in self.contracts.documents["memory-context/v2/examples.json"]
                if row["name"] == "context-positive-multiple-owners"
            )
        )
        response = fixture["query_response"]
        row = response["evidence"][0]
        row.pop("fact")
        row.update(kind=kind, importance="normal", expires_at="2026-11-23T00:00:00Z")
        row["unit"]["statement"] = "这段交流综合了两位发言者的不同安排，仍有待确认。"
        foreign = next(
            dep
            for dep in response["bundle"]["source_dependencies"]
            if dep["owner_scope"] != row["owner_scope"]
        )
        bundle = response["bundle"]
        group = bundle["group_dependencies"][0]
        group["source_dependency_ids"].append(foreign["dependency_id"])
        # There is no separately selected record for the context author's
        # owner; its scope check is needed solely for the retained leaf.
        response["evidence"] = [row]
        bundle.update(
            group_dependencies=[group],
            episode_dependencies=[],
            source_dependencies=[bundle["source_dependencies"][0], foreign],
            evidence_ids=[row["evidence_id"]],
        )
        for result in response["query_results"]:
            result["evidence_ids"] = [row["evidence_id"]]
        for constraint in bundle["selection_constraints"]:
            for target in constraint["owner_resolution"]["targets"]:
                target["witness_group_ids"] = [
                    identity
                    for identity in target["witness_group_ids"]
                    if identity == group["group"]["semantic_group_id"]
                ]
        return fixture

    async def call_contextual_response(self, fixture):
        supplied = fixture["query_response"]

        async def handler(request):
            body = json.loads(request.content)
            response = copy.deepcopy(supplied)
            response["request_id"] = body["query"]["request_id"]
            response["bundle"].update(
                {
                    key: body[key]
                    for key in ("queries", "selection", "include_associated", "budget", "limits")
                }
            )
            response["bundle"]["valid_until"] = utc(time.time() + 60)
            response["bundle"]["evidence_digest"] = digest(response["evidence"])
            response["budget_used"]["bytes"] = len(canonical(response["evidence"]).encode())
            return httpx.Response(200, json=response)

        wire = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        request = fixture["query_request"]
        return await Memory(self.contracts, wire).query_context(
            request["query"]["origin"],
            request["requested_scope"],
            "那次讨论",
            request["budget"],
            queries=request["queries"],
        )

    async def test_contextual_groups_keep_every_author_leaf_in_the_authorized_frame(self):
        for kind in ("summary", "portrait"):
            with self.subTest(kind=kind):
                fixture = self.contextual_response(kind)
                response = await self.call_contextual_response(fixture)
                group = response["bundle"]["group_dependencies"][0]
                source_by_id = {
                    row["dependency_id"]: row for row in response["bundle"]["source_dependencies"]
                }
                owners = {
                    canonical(source_by_id[key]["owner_scope"])
                    for key in group["source_dependency_ids"]
                }
                self.assertEqual(2, len(owners))
                self.assertEqual(
                    fixture["query_response"]["bundle"]["source_dependencies"],
                    response["bundle"]["source_dependencies"],
                )

    async def test_contextual_foreign_leaves_cannot_escape_actual_scope_checks_or_audience(self):
        for change in ("missing_scope", "conversation_id", "actor_id", "audience"):
            with self.subTest(change=change):
                fixture = self.contextual_response("summary")
                bundle = fixture["query_response"]["bundle"]
                source = bundle["source_dependencies"][1]
                if change == "missing_scope":
                    bundle["scope_checks"] = bundle["scope_checks"][:1]
                else:
                    value = "self_private" if change == "audience" else "different:owner"
                    source["owner_scope"][change] = value
                    bundle["scope_checks"][1]["scope"][change] = value
                with self.assertRaises(Fault):
                    await self.call_contextual_response(fixture)

    async def test_typed_group_still_rejects_cross_author_leaf_even_with_authorized_scope(self):
        fixture = self.contextual_response("summary")
        original = next(
            row
            for row in self.contracts.documents["memory-context/v2/examples.json"]
            if row["name"] == "context-positive-multiple-owners"
        )["query_response"]["evidence"][0]
        fixture["query_response"]["evidence"][0] = copy.deepcopy(original)
        with self.assertRaises(Fault) as caught:
            await self.call_contextual_response(fixture)
        self.assertEqual("invalid_input", caught.exception.code)

    def private_contextual_response(self, kind, association):
        fixture = self.contextual_response(kind)
        request, response = fixture["query_request"], fixture["query_response"]
        bundle = response["bundle"]
        for scope in (
            request["requested_scope"],
            response["effective_scope"],
            bundle["effective_scope"],
            response["evidence"][0]["owner_scope"],
            bundle["group_dependencies"][0]["owner_scope"],
            *[row["owner_scope"] for row in bundle["source_dependencies"]],
            *[row["scope"] for row in bundle["scope_checks"]],
        ):
            scope["audience"] = "self_private"
        linked = bundle["scope_checks"][1]
        linked["scope"]["conversation_id"] = "private:linked"
        linked["association_id"] = association
        bundle["source_dependencies"][1]["owner_scope"]["conversation_id"] = "private:linked"
        for key in ("history_snapshot", "input_history_snapshot"):
            bundle[key]["history_checks"] = [
                dict(
                    scope=copy.deepcopy(check["scope"]),
                    history_version=7,
                    association_id=check["association_id"],
                    association_version=check["association_version"],
                )
                for check in bundle["scope_checks"]
            ]
        request["include_associated"] = bundle["include_associated"] = True
        return fixture

    async def test_private_summary_leaf_requires_the_exact_explicit_association(self):
        for association in ("association:linked", None):
            with self.subTest(association=association):
                fixture = self.private_contextual_response("summary", association)
                if association is None:
                    with self.assertRaises(Fault):
                        await self.call_contextual_response(fixture)
                else:
                    actual = await self.call_contextual_response(fixture)
                    self.assertEqual(2, len(actual["bundle"]["source_dependencies"]))

    async def test_associated_derived_row_owner_requires_its_explicit_authorized_scope(self):
        for kind in ("summary", "portrait"):
            for association in ("association:linked", None):
                with self.subTest(kind=kind, association=association):
                    fixture = self.private_contextual_response(kind, association)
                    response = fixture["query_response"]
                    owner = copy.deepcopy(response["bundle"]["scope_checks"][1]["scope"])
                    row = response["evidence"][0]
                    row.update(owner_scope=owner, subject_person_id=owner["person_id"])
                    row["unit"]["subject_person_id"] = owner["person_id"]
                    response["bundle"]["group_dependencies"][0]["owner_scope"] = copy.deepcopy(
                        owner
                    )
                    if association is None:
                        with self.assertRaises(Fault):
                            await self.call_contextual_response(fixture)
                    else:
                        actual = await self.call_contextual_response(fixture)
                        self.assertEqual(owner, actual["evidence"][0]["owner_scope"])
                        self.assertEqual(
                            owner["person_id"], actual["evidence"][0]["subject_person_id"]
                        )

    async def test_default_schema6_and_retained_schema5_output_keep_interpretation_only(self):
        self.assertEqual(6, Memory.extraction_schema)
        self.assertEqual(
            "normal",
            parse(
                self.contracts,
                '{"subject_person_id":"person:a","summary":"记下一段经历","portrait":null}',
                subject_person_id="person:a",
            )["importance"],
        )
        for raw in (
            '{"subject_person_id":"person:a","summary":"x","portrait":null,"owner":"person:fake"}',
            '{"memories":[]}',
            '{"subject_person_id":"person:a","summary":"x","portrait":null,"importance":"forever"}',
            '{"subject_person_id":"person:other","summary":"x","portrait":null}',
            '{"summary":"x","portrait":null}',
        ):
            with self.subTest(raw=raw), self.assertRaises(Fault):
                parse(self.contracts, raw, subject_person_id="person:a")

    async def test_empty_optional_portrait_normalizes_only_the_exact_empty_string(self):
        output = dict(subject_person_id="person:a", summary="非空经历摘要", importance="high")
        for portrait in ("", None, "人物归纳", " "):
            with self.subTest(portrait=portrait):
                expected = dict(output, portrait=None if portrait == "" else portrait)
                self.assertEqual(
                    expected,
                    parse(
                        self.contracts,
                        canonical(dict(output, portrait=portrait)),
                        subject_person_id="person:a",
                    ),
                )
        for value in (
            output,
            [dict(output, portrait="")],
            dict(output, portrait=[]),
            dict(output, portrait={}),
            dict(output, portrait=False),
            dict(output, portrait=0),
            dict(output, portrait="", subject_person_id="person:other"),
        ):
            with self.subTest(invalid=value), self.assertRaises(Fault):
                parse(self.contracts, canonical(value), subject_person_id="person:a")
        with self.assertRaises(Fault) as over_budget:
            parse(
                self.contracts,
                canonical(dict(output, portrait="", summary="字" * 23000)),
                subject_person_id="person:a",
            )
        self.assertEqual("budget_exceeded", over_budget.exception.code)

    async def test_null_batch_lookup_is_read_classification_but_output_is_write(self):
        request = dict(
            job_id="job:one",
            job_ids=["job:one"],
            output=None,
            supported_extraction_schemas=[5],
            window=None,
        )
        path = "/internal/v1/memory/candidates/process"
        self.assertTrue(JsonService._is_query(path, request))
        self.assertFalse(
            JsonService._is_query(path, dict(request, output=dict(summary="x", portrait=None)))
        )

    async def test_ordinary_recall_is_current_and_explicit_history_is_preserved(self):
        scope = dict(
            actor_id="actor:a",
            person_id="person:a",
            audience="self_private",
            conversation_id="conversation:a",
        )
        request = query_request({}, scope, "那次经历", dict(tokens=1000, bytes=1000))
        self.assertEqual("current", request["queries"][0]["intent"])
        history = [
            dict(
                query_id="past",
                text="以前呢",
                kinds=["facts", "episodes"],
                intent="history",
                time_filter=None,
            )
        ]
        self.assertEqual(
            history, query_request({}, scope, "以前呢", {}, queries=history)["queries"]
        )

    async def test_authorized_interpretation_is_not_discarded_for_unresolved_name(self):
        row = dict(
            evidence_id="evidence:summary",
            kind="summary",
            subject_person_id="person:a",
            observed_at="2026-10-24T00:00:00Z",
            valid_from=None,
            valid_to=None,
            importance="normal",
            expires_at="2026-11-23T00:00:00Z",
            unit=dict(record_id="record:a", statement="对方前后讨论了出游安排，仍未确定。"),
        )
        result = dict(query_id="one", status="unresolved")
        material = prompt_material(dict(evidence=[row], query_results=[result]))
        self.assertEqual(row["unit"]["statement"], material["memory_state"][0]["interpretation"])
        self.assertEqual("person:a", material["memory_state"][0]["subject_person_id"])
        self.assertEqual([result], material["memory_coverage"])

    async def test_client_checks_exact_batch_window_and_unknown_write_response(self):
        pending = dict(
            state="pending",
            extraction_schema=5,
            batch_ref="batch:one",
            job_ids=["job:one"],
            window_id="a" * 64,
            subject_person_id="person:a",
            portrait_input=None,
        )
        supplied = pending

        async def handler(request):
            return httpx.Response(200, json=supplied)

        wire = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        memory = Memory(self.contracts, wire)
        actual = await memory.process_candidate(
            "job:one",
            None,
            extraction_schema=5,
            job_ids=["job:one"],
            window=None,
            subject_person_id="person:a",
        )
        self.assertEqual(pending, actual)
        basis = dict(
            subject_person_id="person:a",
            expected_revision=0,
            current_text="",
            summary_refs=[dict(record_id="record:one", record_version=1)],
            summaries=[
                dict(
                    record_id="record:one",
                    record_version=1,
                    subject_person_id="person:a",
                    subject_material=[
                        dict(
                            member_id="input:owner",
                            speaker=dict(
                                kind="person",
                                person_id="person:a",
                                account=dict(namespace="qq", immutable_account_id="a"),
                            ),
                            observed_at="2026-10-25T00:00:00Z",
                            text="我还没有决定。",
                        )
                    ],
                )
            ],
        )
        for level in ("pending", "portrait", "summary", "member", "refs", "size"):
            with self.subTest(level=level):
                supplied = dict(pending, portrait_input=copy.deepcopy(basis))
                if level == "pending":
                    supplied["subject_person_id"] = "person:other"
                elif level == "portrait":
                    supplied["portrait_input"]["subject_person_id"] = "person:other"
                elif level == "summary":
                    supplied["portrait_input"]["summaries"][0]["subject_person_id"] = "person:other"
                elif level == "member":
                    supplied["portrait_input"]["summaries"][0]["subject_material"][0]["speaker"][
                        "person_id"
                    ] = "person:other"
                elif level == "refs":
                    supplied["portrait_input"]["summary_refs"][0]["record_version"] = 2
                else:
                    raw = supplied["portrait_input"]["summaries"][0]["subject_material"][0]
                    raw["text"] = "甲" * 3001
                    supplied["portrait_input"]["summaries"][0]["subject_material"].append(
                        dict(raw, member_id="input:more")
                    )
                with self.assertRaises(Fault) as caught:
                    await memory.process_candidate(
                        "job:one",
                        None,
                        extraction_schema=5,
                        job_ids=["job:one"],
                        window=None,
                        subject_person_id="person:a",
                    )
                self.assertFalse(caught.exception.unknown)
        supplied = dict(pending, job_ids=["job:other"])
        with self.assertRaises(Fault) as caught:
            await memory.process_candidate(
                "job:one", None, extraction_schema=5, job_ids=["job:one"], window=None
            )
        self.assertFalse(caught.exception.unknown)
        supplied = {}
        with self.assertRaises(Fault) as caught:
            await memory.process_candidate(
                "job:one",
                dict(summary="x", portrait=None),
                extraction_schema=5,
                job_ids=["job:one"],
                batch_ref="batch:one",
            )
        self.assertTrue(caught.exception.unknown)

    async def test_client_replays_a_legacy_proposal_without_adding_a_subject_to_it(self):
        output = dict(summary="保留的旧经历", portrait=None, importance="normal")
        original = copy.deepcopy(output)
        seen = []
        result = dict(
            state="committed",
            subject_person_id="person:a",
            record_ids=["record:summary"],
            group_ids=["group:summary"],
            experience_state="saved",
            summary_state="committed",
            summary_record_refs=[dict(record_id="record:summary", record_version=1)],
            portrait=dict(state="not_requested", record_refs=[]),
            covered_job_ids=["job:one"],
            cursor=dict(window_id="a" * 64, job_ids=["job:one"]),
        )

        async def handler(request):
            seen.append(json.loads(request.content))
            return httpx.Response(
                200,
                json=dict(
                    state="completed",
                    extraction_schema=5,
                    batch_ref="batch:legacy",
                    job_ids=["job:one"],
                    result=result,
                ),
            )

        wire = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        memory = Memory(self.contracts, wire)
        receipt = await memory.process_candidate(
            "job:one",
            output,
            extraction_schema=5,
            job_ids=["job:one"],
            batch_ref="batch:legacy",
            subject_person_id="person:a",
        )
        self.assertEqual("completed", receipt["state"])
        self.assertEqual(original, seen[0]["output"])
        self.assertEqual(original, output)
        self.assertNotIn("subject_person_id", seen[0])
        result["subject_person_id"] = "person:other"
        for supplied_output in (output, None):
            with self.subTest(write=supplied_output is not None):
                with self.assertRaises(Fault) as caught:
                    await memory.process_candidate(
                        "job:one",
                        supplied_output,
                        extraction_schema=5,
                        job_ids=["job:one"],
                        batch_ref="batch:legacy",
                        subject_person_id="person:a",
                    )
                self.assertEqual(supplied_output is not None, caught.exception.unknown)

    async def test_unclaimed_is_only_a_bound_receipt_lookup_not_a_new_capture_response(self):
        supplied = dict(state="unclaimed", extraction_schema=5, job_ids=["job:one"])

        async def handler(request):
            return httpx.Response(200, json=supplied)

        wire = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        memory = Memory(self.contracts, wire)
        response = await memory.process_candidate(
            "job:one", None, extraction_schema=5, job_ids=["job:one"], window=None
        )
        self.assertEqual("unclaimed", response["state"])
        supplied["job_ids"] = ["job:other"]
        with self.assertRaises(Fault):
            await memory.process_candidate(
                "job:one", None, extraction_schema=5, job_ids=["job:one"], window=None
            )
        supplied["job_ids"] = ["job:one"]
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        await h.ingest(text="新的输入")
        await h.cycles()
        window = h.core.summary_windows.read([h.turns()[-1]["id"]])
        for values in (dict(window=window), dict(batch_ref="batch:claimed")):
            with self.subTest(fields=list(values)), self.assertRaises(Fault):
                await memory.process_candidate(
                    "job:one", None, extraction_schema=5, job_ids=["job:one"], **values
                )

    async def test_context_changed_is_bound_to_exact_optional_members_not_generic_409(self):
        h = Harness(silence_ms=0)
        self.addAsyncCleanup(h.core.close)
        for text in ("旧上下文", "新的输入"):
            await h.ingest(text=text)
            await h.cycles()
        window = h.core.summary_windows.read([h.turns()[-1]["id"]])
        old = next(
            row["member_id"] for row in window["manifest"]["members"] if row["purpose"] == "context"
        )
        new = next(
            row["member_id"] for row in window["manifest"]["members"] if row["purpose"] == "new"
        )
        supplied = dict(
            state="context_changed",
            extraction_schema=5,
            job_ids=["job:one"],
            window_id=window["manifest"]["window_id"],
            invalid_context_member_ids=[old],
        )
        status = 200

        async def handler(request):
            return httpx.Response(status, json=supplied)

        wire = JsonService(
            "https://synthetic.invalid", "synthetic", transport=httpx.MockTransport(handler)
        )
        self.addAsyncCleanup(wire.close)
        memory = Memory(self.contracts, wire)
        actual = await memory.process_candidate(
            "job:one", None, extraction_schema=5, job_ids=["job:one"], window=window
        )
        self.assertEqual("context_changed", actual["state"])
        correct = copy.deepcopy(supplied)
        for values in (
            dict(invalid_context_member_ids=[new]),
            dict(invalid_context_member_ids=["member:unknown"]),
            dict(invalid_context_member_ids=[old, old]),
            dict(window_id="0" * 64),
            dict(job_ids=["job:other"]),
        ):
            with self.subTest(values=values):
                supplied = dict(correct, **values)
                with self.assertRaises(Fault) as caught:
                    await memory.process_candidate(
                        "job:one", None, extraction_schema=5, job_ids=["job:one"], window=window
                    )
                self.assertTrue(caught.exception.unknown)
        supplied, status = dict(code="version_conflict", execution_state="not_started"), 409
        with self.assertRaises(Fault) as caught:
            await memory.process_candidate(
                "job:one", None, extraction_schema=5, job_ids=["job:one"], window=window
            )
        self.assertEqual("version_conflict", caught.exception.code)
