"""Conversation sealing on the real Core queue; Memory authority is a test double.

The joint suite separately exercises the real source/window owners and atomic
Memory ledger. These tests cover Core timing, durable recovery and exact binding.
"""

import asyncio
import copy
import hashlib
import json
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import httpx
from jsonschema import Draft202012Validator

from support import Harness, contracts
from tianshu_companion.app import create_app
from tianshu_companion.clients import JsonService, Memory
from tianshu_companion.contracts import Fault, canonical, digest
from tianshu_companion.memory_extraction import ProposalStructureError
from tianshu_companion.memory_learning import QUEUE
from tianshu_companion.memory_summaries import (
    check_segment_output,
    check_segment_receipt,
    check_segment_window,
    parse_segment,
    segment_messages,
)


def member(identity, person, *, frame=None, text="原话", role="human"):
    frame = frame or dict(actor_id="actor:a", audience="group", conversation_id="conv:test")
    return dict(
        member_id=identity,
        kind="core",
        reality="real",
        role=role,
        speaker=(
            dict(
                kind="person",
                person_id=person,
                account=dict(namespace="qq", immutable_account_id=person),
            )
            if role == "human"
            else dict(kind="actor", actor_id=frame["actor_id"])
        ),
        source_scope=dict(frame, person_id=person),
        binding_version=1,
        observed_at="2026-10-25T00:00:00Z",
        text=text,
        content_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        reply_refs=[],
        mentioned_accounts=[],
    )


def window_for(segment_id, frame, members, new_ids=None):
    new_ids = new_ids or [row["member_id"] for row in members if row["role"] == "human"]
    targets = {}
    for identity in new_ids:
        row = next(row for row in members if row["member_id"] == identity)
        person = row["source_scope"]["person_id"]
        target = targets.setdefault(
            person,
            dict(
                scope=copy.deepcopy(row["source_scope"]),
                new_member_ids=[],
                portrait_input=None,
                known_memories=[],
            ),
        )
        target["new_member_ids"].append(identity)
    window = dict(
        segment_id=segment_id,
        frame=copy.deepcopy(frame),
        members=copy.deepcopy(members),
        targets=list(targets.values()),
        coverage=dict(complete_new=True, omitted_context=False),
    )
    window["window_id"] = digest(window)
    return window


def output_for(window):
    by_id = {row["member_id"]: row for row in window["members"]}
    return dict(
        extraction_schema=6,
        topics=[],
        results=[
            dict(
                subject_person_id=target["scope"]["person_id"],
                covered_member_ids=copy.deepcopy(target["new_member_ids"]),
                memories=[
                    dict(
                        text="\n".join(by_id[key]["text"] for key in target["new_member_ids"]),
                        kind="experience",
                        state="described",
                        valid_until=None,
                        source_member_ids=copy.deepcopy(target["new_member_ids"]),
                        prior_refs=[],
                        operation="append",
                        importance="normal",
                        retention="medium",
                    )
                ],
                portrait=None,
            )
            for target in window["targets"]
        ],
    )


def receipt_for(window, output, job="job:test", batch="batch:test"):
    targets = {target["scope"]["person_id"]: target for target in window["targets"]}
    return dict(
        state="completed",
        extraction_schema=6,
        job_id=job,
        batch_ref=batch,
        segment_id=window["segment_id"],
        topics=copy.deepcopy(output["topics"]),
        receipt_cursor=1,
        results=[
            dict(
                subject_person_id=result["subject_person_id"],
                scope=copy.deepcopy(targets[result["subject_person_id"]]["scope"]),
                covered_member_ids=copy.deepcopy(result["covered_member_ids"]),
                memory_refs=[
                    dict(record_id=f"record:{result['subject_person_id']}:{i}", record_version=1)
                    for i, _ in enumerate(result["memories"])
                ],
                portrait_state="not_requested",
                portrait_refs=[],
            )
            for result in output["results"]
        ],
    )


class SegmentPeer:
    """Synthetic Memory peer, with distinct material IDs and one ledger per segment."""

    def __init__(self, h):
        self.h = h
        self.materials, self.sealed, self.receipts = {}, {}, {}
        self.accepted, self.creates, self.processed, self.generated = [], [], [], []
        self.lose_create = self.lose_commit = False
        self.invalidated = False
        h.memory.extraction_schema = 6
        h.memory.accept_materials = self.accept
        h.memory.create_segment = self.create
        h.memory.process_candidate = self.process
        self.original_generate = h.gateway.generate
        h.gateway.generate = self.generate

    async def accept(self, request):
        self.accepted.append(copy.deepcopy(request))
        if "observation_refs" in request:
            descriptors = [self.materials[key][0] for key in request["observation_refs"]]
        else:
            event = request["event"]
            turn = self.h.core.store.get("turns", event["aggregate_id"])
            frame = {
                key: event["scope"][key] for key in ("actor_id", "audience", "conversation_id")
            }
            descriptors = []
            for message in turn["bundle"]["messages"]:
                identity = "material:" + digest(message["message_key"])
                text = "\n".join(
                    part["text"] for part in message["parts"] if part["kind"] == "text"
                )
                row = member(identity, event["scope"]["person_id"], frame=frame, text=text)
                row.update(
                    speaker=dict(
                        kind="person",
                        person_id=event["scope"]["person_id"],
                        account=message["author"],
                    ),
                    observed_at=message["sent_at"],
                    reply_refs=copy.deepcopy(message["reply_refs"]),
                )
                descriptor = dict(
                    material_ref=identity,
                    frame=frame,
                    person_id=event["scope"]["person_id"],
                    text_bytes=len(text.encode("utf-8")),
                    member_count=1,
                    source_key=digest(message["message_key"]),
                )
                self.materials[identity] = (descriptor, row)
                descriptors.append(descriptor)
        return dict(schema_version=1, candidate_job_ref=None, materials=copy.deepcopy(descriptors))

    async def create(self, request):
        self.creates.append(copy.deepcopy(request))
        key = request["segment_id"]
        if key in self.sealed:
            assert self.sealed[key][0] == request, "An unknown creation changed its frozen input"
        else:
            members = [self.materials[identity][1] for identity in request["material_refs"]]
            window = window_for(key, request["frame"], members, request["material_refs"])
            window["coverage"]["omitted_context"] = request["omitted_context"]
            window["window_id"] = digest({k: v for k, v in window.items() if k != "window_id"})
            self.sealed[key] = (copy.deepcopy(request), window)
        if self.lose_create:
            self.lose_create = False
            raise Fault("result_unknown", unknown=True)
        return dict(
            schema_version=1,
            segment_id=key,
            job_id="job:" + key,
            state="completed" if key in self.receipts else "pending",
            candidate_job_ref="job:" + key,
        )

    async def process(self, job_id, output, *, extraction_schema, batch_ref=None):
        assert extraction_schema == 6
        self.processed.append((job_id, copy.deepcopy(output), batch_ref))
        segment_id = job_id.removeprefix("job:")
        window = self.sealed[segment_id][1]
        batch = "batch:" + segment_id
        if self.invalidated:
            return dict(
                state="invalidated",
                extraction_schema=6,
                job_id=job_id,
                batch_ref=batch_ref,
                result=None,
            )
        if segment_id in self.receipts:
            return copy.deepcopy(self.receipts[segment_id])
        if output is None:
            return dict(
                state="pending",
                extraction_schema=6,
                job_id=job_id,
                batch_ref=batch,
                segment_window=copy.deepcopy(window),
            )
        check_segment_output(self.h.contracts, output, window)
        receipt = receipt_for(window, output, job_id, batch)
        self.receipts[segment_id] = receipt
        if self.lose_commit:
            self.lose_commit = False
            raise Fault("result_unknown", unknown=True)
        return copy.deepcopy(receipt)

    async def generate(self, turn, messages):
        if not turn["id"].startswith("memory-generation:"):
            return await self.original_generate(turn, messages)
        supplied = json.loads(messages[-1]["content"])
        self.generated.append(copy.deepcopy(supplied))
        return [canonical(output_for(supplied["segment_window"]))], {}


class SegmentLearningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.h = Harness(Path(self.temp.name) / "segments.sqlite", silence_ms=0)
        self.peer = SegmentPeer(self.h)

    async def asyncTearDown(self):
        await self.h.core.close()

    def tasks(self):
        return [
            row
            for row in self.h.core.store.list("metadata", QUEUE)
            if row.get("extraction_schema") == 6
        ]

    async def chat(self, text="原话", account="a", **kwargs):
        await self.h.ingest(
            text=text, account=account, group=True, channel="group:segments", **kwargs
        )
        await self.h.cycles()
        await self.h.core.flush_outbox()
        self.assertEqual("sent", self.h.turns()[-1]["phase"])

    async def drain(self):
        for _ in range(12):
            await self.h.core.memory_learning.work()
            await asyncio.sleep(0)

    async def restart(self):
        await self.h.core.close()
        self.h.core = self.h.new_core()
        self.h.core.recover()

    def offer_revision(self, original):
        # Memory's real revision cases return a distinct material_ref and the
        # same opaque source_key. Exercise that accepted descriptor at the
        # consumer boundary; this double does not attest remote currentness.
        material = dict(original["materials"][0], material_ref="material:revision:2")
        self.h.core.memory_learning._offer_materials(
            dict(
                schema_version=1,
                candidate_job_ref=None,
                materials=[dict(material, frame=original["scope"])],
            )
        )

    async def test_interleaved_people_share_one_sealed_job_and_model_with_exact_coverage(self):
        for person, text in (
            ("a", "我打算周六去"),
            ("b", "我还没决定"),
            ("c", "问你是否有空"),
            ("d", "已经到家"),
        ):
            await self.chat(text, person)
        self.assertEqual(1, len(self.tasks()))
        self.assertEqual("collecting", self.tasks()[0]["state"])
        self.assertEqual([], self.peer.generated)
        self.h.clock.advance(120)
        await self.drain()
        task = self.tasks()[0]
        self.assertEqual("completed", task["state"])
        self.assertEqual(1, len(self.peer.generated))
        self.assertEqual(1, len(self.peer.sealed))
        self.assertEqual(4, len(task["receipt"]["results"]))
        self.assertEqual(
            {row["material_ref"] for row in task["materials"]},
            {key for result in task["receipt"]["results"] for key in result["covered_member_ids"]},
        )
        self.assertEqual(task["output"], output_for(task["segment_window"]))

    async def test_new_speech_extends_quiet_but_duplicate_receipt_does_not(self):
        await self.chat()
        start = self.tasks()[0]
        self.h.clock.advance(100)
        self.h.core.memory_learning.backfill()
        self.assertEqual(start["deadline"], self.tasks()[0]["deadline"])
        await self.chat("后来补充", "b")
        self.assertEqual(self.h.clock() + 120, self.tasks()[0]["deadline"])
        self.h.clock.advance(119)
        await self.drain()
        self.assertEqual([], self.peer.generated)

    async def test_material_revision_seals_old_collecting_input_without_mixing_revisions(self):
        await self.chat("最初的原话", message="same-physical-message")
        first = copy.deepcopy(self.tasks()[0])
        self.h.clock.advance(30)
        self.offer_revision(first)
        old, current = self.tasks()
        previous, revised = old["materials"][0], current["materials"][0]
        self.assertNotEqual(previous["material_ref"], revised["material_ref"])
        self.assertEqual(previous["source_key"], revised["source_key"])
        self.assertEqual("source_revision", old["close_reason"])
        self.assertEqual("pending", old["state"])
        self.assertEqual(first["materials"], old["materials"])
        self.assertEqual([previous["material_ref"]], old["create_request"]["material_refs"])
        self.assertIsNone(old["output"])
        self.assertIsNone(old["receipt"])
        self.assertEqual("collecting", current["state"])
        self.assertEqual(self.h.clock() + 120, current["deadline"])
        self.assertEqual([], self.peer.creates)
        self.assertEqual([], self.peer.generated)

    async def test_material_revision_preserves_old_unknown_window_output_and_receipt_recovery(self):
        await self.chat("原版本", message="same-physical-message")
        self.peer.lose_commit = True
        self.h.clock.advance(120)
        await self.drain()
        frozen = copy.deepcopy(self.tasks()[0])
        self.assertIsNotNone(frozen["output"])
        self.assertIsNone(frozen["receipt"])
        self.offer_revision(frozen)
        self.assertEqual(frozen, self.tasks()[0])
        self.assertEqual("collecting", self.tasks()[1]["state"])
        await self.restart()
        self.h.clock.advance(10)
        await self.drain()
        restored = self.tasks()[0]
        self.assertEqual("completed", restored["state"])
        for key in ("materials", "create_request", "segment_window", "output"):
            self.assertEqual(frozen[key], restored[key])
        self.assertEqual(1, len(self.peer.generated))
        self.assertEqual(1, sum(output is not None for _, output, _ in self.peer.processed))
        self.assertIsNone(self.peer.processed[-1][1])

    async def test_material_revision_after_completion_keeps_old_receipt_and_strict_ref_identity(
        self,
    ):
        await self.chat("原版本", message="same-physical-message")
        self.h.clock.advance(120)
        await self.drain()
        completed = copy.deepcopy(self.tasks()[0])
        self.offer_revision(completed)
        self.assertEqual(completed, self.tasks()[0])
        current = copy.deepcopy(self.tasks()[1])
        descriptor = current["materials"][0]
        self.h.clock.advance(30)
        segments = self.h.core.memory_learning.segments
        self.assertFalse(segments.offer(current["scope"], [descriptor]))
        self.assertEqual(current, self.tasks()[1])
        for field, value in (
            ("text_bytes", descriptor["text_bytes"] + 1),
            ("source_key", "different-source"),
            ("person_id", "other-person"),
        ):
            with self.subTest(field=field), self.assertRaises(Fault) as raised:
                segments.offer(current["scope"], [dict(descriptor, **{field: value})])
            self.assertEqual("idempotency_conflict", raised.exception.code)
        self.assertEqual(current, self.tasks()[1])

    async def test_max_wait_seals_without_requiring_a_quiet_conversation(self):
        await self.chat()
        first = self.tasks()[0]
        for index in range(1, 6):
            self.h.clock.advance(100)
            await self.chat(f"补充{index}")
        self.assertEqual(first["started_at"] + 600, self.tasks()[0]["deadline"])
        self.h.clock.advance(100)
        await self.drain()
        self.assertEqual("completed", self.tasks()[0]["state"])
        self.assertEqual(1, len(self.peer.generated))

    async def test_collecting_window_survives_restart_without_early_model(self):
        await self.chat()
        before = self.tasks()[0]
        await self.restart()
        await self.drain()
        self.assertEqual(before, self.tasks()[0])
        self.h.clock.advance(120)
        await self.drain()
        self.assertEqual("completed", self.tasks()[0]["state"])

    async def test_lost_create_ack_replays_same_segment_before_one_model(self):
        await self.chat()
        self.peer.lose_create = True
        self.h.clock.advance(120)
        await self.drain()
        original = copy.deepcopy(self.tasks()[0]["create_request"])
        self.assertEqual([], self.peer.generated)
        await self.restart()
        self.h.clock.advance(10)
        await self.drain()
        self.assertEqual("completed", self.tasks()[0]["state"])
        self.assertEqual([original, original], self.peer.creates)
        self.assertEqual(1, len(self.peer.generated))

    async def test_lost_commit_ack_restart_uses_receipt_without_replacing_frozen_output(self):
        await self.chat()
        self.peer.lose_commit = True
        self.h.clock.advance(120)
        await self.drain()
        previous = copy.deepcopy(self.tasks()[0])
        self.assertIsNotNone(previous["output"])
        await self.restart()
        self.h.clock.advance(10)
        await self.drain()
        actual = self.tasks()[0]
        self.assertEqual("completed", actual["state"])
        self.assertEqual(previous["output"], actual["output"])
        self.assertEqual(previous["segment_window"], actual["segment_window"])
        self.assertEqual(1, len(self.peer.generated))
        self.assertEqual(1, sum(output is not None for _, output, _ in self.peer.processed))
        self.assertEqual(None, self.peer.processed[-1][1])

    async def test_observation_handoff_is_durable_and_deduplicates_same_accepted_source(self):
        await self.chat()
        original = copy.deepcopy(self.tasks()[0])
        self.h.clock.advance(30)
        identity = self.tasks()[0]["materials"][0]["material_ref"]
        self.peer.materials["observation:duplicate"] = self.peer.materials[identity]
        await self.h.core.memory_learning.observation_archived(
            dict(source_ref="observation:duplicate"), dict(archive_state="archived")
        )
        before = self.tasks()[0]
        await self.restart()
        await self.drain()
        self.assertEqual(1, len(self.tasks()))
        self.assertEqual(original["deadline"], self.tasks()[0]["deadline"])
        self.assertEqual(before["materials"], self.tasks()[0]["materials"])
        self.assertEqual(before["deadline"], self.tasks()[0]["deadline"])
        self.assertEqual(
            "material_accepted",
            self.h.core.store.get(
                "metadata", "memory-observation:" + digest("observation:duplicate")
            )["state"],
        )

    async def test_real_archive_callback_persists_handoff_before_marking_transferred(self):
        from tianshu_companion.observation import Observations

        class Archive:
            async def call(_, endpoint, body, **options):
                return dict(
                    source_ref=body["source_ref"], state="duplicate", archive_state="archived"
                )

        event = dict(
            schema_version=2,
            platform_id="synthetic",
            self_id="42",
            namespace="qq",
            conversation_id="group:7",
            account_id="7",
            event_id="event:passive",
            revision=1,
            sent_at="2026-10-25T00:00:00Z",
            text="只有旁听的发言",
            content_state="text",
            mentioned=False,
            scope_revision=1,
        )
        request = dict(
            event=event,
            source_ref="observation:passive",
            source_digest=digest(event),
            scope_version=1,
            archive_epoch=1,
        )
        attempts = []

        async def handoff(payload, receipt):
            attempts.append(copy.deepcopy(payload))
            if len(attempts) == 1:
                raise Fault("dependency_unavailable")
            await self.h.core.memory_learning.observation_archived(payload, receipt)

        path = Path(self.temp.name) / "observations.sqlite"
        inbox = Observations(path, Archive(), on_archived=handoff)
        self.assertEqual("accepted", inbox.ingest("platform", request)["state"])
        await inbox.flush()
        status = dict(
            instance_id="synthetic",
            self_id="42",
            conversation_id="group:7",
            limit=20,
            cursor=None,
            archive_epoch=1,
        )
        self.assertEqual(
            "pending_memory", inbox.query("platform", status)["items"][0]["archive_state"]
        )
        with closing(inbox._db()) as db, db:
            db.execute("UPDATE inbox SET next_attempt_at=0")
        await inbox.flush()
        self.assertEqual("archived", inbox.query("platform", status)["items"][0]["archive_state"])
        self.assertEqual(request, attempts[1])
        self.assertEqual(
            "material_pending",
            self.h.core.store.get(
                "metadata", "memory-observation:" + digest(request["source_ref"])
            )["state"],
        )

    async def test_invalidated_source_is_terminal_without_claiming_saved(self):
        await self.chat()
        self.peer.invalidated = True
        self.h.clock.advance(120)
        await self.drain()
        task = self.tasks()[0]
        self.assertEqual("invalidated", task["state"])
        self.assertIsNone(task["receipt"])
        self.assertEqual([], self.peer.generated)

    async def assert_preseal_negative_uses_real_client_and_original_request(self, *, lose_ack):
        await self.chat()
        self.h.clock.advance(120)
        calls, persisted = [], {}

        async def handler(request):
            body = json.loads(request.content)
            calls.append((request.url.path, copy.deepcopy(body)))
            if request.url.path == "/internal/v1/memory/candidates/segments":
                if persisted:
                    self.assertEqual(persisted["request"], body)
                else:
                    persisted.update(request=copy.deepcopy(body), job_id="job:negative")
                    if lose_ack:
                        raise httpx.ReadError("Synthetic negative job persisted before lost ACK")
                return httpx.Response(
                    200,
                    json=dict(
                        schema_version=1,
                        segment_id=body["segment_id"],
                        job_id=persisted["job_id"],
                        candidate_job_ref=persisted["job_id"],
                        state="invalidated",
                    ),
                )
            self.assertEqual("/internal/v1/memory/candidates/process", request.url.path)
            self.assertEqual(
                dict(extraction_schema=6, job_id="job:negative", batch_ref=None, output=None),
                body,
            )
            return httpx.Response(
                200,
                json=dict(
                    state="invalidated",
                    extraction_schema=6,
                    job_id="job:negative",
                    batch_ref=None,
                    result=None,
                ),
            )

        client = JsonService(
            "https://memory.synthetic.invalid",
            "companion-only",
            transport=httpx.MockTransport(handler),
        )
        self.addAsyncCleanup(client.close)
        memory = Memory(self.h.contracts, client)
        self.h.memory.create_segment = memory.create_segment
        self.h.memory.process_candidate = memory.process_candidate
        await self.drain()
        if lose_ack:
            uncertain = copy.deepcopy(self.tasks()[0])
            self.assertEqual("pending", uncertain["state"])
            self.assertNotIn("job_id", uncertain)
            self.assertEqual(persisted["request"], uncertain["create_request"])
            self.assertEqual(1, len(calls))
            await self.restart()
            self.h.clock.advance(10)
            await self.drain()
        actual = self.tasks()[0]
        self.assertEqual("invalidated", actual["state"])
        self.assertEqual("job:negative", actual["job_id"])
        self.assertEqual(persisted["request"], actual["create_request"])
        self.assertIsNone(actual["output"])
        self.assertIsNone(actual["receipt"])
        self.assertNotIn("segment_window", actual)
        self.assertNotIn("cursor", actual)
        self.assertEqual([], self.peer.generated)
        self.assertEqual({}, self.peer.receipts)
        self.assertEqual(
            ["/internal/v1/memory/candidates/segments"] * (2 if lose_ack else 1)
            + ["/internal/v1/memory/candidates/process"],
            [path for path, _ in calls],
        )

    async def test_preseal_negative_job_finishes_without_model_or_false_coverage(self):
        await self.assert_preseal_negative_uses_real_client_and_original_request(lose_ack=False)

    async def test_preseal_negative_job_lost_ack_recovers_same_segment_before_receipt_only(self):
        await self.assert_preseal_negative_uses_real_client_and_original_request(lose_ack=True)

    async def test_only_real_segment_creation_and_submission_install_mutation_wait(self):
        await self.chat()
        self.h.clock.advance(120)
        learning = self.h.core.memory_learning
        learning.segments.seal_due()
        item = self.tasks()[0]
        with patch.object(self.h.core, "has_active_dialogue", return_value=True):
            self.assertIsNone(await learning._candidate(item, None))
        self.assertEqual("capture_wait", item["stage"])
        self.assertEqual([], self.peer.creates)
        with patch.object(self.h.core, "has_active_dialogue", return_value=False):
            pending = await learning._candidate(item, None)
        learning._pin_segment_window(item, pending)
        with patch.object(self.h.core, "has_active_dialogue", return_value=True):
            # A fresh complete read is still performed; it does not manufacture
            # captured permission or a mutation barrier merely because of I/O.
            self.assertEqual("pending", (await learning._candidate(item, None))["state"])
            self.assertNotIn(item["stage"], {"capture_wait", "commit_wait"})
            self.assertIsNone(
                await learning._candidate(item, output_for(pending["segment_window"]))
            )
        self.assertEqual("commit_wait", item["stage"])
        self.assertEqual({}, learning.mutation_scopes)
        self.assertEqual({}, self.peer.receipts)

    async def test_role_revocation_does_not_start_model_or_create_job(self):
        await self.chat()
        self.h.core.roles.pop("actor:a")
        self.h.clock.advance(120)
        await self.drain()
        self.assertEqual("blocked", self.tasks()[0]["state"])
        self.assertEqual([], self.peer.creates)
        self.assertEqual([], self.peer.generated)

    async def test_budget_seals_complete_messages_and_context_never_evicts_new_input(self):
        frame = dict(actor_id="actor:a", audience="group", conversation_id="conv:budget")
        segments = self.h.core.memory_learning.segments

        def descriptor(index, size=1, person="person:a"):
            return dict(
                material_ref=f"material:{index}",
                person_id=person,
                text_bytes=size,
                member_count=1,
                source_key=f"source:{index}",
            )

        segments.offer(frame, [descriptor(index) for index in range(33)])
        tasks = self.tasks()
        self.assertEqual([32, 1], [len(task["materials"]) for task in tasks])
        self.assertEqual(["pending", "collecting"], [task["state"] for task in tasks])
        self.assertEqual(32, len(tasks[0]["create_request"]["material_refs"]))
        other = dict(frame, conversation_id="conv:bytes")
        segments.offer(other, [descriptor(100, 48 * 1024), descriptor(101)])
        bytes_tasks = [task for task in self.tasks() if task["scope"] == other]
        self.assertEqual([1, 1], [len(task["materials"]) for task in bytes_tasks])
        self.assertEqual(48 * 1024, bytes_tasks[0]["materials"][0]["text_bytes"])
        people_frame = dict(frame, conversation_id="conv:people")
        segments.offer(people_frame, [descriptor(200 + i, person=f"person:{i}") for i in range(9)])
        self.assertEqual(
            [8, 1],
            [len(task["materials"]) for task in self.tasks() if task["scope"] == people_frame],
        )

    async def test_frame_check_allocates_no_turn_admission_or_reply_binding(self):
        windows = self.h.core.summary_windows
        channel = dict(
            namespace="qq",
            binding_id="binding:readonly",
            channel_conversation_id="group:read",
            thread_id=None,
        )
        request = dict(
            schema_version=1,
            actor_id="actor:a",
            channel=channel,
            delivery_channel=None,
            conversation_id=None,
        )
        before_bindings = copy.deepcopy(self.h.core.bindings)
        response = windows.check("memory", request)
        self.assertEqual([], self.h.turns())
        self.assertEqual(before_bindings, self.h.core.bindings)
        again = windows.check(
            "memory", dict(request, conversation_id=response["frame"]["conversation_id"])
        )
        self.assertEqual(response, again)
        with self.assertRaises(Fault):
            windows.check("platform", request)
        with self.assertRaises(Fault):
            windows.check("memory", dict(request, conversation_id="conv:wrong"))

    async def test_learning_delivery_aliases_share_one_real_counter_and_source_fact_owner(self):
        learning = dict(
            namespace="qq",
            binding_id="binding:learning",
            channel_conversation_id="group:linked",
            thread_id=None,
        )
        delivery = dict(learning, binding_id="qq-group")
        check = dict(
            schema_version=1,
            actor_id="actor:a",
            channel=learning,
            delivery_channel=delivery,
            conversation_id=None,
        )
        response = self.h.core.summary_windows.check("memory", check)
        await self.h.ingest(text="第一句", group=True, channel="group:linked")
        await self.h.cycles()
        first = self.h.turns()[-1]
        self.assertEqual(response["frame"]["conversation_id"], first["conversation_id"])
        store = self.h.core.store
        rows = store.get_many("conversations", [digest(learning), digest(delivery)])
        self.assertEqual({digest(learning), digest(delivery)}, set(rows))
        self.assertEqual(rows[digest(learning)], rows[digest(delivery)])
        self.assertEqual(1, len(store.list("conversations")))
        revision_delivery = dict(delivery, binding_id="binding:delivery-revision-2")
        again = self.h.core.summary_windows.check(
            "memory",
            dict(
                check, conversation_id=first["conversation_id"], delivery_channel=revision_delivery
            ),
        )
        self.assertEqual(response["frame"], again["frame"])
        self.assertEqual(
            rows[digest(delivery)], store.get("conversations", digest(revision_delivery))
        )
        self.assertNotIn(revision_delivery["binding_id"], self.h.core.bindings)
        # The actual delivery source channel is retained; aliasing is not source
        # rewriting or an admission for the readonly learning channel.
        self.assertEqual(delivery, first["bundle"]["collection_key"]["channel"])
        data = first["bundle"]["messages"][0]
        from tianshu_companion.source_sync import selector

        facts = self.h.core.source_facts(
            "memory",
            dict(
                schema_version=1,
                request_id="facts:alias",
                mode="snapshot",
                selectors=[selector(data, "actor:a")],
                turn_ids=[first["id"]],
                include_content=True,
            ),
        )
        self.assertEqual(first["conversation_id"], facts["physicals"][0]["conversation_id"])
        self.assertEqual(delivery, facts["physicals"][0]["content"]["message_key"]["channel"])
        await self.h.ingest(text="第二句", group=True, channel="group:linked")
        await self.h.cycles()
        self.assertEqual([1, 2], [turn["sequence"] for turn in self.h.turns()])

    async def test_two_existing_conversations_are_not_merged_by_frame_discovery(self):
        from tianshu_companion.source_sync import allocate_channel

        learning = dict(
            namespace="qq",
            binding_id="binding:learning",
            channel_conversation_id="group:conflict",
            thread_id=None,
        )
        delivery = dict(learning, binding_id="qq-group")
        for channel in (learning, delivery):
            allocate_channel(self.h.core.store, channel)
        before = copy.deepcopy(self.h.core.store.list("conversations"))
        with self.assertRaises(Fault) as error:
            self.h.core.summary_windows.check(
                "memory",
                dict(
                    schema_version=1,
                    actor_id="actor:a",
                    channel=learning,
                    delivery_channel=delivery,
                    conversation_id=None,
                ),
            )
        self.assertEqual("scope_changed", error.exception.code)
        self.assertEqual(before, self.h.core.store.list("conversations"))

    async def test_full_scope_frames_collect_independently(self):
        segments = self.h.core.memory_learning.segments
        for index, frame in enumerate(
            (
                dict(actor_id="actor:a", audience="group", conversation_id="conv:a"),
                dict(actor_id="actor:b", audience="group", conversation_id="conv:a"),
                dict(actor_id="actor:a", audience="self_private", conversation_id="conv:a"),
                dict(actor_id="actor:a", audience="group", conversation_id="conv:b"),
            )
        ):
            segments.offer(
                frame,
                [
                    dict(
                        material_ref=f"material:{index}",
                        person_id="person:a",
                        text_bytes=3,
                        member_count=1,
                        source_key="same-source-key",
                    )
                ],
            )
        self.assertEqual(4, len(self.tasks()))

    async def test_new_material_after_sealing_belongs_to_next_segment(self):
        await self.chat()
        self.h.clock.advance(120)
        await self.drain()
        previous = copy.deepcopy(self.tasks()[0])
        await self.chat("新的发言")
        self.assertEqual(previous, self.tasks()[0])
        self.assertEqual(["completed", "collecting"], [task["state"] for task in self.tasks()])

    async def test_optional_context_is_whole_window_omitted_before_new_budget_can_overflow(self):
        segments = self.h.core.memory_learning.segments
        frame = dict(actor_id="actor:a", audience="group", conversation_id="conv:context")
        context = dict(
            manifest=dict(window_id="window:optional", coverage=dict(complete=True)),
            members=[dict(member_id=f"context:{index}", text="原话") for index in range(32)],
        )
        segments.offer(
            frame,
            [
                dict(
                    material_ref="material:new",
                    person_id="person:a",
                    text_bytes=3,
                    member_count=1,
                    source_key="source:new",
                )
            ],
            context_windows=[context],
        )
        segments.seal(self.tasks()[0], "deadline")
        sealed = self.tasks()[0]["create_request"]
        self.assertEqual(["material:new"], sealed["material_refs"])
        self.assertEqual([], sealed["context_windows"])
        self.assertTrue(sealed["omitted_context"])

    async def test_learning_frame_http_is_platform_management_only_and_grants_no_reply(self):
        app = create_app(
            self.h.core, {"platform": "synthetic-platform", "memory": "synthetic-memory"}
        )
        self.addCleanup(app.state.log.close)
        channel = dict(
            namespace="qq",
            binding_id="binding:passive",
            channel_conversation_id="group:readonly",
            thread_id=None,
        )
        request = dict(
            schema_version=1,
            actor_id="actor:a",
            channel=channel,
            delivery_channel=None,
            conversation_id=None,
        )
        original = self.h.core._role_allows

        def dialogue_only(actor, capability):
            return False if capability == "memory.write" else original(actor, capability)

        with patch.object(self.h.core, "_role_allows", side_effect=dialogue_only):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                for token, expected in (
                    ("wrong", 401),
                    ("synthetic-memory", 403),
                    ("synthetic-platform", 200),
                ):
                    response = await client.post(
                        "/internal/v1/bot-bindings/learning-frame",
                        json=request,
                        headers={"Authorization": "Bearer " + token},
                    )
                    self.assertEqual(expected, response.status_code)
                memory_response = await client.post(
                    "/internal/v1/memory/consolidation/window/check",
                    json=request,
                    headers={"Authorization": "Bearer synthetic-memory"},
                )
                self.assertEqual(403, memory_response.status_code)
        self.assertNotIn("binding:passive", self.h.core.bindings)
        self.assertEqual([], self.h.turns())

    async def test_segment_context_uses_group_authors_but_legacy_capture_stays_owner_only(self):
        await self.chat("A提出问题", "a")
        first = self.h.turns()[-1]
        await self.chat("B回答问题", "b")
        last = self.h.turns()[-1]
        windows = self.h.core.summary_windows
        legacy = windows.read([last["id"]])
        shared = windows.read([last["id"]], include_dialogue=True)
        self.assertEqual(
            {last["scope"]["person_id"]},
            {row["speaker"]["person_id"] for row in legacy["manifest"]["members"]},
        )
        self.assertEqual(
            {first["scope"]["person_id"], last["scope"]["person_id"]},
            {
                row["speaker"]["person_id"]
                for row in shared["manifest"]["members"]
                if row["role"] == "user"
            },
        )
        checked = windows.check(
            "memory", dict(schema_version=1, request_id="check:shared", manifest=shared["manifest"])
        )
        self.assertEqual("valid", checked["state"])


class SegmentProtocolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.contracts = contracts()
        self.frame = dict(actor_id="actor:a", audience="group", conversation_id="conv:test")
        self.window = window_for(
            "segment:test",
            self.frame,
            [member("material:a", "person:a"), member("material:b", "person:b")],
        )

    async def test_shared_messages_keep_authors_once_and_do_not_mutate_portrait_basis(self):
        target = self.window["targets"][0]
        target["portrait_input"] = dict(
            subject_person_id="person:a",
            expected_revision=0,
            summary_refs=[],
            summaries=[],
            current_text="旧解读",
        )
        self.window["window_id"] = digest(
            {k: v for k, v in self.window.items() if k != "window_id"}
        )
        original = copy.deepcopy(self.window)
        supplied = json.loads(segment_messages(self.contracts, self.window)[-1]["content"])
        self.assertEqual(original, self.window)
        self.assertEqual(
            "", supplied["segment_window"]["targets"][0]["portrait_input"]["current_text"]
        )
        self.assertEqual(self.window["members"], supplied["segment_window"]["members"])
        self.assertNotIn("conversation", supplied)

    async def test_model_output_schema_is_fully_resolved_and_keeps_memory_constraints(self):
        supplied = json.loads(segment_messages(self.contracts, self.window)[-1]["content"])
        schema = supplied["output_schema"]
        self.assertNotIn('"$ref"', canonical(schema))
        memory = schema["properties"]["results"]["items"]["properties"]["memories"]["items"]
        self.assertEqual(
            {
                "text",
                "kind",
                "state",
                "valid_until",
                "source_member_ids",
                "prior_refs",
                "operation",
                "importance",
                "retention",
            },
            set(memory["required"]),
        )
        validator = Draft202012Validator(schema)
        output = output_for(self.window)
        self.assertTrue(validator.is_valid(output))
        for changes in (
            dict(kind="unsupported"),
            dict(state="unsupported"),
            dict(importance="forever"),
            dict(retention="forever"),
            dict(operation="replace", prior_refs=[]),
        ):
            with self.subTest(changes=changes):
                invalid = copy.deepcopy(output)
                invalid["results"][0]["memories"][0].update(changes)
                self.assertFalse(validator.is_valid(invalid))

    async def test_empty_result_is_real_coverage_but_missing_or_duplicate_person_is_not(self):
        output = output_for(self.window)
        output["results"][0]["memories"] = []
        self.assertEqual(output, parse_segment(self.contracts, canonical(output), self.window))
        for transform in (
            lambda value: value["results"].pop(),
            lambda value: value["results"].append(copy.deepcopy(value["results"][0])),
            lambda value: value["results"][0].update(covered_member_ids=["material:b"]),
            lambda value: value["results"][0].update(subject_person_id="person:other"),
        ):
            with self.subTest(transform=transform):
                invalid = copy.deepcopy(output)
                transform(invalid)
                with self.assertRaises(ProposalStructureError):
                    parse_segment(self.contracts, canonical(invalid), self.window)

    async def test_output_references_must_be_provided_and_include_own_new_human(self):
        for change in (
            dict(source_member_ids=["material:b"]),
            dict(source_member_ids=["material:missing"]),
            dict(
                prior_refs=[dict(record_id="record:foreign", record_version=1)], operation="replace"
            ),
            dict(portrait="不允许没有画像输入更新"),
        ):
            with self.subTest(change=change):
                output = output_for(self.window)
                if "portrait" in change:
                    output["results"][0].update(change)
                else:
                    output["results"][0]["memories"][0].update(change)
                with self.assertRaises(ProposalStructureError):
                    parse_segment(self.contracts, canonical(output), self.window)

    async def test_original_reply_reference_shapes_and_reality_reach_shared_model_unchanged(self):
        row = self.window["members"][0]
        row["reality"] = "fictional"
        row["reply_refs"] = [
            dict(
                channel=dict(
                    namespace="qq",
                    binding_id="qq-group",
                    channel_conversation_id="group:original",
                    thread_id=None,
                ),
                message_id="message:referenced",
                revision=1,
            ),
            "native:observation-ref",
        ]
        self.window["window_id"] = digest(
            {key: value for key, value in self.window.items() if key != "window_id"}
        )
        actual = json.loads(segment_messages(self.contracts, self.window)[-1]["content"])
        self.assertEqual(row, actual["segment_window"]["members"][0])
        self.assertIsInstance(actual["segment_window"]["members"][0]["reply_refs"][0], dict)

    async def test_total_memory_budget_applies_to_whole_segment_not_each_person_separately(self):
        rows = [member(f"material:{i}", f"person:{i}") for i in range(5)]
        window = window_for("segment:many", self.frame, rows)
        output = output_for(window)
        for result in output["results"]:
            result["memories"] *= 8
        with self.assertRaises(ProposalStructureError):
            parse_segment(self.contracts, canonical(output), window)

    async def test_client_checks_material_subject_frame_and_creation_request_binding(self):
        calls = []
        frame = self.frame
        descriptor = dict(
            material_ref="material:a",
            frame=frame,
            person_id="person:a",
            text_bytes=3,
            member_count=1,
            source_key="source:a",
        )
        response = dict(schema_version=1, candidate_job_ref=None, materials=[descriptor])

        class Wire:
            async def call(_, path, body, **options):
                calls.append((path, copy.deepcopy(body), options))
                return copy.deepcopy(response)

        # The original event is produced by real Core delivery, rather than a
        # synthetic partial event accepted only by the peer double.
        h = Harness(silence_ms=0)
        try:
            await h.ingest(text="原话")
            await h.cycles()
            event = copy.deepcopy(h.core.store.list("outbox")[0]["event"])
            descriptor["frame"] = {key: event["scope"][key] for key in frame}
            descriptor["person_id"] = event["scope"]["person_id"]
            memory = Memory(self.contracts, Wire())
            request = dict(schema_version=1, event=event, context_windows=[])
            self.assertEqual(response, await memory.accept_materials(request))
            self.assertTrue(calls[-1][2]["uncertain_write"])
            for change in (
                dict(person_id="person:foreign"),
                dict(frame=dict(descriptor["frame"], actor_id="actor:other")),
            ):
                with self.subTest(change=change):
                    original = copy.deepcopy(response)
                    response["materials"][0].update(change)
                    with self.assertRaises(Fault) as failed:
                        await memory.accept_materials(request)
                    self.assertTrue(failed.exception.unknown)
                    response = original
            response = dict(
                schema_version=1,
                segment_id="segment:other",
                job_id="job:test",
                state="pending",
                candidate_job_ref="job:test",
            )
            with self.assertRaises(Fault) as failed:
                await memory.create_segment(
                    dict(
                        schema_version=1,
                        segment_id="segment:test",
                        frame=self.frame,
                        material_refs=["material:a"],
                        context_refs=[],
                        context_windows=[],
                        omitted_context=False,
                    )
                )
            self.assertTrue(failed.exception.unknown)
        finally:
            await h.core.close()

    async def test_window_hash_speaker_scope_and_body_are_all_bound(self):
        for update, rehash in (
            (lambda w: w["members"][0].update(text="被替换"), False),
            (lambda w: w["members"][0]["speaker"].update(person_id="person:b"), True),
            (lambda w: w["members"][0]["source_scope"].update(conversation_id="conv:other"), True),
            (lambda w: w["targets"][0].update(new_member_ids=["material:b"]), True),
        ):
            with self.subTest(rehash=rehash):
                value = copy.deepcopy(self.window)
                update(value)
                if rehash:
                    value["window_id"] = digest(
                        {k: v for k, v in value.items() if k != "window_id"}
                    )
                with self.assertRaises(Fault):
                    check_segment_window(self.contracts, value)

    async def test_receipt_scope_and_complete_coverage_cannot_be_inferred_from_completed_state(
        self,
    ):
        output = output_for(self.window)
        valid = receipt_for(self.window, output)
        self.assertEqual(valid, check_segment_receipt(self.contracts, valid, self.window, output))
        for mutate in (
            lambda r: r["results"].pop(),
            lambda r: r["results"][0]["scope"].update(person_id="person:b"),
            lambda r: r["results"][0].update(covered_member_ids=["material:b"]),
            lambda r: r.update(segment_id="segment:other"),
        ):
            with self.subTest(mutate=mutate):
                response = copy.deepcopy(valid)
                mutate(response)
                with self.assertRaises(Fault):
                    check_segment_receipt(self.contracts, response, self.window, output)

    async def test_client_uses_explicit_schema6_and_retains_uncertain_write_boundary(self):
        calls = []
        response = dict(
            state="pending",
            extraction_schema=6,
            job_id="job:test",
            batch_ref="batch:test",
            segment_window=self.window,
        )

        class Wire:
            async def call(_, path, body, **options):
                calls.append((path, copy.deepcopy(body), options))
                return copy.deepcopy(response)

        memory = Memory(self.contracts, Wire())
        await memory.process_candidate("job:test", None, extraction_schema=6)
        self.assertFalse(calls[-1][2]["uncertain_write"])
        self.assertTrue(JsonService._is_query(calls[-1][0], calls[-1][1]))
        output = output_for(self.window)
        response = receipt_for(self.window, output)
        await memory.process_candidate(
            "job:test", output, extraction_schema=6, batch_ref="batch:test"
        )
        self.assertTrue(calls[-1][2]["uncertain_write"])
        self.assertFalse(JsonService._is_query(calls[-1][0], calls[-1][1]))
        response["batch_ref"] = "batch:other"
        with self.assertRaises(Fault) as failed:
            await memory.process_candidate(
                "job:test", output, extraction_schema=6, batch_ref="batch:test"
            )
        self.assertTrue(failed.exception.unknown)
        with self.assertRaises(Fault):
            await memory.process_candidate("job:test", None, extraction_schema=6.0)
