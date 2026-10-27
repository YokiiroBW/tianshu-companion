"""Actual learning phases pause without becoming permission failure or new work."""

import asyncio
import copy
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from support import Harness
from test_memory_segments import SegmentPeer
from tianshu_companion.clients import JsonService
from tianshu_companion.contracts import canonical
from tianshu_companion.model_selection import ModelSelection


class GroupProcessingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.h = Harness(Path(self.temp.name) / "processing.sqlite", silence_ms=0)
        self.peer = SegmentPeer(self.h)
        self.worker = self.h.core.memory_learning
        self.disabled, self.calls = set(), []

        def handler(request):
            body = json.loads(request.content)
            self.calls.append(copy.deepcopy(body))
            results = []
            for frame in body["frames"]:
                conversation = (
                    frame.get("conversation_id") or frame["channel"]["channel_conversation_id"]
                )
                paused = conversation in self.disabled
                results.append(
                    dict(
                        frame=frame,
                        mode="disabled" if paused else "observe",
                        actor_id=frame["actor_id"],
                        processing_enabled=not paused,
                        policy_version=1,
                        code=None,
                    )
                )
            return httpx.Response(
                200, json=dict(schema_version=1, request_id=body["request_id"], frames=results)
            )

        self.client = JsonService(
            "https://platform.fixture.invalid",
            "fixed-companion",
            transport=httpx.MockTransport(handler),
        )

    async def asyncTearDown(self):
        await self.h.core.close()
        await self.client.close()

    async def chat(self, channel="group:20001"):
        await self.h.ingest(text="当前群本人原话", group=True, channel=channel)
        await self.h.cycles()
        self.assertEqual("sent", self.h.turns()[-1]["phase"])

    def task(self):
        return next(
            row
            for row in self.h.core.store.list("metadata", "memory:learning")
            if row.get("extraction_schema") == 6
        )

    async def prepared(self, model=False):
        await self.chat()
        await self.h.core.flush_outbox()
        self.h.clock.advance(120)
        await self.worker.work()
        await self.worker.work()
        self.assertEqual("model", self.task()["stage"])
        if model:
            await self.worker.work()
            self.assertEqual("commit", self.task()["stage"])

    async def test_disabled_outbox_stays_due_unchanged_and_other_group_advances(self):
        await self.chat()
        await self.chat("group:20002")
        self.h.core.group_processing.client = self.client
        self.disabled.add("group:20001")
        before = [
            copy.deepcopy(row)
            for row in self.h.core.store.list("outbox")
            if row["event"]["scope"]["conversation_id"] == self.h.turns()[0]["conversation_id"]
        ]
        await self.worker.work()
        self.assertEqual(1, len(self.peer.accepted))
        self.assertEqual(
            self.h.turns()[-1]["conversation_id"],
            self.peer.accepted[0]["event"]["scope"]["conversation_id"],
        )
        after = [
            row
            for row in self.h.core.store.list("outbox")
            if row["id"] in {item["id"] for item in before}
        ]
        self.assertEqual(before, after)
        self.assertEqual([], self.peer.generated)

    async def test_paused_commit_wait_does_not_block_foreground_or_consume_attempt_and_resumes_exact_output(
        self,
    ):
        await self.prepared(model=True)
        item = self.task()
        item["stage"] = "commit_wait"
        self.h.core.store.put("metadata", item)
        self.h.core.group_processing.client = self.client
        self.disabled.add("group:20001")
        before, calls = canonical(item), len(self.peer.processed)
        await self.worker.work()
        self.assertFalse(self.worker.admission_blocked(item["scope"]))
        self.assertEqual(before, canonical(self.task()))
        self.assertEqual(calls, len(self.peer.processed))
        self.disabled.clear()
        for _ in range(3):
            await self.worker.work()
            if self.task()["state"] == "completed":
                break
        self.assertEqual("completed", self.task()["state"])
        self.assertEqual(json.loads(before)["output"], self.task()["output"])
        self.assertEqual(1, len(self.peer.generated))

    async def test_selector_await_rechecks_processing_before_fresh_window_or_model(self):
        await self.prepared()
        self.h.core.group_processing.client = self.client
        outer = self

        class Selector:
            async def select(self, request):
                outer.disabled.add("group:20001")
                return ModelSelection(config_version=1, expires_at=outer.h.clock() + 1000)

        self.h.core.default_model_selector = Selector()
        before, attempts = len(self.peer.processed), self.task()["attempts"]
        await self.worker.work()
        self.assertEqual(before, len(self.peer.processed))
        self.assertEqual(attempts, self.task()["attempts"])
        self.assertIsNone(self.task()["output"])
        self.assertEqual([], self.peer.generated)

    async def test_inflight_model_finishes_into_durable_output_then_pauses_without_commit_or_replay(
        self,
    ):
        await self.prepared()
        self.h.core.group_processing.client = self.client
        entered, release = asyncio.Event(), asyncio.Event()
        generate = self.h.gateway.generate

        async def held(turn, messages):
            entered.set()
            await release.wait()
            return await generate(turn, messages)

        self.h.gateway.generate = held
        await self.worker.work(wait=False)
        await asyncio.wait_for(entered.wait(), 2)
        self.disabled.add("group:20001")
        release.set()
        await self.worker.work()
        self.assertEqual("commit", self.task()["stage"])
        output = copy.deepcopy(self.task()["output"])
        calls = len(self.peer.processed)
        await self.worker.work()
        self.assertEqual(calls, len(self.peer.processed))
        self.assertEqual(output, self.task()["output"])
        self.assertEqual(1, len(self.peer.generated))
        self.disabled.clear()
        await self.worker.work()
        self.assertEqual("completed", self.task()["state"])
        self.assertEqual(1, len(self.peer.generated))

    async def test_unknown_commit_stays_exact_during_pause_and_receipt_first_after_restart(self):
        await self.prepared(model=True)
        self.peer.lose_commit = True
        await self.worker.work()
        self.assertIsNotNone(self.task()["output"])
        item = copy.deepcopy(self.task())
        await self.h.core.close()
        self.h.core = self.h.new_core(participation_client=self.client)
        self.h.core.recover()
        self.worker = self.h.core.memory_learning
        self.disabled.add("group:20001")
        self.h.clock.advance(300)
        calls = len(self.peer.processed)
        await self.worker.work()
        self.assertEqual(calls, len(self.peer.processed))
        self.assertEqual(item["output"], self.task()["output"])
        self.assertEqual(item["attempts"], self.task()["attempts"])
        self.disabled.clear()
        await self.worker.work()
        self.assertEqual("completed", self.task()["state"])
        self.assertIsNone(self.peer.processed[-1][1])
        self.assertEqual(1, len(self.peer.generated))

    async def test_old_opaque_segment_without_unique_mapping_preserves_original_request_pending(
        self,
    ):
        frame = dict(actor_id="actor:a", audience="group", conversation_id="conv:unmapped")
        self.worker.segments.offer(
            frame,
            [
                dict(
                    material_ref="material:retained",
                    person_id="person:a",
                    text_bytes=3,
                    member_count=1,
                    source_key="opaque:original",
                )
            ],
        )
        self.h.clock.advance(120)
        self.worker.segments.seal_due()
        item = copy.deepcopy(self.task())
        self.h.core.group_processing.client = self.client
        await self.worker.work()
        self.assertEqual(item, self.task())
        self.assertEqual([], self.peer.creates)
        self.assertEqual([], self.peer.processed)
        self.assertEqual([], self.peer.generated)
        self.assertEqual(1, self.h.core.group_processing.status()["mapping_required_observed"])

    async def test_complete_message_budget_rollover_keeps_native_frame_on_each_segment(self):
        frame = dict(actor_id="actor:a", audience="group", conversation_id="conv:observed")
        physical = dict(
            instance_id="host:one",
            self_id="10001",
            conversation_id="group:20001",
            actor_id="actor:a",
        )
        materials = [
            dict(
                material_ref="material:" + str(index),
                person_id="person:a",
                text_bytes=100,
                member_count=32,
                source_key="opaque:" + str(index),
            )
            for index in range(2)
        ]
        self.worker.segments.offer(frame, materials, processing_frames=[physical])
        rows = self.h.core.store.list("metadata", "memory:learning")
        self.assertEqual(2, len(rows))
        self.assertEqual({"pending", "collecting"}, {row["state"] for row in rows})
        self.assertTrue(all(row["processing_frames"] == [physical] for row in rows))
        self.assertEqual([], self.peer.creates)
        self.assertEqual([], self.peer.processed)

    async def test_reenabled_group_resumes_while_other_group_keeps_finishing_phases(self):
        await self.prepared(model=True)
        original = copy.deepcopy(self.task())
        self.h.core.group_processing.client = self.client
        self.disabled.add("group:20001")
        await self.chat("group:20002")
        await self.worker.work()
        self.assertTrue(self.h.core.group_processing.paused(original))
        self.assertTrue(self.worker.jobs and all(job.done() for job in self.worker.jobs.values()))
        self.disabled.clear()
        for _ in range(6):
            # Keep genuine B outbox work queued, rather than letting the worker
            # become idle and accidentally hide the stale paused hint.
            await self.chat("group:20002")
            await self.worker.work()
            if self.task()["state"] == "completed":
                break
        self.assertEqual("completed", self.task()["state"])
        self.assertEqual(original["output"], self.task()["output"])
        self.assertEqual(1, len(self.peer.generated))
