"""Owner snapshots preserve their facts while batching existing indexed reads."""

import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_source_sync import SourceHarness
from tianshu_companion.clients import epoch, uid, utc
from tianshu_companion.contracts import Fault, digest
from tianshu_companion.source_sync import selector


class SourceFactsBatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = SourceHarness()

    async def asyncTearDown(self):
        await self.h.core.close()

    def query(self, selectors=(), turns=(), *, content=False):
        return dict(
            schema_version=1,
            request_id=uid("facts"),
            mode="snapshot",
            selectors=list(selectors),
            turn_ids=list(turns),
            include_content=content,
        )

    def read(self, request):
        return self.h.core.source_facts("memory", request)

    async def test_head_only_read_is_one_atomic_select_without_writer_transaction(self):
        query = dict(self.query(), mode="head")
        statements = []
        self.h.core.store.db.set_trace_callback(statements.append)
        try:
            with patch.object(
                self.h.core.store, "transaction", side_effect=AssertionError("Writer transaction")
            ):
                response = self.read(query)
        finally:
            self.h.core.store.db.set_trace_callback(None)
        self.assertEqual(response["head"], self.h.core.store.source_head())
        self.assertEqual(["SELECT"], [sql.split()[0] for sql in statements])
        self.assertEqual([], response["turns"])
        self.assertEqual([], response["physicals"])
        self.assertEqual([], response["admissions"])
        with self.assertRaises(Fault):
            self.h.core.source_facts("channel", query)
        with self.assertRaises(Fault):
            self.read(dict(query, unexpected=True))

    async def test_lease_continuation_persists_without_changing_exported_facts_or_head(self):
        await self.h.core.close()
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.h = SourceHarness(Path(temp.name) / "source-lease.sqlite")
        turn = (await self.completed_turns())[0]
        # Persisting a longer lease is required at the real renewal boundary;
        # a healthy conservative deadline now intentionally stays unchanged.
        turn["source_deadline"] = epoch(utc(self.h.clock() + 19))
        self.h.core.store.put("turns", turn)
        query = self.query(turns=[turn["id"]])
        before = self.read(query)
        deadline = turn["source_deadline"] + 600

        class Sender:
            async def expression_context(sender, origin, scope, channel):
                return dict(
                    origin=origin["origin"], scope=scope, channel=channel, expires_at=utc(deadline)
                )

        with patch.object(self.h.core, "expression_sender", return_value=Sender()):
            await self.h.core.continue_delivery_origin(turn)
        after = self.read(query)
        self.assertEqual(before, after)
        self.assertEqual(deadline, self.h.core.store.get("turns", turn["id"])["source_deadline"])
        await self.h.core.close()
        self.h.core = self.h.new_core()
        self.h.core.recover()
        self.assertEqual(before, self.read(query))
        self.assertEqual(deadline, self.h.core.store.get("turns", turn["id"])["source_deadline"])

    async def test_lease_exception_does_not_hide_mixed_transaction_or_other_turn_changes(self):
        turn = (await self.completed_turns())[0]
        store = self.h.core.store
        before = store.source_head()
        with store.transaction():
            turn["source_deadline"] += 100
            turn["bootstrap_until"] += 10
            store.put("turns", turn)
            self.assertEqual(before, store.source_head())
            turn["context_revision"] += 1
            self.h.core._save_turn(turn)
        self.assertEqual(before["sequence"] + 1, store.source_head()["sequence"])
        snapshot = self.read(self.query(turns=[turn["id"]]))
        self.assertEqual(store.source_head(), snapshot["head"])
        self.assertEqual(turn["context_revision"], snapshot["turns"][0]["context_revision"])

    async def test_max_selector_batch_has_bounded_sql_and_preserves_each_actor_and_channel(self):
        h = self.h
        requests = []
        for index in range(128):
            request = h.fanout(
                text=f"合成来源{index}",
                message=f"batch:{index}",
                channel=f"group:batch:{index % 2}",
                group=True,
                targets=["actor:a", "actor:b"],
            )
            await h.submit(request)
            requests.append(request)
        selectors = [
            selector(request["input"], actor)
            for request in reversed(requests)
            for actor in ("actor:b", "actor:a")
        ]
        before = h.core.store.source_head()
        changes = h.core.store.db.total_changes
        statements = []
        h.core.store.db.set_trace_callback(statements.append)
        request = self.query(selectors)
        try:
            response = self.read(request)
        finally:
            h.core.store.db.set_trace_callback(None)
        self.assertEqual(selectors, [row["selector"] for row in response["admissions"]])
        self.assertEqual(
            [chosen["actor_id"] for chosen in selectors],
            [row["scope"]["actor_id"] for row in response["admissions"]],
        )
        self.assertEqual(request["request_id"], response["request_id"])
        self.assertEqual(digest(request), response["request_digest"])
        self.assertEqual(
            [selector(request["input"], "actor:a")["key"] for request in reversed(requests)],
            [row["key"] for row in response["physicals"]],
        )
        self.assertTrue(all(row["content"] is None for row in response["physicals"]))
        self.assertEqual(
            [row["physical_receipt_id"] for row in response["physicals"]],
            [row["physical_receipt_id"] for row in response["admissions"][::2]],
        )
        self.assertEqual(
            [row["physical_receipt_id"] for row in response["admissions"][::2]],
            [row["physical_receipt_id"] for row in response["admissions"][1::2]],
        )
        self.assertEqual(before, response["head"])
        self.assertEqual(before, h.core.store.source_head())
        self.assertEqual(changes, h.core.store.db.total_changes)
        reads = [sql for sql in statements if sql.startswith(("SELECT", "WITH"))]
        self.assertLessEqual(len(reads), 8)
        self.assertFalse(any(sql.startswith(("INSERT", "UPDATE", "DELETE")) for sql in statements))
        print(f"source_facts_batch selectors=256 physicals=128 channels=2 sql_reads={len(reads)}")

    async def test_snapshot_is_fresh_for_edits_retractions_missing_and_content_projection(self):
        h = self.h
        original = h.fanout(text="白色杯子叫晚星", message="cup", targets=["actor:a"])
        await h.submit(original)
        other = h.fanout(text="另一处的来源", channel="private:other", targets=["actor:b"])
        await h.submit(other)
        missing = h.fanout(message="not-admitted", targets=["actor:a"])
        chosen = [
            selector(other["input"], "actor:b"),
            selector(original["input"], "actor:a"),
            selector(missing["input"], "actor:a"),
        ]
        query = self.query(chosen, content=True)
        before = self.read(query)
        self.assertEqual("missing", before["admissions"][2]["state"])
        self.assertEqual("missing", before["physicals"][2]["state"])
        self.assertEqual(original["input"], before["physicals"][1]["content"])

        edit = h.fanout(
            text="白色杯子现在叫暮色", message="cup", revision=2, kind="edit", targets=["actor:a"]
        )
        await h.submit(edit)
        after = self.read(query)
        self.assertEqual(2, after["physicals"][1]["revision"])
        self.assertEqual(edit["input"], after["physicals"][1]["content"])
        self.assertEqual(before["admissions"][::2], after["admissions"][::2])
        self.assertEqual(2, after["admissions"][1]["source"]["message_key"]["revision"])
        self.assertEqual(
            after["physicals"][1]["physical_receipt_id"],
            after["admissions"][1]["physical_receipt_id"],
        )
        self.assertGreater(after["head"]["sequence"], before["head"]["sequence"])
        hidden = self.read(dict(query, include_content=False))
        self.assertIsNone(hidden["physicals"][1]["content"])
        self.assertEqual(after, self.read(query))

        retract = h.fanout(message="cup", revision=3, kind="retract", targets=[], grants=[])
        await h.submit(retract)
        withdrawn = self.read(query)
        self.assertEqual("withdrawn", withdrawn["physicals"][1]["state"])
        self.assertEqual(3, withdrawn["physicals"][1]["revision"])
        self.assertEqual(after["admissions"], withdrawn["admissions"])
        self.assertEqual(before["physicals"][0], withdrawn["physicals"][0])

    async def completed_turns(self):
        for index in range(2):
            await self.h.submit(
                self.h.fanout(
                    text=f"合成已交付来源{index}",
                    channel=f"private:turn:{index}",
                    targets=["actor:a"],
                )
            )
        self.h.clock.advance(5)
        await self.h.cycles(80)
        turns = self.h.turns()
        self.assertEqual(2, len(turns))
        self.assertTrue(all(turn["phase"] == "sent" for turn in turns))
        return turns

    async def test_turn_snapshot_preserves_first_local_event_and_ignores_unrelated_history(self):
        h = self.h
        turns = await self.completed_turns()
        query = self.query(turns=[turns[1]["id"], "turn:missing", turns[0]["id"]])
        initial = self.read(query)
        self.assertEqual(query["turn_ids"], [turn["turn_id"] for turn in initial["turns"]])
        self.assertEqual("missing", initial["turns"][1]["state"])
        original = h.core.store.outboxes_for_turns([turns[0]["id"]])[turns[0]["id"]][0]
        self.assertEqual(original["event"], initial["turns"][2]["committed_event"])

        def measured_read():
            steps = 0

            def progress():
                nonlocal steps
                steps += 1
                return 0

            h.core.store.db.set_progress_handler(progress, 1)
            try:
                response = self.read(query)
            finally:
                h.core.store.db.set_progress_handler(None, 0)
            return response, steps

        _, before_steps = measured_read()
        with h.core.store.transaction():
            for index in range(2000):
                row = copy.deepcopy(original)
                row.update(id=f"event:unrelated:{index}", sequence=100 + index)
                row["event"].update(event_id=row["id"], aggregate_id=f"turn:unrelated:{index}")
                h.core.store.put("outbox", row)
            foreign = copy.deepcopy(original)
            foreign.update(
                id="event:foreign", conversation_id=turns[1]["conversation_id"], sequence=0
            )
            h.core.store.put("outbox", foreign)
            later = copy.deepcopy(original)
            later.update(id="event:later", sequence=9999)
            later["event"]["event_id"] = later["id"]
            h.core.store.put("outbox", later)
        current, after_steps = measured_read()
        self.assertEqual(initial["turns"], current["turns"])
        self.assertEqual(initial["admissions"], current["admissions"])
        self.assertGreater(current["head"]["sequence"], initial["head"]["sequence"])
        self.assertLessEqual(after_steps, before_steps + 150)
        print(
            f"source_facts_turns unrelated=2000 vm_steps_before={before_steps} after={after_steps}"
        )

    async def test_first_event_without_scope_or_reality_does_not_fall_through_to_later_event(self):
        h = self.h
        turns = await self.completed_turns()
        original = h.core.store.outboxes_for_turns([turns[0]["id"]])[turns[0]["id"]][0]
        query = self.query(turns=[turns[0]["id"]])
        for field in ("scope_version", "reality"):
            with self.subTest(field=field):
                first = copy.deepcopy(original)
                first.update(id="event:first", sequence=0)
                first["event"].update(event_id=first["id"], **{field: None})
                h.core.store.put("outbox", first)
                self.assertIsNone(self.read(query)["turns"][0]["committed_event"])
        self.assertEqual(
            original["event"]["sources"], self.read(query)["turns"][0]["input_sources"]
        )

    async def test_quarantined_selector_or_turn_rejects_the_whole_owner_snapshot(self):
        h = self.h
        turns = await self.completed_turns()
        turn = turns[1]
        channel = turn["bundle"]["collection_key"]["channel"]
        conversation = h.core.store.get("conversations", digest(channel))
        conversation["source_quarantined"] = True
        h.core.store.put("conversations", conversation)
        requests = [
            self.query(turns=[turns[0]["id"], turn["id"]]),
            self.query([selector(turn["bundle"]["messages"][0], turn["scope"]["actor_id"])]),
        ]
        for request in requests:
            with self.subTest(request=request), self.assertRaises(Fault) as error:
                self.read(request)
            self.assertEqual("dependency_unavailable", error.exception.code)
        with self.assertRaises(Fault) as error:
            h.core.source_facts("nonebot", self.query())
        self.assertEqual("forbidden", error.exception.code)
