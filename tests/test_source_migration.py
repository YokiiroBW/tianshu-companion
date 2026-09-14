"""Offline synthetic v1 databases; no product/account databases are used."""

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from test_source_sync import SourceHarness
from tianshu_companion.contracts import Fault, canonical, digest
from tianshu_companion.store import Store


def downgrade_fixture(path, *, mixed=False, unknown=False):
    """Keep actual prior table/record shapes after deleting only v2 fixture fields."""
    with closing(sqlite3.connect(path)) as db, db:
        rows = [json.loads(row[0]) for row in db.execute("SELECT body FROM inbox")]
        db.execute("DELETE FROM inbox")
        for index, row in enumerate(rows):
            for key in ("actor_id", "admission", "authorization"):
                row.pop(key, None)
            row["id"] = digest(row["request"]["message_key"])
            if mixed and index == 0:
                row["request"]["target_actor_ids"] = ["actor:b"]
            if unknown:
                row["request"]["target_actor_ids"] = []
            db.execute(
                "INSERT INTO inbox VALUES(?,?,?,?,?,?)",
                (row["id"], row["conversation_id"], row["sequence"], None, None, canonical(row)),
            )
        for table in ("physicals", "admissions", "metadata"):
            db.execute(f"DROP TABLE {table}")
        db.execute("PRAGMA user_version=1")


class MigrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_migration_rolls_back_and_releases_owner_for_safe_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "old.db"
            h = SourceHarness(path)
            request = h.request()
            await h.core.ingest("nonebot", request)
            await h.core.close()
            downgrade_fixture(path)
            put = Store.put

            def fail_admission(store, table, item):
                if table == "admissions":
                    raise OSError("injected storage failure")
                return put(store, table, item)

            with patch.object(Store, "put", fail_admission), self.assertRaises(OSError):
                h.new_core()
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(0, db.execute("SELECT count(*) FROM physicals").fetchone()[0])
                self.assertEqual(0, db.execute("SELECT count(*) FROM admissions").fetchone()[0])
                self.assertIsNone(
                    db.execute("SELECT body FROM metadata WHERE id='source_migration'").fetchone()
                )
            h.core = h.new_core()
            try:
                self.assertEqual(
                    "actor:a", h.facts([dict(input=request)])["admissions"][0]["scope"]["actor_id"]
                )
            finally:
                await h.core.close()

    async def test_missing_v2_head_requires_recovery_and_does_not_silently_reset(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.db"
            h = SourceHarness(path)
            await h.submit(h.fanout())
            await h.core.close()
            with closing(sqlite3.connect(path)) as db, db:
                db.execute("DELETE FROM metadata WHERE id='source_head'")
            with self.assertRaisesRegex(RuntimeError, "trusted recovery required"):
                h.new_core()
            with self.assertRaisesRegex(RuntimeError, "trusted recovery required"):
                h.new_core()  # Failed startup did not leave the owner file locked.

    async def test_proven_actor_preserves_receipt_b_is_missing_and_backup_is_old_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "old.db"
            h = SourceHarness(path)
            request = h.request(message="legacy:p")
            receipt = await h.core.ingest("nonebot", request)
            await h.core.close()
            downgrade_fixture(path)
            h.core = h.new_core()
            try:
                facts = h.facts([dict(input=request)])
                self.assertEqual(
                    receipt["receipt_id"], facts["admissions"][0]["source"]["receipt_id"]
                )
                self.assertEqual("actor:a", facts["admissions"][0]["scope"]["actor_id"])
                self.assertEqual("missing", facts["admissions"][1]["state"])
                self.assertEqual("unclassified", facts["physicals"][0]["classification"]["value"])
                self.assertNotEqual(
                    receipt["receipt_id"], facts["physicals"][0]["physical_receipt_id"]
                )
                backups = list(Path(directory).glob("*.bak"))
                self.assertEqual(1, len(backups))
                with closing(sqlite3.connect(backups[0])) as backup:
                    self.assertEqual(1, backup.execute("PRAGMA user_version").fetchone()[0])
                    self.assertEqual(1, backup.execute("SELECT count(*) FROM inbox").fetchone()[0])
                head = h.facts(head=True)["head"]
                await h.core.close()
                h.core = h.new_core()
                self.assertEqual(head, h.facts(head=True)["head"])
                self.assertEqual(backups, list(Path(directory).glob("*.bak")))
            finally:
                await h.core.close()

    async def test_ambiguous_original_targets_and_mixed_group_quarantined_not_guessed(self):
        for flag in ("mixed", "unknown"):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "old.db"
                h = SourceHarness(path)
                request = h.request()
                await h.core.ingest("nonebot", request)
                await h.core.close()
                downgrade_fixture(path, **{flag: True})
                h.core = h.new_core()
                try:
                    self.assertFalse(h.core.store.list("admissions"))
                    self.assertEqual(1, len(h.core.store.list("inbox")))
                    with self.assertRaises(Fault) as error:
                        h.facts([dict(input=request)])
                    self.assertEqual("dependency_unavailable", error.exception.code)
                    with self.assertRaises(Fault) as error:
                        await h.core.ingest("nonebot", request)
                    self.assertEqual(503, error.exception.status)
                    h.clock.advance(10)
                    h.core.recover()
                    await h.cycles()
                    self.assertFalse(h.gateway.calls)
                    self.assertFalse(h.memory.commits)
                finally:
                    await h.core.close()

    async def test_migration_retract_is_physical_only_and_cannot_revive(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "old.db"
            h = SourceHarness(path)
            request = h.request(message="old:p")
            await h.core.ingest("nonebot", request)
            row = h.core.store.list("inbox")[0]
            # An old control row used the same inbox shape and collection as its input.
            control = h.request(message="old:p", revision=2, kind="retract")
            row.update(id=digest(control["message_key"]), request=control, revision=2, sequence=2)
            row["receipt"]["receipt_id"] = "legacy:control"
            row["source"].update(message_key=control["message_key"], receipt_id="legacy:control")
            h.core.store.put("inbox", row)
            await h.core.close()
            downgrade_fixture(path)
            h.core = h.new_core()
            try:
                facts = h.facts([dict(input=request)])
                self.assertEqual("withdrawn", facts["physicals"][0]["state"])
                self.assertNotEqual(
                    "legacy:control", facts["admissions"][0]["source"]["receipt_id"]
                )
                with self.assertRaises(Fault) as error:
                    await h.submit(h.fanout(message="old:p", revision=3, kind="edit"))
                self.assertEqual("version_conflict", error.exception.code)
            finally:
                await h.core.close()
