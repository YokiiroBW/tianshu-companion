"""Bounded receipt recovery; synthetic attempts never send a real contact."""

import asyncio
import os
from unittest.mock import patch
from uuid import uuid4

import pytest

from support import Harness
from test_proactive import START
from tianshu_companion import observability as obs
from tianshu_companion.store import Store


@pytest.fixture(params=["sqlite", "postgresql"])
def harness(tmp_path, request):
    settings = None
    if request.param == "postgresql":
        url = os.environ.get("TS233_POSTGRES_URL")
        if not url:
            pytest.skip("TS233_POSTGRES_URL is required for isolated real PostgreSQL")
        settings = {
            "storage_backends": {"postgresql_url": url, "namespace": "ts381_" + uuid4().hex}
        }
    with patch("support.Store", lambda path: Store(path, settings=settings)):
        h = Harness(tmp_path / "receipts.sqlite")
    schema = h.core.store.database.schema if h.core.store.backend == "postgresql" else None
    try:
        if request.param == "postgresql":
            assert h.core.store.backend == "postgresql", (
                "isolated PG configuration fell back to SQLite"
            )
        yield h
    finally:
        h.core.store.close()
        if schema is not None:
            import psycopg
            from psycopg import sql

            # Only this fixture's explicit isolated DB and random namespace.
            with psycopg.connect(url, autocommit=True) as db:
                db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


class Receipts:
    native_delivery = True
    available = False

    def __init__(self):
        self.calls, self.active, self.maximum = [], 0, 0
        self.full, self.release = asyncio.Event(), asyncio.Event()

    def offer_pending_events(self):
        pass

    def feedback_for_contacts(self):
        pass

    async def reconcile_proactive(self, attempt):
        self.calls.append((attempt["id"], obs.current_correlation_id()))
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        if self.active == 4:
            self.full.set()
        try:
            await self.release.wait()
        finally:
            self.active -= 1


def pending(h, count=9):
    for index in range(count):
        h.core.store.put(
            "proactive_attempts",
            dict(
                id=f"synthetic-attempt:{index}",
                conversation_id=f"synthetic-room:{index}",
                state="unknown",
                deadline=START + index,
                sequence=index,
                delivery={"expression_id": f"synthetic-expression:{index}"},
            ),
        )


def test_unknown_receipts_rotate_beyond_four_and_overlap_only_the_bounded_reads(harness):
    async def run():
        h, peer = harness, Receipts()
        h.clock.now = START
        h.core.proactive.dispatcher = peer
        pending(h)
        before = h.core.store.list("proactive_attempts")
        try:
            task = asyncio.create_task(h.core.proactive.work())
            await asyncio.wait_for(peer.full.wait(), 1)
            assert len(peer.calls) == peer.active == peer.maximum == 4
            assert len({correlation for _, correlation in peer.calls}) == 4
            peer.release.set()
            await task
            await h.core.proactive.work()
            assert len(peer.calls) == 8
            await h.core.proactive.work()
            assert {key for key, _ in peer.calls} == {item["id"] for item in before}
            assert h.core.store.list("proactive_attempts") == before
            assert peer.active == 0 and peer.maximum == 4
            assert all(obs.valid_correlation_id(value) for _, value in peer.calls)
        finally:
            await h.core.close()

    asyncio.run(run())


def test_cancelled_receipt_batch_drains_all_reads_without_settling_or_resending(harness):
    async def run():
        h, peer = harness, Receipts()
        h.clock.now = START
        h.core.proactive.dispatcher = peer
        pending(h)
        before = h.core.store.list("proactive_attempts")
        try:
            task = asyncio.create_task(h.core.proactive.work())
            await asyncio.wait_for(peer.full.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert peer.active == 0
            assert h.core.store.list("proactive_attempts") == before
            assert not h.core.proactive.lock.locked()
            peer.release.set()
            for _ in range(3):
                await h.core.proactive.work()
            assert {key for key, _ in peer.calls} == {item["id"] for item in before}
            assert h.core.store.list("proactive_attempts") == before
        finally:
            await h.core.close()

    asyncio.run(run())
