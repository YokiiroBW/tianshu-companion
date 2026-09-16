"""Synthetic end-to-end long-form chain over a real SQLite file; no real model or user data."""

import asyncio
import sqlite3
from contextlib import closing

import pytest

from support import Harness
from tianshu_companion.store import Store

WORK = "work:chain"
CHAPTERS = ["work:chain:1", "work:chain:2", "work:chain:3"]
REVISED_ONE = "Chapter one synthetic prose, human revised."


def install(harness, calls, texts):
    """Keep the chat gateway double for chat turns and count chapter generations."""
    original = harness.gateway.generate

    async def generate(turn, messages):
        if not turn["id"].startswith("chapter:"):
            return await original(turn, messages)
        calls.append((turn, messages))
        return [texts[min(len(calls) - 1, len(texts) - 1)]], {"fixture": True, "config_version": 19}

    harness.gateway.generate = generate
    return harness


def open_writing(path, calls, texts, *, first):
    harness = install(Harness(path), calls, texts)
    life = harness.core.life
    life.writing, life.config_version = True, 19
    harness.gateway.available = True
    if first:
        life.create_world("world")
        life.create_room("room", "world")
        life.configure_actor(
            "actor:a", "room", personality_version=1, schedule=[dict(minute=0, activity="writing")]
        )
    else:
        harness.core.recover()  # the real supervised startup path
    return harness


def prepare(writing):
    writing.create_work(
        WORK,
        "actor:a",
        title="Synthetic chain novel",
        outline="Canon outline: two synthetic travellers cross a fictional salt plain.",
        characters=["Nia: synthetic cartographer", "Toma: synthetic guide"],
    )
    writing.add_chapter(CHAPTERS[0], WORK, title="Chapter one", goal="Reach the salt plain.")
    writing.add_chapter(CHAPTERS[1], WORK, title="Chapter two", goal="Cross the salt plain.")
    writing.add_chapter(CHAPTERS[2], WORK, title="Chapter three", goal="Find the far road.")
    writing.record_candidate(
        WORK, "far-road", "The far road is a raised stone causeway.", author="admin", expected=1
    )
    return writing.work_metadata(WORK)


async def produce(writing, chapter, request, *, retry=False):
    meta = writing.chapter_metadata(chapter)
    if retry:
        writing.retry_chapter(chapter, request_id=request, expected=meta["version"])
    else:
        writing.request_chapter(chapter, request_id=request, expected=meta["version"])
    await writing.work()
    return writing.chapter_metadata(chapter)


def publish(writing, chapter, *, reviewer="admin"):
    meta = writing.chapter_metadata(chapter)
    writing.review_chapter(
        chapter,
        meta["current_revision"],
        reviewer=reviewer,
        decision="approved",
        notes="Synthetic human review of motivation, causality and open threads.",
        expected=meta["version"],
    )
    return writing.publish_chapter(chapter, reviewer=reviewer, expected=meta["version"])


def test_two_chapter_chain_review_publication_and_supervised_restart(tmp_path):
    path = tmp_path / "chain.db"
    calls, texts = [], ["Chapter one synthetic prose.", "Chapter two synthetic prose."]

    async def scenario():
        harness = open_writing(path, calls, texts, first=True)
        writing = harness.core.writing
        store = harness.core.store
        head = store.source_head()
        work = prepare(writing)
        writing.set_work_access(WORK, readers=["reader:story"])
        assert [c["state"] for c in work["chapters"]] == ["planned", "planned", "planned"]

        first = await produce(writing, CHAPTERS[0], "req:1")
        assert first["state"] == "draft"
        assert calls[0][0]["config_version"] == 19  # pinned independent writing config
        materials = writing.admin_read_materials(CHAPTERS[0])["material"]
        assert [e["kind"] for e in materials].count("candidate") == 1
        assert [e["standing"] for e in materials if e["kind"] == "goal"] == ["plan"]
        published_first = publish(writing, CHAPTERS[0])
        assert writing.chapter_metadata(CHAPTERS[0])["state"] == "published"
        assert writing.read_chapter(CHAPTERS[0], reader="reader:story")["content"] == texts[0]

        second = await produce(writing, CHAPTERS[1], "req:2")
        assert second["state"] == "draft"
        assert second["basis"]["references"] == [[CHAPTERS[0], published_first, 0]]
        bundle = writing.admin_read_materials(CHAPTERS[1])["material"]
        prior = [e for e in bundle if e["kind"] == "prior_chapter"][0]
        assert prior["standing"] == "canon" and prior["excerpt"] and not prior["truncated"]
        assert prior["source"]["revision_id"] == published_first
        published_second = publish(writing, CHAPTERS[1])
        assert [p["revision_id"] for p in writing.publications(CHAPTERS[1])] == [published_second]
        assert store.source_head() == head  # fictional writing never enters the fact stream

        # Editing an earlier chapter flags later chapters for review; published text is frozen.
        revised = writing.revise_chapter(
            CHAPTERS[0],
            REVISED_ONE,
            editor="admin",
            reason="Motivation of the guide was unclear.",
            expected=writing.chapter_metadata(CHAPTERS[0])["version"],
        )
        downstream = writing.chapter_metadata(CHAPTERS[1])
        assert downstream["state"] == "needs_review"
        assert downstream["review_required"] == ["prior_chapter_changed"]
        assert writing.read_chapter(CHAPTERS[0], reader="reader:story")["content"] == texts[0]
        assert writing.admin_read_revision(published_first)["content"] == texts[0]
        with pytest.raises(ValueError):
            # The newer draft is not approved yet, so the old publication stays authoritative.
            writing.publish_chapter(
                CHAPTERS[0],
                reviewer="admin",
                expected=writing.chapter_metadata(CHAPTERS[0])["version"],
            )
        assert publish(writing, CHAPTERS[0]) == revised
        assert (
            writing.publish_chapter(
                CHAPTERS[0],
                reviewer="admin",
                expected=writing.chapter_metadata(CHAPTERS[0])["version"],
            )
            == revised
        )
        assert [p["supersedes"] for p in writing.publications(CHAPTERS[0])] == [
            None,
            published_first,
        ]

        # Regenerating the dependent chapter re-pins the refreshed canon; the published
        # chapter two text is unchanged, so the newer draft is a pending publication.
        refreshed = await produce(writing, CHAPTERS[1], "req:3", retry=True)
        assert refreshed["state"] == "draft"
        assert refreshed["review_required"] == [] and refreshed["publication_pending"] is True
        assert refreshed["basis"]["references"] == [[CHAPTERS[0], revised, 0]]
        assert refreshed["current_revision"] != published_second
        assert writing.read_chapter(CHAPTERS[1], reader="reader:story")["content"] == texts[1]
        republished = publish(writing, CHAPTERS[1])
        history = writing.publications(CHAPTERS[1])
        assert [p["revision_id"] for p in history] == [published_second, republished]
        assert history[1]["supersedes"] == published_second
        assert writing.read_chapter(CHAPTERS[1], reader="reader:story")["content"] == texts[1]

        # A submitted attempt that never returned stays unknown across a supervised restart.
        awaiting = writing.chapter_metadata(CHAPTERS[2])
        writing.request_chapter(CHAPTERS[2], request_id="req:4", expected=awaiting["version"])
        item = writing._get("chapters", CHAPTERS[2])
        item.update(state="generating")
        writing._save_chapter(item)
        request = store.get("write_requests", "req:4")
        request.update(submitted=True, state="submitted")
        writing._save("requests", request)
        generated = len(calls)
        await harness.core.close()

        restarted = open_writing(path, calls, texts, first=False)
        resumed = restarted.core.writing
        assert resumed.chapter_metadata(CHAPTERS[2])["state"] == "unknown"
        await resumed.work()
        assert len(calls) == generated  # unknown outcomes are never resent automatically
        assert [p["revision_id"] for p in resumed.publications(CHAPTERS[1])] == [
            published_second,
            republished,
        ]
        assert resumed.read_chapter(CHAPTERS[0], reader="reader:story")["content"] == REVISED_ONE
        assert restarted.core.store.source_head() == head
        recovered = resumed.chapter_metadata(CHAPTERS[2])
        resumed.retry_chapter(CHAPTERS[2], request_id="req:5", expected=recovered["version"])
        cancelled = resumed.cancel_chapter(
            CHAPTERS[2], expected=resumed.chapter_metadata(CHAPTERS[2])["version"]
        )
        assert cancelled["state"] == "cancelled" and cancelled["current_revision"] is None
        await resumed.work()
        assert len(calls) == generated

        # Ordinary chat keeps working in the same Core alongside the fictional work.
        await restarted.core.ingest("nonebot", restarted.request("你好"))
        restarted.clock.advance(6)
        await restarted.cycles()
        turns = restarted.core.store.list("turns")
        assert turns and turns[0]["phase"] == "sent"
        await restarted.core.close()

    asyncio.run(scenario())
    with closing(Store(path)) as store:
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 7
        assert len(store.list("write_publications")) == 4
        assert not list(tmp_path.glob("*.bak"))


def test_chain_human_only_authoring_review_and_publication_offline(tmp_path):
    path = tmp_path / "human.db"

    async def scenario():
        harness = open_writing(path, [], [], first=True)
        writing = harness.core.writing
        store = harness.core.store
        head = store.source_head()
        # No independent writing configuration anywhere: generation is never attempted.
        harness.core.life.writing = False
        harness.core.life.config_version = None
        harness.gateway.available = False
        writing.create_work(
            "work:human",
            "actor:a",
            title="Offline work",
            outline="Canon outline written by a human editor.",
            characters=["Nia: synthetic cartographer"],
        )
        writing.add_chapter("work:human:1", "work:human", title="One", goal="Open the story.")
        writing.add_chapter("work:human:2", "work:human", title="Two", goal="Close the story.")
        writing.set_work_access("work:human", readers=["reader:offline"])

        meta = writing.chapter_metadata("work:human:1")
        writing.request_chapter("work:human:1", request_id="req:h1", expected=meta["version"])
        await writing.work()
        assert writing.chapter_metadata("work:human:1")["state"] == "unavailable"
        assert writing.admin_read_materials("work:human:1")["material"]

        authored = writing.revise_chapter(
            "work:human:1",
            "A human-written synthetic opening.",
            editor="admin",
            reason="Author the first revision without any model.",
            expected=writing.chapter_metadata("work:human:1")["version"],
        )
        assert writing.chapter_metadata("work:human:1")["state"] == "draft"
        assert writing.admin_read_revision(authored)["parent"] is None
        published = publish(writing, "work:human:1")
        assert writing.read_chapter("work:human:1", reader="reader:offline")["content"] == (
            "A human-written synthetic opening."
        )

        second = writing.chapter_metadata("work:human:2")
        writing.request_chapter("work:human:2", request_id="req:h2", expected=second["version"])
        await writing.work()
        assert writing.chapter_metadata("work:human:2")["state"] == "unavailable"
        writing.revise_chapter(
            "work:human:2",
            "A human-written synthetic closing that follows chapter one.",
            editor="admin",
            reason="Continue from the published opening.",
            expected=writing.chapter_metadata("work:human:2")["version"],
        )
        follow_up = writing.chapter_metadata("work:human:2")
        assert follow_up["basis"]["references"] == [["work:human:1", published, 0]]
        publish(writing, "work:human:2")
        assert store.source_head() == head
        assert [w["id"] for w in writing.works("actor:a")] == ["work:human"]
        await harness.core.close()

    asyncio.run(scenario())
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 7
