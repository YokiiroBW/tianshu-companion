"""Synthetic long-form boundaries; model doubles exist only in this test module."""

import asyncio
import sqlite3
from contextlib import closing
from unittest.mock import patch

import pytest

from support import Harness
from test_life import setup
from tianshu_companion.contracts import canonical
from tianshu_companion.life import Life
from tianshu_companion.store import WRITING_TABLES, Store
from tianshu_companion.writing import DEFAULT_CHAPTER_RECIPE, STANDING, Writing


def build(store=None, *, writing=True, version=19, chapters=2, **options):
    life, clock, model = setup(store, writing=writing, version=version)
    port = Writing(life, **options)
    port.create_work(
        "work",
        "a",
        title="Synthetic novel",
        outline="Canon outline version one.",
        characters=["A: synthetic protagonist"],
    )
    for index in range(1, chapters + 1):
        port.add_chapter(f"work:{index}", "work", title=f"Chapter {index}", goal=f"Goal {index}")
    return port, life, clock, model


@pytest.fixture
def env():
    port, life, clock, model = build()
    yield port, life, clock, model
    port.store.close()


def draft(port, model, chapter="work:1", output=None, request="req:1"):
    model.output = output or f"Synthetic {chapter} prose."
    meta = port.chapter_metadata(chapter)
    port.request_chapter(chapter, request_id=request, expected=meta["version"])
    asyncio.run(port.work())
    return port.chapter_metadata(chapter)


def review(
    port,
    chapter,
    *,
    decision="approved",
    reviewer="admin",
    notes="Synthetic opinion.",
    acknowledge_drift=False,
):
    meta = port.chapter_metadata(chapter)
    return port.review_chapter(
        chapter,
        meta["current_revision"],
        reviewer=reviewer,
        decision=decision,
        notes=notes,
        expected=meta["version"],
        acknowledge_drift=acknowledge_drift,
    )


def approve_publish(port, chapter, *, reviewer="admin"):
    review(port, chapter, reviewer=reviewer)
    meta = port.chapter_metadata(chapter)
    return port.publish_chapter(chapter, reviewer=reviewer, expected=meta["version"])


def test_recipe_immutable_bounded_and_requires_every_constraint(env):
    port = env[0]
    # The built-in recipe is pinned as version 1 as soon as a chapter needs it.
    assert port._recipe("chapter") == DEFAULT_CHAPTER_RECIPE
    with pytest.raises(ValueError):
        port.put_recipe(dict(DEFAULT_CHAPTER_RECIPE, max_chars=20))
    recipe = dict(DEFAULT_CHAPTER_RECIPE, version=2, max_chars=20)
    key = port.put_recipe(recipe)
    assert port.put_recipe(recipe) == key
    with pytest.raises(ValueError):
        port.put_recipe(dict(recipe, style="Changed without a new version"))
    assert port._recipe("chapter")["version"] == 2
    assert port._recipe("chapter")["max_chars"] == 20
    for bad in (
        {k: v for k, v in recipe.items() if k != "style"},
        dict(recipe, version=0),
        dict(recipe, max_chars=0),
        dict(recipe, max_chars="8000"),
        dict(recipe, min_chars=True),
        dict(recipe, quality=""),
        "not-a-recipe",
    ):
        with pytest.raises(ValueError):
            port.put_recipe(bad)
    with pytest.raises(KeyError):
        port._recipe("missing")


def test_work_needs_fictional_actor_and_versioned_canon_updates(env):
    port = env[0]
    with pytest.raises(KeyError):
        port.create_work("other", "no-such-actor", title="T", outline="O", characters=["C"])
    with pytest.raises(ValueError):
        port.create_work("work", "a", title="T", outline="O", characters=["C"])
    meta = port.work_metadata("work")
    assert meta["outline_version"] == 1 and meta["characters_version"] == 1
    assert meta["fictional"] is True and meta["real_user_sources"].startswith("excluded")
    assert [c["id"] for c in meta["chapters"]] == ["work:1", "work:2"]
    for invalid in (None, True, 0):
        with pytest.raises(ValueError):
            port.update_outline("work", "Changed", author="admin", expected=invalid)
    updated = port.update_outline("work", "Canon outline version two.", author="admin", expected=1)
    assert updated["outline_version"] == 2 and updated["version"] == 2
    with pytest.raises(ValueError):
        port.update_outline("work", "Stale write", author="admin", expected=1)
    assert port.work_metadata("work")["outline"] == "Canon outline version two."
    characters = port.update_characters(
        "work", ["A: synthetic protagonist", "B: synthetic rival"], author="admin", expected=2
    )
    assert characters["characters_version"] == 2
    with pytest.raises(ValueError):
        port.update_characters("work", [], author="admin", expected=3)
    assert port.works("a")[0]["chapters"] == 2
    assert port.works("b") == []
    with pytest.raises(KeyError):
        port.work_metadata("absent")


def test_candidates_stay_proposals_until_explicitly_applied(env):
    port, _, _, model = env
    first = port.record_candidate(
        "work", "new-city", "Port Mirabel exists.", author="admin", expected=1
    )
    assert first["candidates_version"] == 2
    second = port.record_candidate(
        "work", "new-friend", "Ivo is a smuggler.", author="admin", expected=first["version"]
    )
    assert second["candidates_version"] == 3
    with pytest.raises(ValueError):
        port.record_candidate(
            "work", "new-city", "Changed", author="admin", expected=second["version"]
        )
    draft(port, model)
    bundle = port.admin_read_materials("work:1")["material"]
    candidates = [e for e in bundle if e["kind"] == "candidate"]
    assert [c["standing"] for c in candidates] == ["candidate", "candidate"]
    assert all(c["applied"] is False and c["fictional"] is True for c in candidates)
    assert all(c["source"]["candidates_version"] == 3 for c in candidates)
    applied = port.apply_candidates(
        "work", ["new-city"], author="admin", expected=port.work_metadata("work")["version"]
    )
    assert applied["characters_version"] == 2 and applied["candidates_version"] == 4
    assert "Port Mirabel exists." in applied["characters"]
    with pytest.raises(ValueError):
        port.apply_candidates("work", ["new-city"], author="admin", expected=applied["version"])
    with pytest.raises(KeyError):
        port.apply_candidates("work", ["absent"], author="admin", expected=applied["version"])
    with pytest.raises(ValueError):
        port.apply_candidates("work", ["new-friend"], author="admin", expected=1)
    flagged = port.chapter_metadata("work:1")
    assert flagged["state"] == "needs_review"
    assert flagged["review_required"] == ["characters_changed", "candidates_changed"]


def test_chapter_order_insert_move_and_forward_invalidation(env):
    port, _, _, model = env
    third = port.add_chapter("work:3", "work", title="Chapter 3", goal="Goal 3")
    assert third["order"] == 3
    inserted = port.add_chapter("work:0", "work", title="Prologue", goal="Setup", order=1)
    assert inserted["order"] == 1
    assert [c["id"] for c in port.chapters("work")] == ["work:0", "work:1", "work:2", "work:3"]
    assert [c["order"] for c in port.chapters("work")] == [1, 2, 3, 4]
    for bad in (0, 6, "1", True):
        with pytest.raises(ValueError):
            port.add_chapter("work:x", "work", title="T", goal="G", order=bad)
    first = draft(port, model, "work:1")
    second = draft(port, model, "work:2", request="req:2")
    assert second["basis"]["references"] == [["work:1", first["current_revision"], 0]]
    moved = port.move_chapter("work:2", order=1, expected=second["version"])
    assert moved["order"] == 1
    assert [c["id"] for c in port.chapters("work")] == ["work:2", "work:0", "work:1", "work:3"]
    # Reordering changed which chapters precede the others, so their drafts need review.
    flagged = port.chapter_metadata("work:1")
    assert flagged["state"] == "needs_review"
    assert flagged["review_required"] == ["prior_chapter_changed"]
    with pytest.raises(ValueError):
        port.move_chapter("work:2", order=9, expected=moved["version"])
    with pytest.raises(ValueError):
        port.move_chapter("work:2", order=2, expected=second["version"])


def test_pending_attempt_is_invalidated_when_canon_changes(env):
    port, _, _, model = env
    meta = port.chapter_metadata("work:1")
    port.request_chapter("work:1", request_id="req:1", expected=meta["version"])
    assert port.chapter_metadata("work:1")["state"] == "queued"
    port.update_outline("work", "Canon outline changed while queued.", author="admin", expected=1)
    pending = port.chapter_metadata("work:1")
    assert pending["state"] == "invalidated"
    assert pending["review_required"] == ["outline_changed"]
    assert pending["attempt_state"] == "invalidated" and pending["current_revision"] is None
    asyncio.run(port.work())
    assert model.calls == []  # a voided attempt never reaches the model
    port.recheck("work")
    assert port.chapter_metadata("work:1") == pending  # the state is stable across passes
    with pytest.raises(ValueError):
        port.cancel_chapter("work:1", expected=pending["version"])
    again = port.request_chapter("work:1", request_id="req:2", expected=pending["version"])
    assert again["state"] == "queued" and again["basis"]["outline_version"] == 2
    assert again["review_required"] == []  # a re-pinned attempt carries no stale flag
    model.output = "Chapter prose after the re-pin."
    asyncio.run(port.work())
    fresh = port.chapter_metadata("work:1")
    assert fresh["state"] == "draft" and fresh["review_required"] == []


def test_material_digest_detects_changes_without_a_named_version_field(env):
    port, _, _, model = env
    draft(port, model, "work:1", output="Chapter one text.")
    second = draft(port, model, "work:2", request="req:2", output="Chapter two text.")
    assert second["state"] == "draft" and second["review_required"] == []
    port.set_chapter_plan(
        "work:1",
        title="The salt plain",
        goal="Goal 1",
        editor="admin",
        expected=port.chapter_metadata("work:1")["version"],
    )
    flagged = port.chapter_metadata("work:2")
    assert flagged["state"] == "needs_review"
    # A prior chapter title reaches the material without bumping any pinned version field.
    assert flagged["basis"]["outline_version"] == second["basis"]["outline_version"]
    assert flagged["basis"]["references"] == second["basis"]["references"]
    pinned = port._get("chapters", "work:2")
    assert (
        port._material_digest(pinned, port._get("works", "work"))
        != second["basis"]["material_version"]
    )
    assert flagged["review_required"] == ["material_changed"]
    assert port.admin_read_revision(second["current_revision"])["content"] == "Chapter two text."
    # Publishing the prior chapter flips its standing from draft_basis to canon.
    first = port.chapter_metadata("work:1")
    with pytest.raises(ValueError):
        # Its own goal was edited, so the existing text needs an explicit re-review first.
        port.publish_chapter("work:1", reviewer="admin", expected=first["version"])
    review(port, "work:1", acknowledge_drift=True)
    rechecked = port.chapter_metadata("work:1")
    port.publish_chapter("work:1", reviewer="admin", expected=rechecked["version"])
    still = port.chapter_metadata("work:2")
    assert still["state"] == "needs_review"
    assert still["review_required"] == ["material_changed"]
    port.retry_chapter("work:2", request_id="req:3", expected=still["version"])
    asyncio.run(port.work())
    fresh = port.chapter_metadata("work:2")
    assert fresh["state"] == "draft" and fresh["review_required"] == []
    entries = port.admin_read_materials("work:2")["material"]
    prior = [e for e in entries if e["kind"] == "prior_chapter"][0]
    assert prior["standing"] == "canon" and prior["title"] == "The salt plain"
    assert prior["source"]["revision_id"] == port.chapter_metadata("work:1")["current_revision"]


def test_stale_success_response_is_discarded_and_the_new_attempt_survives(env):
    """A response for a superseded attempt never writes chapter content or new state."""
    port, _, _, model = env
    original = model.generate

    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()

        async def gated(turn, messages):
            started.set()
            await release.wait()
            return await original(turn, messages)

        model.generate = gated
        meta = port.chapter_metadata("work:1")
        port.request_chapter("work:1", request_id="old", expected=meta["version"])
        task = asyncio.create_task(port.work())
        await started.wait()
        port.update_outline(
            "work",
            "CHANGED outline while the old attempt was in flight.",
            author="admin",
            expected=port.work_metadata("work")["version"],
        )
        assert port.chapter_metadata("work:1")["state"] == "invalidated"
        port.request_chapter(
            "work:1", request_id="new", expected=port.chapter_metadata("work:1")["version"]
        )
        release.set()
        await task

        chapter = port.chapter_metadata("work:1")
        assert chapter["state"] == "queued" and chapter["attempt"] == "new"
        assert chapter["current_revision"] is None
        assert chapter["review_required"] == []
        assert port.store.list("write_revisions") == []
        old = port.store.get("write_requests", "old")
        assert old["state"] == "superseded" and old["stale"] is True
        assert old["response_received"] is True and old["revision"] is None
        assert old["superseded_by"] == "new"
        assert old["basis"] != port.store.get("write_requests", "new")["basis"]
        new = port.store.get("write_requests", "new")
        assert new["state"] == "reserved" and new["submitted"] is False
        # The surviving attempt still runs and its text carries its own pinned basis.
        model.generate = original
        model.output = "New attempt prose."
        await port.work()
        final = port.chapter_metadata("work:1")
        assert final["state"] == "draft"
        revision = port.admin_read_revision(final["current_revision"])
        assert revision["content"] == "New attempt prose."
        assert revision["basis"] == new["basis"] != old["basis"]
        assert port.store.get("write_requests", "new")["state"] == "draft"
        assert port.store.get("write_requests", "old")["revision"] is None
        # The discarded call carried the superseded material; the surviving call did not.
        assert "CHANGED outline" in model.calls[-1][1][-1]["content"]
        assert "Canon outline version one." in model.calls[0][1][-1]["content"]

    asyncio.run(scenario())


def test_stale_failure_never_fails_the_new_attempt(env):
    port, _, _, model = env
    original = model.generate

    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()

        async def gated(turn, messages):
            started.set()
            await release.wait()
            raise RuntimeError("synthetic upstream failure")

        model.generate = gated
        meta = port.chapter_metadata("work:1")
        port.request_chapter("work:1", request_id="old", expected=meta["version"])
        task = asyncio.create_task(port.work())
        await started.wait()
        port.update_outline(
            "work", "Changed again.", author="admin", expected=port.work_metadata("work")["version"]
        )
        port.request_chapter(
            "work:1", request_id="new", expected=port.chapter_metadata("work:1")["version"]
        )
        release.set()
        await task

        chapter = port.chapter_metadata("work:1")
        assert chapter["state"] == "queued" and chapter["attempt"] == "new"
        assert chapter["failure"] is None and chapter["current_revision"] is None
        old = port.store.get("write_requests", "old")
        assert old["state"] == "superseded" and old["failure"] == "RuntimeError"
        assert port.store.get("write_requests", "new")["state"] == "reserved"
        # The surviving attempt is still able to produce the chapter.
        model.generate = original
        await port.work()
        assert port.chapter_metadata("work:1")["state"] == "draft"

    asyncio.run(scenario())


def test_stale_task_cancellation_never_interrupts_the_new_attempt(env):
    port, _, _, model = env
    original = model.generate

    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()

        async def gated(turn, messages):
            started.set()
            await release.wait()
            return await original(turn, messages)

        model.generate = gated
        meta = port.chapter_metadata("work:1")
        port.request_chapter("work:1", request_id="old", expected=meta["version"])
        task = asyncio.create_task(port.work())
        await started.wait()
        port.update_outline(
            "work",
            "Changed before shutdown.",
            author="admin",
            expected=port.work_metadata("work")["version"],
        )
        port.request_chapter(
            "work:1", request_id="new", expected=port.chapter_metadata("work:1")["version"]
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        chapter = port.chapter_metadata("work:1")
        assert chapter["state"] == "queued" and chapter["attempt"] == "new"
        assert chapter["current_revision"] is None
        old = port.store.get("write_requests", "old")
        assert old["state"] == "superseded" and old["stale"] is True
        assert old["response_received"] is False and old["revision"] is None
        assert port.store.get("write_requests", "new")["state"] == "reserved"
        model.generate = original
        await port.work()
        assert port.chapter_metadata("work:1")["state"] == "draft"

    asyncio.run(scenario())


def test_stale_response_never_revives_a_cancelled_new_attempt(env):
    port, _, _, model = env
    original = model.generate

    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()

        async def gated(turn, messages):
            started.set()
            await release.wait()
            return await original(turn, messages)

        model.generate = gated
        meta = port.chapter_metadata("work:1")
        port.request_chapter("work:1", request_id="old", expected=meta["version"])
        task = asyncio.create_task(port.work())
        await started.wait()
        port.update_outline(
            "work",
            "Changed once more.",
            author="admin",
            expected=port.work_metadata("work")["version"],
        )
        port.request_chapter(
            "work:1", request_id="new", expected=port.chapter_metadata("work:1")["version"]
        )
        cancelled = port.cancel_chapter(
            "work:1", expected=port.chapter_metadata("work:1")["version"]
        )
        assert cancelled["state"] == "cancelled" and cancelled["current_revision"] is None
        release.set()
        await task
        final = port.chapter_metadata("work:1")
        assert final["state"] == "cancelled" and final["current_revision"] is None
        assert port.store.list("write_revisions") == []
        assert port.store.get("write_requests", "old")["state"] == "superseded"
        assert port.store.get("write_requests", "old")["revision"] is None
        assert port.store.get("write_requests", "new")["state"] == "cancelled"
        await port.work()  # nothing queued: the cancelled attempt is never resurrected
        assert port.chapter_metadata("work:1")["state"] == "cancelled"
        assert model.calls  # the discarded upstream call did happen

    asyncio.run(scenario())


def test_publish_is_refused_after_outline_change_until_explicit_re_review(env):
    port, _, _, model = env
    draft(port, model)
    review(port, "work:1")
    approved = port.chapter_metadata("work:1")
    assert approved["review"]["decision"] == "approved"
    assert approved["review"]["matches_basis"] is True
    port.update_outline("work", "New contradictory canon", author="admin", expected=1)
    flagged = port.chapter_metadata("work:1")
    assert flagged["state"] == "needs_review" and flagged["review_required"] == ["outline_changed"]
    assert flagged["review"]["material_version"] == flagged["basis"]["material_version"]
    with pytest.raises(ValueError):
        port.publish_chapter("work:1", reviewer="admin", expected=flagged["version"])
    assert port.publications("work:1") == []
    assert port.chapter_metadata("work:1")["state"] == "needs_review"
    # Explicit re-review against the current dependencies is the only approval that publishes.
    stale = port.reviews("work:1")[-1]
    acknowledged = review(
        port, "work:1", acknowledge_drift=True, notes="Checked against new canon."
    )
    assert acknowledged["acknowledged_drift"] == ["outline_changed"]
    assert acknowledged["material_version"] != stale["material_version"]
    refreshed = port.chapter_metadata("work:1")
    assert refreshed["state"] == "draft" and refreshed["review_required"] == []
    assert refreshed["review"]["matches_basis"] is True
    published = port.publish_chapter("work:1", reviewer="admin", expected=refreshed["version"])
    assert published == refreshed["current_revision"]
    assert [p["revision_id"] for p in port.publications("work:1")] == [published]
    assert port.chapter_metadata("work:1")["state"] == "published"
    with pytest.raises(ValueError):
        port.review_chapter(
            "work:1",
            refreshed["current_revision"],
            reviewer="admin",
            decision="changes_requested",
            notes="n",
            expected=port.chapter_metadata("work:1")["version"],
            acknowledge_drift=True,
        )


def test_prior_chapter_change_invalidates_the_approval_until_re_review(env):
    port, _, _, model = env
    draft(port, model, "work:1", output="Chapter one text.")
    draft(port, model, "work:2", request="req:2", output="Chapter two text.")
    review(port, "work:2")
    approved = port.chapter_metadata("work:2")
    assert approved["review"]["matches_basis"] is True
    port.revise_chapter(
        "work:1",
        "Chapter one, human revised.",
        editor="admin",
        reason="Continuity repair.",
        expected=port.chapter_metadata("work:1")["version"],
    )
    flagged = port.chapter_metadata("work:2")
    assert flagged["state"] == "needs_review"
    assert flagged["review_required"] == ["prior_chapter_changed"]
    with pytest.raises(ValueError):
        port.publish_chapter("work:2", reviewer="admin", expected=flagged["version"])
    assert port.publications("work:2") == []
    review(port, "work:2", acknowledge_drift=True)
    refreshed = port.chapter_metadata("work:2")
    assert refreshed["review_required"] == [] and refreshed["review"]["matches_basis"] is True
    assert port.publish_chapter("work:2", reviewer="admin", expected=refreshed["version"])
    assert port.admin_read_revision(approved["current_revision"])["content"] == "Chapter two text."


def test_approval_is_bound_to_the_pinned_material_across_a_retry(env):
    port, _, _, model = env
    draft(port, model, "work:1", output="Chapter one text.")
    review(port, "work:1")
    port.update_outline("work", "Canon changed after the review", author="admin", expected=1)
    flagged = port.chapter_metadata("work:1")
    with pytest.raises(ValueError):
        port.publish_chapter("work:1", reviewer="admin", expected=flagged["version"])
    # A retry re-pins the material, so the earlier approval no longer authorises a release.
    port.retry_chapter("work:1", request_id="req:2", expected=flagged["version"])
    queued = port.chapter_metadata("work:1")
    with pytest.raises(ValueError):
        port.publish_chapter("work:1", reviewer="admin", expected=queued["version"])
    with pytest.raises(ValueError):
        port.review_chapter(
            "work:1",
            queued["current_revision"],
            reviewer="admin",
            decision="approved",
            notes="n",
            expected=queued["version"],
            acknowledge_drift=True,
        )
    cancelled = port.cancel_chapter("work:1", expected=queued["version"])
    assert cancelled["state"] == "cancelled" and cancelled["current_revision"] is not None
    # The retry already re-pinned the canon, so nothing is outstanding; the older approval
    # however no longer covers those pins, so it cannot authorise a release.
    assert cancelled["review_required"] == []
    assert cancelled["review"]["matches_basis"] is False
    with pytest.raises(ValueError):
        port.publish_chapter("work:1", reviewer="admin", expected=cancelled["version"])
    assert port.publications("work:1") == []
    review(port, "work:1", acknowledge_drift=True, notes="Re-checked against the current canon.")
    rechecked = port.chapter_metadata("work:1")
    assert rechecked["review"]["matches_basis"] is True
    assert port.publish_chapter("work:1", reviewer="admin", expected=rechecked["version"])


def test_generation_requires_independent_configuration_and_offline_review(env):
    port, life, _, model = env
    meta = draft(port, model)
    # Human revision and review keep working while the model is offline.
    model.available = False
    life.writing = False
    revised = port.revise_chapter(
        "work:1",
        "Human prose while offline.",
        editor="admin",
        reason="Tighten the ending.",
        expected=meta["version"],
    )
    assert port.admin_read_revision(revised)["source"]["kind"] == "human"
    review(port, "work:1")
    current = port.chapter_metadata("work:1")
    assert port.publish_chapter("work:1", reviewer="admin", expected=current["version"])
    assert port.chapter_metadata("work:1")["state"] == "published"
    for writing, version, available in ((False, 19, True), (True, None, True), (True, 19, False)):
        candidate, _, _, other = build(writing=writing, version=version, chapters=1)
        try:
            other.available = available
            meta = candidate.chapter_metadata("work:1")
            result = candidate.request_chapter(
                "work:1", request_id="req:x", expected=meta["version"]
            )
            assert result["state"] == "unavailable"
            asyncio.run(candidate.work())
            assert other.calls == []
            assert candidate.admin_read_materials("work:1")["material"]
        finally:
            candidate.store.close()


def test_materials_are_bounded_sourced_and_overflow_is_explicit(env):
    port, _, _, model = env
    draft(port, model, "work:1", output="First chapter with a long synthetic body.")
    draft(port, model, "work:2", request="req:2")
    bundle = port.admin_read_materials("work:2")
    entries = bundle["material"]
    assert {e["standing"] for e in entries} <= STANDING
    assert all(e["fictional"] is True for e in entries)
    assert bundle["material_bytes"] == len(canonical(entries).encode()) > 0
    assert [e["kind"] for e in entries].count("outline") == 1
    assert [e["kind"] for e in entries].count("character") == 1
    prior = [e for e in entries if e["kind"] == "prior_chapter"][0]
    assert prior["standing"] == "draft_basis"
    assert prior["source"]["chapter_id"] == "work:1"
    assert prior["source"]["revision_id"] == port.chapter_metadata("work:1")["current_revision"]
    assert prior["summary_source"] == "derived_excerpt" and prior["excerpt"]
    selection = [e for e in entries if e["kind"] == "selection"][0]
    assert selection["excerpt_chapters"] == ["work:1"] and selection["summary_only_chapters"] == []
    goal = [e for e in entries if e["kind"] == "goal"][0]
    assert goal["standing"] == "plan" and goal["goal"] == "Goal 2"
    assert bundle["real_user_sources"].startswith("excluded")

    tiny, _, _, tiny_model = build(max_material_bytes=120, chapters=1)
    try:
        meta = tiny.chapter_metadata("work:1")
        overflow = tiny.request_chapter("work:1", request_id="req:o", expected=meta["version"])
        assert overflow["state"] == "context_overflow"
        assert overflow["material_bytes"] > 120 and tiny_model.calls == []
        asyncio.run(tiny.work())
        assert tiny_model.calls == []
        retried = tiny.retry_chapter(
            "work:1", request_id="req:o2", expected=tiny.chapter_metadata("work:1")["version"]
        )
        assert retried["state"] == "context_overflow" and tiny_model.calls == []
    finally:
        tiny.store.close()


def test_summary_selection_and_authored_summary_pin_versions():
    port, _, _, model = build(max_prior_chapters=1, chapters=3)
    try:
        draft(port, model, "work:1")
        draft(port, model, "work:2", request="req:2")
        draft(port, model, "work:3", request="req:3")
        entries = port.admin_read_materials("work:3")["material"]
        selection = [e for e in entries if e["kind"] == "selection"][0]
        assert selection["summary_only_chapters"] == ["work:1"]
        assert selection["excerpt_chapters"] == ["work:2"]
        summary_only = [e for e in entries if e["kind"] == "prior_chapter" and e["excerpt"] is None]
        assert len(summary_only) == 1 and summary_only[0]["source"]["chapter_id"] == "work:1"
        assert summary_only[0]["summary"] and summary_only[0]["summary_source"] == "derived_excerpt"
        meta = port.chapter_metadata("work:1")
        port.set_chapter_summary(
            "work:1", "Authored continuity summary.", editor="admin", expected=meta["version"]
        )
        assert port.chapter_metadata("work:1")["summary_version"] == 1
        flagged = port.chapter_metadata("work:3")
        assert flagged["state"] == "needs_review"
        assert flagged["review_required"] == ["prior_chapter_changed"]
        port.retry_chapter("work:3", request_id="req:4", expected=flagged["version"])
        entries = port.admin_read_materials("work:3")["material"]
        authored = [
            e
            for e in entries
            if e["kind"] == "prior_chapter" and e["source"]["chapter_id"] == "work:1"
        ][0]
        assert authored["summary"] == "Authored continuity summary."
        assert authored["summary_source"] == "authored"
    finally:
        port.store.close()


def test_request_replay_conflict_retry_and_expected_versions(env):
    port, _, _, model = env
    meta = port.chapter_metadata("work:1")
    result = port.request_chapter("work:1", request_id="req:1", expected=meta["version"])
    assert result["state"] == "queued"
    assert port.request_chapter("work:1", request_id="req:1", expected=1) == result
    with pytest.raises(ValueError):
        port.request_chapter("work:1", request_id="req:1", expected=None)
    with pytest.raises(ValueError):
        port.request_chapter("work:2", request_id="req:1", expected=1)
    with pytest.raises(ValueError):
        port.request_chapter("work:1", request_id="req:other", expected=result["version"])
    with pytest.raises(ValueError):
        port.retry_chapter("work:2", request_id="req:r", expected=1)
    asyncio.run(port.work())
    assert len(model.calls) == 1
    after = port.chapter_metadata("work:1")
    for invalid in (None, True, 0, result["version"]):
        with pytest.raises(ValueError):
            port.retry_chapter("work:1", request_id="req:2", expected=invalid)
        assert port.chapter_metadata("work:1") == after
    for operation in (
        lambda: port.revise_chapter(
            "work:1", "x", editor="admin", reason="r", expected=after["version"] + 5
        ),
        lambda: port.review_chapter(
            "work:1",
            after["current_revision"],
            reviewer="admin",
            decision="approved",
            notes="n",
            expected=after["version"] - 1,
        ),
        lambda: port.publish_chapter("work:1", reviewer="admin", expected=after["version"] + 3),
        lambda: port.cancel_chapter("work:1", expected=after["version"] + 3),
        lambda: port.set_chapter_plan(
            "work:1", title="T", goal="G", editor="admin", expected=after["version"] + 3
        ),
    ):
        with pytest.raises(ValueError):
            operation()
        assert port.chapter_metadata("work:1") == after


def test_cancel_paths_never_regenerate_or_keep_partial_text(env):
    port, _, _, model = env
    meta = port.chapter_metadata("work:1")
    port.request_chapter("work:1", request_id="req:1", expected=meta["version"])
    queued = port.chapter_metadata("work:1")
    cancelled = port.cancel_chapter("work:1", expected=queued["version"])
    assert cancelled["state"] == "cancelled" and cancelled["current_revision"] is None
    asyncio.run(port.work())
    assert model.calls == []
    with pytest.raises(ValueError):
        port.cancel_chapter("work:1", expected=cancelled["version"])
    with pytest.raises(ValueError):
        port.cancel_chapter("work:1", expected=None)

    async def scenario():
        meta = port.chapter_metadata("work:1")
        port.retry_chapter("work:1", request_id="req:2", expected=meta["version"])
        original, release = model.generate, asyncio.Event()

        async def gated(turn, messages):
            await release.wait()
            return await original(turn, messages)

        model.generate = gated
        task = asyncio.create_task(port.work())
        for _ in range(200):
            if port.chapter_metadata("work:1")["state"] == "generating":
                break
            await asyncio.sleep(0.01)
        running = port.chapter_metadata("work:1")
        assert running["state"] == "generating"
        port.cancel_chapter("work:1", expected=running["version"])
        # The call is already in flight: only the durable intent is recorded here.
        assert port.chapter_metadata("work:1")["state"] == "generating"
        assert port.store.get("write_requests", "req:2")["cancel_requested"] is True
        release.set()
        await task
        final = port.chapter_metadata("work:1")
        assert final["state"] == "cancelled" and final["current_revision"] is None
        assert final["failure"] == "cancelled_before_persist"
        assert model.calls and port.store.list("write_revisions") == []

    asyncio.run(scenario())


def test_restart_marks_submitted_attempt_unknown_without_resend(tmp_path):
    path = tmp_path / "synthetic.db"
    port, _, clock, model = build(Store(path), chapters=1)
    head = port.store.source_head()
    try:
        meta = port.chapter_metadata("work:1")
        port.request_chapter("work:1", request_id="req:1", expected=meta["version"])
        # Simulate a crash after the intent was persisted and before any response was read.
        item = port._get("chapters", "work:1")
        item.update(state="generating")
        port._save_chapter(item)
        request = port.store.get("write_requests", "req:1")
        request.update(submitted=True, state="submitted")
        port._save("requests", request)
    finally:
        port.store.close()
    store = Store(path)
    try:
        restarted = Writing(Life(store, clock, model, 19, asyncio.Semaphore(4), writing=True))
        restarted.recover()
        assert restarted.chapter_metadata("work:1")["state"] == "unknown"
        assert store.get("write_requests", "req:1")["state"] == "unknown"
        asyncio.run(restarted.work())
        assert model.calls == []  # an unknown outcome is never resent automatically
        meta = restarted.chapter_metadata("work:1")
        assert meta["state"] == "unknown" and meta["current_revision"] is None
        restarted.retry_chapter("work:1", request_id="req:2", expected=meta["version"])
        asyncio.run(restarted.work())
        assert restarted.chapter_metadata("work:1")["state"] == "draft"
        assert len(model.calls) == 1
        assert store.source_head() == head
        assert not list(tmp_path.glob("*.bak"))  # a v5 restart is not a migration
    finally:
        store.close()


def test_review_and_publish_history_keep_published_content(tmp_path):
    path = tmp_path / "synthetic.db"
    port, _, _, model = build(Store(path))
    try:
        draft(port, model, output="First synthetic chapter.")
        current = port.chapter_metadata("work:1")
        with pytest.raises(ValueError):
            port.publish_chapter("work:1", reviewer="admin", expected=current["version"])
        with pytest.raises(KeyError):
            port.review_chapter(
                "work:1",
                "unknown-revision",
                reviewer="admin",
                decision="approved",
                notes="n",
                expected=current["version"],
            )
        with pytest.raises(ValueError):
            review(port, "work:1", decision="maybe")
        first = approve_publish(port, "work:1")
        assert first == current["current_revision"]
        published = port.chapter_metadata("work:1")
        assert (
            port.publish_chapter("work:1", reviewer="admin", expected=published["version"]) == first
        )
        assert len(port.publications("work:1")) == 1
        assert published["state"] == "published" and published["publications"] == 1
        assert published["review"]["decision"] == "approved"
        second = port.revise_chapter(
            "work:1",
            "First synthetic chapter, revised.",
            editor="admin",
            reason="Fix the timeline.",
            expected=published["version"],
        )
        assert second != first
        revised = port.chapter_metadata("work:1")
        # A newer draft over a published revision is a pending publication, not drift.
        assert revised["state"] == "draft"
        assert revised["review_required"] == [] and revised["publication_pending"] is True
        assert port.admin_read_revision(first)["content"] == "First synthetic chapter."
        with pytest.raises(ValueError):
            port.publish_chapter("work:1", reviewer="admin", expected=revised["version"])
        approve_publish(port, "work:1")
        assert port.chapter_metadata("work:1")["state"] == "published"
        publications = port.publications("work:1")
        assert [p["revision_id"] for p in publications] == [first, revised["current_revision"]]
        assert publications[1]["supersedes"] == first
        assert publications[0]["supersedes"] is None
    finally:
        port.store.close()
    store = Store(path)
    try:
        restarted = Writing(
            Life(store, port.life.clock, port.life.gateway, 19, asyncio.Semaphore(4), writing=True)
        )
        assert len(restarted.publications("work:1")) == 2
        assert restarted.chapter_metadata("work:1")["state"] == "published"
        assert not list(tmp_path.glob("*.bak"))
    finally:
        store.close()


def test_read_access_is_per_work_and_never_exposes_drafts(env):
    port, _, _, model = env
    draft(port, model)
    original_get = port.store.get

    def guard(table, key):
        assert table != "write_revisions"
        return original_get(table, key)

    with patch.object(port.store, "get", guard), pytest.raises(PermissionError):
        port.read_chapter("work:1", reader="reader")
    port.set_work_access("work", readers=["reader"])
    with pytest.raises(PermissionError):
        port.read_chapter("work:1", reader="reader")  # a draft is never reader-visible
    published = approve_publish(port, "work:1")
    visible = port.read_chapter("work:1", reader="reader")
    assert visible["id"] == published and visible["fictional"] is True
    assert "source" not in visible and "review" not in visible and "content" in visible
    port.create_work("other", "b", title="Other", outline="O", characters=["B"])
    port.add_chapter("other:1", "other", title="One", goal="G")
    with pytest.raises(PermissionError):
        port.read_chapter("other:1", reader="reader")  # grants never cross works
    with pytest.raises(ValueError):
        port.add_chapter("other:1", "work", title="Alias", goal="G")  # ids never alias
    assert port.chapter_metadata("other:1")["work_id"] == "other"
    assert port.works("b")[0]["id"] == "other" and len(port.works("a")) == 1
    assert [w["id"] for w in port.works("b")] == ["other"]
    access = port.store.get("write_access", "work")
    with pytest.raises(ValueError):
        port.set_work_access("work", readers=[])
    port.set_work_access("work", readers=[], expected=access["version"])
    with pytest.raises(PermissionError):
        port.read_chapter("work:1", reader="reader")
    assert port.admin_read_revision(published)["chapter_id"] == "work:1"
    with pytest.raises(KeyError):
        port.read_chapter("absent", reader="reader")


def test_forward_dependency_changes_never_rewrite_published_chapters(env):
    port, _, _, model = env
    draft(port, model, "work:1", output="Chapter one published text.")
    first = approve_publish(port, "work:1")
    port.set_work_access("work", readers=["reader"])
    assert port.read_chapter("work:1", reader="reader")["content"] == "Chapter one published text."
    draft(port, model, "work:2", request="req:2", output="Chapter two draft text.")
    assert port.chapter_metadata("work:2")["basis"]["references"] == [["work:1", first, 0]]
    revised = port.revise_chapter(
        "work:1",
        "Chapter one, human revised.",
        editor="admin",
        reason="Continuity repair.",
        expected=port.chapter_metadata("work:1")["version"],
    )
    downstream = port.chapter_metadata("work:2")
    assert downstream["state"] == "needs_review"
    assert downstream["review_required"] == ["prior_chapter_changed"]
    assert port.read_chapter("work:1", reader="reader")["content"] == "Chapter one published text."
    assert port.admin_read_revision(first)["content"] == "Chapter one published text."
    assert port.admin_read_revision(revised)["content"] == "Chapter one, human revised."
    approve_publish(port, "work:1")
    port.update_outline("work", "Outline changed later.", author="admin", expected=1)
    flagged = port.chapter_metadata("work:1")
    assert flagged["state"] == "needs_review"
    assert flagged["review_required"] == ["outline_changed"]
    # Published text stays readable and unchanged even while flagged for review.
    assert port.read_chapter("work:1", reader="reader")["content"] == "Chapter one, human revised."
    meta = port.chapter_metadata("work:2")
    port.retry_chapter("work:2", request_id="req:3", expected=meta["version"])
    assert port.chapter_metadata("work:2")["state"] == "queued"
    asyncio.run(port.work())
    refreshed = port.chapter_metadata("work:2")
    assert refreshed["state"] == "draft" and refreshed["review_required"] == []
    assert refreshed["basis"]["outline_version"] == 2
    assert refreshed["basis"]["references"] == [
        ["work:1", port.chapter_metadata("work:1")["current_revision"], 0]
    ]


def test_fiction_separation_source_head_and_no_real_user_writes(tmp_path):
    port, _, _, model = build(Store(tmp_path / "synthetic.db"))
    try:
        head = port.store.source_head()
        meta = draft(port, model)
        revision = port.admin_read_revision(meta["current_revision"])
        assert revision["fictional"] is True and revision["canon_effect"] == "none"
        assert revision["parent"] is None and revision["source"]["kind"] == "gateway"
        assert port.admin_read_materials("work:1")["real_user_sources"].startswith("excluded")
        assert port.store.source_head() == head
        assert port.store.list("turns") == [] and port.store.list("physicals") == []
        assert port.store.list("inbox") == [] and port.store.list("outbox") == []
        assert not hasattr(port, "memory")
        assert port.work_metadata("work")["real_user_sources"].startswith("excluded")
    finally:
        port.store.close()


def test_core_wires_writing_port_and_recovers_it(tmp_path):
    async def scenario():
        harness = Harness(tmp_path / "fixture.db")
        try:
            assert isinstance(harness.core.writing, Writing)
            assert harness.core.writing.life is harness.core.life
            called = []
            with patch.object(Writing, "recover", lambda self: called.append(True)):
                harness.core.recover()
            assert called == [True]
        finally:
            await harness.core.close()

    asyncio.run(scenario())


def test_core_level_generation_never_touches_memory_or_chat_facts(tmp_path):
    async def scenario():
        harness = Harness(tmp_path / "fixture.db")
        life = harness.core.life
        life.create_world("w")
        life.create_room("r", "w")
        life.configure_actor(
            "actor:a", "r", personality_version=1, schedule=[dict(minute=0, activity="reading")]
        )
        life.writing, life.config_version = True, 19
        harness.gateway.available = True
        calls = []

        async def generate(turn, messages):
            calls.append((turn, messages))
            return ["Synthetic chapter prose."], {"fixture": True}

        harness.gateway.generate = generate
        head = harness.core.store.source_head()
        writing = harness.core.writing
        writing.create_work("work", "actor:a", title="T", outline="O", characters=["C"])
        writing.add_chapter("work:1", "work", title="One", goal="G")
        meta = writing.chapter_metadata("work:1")
        writing.request_chapter("work:1", request_id="req:1", expected=meta["version"])
        await writing.work()
        assert writing.chapter_metadata("work:1")["state"] == "draft"
        assert calls[0][0]["config_version"] == 19
        assert harness.memory.commits == [] and harness.memory.selections == []
        assert harness.memory.profile_selections == []
        assert harness.core.store.source_head() == head
        assert harness.core.store.list("turns") == []
        await harness.core.close()

    asyncio.run(scenario())


def test_v4_migration_backup_rollback_and_source_preservation(tmp_path):
    path = tmp_path / "synthetic.db"
    store = Store(path)
    head = store.source_head()
    store.put("conversations", {"id": "synthetic", "private": "synthetic preserved"})
    store.close()
    with closing(sqlite3.connect(path)) as db:
        for table in WRITING_TABLES:
            db.execute(f"DROP TABLE {table}")
        db.execute("PRAGMA user_version=4")
        # A conflicting index name makes migration fail midway, after prior DDL.
        db.execute("CREATE TABLE write_works_queue (id TEXT)")
        db.commit()
    with pytest.raises(sqlite3.OperationalError):
        Store(path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 4
        assert not db.execute(
            "SELECT name FROM sqlite_master WHERE name='write_chapters'"
        ).fetchone()
        db.execute("DROP TABLE write_works_queue")
        db.commit()
    with closing(Store(path)) as store:
        assert store.source_head() == head
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 5
        assert store.get("conversations", "synthetic")["private"] == "synthetic preserved"
    backups = sorted(tmp_path.glob("*.pre-writing-v5-*.bak"))
    assert len(backups) == 2
    for backup in backups:
        with closing(sqlite3.connect(backup)) as db:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 4
            assert (
                "synthetic preserved" in db.execute("SELECT body FROM conversations").fetchone()[0]
            )
