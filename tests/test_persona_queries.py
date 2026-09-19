"""Browsing and comparing persona versions: bounded pages, cursors, one revision, two revisions.

Synthetic data only. Two layers are covered here and they answer different questions:

* the pure rules in `persona_queries` answer "how large may a page be, what is a cursor
  bound to, and what does a difference between two revisions mean" - they are checked
  directly, because their refusals must hold even when no store is involved;
* the same rules through the authenticated management port answer "can a future web surface
  read a directory, walk one history kind, read one revision and compare two of them without
  a second management entry point and without writing anything" - and they are checked on
  what the caller really receives.
"""

import asyncio
import json

import pytest

from support import Harness, persona_config
from tianshu_companion.contracts import Fault
from tianshu_companion.persona_queries import (
    COMPARE_FIELDS,
    CURSOR_FIELDS,
    CURSOR_MAX,
    DOCUMENT_MAX_BYTES,
    PAGE_MAX_BYTES,
    QueryError,
    byte_size,
    comparison,
    fit,
    issue_cursor,
    new_secret,
    page_limit,
    read_cursor,
    within_budget,
)
from tianshu_companion.personas import Personas
from tianshu_companion.store import PERSONA_TABLES, Store

A = "actor:a"
B = "actor:b"
CATALOG_KEYS = {
    "subject",
    "state",
    "version",
    "published_revision",
    "draft_revision",
    "retired",
    "updated_at",
}
REVISION_KEYS = {
    "revision_id",
    "fingerprint",
    "parent",
    "source",
    "operator",
    "note",
    "created_at",
    "decision",
    "decided_by",
    "decided_at",
}


def ask(harness, **document):
    """One read through the authenticated management port, exactly as the HTTP route does."""
    return harness.core.manage_persona("persona_admin", document)


def directory(count):
    """A synthetic directory of `count` characters, seeded through the deployment import."""
    return persona_config(
        1,
        **{
            "actor:%02d" % index: dict(version=1, persona="Synthetic role %02d" % index)
            for index in range(count)
        },
    )


def draft(harness, subject, content):
    """One authored revision, written against the version the caller just read."""
    personas = harness.core.personas
    return personas.draft(
        subject,
        content,
        operator="operator:test",
        expected=personas.get(subject)["version"],
        reason="synthetic revision",
    )["revision"]["revision_id"]


def approve(harness, subject, revision):
    personas = harness.core.personas
    personas.approve(
        subject,
        revision,
        operator="reviewer:test",
        expected=personas.get(subject)["version"],
        reason="synthetic review",
    )


def publish(harness, subject, revision):
    personas = harness.core.personas
    return personas.publish(
        subject,
        revision,
        operator="operator:test",
        expected=personas.get(subject)["version"],
        reason="synthetic release",
    )


def page_through(harness, document, limit):
    """Every page of one read, following `next_cursor` until it stops naming one."""
    entries, pages = [], 0
    while True:
        page = ask(harness, limit=limit, **document)
        entries.extend(page["entries"])
        pages += 1
        if not page["next_cursor"]:
            return entries, pages
        document = dict(document, cursor=page["next_cursor"])
        assert pages < 50, "paging did not terminate"


def refused(harness, **document):
    with pytest.raises(Fault) as error:
        ask(harness, **document)
    return error.value.code


# --------------------------------------------------------------------- pure page rules


def test_a_page_size_is_the_documented_range_and_nothing_else():
    assert page_limit(None) == 20
    assert page_limit(1) == 1 and page_limit(100) == 100
    for bad in (True, False, 0, -1, 101, 1.0, "20", [20], {}, 10**9):
        with pytest.raises(QueryError) as error:
            page_limit(bad)
        assert error.value.code == "invalid_input"


def test_only_whole_records_are_kept_and_one_too_large_is_refused():
    entries = [dict(text="x" * 10), dict(text="y" * 10), dict(text="z" * 10)]
    keys = [["k0"], ["k1"], ["k2"]]
    kept, kept_keys = fit(entries, keys, byte_size(entries[0]) * 2 + 1)
    assert kept == entries[:2] and kept_keys == keys[:2]
    assert fit([], [], 16) == ([], [])
    # A record that cannot fit on its own is refused, never trimmed into something that
    # would look like a complete record.
    with pytest.raises(QueryError) as error:
        fit([dict(text="x" * 4000)], [["k"]], 64)
    assert error.value.code == "invalid_input"


def test_a_response_over_its_budget_is_refused_rather_than_shortened():
    assert within_budget({"a": 1}, 1024) == {"a": 1}
    with pytest.raises(QueryError) as error:
        within_budget({"text": "x" * 100}, 10)
    assert error.value.code == "invalid_input"


def test_a_cursor_is_opaque_bounded_and_bound_to_its_request():
    secret = new_secret()
    token = issue_cursor(
        secret,
        dict(
            operation="history_page",
            subject=A,
            kind="revisions",
            limit=20,
            version=3,
            last_key=[2, "revision-id"],
        ),
    )
    assert len(token) <= CURSOR_MAX
    assert "revision-id" not in token and A not in token  # base64 payload, no plain text
    binding = read_cursor(
        secret, token, operation="history_page", subject=A, kind="revisions", limit=20
    )
    assert binding["version"] == 3 and binding["last_key"] == [2, "revision-id"]

    # Another process's key is not this cursor's key: a restart invalidates it.
    with pytest.raises(QueryError):
        read_cursor(
            new_secret(), token, operation="history_page", subject=A, kind="revisions", limit=20
        )
    for wrong in (
        dict(operation="catalog", subject=A, kind="revisions", limit=20),
        dict(operation="history_page", subject=B, kind="revisions", limit=20),
        dict(operation="history_page", subject=A, kind="publications", limit=20),
        dict(operation="history_page", subject=A, kind="revisions", limit=21),
    ):
        with pytest.raises(QueryError) as error:
            read_cursor(secret, token, **wrong)
        assert error.value.code == "invalid_input"
    for broken in (
        token[:-1],
        token + "A",
        token.split(".")[0],
        token.replace(".", "", 1),
        "",
        "not-a-cursor",
        "x" * (CURSOR_MAX + 1),
        12,
        None,
    ):
        with pytest.raises(QueryError):
            read_cursor(
                secret, broken, operation="history_page", subject=A, kind="revisions", limit=20
            )


def test_a_cursor_payload_has_exactly_one_shape():
    secret = new_secret()
    complete = dict(
        operation="catalog", subject=None, kind=None, limit=5, version=None, last_key=[A]
    )
    assert issue_cursor(secret, complete)
    for broken in (
        {k: v for k, v in complete.items() if k != "version"},
        {**complete, "extra": 1},
        {},
        "not-a-binding",
    ):
        with pytest.raises(QueryError) as error:
            issue_cursor(secret, broken)
        assert error.value.code == "invalid_input"
    empty_key = issue_cursor(secret, {**complete, "last_key": []})
    with pytest.raises(QueryError):
        read_cursor(secret, empty_key, operation="catalog", limit=5)
    assert set(CURSOR_FIELDS) == set(complete)


# ----------------------------------------------------------------- pure compare rules


def test_a_comparison_reports_presence_change_and_the_full_values():
    left = {"persona": "旧人格", "tone": "冷", "style": None, "address": "你"}
    right = {"persona": "新人格", "tone": "冷", "address": "你", "extra": 7}
    result = comparison(
        left, right, left_id="r1", right_id="r2", left_fingerprint="f1", right_fingerprint="f2"
    )
    assert result["comparison_scope"] == list(COMPARE_FIELDS)
    assert set(result["fields"]) == set(COMPARE_FIELDS)
    assert result["fields"]["persona"] == dict(
        presence=dict(left=True, right=True), change="modified", left="旧人格", right="新人格"
    )
    assert result["fields"]["tone"]["change"] == "unchanged"
    assert result["fields"]["address"]["change"] == "unchanged"
    # An explicit null is a value and a missing field is an absence; they are not the same.
    assert result["fields"]["style"] == dict(
        presence=dict(left=True, right=False), change="removed", left=None, right=None
    )
    reverse = comparison(
        right, left, left_id="r2", right_id="r1", left_fingerprint="f2", right_fingerprint="f1"
    )
    assert reverse["fields"]["style"]["change"] == "added"
    assert reverse["fields"]["style"]["presence"] == dict(left=False, right=True)
    assert result["additional_fields_present"] is True
    assert result["additional_fields_changed"] is True
    assert result["additional_field_names"] == ["extra"]
    assert result["identical_revision"] is False and result["content_identical"] is False


def test_only_the_four_fields_are_ever_interpreted_as_persona_text():
    content = {
        "persona": "同一段",
        "source": "editor",
        "operator": "admin",
        "permissions": {"admin": True},
        "bindings": ["qq-private"],
    }
    result = comparison(
        content, content, left_id="r1", right_id="r1", left_fingerprint="f", right_fingerprint="f"
    )
    assert set(result["fields"]) == set(COMPARE_FIELDS)
    assert result["additional_field_names"] == ["bindings", "operator", "permissions", "source"]
    assert result["fields"]["persona"]["change"] == "unchanged"
    assert result["identical_revision"] is True and result["content_identical"] is True


def test_unchanged_persona_fields_with_a_changed_extension_are_never_called_identical():
    left = {"persona": "同一段", "tone": "冷", "style": "短", "address": "你", "extra": 1}
    right = {"persona": "同一段", "tone": "冷", "style": "短", "address": "你", "extra": 2}
    result = comparison(
        left, right, left_id="r1", right_id="r2", left_fingerprint="f1", right_fingerprint="f2"
    )
    assert all(field["change"] == "unchanged" for field in result["fields"].values())
    assert result["additional_fields_changed"] is True
    assert result["identical_revision"] is False
    assert result["content_identical"] is False


# ------------------------------------------------------------------------ the directory


def test_catalog_pages_the_whole_directory_without_gaps_or_duplicates():
    async def scenario():
        harness = Harness(personas=directory(7))
        expected = ["actor:%02d" % index for index in range(7)]
        entries, pages = page_through(harness, {"operation": "catalog"}, 2)
        assert [entry["subject"] for entry in entries] == expected
        assert pages == 4  # 2 + 2 + 2 + 1, the last page naming no next cursor
        assert len({entry["subject"] for entry in entries}) == len(expected)
        assert len(ask(harness, operation="catalog")["entries"]) == 7  # default page size
        await harness.core.close()

    asyncio.run(scenario())


def test_catalog_never_carries_persona_text():
    async def scenario():
        harness = Harness(personas=directory(3))
        page = ask(harness, operation="catalog", limit=100)
        for entry in page["entries"]:
            assert set(entry) == CATALOG_KEYS
        encoded = json.dumps(page, ensure_ascii=False)
        for index in range(3):
            assert "Synthetic role %02d" % index not in encoded
        assert page["consistency"] == "live_keyset"
        assert page["count"] == 3 and page["has_more"] is False and page["next_cursor"] is None
        await harness.core.close()

    asyncio.run(scenario())


def test_the_directory_is_a_live_keyset_not_a_promised_snapshot():
    async def scenario():
        harness = Harness(personas=directory(3))
        first = ask(harness, operation="catalog", limit=1)
        assert first["consistency"] == "live_keyset"
        assert [entry["subject"] for entry in first["entries"]] == ["actor:00"]
        assert first["next_cursor"]
        # A character registered behind the cursor is not repeated by that cursor: the page
        # boundary is the last subject that was returned, so reopening is the only way to see
        # it. Retiring one that was already returned does not bring it back either.
        harness.core.personas.import_config(
            persona_config(9, **{"actor:-": dict(version=1, persona="Registered later")})
        )
        rest, _ = page_through(harness, {"operation": "catalog", "cursor": first["next_cursor"]}, 1)
        assert [entry["subject"] for entry in rest] == ["actor:01", "actor:02"]
        reopened, _ = page_through(harness, {"operation": "catalog"}, 5)
        assert [entry["subject"] for entry in reopened] == [
            "actor:-",
            "actor:00",
            "actor:01",
            "actor:02",
        ]
        await harness.core.close()

    asyncio.run(scenario())


# ------------------------------------------------------------- one history, page by page


def test_history_page_walks_one_kind_page_by_page_without_gaps():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = harness.core.personas
        seeded = personas.get(A)["published_revision"]
        revisions = [draft(harness, A, {"persona": "版本 %d" % index}) for index in range(6)]
        approve(harness, A, revisions[-1])
        publish(harness, A, revisions[-1])
        entries, pages = page_through(
            harness, {"operation": "history_page", "subject": A, "kind": "revisions"}, 2
        )
        assert [entry["revision_id"] for entry in entries] == [seeded, *revisions]
        assert pages == 4
        assert (
            entries[-1]["decision"] == "approved" and entries[-1]["decided_by"] == "reviewer:test"
        )
        assert entries[0]["decision"] is None  # the deployment seed carried no decision
        assert ask(harness, operation="history_page", subject=A, kind="publications")["count"] == 2
        assert ask(harness, operation="history_page", subject=A, kind="approvals")["count"] == 1
        assert ask(harness, operation="history_page", subject=A, kind="rollbacks")["count"] == 0
        await harness.core.close()

    asyncio.run(scenario())


def test_history_entries_carry_no_persona_text():
    async def scenario():
        harness = Harness(personas=persona_config())
        draft(harness, A, {"persona": "秘密人格正文", "tone": "秘密语气"})
        page = ask(harness, operation="history_page", subject=A, kind="revisions")
        assert all(set(entry) == REVISION_KEYS for entry in page["entries"])
        encoded = json.dumps(page, ensure_ascii=False)
        assert "秘密人格正文" not in encoded and "秘密语气" not in encoded
        assert page["entries"][0]["fingerprint"]  # a fingerprint is not the text
        await harness.core.close()

    asyncio.run(scenario())


def test_a_stale_history_cursor_is_refused_instead_of_stitching_two_versions():
    async def scenario():
        harness = Harness(personas=persona_config())
        for index in range(4):
            draft(harness, A, {"persona": "版本 %d" % index})
        first = ask(harness, operation="history_page", subject=A, kind="revisions", limit=2)
        assert first["next_cursor"]
        # Every later write of this character advances its version: a draft, an approval, a
        # rejection, a publication, a rollback, a retirement or a restore.
        draft(harness, A, {"persona": "更新的版本"})
        assert (
            refused(
                harness,
                operation="history_page",
                subject=A,
                kind="revisions",
                limit=2,
                cursor=first["next_cursor"],
            )
            == "version_conflict"
        )
        # Reopening is the documented way forward, and it shows the newer page.
        reopened = ask(harness, operation="history_page", subject=A, kind="revisions", limit=2)
        assert [entry["revision_id"] for entry in reopened["entries"]] == [
            entry["revision_id"] for entry in first["entries"]
        ]
        await harness.core.close()

    asyncio.run(scenario())


def test_a_cursor_never_crosses_a_subject_a_kind_or_an_operation():
    async def scenario():
        harness = Harness(personas=persona_config())
        for index in range(4):
            draft(harness, A, {"persona": "版本 %d" % index})
            draft(harness, B, {"persona": "另一个角色 %d" % index})
        cursor = ask(harness, operation="history_page", subject=A, kind="revisions", limit=2)[
            "next_cursor"
        ]
        assert cursor
        assert (
            refused(
                harness,
                operation="history_page",
                subject=B,
                kind="revisions",
                limit=2,
                cursor=cursor,
            )
            == "invalid_input"
        )
        assert (
            refused(
                harness,
                operation="history_page",
                subject=A,
                kind="publications",
                limit=2,
                cursor=cursor,
            )
            == "invalid_input"
        )
        assert refused(harness, operation="catalog", limit=2, cursor=cursor) == "invalid_input"
        for broken in (cursor[:-1], cursor + "A", "x", 7, ""):
            assert (
                refused(
                    harness,
                    operation="history_page",
                    subject=A,
                    kind="revisions",
                    limit=2,
                    cursor=broken,
                )
                == "invalid_input"
            )
        # A cursor of another page size is another request, not a smaller one.
        assert (
            refused(
                harness,
                operation="history_page",
                subject=A,
                kind="revisions",
                limit=3,
                cursor=cursor,
            )
            == "invalid_input"
        )
        assert (
            refused(harness, operation="history_page", subject=A, kind="invented")
            == "invalid_input"
        )
        assert (
            refused(harness, operation="history_page", subject="actor:ghost", kind="revisions")
            == "not_found"
        )
        await harness.core.close()

    asyncio.run(scenario())


# --------------------------------------------------------- one revision, two revisions


def test_one_revision_is_read_with_where_it_stands_right_now():
    async def scenario():
        harness = Harness(personas=persona_config())
        seed = ask(harness, operation="get", subject=A)["persona"]["published_revision"]
        revision = draft(harness, A, {"persona": "新的正文", "tone": "温和"})
        approve(harness, A, revision)
        answer = ask(harness, operation="revision", subject=A, revision_id=revision)
        assert answer["revision"]["content"] == {"persona": "新的正文", "tone": "温和"}
        assert answer["revision"]["parent"] == seed
        assert answer["revision"]["approved_by"] == "reviewer:test"
        assert answer["subject"] == A
        assert answer["is_draft"] is True and answer["is_published"] is False
        assert answer["state"] == "approved"
        assert answer["published_revision"] == seed and answer["draft_revision"] == revision
        assert answer["persona_version"] == harness.core.personas.get(A)["version"]
        # The published revision is readable too, and reports itself as the live one.
        live = ask(harness, operation="revision", subject=A, revision_id=seed)
        assert live["is_published"] is True and live["is_draft"] is False
        assert live["revision"]["content"] == {"persona": "Role A"}
        await harness.core.close()

    asyncio.run(scenario())


def test_another_characters_revision_is_never_exposed_through_this_subject():
    async def scenario():
        harness = Harness(personas=persona_config())
        other = draft(harness, B, {"persona": "另一个角色的正文"})
        assert (
            refused(harness, operation="revision", subject=A, revision_id=other) == "invalid_input"
        )
        assert refused(harness, operation="compare", subject=A, left=other, right=other) == (
            "invalid_input"
        )
        assert (
            refused(harness, operation="revision", subject=A, revision_id="0" * 64) == "not_found"
        )
        assert refused(harness, operation="revision", subject=A) == "invalid_input"
        assert refused(harness, operation="compare", subject=A, left=other) == "invalid_input"
        await harness.core.close()

    asyncio.run(scenario())


def test_the_same_revision_compared_with_itself_reports_no_difference():
    async def scenario():
        harness = Harness(personas=persona_config())
        seed = ask(harness, operation="get", subject=A)["persona"]["published_revision"]
        answer = ask(harness, operation="compare", subject=A, left=seed, right=seed)
        assert answer["identical_revision"] is True and answer["content_identical"] is True
        assert all(field["change"] == "unchanged" for field in answer["fields"].values())
        assert (
            answer["fields"]["persona"]["left"] == answer["fields"]["persona"]["right"] == "Role A"
        )
        assert answer["additional_fields_present"] is False
        assert answer["additional_fields_changed"] is False
        assert answer["additional_field_names"] == []
        assert answer["left"] == answer["right"]
        await harness.core.close()

    asyncio.run(scenario())


def test_chinese_newlines_and_absent_fields_compare_exactly():
    async def scenario():
        harness = Harness(personas=persona_config())
        first = draft(harness, A, {"persona": "第一段\n第二段\n", "tone": "温和"})
        second = draft(
            harness, A, {"persona": "第一段\r\n第二段\n", "style": "短句", "address": "你"}
        )
        answer = ask(harness, operation="compare", subject=A, left=first, right=second)
        assert answer["fields"]["persona"]["change"] == "modified"
        assert answer["fields"]["persona"]["left"] == "第一段\n第二段\n"
        assert answer["fields"]["persona"]["right"] == "第一段\r\n第二段\n"
        assert answer["fields"]["tone"]["change"] == "removed"
        assert answer["fields"]["style"] == dict(
            presence=dict(left=False, right=True), change="added", left=None, right="短句"
        )
        assert answer["fields"]["address"]["change"] == "added"
        assert answer["content_identical"] is False and answer["identical_revision"] is False
        await harness.core.close()

    asyncio.run(scenario())


def test_a_rollback_derived_revision_states_that_it_replays_the_older_text():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = harness.core.personas
        seed = ask(harness, operation="get", subject=A)["persona"]["published_revision"]
        second = draft(harness, A, {"persona": "第二版"})
        approve(harness, A, second)
        publish(harness, A, second)
        before = ask(harness, operation="revision", subject=A, revision_id=seed)["revision"]
        rolled = personas.rollback(
            A, seed, operator="operator:test", expected=personas.get(A)["version"], reason="back"
        )
        restored = rolled["restored_revision"]
        answer = ask(harness, operation="compare", subject=A, left=seed, right=restored)
        # Different immutable revisions whose full content is the same bytes: the comparison
        # says exactly that and nothing more.
        assert answer["identical_revision"] is False
        assert answer["content_identical"] is True
        assert all(field["change"] == "unchanged" for field in answer["fields"].values())
        assert (
            answer["fields"]["persona"]["left"] == answer["fields"]["persona"]["right"] == "Role A"
        )
        # Publishing and rolling back move pointers; they never rewrite an immutable revision.
        after = ask(harness, operation="revision", subject=A, revision_id=seed)["revision"]
        assert after["content"] == before["content"]
        assert after["fingerprint"] == before["fingerprint"]
        assert after["approved_by"] == before["approved_by"]
        await harness.core.close()

    asyncio.run(scenario())


def test_four_unchanged_fields_with_a_changed_extension_never_read_as_identical():
    async def scenario():
        harness = Harness(personas=persona_config())
        first = draft(harness, A, {"persona": "同一段", "tone": "冷", "extra": 1})
        second = draft(harness, A, {"persona": "同一段", "tone": "冷", "extra": 2})
        answer = ask(harness, operation="compare", subject=A, left=first, right=second)
        assert all(field["change"] == "unchanged" for field in answer["fields"].values())
        assert answer["additional_fields_present"] is True
        assert answer["additional_fields_changed"] is True
        assert answer["additional_field_names"] == ["extra"]
        assert answer["identical_revision"] is False and answer["content_identical"] is False
        assert answer["comparison_scope"] == ["persona", "tone", "style", "address"]
        await harness.core.close()

    asyncio.run(scenario())


def test_a_maximum_size_revision_still_fits_the_documented_budget():
    async def scenario():
        harness = Harness(personas=persona_config())
        content = {
            "persona": "人" * 20000,
            "tone": "语" * 20000,
            "style": "风" * 20000,
            "address": "称" * 4000,
            **{"extra%02d" % index: "扩" * 4000 for index in range(12)},
        }
        revision = draft(harness, A, content)
        one = ask(harness, operation="revision", subject=A, revision_id=revision)
        assert byte_size(one) <= DOCUMENT_MAX_BYTES
        assert one["revision"]["content"]["persona"] == "人" * 20000
        other = draft(harness, A, {"persona": "人" * 19999, "tone": "语" * 20000})
        answer = ask(harness, operation="compare", subject=A, left=revision, right=other)
        assert byte_size(answer) <= DOCUMENT_MAX_BYTES
        assert answer["fields"]["persona"]["change"] == "modified"
        # The comparison is one document with a hard bound; the single-revision read is the
        # documented way to read either side in full.
        assert answer["left"]["revision_id"] == revision
        await harness.core.close()

    asyncio.run(scenario())


# ------------------------------------------------------- reading writes nothing, ever


def test_reading_writes_no_fact_no_pointer_and_no_ledger_row():
    async def scenario():
        harness = Harness(personas=persona_config())
        for index in range(5):
            draft(harness, A, {"persona": "版本 %d" % index})
        store = harness.core.store
        before = {table: len(store.list(table)) for table in sorted(PERSONA_TABLES)}
        head = store.source_head()
        versions = {subject: harness.core.personas.get(subject)["version"] for subject in (A, B)}
        first = ask(harness, operation="history_page", subject=A, kind="revisions", limit=2)
        seed = ask(harness, operation="get", subject=A)["persona"]["published_revision"]
        revision = ask(harness, operation="history_page", subject=A, kind="revisions")["entries"][
            -1
        ]
        for document in (
            {"operation": "catalog", "limit": 1},
            {
                "operation": "catalog",
                "limit": 1,
                "cursor": ask(harness, operation="catalog", limit=1)["next_cursor"],
            },
            {
                "operation": "history_page",
                "subject": A,
                "kind": "revisions",
                "limit": 2,
                "cursor": first["next_cursor"],
            },
            {"operation": "history_page", "subject": A, "kind": "approvals"},
            {"operation": "history_page", "subject": A, "kind": "publications"},
            {"operation": "history_page", "subject": A, "kind": "rollbacks"},
            {"operation": "revision", "subject": A, "revision_id": revision["revision_id"]},
            {"operation": "compare", "subject": A, "left": seed, "right": revision["revision_id"]},
        ):
            assert ask(harness, **document)["schema_version"] == 1
        assert {table: len(store.list(table)) for table in sorted(PERSONA_TABLES)} == before
        assert store.source_head() == head
        assert {
            subject: harness.core.personas.get(subject)["version"] for subject in (A, B)
        } == versions
        assert store.list("persona_operations") == []
        await harness.core.close()

    asyncio.run(scenario())


def test_a_page_read_does_not_grow_with_the_history_behind_it():
    async def scenario():
        measurements = []
        for extra in (0, 30):
            harness = Harness(personas=persona_config())
            for index in range(4 + extra):
                draft(harness, A, {"persona": "版本 %d" % index})
            queries = []
            harness.core.store.db.set_trace_callback(queries.append)
            page = ask(harness, operation="history_page", subject=A, kind="revisions", limit=3)
            harness.core.store.db.set_trace_callback(None)
            history = [query for query in queries if "persona_revisions" in query]
            assert history and all("LIMIT" in query for query in history)
            assert any("ORDER BY position,id LIMIT" in query for query in history)
            measurements.append((page["count"], len(queries)))
            await harness.core.close()
        # The same page size costs the same number of statements whatever the history holds:
        # nothing is read into memory and sliced, and every read is bounded by its own LIMIT.
        assert measurements[0] == measurements[1] == (3, measurements[0][1])
        assert measurements[0][1] <= 12

    asyncio.run(scenario())


def test_the_directory_read_is_bounded_the_same_way():
    async def scenario():
        measurements = []
        for count in (4, 40):
            harness = Harness(personas=directory(count))
            queries = []
            harness.core.store.db.set_trace_callback(queries.append)
            page = ask(harness, operation="catalog", limit=3)
            harness.core.store.db.set_trace_callback(None)
            directory_query = [
                query for query in queries if query.startswith("SELECT body FROM persona_personas")
            ]
            assert len(directory_query) == 1
            assert "ORDER BY id LIMIT" in directory_query[0]
            assert "OFFSET" not in directory_query[0].upper()
            measurements.append((page["count"], len(queries)))
            await harness.core.close()
        assert measurements[0] == measurements[1] == (3, measurements[0][1])

    asyncio.run(scenario())


def test_a_full_page_stays_inside_its_byte_budget():
    async def scenario():
        harness = Harness(personas=directory(40))
        catalog = ask(harness, operation="catalog", limit=100)
        assert catalog["count"] == 40
        assert byte_size(catalog) <= PAGE_MAX_BYTES
        history = ask(
            harness, operation="history_page", subject="actor:00", kind="revisions", limit=100
        )
        assert byte_size(history) <= PAGE_MAX_BYTES
        # A bounded page never returns more than it was asked for, and it always says whether
        # there is more instead of ending short and looking complete.
        assert catalog["has_more"] is False and catalog["next_cursor"] is None
        assert history["has_more"] is False and history["next_cursor"] is None
        await harness.core.close()

    asyncio.run(scenario())


def test_reads_are_served_by_the_single_application_entry_point_only():
    """No second entry point: both adapters submit the same read document."""
    store = Store(":memory:")
    try:
        personas = Personas(store, lambda: 1000.0)
        personas.import_config(persona_config())
        catalog = personas.manage({"operation": "catalog"})
        assert catalog["operation"] == "catalog" and catalog["consistency"] == "live_keyset"
        page = personas.manage({"operation": "history_page", "subject": A, "kind": "revisions"})
        assert page["operation"] == "history_page"
        seed = personas.get(A)["published_revision"]
        one = personas.manage({"operation": "revision", "subject": A, "revision_id": seed})
        assert one["operation"] == "revision" and one["revision"]["content"] == {
            "persona": "Role A"
        }
        two = personas.manage({"operation": "compare", "subject": A, "left": seed, "right": seed})
        assert two["operation"] == "compare" and two["content_identical"] is True
    finally:
        store.close()
