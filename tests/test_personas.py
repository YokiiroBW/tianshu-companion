"""Registered persona versions: drafts, immutable revisions, explicit publication, snapshots.

Synthetic data only. The recording gateway double in `support` lets the assertions read the
exact system prompt a revision produced, so "an unapproved revision never reaches a model"
is checked on the real request bytes rather than on an internal flag.
"""

import asyncio
import sqlite3
from contextlib import closing

import pytest

from support import Harness, persona_config
from tianshu_companion.contracts import Fault
from tianshu_companion.personas import (
    REVISION_SUFFIX,
    PersonaError,
    Personas,
    deployment,
    fingerprint,
    normalize,
)
from tianshu_companion.store import PERSONA_TABLES, Store

A = "actor:a"
B = "actor:b"


def personas_of(harness):
    return harness.core.personas


async def send(harness, text="你好"):
    await harness.ingest(text=text)
    harness.clock.advance(6)
    await harness.cycles()


async def seal_turn(harness, text):
    """Ingest one message, seal it, and return the turn a tick prepared from it."""
    await harness.ingest(text=text)
    harness.clock.advance(6)
    await harness.core.tick()
    return harness.turns()[-1]


def system_prompts(harness):
    return [messages[0]["content"] for _, messages in harness.gateway.calls]


def drafts(harness, subject, content, *, operator="admin", reason="editorial change"):
    """Draft `content` against the version the caller just read."""
    return personas_of(harness).draft(
        subject,
        content,
        operator=operator,
        expected=personas_of(harness).get(subject)["version"],
        reason=reason,
    )


def approve(harness, subject, revision, *, operator="admin"):
    return personas_of(harness).approve(
        subject,
        revision,
        operator=operator,
        expected=personas_of(harness).get(subject)["version"],
        reason="reviewed",
    )


def publish(harness, subject, revision, *, operator="admin"):
    return personas_of(harness).publish(
        subject,
        revision,
        operator=operator,
        expected=personas_of(harness).get(subject)["version"],
        reason="released",
    )


def publish_content(harness, subject, content, *, operator="admin"):
    revision = drafts(harness, subject, content, operator=operator)["revision"]["revision_id"]
    approve(harness, subject, revision, operator=operator)
    return publish(harness, subject, revision, operator=operator)


# --------------------------------------------------------------- deployment import


def test_initial_configuration_import_is_idempotent_and_restart_safe(tmp_path):
    document = persona_config()
    path = tmp_path / "personas.db"

    async def scenario():
        harness = Harness(path, personas=document)
        store = harness.core.store
        head = store.source_head()
        first = personas_of(harness).get(A)
        assert first["state"] == "published"
        assert first["published"]["content"] == {"persona": "Role A"}
        assert first["published"]["source"] == "initial_config"
        assert store.list("persona_imports") and len(store.list("persona_revisions")) == 2
        # Persona facts are versioned history, not source facts: the durable source head
        # that Memory reconciles against must not move.
        assert store.source_head() == head
        await harness.core.close()

        # A restart on the same source imports nothing and keeps the same live revision.
        again = Harness(path, personas=document)
        assert (
            personas_of(again).get(A)["published"]["revision_id"]
            == (first["published"]["revision_id"])
        )
        assert len(again.core.store.list("persona_revisions")) == 2
        assert len(again.core.store.list("persona_imports")) == 1
        await again.core.close()

        # A published version is never replaced by a redeployment, even at a new version.
        # The deployment only records its pending identity; a person must draft and publish.
        redeployed = persona_config(version=2, **{A: dict(version=9, persona="Redeployed A")})
        restarted = Harness(path, personas=redeployed)
        after = personas_of(restarted).get(A)
        assert after["published"]["content"] == {"persona": "Role A"}
        assert after["published_revision"] == first["published"]["revision_id"]
        assert after["state"] == "published" and after["draft"] is None
        assert after["imported"] == 9
        assert any(
            r["content"] == {"persona": "Redeployed A"} for r in personas_of(restarted).revisions(A)
        )
        assert len(restarted.core.store.list("persona_publications")) == 2
        assert restarted.core.store.list("persona_approvals") == []
        await restarted.core.close()

    asyncio.run(scenario())


def test_deployment_shape_has_one_owner():
    document = dict(
        config_version=4,
        roles={A: {"version": 1, "persona": "plain"}},
        personas={"roles": {A: {"version": 1, "persona": "registered"}}},
    )
    assert deployment(document) == (4, {A: {"version": 1, "persona": "registered"}})
    assert deployment(dict(config_version=4, roles={A: "text"})) == (4, {A: "text"})
    with pytest.raises(PersonaError):
        deployment({"config_version": 1})


# ------------------------------------------------------------------- draft lifecycle


def test_a_draft_is_never_used_until_it_is_approved_and_published():
    async def scenario():
        harness = Harness(personas=persona_config())
        await send(harness, "第一轮")
        assert "Role A" in system_prompts(harness)[0]

        revision = drafts(harness, A, {"persona": "Draft A"})["revision"]["revision_id"]
        after_draft = personas_of(harness).get(A)
        assert after_draft["state"] == "draft" and after_draft["pending_approval"] is False
        await send(harness, "草稿期间")
        assert "Draft A" not in system_prompts(harness)[1]
        assert "Role A" in system_prompts(harness)[1]

        approve(harness, A, revision)
        assert personas_of(harness).get(A)["state"] == "approved"
        await send(harness, "批准但未发布")
        assert "Draft A" not in system_prompts(harness)[2]

        publish(harness, A, revision)
        await send(harness, "发布之后")
        assert "Draft A" in system_prompts(harness)[3]
        assert harness.core.store.get("turns", harness.turns()[3]["id"])["role"]["revision_id"] == (
            revision
        )
        await harness.core.close()

    asyncio.run(scenario())


def test_publication_requires_an_approval_of_exactly_these_bytes():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        revision = drafts(harness, A, {"persona": "Draft A"})["revision"]["revision_id"]
        with pytest.raises(PersonaError) as unapproved:
            publish(harness, A, revision)
        assert unapproved.value.code == "invalid_input"
        assert personas.get(A)["published"]["content"] == {"persona": "Role A"}

        approve(harness, A, revision)
        assert publish(harness, A, revision)["persona"]["published_revision"] == revision
        # Republishing the live revision replays one publication, never a second row.
        assert publish(harness, A, revision)["persona"]["published_revision"] == revision
        assert len(personas.publications(A)) == 2  # the seed and this publication
        await harness.core.close()

    asyncio.run(scenario())


def test_a_rejected_draft_does_not_survive_as_an_approval():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        revision = drafts(harness, A, {"persona": "Draft A"})["revision"]["revision_id"]
        approve(harness, A, revision)
        personas.reject(
            A, revision, operator="admin", expected=personas.get(A)["version"], reason="not good"
        )
        after = personas.get(A)
        assert after["state"] == "published" and after["draft"] is None
        # The history keeps the revision, but the withdrawn approval can no longer release it.
        assert [r["revision_id"] for r in personas.revisions(A)].count(revision) == 1
        with pytest.raises(PersonaError) as withdrawn:
            publish(harness, A, revision)
        assert withdrawn.value.code == "invalid_input"
        assert harness.core.store.get("persona_revisions", revision)["content"] == {
            "persona": "Draft A"
        }
        await harness.core.close()

    asyncio.run(scenario())


def test_two_editors_cannot_overwrite_each_others_draft():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        seen = personas.get(A)["version"]
        first = personas.draft(
            A, {"persona": "Editor one"}, operator="editor-1", expected=seen, reason="one"
        )
        with pytest.raises(PersonaError) as stale:
            personas.draft(
                A, {"persona": "Editor two"}, operator="editor-2", expected=seen, reason="two"
            )
        assert stale.value.code == "version_conflict"
        # The loser's text is nowhere: neither as a draft nor as a revision row.
        assert personas.get(A)["draft"]["content"] == {"persona": "Editor one"}
        assert all(r["content"] != {"persona": "Editor two"} for r in personas.revisions(A))
        assert first["persona"]["version"] == seen + 1
        await harness.core.close()

    asyncio.run(scenario())


def test_identical_content_from_the_same_parent_is_one_revision_and_replay_is_refused():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        first = drafts(harness, A, {"persona": "Same text"})["revision"]
        personas.draft(
            A,
            {"persona": "Other text"},
            operator="admin",
            expected=personas.get(A)["version"],
            reason="move on",
        )
        second = personas.draft(
            A,
            {"persona": "Same text"},
            operator="admin",
            expected=personas.get(A)["version"],
            reason="same content again",
        )["revision"]
        # Same bytes from the same parent revision are the same immutable revision: this is
        # a replay of one revision, not a second copy of the same text.
        assert second["revision_id"] == first["revision_id"]
        assert second["parent"] == first["parent"]
        assert (
            len([r for r in personas.revisions(A) if r["content"] == {"persona": "Same text"}]) == 1
        )
        assert personas.get(A)["draft"]["revision_id"] == first["revision_id"]

        row = harness.core.store.get("persona_revisions", first["revision_id"])
        harness.core.store.put(
            "persona_revisions", dict(row, content={"persona": "tampered"}, fingerprint="f" * 64)
        )
        with pytest.raises(PersonaError) as immutable:
            personas.draft(
                A,
                {"persona": "Same text"},
                operator="admin",
                expected=personas.get(A)["version"],
                reason="replay",
            )
        assert immutable.value.code == "invalid_input"
        await harness.core.close()

    asyncio.run(scenario())


# ----------------------------------------------------------------- operation identity


def with_request(operation, subject=A, **fields):
    """One application operation document, as an adapter would submit it."""
    document = {"operation": operation, "subject": subject, "request_id": "ops:1"}
    document.update(fields)
    return document


def test_a_replayed_request_returns_its_recorded_result_and_writes_nothing_again():
    async def scenario():
        harness = Harness(personas=persona_config())
        core = harness.core
        personas = personas_of(harness)
        document = {
            "operation": "draft",
            "subject": A,
            "operator": "admin",
            "reason": "editorial change",
            "expected": personas.get(A)["version"],
            "content": {"persona": "Requested text"},
            "request_id": "ops:draft-1",
        }
        first = core.manage_persona("persona_admin", document)
        revisions = len(personas.revisions(A))

        # The response was lost; the caller retries the very same request.
        replay = core.manage_persona("persona_admin", dict(document))
        assert replay == first  # the recorded result, not a re-run
        assert len(personas.revisions(A)) == revisions
        assert len(core.store.list("persona_operations")) == 1
        assert personas.get(A)["version"] == first["persona"]["version"]

        # A different operation under its own identity is a separate request.
        other = core.manage_persona(
            "persona_admin",
            dict(
                document,
                request_id="ops:draft-2",
                expected=personas.get(A)["version"],
                content={"persona": "Second text"},
            ),
        )
        assert other["revision"]["content"] == {"persona": "Second text"}
        assert len(core.store.list("persona_operations")) == 2
        await core.close()

    asyncio.run(scenario())


def test_one_request_id_cannot_carry_two_different_requests():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        first = harness.core.manage_persona(
            "persona_admin",
            with_request(
                "draft",
                operator="admin",
                reason="one",
                expected=personas.get(A)["version"],
                content={"persona": "First"},
            ),
        )
        assert first["revision"]["content"] == {"persona": "First"}
        version = personas.get(A)["version"]
        with pytest.raises(Fault) as reused:
            harness.core.manage_persona(
                "persona_admin",
                with_request(
                    "draft",
                    operator="admin",
                    reason="one",
                    expected=version,
                    content={"persona": "Different content"},
                ),
            )
        assert reused.value.code == "invalid_input"
        # Nothing was applied and the identity still belongs to the first request.
        assert personas.get(A)["draft"]["content"] == {"persona": "First"}
        assert len(personas.revisions(A)) == 2  # the seed and the first draft
        assert len(harness.core.store.list("persona_operations")) == 1
        await harness.core.close()

    asyncio.run(scenario())


def test_a_write_without_an_operation_identity_is_refused():
    """Reads need none; every write operation refuses to run without one."""

    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        assert harness.core.manage_persona("persona_admin", {"operation": "list"})["subjects"]
        draft = dict(with_request("draft", operator="admin", reason="r"))
        draft.pop("request_id")
        draft.update(expected=personas.get(A)["version"], content={"persona": "Unidentified"})
        with pytest.raises(Fault) as missing:
            harness.core.manage_persona("persona_admin", draft)
        assert missing.value.code == "invalid_input"

        revision = drafts(harness, A, {"persona": "Pending text"})["revision"]["revision_id"]
        bare = {"subject": A, "operator": "admin", "reason": "no identity"}
        for operation in ("approve", "reject", "publish", "rollback"):
            with pytest.raises(PersonaError) as missing:
                personas.manage(
                    dict(
                        bare,
                        operation=operation,
                        revision_id=revision,
                        expected=personas.get(A)["version"],
                    )
                )
            assert missing.value.code == "invalid_input"
        for operation in ("retire", "restore"):
            with pytest.raises(PersonaError) as missing:
                personas.manage(
                    dict(bare, operation=operation, expected=personas.get(A)["version"])
                )
            assert missing.value.code == "invalid_input"
        # Not one of those attempts changed anything.
        assert personas.get(A)["retired"] is False
        assert personas.approvals(A) == []
        assert personas.publications(A) == [] or all(
            row["kind"] == "seed" for row in personas.publications(A)
        )
        assert personas.get(A)["draft"]["content"] == {"persona": "Pending text"}
        assert harness.core.store.list("persona_operations") == []
        await harness.core.close()

    asyncio.run(scenario())


def test_identical_approval_retries_never_add_a_second_decision():
    """The reviewer's finding: approval is a visible fact, and a retry must not repeat it."""

    async def scenario():
        harness = Harness(personas=persona_config())
        core = harness.core
        personas = personas_of(harness)
        revision = drafts(harness, A, {"persona": "Reviewed text"})["revision"]["revision_id"]
        document = with_request(
            "approve",
            revision_id=revision,
            operator="reviewer",
            reason="reviewed",
            expected=personas.get(A)["version"],
            request_id="ops:approve-1",
        )
        first = core.manage_persona("persona_admin", document)
        assert personas.get(A)["state"] == "approved"
        assert len(personas.approvals(A)) == 1
        approved_version = first["persona"]["version"]

        harness.clock.advance(3600)
        replay = core.manage_persona("persona_admin", dict(document))
        assert replay == first
        assert len(personas.approvals(A)) == 1
        assert personas.get(A)["version"] == approved_version
        assert len(personas.publications(A)) == 1  # only the deployment seed
        await core.close()

    asyncio.run(scenario())


def test_the_expected_version_covers_the_approval_state():
    """Two operators cannot both approve the same pending draft from one stale read."""

    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        revision = drafts(harness, A, {"persona": "Reviewed text"})["revision"]["revision_id"]
        seen = personas.get(A)["version"]
        personas.approve(A, revision, operator="reviewer-1", expected=seen, reason="reviewed")
        # A second approval of the same draft from the same read is refused: the state the
        # caller approved was already superseded, so this is a stale edit rather than a
        # second signature.
        with pytest.raises(PersonaError) as stale:
            personas.approve(A, revision, operator="reviewer-2", expected=seen, reason="reviewed")
        assert stale.value.code == "version_conflict"
        assert len(personas.approvals(A)) == 1
        # The operator who re-reads succeeds under a new request.
        personas.approve(
            A,
            revision,
            operator="reviewer-2",
            expected=personas.get(A)["version"],
            reason="reviewed",
        )
        assert len(personas.approvals(A)) == 2
        await harness.core.close()

    asyncio.run(scenario())


def test_operation_identities_are_durable_across_a_restart(tmp_path):
    path = tmp_path / "requests.db"
    document = {
        "operation": "draft",
        "subject": A,
        "operator": "admin",
        "reason": "before the restart",
        "expected": 2,
        "content": {"persona": "Durable text"},
        "request_id": "ops:durable",
    }
    recorded = []

    async def scenario():
        harness = Harness(path, personas=persona_config())
        first = harness.core.manage_persona("persona_admin", document)
        assert first["persona"]["draft"]["content"] == {"persona": "Durable text"}
        recorded.append(first)
        await harness.core.close()

    asyncio.run(scenario())

    async def restarted():
        harness = Harness(path, personas=persona_config())
        personas = personas_of(harness)
        revisions = len(personas.revisions(A))
        replay = harness.core.manage_persona("persona_admin", dict(document))
        assert replay == recorded[0]  # the result the first process committed
        assert len(personas.revisions(A)) == revisions
        assert len(harness.core.store.list("persona_operations")) == 1
        # The identity is still bound to its request after the restart, not merely the row.
        with pytest.raises(Fault) as reused:
            harness.core.manage_persona(
                "persona_admin",
                dict(document, content={"persona": "Changed after restart"}, expected=3),
            )
        assert reused.value.code == "invalid_input"
        assert personas.get(A)["draft"]["content"] == {"persona": "Durable text"}
        await harness.core.close()

    asyncio.run(restarted())


def test_an_operation_identity_is_scoped_to_the_authorization_surface():
    """Two credentials are two surfaces: one cannot block or replay the other's request."""

    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        document = with_request(
            "draft",
            operator="admin",
            reason="editorial change",
            expected=personas.get(A)["version"],
            content={"persona": "Surface one"},
        )
        first = harness.core.manage_persona("persona_admin", dict(document))
        assert first["revision"]["content"] == {"persona": "Surface one"}
        # The same request id on another surface is a different operation, and the version
        # check still applies to it.
        with pytest.raises(PersonaError) as stale:
            personas.manage(dict(document, scope="persona_console"))
        assert stale.value.code == "version_conflict"
        second = personas.manage(
            dict(document, scope="persona_console", expected=personas.get(A)["version"])
        )
        assert second["revision"] is not None
        assert len(harness.core.store.list("persona_operations")) == 2
        await harness.core.close()

    asyncio.run(scenario())


# -------------------------------------------------------------------------- history


def test_history_is_append_only_and_rollback_is_a_new_published_revision():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        original = personas.get(A)["published"]["revision_id"]
        revision = publish_content(harness, A, {"persona": "Version two"})["persona"]
        second = revision["published_revision"]
        assert second != original
        # Publishing again without a new draft is a no-op, not a second publication row.
        assert publish(harness, A, second)["persona"]["published_revision"] == second
        assert len(personas.publications(A)) == 2

        restored = personas.rollback(
            A,
            original,
            operator="admin",
            expected=personas.get(A)["version"],
            reason="version two regressed",
        )
        third = restored["restored_revision"]
        assert third not in (original, second)
        assert restored["persona"]["published"]["content"] == {"persona": "Role A"}
        assert restored["persona"]["published"]["revision_id"] == third

        # Old revisions stay byte-identical and readable.
        assert personas.revision(original)["content"] == {"persona": "Role A"}
        assert personas.revision(second)["content"] == {"persona": "Version two"}
        # The chain is linear and traceable end to end.
        assert [r["revision_id"] for r in personas.revisions(A)] == [original, second, third]
        assert [p["revision_id"] for p in personas.publications(A)] == [original, second, third]
        kinds = [p["kind"] for p in personas.publications(A)]
        assert kinds == ["seed", "publish", "rollback"]
        assert personas.rollbacks(A)[0]["target_revision"] == original
        assert personas.rollbacks(A)[0]["restored_revision"] == third
        # Going back must not revive the approval that was given for version two.
        assert personas.revisions(A)[1]["approved_by"] == "admin"
        assert personas.revisions(A)[2]["approved_by"] == "admin"
        assert restored["persona"]["published"]["approved_at"] is not None
        await harness.core.close()

    asyncio.run(scenario())


def test_rollback_only_accepts_a_previously_published_revision_and_never_retires_grants():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        unapproved = drafts(harness, A, {"persona": "Never published"})["revision"]["revision_id"]
        with pytest.raises(PersonaError):
            personas.rollback(
                A,
                unapproved,
                operator="admin",
                expected=personas.get(A)["version"],
                reason="no",
            )
        live = personas.get(A)["published"]["revision_id"]
        with pytest.raises(PersonaError):
            personas.rollback(
                A, live, operator="admin", expected=personas.get(A)["version"], reason="no-op"
            )
        assert len(personas.publications(A)) == 1
        await harness.core.close()

    asyncio.run(scenario())


def test_a_rolled_back_revision_still_cannot_restore_a_withdrawn_capability():
    """Persona text is never authority, so republishing old text revives nothing.

    The role registry is the authority for what a character may do. Rolling a persona back
    does not re-register a character whose registry entry was withdrawn, and the traffic
    guard refuses before a persona is ever read.
    """
    from tianshu_companion.contracts import Fault

    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        original = personas.get(A)["published"]["revision_id"]
        publish_content(harness, A, {"persona": "Version two"})
        # The registry authority withdraws this character's role.
        del harness.core.roles[A]
        request = harness.request()
        with pytest.raises(Fault):
            await harness.core.ingest("nonebot", request)
        # A persona rollback cannot bring the capability back.
        personas.rollback(
            A, original, operator="admin", expected=personas.get(A)["version"], reason="restore"
        )
        with pytest.raises(Fault):
            await harness.core.ingest("nonebot", harness.request())
        assert not harness.core.store.list("inbox")
        await harness.core.close()

    asyncio.run(scenario())


def test_cross_character_isolation_of_revisions_and_publication():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        other = personas.get(B)["published"]["revision_id"]
        before_a, before_b = personas.get(A)["version"], personas.get(B)["version"]

        with pytest.raises(PersonaError) as foreign:
            personas.publish(A, other, operator="admin", expected=before_a, reason="not mine")
        assert foreign.value.code == "invalid_input"
        with pytest.raises(PersonaError):
            personas.approve(A, other, operator="admin", expected=before_a, reason="not mine")
        with pytest.raises(PersonaError) as malformed:
            personas.draft(
                A,
                {"persona": "x"},
                operator="admin",
                expected=before_a,
                reason="ok",
                from_config=dict(persona="cross"),
            )
        assert malformed.value.code == "invalid_input"
        assert personas.get(A)["version"] == before_a
        assert personas.get(B)["version"] == before_b
        assert [r["revision_id"] for r in personas.revisions(B)] == [other]
        # Every revision row read through A belongs to A's chain: the parent links and the
        # stored subjects never cross over to the other character.
        assert [r["content"] for r in personas.revisions(A)] == [{"persona": "Role A"}]
        assert [r["parent"] for r in personas.revisions(A)] == [None]
        assert all(row["subject"] == A for row in harness.core.store.list("persona_revisions", A))
        # Reading another character's history requires naming it, not borrowing a revision.
        assert personas.revisions(B)[0]["content"] == {"persona": "Role B"}
        await harness.core.close()

    asyncio.run(scenario())


def test_retire_stops_new_turns_without_touching_prepared_ones():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        personas.retire(A, operator="admin", expected=personas.get(A)["version"], reason="off duty")
        assert personas.get(A)["state"] == "retired"
        with pytest.raises(PersonaError):
            personas.pin(A)
        await send(harness, "退休之后")
        turn = harness.turns()[0]
        assert turn["phase"] == "failed" and turn["failure"] == "Persona is retired"
        assert not harness.gateway.calls

        personas.restore(
            A, operator="admin", expected=personas.get(A)["version"], reason="back on duty"
        )
        await send(harness, "恢复之后")
        assert "Role A" in system_prompts(harness)[0]
        await harness.core.close()

    asyncio.run(scenario())


def test_recover_reports_an_unresolved_pointer_instead_of_inventing_one(tmp_path):
    path = tmp_path / "recover.db"

    async def scenario():
        harness = Harness(path, personas=persona_config())
        live = personas_of(harness).get(A)["published_revision"]
        await harness.core.close()

        with closing(Store(path)) as store:
            store.delete("persona_revisions", live)
        restarted = Harness(path, personas=persona_config())
        restarted.core.recover()
        assert restarted.core.persona_problems == [dict(subject=A, pointer="published_revision")]
        # The pointer is preserved for an explicit repair; nothing was re-approved.
        assert personas_of(restarted).store.get("persona_personas", A)["published_revision"] == live
        await restarted.core.close()

    asyncio.run(scenario())


def test_persona_content_is_bounded_and_scalar_only():
    assert normalize({"persona": "text", "tone": "calm"}) == {"persona": "text", "tone": "calm"}
    for bad in (
        {},
        {"persona": ""},
        {"tone": "no persona"},
        {"persona": "x" * 20001},
        {"persona": "ok", "nested": {"permissions": ["admin"]}},
        {"persona": "ok", "nested": [1]},
    ):
        with pytest.raises(PersonaError):
            normalize(bad)
    # Scalars stay scalars: a persona can carry plain flags and numbers, never a structure
    # that an adapter might later read as authority.
    assert normalize({"persona": "ok", "reviewed": True, "weight": 2})["reviewed"] is True
    assert fingerprint(normalize({"persona": "ok"})) == fingerprint({"persona": "ok"})
    assert REVISION_SUFFIX == "persona-revision/v1"


# ------------------------------------------------------------------ snapshot timing


def test_publication_lands_on_turns_prepared_after_it_and_never_on_earlier_ones():
    async def scenario():
        harness = Harness(personas=persona_config())
        # The gate holds the first turn inside generation, which is the strongest form of
        # "already prepared": the publication happens while that turn is still live.
        gate = asyncio.Event()
        harness.gateway.gates[1] = gate

        first = await seal_turn(harness, "在途")
        await harness.core.tick()
        in_flight = harness.core.store.get("turns", first["id"])
        assert in_flight["phase"] == "generating"  # held by the gate, already prepared
        pinned = in_flight["role"]["revision_id"]

        # The second message seals into a second turn. A turn that has not reached its
        # preparation boundary carries no persona at all, and this publication is exactly
        # what its boundary will read.
        await harness.ingest(text="等待")
        harness.clock.advance(6)
        harness.core._seal_due(harness.clock())
        second = harness.turns()[-1]
        waiting = harness.core.store.get("turns", second["id"])
        assert waiting["phase"] == "queued" and waiting["role"] is None
        assert waiting["config_version"] is None

        revision = publish_content(harness, A, {"persona": "Published mid-flight"})
        new_live = revision["persona"]["published_revision"]
        assert new_live != pinned
        assert harness.core.store.get("turns", first["id"])["role"]["revision_id"] == pinned

        gate.set()
        await harness.cycles(80)
        refreshed = {t["id"]: t for t in harness.turns()}
        assert refreshed[first["id"]]["phase"] == "sent"
        assert refreshed[first["id"]]["role"]["revision_id"] == pinned
        assert "Role A" in system_prompts(harness)[0]
        # The unprepared turn pins the revision live at its own preparation boundary.
        assert refreshed[second["id"]]["role"]["revision_id"] == new_live
        assert refreshed[second["id"]]["phase"] == "sent"
        assert "Published mid-flight" in system_prompts(harness)[1]
        await harness.core.close()

    asyncio.run(scenario())


def test_a_turn_keeps_its_persona_through_generation_and_delivery():
    async def scenario():
        harness = Harness(personas=persona_config())
        gate = asyncio.Event()
        harness.gateway.gates[1] = gate
        first = await seal_turn(harness, "在途")
        turn = harness.core.store.get("turns", first["id"])
        assert turn["phase"] in {"preparing", "generating"}
        pinned = turn["role"]["revision_id"]

        publish_content(harness, A, {"persona": "Later version"})
        gate.set()
        await harness.cycles(80)
        finished = harness.core.store.get("turns", first["id"])
        assert finished["phase"] == "sent"
        assert finished["role"]["revision_id"] == pinned
        assert "Role A" in system_prompts(harness)[0]
        assert all("Later version" not in prompt for prompt in system_prompts(harness))
        await harness.core.close()

    asyncio.run(scenario())


def test_a_corrupted_pinned_snapshot_fails_the_turn_instead_of_calling_a_model():
    async def scenario():
        harness = Harness(personas=persona_config())
        # A snapshot whose bytes no longer match its own revision is a corrupt or tampered
        # persona, and every model call re-reads it rather than trusting the copy pinned at
        # preparation. A turn holding one fails where it stands.
        live = personas_of(harness).get(A)["published"]
        await harness.ingest(text="待处理")
        harness.clock.advance(6)
        harness.core._seal_due(harness.clock())
        turn = harness.turns()[0]
        assert turn["phase"] == "queued" and turn["role"] is None
        harness.core.store.put(
            "turns",
            dict(
                turn,
                config_version=harness.core.config_version,
                role=dict(
                    live,
                    persona="tampered",
                    content={"persona": "tampered"},
                    pinned_at=harness.clock(),
                ),
            ),
        )
        await harness.core._process(turn["id"])
        finished = harness.turns()[0]
        assert finished["phase"] == "failed"
        assert finished["failure"] == "dependency_unavailable"
        assert not harness.gateway.calls
        await harness.core.close()

    asyncio.run(scenario())


def test_a_character_without_a_published_revision_cannot_prepare_a_turn():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        # The live pointer is cleared, as a withdrawn publication would leave it.
        personas.store.put(
            "persona_personas",
            dict(personas.store.get("persona_personas", A), published_revision=None),
        )
        await send(harness, "无已发布角色")
        turn = harness.turns()[0]
        assert turn["phase"] == "failed"
        assert turn["failure"] == "Persona is not published"
        assert not harness.gateway.calls
        await harness.core.close()

    asyncio.run(scenario())


# ----------------------------------------------------------------------- management


def test_management_port_is_gated_on_its_own_service_credential():
    from tianshu_companion.contracts import Fault

    async def scenario():
        harness = Harness(personas=persona_config())
        core = harness.core
        for service in ("nonebot", "platform", "memory"):
            with pytest.raises(Fault) as forbidden:
                core.manage_persona(service, {"operation": "list"})
            assert forbidden.value.code == "forbidden"
            # Browsing is the same one credential: a chat, ingest or bridge service reaches
            # neither a write nor a read of the registered personas.
            with pytest.raises(Fault) as browse:
                core.manage_persona(service, {"operation": "catalog"})
            assert browse.value.code == "forbidden"
        listed = core.manage_persona("persona_admin", {"operation": "list"})
        assert listed["subjects"] == [A, B]
        catalog = core.manage_persona("persona_admin", {"operation": "catalog"})
        assert [entry["subject"] for entry in catalog["entries"]] == [A, B]
        assert catalog["consistency"] == "live_keyset"
        directory = core.manage_persona(
            "persona_admin", {"operation": "history_page", "subject": A, "kind": "revisions"}
        )
        assert directory["count"] == 1 and directory["has_more"] is False

        with pytest.raises(Fault) as conflict:
            core.manage_persona(
                "persona_admin",
                {
                    "operation": "draft",
                    "subject": A,
                    "operator": "admin",
                    "reason": "x",
                    "expected": 99,
                    "content": {"persona": "y"},
                    "request_id": "port:conflict",
                },
            )
        assert conflict.value.code == "version_conflict"
        with pytest.raises(Fault) as unknown:
            core.manage_persona("persona_admin", {"operation": "get", "subject": "actor:ghost"})
        assert unknown.value.code == "not_found"
        with pytest.raises(Fault) as missing:
            core.manage_persona(
                "persona_admin", {"operation": "revision", "subject": A, "revision_id": "0" * 64}
            )
        assert missing.value.code == "not_found"
        with pytest.raises(Fault) as bad:
            core.manage_persona("persona_admin", {"operation": "explode"})
        assert bad.value.code == "invalid_input"
        await harness.core.close()

    asyncio.run(scenario())


def test_management_port_refuses_without_registered_personas():
    from tianshu_companion.contracts import Fault

    async def scenario():
        harness = Harness()  # personas not enabled: the port is absent, not open
        for document in ({"operation": "list"}, {"operation": "catalog"}):
            with pytest.raises(Fault) as error:
                harness.core.manage_persona("persona_admin", document)
            assert error.value.code == "dependency_unavailable"
        await harness.core.close()

    asyncio.run(scenario())


def test_management_can_import_and_draft_from_the_deployment_document():
    async def scenario():
        harness = Harness(personas=persona_config())
        personas = personas_of(harness)
        document = persona_config(version=7, **{"actor:c": dict(version=3, persona="Role C")})
        result = harness.core.manage_persona(
            "persona_admin", {"operation": "import", "config": document}
        )
        assert result["imported"] == ["actor:c"] and result["skipped"] is False
        assert personas.get("actor:c")["published"]["content"] == {"persona": "Role C"}
        replayed = harness.core.manage_persona(
            "persona_admin", {"operation": "import", "config": document}
        )
        assert replayed["skipped"] is True and replayed["unchanged"] == ["actor:c"]

        drafted = harness.core.manage_persona(
            "persona_admin",
            {
                "operation": "draft",
                "subject": A,
                "operator": "admin",
                "reason": "config moved",
                "expected": personas.get(A)["version"],
                "from_config": dict(persona="Deployed A"),
                "request_id": "port:draft-from-config",
            },
        )
        assert drafted["revision"]["content"] == {"persona": "Deployed A"}
        assert drafted["persona"]["state"] == "draft"
        await harness.core.close()

    asyncio.run(scenario())


# ------------------------------------------------------------------------ migration


def test_v7_database_migrates_to_v9_with_backup_and_failed_migration_recovery(tmp_path):
    path = tmp_path / "legacy.db"
    with closing(Store(path)) as store:
        head = store.source_head()
        store.put("conversations", {"id": "synthetic", "private": "synthetic preserved"})
    with closing(sqlite3.connect(path)) as db, db:
        for table in PERSONA_TABLES:
            db.execute(f"DROP TABLE {table}")
        db.execute("PRAGMA user_version=7")
        # A conflicting index name makes the migration fail midway, after prior DDL.
        db.execute("CREATE TABLE persona_revisions_subject (id TEXT)")
    with pytest.raises(sqlite3.OperationalError):
        Store(path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 7
        assert not db.execute(
            "SELECT name FROM sqlite_master WHERE name='persona_personas'"
        ).fetchone()
        db.execute("DROP TABLE persona_revisions_subject")
        db.commit()
    with closing(Store(path)) as store:
        assert store.source_head() == head
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 9
        assert store.get("conversations", "synthetic")["private"] == "synthetic preserved"
        assert store.list("persona_personas") == []
        assert store.list("persona_operations") == []
    # One backup per open, labelled with the version the process started from: this file was
    # at v7, so the persona step is the label even though the open also applies the v9 step.
    backups = sorted(tmp_path.glob("*.pre-persona-v8-*.bak"))
    assert len(backups) == 2
    for backup in backups:
        with closing(sqlite3.connect(backup)) as db:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 7
            assert (
                "synthetic preserved" in db.execute("SELECT body FROM conversations").fetchone()[0]
            )


def test_import_on_a_migrated_legacy_database_registers_the_deployed_roles(tmp_path):
    path = tmp_path / "legacy-roles.db"
    with closing(Store(path)) as store:
        assert store.list("persona_personas") == []
    with closing(sqlite3.connect(path)) as db, db:
        for table in PERSONA_TABLES:
            db.execute(f"DROP TABLE {table}")
        db.execute("PRAGMA user_version=7")

    async def scenario():
        harness = Harness(path, personas=persona_config(version=12))
        people = personas_of(harness)
        assert people.subjects() == [A, B]
        assert people.get(A)["published"]["content"] == {"persona": "Role A"}
        assert people.get(A)["imported"] == 1
        # The import is keyed by source; importing the same deployment again does nothing.
        assert people.import_config(persona_config(version=12))["skipped"] is True
        head = harness.core.store.source_head()
        await send(harness, "迁移后仍可对话")
        assert "Role A" in system_prompts(harness)[0]
        assert harness.core.store.source_head() != head  # chat still commits source facts
        await harness.core.close()

    asyncio.run(scenario())
    with closing(Store(path)) as store:
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 9
        assert len(store.list("persona_personas")) == 2


def test_personas_module_never_writes_outside_its_own_tables(tmp_path):
    """Data ownership: the persona domain writes persona tables and nothing else."""
    path = tmp_path / "ownership.db"

    async def scenario():
        harness = Harness(path, personas=persona_config())
        core = harness.core
        before = {
            table: len(core.store.list(table))
            for table in ("turns", "replies", "collections", "inbox", "outbox", "conversations")
        }
        personas = personas_of(harness)
        revision = drafts(harness, A, {"persona": "Owned"})["revision"]["revision_id"]
        approve(harness, A, revision)
        publish(harness, A, revision)
        personas.rollback(
            A,
            personas.publications(A)[0]["revision_id"],
            operator="admin",
            expected=personas.get(A)["version"],
            reason="back",
        )
        after = {table: len(core.store.list(table)) for table in before}
        assert before == after
        await harness.core.close()

    asyncio.run(scenario())


def test_direct_persona_object_shares_the_single_application_entry_point(tmp_path):
    """The CLI and the management port submit the same document to the same use case."""
    path = tmp_path / "same-port.db"
    with closing(Store(path)) as store:
        personas = Personas(store, lambda: 1000.0)
        assert personas.import_config(persona_config())["imported"] == [A, B]
        drafted = personas.manage(
            {
                "operation": "draft",
                "subject": A,
                "operator": "cli-admin",
                "reason": "from cli",
                "expected": personas.get(A)["version"],
                "content": {"persona": "CLI text"},
                "request_id": "cli:1",
            }
        )
        assert drafted["operation"] == "draft"
        assert drafted["persona"]["draft"]["content"] == {"persona": "CLI text"}
        assert drafted["persona"]["draft"]["operator"] == "cli-admin"
