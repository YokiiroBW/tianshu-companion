"""AL13 owner doubles: time windows, withdrawal and private/public separation."""

import asyncio
import copy
import json
from types import SimpleNamespace

import pytest

from test_life import setup
from tianshu_companion.contracts import Fault
from tianshu_companion.diary_materials import civil_window


class Memory:
    available = True

    def __init__(self, life):
        self.life = life
        self.revoked = False
        self.queries, self.derived = [], []
        self.core = SimpleNamespace(proactive=None)
        self.ref = dict(episode_id="episode:read", expected_revision=1)
        self.item = dict(
            ref=dict(kind="episode", id="episode:read", revision=1),
            episode_refs=[self.ref],
            fact=dict(
                source={},
                relation="participant",
                provenance="digital_action",
                summary="Actually read pages 2 to 3, with a missing figure",
                occurred_at="2026-09-13T10:00:00Z",
                learned_at="2026-09-14T11:00:00Z",
                content_reads=[
                    dict(
                        coverage=[
                            dict(unit="pages", start=2, end=3, complete=False, gaps=["figure"])
                        ]
                    )
                ],
            ),
        )

    async def query(self, actor, context, **options):
        self.queries.append((copy.deepcopy(context), copy.deepcopy(options)))
        return dict(
            actor_id=actor,
            role_version=7,
            context=context,
            items=[] if context["audience"]["kind"] == "public" else [copy.deepcopy(self.item)],
            omitted=False,
            complete=True,
            next_cursor=None,
        )

    async def check(self, page):
        if self.revoked:
            raise Fault("scope_changed")

    async def append_validated_derived(self, actor, operation, call, value, known, validate):
        await validate()
        assert all(
            json.dumps(ref, sort_keys=True, separators=(",", ":")) in known
            for ref in value["episode_refs"]
        )
        self.derived.append(value)


def test_yesterday_window_uses_civil_dst_boundaries():
    assert civil_window("2026-03-08", "America/New_York", 0) == {
        "start": "2026-03-08T05:00:00.000Z",
        "end": "2026-03-09T04:00:00.000Z",
    }
    assert civil_window("2026-11-01", "America/New_York", 0) == {
        "start": "2026-11-01T04:00:00.000Z",
        "end": "2026-11-02T05:00:00.000Z",
    }


def test_late_learning_preserves_occurrence_and_no_subscription_keeps_private():
    async def run():
        life, clock, model = setup()
        memory = life.actor_memory = Memory(life)
        model.output = json.dumps(
            dict(
                content="I read only these pages.",
                reflections=[
                    dict(
                        kind="reflection",
                        text="The missing figure leaves my understanding tentative.",
                        episode_refs=[memory.ref],
                        importance="normal",
                        retention="medium",
                    )
                ],
                questions=["What does the missing figure show?"],
            )
        )
        await life._diary_work("a")
        diary = life.store.list("life_diaries")[0]
        assert diary["state"] == "draft" and diary["published_revision"] is None
        assert not diary["fictional"]
        assert diary["provenance"]["source_kinds"] == ["digital_experience"]
        material = life.store.get("life_materials", diary["id"])["material"]
        assert material["entries"][0]["occurred_at"] != material["entries"][0]["learned_at"]
        assert (
            memory.derived[0]["text"]
            != life.store.get("life_revisions", diary["current_revision"])["content"]
        )
        concerns = life.store.list("life_concerns")
        assert concerns[0]["actor_only"] and concerns[0]["autonomy_basis"]["items"]
        assert not life.store.list("proactive_motives")
        assert len(model.calls) == 1
        with pytest.raises(Fault):
            life.admin_read_revision(diary["current_revision"])
        assert (await life.admin_read_revision_verified(diary["current_revision"]))["content"]
        memory.revoked = True
        with pytest.raises(Fault):
            await life.admin_read_revision_verified(diary["current_revision"])
        life.store.close()

    asyncio.run(run())


def test_withdrawal_during_generation_cannot_commit_or_relearn():
    async def run():
        life, clock, model = setup()
        memory = life.actor_memory = Memory(life)

        async def revoke(turn, messages):
            memory.revoked = True
            return [json.dumps(dict(content="Stale text", reflections=[], questions=[]))], {
                "fixture": True
            }

        model.generate = revoke
        await life._diary_work("a")
        assert life.store.list("life_diaries")[0]["state"] == "failed"
        assert not life.store.list("life_revisions")
        assert memory.derived == []
        life.store.close()

    asyncio.run(run())


def test_budget_empty_page_never_means_no_experiences():
    async def run():
        life, clock, model = setup()
        memory = life.actor_memory = Memory(life)

        async def incomplete(actor, context, **options):
            return dict(
                actor_id=actor,
                role_version=7,
                context=context,
                items=[],
                omitted=True,
                complete=False,
                next_cursor="same-position",
            )

        memory.query = incomplete
        result = await life.diary_sources.prepare("a", "2026-09-13")
        assert result["state"] == "material_incomplete"
        assert result["provenance"]["selection_complete"] is False
        assert model.calls == []
        life.store.close()

    asyncio.run(run())


def test_public_material_is_selected_independently_without_private_prose():
    async def run():
        life, clock, model = setup()
        life.actor_memory = Memory(life)
        private = await life.diary_sources.prepare("a", "2026-09-13")
        public = await life.diary_sources.prepare("a", "2026-09-13", public=True)
        assert private["id"] != public["id"]
        assert public["state"] == "skipped"
        public_material = life.store.get("life_materials", public["id"])["material"]
        assert public_material["entries"] == []
        with pytest.raises(Fault):
            await life.diary_sources.publish(
                private["id"], reviewer="admin", expected=private["version"]
            )
        life.store.close()

    asyncio.run(run())


def test_real_diary_reader_rechecks_each_read_and_sync_path_cannot_bypass():
    from test_life_read import live, published, revision_body, SERVICE

    async def run():
        fixture, port = live()
        diary = published(fixture)
        provenance = dict(
            kind="digital",
            source_kinds=["digital_experience"],
            selection_complete=True,
            timezone="UTC",
            occurred_from="2026-09-20T00:00:00Z",
            occurred_before="2026-09-21T00:00:00Z",
        )
        diary.update(
            fictional=False, provenance=provenance, source_capture={"proofs": ["private-proof"]}
        )
        fixture.store.put("life_diaries", diary)
        revision = fixture.store.get("life_revisions", diary["published_revision"])
        revision.update(fictional=False, provenance=provenance)
        fixture.store.put("life_revisions", revision)
        checks = []

        async def check(item, *, public):
            checks.append(item["id"])
            assert public

        port.source_checker = check
        with pytest.raises(Fault):
            port.handle(SERVICE, "revision", revision_body(diary))
        result = await port.handle_verified(SERVICE, "revision", revision_body(diary))
        assert result["fictional"] is False and result["provenance"] == provenance
        assert "private-proof" not in json.dumps(result)

        async def revoke(item, *, public):
            raise Fault("scope_changed")

        port.source_checker = revoke
        for operation, body in (
            ("revision", revision_body(diary)),
            ("diaries", dict(schema_version=1, actor_id="actor:a")),
        ):
            with pytest.raises(Fault):
                await port.handle_verified(SERVICE, operation, body)
        assert checks == [diary["id"]]
        fixture.store.close()

    asyncio.run(run())


def test_unknown_reflection_resumes_same_conclusion_without_regeneration():
    async def run():
        life, clock, model = setup()
        memory = life.actor_memory = Memory(life)
        model.output = json.dumps(
            dict(
                content="A brief private entry.",
                reflections=[
                    dict(
                        kind="understanding",
                        text="This is partial understanding.",
                        episode_refs=[memory.ref],
                        importance="normal",
                        retention="medium",
                    )
                ],
                questions=[],
            )
        )
        keys = []

        async def unknown(actor, operation, call, value, known, validate):
            keys.append((operation, call, copy.deepcopy(value)))
            if len(keys) == 1:
                raise Fault("result_unknown", unknown=True)

        memory.append_validated_derived = unknown
        await life._diary_work("a")
        assert life.store.list("life_diaries")[0]["state"] == "postprocessing"
        await life._diary_work("a")
        assert life.store.list("life_diaries")[0]["state"] == "draft"
        assert len(model.calls) == 1 and keys[0] == keys[1]
        life.store.close()

    asyncio.run(run())


def test_clean_public_original_projection_does_not_upgrade_private_source():
    from support import contracts
    from tianshu_companion.actor_memory import SOURCE_PREFIX, STATE_PREFIX
    from tianshu_companion.contracts import digest
    from tianshu_companion.diary_disclosure import DiaryDisclosure

    async def run():
        life, clock, model = setup()
        actor = "actor:a"
        source = dict(
            producer="companion", object_id="private-read", revision="1", content_hash="a" * 64
        )
        old = dict(
            actor_id=actor,
            source=source,
            fact=dict(
                occurred_at="2026-09-13T10:00:00Z", source_constraints=dict(kind="actor_only")
            ),
            observed=dict(
                scope=None,
                operation_ref="reading:one",
                content_ref={"private_query": "must never enter fact"},
            ),
        )
        old["id"] = SOURCE_PREFIX + digest(source)
        life.store.put("metadata", old)
        life.store.put("metadata", dict(id=STATE_PREFIX + actor, generation="a" * 32, sequence=1))
        sent = []

        async def flush(item):
            sent.append(item)
            item["state"] = "actor_memory_delivered"
            life.store.put("outbox", item)

        memory = SimpleNamespace(
            core=SimpleNamespace(clock=clock, contracts=contracts()), store=life.store, flush=flush
        )
        disclosure = DiaryDisclosure(memory)
        original = dict(
            text="Public original only",
            coverage=dict(unit="characters", start=0, end=20),
            complete=False,
            gaps=[],
        )
        await disclosure._record(old, dict(disclosure_version="b" * 64), original)
        await disclosure._record(old, dict(disclosure_version="b" * 64), original)
        assert len(sent) == 1
        assert life.store.get("metadata", old["id"])["fact"]["source_constraints"] == dict(
            kind="actor_only"
        )
        fact = life.store.get("metadata", SOURCE_PREFIX + digest(sent[0]["source"]))["fact"]
        assert fact["source_constraints"] == dict(kind="public")
        assert "private_query" not in json.dumps(fact)
        assert fact["occurred_at"] == old["fact"]["occurred_at"]
        assert fact["learned_at"] != fact["occurred_at"]

        async def changed(actor, observed):
            return dict(disclosure_version="withdrawn")

        disclosure.proof = changed
        with pytest.raises(Fault):
            await disclosure.check(dict(old, disclosure_version="b" * 64))
        life.store.close()

    asyncio.run(run())


def test_original_group_share_uses_real_context_and_never_public_pointer():
    from tianshu_companion.proactive import Proactive
    from tianshu_companion.diary_sharing import DiarySharing

    async def run():
        life, clock, model = setup()
        memory = life.actor_memory = Memory(life)
        proactive = Proactive(life.store, clock, lambda sub: dict(allowed=True))
        sub = proactive.register_subscription(
            actor_id="a",
            person_id="person:member",
            audience="group",
            conversation_id="conversation:original-group",
            channel=dict(
                namespace="qq",
                binding_id="group",
                channel_conversation_id="original",
                thread_id=None,
            ),
            consent=dict(
                registered_by="admin", basis="explicit_admin_registration", evidence_ref="consent:1"
            ),
            quiet=("22:00", "08:00"),
        )
        sub = life.store.get("proactive_subscriptions", sub["id"])
        contexts = []

        async def current(candidate):
            motive = life.store.get("proactive_motives", candidate["subject_id"])
            assert motive is not None and candidate["subject_version"] == motive["version"]
            if not contexts:
                assert motive["state"] == "paused" and "Actually read" not in motive["summary"]
            contexts.append(copy.deepcopy(candidate))
            return dict(
                scope={
                    key: sub[key]
                    for key in ("actor_id", "person_id", "audience", "conversation_id")
                },
                origin=dict(assertion_ref="real-context:" + str(len(contexts))),
            )

        memory.core = SimpleNamespace(
            proactive=proactive, runtime_execution=SimpleNamespace(current_context=current)
        )
        await DiarySharing(life.diary_sources).prepare(
            dict(conversation_id="a", day="2026-09-13"), sub
        )
        actor = life._get("actors", "a")
        actor["diary_prepared_day"] = "2026-09-13"
        life._save("actors", actor)
        model.output = json.dumps(
            dict(
                content="Only our original group's permitted observation.",
                reflections=[],
                questions=[],
            )
        )
        await life._diary_work("a")
        diary = life.store.list("life_diaries")[0]
        motive = life.store.get("proactive_motives", diary["share_intent"]["motive_id"])
        assert (
            motive["state"] == "active"
            and motive["summary"] == "Only our original group's permitted observation."
        )
        assert motive["sources"][0]["version"] == diary["version"]
        assert diary["published_revision"] is None and diary["state"] == "draft"
        assert all(context[0]["audience"]["kind"] == "group" for context in memory.queries)
        assert len(contexts) >= 4
        with pytest.raises(Fault):
            await life.diary_sources.check(diary, public=True)
        with pytest.raises(Fault):
            await life.diary_sources.check(
                diary,
                audience=dict(conversation_id="other", audience="group", person_id="person:member"),
            )
        proactive.revoke_subscription(sub["id"], reason="withdrawn")
        with pytest.raises(Fault):
            await life.diary_sources.check(diary)
        life.store.close()

    asyncio.run(run())


@pytest.mark.parametrize("revoke_at", [1, 2])
def test_withdrawn_diary_question_is_never_reinjected_into_goal_model(revoke_at):
    from test_autonomous_life import autonomy_setup, save_goal
    from test_actor_memory import close

    async def run():
        h, owners, services, model = await autonomy_setup()
        try:
            concern = save_goal(h, "autonomy:diary:withdrawn")
            concern.update(
                title="WITHDRAWN_PRIVATE_QUESTION",
                goal="WITHDRAWN_PRIVATE_QUESTION",
                autonomy_basis={"test_basis": True},
            )
            h.core.store.put("life_concerns", concern)
            original = h.core.actor_memory.check
            count = 0

            async def check(material):
                nonlocal count
                if material.get("test_basis"):
                    count += 1
                    if count >= revoke_at:
                        raise Fault("scope_changed")
                else:
                    await original(material)

            h.core.actor_memory.check = check
            await h.core.life.autonomy.work(actor_id="actor:a")
            assert "WITHDRAWN_PRIVATE_QUESTION" not in json.dumps(model.goals)
            if revoke_at == 1:
                assert model.goals and model.goals[-1]["concerns"] == []
            else:
                assert model.goals == []
        finally:
            await close(h, services)

    asyncio.run(run())
