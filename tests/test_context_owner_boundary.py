"""Consume same-group owners through the existing schema-bound JSON client."""

import copy
import json
import unittest

import httpx

from support import Harness
from tianshu_companion.clients import JsonService, Memory, utc
from tianshu_companion.contracts import Fault, canonical


def unit(owner, name):
    key = owner.split(":", 1)[-1]
    return dict(
        record_id="record:" + key,
        record_version=1,
        semantic_group_id="group:" + key,
        subject_person_id=owner,
        statement="我的猫叫" + name,
        conditions=["群里公开提到"],
        negations=[],
        valid_time="current",
        uncertainty="inferred",
        reality="real",
        visibility="shared_projection",
        sources=[
            dict(
                kind="shareable_projection",
                owner="memory",
                projection_ref="projection:" + key,
                projection_version=1,
            )
        ],
    )


class ContextOwnerBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.h = Harness(silence_ms=0)
        self.origin = self.h.request()["command"]["origin"]
        self.scope = dict(
            actor_id="actor:a",
            person_id="person:viewer",
            audience="group",
            conversation_id="conv:shared",
        )
        units = [unit("person:second", "桃酥"), unit("person:third", "雪团")]
        self.response = dict(
            schema_version=1,
            effective_scope=self.scope,
            version_domain="memory-context/v1",
            association_version=1,
            scope_checks=[
                dict(
                    scope=dict(self.scope, person_id=person),
                    scope_version=1,
                    association_id=None,
                    association_version=1,
                )
                for person in (self.scope["person_id"], "person:second", "person:third")
            ],
            scope_version=1,
            verified_at=utc(self.h.clock()),
            valid_until=utc(self.h.clock() + 3600),
            selected_units=units,
            dependency_groups=[
                dict(
                    semantic_group_id=item["semantic_group_id"],
                    record_ids=[item["record_id"]],
                    complete=True,
                )
                for item in units
            ],
            budget_used=dict(
                tokens=len(canonical(units).encode()), bytes=len(canonical(units).encode())
            ),
            omissions=[],
            coverage=dict(
                matched_groups=2,
                returned_groups=2,
                complete=True,
                time_basis="source_sent_at",
                history_complete=False,
                missing_source_times=0,
            ),
        )
        self.requests = []
        self.history_versions = {}

        def handler(incoming):
            request = json.loads(incoming.content)
            self.requests.append((incoming.url.path, request))
            response = copy.deepcopy(self.response)
            request_id = request["query"]["request_id"]
            if incoming.url.path.endswith(("/history", "/input-history")):
                result = dict(
                    schema_version=1,
                    request_id=request_id,
                    effective_scope=response["effective_scope"],
                    scope_version=response["scope_version"],
                    verified_at=response["verified_at"],
                    valid_until=response["valid_until"],
                    snapshot=dict(
                        association_version=response["association_version"],
                        history_checks=[
                            dict(
                                scope=row["scope"],
                                history_version=self.history_versions.get(
                                    row["scope"]["person_id"], row["scope_version"]
                                ),
                                association_id=row["association_id"],
                                association_version=row["association_version"],
                            )
                            for row in response["scope_checks"]
                        ],
                    ),
                )
            else:
                if not all(request["budget"].values()):
                    response.update(
                        selected_units=[],
                        dependency_groups=[],
                        budget_used=dict(tokens=0, bytes=0),
                        omissions=["budget"],
                    )
                result = dict(response, request_id=request_id)
            return httpx.Response(200, json=result)

        self.client = JsonService(
            "https://synthetic.invalid",
            "synthetic-owner-boundary",
            transport=httpx.MockTransport(handler),
        )
        self.memory = Memory(self.h.contracts, self.client)

    async def asyncTearDown(self):
        await self.client.close()
        await self.h.core.close()

    async def select(self, *, budget=None, known=None):
        return await self.memory.select(
            self.origin,
            self.scope,
            "刚才群里提到的猫叫什么？",
            budget or dict(tokens=8192, bytes=8192),
            1 if known else None,
            known=known,
        )

    async def test_multiple_public_owners_preserve_their_facts_and_zero_budget_pins(self):
        selected = await self.select()
        self.assertEqual(self.response["selected_units"], selected["selected_units"])
        self.assertEqual(
            ["person:second", "person:third"],
            [item["subject_person_id"] for item in selected["selected_units"]],
        )
        self.assertEqual(self.scope, selected["effective_scope"])
        self.assertFalse(self.requests[-1][1]["include_associated"])
        verified = await self.select(budget=dict(tokens=0, bytes=0), known=selected)
        self.assertEqual([], verified["selected_units"])
        self.assertEqual(selected["scope_checks"], verified["scope_checks"])
        expected = {
            key: selected[key] for key in ("scope_version", "association_version", "scope_checks")
        }
        for method in (self.memory.history, self.memory.input_history):
            snapshot = await method(self.origin, self.scope, expected=expected)
            self.assertEqual(
                [row["scope"] for row in selected["scope_checks"]],
                [row["scope"] for row in snapshot["history_checks"]],
            )
            self.assertEqual(snapshot, await method(self.origin, self.scope, known=snapshot))

    async def test_group_checks_reject_foreign_boundaries_associations_duplicates_and_unfit_owners(
        self,
    ):
        original = copy.deepcopy(self.response)
        changes = [
            ("wrong-first", lambda r: r["scope_checks"].reverse()),
            ("actor", lambda r: r["scope_checks"][1]["scope"].update(actor_id="actor:b")),
            (
                "conversation",
                lambda r: r["scope_checks"][1]["scope"].update(conversation_id="conv:other"),
            ),
            ("private", lambda r: r["scope_checks"][1]["scope"].update(audience="self_private")),
            ("association", lambda r: r["scope_checks"][1].update(association_id="link:private")),
            ("association-version", lambda r: r["scope_checks"][1].update(association_version=2)),
            ("duplicate", lambda r: r["scope_checks"].append(copy.deepcopy(r["scope_checks"][1]))),
            ("missing-owner", lambda r: r["scope_checks"].pop()),
            ("viewer-version", lambda r: r["scope_checks"][0].update(scope_version=2)),
            (
                "unfit-owner",
                lambda r: r["scope_checks"].append(
                    dict(
                        r["scope_checks"][0], scope=dict(self.scope, person_id="person:unselected")
                    )
                ),
            ),
        ]
        for name, change in changes:
            with self.subTest(name=name):
                self.response = copy.deepcopy(original)
                change(self.response)
                with self.assertRaises(Fault):
                    await self.select()

    async def test_group_checks_keep_the_published_seventeen_scope_bound(self):
        units = [unit(f"person:owner-{index}", f"合成猫{index}") for index in range(16)]
        self.response.update(
            selected_units=units,
            dependency_groups=[
                dict(
                    semantic_group_id=item["semantic_group_id"],
                    record_ids=[item["record_id"]],
                    complete=True,
                )
                for item in units
            ],
            scope_checks=[self.response["scope_checks"][0]]
            + [
                dict(
                    scope=dict(self.scope, person_id=item["subject_person_id"]),
                    scope_version=1,
                    association_id=None,
                    association_version=1,
                )
                for item in units
            ],
            budget_used=dict(
                tokens=len(canonical(units).encode()), bytes=len(canonical(units).encode())
            ),
        )
        selected = await self.select(budget=dict(tokens=16384, bytes=16384))
        self.assertEqual(17, len(selected["scope_checks"]))
        self.response["scope_checks"].append(
            dict(
                self.response["scope_checks"][0],
                scope=dict(self.scope, person_id="person:eighteenth"),
            )
        )
        with self.assertRaises(Fault):
            await self.select(budget=dict(tokens=16384, bytes=16384))

    async def test_group_history_rejects_other_owner_boundary_changes_without_a_known_snapshot(
        self,
    ):
        original = copy.deepcopy(self.response)
        for method in (self.memory.history, self.memory.input_history):
            for changed in (
                dict(actor_id="actor:b"),
                dict(conversation_id="conv:other"),
                dict(audience="self_private"),
            ):
                with self.subTest(method=method.__name__, changed=changed):
                    self.response = copy.deepcopy(original)
                    self.response["scope_checks"][1]["scope"].update(changed)
                    with self.assertRaises(Fault):
                        await method(self.origin, self.scope)

    async def test_known_owner_change_fails_in_query_history_and_input_history(self):
        selected = await self.select()
        for method in (self.memory.history, self.memory.input_history):
            snapshot = await method(self.origin, self.scope)
            self.response["scope_checks"][1]["scope_version"] += 1
            with self.assertRaises(Fault):
                await method(self.origin, self.scope, known=snapshot)
            self.response["scope_checks"][1]["scope_version"] -= 1
        self.response["scope_checks"].pop()
        with self.assertRaises(Fault):
            await self.select(budget=dict(tokens=0, bytes=0), known=selected)

    async def test_additive_content_change_keeps_known_history_but_rejects_expected_content(self):
        selected = await self.select()
        expected = {
            key: selected[key] for key in ("scope_version", "association_version", "scope_checks")
        }
        for method in (self.memory.history, self.memory.input_history):
            with self.subTest(method=method.__name__):
                self.response["scope_version"] = 1
                self.response["scope_checks"][0]["scope_version"] = 1
                self.response["scope_checks"][1]["scope_version"] = 1
                snapshot = await method(self.origin, self.scope)
                self.response["scope_version"] = 2
                self.response["scope_checks"][0]["scope_version"] = 2
                self.response["scope_checks"][1]["scope_version"] = 2
                self.history_versions = {self.scope["person_id"]: 1, "person:second": 1}
                self.assertEqual(snapshot, await method(self.origin, self.scope, known=snapshot))
                with self.assertRaises(Fault):
                    await method(self.origin, self.scope, expected=expected)

    async def test_a_complete_semantic_group_cannot_mix_two_owners(self):
        first, second = self.response["selected_units"]
        second["semantic_group_id"] = first["semantic_group_id"]
        self.response["dependency_groups"] = [
            dict(
                semantic_group_id=first["semantic_group_id"],
                record_ids=[first["record_id"], second["record_id"]],
                complete=True,
            )
        ]
        with self.assertRaises(Fault):
            await self.select()

    async def test_other_owner_does_not_allow_private_units_or_raw_sources(self):
        await self.h.ingest(text="合成的群原始来源", group=True, channel="group:raw-boundary")
        source = self.h.core.store.list("collections")[0]["messages"][0]["source"]
        original = copy.deepcopy(self.response)
        for change in (
            lambda r: r["selected_units"][0].update(visibility="self_private"),
            lambda r: r["selected_units"][0].update(
                sources=[dict(kind="raw_message", source=source)]
            ),
        ):
            with self.subTest(change=change):
                self.response = copy.deepcopy(original)
                change(self.response)
                self.h.contracts.check("identity-memory#unit", self.response["selected_units"][0])
                with self.assertRaises(Fault) as error:
                    await self.select()
                self.assertEqual("forbidden", error.exception.code)

    async def test_private_association_path_keeps_its_existing_shape_and_subject_rule(self):
        self.scope = dict(self.scope, audience="self_private", conversation_id="conv:private")
        self.response.update(
            effective_scope=self.scope,
            selected_units=[],
            dependency_groups=[],
            budget_used=dict(tokens=0, bytes=0),
            scope_checks=[
                dict(scope=self.scope, scope_version=1, association_id=None, association_version=1),
                dict(
                    scope=dict(
                        self.scope, person_id="person:associated", conversation_id="conv:linked"
                    ),
                    scope_version=1,
                    association_id="link:explicit",
                    association_version=2,
                ),
            ],
        )
        selected = await self.select()
        self.assertEqual(self.response["scope_checks"], selected["scope_checks"])
        self.assertTrue(self.requests[-1][1]["include_associated"])
        self.response["selected_units"] = [unit("person:associated", "他人的猫")]
        self.response["dependency_groups"] = [
            dict(
                semantic_group_id=self.response["selected_units"][0]["semantic_group_id"],
                record_ids=[self.response["selected_units"][0]["record_id"]],
                complete=True,
            )
        ]
        with self.assertRaises(Fault):
            await self.select()
