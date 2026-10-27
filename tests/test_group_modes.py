"""Actual adapter journal policy, triggers and source-preserving queue controls."""

import asyncio
import copy
import json

import pytest

from test_participation_adapter import Fixture
from test_observation_adapter import policy
from group_policy import decision, normalize
from tianshu_companion.clients import utc


def rule(mode, actor="actor:a", keywords=()):
    return dict(mode=mode, actor_id=actor, keywords=list(keywords))


def group_policy(mode="observe", groups=None, keywords=()):
    return dict(version=2, default=rule(mode, keywords=keywords), groups=groups or {})


async def apply(fixture, value, revision=1, group_revisions=None, private=None, enabled=True):
    return await fixture.call(
        "/observation/apply",
        dict(
            request_id=f"group-apply:{revision}",
            account_id="10001",
            revision=revision,
            enabled=enabled,
            group_policy=value,
            private_policy=private or policy(),
            group_revisions=group_revisions or {},
            group_default_revision=revision,
        ),
    )


@pytest.mark.parametrize(
    "old,default,override",
    [
        (policy(observe=False), "disabled", "disabled"),
        (policy(), "observe", "observe"),
        (policy("whitelist", ["20001"]), "observe", "reply"),
        (policy("blacklist", ["20001"]), "reply", "observe"),
    ],
)
def test_legacy_migration_is_deterministic_and_never_active(old, default, override):
    value = normalize(old)
    assert value["default"]["mode"] == default
    assert value["groups"].get("20001", value["default"])["mode"] == override
    assert "active" not in json.dumps(value)
    assert normalize(value) == value


@pytest.mark.parametrize(
    "mode,observed,explicit,ordinary",
    [
        ("disabled", False, False, False),
        ("observe", True, False, False),
        ("reply", True, True, False),
        ("active", True, True, True),
    ],
)
def test_four_modes_gate_actual_capture_and_literal_keyword_without_fake_mention(
    tmp_path, mode, observed, explicit, ordinary
):
    f = Fixture(tmp_path / "host.sqlite")

    async def run():
        value = group_policy(mode, keywords=["[天枢]"])
        await apply(f, value)
        assert f.service.input_enabled("10001", "group:20001") is observed
        verdict = decision(value, "group:20001", text="请[天枢]看看", mentioned=False)
        assert verdict["reply_triggered"] is explicit
        assert verdict["active"] is ordinary
        captured = await f.service.capture_observation(
            "10001", "group:20001", "10002", "1", utc(), "请[天枢]看看", False
        )
        assert captured is observed
        if observed:
            row = f.service.db.execute(
                "SELECT payload FROM observations WHERE native_id='1'"
            ).fetchone()
            assert json.loads(row[0])["mentioned"] is False
            assert (
                await f.service.observation_claimed("10001", "group:20001", "10002", "1")
            ) is explicit
        assert (
            await f.service.capture_result("10001", "group:20001", "10002", "2", utc(), "ordinary")
            == "no_route"
        )
        assert f.sent == []

    try:
        asyncio.run(run())
    finally:
        f.service.close()


def test_other_group_changes_preserve_pending_send_and_off_on_never_revives_old_claim(tmp_path):
    f = Fixture(tmp_path / "host.sqlite")

    async def run():
        groups = {"20001": rule("active"), "20002": rule("reply")}
        await apply(f, group_policy(groups=groups), group_revisions={"20001": 1, "20002": 1})
        assert await f.service.capture_observation(
            "10001", "group:20001", "10002", "11", utc(), "hello", True
        )
        a = copy.deepcopy(groups)
        a["20001"] = rule("disabled")
        await apply(f, group_policy(groups=a), 2, {"20001": 2, "20002": 1})
        delivery = dict(
            connection_id="bot:actual",
            account_id="10001",
            policy_revision=1,
            delivery=dict(
                reply_id="reply:b",
                attempt_id="attempt:b",
                conversation_id="group:20002",
                namespace="qq",
                thread_id=None,
                text="B remains enabled",
                turn_id="turn:b",
                segment_sequence=1,
            ),
        )
        receipt = await f.call("/observation/messages/send", delivery)
        assert receipt["state"] == "sent"
        assert f.sent == [("10001", "group:20002", "B remains enabled")]
        await apply(f, group_policy(groups=groups), 3, {"20001": 3, "20002": 1})
        assert not await f.service.observation_claimed("10001", "group:20001", "10002", "11")
        assert await f.call("/observation/messages/send", delivery) == receipt
        assert len(f.sent) == 1

    try:
        asyncio.run(run())
    finally:
        f.service.close()


def test_private_observe_false_and_expired_account_stop_capture_and_precise_route(tmp_path):
    f = Fixture(tmp_path / "host.sqlite")

    async def run():
        await apply(f, group_policy("active"), private=policy("blacklist", observe=False))
        await f.call(
            "/bindings/apply",
            dict(
                request_id="exact",
                connection_id="exact",
                revision=1,
                account_id="10001",
                conversation={"kind": "private", "id": "10002"},
                allowed_authors=["10002"],
                enabled=True,
            ),
        )
        assert not f.service.input_enabled("10001", "private:10002")
        assert not await f.service.capture_observation(
            "10001", "private:10002", "10002", "12", utc(), "private", False
        )
        assert (
            await f.service.capture_result(
                "10001", "private:10002", "10002", "12", utc(), "private"
            )
            == "no_route"
        )
        f.service.db.execute("UPDATE observation_accounts SET last_poll=0")
        assert not f.service.input_enabled("10001", "group:20001")
        assert not await f.service.capture_observation(
            "10001", "group:20001", "10002", "13", utc(), "stale", False
        )
        assert f.service.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
        assert f.sent == []

    try:
        asyncio.run(run())
    finally:
        f.service.close()


def test_unbound_actor_archives_but_cannot_reply_or_claim(tmp_path):
    f = Fixture(tmp_path / "host.sqlite")

    async def run():
        await apply(f, dict(version=2, default=rule("observe", None), groups={}))
        assert await f.service.capture_observation(
            "10001", "group:20001", "10002", "21", utc(), "archive", True
        )
        assert not await f.service.observation_claimed("10001", "group:20001", "10002", "21")
        capabilities = await f.call("/observation/capabilities", {})
        assert capabilities["group_policy_version"] == 2
        assert (
            capabilities["ordinary_claim_available"] is True
        )  # Explicit synthetic controlled host.
        f.service.participation.controlled = lambda: False
        assert (await f.call("/observation/capabilities", {}))["ordinary_claim_available"] is False

    try:
        asyncio.run(run())
    finally:
        f.service.close()
