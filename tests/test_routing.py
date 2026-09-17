"""TS-024: explicit functional command routing and the single reply owner.

Everything here is synthetic. No real GsCore API, no real group-management action and no
real account, channel, device or paid model is involved, and nothing asserts that a real
game result was produced.
"""

import asyncio
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest

from support import Harness
from tianshu_companion.clients import command, digest, uid, utc
from tianshu_companion.contracts import Fault
from tianshu_companion.direct import (
    CONTRACT_GAP,
    PLUGIN_GAP,
    SyntheticPlugin,
    configured_matcher,
    usage_line,
)
from tianshu_companion.store import DIRECT_TABLES, Store
from tianshu_nonebot.bridge import Bridge
from tianshu_nonebot.routing import BridgeDelivery, DirectRouter, registry_matcher

QUERY = "companion.test.query"
MENU = "companion.test.menu"
SLOW = "companion.test.slow"
SLOW_TIMEOUT = "companion.test.slow_timeout"
MENTION = "companion.test.mention"

QUERY_SHAPE = {
    "fields": [
        dict(name="subject", type="token", required=True),
        dict(name="detail", type="rest", required=False),
    ]
}
NONE_SHAPE = {"fields": []}


def spec(command_id, name, **overrides):
    value = dict(
        command_id=command_id,
        version=1,
        name=name,
        platforms=["qq"],
        audiences=["self_private", "group"],
        parameters=NONE_SHAPE,
        reply_to="bridge",
    )
    value.update(overrides)
    return value


def query_spec(**overrides):
    value = spec(QUERY, "/查询", parameters=QUERY_SHAPE)
    value["aliases"] = ["/query"]
    value.update(overrides)
    return value


def menu_spec(**overrides):
    return spec(MENU, "/菜单", reply_to="core", **overrides)


def slow_spec(**overrides):
    return spec(SLOW, "/慢任务", timeout=60, **overrides)


def slow_timeout_spec(**overrides):
    return spec(SLOW_TIMEOUT, "/超时任务", timeout=1, **overrides)


def mention_spec(**overrides):
    return spec(
        MENTION,
        "/点名",
        audiences=["group"],
        actor_allowlist=["actor:a"],
        **overrides,
    )


def register(h, *specs):
    for item in specs:
        h.core.direct.register_command(**item)
    return h


async def settle(h, count=8):
    await h.cycles(count)


async def establish(h, **kwargs):
    """One ordinary accepted message, so Core knows the channel and the account's actor."""
    await h.ingest(**kwargs)
    h.clock.advance(6)
    await h.cycles(8)
    channel = h.request(**kwargs)["message_key"]["channel"]
    return h.core.store.get("conversations", digest(channel))["conversation_id"]


def body_for(h, text, **kwargs):
    return h.request(text=text, **kwargs)


async def run_direct(h, text, **kwargs):
    body = body_for(h, text, **kwargs)
    return await h.core.direct_command("nonebot", body), body


class BridgeSender:
    """The existing outbound path: Core -> published send route -> Bridge.send."""

    available = True

    def __init__(self, bridge):
        self.bridge = bridge

    async def send(self, request):
        return await self.bridge.send("companion", request)

    async def reconcile(self, request):
        return await self.bridge.reconcile(request)


async def run_capability(
    h, body, *, reply_to, command_id=QUERY, version=1, parameters=None, **extra
):
    """Core's explicit capability entry, on the same authenticated message version."""
    request = dict(
        command=command(
            dict(assertion_ref=body["command"]["origin"]["assertion_ref"]), uid("cap"), h.clock()
        ),
        capability_id=command_id,
        command_version=version,
        channel=body["message_key"]["channel"],
        parameters=parameters or {},
        reply_to=reply_to,
        entry_ref=extra.pop("entry_ref", "entry:test"),
    )
    request.update(extra)
    return await h.core.capability("nonebot", request)


# --------------------------------------------------------------------------- registry


def test_registration_shape_scope_immutability_and_conflicts():
    h = Harness()
    registry = h.core.direct
    registry.register_command(**query_spec())
    view = registry.commands(namespace="qq")[0]
    assert view["name"] == "/查询"
    assert view["names"] == ["/查询", "/query"]
    assert view["usage"] == "/查询 <subject> [<detail>]"
    assert view["reply_to"] == "bridge"
    assert view["platforms"] == ["qq"]
    assert registry.commands(namespace="tg") == []
    registry.register_command(**query_spec())  # same content and version: idempotent
    with pytest.raises(ValueError):
        registry.register_command(**query_spec(platforms=["tg"]))
    with pytest.raises(ValueError):
        registry.register_command(**query_spec(description="changed"))
    # A second owner for one name inside one scope is the double reply this task forbids.
    with pytest.raises(ValueError):
        registry.register_command(**query_spec(command_id="other", aliases=["/查询"]))
    registry.register_command(**query_spec(version=2, expected=1))
    assert registry.commands()[0]["command_version"] == 2
    with pytest.raises(ValueError):
        registry.register_command(**query_spec(version=3, expected=1))
    with pytest.raises(ValueError):
        registry.register_command(**query_spec(version=4, expected=99))
    for bad in (
        {"fields": [dict(name="a", type="rest"), dict(name="b", type="token")]},
        {"fields": [dict(name="a", type="unknown")]},
        {"fields": [dict(name="a", type="token", required=False), dict(name="b", type="token")]},
        {"fields": [dict(name="a", type="token"), dict(name="a", type="token")]},
        {"fields": [dict(name="a", type="token", choices=[])]},
        {"fields": []},
        {"other": []},
    ):
        with pytest.raises(ValueError):
            registry.register_command(**query_spec(command_id="bad", version=1, parameters=bad))
    with pytest.raises(ValueError):
        registry.register_command(**query_spec(command_id="bad", version=1, reply_to="both"))
    with pytest.raises(ValueError):
        registry.register_command(**query_spec(command_id="bad", version=1, audiences=["room"]))
    with pytest.raises(ValueError):
        registry.register_command(**query_spec(command_id="bad", version=1, name="/a b"))
    h.core.store.close()


def test_usage_line_is_derived_from_the_registration():
    assert usage_line("/a", []) == "/a"
    assert usage_line("/a", [dict(name="x", required=True)]) == "/a <x>"
    assert usage_line("/a", [dict(name="x", required=False)]) == "/a [<x>]"


# ---------------------------------------------------------------------------- routing


def test_only_a_registered_complete_command_is_claimed():
    h = Harness()
    register(h, query_spec(), slow_spec(platforms=["tg"]), menu_spec())
    match = h.core.direct.match
    assert match("/查询 天气", namespace="qq", audience="group")["matched"]
    assert match("/query 天气 明天", namespace="qq", audience="self_private")["matched"]
    for text in (
        "帮我查询一下天气",
        "查询 天气",
        "@bot /查询 天气",
        "/查询x",
        "/帮助",
    ):
        assert not match(text, namespace="qq", audience="group")["matched"]
    # Arguments belong to the registration: an extra trailing token fills the optional
    # `detail` field instead of making the text "not a command".
    trailing = match("/查询 天气 的", namespace="qq", audience="group")
    assert trailing["matched"] and trailing["parameters_valid"]
    assert trailing["parameters"] == {"subject": "天气", "detail": "的"}
    assert not match("/慢任务", namespace="qq", audience="group")["matched"]
    assert match("/慢任务", namespace="tg", audience="group")["matched"]
    assert not match("/菜单", namespace="tg", audience="group")["matched"]
    # A Core-owned registration is capability-only: the fast path never claims it.
    menu = match("/菜单", namespace="qq", audience="group")
    assert not menu["matched"] and menu["reason"] == "reply_owner_core"
    h.core.store.close()


def test_parameter_shape_violations_are_reported_never_guessed():
    h = Harness()
    register(h, query_spec())
    good = h.core.direct.match("/查询 天气", namespace="qq", audience="group")
    assert good["parameters"] == {"subject": "天气"}
    assert good["parameters_valid"]
    extra = h.core.direct.match("/查询 天气 明天 后天", namespace="qq", audience="group")
    assert extra["parameters"] == {"subject": "天气", "detail": "明天 后天"}
    missing = h.core.direct.match("/查询", namespace="qq", audience="group")
    assert missing["matched"] and not missing["parameters_valid"]
    assert missing["parameter_error"] == "missing_argument:subject"
    h.core.store.close()


def test_a_broken_parameter_shape_is_answered_with_the_registered_usage_line():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        response, _ = await run_direct(h, "/查询")
        request_id = response["request"]["id"]
        await h.core.direct.work()
        view = h.core.direct.request_view(request_id)
        assert view["state"] == "failed"
        assert view["blocked_reason"] == "invalid_parameters"
        assert view["reply"]["text"] == "/查询 <subject> [<detail>]"
        assert h.direct_plugin.calls == []
        assert view["reply_state"] == "sent"
        await h.core.close()

    asyncio.run(scenario())


def test_command_bypasses_debounce_and_never_takes_a_turn_or_model_slot():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        turns, calls = len(h.turns()), len(h.gateway.calls)
        assert turns == 1 and calls == 1
        response, _ = await run_direct(h, "/查询 天气")
        assert response["owner"] == "direct"
        assert response["request"]["state"] == "pending"
        await h.core.direct.work()
        view = h.core.direct.request_view(response["request"]["id"])
        assert view["state"] == "completed"
        assert view["reply_state"] == "sent"
        # No collection, no turn, no model call: nothing waited for the chat silence window.
        assert len(h.turns()) == turns
        assert len(h.gateway.calls) == calls
        assert h.core.store.list("collections", states=["collecting"]) == []
        # The command added no message to the conversation's existing collection either, so
        # its text never entered the chat chain that the sealed turn belongs to.
        collections = h.core.store.list("collections", h.turns()[0]["conversation_id"])
        assert len(collections) == 1
        accepted = [
            message
            for message in h.core.store.list("inbox", h.turns()[0]["conversation_id"])
            if not message["stale"]
        ]
        assert len(accepted) == 1
        assert accepted[0]["request"]["parts"] == [{"kind": "text", "text": "你好"}]
        assert h.delivery.calls[0]["reply_id"] == digest([view["id"], "reply"])
        await h.core.close()

    asyncio.run(scenario())


def test_unmatched_text_still_follows_the_companion_chain():
    async def scenario():
        h = Harness()
        register(h, query_spec(), menu_spec())
        conversation_id = await establish(h)
        for text in ("帮我查询一下天气", "早上好", "/菜单"):
            response, _ = await run_direct(h, text)
            # The bridge claimed it; Core is authoritative and hands it to the chat chain.
            assert response["owner"] == "companion"
            assert response["request"] is None
            assert response["receipt"]["conversation_id"] == conversation_id
        assert h.direct_plugin.calls == []
        assert h.core.direct.requests() == []
        # One sealed collection from the setup message, one still collecting the three texts.
        collecting = h.core.store.list("collections", conversation_id, ["collecting"])
        assert len(collecting) == 1 and len(collecting[0]["messages"]) == 3
        await h.core.close()

    asyncio.run(scenario())


def test_direct_command_does_not_depend_on_platform_or_chat_model():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        h.origins.unavailable = True
        h.memory.unavailable = True
        h.gateway.fail = True
        selections = len(h.memory.selections)
        response, _ = await run_direct(h, "/查询 天气")
        assert response["owner"] == "direct"
        await h.core.direct.work()
        view = h.core.direct.request_view(response["request"]["id"])
        assert view["state"] == "completed"
        assert view["reply_state"] == "sent"
        assert len(h.memory.selections) == selections
        await h.core.close()

    asyncio.run(scenario())


# ------------------------------------------------------- one execution, one reply owner


def test_duplicate_sdk_event_executes_once_and_rejects_changed_parameters():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        body = body_for(h, "/查询 天气")
        first = await h.core.direct_command("nonebot", body)
        duplicate = await h.core.direct_command("nonebot", body)
        assert duplicate["request"]["id"] == first["request"]["id"]
        await h.core.direct.work()
        assert len(h.direct_plugin.calls) == 1
        changed = h.request(text="/查询 时间", message=body["message_key"]["message_id"])
        with pytest.raises(Fault) as error:
            await h.core.direct_command("nonebot", changed)
        assert error.value.code == "idempotency_conflict"
        assert len(h.direct_plugin.calls) == 1
        await h.core.close()

    asyncio.run(scenario())


def test_the_durable_request_is_bound_to_the_message_version():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        body = body_for(h, "/查询 天气")
        response = await h.core.direct_command("nonebot", body)
        expected = h.core.direct.command_request_key(
            body["message_key"]["channel"],
            body["message_key"]["message_id"],
            body["message_key"]["revision"],
        )
        assert response["request"]["id"] == expected
        assert response["request"]["message_key"] == body["message_key"]
        # A later revision of the same message id is a different version, not a duplicate, and
        # the undispatched older version is superseded so it can never answer twice.
        edited = h.request(text="/查询 时间", message=body["message_key"]["message_id"], revision=2)
        second = await h.core.direct_command("nonebot", edited)
        assert second["request"]["id"] != expected
        assert h.core.direct.request_view(expected)["state"] == "superseded"
        await h.core.direct.work()
        assert len(h.direct_plugin.calls) == 1
        assert len(h.delivery.calls) == 1
        await h.core.close()

    asyncio.run(scenario())


def test_two_entries_share_one_execution_on_the_same_message_version():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        body = body_for(h, "/查询 天气")
        entry = await h.core.direct_command("nonebot", body)
        request_id = entry["request"]["id"]
        assert h.core.direct.request_view(request_id)["entries"] == ["command"]
        capability = await run_capability(
            h,
            body,
            reply_to="bridge",
            parameters={"subject": "天气"},
            message_key=body["message_key"],
        )
        assert capability["request"]["id"] == request_id
        assert capability["request"]["entries"] == ["command", "capability"]
        await h.core.direct.work()
        # One durable request, one attempt, one plugin call - and a second pass over the
        # command entry still adds nothing, because there is nothing left to execute.
        assert len(h.core.direct.requests()) == 1
        assert len(h.direct_plugin.calls) == 1
        assert len(h.core.direct.attempts(request_id)) == 1
        assert h.core.direct.request_view(request_id)["attempt_count"] == 1
        await h.core.direct.work()
        entry_again = await h.core.direct_command("nonebot", body)
        assert entry_again["request"]["id"] == request_id
        await h.core.direct.work()
        assert len(h.direct_plugin.calls) == 1
        assert len(h.core.direct.attempts(request_id)) == 1
        await h.core.close()

    asyncio.run(scenario())


def test_concurrent_entries_share_one_execution():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        body = body_for(h, "/查询 天气")
        gate = asyncio.Event()
        h.direct_plugin.gate = gate
        # Both entries for one message version start before either can finish.
        command = asyncio.create_task(h.core.direct_command("nonebot", body))
        capability = asyncio.create_task(
            run_capability(
                h,
                body,
                reply_to="bridge",
                parameters={"subject": "天气"},
                message_key=body["message_key"],
            )
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not gate.is_set()
        results = await asyncio.gather(command, capability)
        assert results[0]["request"]["id"] == results[1]["request"]["id"]
        assert len(h.core.direct.requests()) == 1
        assert len(h.core.direct.attempts(results[0]["request"]["id"])) == 1
        assert len(h.direct_plugin.calls) == 1
        gate.set()
        await h.cycles(6)
        assert len(h.direct_plugin.calls) == 1
        await h.core.close()

    asyncio.run(scenario())


def test_concurrent_entries_execute_exactly_once():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        body = body_for(h, "/查询 天气")
        entry = await h.core.direct_command("nonebot", body)
        request_id = entry["request"]["id"]
        gate = asyncio.Event()
        h.direct_plugin.gate = gate
        first = asyncio.create_task(h.core.direct.dispatch(request_id))
        second = asyncio.create_task(h.core.direct.dispatch(request_id))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        # Both dispatch calls really overlap: the plugin is still running behind the gate.
        assert not gate.is_set()
        third = await h.core.direct.dispatch(request_id)
        assert third["state"] == "dispatching"
        gate.set()
        await first
        await second
        assert len(h.direct_plugin.calls) == 1
        assert len(h.core.direct.attempts(request_id)) == 1
        assert h.core.direct.request_view(request_id)["attempt_count"] == 1
        await h.core.close()

    asyncio.run(scenario())


def test_bridge_owned_reply_is_delivered_natively_and_core_gets_no_payload():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        body = body_for(h, "/查询 天气")
        entry = await run_capability(
            h,
            body,
            reply_to="bridge",
            parameters={"subject": "天气"},
            message_key=body["message_key"],
        )
        await h.core.direct.work()
        view = h.core.direct.request_view(entry["request"]["id"])
        assert view["reply_state"] == "sent"
        assert view["delivery_verified"]
        assert view["delivery_evidence"] == "published_contract_receipt"
        assert view["result_verified"] is False
        assert view["result_evidence"] == "synthetic_adapter_result"
        assert view["delivery_port_available"]
        assert view["plugin_gap"] == PLUGIN_GAP
        assert len(h.delivery.calls) == 1
        # Core is told the delivery state only; it is never handed a payload to announce.
        assert entry["result"] is None and entry["reply"] is None
        assert entry["reply_owner"] == "bridge"
        await h.core.close()

    asyncio.run(scenario())


def test_core_owned_reply_returns_the_result_and_never_touches_the_channel():
    async def scenario():
        h = Harness()
        register(h, menu_spec())
        await establish(h)
        body = body_for(h, "/菜单")
        entry = await run_capability(
            h, body, reply_to="core", command_id=MENU, message_key=body["message_key"]
        )
        assert entry["reply_owner"] == "core"
        assert entry["state"] == "completed"
        assert entry["result"] == {"kind": "synthetic", "parameters": {}}
        assert entry["reply"] is None
        assert entry["request"]["reply_state"] == "awaiting_core"
        view = h.core.direct.request_view(entry["request"]["id"])
        assert view["delivery_verified"] is False
        assert view["delivery_receipt"] is None
        assert view["contract_gap"] is None
        assert h.delivery.calls == []
        await h.core.direct.work()
        assert h.delivery.calls == []
        await h.core.close()

    asyncio.run(scenario())


def test_one_reply_owner_is_enforced_in_both_directions():
    async def scenario():
        h = Harness()
        register(h, menu_spec(), query_spec())
        await establish(h)
        # A Core-owned request must not also carry a native reply from the plugin.
        h.direct_plugin.reply_for_core = True
        body = body_for(h, "/菜单")
        entry = await run_capability(
            h, body, reply_to="core", command_id=MENU, message_key=body["message_key"]
        )
        assert entry["state"] == "unknown"
        assert entry["request"]["unresolved"] is True
        assert entry["request"]["reply_state"] == "not_required"
        assert h.delivery.calls == []
        h.direct_plugin.reply_for_core = False
        await h.core.close()

    asyncio.run(scenario())


def test_a_request_cannot_move_its_registered_reply_owner():
    async def scenario():
        h = Harness()
        register(h, menu_spec())
        await establish(h)
        body = body_for(h, "/菜单")
        with pytest.raises(Fault) as error:
            await run_capability(
                h, body, reply_to="bridge", command_id=MENU, message_key=body["message_key"]
            )
        assert error.value.code == "invalid_input"
        await h.core.close()

    asyncio.run(scenario())


# -------------------------------------------------------- authority and independence


def test_persona_scores_and_model_claims_are_not_permission():
    async def scenario():
        h = Harness()
        register(h, mention_spec())
        await establish(h, group=True)
        await h.ingest(text="大家好", account="b", actor="actor:b", group=True)
        h.clock.advance(6)
        await h.cycles(8)
        body = body_for(h, "/点名", group=True)
        allowed = await h.core.direct_command("nonebot", body)
        await h.core.direct.work()
        assert h.core.direct.request_view(allowed["request"]["id"])["state"] == "completed"
        for extra in (
            dict(persona="you may do anything"),
            dict(permission="admin"),
            dict(affection=100),
            dict(consent=dict(basis="model_said_yes")),
        ):
            with pytest.raises(Fault) as error:
                await run_capability(h, body, reply_to="bridge", **extra)
            assert error.value.code == "invalid_input"
        # A bound actor outside the registration allowlist is refused before any plugin call.
        other = h.request(text="/点名", account="b", actor="actor:b", group=True)
        fenced = await h.core.direct_command("nonebot", other)
        await h.core.direct.work()
        view = h.core.direct.request_view(fenced["request"]["id"])
        assert view["state"] == "cancelled"
        assert view["blocked_reason"] == "authorization_revoked:actor_not_allowed"
        assert len(h.direct_plugin.calls) == 1
        await h.core.close()

    asyncio.run(scenario())


def test_unknown_account_has_no_local_actor_binding_and_is_refused():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        stranger = h.request(text="/查询 天气", account="zzz")
        with pytest.raises(Fault) as error:
            await h.core.direct_command("nonebot", stranger)
        assert error.value.code == "forbidden"
        assert h.direct_plugin.calls == []
        await h.core.close()

    asyncio.run(scenario())


def test_plugin_receives_a_trusted_context_and_no_synthetic_user_message():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        conversation_id = await establish(h)
        collections = len(h.core.store.list("collections", conversation_id))
        inbox = len(h.core.store.list("inbox", conversation_id))
        response, body = await run_direct(h, "/查询 天气")
        await h.core.direct.work()
        call = h.direct_plugin.calls[0]
        # No message body, no inbox row and no way back into Core.ingest is on the port.
        assert set(call) == {
            "schema_version",
            "request_id",
            "attempt_id",
            "command_id",
            "command_version",
            "entry",
            "actor_id",
            "person_id",
            "audience",
            "conversation_id",
            "channel",
            "parameters",
            "parameter_error",
            "reply_to",
            "usage",
            "submitted_at",
        }
        assert "parts" not in call and "message_key" not in call
        assert call["parameters"] == {"subject": "天气"}
        assert call["actor_id"] == "actor:a"
        assert call["conversation_id"] == conversation_id
        # The direct path writes no collection and no inbox row of its own.
        assert len(h.core.store.list("collections", conversation_id)) == collections
        assert len(h.core.store.list("inbox", conversation_id)) == inbox
        assert response["request"]["channel"] == body["message_key"]["channel"]
        await h.core.close()

    asyncio.run(scenario())


def test_revoked_authority_cancels_before_any_plugin_or_delivery_call():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        response, _ = await run_direct(h, "/查询 天气")
        request_id = response["request"]["id"]
        del h.core.roles["actor:a"]
        h.clock.advance(2)
        await h.core.direct.work()
        view = h.core.direct.request_view(request_id)
        assert view["state"] == "cancelled"
        assert view["blocked_reason"] == "authorization_revoked:unknown_role"
        assert view["reply_state"] == "not_required"
        assert h.direct_plugin.calls == []
        assert h.delivery.calls == []
        # A binding removal is picked up the same way.
        register(h, slow_spec())
        second, _ = await run_direct(h, "/慢任务")
        h.core.bindings.pop("qq-private")
        await h.core.direct.work()
        assert (
            h.core.direct.request_view(second["request"]["id"])["blocked_reason"]
            == "authorization_revoked:unknown_binding"
        )
        await h.core.close()

    asyncio.run(scenario())


def test_revoked_registration_is_refused_before_dispatch():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        response, _ = await run_direct(h, "/查询 天气")
        request_id = response["request"]["id"]
        h.core.direct.revoke_command(QUERY, reason="operator_revoked", expected=1)
        await h.core.direct.work()
        view = h.core.direct.request_view(request_id)
        assert view["state"] == "cancelled"
        assert view["blocked_reason"] == "authorization_revoked:registration_missing"
        assert h.direct_plugin.calls == []
        # A revoked name is no longer claimable, so the text follows the chat chain again.
        assert not h.core.direct.match("/查询 天气", namespace="qq", audience="self_private")[
            "matched"
        ]
        h.core.direct.register_command(**query_spec(expected=1))
        assert h.core.direct.match("/查询 天气", namespace="qq", audience="self_private")["matched"]
        await h.core.close()

    asyncio.run(scenario())


# -------------------------------------------------------------- delivery discipline


def test_unknown_delivery_is_never_resent_and_a_restart_does_not_resubmit():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        h.delivery.states = ["unknown"]
        body = body_for(h, "/查询 天气")
        entry = await h.core.direct_command("nonebot", body)
        request_id = entry["request"]["id"]
        await h.core.direct.work()
        view = h.core.direct.request_view(request_id)
        assert view["reply_state"] == "unknown"
        assert view["unresolved"] is True
        assert view["delivery_verified"] is False
        for _ in range(3):
            await h.core.direct.work()
            await h.core.tick()
        assert len(h.delivery.calls) == 1
        h.core.direct.recover()
        await h.core.direct.work()
        assert len(h.delivery.calls) == 1
        assert h.core.direct.request_view(request_id)["reply_state"] == "unknown"
        with pytest.raises(ValueError):
            h.core.direct.retry(request_id, reason="operator", redeliver=True)
        await h.core.close()

    asyncio.run(scenario())


def test_redelivery_requires_proof_and_an_explicit_request():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        h.delivery.states = ["failed"]
        entry = await h.core.direct_command("nonebot", body_for(h, "/查询 天气"))
        request_id = entry["request"]["id"]
        await h.core.direct.work()
        assert h.core.direct.request_view(request_id)["reply_state"] == "failed"
        # No proof the earlier attempt did not execute: an explicit redelivery is refused.
        with pytest.raises(ValueError):
            h.core.direct.retry(request_id, reason="operator", redeliver=True)
        # The capability itself completed, so it is not re-executed either.
        with pytest.raises(ValueError):
            h.core.direct.retry(request_id, reason="operator")
        assert len(h.direct_plugin.calls) == 1
        # With `retry_safe=true` the bridge proved non-execution, so a redelivery is allowed.
        # The reply id is unchanged, so the bridge still de-duplicates it.
        h.delivery.retry_safe = True
        h.delivery.states = ["failed"]
        second = await h.core.direct_command("nonebot", body_for(h, "/查询 时间"))
        second_id = second["request"]["id"]
        await h.core.direct.work()
        assert h.core.direct.request_view(second_id)["reply_state"] == "failed"
        h.core.direct.retry(second_id, reason="operator", redeliver=True)
        await h.core.direct.work()
        view = h.core.direct.request_view(second_id)
        assert view["reply_state"] == "sent"
        assert view["state"] == "completed"
        # Only the reply was resubmitted: the plugin ran exactly once per request.
        assert len(h.direct_plugin.calls) == 2
        assert [call["reply_id"] for call in h.delivery.calls] == [
            digest([request_id, "reply"]),
            digest([second_id, "reply"]),
            digest([second_id, "reply"]),
        ]
        await h.core.close()

    asyncio.run(scenario())


def test_reconcile_is_the_only_way_out_of_an_unknown_delivery():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        h.delivery.states = ["unknown"]
        entry = await h.core.direct_command("nonebot", body_for(h, "/查询 天气"))
        request_id = entry["request"]["id"]
        await h.core.direct.work()
        assert h.core.direct.request_view(request_id)["reply_state"] == "unknown"
        sent = h.delivery.receipt(h.delivery.calls[0], "sent")
        h.delivery.answers[h.delivery.calls[0]["reply_id"]] = sent
        await h.core.direct.reconcile(request_id)
        view = h.core.direct.request_view(request_id)
        assert view["reply_state"] == "sent"
        assert view["delivery_verified"]
        assert len(h.delivery.calls) == 1
        await h.core.close()

    asyncio.run(scenario())


def test_a_conversation_that_disappears_is_reported_instead_of_faked():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        gate = asyncio.Event()
        h.direct_plugin.gate = gate
        body = body_for(h, "/查询 天气")
        entry = await h.core.direct_command("nonebot", body)
        request_id = entry["request"]["id"]
        worker = asyncio.create_task(h.core.direct.work())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        h.core.store.delete("conversations", digest(body["message_key"]["channel"]))
        gate.set()
        await worker
        view = h.core.direct.request_view(request_id)
        assert view["state"] == "completed"
        assert view["reply_state"] == "not_started"
        assert view["blocked_reason"] == "conversation_unmapped"
        assert view["delivery_verified"] is False
        assert h.delivery.calls == []
        await h.core.close()

    asyncio.run(scenario())


def test_a_missing_delivery_port_is_reported_and_never_faked():
    async def scenario():
        h = Harness(direct_options={"deliver": None})
        register(h, query_spec())
        await establish(h)
        body = body_for(h, "/查询 天气")
        entry = await h.core.direct_command("nonebot", body)
        request_id = entry["request"]["id"]
        await h.core.direct.work()
        view = h.core.direct.request_view(request_id)
        assert view["state"] == "completed"
        assert view["reply_state"] == "not_started"
        assert view["blocked_reason"] == "delivery_port_unavailable"
        assert view["delivery_port_available"] is False
        assert view["delivery_verified"] is False
        assert view["contract_gap"] == CONTRACT_GAP
        await h.core.close()

    asyncio.run(scenario())


def test_a_missing_plugin_is_reported_and_never_faked():
    async def scenario():
        h = Harness(direct_options={"adapter": None})
        register(h, query_spec())
        await establish(h)
        body = body_for(h, "/查询 天气")
        entry = await h.core.direct_command("nonebot", body)
        request_id = entry["request"]["id"]
        await h.core.direct.work()
        view = h.core.direct.request_view(request_id)
        assert view["state"] == "pending"
        assert view["blocked_reason"] == "plugin_unavailable"
        assert view["adapter_available"] is False
        assert view["delivery_verified"] is False
        assert h.delivery.calls == []
        await h.core.close()

    asyncio.run(scenario())


# ------------------------------------------------------- slow plugin and cancellation


def test_slow_plugin_does_not_block_admission_or_the_chat_schedule():
    async def scenario():
        h = Harness()
        register(h, slow_spec())
        await establish(h)
        gate = asyncio.Event()
        h.direct_plugin.gate = gate
        response, _ = await run_direct(h, "/慢任务")
        request_id = response["request"]["id"]
        worker = asyncio.create_task(h.core.direct.work())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert h.core.direct.request_view(request_id)["state"] == "dispatching"
        before = len(h.gateway.calls)
        await h.ingest(text="普通消息")
        h.clock.advance(6)
        await h.cycles(8)
        assert len(h.gateway.calls) == before + 1
        await h.core.tick()
        assert h.core.direct.request_view(request_id)["state"] == "dispatching"
        gate.set()
        await worker
        assert len(h.direct_plugin.calls) == 1
        assert h.core.direct.request_view(request_id)["reply_state"] == "sent"
        await h.core.close()

    asyncio.run(scenario())


def test_slow_task_settles_only_by_a_trusted_callback_for_its_own_attempt():
    async def scenario():
        h = Harness()
        register(h, slow_spec())
        await establish(h)
        h.direct_plugin.slow = {SLOW}
        entry = await h.core.direct_command("nonebot", body_for(h, "/慢任务"))
        request_id = entry["request"]["id"]
        await h.core.direct.work()
        view = h.core.direct.request_view(request_id)
        assert view["state"] == "awaiting_result"
        assert view["reply_state"] == "not_started"
        assert h.delivery.calls == []
        attempt_id = view["attempt_id"]
        with pytest.raises(Fault):
            h.core.direct.settle(
                attempt_id, h.direct_plugin.complete(request_id, "direct-attempt:other")
            )
        h.core.direct.settle(attempt_id, h.direct_plugin.complete(request_id, attempt_id))
        assert h.core.direct.request_view(request_id)["reply_state"] == "ready_to_deliver"
        await h.core.direct.work()
        assert h.core.direct.request_view(request_id)["reply_state"] == "sent"
        assert len(h.delivery.calls) == 1
        await h.core.close()

    asyncio.run(scenario())


def test_plugin_timeout_settles_unknown_and_is_never_resent():
    async def scenario():
        h = Harness()
        register(h, slow_timeout_spec())
        await establish(h)
        gate = asyncio.Event()
        h.direct_plugin.gate = gate
        entry = await h.core.direct_command("nonebot", body_for(h, "/超时任务"))
        request_id = entry["request"]["id"]
        await asyncio.wait_for(h.core.direct.work(), timeout=10)
        view = h.core.direct.request_view(request_id)
        assert view["state"] == "unknown"
        assert view["unresolved"] is True
        assert view["reply_state"] == "not_required"
        gate.set()
        await h.core.direct.work()
        assert len(h.direct_plugin.calls) == 1
        await h.core.close()

    asyncio.run(scenario())


def test_cancel_before_dispatch_and_cancel_in_flight_withholds_the_reply():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        first, _ = await run_direct(h, "/查询 天气")
        view = h.core.direct.cancel(first["request"]["id"], reason="explicit_user_cancel")
        assert view["state"] == "cancelled"
        assert view["reply_state"] == "not_required"
        await h.core.direct.work()
        assert h.direct_plugin.calls == []
        gate = asyncio.Event()
        h.direct_plugin.gate = gate
        second, _ = await run_direct(h, "/查询 时间")
        request_id = second["request"]["id"]
        worker = asyncio.create_task(h.core.direct.work())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert h.core.direct.request_view(request_id)["state"] == "dispatching"
        h.core.direct.cancel(request_id, reason="explicit_user_cancel")
        gate.set()
        await worker
        final = h.core.direct.request_view(request_id)
        assert final["state"] == "cancelled"
        assert final["reply_state"] == "not_required"
        assert h.delivery.calls == []
        with pytest.raises(ValueError):
            h.core.direct.cancel(request_id, reason="again")
        await h.core.close()

    asyncio.run(scenario())


def test_restart_marks_an_interrupted_submit_unknown_without_resending():
    async def scenario():
        h = Harness()
        register(h, slow_spec())
        await establish(h)
        h.direct_plugin.slow = {SLOW}
        entry = await h.core.direct_command("nonebot", body_for(h, "/慢任务"))
        request_id = entry["request"]["id"]
        await h.core.direct.work()
        assert h.core.direct.request_view(request_id)["state"] == "awaiting_result"
        h.core.direct.recover()
        view = h.core.direct.request_view(request_id)
        assert view["state"] == "unknown"
        assert view["blocked_reason"] == "interrupted_task"
        assert view["unresolved"] is True
        await h.core.direct.work()
        assert len(h.direct_plugin.calls) == 1
        h.core.direct.recover()
        assert h.core.direct.request_view(request_id)["state"] == "unknown"
        assert h.core.direct.attempts(request_id)[0]["state"] == "accepted"
        await h.core.close()

    asyncio.run(scenario())


def test_restart_keeps_a_pending_request_dispatchable():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        entry = await h.core.direct_command("nonebot", body_for(h, "/查询 天气"))
        request_id = entry["request"]["id"]
        h.core.direct.recover()
        assert h.core.direct.request_view(request_id)["state"] == "pending"
        await h.core.direct.work()
        assert h.core.direct.request_view(request_id)["reply_state"] == "sent"
        assert len(h.direct_plugin.calls) == 1
        await h.core.close()

    asyncio.run(scenario())


def test_a_late_callback_stays_on_its_own_attempt():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        h.direct_plugin.states = ["unknown"]
        entry = await h.core.direct_command("nonebot", body_for(h, "/查询 天气"))
        request_id = entry["request"]["id"]
        await h.core.direct.work()
        old_attempt = h.core.direct.request_view(request_id)["attempt_id"]
        assert h.core.direct.request_view(request_id)["state"] == "unknown"
        h.core.direct.retry(request_id, reason="operator")
        await h.core.direct.work()
        fresh = h.core.direct.request_view(request_id)
        assert fresh["attempt_id"] != old_attempt
        assert fresh["state"] == "completed"
        h.core.direct.settle(
            old_attempt, h.direct_plugin.complete(request_id, old_attempt, reply="旧回包")
        )
        after = h.core.direct.request_view(request_id)
        assert after["state"] == "completed"
        assert after["reply"]["text"] != "旧回包"
        attempts = h.core.direct.attempts(request_id)
        assert [item["id"] for item in attempts] == [old_attempt, fresh["attempt_id"]]
        assert attempts[0]["stale"] is True and attempts[1]["stale"] is False
        await h.core.close()

    asyncio.run(scenario())


def test_commands_run_while_both_chat_turns_are_busy():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        gates = [asyncio.Event(), asyncio.Event()]
        h.gateway.gates[2] = gates[0]
        h.gateway.gates[3] = gates[1]
        await h.ingest(text="第一条")
        h.clock.advance(6)
        await h.cycles(6)
        await h.ingest(text="第二条")
        h.clock.advance(6)
        await h.cycles(6)
        assert len(h.core.store.list("turns", states=["generating"])) == 2
        response, _ = await run_direct(h, "/查询 天气")
        await h.core.direct.work()
        view = h.core.direct.request_view(response["request"]["id"])
        assert view["state"] == "completed"
        assert view["reply_state"] == "sent"
        # The two chat turns are still exactly where they were: the command took no slot.
        assert len(h.core.store.list("turns", states=["generating"])) == 2
        assert len(h.gateway.calls) == 3
        for gate in gates:
            gate.set()
        await h.cycles(24)
        await h.core.close()

    asyncio.run(scenario())


# ------------------------------------------------------------------ bridge integration


class CoreClient:
    def __init__(self, core):
        self.core = core

    async def call(self, path, request):
        if path == "/internal/v1/conversation/direct-command":
            return await self.core.direct_command("nonebot", request)
        return await self.core.ingest("nonebot", request)


def test_bridge_reuses_capture_and_send_and_never_creates_a_second_exit():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        conversation_id = await establish(h)
        channel = h.request()["message_key"]["channel"]
        native = []

        async def verify(service, request):
            if service != "companion":
                raise Fault("forbidden")

        async def send_native(destination, text):
            native.append((destination, text))
            return ["native:" + str(len(native))]

        bridge_store = Store(":memory:")
        try:
            bridge = Bridge(
                bridge_store,
                h.contracts,
                CoreClient(h.core),
                destinations={conversation_id: channel},
                send_native=send_native,
                verify_send=verify,
                clock=h.clock,
            )
            router = DirectRouter(bridge, matcher=registry_matcher(h.core.direct.commands()))
            h.core.direct.delivery_port = BridgeDelivery(bridge)
            body = body_for(h, "/查询 天气")
            # The ownership decision and its durable record go through Bridge.capture.
            assert router.capture(body, audience="self_private") == "direct"
            assert router.capture(body, audience="self_private") == "direct"
            assert len(bridge_store.list("inbox")) == 1
            assert bridge_store.list("inbox")[0]["state"] == "direct_owned"
            result = await router.submit(body, audience="self_private")
            assert result["owner"] == "direct" and result["submitted"]
            request_id = result["response"]["request"]["id"]
            await h.core.direct.work()
            view = h.core.direct.request_view(request_id)
            assert view["reply_state"] == "sent"
            assert view["delivery_verified"]
            # The reply left through Bridge.send, with its own de-duplication intact.
            assert len(native) == 1
            assert native[0][0] == channel
            assert native[0][1] == "[合成] " + QUERY
            stored = bridge_store.list("replies")[0]
            assert stored["receipt"]["state"] == "sent"
            assert stored["receipt"]["channel_message_ids"] == ["native:1"]
            # An ordinary sentence is never claimed.
            assert router.capture(h.request(text="早上好"), audience="self_private") == "companion"
            # A claim Core refuses is handed back to the companion queue, not answered twice.
            stale = h.request(text="/不存在")
            claiming = DirectRouter(
                bridge,
                matcher=configured_matcher({("qq", "self_private"): {"/不存在"}}),
            )
            handed = await claiming.submit(stale, audience="self_private")
            assert handed["owner"] == "companion" and not handed["submitted"]
            assert bridge_store.list("inbox", states=["pending"])
            await bridge.flush()
            # The handed-back claim and the ordinary sentence both reached Core's own queue.
            assert len(bridge_store.list("inbox", states=["accepted"])) == 2
            assert not bridge_store.list("inbox", states=["pending"])
            # Core ingested the handed-back text once and the command text not at all: its
            # conversation carries the handed-back text and the ordinary sentence, and no
            # message anywhere in it is the command.
            collections = h.core.store.list("collections", conversation_id)
            assert len(collections) == 2
            ingested = [
                message["request"]["parts"][0]["text"]
                for message in h.core.store.list("inbox", conversation_id)
                if not message["stale"]
            ]
            assert sorted(ingested) == sorted(["/不存在", "你好", "早上好"])
            assert "/查询" not in " ".join(ingested)
            assert len(h.core.direct.requests()) == 1
            # The handed-back owner is durable: a duplicate SDK event for the same message
            # version is never re-decided into a command execution behind Core's back.
            assert claiming.capture(stale, audience="self_private") == "companion"
        finally:
            bridge_store.close()
            await h.core.close()

    asyncio.run(scenario())


def test_the_registry_port_is_the_bridges_single_source_of_names():
    async def scenario():
        h = Harness()
        register(h, query_spec(), menu_spec(), slow_spec(platforms=["tg"]))
        table = h.core.commands("nonebot")
        assert table["registry"] == h.core.direct.registry_version()
        assert sorted(item["name"] for item in table["commands"]) == ["/慢任务", "/查询", "/菜单"]
        matcher = registry_matcher(table["commands"])
        # The table Core serves is exactly what the bridge routes with, so the two can never
        # disagree about which names this product owns.
        assert matcher("/查询 天气", namespace="qq", audience="self_private")["matched"]
        assert not matcher("/没有 天气", namespace="qq", audience="self_private")["matched"]
        # A Core-owned registration is capability-only, so the fast path never claims it and
        # its bare text keeps following the companion chain.
        assert not matcher("/菜单", namespace="qq", audience="self_private")["matched"]
        assert not matcher("/慢任务", namespace="qq", audience="group")["matched"]
        assert matcher("/慢任务", namespace="tg", audience="group")["matched"]
        # A different caller may not read another product's command table.
        with pytest.raises(Fault) as error:
            h.core.commands("platform")
        assert error.value.code == "forbidden"
        await h.core.close()

    asyncio.run(scenario())


def test_bridge_router_never_claims_without_a_known_scope():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        bridge_store = Store(":memory:")
        try:
            bridge = Bridge(bridge_store, h.contracts, None, destinations={}, clock=h.clock)
            router = DirectRouter(bridge, matcher=registry_matcher(h.core.direct.commands()))
            body = body_for(h, "/查询 天气")
            # Without a known platform/audience scope nothing is claimed as a command.
            assert router.capture(body, audience=None) == "companion"
            assert bridge_store.list("inbox")[0]["owner"] == "companion"
            # A text the bridge did claim is refused at submit time when no Core client is
            # wired, rather than executed, dropped or answered from the bridge's own guess.
            claiming = DirectRouter(
                bridge, matcher=configured_matcher({("qq", "self_private"): {"/查询"}})
            )
            claimed = h.request(text="/查询 晴天")
            assert claiming.capture(claimed, audience="self_private") == "direct"
            with pytest.raises(Fault) as error:
                await claiming.submit(claimed, audience="self_private")
            assert error.value.code == "dependency_unavailable"
        finally:
            bridge_store.close()
            await h.core.close()

    asyncio.run(scenario())


class SharedExit:
    """The real published path: Core's own sender and the functional port are one Bridge."""

    def __init__(self, h, *, slow=None):
        self.h = h
        self.channel = h.request()["message_key"]["channel"]
        self.conversation_id = h.core.store.get("conversations", digest(self.channel))[
            "conversation_id"
        ]
        self.native = []
        self.slow = slow
        self.bridge_store = Store(":memory:")

        async def send_native(destination, text):
            if self.slow is not None:
                await self.slow.wait()
            self.native.append(text)
            return ["native:" + str(len(self.native))]

        async def verify(service, request):
            if service != "companion":
                raise Fault("forbidden")

        self.bridge = Bridge(
            self.bridge_store,
            h.contracts,
            None,
            destinations={self.conversation_id: self.channel},
            send_native=send_native,
            verify_send=verify,
            clock=h.clock,
        )
        h.core.sender = BridgeSender(self.bridge)
        h.core.direct.delivery_port = BridgeDelivery(self.bridge)

    @property
    def positions(self):
        return [reply["sequence"] for reply in self.bridge_store.list("replies")]

    def chat_segments(self):
        return [text for text in self.native if text.startswith("合成回复")]

    def functional(self):
        return [text for text in self.native if text.startswith("[合成]")]

    async def deliver_until(self, request_id, *, passes=40):
        """Run the direct worker the way the application's 0.5s worker does."""
        for _ in range(passes):
            await self.h.core.direct.work()
            await self.h.cycles(4)
            if self.h.core.direct.request_view(request_id)["reply_state"] == "sent":
                return
        raise AssertionError(
            "request never delivered: " + repr(self.h.core.direct.request_view(request_id))
        )

    async def settle_chat(self, *, cycles=120):
        await self.h.cycles(cycles)

    def close(self):
        self.bridge_store.close()


def test_a_functional_reply_between_chat_segments_does_not_lose_the_next_segment():
    """The exact acceptance case: 301 -> 401 -> 501 -> 402 must not drop the last segment."""

    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        exit_ = SharedExit(h)
        try:
            gate = asyncio.Event()
            h.gateway.gates[2] = gate
            await h.ingest(text="chat")
            h.clock.advance(6)
            await h.cycles(8)
            first = await h.core.direct_command("nonebot", h.request(text="/查询 first"))
            await h.core.direct.work()
            # The model finishes; the turn now sends its segments one at a time.
            gate.set()
            for _ in range(200):
                await h.cycles(1)
                if len(exit_.chat_segments()) == 1:
                    break
            assert len(exit_.chat_segments()) == 1
            assert [t["phase"] for t in h.turns()][-1] == "sending"
            # A second functional reply arrives between the turn's two segments.
            second = await h.core.direct_command("nonebot", h.request(text="/查询 second"))
            await h.core.direct.work()
            waiting = h.core.direct.request_view(second["request"]["id"])
            # It waits for the legal message boundary instead of overtaking the turn's band.
            assert waiting["reply_state"] == "ready_to_deliver"
            assert waiting["deferred_reason"] == "outbound_band_busy"
            assert len(exit_.native) == 2
            await exit_.settle_chat()
            await exit_.deliver_until(second["request"]["id"])
            # Every reply that was owed went out, exactly once.
            assert [t["phase"] for t in h.turns()] == ["sent", "sent"]
            assert sorted(exit_.native) == sorted(
                [
                    "[合成] " + QUERY,
                    "[合成] " + QUERY,
                    "合成回复一",
                    "合成回复二",
                ]
            )
            assert h.core.direct.request_view(first["request"]["id"])["delivery_verified"]
            assert h.core.direct.request_view(second["request"]["id"])["delivery_verified"]
            positions = exit_.positions
            assert positions == sorted(positions) and len(set(positions)) == 4
            # The turn stayed one ordered unit: both of its segments in a single band.
            chat = [
                reply
                for reply in exit_.bridge_store.list("replies")
                if reply["sequence"] // 100 == 4
            ]
            assert [reply["sequence"] for reply in chat] == [401, 402]
            assert exit_.positions[-1] == 501
        finally:
            exit_.close()
            await h.core.close()

    asyncio.run(scenario())


def test_repeated_functional_replies_cannot_overtake_a_turn_between_segments():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        exit_ = SharedExit(h)
        try:
            gate = asyncio.Event()
            h.gateway.gates[2] = gate
            await h.ingest(text="chat")
            h.clock.advance(6)
            await h.cycles(8)
            gate.set()
            # Three functional replies while the turn still has segments to hand over.
            entries = [
                await h.core.direct_command("nonebot", h.request(text="/查询 %d" % index))
                for index in range(3)
            ]
            for _ in range(60):
                for entry in entries:
                    await h.core.direct.work()
                await h.cycles(4)
                if all(
                    h.core.direct.request_view(entry["request"]["id"])["reply_state"] == "sent"
                    for entry in entries
                ) and [t["phase"] for t in h.turns()] == ["sent", "sent"]:
                    break
            assert [t["phase"] for t in h.turns()] == ["sent", "sent"]
            for entry in entries:
                view = h.core.direct.request_view(entry["request"]["id"])
                assert view["reply_state"] == "sent" and view["delivery_verified"]
            assert len(exit_.functional()) == 3
            assert exit_.chat_segments() == ["合成回复一", "合成回复二"]
            positions = exit_.positions
            assert positions == sorted(positions) and len(set(positions)) == 5
            bands = {}
            for reply in exit_.bridge_store.list("replies"):
                bands.setdefault(reply["sequence"] // 100, []).append(reply["sequence"] % 100)
            # Every functional reply has its own single-segment band and the turn has one band.
            assert sorted(bands.values()) == [[1], [1], [1], [1, 2]]
        finally:
            exit_.close()
            await h.core.close()

    asyncio.run(scenario())


def test_functional_replies_before_a_turn_keep_the_turn_in_one_band():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        exit_ = SharedExit(h)
        try:
            gate = asyncio.Event()
            h.gateway.gates[2] = gate
            await h.ingest(text="chat")
            h.clock.advance(6)
            await h.cycles(8)
            # Two functional replies answer while the turn is still generating, so neither
            # waits for the chat model and the turn takes a band above both of them.
            for text in ("/查询 first", "/查询 second"):
                entry = await h.core.direct_command("nonebot", h.request(text=text))
                await h.core.direct.work()
                assert h.core.direct.request_view(entry["request"]["id"])["reply_state"] == "sent"
            gate.set()
            await h.cycles(80)
            assert [t["phase"] for t in h.turns()] == ["sent", "sent"]
            assert len(exit_.chat_segments()) == 2
            assert len(exit_.functional()) == 2
            positions = exit_.positions
            assert positions == sorted(positions) and len(set(positions)) == 4
            assert positions[:2] == [301, 401]
            assert positions[2:] == [501, 502]
        finally:
            exit_.close()
            await h.core.close()

    asyncio.run(scenario())


def test_slow_outbound_io_defers_a_functional_reply_instead_of_interleaving():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        hold = asyncio.Event()
        exit_ = SharedExit(h, slow=hold)
        try:
            gate = asyncio.Event()
            h.gateway.gates[2] = gate
            await h.ingest(text="chat")
            h.clock.advance(6)
            await h.cycles(8)
            gate.set()
            # Kick the chat delivery, then let its first segment sit inside a slow channel.
            for _ in range(6):
                await h.cycles(1)
            entry = await h.core.direct_command("nonebot", h.request(text="/查询 slow"))
            await h.core.direct.work()
            view = h.core.direct.request_view(entry["request"]["id"])
            assert view["reply_state"] in {"ready_to_deliver", "sent"}
            assert view["reply_state"] != "unknown"
            hold.set()
            await exit_.deliver_until(entry["request"]["id"])
            await h.cycles(120)
            assert [t["phase"] for t in h.turns()] == ["sent", "sent"]
            assert len(exit_.chat_segments()) == 2
            assert exit_.functional() == ["[合成] " + QUERY]
            positions = exit_.positions
            assert positions == sorted(positions) and len(set(positions)) == 3
        finally:
            exit_.close()
            await h.core.close()

    asyncio.run(scenario())


def test_concurrent_functional_and_chat_sends_never_duplicate_a_segment():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        exit_ = SharedExit(h)
        try:
            h.gateway.gates[2] = asyncio.Event()
            await h.ingest(text="chat")
            h.clock.advance(6)
            await h.cycles(8)
            entries = [
                await h.core.direct_command("nonebot", h.request(text="/查询 %d" % index))
                for index in range(2)
            ]
            h.gateway.gates[2].set()
            # Both workers run at once: the direct worker and the chat scheduler.
            for _ in range(60):
                await asyncio.gather(
                    *(h.core.direct.work() for _ in entries),
                    h.cycles(3),
                )
                if all(
                    h.core.direct.request_view(entry["request"]["id"])["reply_state"] == "sent"
                    for entry in entries
                ) and [t["phase"] for t in h.turns()] == ["sent", "sent"]:
                    break
            assert [t["phase"] for t in h.turns()] == ["sent", "sent"]
            assert len(exit_.chat_segments()) == 2
            assert len(exit_.functional()) == 2
            # Nothing was sent twice: the exit holds exactly one reply per reply id.
            replies = exit_.bridge_store.list("replies")
            assert len(replies) == len({reply["id"] for reply in replies}) == 4
            assert len(exit_.native) == 4
            assert exit_.positions == sorted(exit_.positions)
        finally:
            exit_.close()
            await h.core.close()

    asyncio.run(scenario())


def test_a_cancelled_turn_releases_the_boundary_for_a_waiting_functional_reply():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        exit_ = SharedExit(h)
        try:
            gate = asyncio.Event()
            h.gateway.gates[2] = gate
            await h.ingest(text="chat")
            h.clock.advance(6)
            await h.cycles(8)
            gate.set()
            for _ in range(200):
                await h.cycles(1)
                if len(exit_.chat_segments()) == 1:
                    break
            turn = [t for t in h.turns() if t["phase"] == "sending"][0]
            entry = await h.core.direct_command("nonebot", h.request(text="/查询 cancel"))
            await h.core.direct.work()
            assert (
                h.core.direct.request_view(entry["request"]["id"])["deferred_reason"]
                == "outbound_band_busy"
            )
            await h.cancel(turn)
            await exit_.deliver_until(entry["request"]["id"])
            view = h.core.direct.request_view(entry["request"]["id"])
            assert view["reply_state"] == "sent" and view["deferred_reason"] is None
            assert exit_.functional() == ["[合成] " + QUERY]
            # The cancelled turn released the boundary without sending its remaining segment.
            assert len(exit_.chat_segments()) == 1
            assert [t["phase"] for t in h.turns()][-1] in {"cancelled", "failed"}
        finally:
            exit_.close()
            await h.core.close()

    asyncio.run(scenario())


def test_a_restart_between_chat_segments_still_delivers_each_segment_once():
    async def scenario():
        directory = scratch("routing-restart")
        path = str(directory / "companion.db")
        h = Harness(path=path)
        register(h, query_spec())
        await establish(h)
        exit_ = SharedExit(h)
        try:
            gate = asyncio.Event()
            h.gateway.gates[2] = gate
            await h.ingest(text="chat")
            h.clock.advance(6)
            await h.cycles(8)
            gate.set()
            for _ in range(200):
                await h.cycles(1)
                if len(exit_.chat_segments()) == 1:
                    break
            entry = await h.core.direct_command("nonebot", h.request(text="/查询 restart"))
            await h.core.direct.work()
            deferred = h.core.direct.request_view(entry["request"]["id"])
            assert deferred["deferred_reason"] == "outbound_band_busy"
            # Restart on the same database while the turn still owes a segment.
            origins = dict(h.origins.values)
            accounts = dict(h.memory.accounts)
            h.core.store.close()
            reopened = Harness(path=path)
            # The source assertions and identity mapping are in-memory doubles; the durable
            # facts being recovered - requests, replies, bands - are the database's.
            reopened.origins.values.update(origins)
            reopened.memory.accounts.update(accounts)
            reopened.core.recover()
            reopened.core.sender = BridgeSender(exit_.bridge)
            reopened.core.direct.delivery_port = BridgeDelivery(exit_.bridge)
            h = reopened
            for _ in range(80):
                await h.core.direct.work()
                await h.cycles(4)
                if h.core.direct.request_view(entry["request"]["id"])["reply_state"] == "sent":
                    break
            view = h.core.direct.request_view(entry["request"]["id"])
            assert view["reply_state"] == "sent" and view["delivery_verified"]
            assert [t["phase"] for t in h.turns()] == ["sent", "sent"]
            assert len(exit_.chat_segments()) == 2
            assert exit_.functional() == ["[合成] " + QUERY]
            assert exit_.positions == sorted(exit_.positions)
            assert len(exit_.native) == len(set(exit_.native)) == 3
        finally:
            exit_.close()
            await h.core.close()
            shutil.rmtree(directory, ignore_errors=True)

    asyncio.run(scenario())


class InterruptedDelivery:
    """A port that dies at the IO point after the delivery intent was already durable.

    Raising a BaseException (not an Exception) propagates out of `Direct.deliver` before it
    can settle, which is exactly the durable state an ungraceful exit leaves behind: the row
    is `completed + submitting` with the frozen delivery document and no receipt.

    This reproduces that state through the real production path - `dispatch` executes the
    plugin, `_record_delivery_intent` commits, then the process "dies" - but it is an
    interrupted coroutine at the exact crash point, not a real process kill.
    """

    available = True

    def __init__(self):
        self.attempts = 0
        self.answers = {}

    async def send(self, request):
        self.attempts += 1
        raise KeyboardInterrupt

    async def reconcile(self, request):
        return self.answers.get(request["reply_id"])


def delivery_receipt(clock, delivery, state, *, native=None):
    return dict(
        schema_version=1,
        request_id=delivery["command"]["request_id"],
        reply_id=delivery["reply_id"],
        segment_sequence=delivery["segment_sequence"],
        attempt_id="attempt:reconcile",
        state=state,
        channel_message_ids=list(native or []),
        observed_at=utc(clock()),
        retry_safe=state == "failed",
    )


async def crash_after_intent(name):
    """Drive the real path to the crash point and leave the durable row behind."""
    directory = scratch(name)
    path = str(directory / "companion.db")
    crasher = InterruptedDelivery()
    h = Harness(path=path, direct_options={"deliver": crasher})
    register(h, query_spec())
    await establish(h)
    entry = await h.core.direct_command("nonebot", h.request(text="/查询 天气"))
    request_id = entry["request"]["id"]
    with pytest.raises(KeyboardInterrupt):
        await h.core.direct.work()
    row = h.core.store.get("direct_requests", request_id)
    return directory, path, h, request_id, row


def test_a_committed_delivery_intent_survives_a_restart_and_reconciles_to_sent():
    async def scenario():
        directory, path, h, request_id, row = await crash_after_intent("routing-intent-sent")
        try:
            # The crash point really is a committed intent: completed, submitting, no receipt.
            assert row["state"] == "completed"
            assert row["reply_state"] == "submitting"
            assert row["delivery_receipt"] is None
            assert row["unresolved"] in (False, None)
            assert row["delivery"]["reply_id"] == digest([request_id, "reply"])
            h.core.store.close()

            reader = InterruptedDelivery()
            r = Harness(path=path, direct_options={"deliver": reader})
            r.core.recover()
            recovered = r.core.direct.request_view(request_id)
            # Recovered as unknown and unresolved, never as "probably sent".
            assert recovered["reply_state"] == "unknown"
            assert recovered["unresolved"] is True
            assert recovered["blocked_reason"] == "interrupted_delivery"
            # Nothing was re-executed and nothing was resent.
            assert r.direct_plugin.calls == []
            assert reader.attempts == 0
            for _ in range(3):
                await r.core.direct.work()
            assert r.direct_plugin.calls == []
            assert reader.attempts == 0

            # The same delivery evidence answers: same reply id, same request id.
            delivery = r.core.store.get("direct_requests", request_id)["delivery"]
            reader.answers[delivery["reply_id"]] = delivery_receipt(
                r.clock, delivery, "sent", native=["native:reconciled"]
            )
            await r.core.direct.reconcile(request_id)
            settled = r.core.direct.request_view(request_id)
            assert settled["reply_state"] == "sent"
            assert settled["delivery_verified"] is True
            assert settled["delivery_evidence"] == "published_contract_receipt"
            assert settled["unresolved"] is False
            assert settled["blocked_reason"] is None
            assert settled["delivery_receipt"]["request_id"] == delivery["command"]["request_id"]
            assert reader.attempts == 0
            await r.core.close()
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    asyncio.run(scenario())


def test_a_committed_delivery_intent_that_never_arrived_is_never_resent():
    async def scenario():
        directory, path, h, request_id, _ = await crash_after_intent("routing-intent-failed")
        try:
            h.core.store.close()
            reader = InterruptedDelivery()
            r = Harness(path=path, direct_options={"deliver": reader})
            r.core.recover()
            delivery = r.core.store.get("direct_requests", request_id)["delivery"]
            reader.answers[delivery["reply_id"]] = delivery_receipt(r.clock, delivery, "failed")
            await r.core.direct.reconcile(request_id)
            settled = r.core.direct.request_view(request_id)
            # The channel never took it: recorded honestly, and still not sent a second time.
            assert settled["reply_state"] == "failed"
            assert settled["delivery_verified"] is False
            assert settled["unresolved"] is False
            for _ in range(3):
                await r.core.direct.work()
            assert reader.attempts == 0
            assert r.direct_plugin.calls == []
            await r.core.close()
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    asyncio.run(scenario())


def test_a_committed_delivery_intent_stays_unknown_and_ignores_a_late_callback():
    async def scenario():
        directory, path, h, request_id, row = await crash_after_intent("routing-intent-unknown")
        try:
            attempt_id = row["attempt_id"]
            h.core.store.close()
            reader = InterruptedDelivery()
            r = Harness(path=path, direct_options={"deliver": reader})
            r.core.recover()
            # No answer available: it stays unknown and unresolved, and is never resent.
            await r.core.direct.reconcile(request_id)
            waiting = r.core.direct.request_view(request_id)
            assert waiting["reply_state"] == "unknown"
            assert waiting["unresolved"] is True
            for _ in range(3):
                await r.core.direct.work()
            assert reader.attempts == 0

            # A late plugin callback for the old attempt cannot rewrite the recovered reply:
            # the attempt already has a receipt, so a differing late result is refused and
            # the request keeps its unknown outcome.
            late = r.direct_plugin.complete(request_id, attempt_id, reply="晚回包")
            with pytest.raises(Fault) as error:
                r.core.direct.settle(attempt_id, late)
            assert error.value.code == "idempotency_conflict"
            after = r.core.direct.request_view(request_id)
            assert after["reply_state"] == "unknown"
            assert after["unresolved"] is True
            assert (after["reply"] or {}).get("text") != "晚回包"
            assert r.direct_plugin.calls == []
            await r.core.close()
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    asyncio.run(scenario())


def test_a_functional_reply_never_costs_a_pending_chat_turn_its_reply():
    async def scenario():
        h = Harness()
        register(h, query_spec())
        await establish(h)
        conversation_id = h.core.store.get(
            "conversations", digest(h.request()["message_key"]["channel"])
        )["conversation_id"]
        channel = h.request()["message_key"]["channel"]
        native = []

        async def send_native(destination, text):
            native.append(text)
            return ["native:" + str(len(native))]

        async def verify(service, request):
            if service != "companion":
                raise Fault("forbidden")

        bridge_store = Store(":memory:")
        try:
            bridge = Bridge(
                bridge_store,
                h.contracts,
                None,
                destinations={conversation_id: channel},
                send_native=send_native,
                verify_send=verify,
                clock=h.clock,
            )
            # The chat path and the command path share one outbound exit, so Core's own
            # sender is the same published send route the bridge serves.
            h.core.sender = BridgeSender(bridge)
            h.core.direct.delivery_port = BridgeDelivery(bridge)
            # A chat turn is sealed and still generating while the command arrives.
            gate = asyncio.Event()
            h.gateway.gates[2] = gate
            await h.ingest(text="第一条")
            h.clock.advance(6)
            await h.cycles(8)
            assert [t for t in h.turns() if t["phase"] == "generating"]
            entry = await h.core.direct_command("nonebot", h.request(text="/查询 天气"))
            await h.core.direct.work()
            assert h.core.direct.request_view(entry["request"]["id"])["reply_state"] == "sent"
            # The model finishes: the turn must still reach the user, in one increasing order
            # behind the functional receipt, instead of being rejected as an older position.
            gate.set()
            await h.cycles(40)
            # The turn reached the user in full - both of its segments - and the functional
            # receipt went out before it.
            assert [t["phase"] for t in h.turns()] == ["sent", "sent"]
            assert native[0] == "[合成] " + QUERY
            assert len([text for text in native if text.startswith("合成回复")]) == 2
            positions = [reply["sequence"] for reply in bridge_store.list("replies")]
            assert positions == sorted(positions) and len(set(positions)) == 3
            # The two segments of the chat turn stayed in one band, so the turn is ordered
            # as a unit behind the functional receipt instead of straddling two slots.
            assert positions[0] < positions[1]
            assert positions[2] - positions[1] == 1
            assert positions[1] // 100 == positions[2] // 100
        finally:
            bridge_store.close()
            await h.core.close()

    asyncio.run(scenario())


def test_shipped_synthetic_plugin_covers_a_read_only_query_and_a_slow_task():
    async def scenario():
        plugin = SyntheticPlugin(None)
        h = Harness(direct_options={"adapter": plugin})
        for item in plugin.registrations():
            h.core.direct.register_command(**item)
        await establish(h)
        entry = await h.core.direct_command("nonebot", body_for(h, "/查询 天气 明天"))
        await h.core.direct.work()
        view = h.core.direct.request_view(entry["request"]["id"])
        assert view["state"] == "completed"
        assert view["result"] == {
            "kind": "read_only_query",
            "subject": "天气",
            "detail": "明天",
            "synthetic": True,
        }
        assert view["result_verified"] is False
        assert view["result_evidence"] == "synthetic_adapter_result"
        assert view["plugin_gap"] == PLUGIN_GAP
        assert h.delivery.calls[0]["text"] == "[合成适配器] 天气"
        slow = await h.core.direct_command("nonebot", body_for(h, "/慢任务"))
        await h.core.direct.work()
        slow_view = h.core.direct.request_view(slow["request"]["id"])
        assert slow_view["state"] == "awaiting_result"
        h.core.direct.settle(
            slow_view["attempt_id"],
            dict(
                adapter="synthetic",
                request_id=slow_view["id"],
                attempt_id=slow_view["attempt_id"],
                state="completed",
                task_ref=None,
                result=dict(kind="slow_task", synthetic=True),
                reply=dict(text="[合成适配器] 慢任务完成"),
                channel_message_ids=[],
            ),
        )
        await h.core.direct.work()
        assert h.core.direct.request_view(slow_view["id"])["reply_state"] == "sent"
        assert h.delivery.calls[-1]["text"] == "[合成适配器] 慢任务完成"
        await h.core.close()

    asyncio.run(scenario())


# ------------------------------------------------------------------------ migration


def scratch(name):
    """A real directory holding a real SQLite file for the migration test.

    pytest's `tmp_path` and `tempfile` create their directories with mode 0o700, and this
    host's file sandbox refuses to *list* such a directory (`os.scandir` -> WinError 5).
    The backup files below must actually be inspected, so this test uses its own scratch
    directory with ordinary permissions instead of leaving the migration unverified.
    """
    path = Path(".runtime/tests") / f"{name}-{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_v6_migration_backup_rollback_and_source_preservation():
    directory = scratch("routing-migration")
    path = directory / "synthetic.db"
    store = Store(path)
    head = store.source_head()
    store.put("conversations", {"id": "synthetic", "private": "synthetic preserved"})
    store.close()
    with closing(sqlite3.connect(path)) as db, db:
        for table in DIRECT_TABLES:
            db.execute(f"DROP TABLE {table}")
        db.execute("PRAGMA user_version=6")
        # A conflicting index name makes migration fail midway, after prior DDL.
        db.execute("CREATE TABLE direct_requests_pending (id TEXT)")
    with pytest.raises(sqlite3.OperationalError):
        Store(path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 6
        assert not db.execute(
            "SELECT name FROM sqlite_master WHERE name='direct_attempts'"
        ).fetchone()
        db.execute("DROP TABLE direct_requests_pending")
        db.commit()
    with closing(Store(path)) as store:
        assert store.source_head() == head
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 7
        assert store.get("conversations", "synthetic")["private"] == "synthetic preserved"
        assert store.list("direct_requests") == []
        assert store.list("direct_commands") == []
        assert store.list("direct_attempts") == []
    backups = sorted(directory.glob("*.pre-routing-v7-*.bak"))
    assert len(backups) == 2
    for backup in backups:
        with closing(sqlite3.connect(backup)) as db:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 6
            assert (
                "synthetic preserved" in db.execute("SELECT body FROM conversations").fetchone()[0]
            )
    shutil.rmtree(directory, ignore_errors=True)


def test_the_request_scan_is_bounded_and_every_request_is_reached():
    async def scenario():
        h = Harness(
            direct_options={"max_scan": 1, "request_expiry": 60, "adapter": None, "deliver": None}
        )
        register(h, query_spec())
        await establish(h)
        identities = []
        for index in range(3):
            entry = await h.core.direct_command(
                "nonebot", h.request(text="/查询 天气", message="msg:%d" % index)
            )
            identities.append(entry["request"]["id"])
        h.clock.advance(61)
        states = []
        for _ in range(3):
            await h.core.direct.work()
            states.append(
                sorted(
                    item["id"] for item in h.core.direct.requests() if item["state"] == "cancelled"
                )
            )
        # One row per pass, a rotating cursor, and every request is reached exactly once.
        assert [len(item) for item in states] == [1, 2, 3]
        assert states[-1] == sorted(identities)
        await h.core.close()

    asyncio.run(scenario())


def test_the_rotating_scan_reaches_every_request_in_every_geometry():
    async def scenario():
        # Requests opened in the same millisecond share one `position`, and the cursor is
        # parked at the global last row - the geometry where "the wrap-around segment moves
        # the cursor backwards" could in principle re-scan the head and starve a middle band.
        for total, max_scan in ((7, 2), (5, 1), (2, 5), (9, 3)):
            h = Harness(
                direct_options={
                    "max_scan": max_scan,
                    "request_expiry": 60,
                    "adapter": None,
                    "deliver": None,
                }
            )
            register(h, query_spec())
            await establish(h)
            identities = []
            for index in range(total):
                entry = await h.core.direct_command(
                    "nonebot", h.request(text="/查询 天气", message="msg:%d" % index)
                )
                identities.append(entry["request"]["id"])
            rows = [
                (row[0], row[1])
                for row in h.core.direct.store.db.execute(
                    "SELECT id,position FROM direct_requests ORDER BY position,id"
                ).fetchall()
            ]
            assert len({position for _, position in rows}) == 1  # one millisecond
            # Park the durable cursor on the global last row, the starting point the
            # wrap-around is most likely to mishandle.
            last = max(rows, key=lambda row: (row[1], row[0]))
            h.core.direct.store.put(
                "metadata",
                dict(id="direct_scan_pending", position=last[1], row_id=last[0]),
            )
            h.clock.advance(61)
            seen = set()
            for _ in range(-(-total // max_scan) + 2):
                h.core.direct.tick(force=True)
                seen = {
                    item["id"] for item in h.core.direct.requests() if item["state"] == "cancelled"
                }
                if seen == set(identities):
                    break
            assert seen == set(identities), (total, max_scan, sorted(set(identities) - seen))
            await h.core.close()

    asyncio.run(scenario())
