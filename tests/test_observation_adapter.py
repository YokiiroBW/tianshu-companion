"""Host-independent v2 observation and claim rules, including durable restart."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "integrations" / "shared"))
from tianshu_adapter_rpc import AdapterService, PREFIX  # noqa: E402


def run(coro):
    return asyncio.run(coro)


def policy(mode="observe_only", names=(), observe=True):
    return {"observe": observe, "mode": mode, "list": list(names)}


def test_group_private_modes_and_restart(tmp_path):
    calls = []

    async def accounts():
        return [
            {"id": "10001", "platform": "qq", "label": "synthetic"},
            {"id": "90009", "platform": "qq", "label": "second"},
        ]

    async def send(self_id, target, text):
        calls.append((self_id, target, text))
        return "receipt-1"

    path = tmp_path / "adapter.sqlite"
    service = AdapterService(path, "nonebot", accounts, send)

    async def rpc(name, body):
        status, result = await service.handle(
            PREFIX + "/observation/" + name,
            "Bearer " + service.access_key,
            json.dumps(body).encode(),
        )
        assert status == 200, result
        return result

    async def scenario():
        assert (await rpc("capabilities", {}))["protocol"] == "tianshu.bot-observation/v2"
        request = {
            "request_id": "initial",
            "account_id": "10001",
            "revision": 1,
            "enabled": True,
            "group_policy": policy(),
            "private_policy": policy(),
        }
        assert (await rpc("apply", request))["revision"] == 1
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "44", "2026-09-29T00:00:00Z", "hello", True
        )
        assert not await service.observation_claimed("10001", "group:20002", "30003", "44")
        # A second bot with the same group and native event id has no enrollment.
        assert not await service.capture_observation(
            "90009", "group:20002", "30003", "44", "2026-09-29T00:00:00Z", "hello", True
        )
        request = {
            **request,
            "request_id": "white",
            "revision": 2,
            "group_policy": policy("whitelist"),
        }
        await rpc("apply", request)
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "45", "2026-09-29T00:00:01Z", "hello", True
        )
        assert not await service.observation_claimed("10001", "group:20002", "30003", "45")
        request = {
            **request,
            "request_id": "black",
            "revision": 3,
            "group_policy": policy("blacklist"),
            "private_policy": policy("blacklist"),
        }
        await rpc("apply", request)
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "46", "2026-09-29T00:00:02Z", "hello", False
        )
        assert not await service.observation_claimed("10001", "group:20002", "30003", "46")
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "47", "2026-09-29T00:00:03Z", "hello", True
        )
        assert await service.observation_claimed("10001", "group:20002", "30003", "47")
        assert await service.capture_observation(
            "10001", "private:30003", "30003", "48", "2026-09-29T00:00:04Z", "hello", False
        )
        assert await service.observation_claimed("10001", "private:30003", "30003", "48")
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "49", "2026-09-29T00:00:05Z", "", True, "unsupported"
        )
        assert not await service.observation_claimed("10001", "group:20002", "30003", "49")
        queued = await rpc("poll", {"account_id": "10001", "limit": 20})
        assert len(queued["events"]) == 6
        assert calls == []  # Observation never invokes the SDK sender.
        assert queued["events"][0]["event"]["scope_revision"] == 1
        await rpc("ack", {"account_id": "10001", "event_ids": [queued["events"][0]["id"]]})

    run(scenario())
    service.close()
    restarted = AdapterService(path, "nonebot", accounts, send)
    service = restarted
    queued = run(rpc("poll", {"account_id": "10001", "limit": 20}))
    assert len(queued["events"]) == 5
    assert run(service.observation_claimed("10001", "group:20002", "30003", "47"))
    service.close()


def test_send_rechecks_tightened_policy(tmp_path):
    calls = []

    async def accounts():
        return [{"id": "10001", "platform": "qq", "label": "synthetic"}]

    async def send(self_id, target, text):
        calls.append((self_id, target, text))
        return "555"

    async def send_media(self_id, target, text, media):
        calls.append((self_id, target, text, media))
        return "556"

    service = AdapterService(tmp_path / "adapter.sqlite", "astrbot", accounts, send, send_media)

    async def rpc(name, body):
        return await service.handle(
            PREFIX + "/observation/" + name,
            "Bearer " + service.access_key,
            json.dumps(body).encode(),
        )

    request = {
        "request_id": "initial",
        "account_id": "10001",
        "revision": 1,
        "enabled": True,
        "group_policy": policy("blacklist"),
        "private_policy": policy(),
    }
    assert run(rpc("apply", request))[0] == 200
    delivery = {
        "reply_id": "reply-1",
        "attempt_id": "attempt-1",
        "conversation_id": "group:20002",
        "namespace": "qq",
        "thread_id": None,
        "text": "answer",
        "turn_id": "turn:1",
        "segment_sequence": 1,
    }
    command = {
        "connection_id": "bot:abc",
        "account_id": "10001",
        "policy_revision": 1,
        "delivery": delivery,
    }
    status, receipt = run(rpc("messages/send", command))
    assert status == 200 and receipt["state"] == "sent" and len(calls) == 1
    assert run(rpc("messages/send", command))[1] == receipt
    from adapter_media_fixture import original_png

    _, reference, media = original_png()
    image_command = {
        **command,
        "delivery": {
            **delivery,
            "reply_id": "pure-image",
            "attempt_id": "pure-image",
            "text": "",
            "content_refs": [reference],
            "media": [media],
        },
    }
    status, image_receipt = run(rpc("messages/send", image_command))
    assert status == 200 and image_receipt["state"] == "sent"
    assert image_receipt["channel_message_ids"] == ["556"]
    assert run(rpc("messages/send", image_command))[1] == image_receipt
    assert len(calls) == 2 and calls[-1][2] == ""
    empty = {
        **command,
        "delivery": {**delivery, "reply_id": "empty", "text": ""},
    }
    assert run(rpc("messages/send", empty))[0] == 403
    assert len(calls) == 2
    request = {**request, "request_id": "tighten", "revision": 2, "group_policy": policy()}
    assert run(rpc("apply", request))[0] == 200
    command["delivery"] = {**delivery, "reply_id": "reply-2"}
    command["policy_revision"] = 2
    assert run(rpc("messages/send", command))[0] == 403
    assert len(calls) == 2
    service.close()


def test_claim_requires_current_policy_and_live_platform_lease(tmp_path):
    async def accounts():
        return [{"id": "10001", "platform": "qq", "label": "synthetic"}]

    async def send(*_):
        raise AssertionError("capture must never send")

    service = AdapterService(tmp_path / "adapter.sqlite", "nonebot", accounts, send)

    async def rpc(name, body):
        status, result = await service.handle(
            PREFIX + "/observation/" + name,
            "Bearer " + service.access_key,
            json.dumps(body).encode(),
        )
        assert status == 200, result
        return result

    async def scenario():
        request = {
            "request_id": "allow",
            "account_id": "10001",
            "revision": 1,
            "enabled": True,
            "group_policy": policy("whitelist", ["20002"]),
            "private_policy": policy(),
        }
        await rpc("apply", request)
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "44", "2026-09-29T00:00:00Z", "hello", True
        )
        assert await service.observation_claimed("10001", "group:20002", "30003", "44")
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "44b", "2026-09-29T00:00:00Z", "hello", True
        )
        await rpc(
            "apply", {**request, "request_id": "tighten", "revision": 2, "group_policy": policy()}
        )
        assert await service.observation_claimed("10001", "group:20002", "30003", "44")
        assert not await service.observation_claimed("10001", "group:20002", "30003", "44b")
        await rpc("apply", {**request, "request_id": "allow-again", "revision": 3})
        service.db.execute("UPDATE observation_accounts SET last_poll=0 WHERE account_id='10001'")
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "45", "2026-09-29T00:00:01Z", "hello", True
        )
        assert not await service.observation_claimed("10001", "group:20002", "30003", "45")
        polled = await rpc("poll", {"account_id": "10001", "limit": 20})
        assert [item["reply_claimed"] for item in polled["events"]] == [True, False, False]
        assert not await service.observation_claimed("10001", "group:20002", "30003", "45")

    run(scenario())
    service.close()


def test_poll_waits_for_matcher_claim_decision(tmp_path):
    async def accounts():
        return [{"id": "10001", "platform": "qq", "label": "synthetic"}]

    async def send(*_):
        raise AssertionError("no SDK send")

    service = AdapterService(tmp_path / "adapter.sqlite", "nonebot", accounts, send)

    async def rpc(name, body):
        status, result = await service.handle(
            PREFIX + "/observation/" + name,
            "Bearer " + service.access_key,
            json.dumps(body).encode(),
        )
        assert status == 200, result
        return result

    async def scenario():
        await rpc(
            "apply",
            {
                "request_id": "allow",
                "account_id": "10001",
                "revision": 1,
                "enabled": True,
                "group_policy": policy("whitelist", ["20002"]),
                "private_policy": policy(),
            },
        )
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "44", "2026-09-29T00:00:00Z", "hello", True
        )
        # Platform wins the scheduler before NoneBot's priority-2 matcher.
        assert (await rpc("poll", {"account_id": "10001", "limit": 20}))["events"] == []
        assert await service.observation_claimed("10001", "group:20002", "30003", "44")
        polled = await rpc("poll", {"account_id": "10001", "limit": 20})
        assert len(polled["events"]) == 1 and polled["events"][0]["reply_claimed"]
        await rpc("ack", {"account_id": "10001", "event_ids": [polled["events"][0]["id"]]})
        assert (await rpc("poll", {"account_id": "10001", "limit": 20}))["events"] == []
        # A matcher that never runs is resolved after its bound and never
        # gains reply ownership just because the next Platform poll arrives.
        assert await service.capture_observation(
            "10001", "group:20002", "30003", "45", "2026-09-29T00:00:01Z", "hello", True
        )
        service.db.execute("UPDATE observations SET claim_deadline=0 WHERE native_id='45'")
        polled = await rpc("poll", {"account_id": "10001", "limit": 20})
        assert len(polled["events"]) == 1 and not polled["events"][0]["reply_claimed"]
        assert not await service.observation_claimed("10001", "group:20002", "30003", "45")

    run(scenario())
    service.close()
