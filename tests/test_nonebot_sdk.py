"""Real NoneBot 2.5 / OneBot 2.4 models, with only synthetic account data."""

import asyncio

import httpx
import pytest
from nonebot.adapters.onebot.v11 import GroupMessageEvent

from tianshu_nonebot.journal import Conflict, Journal
from tianshu_nonebot.platform import PlatformError, PlatformPort
from tianshu_nonebot.runtime import BotRuntime
from tianshu_nonebot.sdk import UnsupportedEvent, send_text, text_event, verified_sender


def group_event(message_id=12, text="hello", user_id=7, segments=None):
    message = segments or [{"type": "text", "data": {"text": text}}]
    return GroupMessageEvent.model_validate(
        {
            "time": 1_700_000_000,
            "self_id": 42,
            "post_type": "message",
            "sub_type": "normal",
            "user_id": user_id,
            "message_type": "group",
            "message_id": message_id,
            "message": message,
            "original_message": message,
            "raw_message": text,
            "font": 0,
            "sender": {"user_id": user_id},
            "group_id": 99,
            "to_me": True,
        }
    )


class FakeBot:
    self_id = "42"

    def __init__(self, result=None, failure=None):
        self.calls = []
        self.result = result or {"message_id": 888}
        self.failure = failure

    async def send_group_msg(self, **kwargs):
        self.calls.append(kwargs)
        if self.failure:
            raise self.failure
        return self.result

    async def send_private_msg(self, **kwargs):
        return await self.send_group_msg(**kwargs)


def delivery(**changes):
    value = dict(
        reply_id="reply:1",
        attempt_id="attempt:1",
        namespace="qq",
        conversation_id="group:99",
        thread_id=None,
        text="answer",
        turn_id="turn:1",
        segment_sequence=1,
    )
    value.update(changes)
    return value


def test_real_sdk_event_and_text_sender():
    bot = FakeBot()
    value = text_event(bot, group_event())
    assert value.platform_body("conn", "host", bot.self_id) == {
        "schema_version": 1,
        "connection_id": "conn",
        "platform_id": "host",
        "self_id": "42",
        "event_id": "12",
        "revision": 1,
        "namespace": "qq",
        "conversation_id": "group:99",
        "thread_id": None,
        "account_id": "7",
        "sent_at": "2023-11-14T22:13:20.000Z",
        "text": "hello",
    }
    assert asyncio.run(send_text(bot, delivery())) == ["888"]
    assert bot.calls[0]["group_id"] == 99
    assert [(s.type, s.data) for s in bot.calls[0]["message"]] == [("text", {"text": "answer"})]
    with pytest.raises(UnsupportedEvent):
        text_event(bot, group_event(segments=[{"type": "image", "data": {"file": "x"}}]))
    assert (
        text_event(
            bot,
            group_event(
                segments=[
                    {"type": "at", "data": {"qq": "42"}},
                    {"type": "text", "data": {"text": "hello"}},
                ]
            ),
        ).text
        == "hello"
    )
    with pytest.raises(UnsupportedEvent):
        text_event(
            bot,
            group_event(
                segments=[
                    {"type": "at", "data": {"qq": "100"}},
                    {"type": "text", "data": {"text": "hello"}},
                ]
            ),
        )
    with pytest.raises(UnsupportedEvent):
        text_event(bot, group_event(user_id=42))
    with pytest.raises(ValueError):
        asyncio.run(send_text(FakeBot(result={"message_id": None}), delivery()))


def test_sender_conflicts_invalid_qq_and_cq_output_remains_text():
    from types import SimpleNamespace

    bot = FakeBot()
    for value in (True, 1.5, "０１００１", "001", None):
        with pytest.raises(UnsupportedEvent):
            verified_sender(bot, SimpleNamespace(self_id="42", user_id=value, sender=None))
    with pytest.raises(UnsupportedEvent):
        verified_sender(
            bot,
            SimpleNamespace(self_id="42", user_id="1001", sender=SimpleNamespace(user_id="1002")),
        )
    output = "[CQ:at,qq=all] [CQ:image,file=private]"
    assert asyncio.run(send_text(bot, delivery(text=output))) == ["888"]
    assert bot.calls[-1]["message"][0].type == "text"
    assert "&#91;CQ:at" in str(bot.calls[-1]["message"])
    for target in ("group:０９９", "group:099", "group:-1", "private:0", "group:99:1"):
        with pytest.raises(UnsupportedEvent):
            asyncio.run(send_text(bot, delivery(conversation_id=target)))


def test_journal_recovery_dedup_and_unknown(tmp_path):
    path = tmp_path / "bot.db"
    body = text_event(FakeBot(), group_event()).platform_body("conn", "host", "42")
    journal = Journal(path)
    journal.bind("conn", "host", "42")
    with pytest.raises(Conflict):
        journal.bind("other", "host", "42")
    key = journal.capture(body)
    assert journal.capture(body) == key
    with pytest.raises(Conflict):
        journal.capture({**body, "text": "changed"})
    assert journal.start_event(key)
    assert journal.claim(delivery())
    journal.close()
    journal = Journal(path)
    journal.recover_events()
    assert journal.pending_events() == []
    assert journal.unknown_events()[0][0] == key
    assert not journal.claim(delivery())
    assert journal.unacked()[0]["state"] == "unknown"
    with pytest.raises(Conflict):
        journal.claim(delivery(attempt_id="attempt:2"))
    journal.close()


class FakePort:
    def __init__(self):
        self.events, self.acks = [], []
        self.fail_ack_once = False

    async def event(self, body):
        self.events.append(body)
        return {"event_id": body["event_id"], "state": "accepted"}

    async def ack(self, body):
        self.acks.append(body)
        if self.fail_ack_once:
            self.fail_ack_once = False
            raise PlatformError()
        return {"reply_id": body["reply_id"], "state": body["state"]}

    async def close(self):
        pass


def test_runtime_one_send_and_ack_after_reconnect(tmp_path):
    async def run():
        port, bot = FakePort(), FakeBot()
        options = dict(
            connection_id="conn",
            platform_id="host",
            bot_self_id="42",
            allowed_conversations=["group:99"],
            journal_path=tmp_path / "b.db",
            platform_url="http://127.0.0.1:8000",
            token="secret",
            get_bot=lambda _: bot,
            port=port,
        )
        runtime = BotRuntime(**options)
        assert await runtime.capture(bot, group_event())
        assert await runtime.capture(bot, group_event())
        await runtime.flush_events()
        assert len(port.events) == 1
        assert not await runtime.capture(bot, group_event(user_id=42))
        await runtime.deliver(bot, delivery())
        await runtime.flush_acks()
        assert len(bot.calls) == 1
        assert port.acks[0]["state"] == "sent"
        assert port.acks[0]["channel_message_ids"] == ["888"]
        await runtime.close()
        other = BotRuntime(**options)
        await other.deliver(bot, delivery())
        assert len(bot.calls) == 1
        await other.close()

    asyncio.run(run())


def test_platform_port_uses_connection_bearer_and_exact_paths():
    calls = []

    def handler(request):
        calls.append((request.url.path, request.headers["Authorization"], request.content))
        if request.url.path.endswith("/events/status"):
            return httpx.Response(200, json={"found": False, "state": None})
        if request.url.path.endswith("/replies/claim"):
            return httpx.Response(200, json={"deliveries": [delivery()]})
        return httpx.Response(200, json={"reply_id": "reply:1"})

    async def run():
        port = PlatformPort(
            "http://127.0.0.1:80", "scope-token", transport=httpx.MockTransport(handler)
        )
        assert (await port.claim("conn", "instance"))[0]["attempt_id"] == "attempt:1"
        assert not (
            await port.event_status({"connection_id": "conn", "event_id": "12", "account_id": "7"})
        )["found"]
        await port.close()

    asyncio.run(run())
    assert all(auth == "Bearer scope-token" for _, auth, _ in calls)
    assert [path for path, _, _ in calls] == [
        "/internal/v1/bot/replies/claim",
        "/internal/v1/bot/events/status",
    ]
    with pytest.raises(ValueError):
        PlatformPort("http://platform.example", "scope-token")


def test_lost_ack_replays_receipt_without_repeating_sdk_send(tmp_path):
    async def run():
        bot, port = FakeBot(), FakePort()
        port.fail_ack_once = True
        options = dict(
            connection_id="conn",
            platform_id="host",
            bot_self_id="42",
            allowed_conversations=["group:99"],
            journal_path=tmp_path / "replay.db",
            platform_url="http://127.0.0.1:8000",
            token="secret",
            get_bot=lambda _: bot,
            port=port,
        )
        runtime = BotRuntime(**options)
        await runtime.deliver(bot, delivery())
        await runtime.flush_acks()
        assert len(bot.calls) == 1
        await runtime.close()
        recovered = BotRuntime(**options)
        await recovered.flush_acks()
        assert len(bot.calls) == 1
        assert [ack["state"] for ack in port.acks] == ["sent", "sent"]
        assert port.acks[0] == port.acks[1]
        await recovered.close()

    asyncio.run(run())
