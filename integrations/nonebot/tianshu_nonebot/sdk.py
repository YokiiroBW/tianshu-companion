"""The verified OneBot v11 SDK boundary. No raw event upload or synthetic sender lives here."""

import re
from dataclasses import dataclass
from datetime import datetime, timezone


class UnsupportedEvent(ValueError):
    """The event is outside the explicitly enabled text slice."""


def qq_id(value):
    if type(value) is int and value > 0:
        return str(value)
    if type(value) is str and re.fullmatch(r"[1-9][0-9]*", value):
        return value
    raise UnsupportedEvent("Invalid QQ identifier")


def verified_sender(bot, event):
    self_id = qq_id(bot.self_id)
    if qq_id(event.self_id) != self_id:
        raise UnsupportedEvent("Bot account mismatch")
    sender = qq_id(event.user_id)
    nested = getattr(getattr(event, "sender", None), "user_id", None)
    if nested is not None and qq_id(nested) != sender:
        raise UnsupportedEvent("Conflicting sender identifiers")
    if sender == self_id:
        raise UnsupportedEvent("Self message")
    return sender, self_id


def display_name(value):
    if type(value) is not str:
        return None
    text = "".join(
        char
        for char in value
        if ord(char) >= 32
        and ord(char) != 127
        and not 0x202A <= ord(char) <= 0x202E
        and not 0x2066 <= ord(char) <= 0x2069
    ).strip()[:80]
    return text or None


@dataclass(frozen=True)
class TextEvent:
    event_id: str
    namespace: str
    conversation_id: str
    thread_id: None
    account_id: str
    sent_at: str
    text: str
    nickname: str | None = None
    group_card: str | None = None
    message_context: dict | None = None

    def platform_body(self, connection_id: str, platform_id: str, self_id: str) -> dict:
        names = {"nickname": self.nickname, "group_card": self.group_card}
        value = {
            "schema_version": 2 if any(names.values()) else 1,
            "connection_id": connection_id,
            "platform_id": platform_id,
            "self_id": self_id,
            "revision": 1,
            **{k: v for k, v in self.__dict__.items() if k not in names and k != "message_context"},
            **(names if any(names.values()) else {}),
        }
        if self.message_context is not None:
            value.update(schema_version=3, **names, message_context=self.message_context)
        return value


def conversation_id(event) -> str:
    if event.message_type == "group":
        return "group:" + qq_id(event.group_id)
    if event.message_type == "private":
        return "private:" + qq_id(event.user_id)
    raise UnsupportedEvent("Only OneBot v11 private and group messages are supported")


def event_enabled(bot, event, enabled, *, reply=False):
    """Typed physical routing only; do not touch or normalize the message body."""
    from nonebot.adapters.onebot.v11 import Bot, GroupMessageEvent, PrivateMessageEvent

    if not isinstance(bot, Bot) or not isinstance(event, (GroupMessageEvent, PrivateMessageEvent)):
        return False
    try:
        verified_sender(bot, event)
        return enabled(qq_id(bot.self_id), conversation_id(event), reply=reply)
    except (UnsupportedEvent, ValueError, KeyError):
        return False


def text_event(bot, event, *, enabled=None) -> TextEvent:
    """Read typed SDK fields; reject media instead of losing segments or trusting CQ text."""
    from nonebot.adapters.onebot.v11 import GroupMessageEvent, PrivateMessageEvent

    if not isinstance(event, (GroupMessageEvent, PrivateMessageEvent)):
        raise UnsupportedEvent("Only OneBot v11 message events are supported")
    sender, _ = verified_sender(bot, event)
    if enabled is not None and not enabled(qq_id(bot.self_id), conversation_id(event), reply=True):
        raise UnsupportedEvent("group reply disabled")
    if getattr(event, "sub_type", None) == "anonymous":
        raise UnsupportedEvent("Anonymous messages have no stable account owner")
    # OneBot's event preprocessor removes reply and @bot segments from message.
    # original_message is the SDK's preserved copy of the actual received chain.
    segments = list(event.original_message)
    if not segments:
        raise UnsupportedEvent("Empty message")
    from .rpc import RpcError, onebot_input

    try:
        normalized = onebot_input(
            [dict(type=s.type, data=s.data) for s in segments],
            qq_id(bot.self_id),
            group=event.message_type == "group",
        )
    except RpcError as error:
        raise UnsupportedEvent("Invalid message metadata") from error
    if normalized["content_state"] != "text":
        raise UnsupportedEvent("Media or empty text is not enabled")
    text = normalized["text"]
    return TextEvent(
        event_id=str(event.message_id),
        namespace="qq",
        conversation_id=conversation_id(event),
        thread_id=None,
        account_id=sender,
        sent_at=datetime.fromtimestamp(int(event.time), timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        text=text,
        message_context=normalized["message_context"],
        nickname=display_name(getattr(getattr(event, "sender", None), "nickname", None)),
        group_card=display_name(getattr(getattr(event, "sender", None), "card", None))
        if event.message_type == "group"
        else None,
    )


def observation_event(bot, event, *, enabled=None) -> dict:
    """Normalize a real inbound SDK event, including unsupported content metadata."""
    from nonebot.adapters.onebot.v11 import GroupMessageEvent, PrivateMessageEvent

    if not isinstance(event, (GroupMessageEvent, PrivateMessageEvent)):
        raise UnsupportedEvent("not a OneBot message")
    sender, self_id = verified_sender(bot, event)
    if enabled is not None and not enabled(self_id, conversation_id(event)):
        raise UnsupportedEvent("group disabled")
    if getattr(event, "sub_type", None) == "anonymous":
        raise UnsupportedEvent("no stable sender")
    from .rpc import RpcError, onebot_input

    try:
        normalized = onebot_input(
            [dict(type=s.type, data=s.data) for s in event.original_message],
            self_id,
            group=event.message_type == "group",
        )
    except RpcError as error:
        raise UnsupportedEvent("Invalid message metadata") from error
    return {
        "account_id": self_id,
        "conversation": conversation_id(event),
        "author": sender,
        "event_id": str(event.message_id),
        "sent_at": datetime.fromtimestamp(int(event.time), timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        **normalized,
        "nickname": display_name(getattr(getattr(event, "sender", None), "nickname", None)),
        "group_card": display_name(getattr(getattr(event, "sender", None), "card", None))
        if isinstance(event, GroupMessageEvent)
        else None,
    }


async def send_text(bot, delivery: dict) -> list[str]:
    """Use the connected OneBot Bot and accept only a real SDK message_id."""
    from nonebot.adapters.onebot.v11 import Message, MessageSegment

    from .rpc import RpcError, media_parts, onebot_media

    if delivery.get("namespace") != "qq" or delivery.get("thread_id") is not None:
        raise UnsupportedEvent("The delivery is not a OneBot v11 text destination")
    text = delivery.get("text")
    if not isinstance(text, str) or len(text.encode("utf-8")) > 32768:
        raise UnsupportedEvent("The delivery is not text")
    target = delivery.get("conversation_id", "")
    message = Message(MessageSegment.text(text))
    try:
        media = media_parts(delivery)
    except RpcError:
        raise UnsupportedEvent("Invalid inline media") from None
    if not (text or media):
        raise UnsupportedEvent("The delivery is empty")
    for segment in onebot_media(media):
        message += MessageSegment(segment["type"], segment["data"])
    if type(target) is str and target.startswith("group:"):
        result = await bot.send_group_msg(group_id=int(qq_id(target[6:])), message=message)
    elif type(target) is str and target.startswith("private:"):
        result = await bot.send_private_msg(user_id=int(qq_id(target[8:])), message=message)
    else:
        raise UnsupportedEvent("Unknown conversation destination")
    if not isinstance(result, dict) or isinstance(result.get("message_id"), bool):
        raise ValueError("SDK did not confirm a message ID")
    message_id = result.get("message_id")
    if not isinstance(message_id, (int, str)) or not str(message_id).strip():
        raise ValueError("SDK did not confirm a message ID")
    return [str(message_id)]
