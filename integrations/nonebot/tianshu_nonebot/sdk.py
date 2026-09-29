"""The verified OneBot v11 SDK boundary. No raw event upload or synthetic sender lives here."""

from dataclasses import dataclass
from datetime import datetime, timezone
import re


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

    def platform_body(self, connection_id: str, platform_id: str, self_id: str) -> dict:
        names = {"nickname": self.nickname, "group_card": self.group_card}
        return {
            "schema_version": 2 if any(names.values()) else 1,
            "connection_id": connection_id,
            "platform_id": platform_id,
            "self_id": self_id,
            "revision": 1,
            **{k: v for k, v in self.__dict__.items() if k not in names},
            **(names if any(names.values()) else {}),
        }


def conversation_id(event) -> str:
    if event.message_type == "group":
        return "group:" + qq_id(event.group_id)
    if event.message_type == "private":
        return "private:" + qq_id(event.user_id)
    raise UnsupportedEvent("Only OneBot v11 private and group messages are supported")


def text_event(bot, event) -> TextEvent:
    """Read typed SDK fields; reject media instead of losing segments or trusting CQ text."""
    from nonebot.adapters.onebot.v11 import GroupMessageEvent, PrivateMessageEvent

    if not isinstance(event, (GroupMessageEvent, PrivateMessageEvent)):
        raise UnsupportedEvent("Only OneBot v11 message events are supported")
    sender, _ = verified_sender(bot, event)
    if getattr(event, "sub_type", None) == "anonymous":
        raise UnsupportedEvent("Anonymous messages have no stable account owner")
    segments = list(event.get_message())
    if not segments:
        raise UnsupportedEvent("Empty message")
    parts = []
    for segment in segments:
        if segment.type == "text":
            parts.append(str(segment.data["text"]))
        elif segment.type == "at" and qq_id(segment.data.get("qq")) == qq_id(bot.self_id):
            continue  # Addressing this bot is transport metadata, not user text.
        else:
            raise UnsupportedEvent("Media, third-party mentions and reply segments are not enabled")
    text = "".join(parts)
    if not text.strip() or len(text) > 8000:
        raise UnsupportedEvent("Text must contain 1–8000 characters")
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
        nickname=display_name(getattr(getattr(event, "sender", None), "nickname", None)),
        group_card=display_name(getattr(getattr(event, "sender", None), "card", None))
        if event.message_type == "group"
        else None,
    )


def observation_event(bot, event) -> dict:
    """Normalize a real inbound SDK event, including unsupported content metadata."""
    from nonebot.adapters.onebot.v11 import GroupMessageEvent, PrivateMessageEvent

    if not isinstance(event, (GroupMessageEvent, PrivateMessageEvent)):
        raise UnsupportedEvent("not a OneBot message")
    sender, self_id = verified_sender(bot, event)
    if getattr(event, "sub_type", None) == "anonymous":
        raise UnsupportedEvent("no stable sender")
    parts = []
    mentioned = False
    unsupported = False
    for segment in event.get_message():
        if segment.type == "text":
            parts.append(str(segment.data.get("text", "")))
        elif segment.type == "at" and qq_id(segment.data.get("qq")) == self_id:
            mentioned = True
        else:
            unsupported = True
    text = "".join(parts)[:8000]
    content_state = "unsupported" if unsupported or not text.strip() else "text"
    return {
        "account_id": self_id,
        "conversation": conversation_id(event),
        "author": sender,
        "event_id": str(event.message_id),
        "sent_at": datetime.fromtimestamp(int(event.time), timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "text": text if content_state == "text" else "",
        "mentioned": mentioned,
        "content_state": content_state,
        "nickname": display_name(getattr(getattr(event, "sender", None), "nickname", None)),
        "group_card": display_name(getattr(getattr(event, "sender", None), "card", None))
        if isinstance(event, GroupMessageEvent)
        else None,
    }


async def send_text(bot, delivery: dict) -> list[str]:
    """Use the connected OneBot Bot and accept only a real SDK message_id."""
    from nonebot.adapters.onebot.v11 import Message, MessageSegment

    if delivery.get("namespace") != "qq" or delivery.get("thread_id") is not None:
        raise UnsupportedEvent("The delivery is not a OneBot v11 text destination")
    text = delivery.get("text")
    if not isinstance(text, str) or not text or len(text.encode("utf-8")) > 32768:
        raise UnsupportedEvent("The delivery is not text")
    target = delivery.get("conversation_id", "")
    message = Message(MessageSegment.text(text))
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
