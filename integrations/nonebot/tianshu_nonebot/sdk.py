"""The verified OneBot v11 SDK boundary. No raw event upload or synthetic sender lives here."""

from dataclasses import dataclass
from datetime import datetime, timezone


class UnsupportedEvent(ValueError):
    """The event is outside the explicitly enabled text slice."""


@dataclass(frozen=True)
class TextEvent:
    event_id: str
    namespace: str
    conversation_id: str
    thread_id: None
    account_id: str
    sent_at: str
    text: str

    def platform_body(self, connection_id: str, platform_id: str, self_id: str) -> dict:
        return {
            "schema_version": 1,
            "connection_id": connection_id,
            "platform_id": platform_id,
            "self_id": self_id,
            "revision": 1,
            **self.__dict__,
        }


def conversation_id(event) -> str:
    if event.message_type == "group":
        return f"group:{int(event.group_id)}"
    if event.message_type == "private":
        return f"private:{int(event.user_id)}"
    raise UnsupportedEvent("Only OneBot v11 private and group messages are supported")


def text_event(bot, event) -> TextEvent:
    """Read typed SDK fields; reject media instead of losing segments or trusting CQ text."""
    from nonebot.adapters.onebot.v11 import GroupMessageEvent, PrivateMessageEvent

    if not isinstance(event, (GroupMessageEvent, PrivateMessageEvent)):
        raise UnsupportedEvent("Only OneBot v11 message events are supported")
    if str(event.self_id) != str(bot.self_id) or str(event.user_id) == str(bot.self_id):
        raise UnsupportedEvent("The event is not an incoming message for this bot")
    if getattr(event, "sub_type", None) == "anonymous":
        raise UnsupportedEvent("Anonymous messages have no stable account owner")
    segments = list(event.get_message())
    if not segments:
        raise UnsupportedEvent("Empty message")
    parts = []
    for segment in segments:
        if segment.type == "text":
            parts.append(str(segment.data["text"]))
        elif segment.type == "at" and str(segment.data.get("qq")) == str(bot.self_id):
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
        account_id=str(event.user_id),
        sent_at=datetime.fromtimestamp(int(event.time), timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        text=text,
    )


def observation_event(bot, event) -> dict:
    """Normalize a real inbound SDK event, including unsupported content metadata."""
    from nonebot.adapters.onebot.v11 import GroupMessageEvent, PrivateMessageEvent

    if not isinstance(event, (GroupMessageEvent, PrivateMessageEvent)):
        raise UnsupportedEvent("not a OneBot message")
    if str(event.self_id) != str(bot.self_id) or str(event.user_id) == str(bot.self_id):
        raise UnsupportedEvent("not an incoming message for this bot")
    if getattr(event, "sub_type", None) == "anonymous":
        raise UnsupportedEvent("no stable sender")
    parts = []
    mentioned = False
    unsupported = False
    for segment in event.get_message():
        if segment.type == "text":
            parts.append(str(segment.data.get("text", "")))
        elif segment.type == "at" and str(segment.data.get("qq")) == str(bot.self_id):
            mentioned = True
        else:
            unsupported = True
    text = "".join(parts)[:8000]
    content_state = "unsupported" if unsupported or not text.strip() else "text"
    return {
        "account_id": str(bot.self_id),
        "conversation": conversation_id(event),
        "author": str(event.user_id),
        "event_id": str(event.message_id),
        "sent_at": datetime.fromtimestamp(int(event.time), timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "text": text if content_state == "text" else "",
        "mentioned": mentioned,
        "content_state": content_state,
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
    if target.startswith("group:"):
        result = await bot.send_group_msg(group_id=int(target[6:]), message=message)
    elif target.startswith("private:"):
        result = await bot.send_private_msg(user_id=int(target[8:]), message=message)
    else:
        raise UnsupportedEvent("Unknown conversation destination")
    if not isinstance(result, dict) or isinstance(result.get("message_id"), bool):
        raise ValueError("SDK did not confirm a message ID")
    message_id = result.get("message_id")
    if not isinstance(message_id, (int, str)) or not str(message_id).strip():
        raise ValueError("SDK did not confirm a message ID")
    return [str(message_id)]
