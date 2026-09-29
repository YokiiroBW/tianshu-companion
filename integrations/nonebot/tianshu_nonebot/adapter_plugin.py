"""NoneBot 2.5 FastAPI driver adapter transport, loaded after OneBot registration."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

if "tianshu_nonebot.plugin" in sys.modules:
    raise RuntimeError("Tianshu pull and adapter transports cannot share a NoneBot host")

from fastapi import Request
from fastapi.responses import JSONResponse
from nonebot import get_asgi, get_bots, get_driver, get_plugin_config, on_message
from nonebot.adapters import Bot as BaseBot
from nonebot.adapters import Event as BaseEvent
from nonebot.adapters.onebot.v11 import Bot as OneBotBot
from nonebot.plugin import PluginMetadata
from pydantic import BaseModel

from .rpc import AdapterService, MAX_REQUEST, PREFIX
from .sdk import UnsupportedEvent, observation_event, text_event


class Config(BaseModel):
    tianshu_adapter_data_dir: Path = Path("data/tianshu_nonebot")
    tianshu_adapter_priority: int = 5


__plugin_meta__ = PluginMetadata(
    name="天枢陪伴 OneBot 适配器",
    description="由天枢平台主动读取事件并发送回复；需先绑定精确会话和作者",
    usage="安装后本机显示连接密钥，在天枢网页填写插件地址和密钥。",
    type="application",
    config=Config,
    supported_adapters={"~nonebot.adapters.onebot.v11"},
)

settings = get_plugin_config(Config)
driver = get_driver()
if getattr(driver, "type", None) != "fastapi":
    raise RuntimeError("Tianshu adapter requires NoneBot FastAPI driver")


async def _accounts():
    result = []
    for bot in get_bots().values():
        if not isinstance(bot, OneBotBot):
            continue
        try:
            info = await asyncio.wait_for(bot.get_login_info(), 3)
        except Exception:
            continue
        if not isinstance(info, dict) or str(info.get("user_id")) != str(bot.self_id):
            continue
        result.append(
            {
                "id": str(bot.self_id),
                "platform": "qq",
                "label": str(info.get("nickname") or bot.self_id)[:128],
            }
        )
    return result


async def _send(self_id: str, target: str, text: str):
    bot = get_bots().get(self_id)
    if not isinstance(bot, OneBotBot):
        raise RuntimeError("SDK offline")
    from nonebot.adapters.onebot.v11 import Message, MessageSegment

    message = Message(MessageSegment.text(text))
    kind, value = target.split(":", 1)
    if kind == "group":
        response = await bot.send_group_msg(group_id=int(value), message=message)
    else:
        response = await bot.send_private_msg(user_id=int(value), message=message)
    if not isinstance(response, dict):
        raise ValueError("SDK receipt missing")
    return response.get("message_id")


service = AdapterService(
    settings.tianshu_adapter_data_dir / "adapter.sqlite3", "nonebot", _accounts, _send
)
semaphore = asyncio.Semaphore(8)
app = get_asgi()
_routes = []


async def _rpc(request: Request):
    if request.headers.get("content-type", "").split(";", 1)[0].lower() != "application/json":
        return JSONResponse({"code": "invalid_input", "retryable": False}, status_code=400)
    if semaphore.locked():
        return JSONResponse({"code": "busy", "retryable": True}, status_code=429)
    async with semaphore:
        try:
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > MAX_REQUEST:
                    return JSONResponse(
                        {"code": "invalid_input", "retryable": False}, status_code=400
                    )
            status, payload = await asyncio.wait_for(
                service.handle(request.url.path, request.headers.get("authorization"), bytes(body)),
                20,
            )
        except asyncio.TimeoutError:
            status, payload = 503, {"code": "dependency_unavailable", "retryable": True}
        return JSONResponse(payload, status_code=status)


for suffix in (
    "capabilities",
    "bindings/apply",
    "bindings/status",
    "events/poll",
    "events/ack",
    "messages/send",
    "messages/status",
    "observation/capabilities",
    "observation/apply",
    "observation/status",
    "observation/poll",
    "observation/ack",
    "observation/messages/send",
    "observation/messages/status",
):
    path = f"{PREFIX}/{suffix}"
    app.add_api_route(
        path,
        _rpc,
        methods=["POST"],
        include_in_schema=False,
        name=f"tianshu-adapter-{suffix.replace('/', '-')}",
    )
    _routes.append(app.routes[-1])


async def _observe(bot: BaseBot, event: BaseEvent) -> bool:
    if not isinstance(bot, OneBotBot):
        return False
    try:
        value = observation_event(bot, event)
    except (UnsupportedEvent, ValueError, KeyError, OverflowError, OSError):
        return False
    await service.capture_observation(
        value["account_id"],
        value["conversation"],
        value["author"],
        value["event_id"],
        value["sent_at"],
        value["text"],
        value["mentioned"],
        value["content_state"],
        True,
        value["nickname"],
        value["group_card"],
    )
    return False  # Observing never claims reply ownership in NoneBot.


observer = on_message(rule=_observe, priority=1, block=False)


@observer.handle()
async def _observed():
    return None


async def _claim_observation_reply(bot: BaseBot, event: BaseEvent) -> bool:
    if not isinstance(bot, OneBotBot):
        return False
    try:
        value = observation_event(bot, event)
    except (UnsupportedEvent, ValueError, KeyError, OverflowError, OSError):
        return False
    return await service.observation_claimed(
        value["account_id"], value["conversation"], value["author"], value["event_id"]
    )


# A live Platform lease and the current policy are required for ownership.
observation_reply = on_message(rule=_claim_observation_reply, priority=2, block=True)


@observation_reply.handle()
async def _claimed_observation_reply():
    return None


async def _scope(bot: BaseBot, event: BaseEvent) -> bool:
    if not isinstance(bot, OneBotBot):
        return False
    try:
        value = text_event(bot, event)
    except (UnsupportedEvent, ValueError, KeyError):
        return False
    return await service.capture(
        str(bot.self_id),
        value.conversation_id,
        value.account_id,
        value.event_id,
        value.sent_at,
        value.text,
        value.nickname,
        value.group_card,
    )


# Rule runs before blocking. Unknown authors and conversations pass to other plugins.
matcher = on_message(rule=_scope, priority=settings.tianshu_adapter_priority, block=True)


@matcher.handle()
async def _handled():
    return None


def unregister():
    """Explicit host shutdown/unload hook; NoneBot itself has no hot-unload API."""
    for route in _routes:
        if route in app.routes:
            app.routes.remove(route)
    _routes.clear()
    service.close()


driver.on_shutdown(unregister)
