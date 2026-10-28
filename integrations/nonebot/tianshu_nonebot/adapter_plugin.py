"""NoneBot 2.5 FastAPI driver adapter transport, loaded after OneBot registration."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Literal

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

from .rpc import PREFIX, AdapterService, onebot_media, request_limit
from .sdk import UnsupportedEvent, event_enabled, group_identity, observation_event, text_event
from .participation_host import execution, install as install_participation_host
from .adapter_lifecycle import AdapterLifecycle


class Config(BaseModel):
    tianshu_adapter_data_dir: Path = Path("data/tianshu_nonebot")
    tianshu_adapter_priority: int = 5
    tianshu_adapter_kind: Literal["nonebot", "onebot"] = "nonebot"
    storage_backends: dict = {}
    tianshu_adapter_storage_environ: dict[str, str] | None = None


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


async def _send(self_id: str, target: str, text: str, media=None):
    bot = get_bots().get(self_id)
    if not isinstance(bot, OneBotBot):
        raise RuntimeError("SDK offline")
    from nonebot.adapters.onebot.v11 import Message, MessageSegment

    message = Message(MessageSegment.text(text))
    for segment in onebot_media(media or []):
        message += MessageSegment(segment["type"], segment["data"])
    kind, value = target.split(":", 1)
    if kind == "group":
        response = await bot.send_group_msg(group_id=int(value), message=message)
    else:
        response = await bot.send_private_msg(user_id=int(value), message=message)
    if not isinstance(response, dict):
        raise ValueError("SDK receipt missing")
    return response.get("message_id")


service = AdapterService(
    settings.tianshu_adapter_data_dir / "adapter.sqlite3",
    settings.tianshu_adapter_kind,
    _accounts,
    _send,
    _send,
    storage_backends=settings.storage_backends,
    storage_environ=settings.tianshu_adapter_storage_environ,
)
lifecycle = AdapterLifecycle(service)
semaphore = asyncio.Semaphore(8)
app = get_asgi()
app.state.lifecycle = lifecycle
_routes = []


async def _rpc(request: Request):
    async with lifecycle.operation() as accepted:
        if not accepted:
            return JSONResponse(
                {"code": "dependency_unavailable", "retryable": True}, status_code=503
            )
        return await _handle_rpc(request)


async def _handle_rpc(request: Request):
    if not service.authorized(request.headers.get("authorization")):
        return JSONResponse({"code": "unauthorized", "retryable": False}, status_code=401)
    if request.headers.get("content-type", "").split(";", 1)[0].lower() != "application/json":
        return JSONResponse({"code": "invalid_input", "retryable": False}, status_code=400)
    if semaphore.locked():
        return JSONResponse({"code": "busy", "retryable": True}, status_code=429)
    async with semaphore:
        try:
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > request_limit(request.url.path):
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
    "observation/groups",
    "observation/apply",
    "observation/status",
    "observation/poll",
    "observation/ack",
    "observation/messages/send",
    "observation/messages/status",
    "participation/capabilities",
    "participation/ownership",
    "participation/candidate",
    "participation/decision",
    "participation/claim",
    "participation/register",
    "participation/bind",
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


async def _observe(bot: BaseBot, event: BaseEvent, state) -> bool:
    proof = execution(state)
    if not (proof.accepted if proof else lifecycle.accepting_events):
        return False
    if not isinstance(bot, OneBotBot):
        return False
    try:
        group = group_identity(bot, event)
        if group is not None:
            await service.note_group(
                *group, fetch=lambda: bot.get_group_info(group_id=int(group[1]))
            )
            if not service.input_enabled(group[0], "group:" + group[1]):
                return False
        value = observation_event(bot, event, enabled=service.input_enabled)
    except (UnsupportedEvent, ValueError, KeyError, OverflowError, OSError):
        return False
    if proof:
        proof.captured_value(value)
    try:
        outcome = await service.capture_observation_result(
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
            value["message_context"],
        )
        if proof:
            await proof.step("capture", outcome)
    except BaseException:
        if proof:
            proof.failed = True
        raise
    return False  # Observing never claims reply ownership in NoneBot.


observer = on_message(rule=_observe, priority=1, block=False)


@observer.handle()
async def _observed():
    return None


async def _claim_observation_reply(bot: BaseBot, event: BaseEvent, state) -> bool:
    proof = execution(state)
    if not (proof.accepted if proof else lifecycle.accepting_events):
        return False
    if not isinstance(bot, OneBotBot):
        return False
    if not event_enabled(bot, event, service.input_enabled, reply=True):
        return False
    try:
        value = observation_event(bot, event, enabled=service.input_enabled)
    except (UnsupportedEvent, ValueError, KeyError, OverflowError, OSError):
        return False
    try:
        outcome = await service.observation_claim_result(
            value["account_id"], value["conversation"], value["author"], value["event_id"]
        )
        if proof:
            await proof.step("explicit_reply", outcome)
        return outcome == "claimed"
    except BaseException:
        if proof:
            proof.failed = True
        raise


# A live Platform lease and the current policy are required for ownership.
observation_reply = on_message(rule=_claim_observation_reply, priority=2, block=True)


@observation_reply.handle()
async def _claimed_observation_reply():
    return None


async def _scope(bot: BaseBot, event: BaseEvent, state) -> bool:
    proof = execution(state)
    if not (proof.accepted if proof else lifecycle.accepting_events):
        return False
    if not isinstance(bot, OneBotBot):
        return False
    try:
        value = text_event(bot, event, enabled=service.input_enabled)
    except (UnsupportedEvent, ValueError, KeyError):
        return False
    try:
        outcome = await service.capture_result(
            str(bot.self_id),
            value.conversation_id,
            value.account_id,
            value.event_id,
            value.sent_at,
            value.text,
            value.nickname,
            value.group_card,
            value.message_context,
        )
        if proof:
            await proof.step("scope", "claimed" if outcome == "captured" else outcome)
        return outcome == "captured"
    except BaseException:
        if proof:
            proof.failed = True
        raise


# Rule runs before blocking. Unknown authors and conversations pass to other plugins.
matcher = on_message(rule=_scope, priority=settings.tianshu_adapter_priority, block=True)


@matcher.handle()
async def _handled():
    return None


participation_host = install_participation_host(sys.modules[__name__])


def unregister():
    """Explicit host shutdown/unload hook; NoneBot itself has no hot-unload API."""
    if lifecycle.closed:
        return
    if lifecycle.pending:
        raise RuntimeError("Drain the Tianshu adapter before unregistering it")
    lifecycle.closing = True
    lifecycle.accepting_events = False
    for route in _routes:
        if route in app.routes:
            app.routes.remove(route)
    _routes.clear()
    participation_host.close()
    service.close()
    lifecycle.closed = True


async def shutdown():
    # The driver lifespan is the final owner. The unified host first drains
    # while callbacks are admitted; normal standalone shutdown also waits here.
    lifecycle.closing = True
    await lifecycle.drain(timeout=None)
    unregister()


driver.on_shutdown(shutdown)
