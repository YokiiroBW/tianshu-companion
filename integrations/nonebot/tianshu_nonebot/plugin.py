"""Load with nonebot.load_plugin('tianshu_nonebot.plugin') after OneBot v11 registration."""

import sys

if "tianshu_nonebot.adapter_plugin" in sys.modules:
    raise RuntimeError("Tianshu pull and adapter transports cannot share a NoneBot host")

from pathlib import Path

from nonebot import get_bots, get_driver, get_plugin_config, on_message
from nonebot.adapters import Bot as BaseBot
from nonebot.adapters import Event as BaseEvent
from nonebot.adapters.onebot.v11 import Bot as OneBotBot
from nonebot.plugin import PluginMetadata
from pydantic import BaseModel, SecretStr

from .runtime import BotRuntime


class Config(BaseModel):
    tianshu_nonebot_connection_id: str
    tianshu_nonebot_platform_id: str
    tianshu_nonebot_bot_self_id: str
    tianshu_nonebot_allowed_conversations: tuple[str, ...]
    tianshu_nonebot_platform_url: str
    tianshu_nonebot_token: SecretStr
    tianshu_nonebot_journal_path: Path
    tianshu_nonebot_ca_file: Path | None = None
    tianshu_nonebot_priority: int = 5
    tianshu_nonebot_group_requires_mention: bool = True


__plugin_meta__ = PluginMetadata(
    name="天枢陪伴 OneBot v11",
    description="将明确授权的 OneBot v11 文本会话接入天枢平台",
    usage="由管理员预先创建连接和会话白名单；本插件不提供聊天指令配置入口。",
    type="application",
    config=Config,
    supported_adapters={"~nonebot.adapters.onebot.v11"},
)

settings = get_plugin_config(Config)


def _connected_bot(self_id):
    bot = get_bots().get(self_id)
    return bot if isinstance(bot, OneBotBot) else None


runtime = BotRuntime(
    connection_id=settings.tianshu_nonebot_connection_id,
    platform_id=settings.tianshu_nonebot_platform_id,
    bot_self_id=settings.tianshu_nonebot_bot_self_id,
    allowed_conversations=settings.tianshu_nonebot_allowed_conversations,
    journal_path=settings.tianshu_nonebot_journal_path,
    platform_url=settings.tianshu_nonebot_platform_url,
    token=settings.tianshu_nonebot_token.get_secret_value(),
    get_bot=_connected_bot,
    group_requires_mention=settings.tianshu_nonebot_group_requires_mention,
    ca_file=settings.tianshu_nonebot_ca_file,
)


async def _scope(bot: BaseBot, event: BaseEvent) -> bool:
    return isinstance(bot, OneBotBot) and runtime.eligible(bot, event)


matcher = on_message(rule=_scope, priority=settings.tianshu_nonebot_priority, block=True)


@matcher.handle()
async def _handle(bot: OneBotBot, event: BaseEvent):
    # No matcher.send/finish: Platform/Core is the sole reply authority.
    await runtime.capture(bot, event)


driver = get_driver()
driver.on_startup(runtime.start)
driver.on_shutdown(runtime.close)
