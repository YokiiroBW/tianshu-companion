"""Expose existing life and memory tools through the common registry."""

from ..contracts import Fault


class NativeSkill:
    def __init__(self, domain, operations, permission):
        self.domain, self.operations, self.permission = domain, operations, permission

    def validate_config(self, config):
        if any(config.values()):
            raise Fault("invalid_input")

    def availability(self, registry, actor, config):
        return dict(state="available", can_execute=True, reason_code=None)

    def tools(self, actions, turn, definition):
        if not actions.core._turn_allows(turn, self.permission):
            return []
        names = {self.operations[operation] for operation in definition["operations"]}
        return [tool for tool in actions.domain_tools(turn) if tool["function"]["name"] in names]

    def context(self, actions, turn):
        return None

    def instructions(self):
        return "Use actual domain receipts and current versions. Queued work is not completed work."

    def public_options(self, config):
        return {}

    async def execute(self, actions, turn_id, tool, *, model_slot_held=False):
        return await actions.execute_domain(turn_id, tool, model_slot_held=model_slot_held)


def definition(skill_id, title, description, domain, operations, handler_id=None):
    return dict(
        id=skill_id,
        version="1.0.0",
        title=title,
        description=description,
        domain=domain,
        operations=list(operations),
        handler_id=handler_id or skill_id,
    )


def install(registry):
    from .image import ImageSkill
    from .gscore import GSCoreSkill
    from ..role_actions import OPERATIONS

    life = {
        operation: "life_" + operation.replace(".", "_")
        for operation in OPERATIONS
        if operation != "image.request"
    }
    life.update({"life.read": "life_read", "life.send_content": "life_send_content"})
    registry.register(
        definition(
            "life.manage",
            "生活、阅读与写作",
            "管理角色生活事项，阅读真实原件和创作内容。",
            "life",
            life,
        ),
        NativeSkill("life", life, "dialogue"),
    )
    registry.register(
        definition(
            "memory.propose",
            "记忆提案",
            "依据本次真实用户输入提出记忆、纠正或遗忘。",
            "memory",
            ["memory.propose"],
        ),
        NativeSkill("memory", {"memory.propose": "memory_propose"}, "memory.write"),
    )
    registry.register(
        definition(
            "image.generate",
            "角色生图",
            "依据请求、真实穿搭或角色工作流生成着装、姿态和场景图。",
            "image",
            ["image.request"],
        ),
        ImageSkill(),
    )
    registry.register(
        definition(
            "game.guides",
            "游戏资料与攻略",
            "查询真实游戏攻略和已核实配队资料；依据实际片段综合建议。",
            "game",
            ["game.query"],
            "gscore.query",
        ),
        GSCoreSkill(),
    )
    for skill_id, title, description, domain, operations, handler in (
        (
            "subscription.manage",
            "订阅",
            "管理实际安装的订阅服务。",
            "subscription",
            ["subscription.manage"],
            "subscription.manage",
        ),
        (
            "media.video",
            "视频资料",
            "从真实媒体适配器检索或读取视频。",
            "media",
            ["media.video"],
            "media.video",
        ),
        (
            "media.image",
            "图片资料",
            "从真实媒体适配器检索或读取图片。",
            "media",
            ["media.image"],
            "media.image",
        ),
        (
            "media.comic",
            "漫画资料",
            "从真实媒体适配器检索或读取漫画。",
            "media",
            ["media.comic"],
            "media.comic",
        ),
        (
            "web.search",
            "网页搜索",
            "由实际安装的搜索适配器检索网页。",
            "web",
            ["web.search"],
            "web.search",
        ),
    ):
        registry.register(definition(skill_id, title, description, domain, operations, handler))
