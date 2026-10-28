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

    def activity_tools(self, actions, context, definition):
        activity, _ = actions.core.runtime_execution.actions.check(context)
        if self.domain != "life" or not actions.core.runtime_execution.actions.allows(
            context, self.permission
        ):
            return []
        # User-memory native handlers deliberately have no actor-activity surface.
        prefixes = (
            ("concern.", "activity.", "reading.", "content.")
            if activity.get("actor_only")
            else (
                "concern.",
                "activity.",
                "reading.",
                "content.",
                "writing.",
                "chapter.",
                "outfit.",
                "album.",
            )
        )
        return actions.life_tools(
            op
            for op in definition["operations"]
            if op in self.operations and op.startswith(prefixes)
        )

    async def activity_execute(self, actions, context, tool, *, model_slot_held=False):
        return await actions.core.runtime_execution.actions.execute(
            context, tool, model_slot_held=model_slot_held
        )


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
    from ..role_actions import OPERATIONS
    from .actor_memory import ActorMemorySkill
    from .chat import ChatHistorySkill
    from .gscore import GSCoreSkill
    from .image import ImageSkill
    from .platform import PlatformSkill

    life = {
        operation: "life_" + operation.replace(".", "_")
        for operation in OPERATIONS
        if operation != "image.request"
    }
    life.update({"life.read": "life_read", "life.send_content": "life_send_content"})
    registry.register(
        definition(
            "platform.actions",
            "角色平台能力",
            "发现和调用已绑定的真实平台能力，沿用原执行回执。",
            "life",
            PlatformSkill.operations,
        ),
        PlatformSkill(),
    )
    registry.register(
        definition(
            "actor-memory",
            "自身经历与认识",
            "按真实来源查阅自身经历，并保留可修正的理解和兴趣；不写到用户画像。",
            "memory",
            ["actor-memory.recall", "actor-memory.remember"],
        ),
        ActorMemorySkill(),
    )
    registry.register(
        definition(
            "chat.history",
            "聊天历史查证",
            "在当前真实会话中按需搜索与邻读已归档原话，保留说话人、时间和来源。",
            "chat",
            ["chat.search", "chat.read"],
        ),
        ChatHistorySkill(),
    )
    registry.register(
        definition(
            "memory.recall",
            "记忆检索",
            "检索当前可见的经历与画像，或查阅自然淡出的有源历史资料。",
            "memory",
            ["memory.recall"],
        ),
        NativeSkill("memory", {"memory.recall": "memory_recall"}, "memory.read"),
    )
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
