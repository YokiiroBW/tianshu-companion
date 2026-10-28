# 自主目标与真实阅读

TS-384 基于 R14 Companion `225f62a9c201acfc283bc029562f118f16d5e524`。没有第二个日程、记忆或能力账本。`life_goals.AutonomousGoals` 在原 Life 公平后台阶段中选择具体目标，目标保存在 `life_concerns`，执行与续点归 `life_activities`；metadata 仅保存原模型/动作的持久请求与回执。

固定作息继续作为每日意图与时间边界。实际工具模型可用时，不再把每个作息名称自动变成执行中的活动，也不为这些阶段编造已经发生的经历。自主选择可新建、继续、延后、放弃或休息；手动占用、既有显式活动、近期显式提醒和到期事项优先。默认睡眠、早餐、午休和晚餐阶段不启动探索；自定义作息继续由其原配置提供边界。日常关注只作为当日临时材料，不自动成为长期偏好。

目标选择默认每 30 分钟重新考虑；休息/延期使用本次明确的下一时间点。实际目标完成后，下一次原后台阶段可立即重新选择，检索词包括刚完成活动的标题。停用/重启保留实际 checkpoint，不填补离线工作；已提交或未知动作的原操作归属保留，重新规划不能绕过它。活动仍使用原每步八轮预算和原后台模型槽。

`ActivityActions` 提供独立 actor activity context（actor、activity、operation、runtime epoch），不创建用户 turn、person 或虚假的来源。后台与对话共用 `SkillRegistry`、同一原生工具 schema renderer 及安装的 handler。自身召回/派生复用原 ActorMemory 可靠命令核心；活动使用 actor_only 受众与实际工具返回的 episode refs，不调用用户记忆提案、摘要、画像或召回路径。一次实际阅读先进入原 actor outbox，再能通过召回取得 Memory 已受理的 episode 依据；未知命令先查询原回执，不换键重发。

`platform.actions` 通过既有 `services.actor_memory_authority` 的 Companion 身份连接原 Platform `actor-capabilities` 端口；没有新 token、账号选择或服务配置字段。实际 `discover` 返回绑定与输入 schema；工具模型再调用 `platform_invoke`、`platform_get`、`platform_reconcile` 或 `platform_cancel`。调用沿原稳定意图键与 invocation，queued/running 不表示成功。未配置权威服务、未绑定、停用或 handler 缺失时如实不可用。新 actor_only 探索只使用 owner 声明的 read 能力；账号对外写入需要后续明确披露上下文。内容候选的 `adapter_required` 保留真实缺口，不将目录摘要当成原文。

读取仍通过 Knowledge 的原件 acquire/read。实际文本、图片、音轨和抽样帧沿原 multimodal 消息交给模型；覆盖和缺口由 owner 返回。持久校验同时保存原请求 range 和实际 coverage：再次校验必须重放原请求，避免预算截断后的实际覆盖范围改变表示字节而误判版本变化。该内部字段不改变公共合同。到达视频时长不代表所有模态完整观看或已经理解。

自身私有材料推导出的目标、活动和原件保留 actor_only 标记；外部具体会话不能读取它们。持久公共 activity 只显示通用状态，私有边界事件不进入公开虚构 timeline、日记或主动分享。每轮继续前复核原件、actor-memory 材料、目标来源和角色 epoch；规划后来源撤回、角色停用或手动事项变化使旧输出失效。

验证入口：`tests/test_autonomous_life.py`。它使用合成 owner HTTP 协议和录制模型，从零用户消息推进目标→实际发现回执→原件→真实区间→actor episode→认识→即时后继目标，并覆盖重启、unknown、撤回、角色变化、失败阅读、跨日延期与原请求范围核验。它不是实际外站或真实模型验收；单容器安装 wheel、真实 Platform 插件和 Memory owner 联合由根 TS-387 验收。
