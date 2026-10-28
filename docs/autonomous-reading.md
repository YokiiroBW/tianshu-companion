# 自主目标与真实阅读

TS-384 基于 R14 Companion `225f62a9c201acfc283bc029562f118f16d5e524`。没有第二个日程、记忆或能力账本。`life_goals.AutonomousGoals` 在原 Life 公平后台阶段中选择具体目标，目标保存在 `life_concerns`，执行与续点归 `life_activities`；metadata 仅保存原模型/动作的持久请求与回执。

固定作息继续作为每日意图与时间边界。实际工具模型可用时，不再把每个作息名称自动变成执行中的活动，也不为这些阶段编造已经发生的经历。自主选择可新建、继续、延后、放弃或休息；手动占用、既有显式活动、近期显式提醒和到期事项优先。默认睡眠、早餐、午休和晚餐阶段不启动探索；自定义作息继续由其原配置提供边界。日常关注只作为当日临时材料，不自动成为长期偏好。

目标选择默认每 30 分钟重新考虑；休息/延期使用本次明确的下一时间点。实际目标完成后，下一次原后台阶段可立即重新选择，检索词包括刚完成活动的标题。停用/重启保留实际 checkpoint，不填补离线工作；已提交或未知动作的原操作归属保留，重新规划不能绕过它。活动仍使用原每步八轮预算和原后台模型槽。

`ActivityActions` 提供独立 actor activity context（actor、activity、operation、runtime epoch），不创建用户 turn、person 或虚假的来源。后台与对话共用 `SkillRegistry`、同一原生工具 schema renderer 及安装的 handler。自身召回/派生复用原 ActorMemory 可靠命令核心；活动使用 actor_only 受众与实际工具返回的 episode refs，不调用用户记忆提案、摘要、画像或召回路径。一次实际阅读先进入原 actor outbox，再能通过召回取得 Memory 已受理的 episode 依据；未知命令先查询原回执，不换键重发。

`platform.actions` 通过既有 `services.actor_memory_authority` 的 Companion 身份连接原 Platform `actor-capabilities` 端口；没有新 token、账号选择或服务配置字段。实际 `discover` 返回绑定与输入 schema；工具模型再调用 `platform_invoke`、`platform_get`、`platform_reconcile` 或 `platform_cancel`。调用沿原稳定意图键与 invocation，queued/running 不表示成功。未配置权威服务、未绑定、停用或 handler 缺失时如实不可用。新 actor_only 探索只使用 owner 声明的 read 能力；账号对外写入需要后续明确披露上下文。内容候选的 `adapter_required` 保留真实缺口，不将目录摘要当成原文。

读取仍通过 Knowledge 的原件 acquire/read。实际文本、图片、音轨和抽样帧沿原 multimodal 消息交给模型；覆盖和缺口由 owner 返回。持久校验同时保存原请求 range 和实际 coverage：再次校验必须重放原请求，避免预算截断后的实际覆盖范围改变表示字节而误判版本变化。该内部字段不改变公共合同。到达视频时长不代表所有模态完整观看或已经理解。

自身私有材料推导出的目标、活动和原件保留 actor_only 标记；外部具体会话不能读取它们。持久公共 activity 只显示通用状态，私有边界事件不进入公开虚构 timeline、日记或主动分享。每轮继续前复核原件、actor-memory 材料、目标来源和角色 epoch；规划后来源撤回、角色停用或手动事项变化使旧输出失效。

验证入口：`tests/test_autonomous_life.py`。它使用合成 owner HTTP 协议和录制模型，从零用户消息推进目标→实际发现回执→原件→真实区间→actor episode→认识→即时后继目标，并覆盖重启、unknown、撤回、角色变化、失败阅读、跨日延期与原请求范围核验。它不是实际外站或真实模型验收；单容器安装 wheel、真实 Platform 插件和 Memory owner 联合由根 TS-387 验收。

TS-386 / AL12 在原 `Observations.flush` 收到真正归档回执后组合交接：原 `MemoryLearning.observation_archived` 保持原算法，独立 `ActorObservation` 只安排角色自身的旁听筛选。不依赖自动用户记忆提取；observe/reply/active 均可保留有趣的旁听，回复许可与保留决定没有绑定。每批同群消息最多 12 条 / 32 KiB，安静 5 秒后处理、持续输入最长合并 30 秒；语义筛选最多选择 6 个原件。采用原自动 dispatcher、公平轮转、前台优先和后台模型槽，复用 participation 功能路由，不建立另一套调度器。

归档描述符保留原 `source_ref/source_digest/scope_version/archive_epoch`；Memory `observation-source` 窄只读口按冻结 `source_facts/source_check` 返回真实 observer / observation 事实和当前来源版本。模型前后复核完整 generation / sequence / access 版本及当前角色、群代际和本地 epoch。未知模型结果不重跑；已选原件进入既有 actor outbox，`append_episode` 明确 observer，未知写先核原回执。使用实际受理 episode 的精确 revision 形成可修正的 reflection，再让下一轮目标实际召回该材料。参与者阅读的原事实与范围校验保持独立。

每群任务保存原精确 policy_version，A→B→A 和停用→启用都不能复活旧包；别群策略更新不使当前群重发。已完成经历仍按原 Memory source / archive / binding / forget 检查，不因暂停新自动工作删除历史。撤回材料不能用于后续目标或活动。旁听派生目标与其公开探索主题保留自身记忆依据；公开 UI、虚构日记和主动事件继续受 actor_only 隔离。外部发现存在标准 query 字段时只投影已由语义筛选给出的公共主题，不组装原群原话、native 元数据或内部 scope；其他能力继续服从实际 owner schema，没有字符串关键词审批门槛。

旁听验证入口 `tests/test_actor_observation.py`。真实群策略代际回归额外需要实际 Platform `services.platform.group_policy` 包；普通仅安装 Companion 的测试环境会明确跳过这一联合节点，根单容器联合环境应启用它。Memory 生产者需同时使用 TS-388 `9453e5e3fd43c80a41f9bd4601f0181e2e7c6db5` 或后继；未发布该窄口时如实保留依赖不可用，不冒称已经记住。
