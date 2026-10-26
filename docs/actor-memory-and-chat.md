# 历史查询与 BOT 自身记忆

两路均按需读取，不把历史自动注入每轮聊天。`services.chat_history` 沿现有服务配置的 `url/token_env/ca_file` 连接 Chat；`chat_history_mappings` 是 `[{channel, mapping_id}]`，其中 channel 为原入站完整 channel，mapping_id 为 Memory 中登记的同一可信映射。模型只能给关键词、时间、真实作者及分页游标，不能替换 channel、actor、映射或服务 URL。

`chat_history_search` 默认一页 20 条，`chat_history_read` 按返回的 locator、正文 hash 和同范围邻读取文。正文先经 Chat 当前性检查，再逐条核全部 native aliases 的 Memory 已知删除状态；任一已知删除优先，全部不支持的项不送模型。登记映射下的未导入 Core 历史可以使用，但始终标为 archive evidence，不能宣称 Core-current 或完整实时历史。整次 owner 检查共用原 15 秒读取预算，分包不续时。后续实际模型与未来 BOT 引用会再次校验这些精确来源。当前 schema6 无法表达此证明时，省略整个 BOT 派生单元，用户原话照原链路保留。

`services.actor_memory_authority` 使用 Companion 独立 Platform 凭据，须具备 `actor.capabilities` 和 `actor.memory.authorize`。先经 `/internal/v1/actor-capabilities/operate` 的 `discover` 取得权威 role_version，再调用 actor/context authority。Companion 本地角色版和 Memory domain_version 不代替该版本。Memory 使用原 `services.memory`；Memory 回调 `/internal/v1/actor-self-memory/source`，只接受现有 `callers.memory` 身份，operation 为 `source_facts/source_check`。

仅实际 BOT `Reading.action(return_content=True)` 收到的原件区间形成 Comp-owned 阅读证据 leaf，与原阅读事实、进度及既有 outbox 同事务保存。leaf 保留真实 Knowledge 引用、覆盖区间与完整读取签名，每次 current 检查实际回读原 owner；它不接管整件 Knowledge 原件。完整区间引文能放入原 4000 字符摘要预算时保留，否则只保精确范围与引用，需按原 Reading 能力再查。不把浏览器中的人工阅读进度、原虚构 Life 事件或步骤完成改成 BOT 的真实经历，也不推导全件完成或理解。

outbox 沿现有自动 Memory dispatcher 公平推进。未知提交先查原回执，保持原正文、键、请求号及角色版；当前角色版可用于查旧回执，不能改旧命令重写。回执必须指向首次请求号。认识写入工具只接受实际自身检索返回的 episode_refs；明确 not_started 可在同角色版下查回执后重交原包，未知且无回执继续保未知。内部 actor_only 仅要求当前可读，外部群私上下文另核可披露权限。

运行装配未配置 `services.knowledge` 时，用原 `services.memory` 的 URL、凭据及 CA 建立独立 Knowledge 客户端并纳入原关闭生命周期。每个客户端仍按原普通 3/总 4 容量和 15 秒预算运行；按依赖边界分离是为避免外层 Memory 请求占用容量时，来源回调等待同一客户端自己的名额，不是增加模型槽。显式 Knowledge 配置与直接构造 Core 的旧注入行为保持原样。

`actor_memory_recall` 的每项实际取回完整 episode fact，保 provenance/outcome/content_reads；不能完整核验或放入同次读取预算的整项不进入模型。对话、内部计划和活动共用这份有来源投影，实际下一次模型前复核。认识属于解释，现实动作、观察、转述与虚构材料的原性质不变；自身经历不写到用户 person。现可发布 fictional diary 的读取链仅有原日记 ACL，不能承载每条真实材料的受众与遗忘复核，因此首版不向日记模型注入自身真实记忆。

后台动态生成须同时配置独立 `services.gateway_background` 与 `services.provider_selector_background`，两端固定真实身份 `companion_background`。只有后台 Gateway 时仅允许显式 `life_config_version` 的独立已发布写作版本，不借前台动态 grant 或默认版本；后台 selector 单独配置拒绝。未配置后台 Gateway 保原兼容行为，不冒称 Gateway 已使用 background。真实受保护 binding、服务凭据和发布配置由协调者管理。

本批合同使用协调者冻结 extensions-v7，原 rev6 用户记忆协议不变。没有新增数据库、worker、队列、模型步骤或总槽位；未配置的新服务继续明确不可用。测试中的 HTTP owner 为合成来源，实际跨产品联合、生产配置和外站行为另行验收。
