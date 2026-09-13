# TS-020 实现与边界

依据主仓库 `docs/development/workstreams/companion-memory.md` C1/C2、V2 第 3/5 节、消息防抖专题及 `contracts/text-dialogue/v1` 1.0.0。manifest 的 LF SHA256 固定为 `81e6cc4ddef7c6f82e055d4cb04b090db036dd5c52763473ce697aa02db478a1`；启动验证所有发布文件，未复制共享 schema。

## 技术选择

Python 3.12 + FastAPI/httpx/jsonschema；标准库 SQLite/WAL、FULL 同步与独占所有者文件锁。选择 SQLite 用于当前单节点隔离验收，避免引入未验证的数据库、队列和分布式租约。数据库表分别持有会话、collector、inbox、turn、reply、outbox 和幂等结果；可检索列之外的版本化文档用 JSON 保存。事务不跨网络等待。原子收件/预约、封存序列、终态/outbox 的行为通过真实 SQLite 文件重开验证。未实现或验证 PostgreSQL/多进程迁移，不把 SQLite 结果当生产 PG 证据；表结构升级要新增明确迁移，未知版本拒绝打开。

旧源码只读审查了 together-companion 的会话归档测试与 reality-companion 的布尔发送入口。它们依赖 AstrBot 房间/插件状态且缺逐段 unknown 回执，不适合本版恢复语义；没有复制旧权限、世界、日记和设备代码。

## 状态与容量

| 阶段 | 行为 | 会话槽 |
| --- | --- | --- |
| collecting（内部 collector） | 先可靠收件与预约，严格 accepted_at < deadline 才追加；旧 timer 核对 ID/revision/deadline | 不占活跃槽，占预约队列 |
| queued | 已封存结构化组；封存顺序按 deadline、最早受理序号 | 不占活跃槽，占排队容量 |
| preparing / waiting_dependency | 固定人格与网关配置版本、按需选取证据；实际依赖等已发送稳定结果 | 占槽 |
| generating | 全局模型信号量独立；普通新消息不取消该轮 | 占槽和模型槽 |
| ready_to_send / sending | 模型槽已释放；全会话跨角色按轮次、段落提交 | 占槽 |
| reconciling | 不明发送等待可验证回执，禁重放 | 占槽 |
| sent / failed / cancelled / observed / closed_unknown | 封账与 outbox 原子保存 | 释放槽 |

每个新 collector 在首条受理前检查 `queued + collectors < max_queued_turns`，因此满队列不会丢已受理输入。数量/字节超限的新增消息返回 429，已有组以 `resource_limit/possibly_incomplete` 封存；该片段只观察不调用生成/工具。下一完整静默组关联续接链并保留之前片段，已关联链不再挂到无关后续组；最大续接深度 32、总原生上下文 512 KiB，超限明确失败，原文仍在 inbox。W=0 立即封存，max_wait 默认关闭，配置上限触发的片段同样不执行。单轮最多 16 段，每段最多 32 KiB；一旦有 unknown，未发送的后段不继续提交。

所有人物、角色共享 channel_key 对应的两个活跃槽。同组保留逐消息角色目标；首条已核验 scope 的角色承担本轮回复，其他目标保留在结构化消息中，不冒用另一个角色来源。这是首版确定性仲裁，尚无多角色分别生成。群内未明确面向该角色时只观察。引用和插话保留独立 source；不因为引用存在就读取私密原文。

依赖识别目前覆盖“按你…方案/刚才…方案/照你…说/your plan/proposal”这类明确前轮方案短语，只等待该会话前一轮实际已发送结果与固定 result_version；没有通用语义规划器。其他输入保留原文一次主生成，不假装能可靠识别全部自然语言依赖。模型不接收工具执行权限。

## 身份、记忆和版本

固定认证服务→issuer 配置解析来源；核对 issuer、受众服务、来源引用、有效期、已验证账号、已验证渠道及本机 binding。不能用 payload 的 person_id、actor_id、scope 扩权。入站首次可无逻辑 ID；conversation 由核心渠道映射签发，person 由 memory resolve/register 签发。未配置来源/记忆服务拒绝受理，已有收件进入后续依赖故障时封账释放槽。

首次渠道映射通过既有 ingest_response 返回。ASGI 发送完受理响应后才释放该输入的处理门控，未完成响应即崩溃时等接入方重投拿回同一受理回执。薄桥验证可信响应的 collection_key 并持久保存 channel_key→conversation_id；来源 issuer 从 `Bridge.channel_mapping` 读取该映射，不能从聊天正文或用户传入的 ID 学习映射。即使 W=0，核心也可能早于接入方保存映射：仅首次映射尚未完成阶段的 dependency_unavailable/503 最多等待 5 秒，且受原命令 deadline 上界约束，每 100ms 重试，可取消；明确鉴权拒绝、scope/version/idempotency 冲突不重试。成功选择后不再应用该重试路径。`tests/test_bootstrap.py` 用真实 ASGI 客户端、SQLite 薄桥和严格测试 issuer/select 验证首次 null→回执保存→非 null→生成的顺序；真实 issuer 服务仍待 L0。

问候选择预算为零，时间/历史线索按需请求至多 2048 token / 8192 bytes。只装配完整已选语义组及必要来源，不用整份账号历史。首版每轮只有一次有预算的 selection，不存在每次补查重置预算的调用链；装配和 budget_used 持久保存，零预算核验不追加已选内容。若将来增加补查，必须扣除累计已装配内容并去重后传剩余额度，不能复用初始预算。

普通相邻聊天另有核心拥有的短期窗口，与长期 memory/select 预算分开。`short_context` 本地配置默认 `max_turns=4 / max_bytes=8192 / max_age_seconds=1800`；允许设零关闭，硬上界分别为 16 轮、64 KiB、24 小时。选择最多检查前 `2*max_turns` 个会话轮次，通过会话/序号索引加 LIMIT 读取，不扫描账号历史；来源修订和 sent 回执也用索引定位。仅相同 conversation/person/actor/audience、渠道/线程/作者账号、binding_version、当前 memory scope_version 和本机 context_revision 的组有资格进入。近期相邻是首版相关性规则，没有额外语义规划模型。

每个选中单元保留完整 messages、真实作者、collection/turn/source/reply 引用，以及当前 phase/delivery_state；回复文本只读取有真实 sent 回执和渠道 message_id 的记录。T1 尚在生成时 T2 可拿已受理的 T1 输入，replies 为空；不等待 T1、不读取候选或发送中/unknown 文本。部分已发送时，只纳入确认 sent 的段落，并保留轮次的部分/不确定状态。按从近到远选取完整组，预算计算 canonical JSON 的 UTF-8 字节（包括消息和回复元数据）；整组放不下就停止窗口，不截断句子或继续挑零散旧片段。模型总上下文仍受既有 512 KiB 上限。未完整或续接中的组不进入普通窗口。

历史组从现有持久 bundle/receipt 重建；本轮仅额外保存所用 turn/reply ID、字节数、失效版本和最早过期时刻，不复制一份历史正文缓存。每次生成前先完成现有来源/记忆范围核验，再装配窗口，发送前复核有效性。当前来源编辑、撤回或权限失效命令在同一事务中提高整段逻辑会话的 context_revision，保守排除此前所有历史组及它们的衍生回复；使用旧窗口生成但尚未发出的候选也停止发送。这样 T1 被撤回后，不会通过 T2 曾复述的答案重新带回 T1。当前 memory scope_version 变化（包括更正/遗忘）会排除旧版历史；记忆不可用时不会使用短期窗口绕过授权。显式方案依赖和续接片段复用也检查当前来源/范围版本，不能成为旁路。已发送账本保留事实，不伪称回滚；旧版本历史记录缺少新 context_revision 时保守不恢复。

未完整 collector 自首次受理就持久保存不可上调的 `source_context_revision`，不依赖是否生成过回复；封存、observed、补句和续接不会把旧来源标记改成当前版本。挑选 continuation 候选时必须匹配当前会话修订；使用已关联链时逐片段再次核验，覆盖生成前和每段发送前。observed 轮次收到 permission_revoked 即使取消返回 too_late，旧片段也不能被新请求重新关联；已关联的在途链会停止待发部分。缺标记的旧数据保守排除或使既有关联链失败，不能补写当前版本；未失效的正常完整续接及原容量/字节预算继续适用。

生成前和每段发送前重新解析当前来源、账号映射，使用 `known_scope_version + 零预算` 验证记忆授权/版本，并检查本机输入和续接来源的当前修订。不可用/过期/变更即关闭待发部分；已发部分保留。检查与远端权限变更之间不存在跨服务原子性承诺。没有私密离线缓存。取消接口核对明确 turn_id、expected_version 和来源；普通“等等”与引用内容不会调用取消。

## 恢复、发送与 outbox

真实发送前保存稳定 reply_id、顺序、请求和 attempted_at。异常无法证明未执行则 unknown；适配器也独立持久 attempt，重复请求只返回原事实。核心默认不重试失败段，retry_safe 仍如实记录。unknown 到期为 closed_unknown 并释放槽，后轮可继续；这保证本地提交顺序，不保证远端实际到达顺序。迟到的核实回执只更新 reply 和 `conversation.projection_changed/delivery_changed` 本地投影，不再提交记忆、不重新开槽。为保持 v1 closed_unknown 的 wire 不变量，轮次历史仍显示 unknown，已查证的新事实在 reply 记录中。

重启保留 collector UTC deadline；已准备待发内容继续核验后发送。中断的准备/模型调用关闭失败，不自动重跑可能已经计费的模型；中断 sending 变 unknown，先核对。已确认 sent 永不重发。

有权威 scope_version 的终态在事务中生成合法 `conversation.turn_committed`；outbox 至少一次投递，消费回执保留 accepted/duplicate/stale_source/scope_changed 与 confirmed_memory_written=false。来源仍 pending，未接 Chat Audit 不虚报已永久归档。早期故障缺 scope_version 时保留本地 blocked_scope，turn 仍正常 failed/released；不投递伪造事件。`repair_blocked_scope` 是部署内部修复端口，需要当前输入修订/取消/遗忘/范围核验器，默认不安装；不依赖过期原用户引用，也不自创公开来源查询路由。

默认 reconcile 不可查；真实来源签发、撤销传播、发送权限核验、回执查询与 blocked_scope 修复核验需要协调服务实现。网页 snapshot/SSE 尚未实现，本地 delivery_changed 投影不能当完整 I17 订阅服务。当前没有短期 inbox 清理或长期归档任务，数据库应位于隔离路径；在长期/高流量使用前必须补受控保留与索引策略。

## 验收层级

测试替身全部位于 tests；生产应用没有固定成功服务。测试覆盖真实本地数据库/事务/HTTP 路由/客户端序列化，不能证明真实外部授权和送达。`tests/trace_scenario.py` 记录实际模块重开后的两轮、unknown、outbox 状态到 JSON；各检查点注明合成依赖。真实 memory→gateway→QQ/TG、L0/L1、渠道 actual arrival、TLS 部署和生产 PostgreSQL 均未运行。
