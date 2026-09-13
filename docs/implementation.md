# 陪伴核心实现与边界（TS-020 / TS-021）

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

本人普通记忆仍按时间/历史线索请求至多 2048 token / 8192 bytes，无历史线索时零预算。群中真正需要生成的完整输入另调用已发布的 `POST /internal/v1/memory/profiles/select`；每个目标最多 4096 bytes / 保守 tokens，并传整轮剩余额度的较小值。启动同时验证 `profile-memory/v1` 的 manifest（LF SHA256 `488d05438dd5b5abaa43a66a7eab0eb5cf615d5af01a964a7286cd23e68f7eb7`）、依赖和文件哈希；合同必须和文字包一起部署，未复制共享 schema。

模型附加语境共享 16 KiB 的 canonical JSON UTF-8 预算，tokens 同字节数保守估计，不能当模型实耗。计入普通记忆、依赖组、画像目标和完整画像单位、短期消息及确认回复、续接片段和显式已发送依赖；当前入站正文和人格仍受总原生上下文 512 KiB 上限。先保留普通记忆和必要续接/依赖，再按短期窗口取完整组，最后查询画像；画像装配超额整目标省略，短期整组省略，必要续接/依赖超额明确失败。既不裁掉条件/否定/时间/不确定性，也不拆因果语义组。`context_budget_used` 持久记录实际附加语境装配量，多目标不会重置预算，零预算探针不追加正文。

群目标仅当前已核验核心 conversation_id；人物目标仅来自当前输入和当前群获准短期窗口中已核验的 person_id。查询当前群 topic/style，以及这些人物 interest/style，按当前请求人来源与完整 scope 查询；不会用别人的账号冒充请求人。明确账号提及可优先排列窗口内已知作者，昵称、TG username 或未见过的账号不能签发/关联身份，也不会查询账号目录或其他群。目标不在窗口时不猜测；空画像不表示人物不存在。私聊保留本人普通记忆和原短期边界，不额外查询群画像。

核心短期窗口的本地配置默认 `max_turns=4 / max_bytes=8192 / max_age_seconds=1800`，且再受上述整轮剩余额度限制；允许设零关闭，配置硬上界分别为 16 轮、64 KiB、24 小时。选择最多检查前 `2*max_turns` 个会话轮次，通过会话/序号索引加 LIMIT 读取，不扫描账号历史；来源修订和 sent 回执也用索引定位。私聊仍仅相同 conversation/person/actor/audience、渠道/线程/作者账号、binding_version、memory scope_version 和本机 context_revision。群内允许相同角色、精确渠道/线程/会话/受众下其他作者的完整公开组，包括未呼叫角色而已 observed 的完整组；每组显式标记 person_id 与作者账号，不合并不同作者。其他作者的 binding_version 和文字版本使用其原来源独立复核，不比较不同人物版本的整数。近期相邻是首版相关性规则，没有额外语义规划模型。

每个选中单元保留完整 messages、真实作者、collection/turn/source/reply 引用，以及当前 phase/delivery_state；回复文本只读取有真实 sent 回执和渠道 message_id 的记录。T1 尚在生成时 T2 可拿已受理的 T1 输入，replies 为空；不等待 T1、不读取候选或发送中/unknown 文本。部分已发送时，只纳入确认 sent 的段落，并保留轮次的部分/不确定状态。按从近到远选取完整组，预算计算 canonical JSON 的 UTF-8 字节（包括消息和回复元数据）；整组放不下就停止窗口，不截断句子或继续挑零散旧片段。模型总上下文仍受既有 512 KiB 上限。未完整或续接中的组不进入普通窗口。

历史组从现有持久 bundle/receipt 重建；额外保存所用 turn/reply ID、字节数、失效版本、最早过期时刻和依赖复核元数据，不复制历史正文缓存。画像使用独立 `version_domain=profile-memory/v1`，不能复用文字探针，即使版本整数相同；画像探针保留请求人、目标、查询、类别、原来源和版本。生成前和每段发送前分别重新核验来源、账号绑定及各领域的 `known_scope_version + 零预算`。历史已发送回复还继承其画像/其他作者依赖的复核记录，避免旧画像经衍生回复重新注入；记录去重后最多 64 条，超出则整历史组省略，必要依赖超出则失败。不缓存服务响应；初次组验证失效/不可用则整组省略，当前来源或已使用的语境在发送前失效则停止待发部分。

当前来源编辑、撤回或权限失效命令在同一事务中提高整段逻辑会话的 context_revision，保守排除此前所有历史组及衍生回复；使用旧窗口生成但尚未发出的候选也停止发送。当前或其他作者文字版本、更正/遗忘、画像公开 epoch 变化都会排除依赖旧版的历史。显式方案依赖和续接片段复用也检查当前来源/范围版本，不能成为旁路。已发送账本保留事实，不伪称回滚；缺少来源标记的旧 observed 历史保守不恢复。

未完整 collector 自首次受理就持久保存不可上调的 `source_context_revision`，不依赖是否生成过回复；封存、observed、补句和续接不会把旧来源标记改成当前版本。挑选 continuation 候选时必须匹配当前会话修订；使用已关联链时逐片段再次核验，覆盖生成前和每段发送前。observed 轮次收到 permission_revoked 即使取消返回 too_late，旧片段也不能被新请求重新关联；已关联的在途链会停止待发部分。缺标记的旧数据保守排除或使既有关联链失败，不能补写当前版本；未失效的正常完整续接及原容量/字节预算继续适用。

生成前和每段发送前重新解析当前来源、账号映射，使用 `known_scope_version + 零预算` 验证记忆授权/版本，并检查本机输入和续接来源的当前修订。不可用/过期/变更即关闭待发部分；已发部分保留。检查与远端权限变更之间不存在跨服务原子性承诺。没有私密离线缓存。取消接口核对明确 turn_id、expected_version 和来源；普通“等等”与引用内容不会调用取消。

## 恢复、发送与 outbox

真实发送前保存稳定 reply_id、顺序、请求和 attempted_at。异常无法证明未执行则 unknown；适配器也独立持久 attempt，重复请求只返回原事实。核心默认不重试失败段，retry_safe 仍如实记录。unknown 到期为 closed_unknown 并释放槽，后轮可继续；这保证本地提交顺序，不保证远端实际到达顺序。迟到的核实回执只更新 reply 和 `conversation.projection_changed/delivery_changed` 本地投影，不再提交记忆、不重新开槽。为保持 v1 closed_unknown 的 wire 不变量，轮次历史仍显示 unknown，已查证的新事实在 reply 记录中。

重启保留 collector UTC deadline；已准备待发内容继续核验后发送。中断的准备/模型调用关闭失败，不自动重跑可能已经计费的模型；中断 sending 变 unknown，先核对。已确认 sent 永不重发。

有权威 scope_version 的终态在事务中生成合法 `conversation.turn_committed`；outbox 至少一次投递，消费回执保留 accepted/duplicate/stale_source/scope_changed 与 confirmed_memory_written=false。来源仍 pending，未接 Chat Audit 不虚报已永久归档。早期故障缺 scope_version 时保留本地 blocked_scope，turn 仍正常 failed/released；不投递伪造事件。`repair_blocked_scope` 是部署内部修复端口，需要当前输入修订/取消/遗忘/范围核验器，默认不安装；不依赖过期原用户引用，也不自创公开来源查询路由。

默认 reconcile 不可查；真实来源签发、撤销传播、发送权限核验、回执查询与 blocked_scope 修复核验需要协调服务实现。网页 snapshot/SSE 尚未实现，本地 delivery_changed 投影不能当完整 I17 订阅服务。当前没有短期 inbox 清理或长期归档任务，数据库应位于隔离路径；在长期/高流量使用前必须补受控保留与索引策略。

## 验收层级

测试替身全部位于 tests；生产应用没有固定成功服务。测试覆盖真实本地数据库/事务/HTTP 路由/客户端序列化，不能证明真实外部授权和送达。`tests/trace_scenario.py` 记录实际模块重开后的两轮、unknown、outbox 状态到 JSON；各检查点注明合成依赖。真实 memory→gateway→QQ/TG、L0/L1、渠道 actual arrival、TLS 部署和生产 PostgreSQL 均未运行。

TS-021 定向命令：`python -m pytest tests/test_profile_context.py tests/test_short_context.py tests/test_continuation_revision.py -q`。可选组件联合用例：显式设置 `TIANSHU_MEMORY_REPO` 为 Memory 仓库路径后运行 `python -m pytest tests/test_profile_joint.py -q`；只读 `git archive 69b29f3 src`，在临时目录加载固定版本实际生产者，消费者通过 httpx ASGITransport 调用真实 Memory HTTP 应用。Memory 数据库、发布批准、来源账本全部为临时合成夹具，Core issuer/model/sender 也是替身；未设置变量时明确跳过。该用例不是网络/TLS 验收，不连接真实来源、账号或现存数据库。Memory 真实 SourceAuthority 仍未接，缺失时继续 503，不能宣称完整 L0。启动、安装、静态和完整套件沿用根 AGENTS 的实际命令。
