# 功能指令分流与唯一回复责任（TS-024）

本模块属于 Core，与生活/日记/图像/写作/主动联系共用同一个 SQLite 单进程所有者。
目标只有两句话：**登记的完整功能指令走快路，不再排聊天防抖、不占两个聊天轮次名额；
含糊或未登记的自然语言继续走原有陪伴链**。任何一条消息**只有一个回复方**。

`core.direct` 是可信同产品 Python 端口；内部 HTTP 入口
`POST /internal/v1/conversation/direct-command`（命令快路）与
`POST /internal/v1/capability/execute`（Core 显式能力调用）是同一个执行引擎的两个入口。
宿主必须先鉴权再调用；不能把聊天文本、模型输出或 persona 直接解包成命令参数或权限。

## 分流判据

一条文本只有在**同时**满足下面全部条件时才被当成功能指令：

1. 去掉首尾空白后的**第一个 token** 与某个已登记命令名（或别名）**完全**相等；
   `/查询x`、`查询 天气`、`@bot /查询 天气`、`帮我查询一下天气` 都不是命令。
2. 该登记覆盖这条消息的平台（`platforms`）与受众（`audiences`）。
3. 登记存在且未被撤销（`state="registered"`）。
4. 登记的 `reply_to` 是 `"bridge"`。`reply_to="core"` 的登记**只能**由 Core 显式能力调用
   触发；它的裸命令文本继续走陪伴链——功能指令不得因为聊天模型不可达而失败。

匹配结果 `match()` 始终给出 `matched`、`reason`、`command`、`parameters`、
`parameters_valid`、`parameter_error`；`reason` 取值含义：

| reason | 含义 |
| --- | --- |
| `empty` | 空文本 |
| `not_a_command` | 首 token 不是任何已登记命令名 |
| `not_this_platform` / `not_this_audience` | 命令存在但不覆盖该平台/受众 |
| `reply_owner_core` | 命令存在但回复方是 Core，快路不认领 |
| `registration_missing` | 命令已撤销 |

**参数形态由登记决定，永不猜测**：`token` / `integer` / `rest` 三类字段，
必填缺失即 `missing_argument:<name>`，多余参数即 `unexpected_argument`，
类型不符即 `invalid_argument:<name>`，`choices` 之外即 `invalid_choice:<name>`。
参数不合法时**不执行**、**不调用模型**，直接用登记推导出的 `usage` 行回复：

```text
/查询 <subject> [<detail>]
```

## 责任方与「禁止双答」

`Direct` 的每条请求都带一个明确的回复责任方：

- `reply_to="bridge"`：插件（功能产品）负责回复。Core 只把结果与投递状态记进请求视图；
  **Core 返回给调用方的 `result` 与 `reply` 都是 `null`**，投递走既有出站管线。
- `reply_to="core"`：Core（陪伴轮次）负责回复。请求停在 `reply_state="awaiting_core"`，
  适配器**不得**同时给出回包；一旦给了，请求收口为 `unknown`（`unresolved=true`）而不是
  在两条线上各回一句。

入口与能力调用共用一次执行：命令快路与 `core.capability(...)` 只要带**同一条消息版本**
（`message_key`）就折叠到同一个 `direct_requests` 行，`entries` 记录
`["command"]` / `["capability"]`，只跑一次适配器、只投递一次。
同一 `message_key` 的重复 SDK 事件返回同一请求（幂等）；同键**不同参数**报
`idempotency_conflict`；同一消息 id 的更高 `revision` 是新版本，会**取代**旧版本，
旧版本若尚未派发则直接 `superseded`，绝不出现两条回复。

## 单一执行引擎

```python
core.direct.register_command(
    command_id="companion.synthetic.query", version=1, name="/查询", aliases=["/query"],
    platforms=["qq"], audiences=["self_private", "group"],
    parameters={"fields": [{"name": "subject", "type": "token", "required": True},
                           {"name": "detail", "type": "rest", "required": False}]},
    reply_to="bridge", timeout=20,
)
core.direct.revoke_command("companion.synthetic.query", reason="operator_revoked", expected=1)
```

登记内容按 `(command_id, version)` **不可变**：同版本异内容报
`Command version is immutable`；改内容必须换版本并用 `expected=<当前版本>` 做乐观并发；
同一平台/受众范围里同名命令**只能有一个 owner**（不同 `command_id` 抢同一个名字直接拒绝），
同 `command_id` 的新版本会取代旧版本。

`register_command` 是唯一的命令来源。桥接侧不复制命令表：Core 通过
`core.commands("nonebot")`（HTTP `POST /internal/v1/direct/commands`）把
`registry` 指纹与命令表交给已鉴权的 `nonebot`，桥接用它构造匹配表
（`tianshu_nonebot.routing.registry_matcher`），两边不可能各自以为拥有同一个名字。

`Direct` 复用主动联系模块的既有模式：显式登记、持久记录、闸门决策、派发 attempt、
`settle` / `retry` / `recover` / `tick` / `work`，以及 `metadata` 表里的**有界轮转扫描游标**。

## 独立快速路

命令快路**不经过**聊天防抖、不创建 `collections`、不占用两个聊天轮次名额、不调用聊天模型：

- `Core.direct_command` 只校验入站合同、会话存在与**本地** actor 绑定，然后 `match()`；
- 执行在 `direct_worker`（应用里 0.5 秒后台任务）里进行，慢插件等待期间普通聊天照常
  完成派发与回复；
- 两个聊天轮次都在生成时，命令仍然照常执行并送达。

## 授权来自本地事实，persona 不是权限

派发前 `_claim` 在同一事务内重做一次本地授权复核（`_proactive_guard` 同源做法）：

- 会话存在且未隔离（`blocked_scope`）；
- 账号在**该会话已受理的入站**里存在 actor/person 绑定——没有绑定即 `Fault("forbidden")`，
  绝不从 payload 推断身份；
- 命令登记覆盖该平台/受众，且未被撤销；
- 请求 actor 在该登记的 `actor_allowlist` 内（若登记了）；
- 角色存在（`roles`）且绑定存在（`bindings`）。

复核失败时请求收口为 `cancelled` 并记
`blocked_reason="authorization_revoked:<原因>"`，**不调用插件、不投递**。

`core.capability` 的字段集是封闭的：出现 `persona`、`permission`、`affection`、`consent`
等未知字段直接 `invalid_input`。模型文本、好感度、前端字段从来不是权限。

## 慢任务与在途纪律

适配器 `execute(request)` 可以立即返回结果，也可以返回 `state="accepted"` 表示长任务：
请求进入 `awaiting_result`，`settle(attempt_id, result)` 是唯一的收口入口。
调用形状（端口契约）：

```python
{"schema_version": 1, "request_id": ..., "attempt_id": ..., "command_id": ...,
 "command_version": ..., "entry": "command", "actor_id": ..., "person_id": ...,
 "audience": ..., "conversation_id": ..., "channel": {...}, "parameters": {...},
 "parameter_error": None, "reply_to": ..., "usage": ..., "submitted_at": ...}
```

- **不注入伪造用户消息**：端口上没有消息正文、没有 `message_key`、也没有回到 `Core.ingest`
  的入口，插件无法用「假装收到一条用户消息」的方式触发别的插件。
- 每次派发恰好一个 attempt，id 为 `direct-attempt:<digest([request_id, attempt_no])>`；
  晚回包只落**自己的** attempt（记 `stale=true`），不改写请求或更新的 attempt。
- 超时、异常、重启都记 `unknown`（`unresolved=true`），**绝不自动重发**。
- `retry(request_id, reason=..., redeliver=...)` 是唯一的显式重试入口；
  `redeliver=True` 只在最近一次投递 `failed` 且回执 `retry_safe is True`（桥接已证实
  未执行）时允许，`reply_id` 不变，桥接仍会去重。`unknown` 只能靠 `reconcile()` 只读核对。
- `recover()`：`dispatching` → `unknown("interrupted_dependency_call")`，
  `awaiting_result` → `unknown("interrupted_task")`；已完成的执行不会被重跑；
  已提交投递意图（`submitting`）的行同样收口成 `unknown` + `unresolved`，
  见下文「投递意图与重启恢复」。

## 回复投递复用既有出口

`reply_to="bridge"` 的回复**不新建第二条消息出口**：`Direct.delivery_request` 生成冻结的
已发布 `conversation#send_request`，`turn_id` 是该直接请求自己（它拥有自己的回复，
不借用陪伴轮次），`reply_id = digest([request_id, "reply"])` 稳定可去重，
`turn_sequence` 从会话唯一的串行计数器分配，所以聊天回复与功能回复不会互相超越；
`command` 信封在**投递时**才用请求上存的 `origin_ref` 现签，慢任务不会继承过期 deadline。

在同一个进程内，交付端口可以就是 NoneBot 薄桥的 `BridgeDelivery`（`Bridge.send`：
目的地核验、`reply_id` 去重、`unknown` 纪律都已经在里面）；Core 与桥接分进程时注入既有
`Sender` 客户端即可，两者落到同一个 `Bridge.send`。

### 共用一个出口时的顺序（会话出站序号 `send_band`）

`Bridge.send` 对同一会话只接受**严格递增**的位置（`turn_sequence * 100 + segment_sequence`），
拒绝更小的位置为 `version_conflict`。陪伴链在**封盘时**分配 `turn_sequence`，而功能回复在
**投递时**取号——因此一个「先封盘、后投递」的陪伴轮次位置可能低于已经发出的功能回执，
`Bridge.send` 会拒绝它，**用户就丢了一条聊天回复**。

号只能发一次，且一个轮次的所有分段必须落在同一个号里（否则同一条回复会横跨两个位置）。
网关已限制一轮至多 16 个分段（`len(segments) > 16` 即 `invalid_input`），所以
`号 * 100 + 分段号` 的编码在号递增时不会与别的号段相撞。
所以由 Core 统一发放会话出站序号（`conversations.send_band` / `send_band_owner`，单元是轮次或
直接请求），投递时**每个单元按自己的当前号重新判定**：

- 单元自己的号 `>= send_band`（号唯一，相等只可能是自己）→ **沿用自己的号**，同一轮的所有
  分段共用一个号，回执、重试与整轮身份都不变；
- 否则 → **取 `max(turn_sequence, send_band) + 1` 的新号**（只在被别人超越后发生一次）。

功能回复投递前还要过一道**合法消息边界**：若该会话当前的号仍被一个**已经开始发送**的陪伴轮次
持有（`turns.send_sequence` 已分配，或该轮已有分段离开 `pending`），功能回复不抢号、不改号，而是
把请求留在 `completed + ready_to_deliver`，记 `deferred_reason="outbound_band_busy"` 与
`deferred_on=<轮次id>`。轮次到达终态后 `Core.tick` 立即唤起一次 direct worker，把等待的功能回复
按**下一个号**发出。

- **不等于排队等聊天模型**：还没开始发送的轮次（仍在生成/准备）不持有边界，功能回复照常立刻取号，
  该轮稍后发自己的分段时自然拿到**更高**的号。等待只发生在「该轮的分段已经在出口排队」这个
  消息边界上，模型早已产出文本；
- 无序抢占会真的丢分段：若功能回复先取到 `501`，该轮在途的第二段 `402` 会被 `Bridge.send`
  判为旧位置而失败——所以这里宁可等边界，也不让功能回执夺走在途聊天轮次的分段；
- 边界等待有界：持有边界的轮次最终一定进入 `sent`/`failed`/`cancelled`/`closed_unknown`
  （回执、投递超时或对账窗口封顶），届时功能回复照发；
- 重启后不靠内存推断：号与会话记录都在库里；已被对账成 `sent` 的旧轮次若号已被超越，它的下一个
  分段取新号，不会重发已发过的分段（老位置的去重仍由 `reply_id` 负责）。

定向回归（真实 `Bridge`，同一出口）：
`tests/test_routing.py::test_a_functional_reply_between_chat_segments_does_not_lose_the_next_segment`
（301 → 401 → 501 → 402 的验收场景）、`test_repeated_functional_replies_cannot_overtake_a_turn_between_segments`、
`test_functional_replies_before_a_turn_keep_the_turn_in_one_band`、
`test_slow_outbound_io_defers_a_functional_reply_instead_of_interleaving`、
`test_concurrent_functional_and_chat_sends_never_duplicate_a_segment`、
`test_a_cancelled_turn_releases_the_boundary_for_a_waiting_functional_reply`、
`test_a_restart_between_chat_segments_still_delivers_each_segment_once`、
`test_a_functional_reply_never_costs_a_pending_chat_turn_its_reply`。

### 投递意图与重启恢复（`completed + submitting`）

投递前先在同一个事务里落一条 `reply_state="submitting"` 与**冻结的投递文档**（`delivery`：
`reply_id`、`segment_sequence`、`command.request_id` 都在里面），然后才做 IO。进程在 IO 中途
异常退出时，库里留下的就是这一行：意图已提交、结果未知。

`recover()` 除了把在途执行记成 `unknown` 之外，还会专门认领这些 `submitting` 行，把它们标成
`reply_state="unknown"`、`unresolved=true`、`blocked_reason="interrupted_delivery"`：

- **不重跑功能**（适配器不会被再次调用，请求仍停在 `completed`）；
- **不伪造没发**（不写 `failed`，也不假装 `sent`）；
- **不盲目重发**（`work()` 不会再向渠道发一次）；
- 只允许用**同一份投递证据**（同一 `reply_id`/`request_id`/`segment_sequence`）走只读
  `reconcile()`：渠道说已送达才 `sent`，说没送达才 `failed`，仍无回音就保持 `unknown`。
  晚到的旧 attempt 回包只落旧 attempt，不会改写已恢复的回复结论。

定向回归：`test_a_committed_delivery_intent_survives_a_restart_and_reconciles_to_sent`、
`test_a_committed_delivery_intent_that_never_arrived_is_never_resent`、
`test_a_committed_delivery_intent_stays_unknown_and_ignores_a_late_callback`
（都用真实文件库、真实重开连接；崩溃点用端口在意图提交后抛 `KeyboardInterrupt` 复现，是"IO 中途
异常退出的那条记录"，不是真实进程 kill）。

## 诚实标注（未配置/未核实即失败）

请求视图显式给出证据等级，不把合成结果说成真实结果：

| 字段 | 含义 |
| --- | --- |
| `reply_owner` / `reply_owner_enforced` | 唯一回复方及是否被约束 |
| `delivery_verified` | 只有已发布 `send_receipt` 为 `sent` 且带 channel message id 才为真 |
| `delivery_evidence` | `published_contract_receipt` / `delivery_<state>` / `null` |
| `result_verified` | 只有 `adapter="contract"` 的已发布结果才为真 |
| `result_evidence` | `published_contract_result` / `synthetic_adapter_result` |
| `adapter_available` / `delivery_port_available` | 依赖是否就位 |
| `contract_gap` | 桥接负责回复但没有交付端口时的精确说明 |
| `plugin_gap` | 适配器不是 `contract` 时的精确说明 |
| `deferred_reason` / `deferred_on` | 等待合法消息边界时的原因与所等轮次（`outbound_band_busy`），发出后清空 |

- 没有交付端口：请求 `completed` 但 `reply_state="not_started"`、
  `blocked_reason="delivery_port_unavailable"`，**不声称已回复**；
- 没有插件：请求停在 `pending`、`blocked_reason="plugin_unavailable"`；
- 会话不存在：`blocked_reason="conversation_unmapped"`；
- 会话被重新映射（身份不一致）：`blocked_reason="conversation_mismatch"`，
  与「不存在」区分开，不把回复发到别处；
- 投递 `unknown`：`reply_state="unknown"`、`unresolved=true`，只能靠 `reconcile()` 改变；
- 重启时停在已提交投递意图：`reply_state="unknown"`、`unresolved=true`、
  `blocked_reason="interrupted_delivery"`，同样只靠 `reconcile()` 改变；
- 等合法消息边界：仍是 `reply_state="ready_to_deliver"` 且 `deferred_reason` 非空，
  **不是**已送达也**不是**失败，轮次终态后自动发出；
- 显式重投（`retry(redeliver=True)`）除要求最近回执 `failed` 且 `retry_safe=true` 外，
  还要求该回执的 `reply_id`/`segment_sequence` **确属本次投递文档**——别人的回执不能当作
  本次未执行的证明。

随包提供的 `SyntheticPlugin`（`adapter="synthetic"`）覆盖一个只读查询 `/查询` 和一个慢任务
`/慢任务`（`state="accepted"` + `settle`）。它**不是**任何真实 GsCore 或群管 API，
`result_verified` 永远为 `false`，证据标签是 `synthetic_adapter_result`。

## 配置接线

`config["direct"]`（缺省即完全不注册，任何文本都走陪伴链）：

```json
{
  "direct": {
    "adapter": "synthetic",
    "timeout": 20,
    "request_expiry": 600,
    "commands": [
      {"command_id": "companion.synthetic.query", "version": 1, "name": "/查询",
       "platforms": ["qq"], "audiences": ["self_private", "group"],
       "parameters": {"fields": [{"name": "subject", "type": "token", "required": true}]},
       "reply_to": "bridge"}
    ]
  }
}
```

`adapter` 只接受 `"synthetic"`（本任务范围内）；真实功能插件应由功能产品通过同一端口注入。
`core.recover()` 与 `core.tick()` 已接线；应用另有 0.5 秒 `direct_worker`。

## 数据库

`user_version` 由 6 提升到 7，新增 `direct_commands` / `direct_requests` / `direct_attempts`
三张表与索引（`direct_commands_scope`、`direct_requests_pending`、`direct_requests_version`、
`direct_attempts_request`）。v6 打开前先用 SQLite backup 落
`<database>.pre-routing-v7-<随机值>.bak`，随后在单事务里建表/索引/升版本；
中途失败回滚并释放 owner 锁，修复后重试。恢复备份须停机并隔离 DB/WAL/SHM。

## 定向验证

```text
python -m pytest tests/test_routing.py -q
```

覆盖：登记不可变/范围独占/版本取代、usage 推导、只有登记的完整命令被认领、
参数形态拒绝与 usage 回复、快路不排防抖不占轮次不调模型、未登记文本继续走陪伴链、
平台/模型不可达仍能执行、重复事件只执行一次、同键异参拒绝、消息版本绑定与取代、
两个入口共用一次执行、并发入口只执行一次、bridge/core 两条回复责任互斥、
persona/好感度不是权限、撤销授权与撤销登记在派发前取消、unknown 不重发、
redeliver 需 `retry_safe` 证明、`reconcile` 是 unknown 的唯一出口、
会话消失/交付端口缺失/插件缺失如实失败、慢任务不阻塞聊天且只由可信回包收口、
插件超时、取消（派发前与在途）、重启中断记 unknown 且不重发、
晚回包只落旧 attempt、两个聊天轮次忙碌时命令仍执行、桥接复用 `capture`/`send` 而不建第二条出口、
未配置扫描上限的有界轮转（含"所有请求同一毫秒位置、游标停在最后一行"的几何）、
v6→v7 迁移备份/回滚/水位不变，以及本轮的共享出口顺序与投递意图恢复：

- 真实 `Bridge` 下功能回执与聊天分段交织（301→401→501→402 场景）：分段一条不丢、
  不重复，一个轮次的分段始终在同一个号里；
- 多次功能抢占、慢 IO、并发 worker、轮次被取消、发送中途重启：每种几何下应发的每条
  都恰好发出一次；
- 已提交投递意图（`completed + submitting`）跨真实重开连接：恢复成 `unknown` 且
  `unresolved`，不重跑功能、不伪造未送达、不自动重发，只用同一份投递证据
  把 sent / failed / 仍未知三种结论分别落地，晚到旧 attempt 回包不改写结论。
