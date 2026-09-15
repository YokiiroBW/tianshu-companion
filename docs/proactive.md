# 角色主动联系与静默调度（TS-074）

本模块属于 Core，与生活/日记/图像/写作共用同一个 SQLite 单进程所有者。
`core.proactive` 是**可信同产品 Python 端口**：不新增 HTTP 路由、不新建跨产品合同、
不读 Memory 数据库、不读其他产品数据库，也不依赖 TS-017。
宿主必须先鉴权再调用任何变更或管理方法；不能把聊天文本、模型输出或前端 JSON
直接解包为订阅、目标或提醒。

## 默认关闭与唯一入口

主动联系**默认关闭**：没有 `register_subscription` 就没有任何候选能进入 `ready`。
订阅必须显式登记 (actor, person, audience, conversation) 目标，并给出 `consent`
（`registered_by`、`basis`、`evidence_ref`）：

- `basis` 只接受 `explicit_user_request` 或 `explicit_admin_registration`；
- 普通聊天文本、模型回复、画像字段、健康数据都不是同意，也没有任何代码路径把它们变成同意；
- 本模块没有健康数据输入口，也不推断现实经历；只有显式登记的提醒与角色目标会生成候选。

```python
proactive = core.proactive
proactive.put_template("remind", 1, body="该{summary}了，现在{due_time}。")
subscription = proactive.register_subscription(
    "actor:a",
    person_id="person:1",
    audience="self_private",
    conversation_id="conv:1",
    channel={"namespace": "qq", "binding_id": "qq-private",
             "channel_conversation_id": "private:a", "thread_id": None},
    consent={"registered_by": "admin:1", "basis": "explicit_user_request",
             "evidence_ref": "consent:1"},
    timezone_name="+08:00", quiet=("22:00", "08:00"),
    cooldown_seconds=3600, daily_limit=2, unanswered_limit=2, expiry_seconds=21600,
)
proactive.register_reminder(
    "reminder:1", actor_id="actor:a", subscription_id=subscription["id"],
    summary="喝水", due_at=1737000000.0, template_id="remind", template_version=1,
    registered_by="admin:1", evidence_ref="explicit:1",
)
```

## 文案只用显式模板

提醒和角色目标的正文只来自 `put_template` 登记的不可变版本化模板，
占位符仅限 `{summary}`、`{due_time}`、`{due_date}`、`{timezone}`，
全部来自登记内容与配置时区。**没有任何回退到聊天模型线路的路径**，
也不使用 `config_version`；模板缺失报 `KeyError`，未知占位符报 `ValueError`。
本模块不调用模型，因此不占用 Core 的模型槽位。

## 状态与闸门

日程由 Core 的 `tick()` 驱动（应用另有 2 秒主动联系后台任务），
不阻塞普通聊天：决策在事务内同步完成，派发在独立任务里 awaits 传输。

候选状态：`pending` / `deferred` / `suppressed` / `ready` / `sending` /
`sent` / `failed` / `unknown` / `expired` / `cancelled`。
每次评估按顺序判断：

1. 订阅不存在、非 `active`，或**订阅配置版本**已变 → `cancelled`；
2. 提醒/目标已取消、暂停、完成，或其版本已变 → `cancelled`；
3. 已过 `due_at + expiry_seconds` → `expired`（`due_window_passed`）；
4. 落在订阅时区的静默窗口内 → `deferred`（`quiet_hours`），目标为窗口结束时刻；
   若窗口结束已晚于到期 → `expired`（`quiet_window_passed_expiry`）；
5. 距上次联系不足冷却 → `deferred`（`cooldown`），同样受到期约束；
6. 未回复抑制：自上次联系后没有该 person 的真实 inbound，且未回复数达到上限
   → `suppressed`（`unanswered_limit`），等真实回复后自动恢复；
7. 当日额度用尽 → `suppressed`（`daily_quota`），目标为下一个本地零点；
8. 否则 `ready`。

未回复计数只依据 Core 自己已接受的 `collections`（真实 inbound）；
主动联系只写 `proactive_*` 表，因此永远不会被当成用户输入。

## 有界扫描与公平性

每次 tick 的扫描是**有界且公平**的，既不无限全扫也不靠放大上限：

- **事项扫描**（`goals`/`reminders` 各一次）：只取 `active` 且已到期的事项，
  并且**当前 occurrence 已经有候选的事项完全不占扫描名额**——已处理、已延后、
  已抑制、已失败或已过期的事项即使排在队头也不会挡住后面的到期事项。
- **候选扫描**：只取仍待决策（`pending`/`deferred`/`suppressed`/`ready`）的候选，
  同样按轮转窗口推进，否则最早的若干个候选会被反复评估而排在后面的候选永远停在 `pending`。
- 两处都用 `metadata` 里的**持久轮转游标**按 `(deadline, id)` 前进并回绕：
  每次至多扫描 `max_subjects`（每类）或 `max_candidates` 行，游标之后的一段与回绕段共享同一预算，
  因此在 `ceil(N / limit)` 次 tick 内每个对象都会被访问到，进程重启后从原位置继续而不是回到队头。
- 登记本身不可用（模板行缺失、渲染超出模板上限）的事项不会让整个 tick 失败，
  只在该事项上记 `blocked_reason` 并退出扫描；用带 `expected` 的重新登记即可清除并恢复。
- 候选清理只删该事项已终结且**不属于其当前 occurrence** 的历史候选，
  避免把已结算的 occurrence 重新物化成新候选。

一次性事项过期后保持 `active` 且保留原 `due_at`（可见、不静默重排），
周期目标则跳到下一个未来 occurrence，不补播错过的时段。

时间语义：所有到期时刻都是绝对 UTC epoch，静默窗口是配置时区的 civil minute。
跨午夜窗口（start > end）与同一天窗口都支持；决策在每次 tick 从持久行重算，
时钟前进/回退、进程重启都不会依赖已存的本地时间。
夏令时缺口（不存在的 civil minute）取缺口后第一个真实时刻，
回拨重复时刻固定取较早一次（fold=0）。Windows 无 tzdb 时 IANA 名称按 `life.zone`
既有约定报错，不会偷偷当成 UTC；本机未安装 `tzdata`，命名时区的实际 DST 行为
在验证中用测试自建的 TZif 文件覆盖（见下）。

## 派发、去重与未知结果

派发前在**同一个所有者事务**内重读并复核来源、授权、候选版本、闸门与取消，
然后才写入提交意图（attempt `submitted`）并消耗额度：

- 额度只在提交时计入，且与提交意图同事务，因此并发争抢额度时只有一个候选拿到当日名额，
  其余变为 `suppressed(daily_quota)`；触达失败不退还额度（宁可少发，不可多发）；
- `guard` 是宿主提供的同步本地授权复核（channel binding、namespace、audience、
  actor 角色、会话存在与来源隔离）。登记时与提交事务内各执行一次，
  撤权/取消的候选不会再发起传输调用；
- attempt 以 `request_id` 幂等；结果必须回带同一 `request_id`/`attempt_id`，
  `sent` 必须带 channel message ids；
- 旧 attempt 的晚回包只落在这条 attempt 上（`stale=true`），
  不改写候选、也不改写更新的 attempt；
- 结果不明（超时、异常、重启时仍是 `submitted`）一律记 `unknown`，
  **绝不自动重发**；只有显式 `retry_candidate(...)` 会开启新 attempt；
- 已经提交的传输调用无法撤回；撤权只阻止后续联系，不声称能追回已发出的请求。

## 合同缺口（未虚报完成）

现有已发布 `text-dialogue/v1` 只有一个出站文档 `conversation#send_request`，
它以已存在 turn 的 `turn_id`/`turn_sequence`/`reply_id` 为键，而 turn 只在
真实 inbound collection 封存后才存在。用它发送用户从未触发的联系，就必须伪造
inbound 消息或来源证明，本任务明确禁止。因此：

- 本模块拥有日程、候选与投递 attempt，并把传输调用交给注入的 `dispatcher` 端口；
- 生产默认没有 dispatcher：到期候选保持 `ready`，`candidate_view()["dispatch"]` 返回
  `available=false` 与 `contract_gap=<精确说明>`，不写 attempt、不消耗额度；
- 候选视图的 `delivered` 只有在**声明 `adapter="contract"` 的已发布合同回执**且
  state=`sent` 且带 channel message ids 时才为真；测试合成适配器
  （`adapter="synthetic"`）即使回 `sent`，候选仍是 `delivered=false`，
  `delivery_evidence="synthetic_adapter_receipt"`；
- 因此**本任务不声称任何主动消息已真实送达**，也不把候选叫成已送达。

## 数据模型

| 表 | 作用 |
| --- | --- |
| `proactive_subscriptions` | 显式订阅：actor/person/audience/conversation/channel、时区、静默窗口、冷却、每日额度、未回复上限、到期窗口、consent、`config_version` |
| `proactive_templates` | 按 (id, version) 不可变的显式模板与允许占位符 |
| `proactive_goals` | 角色持续目标：状态、到期/周期、模板引用、`next_due_at` |
| `proactive_reminders` | 显式登记的一次性提醒：`due_at`、模板、basis/registered_by/evidence_ref |
| `proactive_candidates` | 到期候选快照：钉住的订阅/目标版本、渲染文案与 `content_version`、决策与延后时刻、attempt 与投递证据 |
| `proactive_attempts` | 提交意图与结果：幂等 request/attempt id、adapter、receipt、stale、`verified_delivery` |
| `proactive_quota` | 按 (订阅, 本地日期) 的额度账本；在提交事务内自增 |

`candidates(...)` / `subjects(actor_id)` / `subscriptions(actor_id)` 是管理读端口，
按 actor 过滤；宿主必须自行鉴权。主动联系不改写真实 source-facts 水位
（`source_head` 不变），也不写 Memory 事实事件。

## 数据库与迁移

数据库 `user_version=6`。v5 打开前用 SQLite backup 保存完整视图（含 WAL）至
`<database>.pre-proactive-v6-<random>.bak`，然后在同一事务内创建 7 张 `proactive_*`
表与索引并提升版本；失败回滚 DDL/版本并释放 owner 锁。
v1–v4 仍用既有备份前缀，旧对话事实、`source_head`、生活/图像/写作行为不变。

恢复必须先停 Core、确认无 owner，隔离保存当前 DB/WAL/SHM，再复制所选完整备份到目标路径；
禁止运行中覆盖数据库或单独拼接旧 WAL，也不自行清除水位/隔离标记。
本次验证只用临时合成数据库，未操作生产数据。

## 本轮未做（边界）

没有 HTTP/网页路由、没有新建跨产品合同、没有真实渠道发送、没有 Memory 读写、
没有读取其他产品数据库、没有依赖 TS-017。模板不做模型改写，也不保证文案质量与语气。
`guard` 只覆盖 Core 本地权威事实；尚未到达 Core 的远端 Memory 撤销属于已声明缺口，
本模块不声称能复核它。多进程并发不在范围内（SQLite 单进程所有者）。

## 实际验证

- `python -m pytest tests/test_proactive.py -q`：**24 passed**。
  覆盖默认关闭与仅显式登记、模板缺失/未知占位符/不可变版本、静默/冷却/未回复/
  额度/到期五类决策与顺序、跨午夜窗口与时钟前后跳、TZif 真实夏令时缺口与回拨重复、
  并发争抢当日额度、撤权与本地授权复核、显式取消与取消目标、晚回包只落旧 attempt、
  unknown 不自动重发与重启恢复、无 dispatcher 时不声称送达且不耗额度、
  主动提交期间普通聊天照常完成、Core tick 与读端口按 actor 隔离、
  失败不吃掉目标周期、v5→v6 迁移备份/回滚/水位不变；
  返修另覆盖：过期/已处理/待处理队头不再挡住到期事项（goal 与 reminder 两类）、
  超过批量的积压按 `max_subjects` 有界排空且每 tick 调用次数可测、
  已有候选不占扫描名额、轮转游标在重启后继续（含回收流程本身推进）、
  以及游标必须停在全局最后一行（回绕段与游标段是两次查询）、
  候选队列同样轮转而不是让后面的候选永远停在 `pending`、
  不可用登记只记 `blocked_reason` 并退出扫描、周期目标在扫描压力下仍推进。
- `python -m pytest -q`（设置 `TIANSHU_TLS_PYTHON`）：见交接记录。
- `python -m ruff format --check src integrations tests scripts`、
  `python -m ruff check src integrations tests scripts`、
  `python -m compileall -q src integrations tests scripts`：通过。

全部使用合成模型、合成订阅/提醒/目标、临时 SQLite 与虚构标识；
没有真实模型、GPU、渠道、设备或生产数据。
