# 角色生活与已发布日记的授权只读端口（TS-077）

本模块在既有 `core.life` 可信端口之外，提供一个**只读**内部 HTTP 端口，让已登记的外部读取服务
读取角色生活状态与**已发布**日记。它不写任何生活事实、不触发时间推进、不调用模型、不调用
Memory/Platform/渠道，也不新增第二套授权存储：授权仍是 `core.life.set_diary_access` 写下的
`life_access` 事实，只是读取时必须同时满足部署表与剧情授权。

`core.life.snapshot()` 会 `tick(force=True)`，因此**不能**被这个端口使用；快照读的是持久行本身。

## 部署

`life_readers` 是可选配置段：调用服务名 → 该服务固定读者身份与它可见角色。

```json
"callers": {"story_reader": {"token_env": "STORY_READER_TOKEN"}},
"life_readers": {
  "story_reader": {"reader_id": "reader:story", "actor_ids": ["actor:a", "actor:b"]}
}
```

- 键必须是已有 `callers` 服务名：没有凭据的服务不能被授予读者身份，否则启动即失败。
- `reader_id` 是非空字符串，长度 ≤128 字符；`actor_ids` 是 1..64 个互不重复的非空字符串。
  同一 `reader_id` 不得出现在两个服务上（一次请求只有一个读者身份）。
- 段缺失（`null`）→ 四个路由一律 503 `dependency_unavailable`，不假装端口存在。
- 段为空对象 → 端口存在但无人登记，任何已认证调用者都是 403。
- 任何一条不合法都在启动时 `ValueError`，绝不半配置启动；错误只说明配置错误，不掩盖其它故障。

普通RoleRuntime新增角色可由部署一次登记的独立生活读服务自动浏览，不需每角色修改静态配置：

```json
"callers": {
  "platform": {"token_env": "PLATFORM_ROLE_TOKEN", "issuer": "platform"},
  "platform_life": {"token_env": "PLATFORM_LIFE_READ_TOKEN"}
},
"life_readers": {
  "platform_life": {
    "reader_id": "reader:platform-life", "actor_ids": [], "runtime_roles": true
  }
}
```

`reader_id`由部署选定，与该已配置服务凭据一一对应；不能由HTTP请求自报。
`runtime_roles`缺省为false；true时actor_ids可空，并授权枚举Platform已受理的持久RoleRuntime角色。
首次启用创建生活实体时安装该固定reader到原life_access；启动接线也补已有角色的缺失授权。
已有life_access行一律不覆盖，包括显式readers=[]撤回，重启/角色重放及读取都不会重新授权。
读路径继续实时检查life_access，不写grant、不tick、不调用模型。静态角色仍要求actor_ids与显式剧情授权。
动态及静态候选合计最多64，超预算明确拒绝；角色停用可继续读取已授权历史和暂停状态。

### 今日计划与时间线（life-read/v1）

保留四个旧端点，并新增POST`/internal/v1/life-read/today`和`/timeline`，使用同一Bearer/剧情授权、
媒体类型和16KiB请求预算。部署合同在同根`life-read/v1`；未装包新端点503，旧端点仍可用。
两端点只返回持久状态，角色启用后由后台生活工作器登记当日计划；手动configure但尚未登记计划返回404。

`today`请求`{schema_version:1,actor_id}`，返回actor/day/timezone/enabled、state_basis与observed_at，
以及plan_id/version/state/generation_state/generated_by/current_phase_id和最多24条entries。
entries含稳定phase_id、civil minute、activity、detail、state与generation_state。计划是意图，
不能把它当作已经发生的体验。plan.state为active/paused/completed/superseded；entry.state为
planned/current/elapsed/skipped。生成状态封闭为queued/generating/completed/unavailable/failed/
interrupted/superseded/skipped，generated_by为baseline或gateway。

`timeline`请求`{schema_version:1,actor_id,day,limit?,after?}`，limit为1..50，默认20。
游标`{position,known_id}`直接完整传回；position是已有life_known.sequence整数（learned_at秒）持久排序键，
不是请求时补写字段。时间线按角色获知事件时的本地day、position/id降序分页，返回event_id/known_id/
kind/summary/occurred_at/learned_at/via/phase_id/plan_id/generated_by；occurred_at和learned_at是UTC
epoch数字，获知日可以与发生日不同。generated_by为baseline/gateway/simulation，next_after空表示末页。
分页用同position与更早position两个有界索引seek，不用OFFSET/COUNT，不受旧深页长度影响。

管理端`POST /internal/v1/life-generation/retry`复用Platform角色管理凭据，生活读凭据不能调用。
请求`{actor_id,plan_id,phase_id,expected_version}`：phase_id为null表示日计划，非空必须是当前阶段。
expected_version来自today.plan.version；过期409，角色未启用/计划不匹配404，当前任务不可重试400。
只受理unavailable/failed/interrupted，回执含schema_version/actor_id/plan_id/plan_version/state，
state只为queued或unavailable，不声称已生成。页面管理入口随后沿独立授权读链刷新today/timeline。
重试为每次模型请求递增持久request_attempt；选模turn_id与Gateway的X-Tianshu-Turn-ID严格一致，
不会把旧模型授权用于新的attempt。旧回包仍需通过当前角色/内容版本和attempt归属复核。

调用者身份只来自 bearer 凭据（与其它内部端口同一 `tokens` 表）；请求体里的 `reader_id`
一律 400 `invalid_input`，不能自报身份。若某调用者的凭据环境变量未设置（启动时没有该 token），
而它又出现在 `life_readers` 里，同样按配置错误启动失败——不接受一个永远无法认证的读者登记。

## 请求与响应

统一信封：请求 `schema_version` 必须为整数 1，未知字段一律 400。四个端口都是 POST，
且都要求 `Content-Type: application/json`（允许带合法的 `charset` 参数，如
`application/json; charset=utf-8`）。缺失、写成别的媒体类型、写成两个 `Content-Type`
（无论取值是否相同）或参数不合语法，都在**进入只读端口之前**按 400 `invalid_input` 拒绝——
媒体类型是被检查的声明，不是靠 `strict_json` 反推的结论。凭据仍然先判定：没有有效 bearer
头时，媒体类型错误也只会得到 401。此约束只作用于这四个路由，其他内部端口行为不变。

| 路由 | 请求（除 schema_version 外都可选） |
| --- | --- |
| `/internal/v1/life-read/actors` | `limit?` 1..50（默认 20）、`after_actor_id?` |
| `/internal/v1/life-read/snapshot` | `actor_id` 必填 |
| `/internal/v1/life-read/diaries` | `actor_id` 必填、`limit?` 1..50（默认 20）、`after?` |
| `/internal/v1/life-read/revision` | `actor_id`、`diary_id`、`revision_id`、`expected_diary_version` 均必填 |

`limit` 必须是正整数，bool、字符串、0、51 及 `null` 全部拒绝：缺省是缺省，`null` 是错误。

`actors` 按 actor_id 的 Unicode 码点升序返回，只枚举部署表中、且当前有剧情授权的角色；
每项是 `actor_id`、`actor_version`、`world_id`、`room_id`。`next_after_actor_id` 为最后一名的
actor_id，取满一页才有下一页；不返回总数，不做全表扫描。

`snapshot` 读持久行并逐项校验：角色/房间/世界必须存在，房间必须属于角色所在世界，版本必须是
正整数，`mood` 必须非空。返回 `state_basis = "last_persisted"`，`activity` 就是角色行里的当前值
（可能是 `null`），`observed_at` 是本次读取时刻。任何不一致都是 503 `dependency_unavailable`，
不返回半个快照、也不顺手结算。

`diaries` 只返回 `published_revision` 非空的日记，按 `(day, diary_id)` 降序；每项含
`diary_id`、`actor_id`、`day`、`state`（当前真实状态，例如发布后又起草编辑仍是 `draft`）、
`version`、`published_revision_id`、`fictional`、`captured`。不含正文、不含 `current_revision`、
不含 `config_version`、不含修改来源。`captured` 的三个字段（`recipe_id`、`recipe_version`、
`material_version`）来自**日记行冻结的 recipe**，不是当前配方；并且与 `revision` 走**同一处**
内容寻址校验：重算 `digest([actor_id, day, 完整 recipe, material_version])` 必须等于该行的 id，
否则 503。列表既然给出了配方版本，就必须像正文那样证明它；损坏行不会被跳过、也不会被改写。
`next_after` 是 `{"day": ..., "diary_id": ...}`，是位置键而不是行引用：游标指向的行已被删除
也能继续翻页。

`revision` 按 `revision_id` 精确读取**已发布指针指向的**修订，绝不回退到当前草稿：

- `expected_diary_version` 与行的 `version` 不一致 → 409 `version_conflict`（带 `current_version`），
  且在读取任何正文之前完成比较，不会读取 `life_revisions`。
- 指针名不存在、指向缺行、指向别的日记、素材哈希与日记行不符 → 503，不静默替代。
- `captured.config_version` 是沿父链走到最近一次网关生成的**真实回执**读出的配置版本；人工修订
  继承其父的证明。回执必须通过已发布 `model#route_receipt` 合同、`caller_service == "companion"`、
  `outcome == "succeeded"`、`config_version` 为正整数，且与该日记行的 `config_version` 一致；
  否则 503——不把“配置换过之后重试的日记”按新配置重新标注。
- 父链必须严格线性；环、断链、超过 64 层（429 `budget_exceeded`）都是明确失败，不截断、不跳过。
- 正文与回执细节（编辑器、原因、provider、model）不返回给外部读者。

## 错误与预算

- 401 `unauthorized`：无 bearer 或凭据不匹配任何调用者。
- 403 `forbidden`：凭据有效但该服务不是读者。
- 404 `not_found`：角色不在部署表、没有剧情授权、或修订不是已发布指针——三者不可区分，
  避免探测存在性。授权每次请求实时读取，撤销立即生效。
- 409 `version_conflict`：`expected_diary_version` 过期。
- 400 `invalid_input`：信封/字段/游标不合法，或原始请求体超过 16,384 UTF-8 字节
  （在按字节累积时判定，不先解析）。
- 429 `budget_exceeded`：`diaries` 响应超过 262,144 字节、`revision` 响应超过 1,048,576 字节，
  或父链超过 64 层。预算按整份响应判定，宁可拒绝也不截断。
- 503 `dependency_unavailable`：端口未部署，或持久行无法解释（缺行、类型异常、回执不能证明捕获）。

响应体沿用 `Fault.wire` 统一错误结构，含 `schema_version`、`request_id`、`code`、
`execution_state`、`retryable`。

## 派生索引与恢复点

`diaries` 的分页依赖一个派生索引：

```sql
CREATE INDEX IF NOT EXISTS life_diaries_published_page ON life_diaries(
  conversation_id, json_extract(body,'$.day') DESC, id DESC)
WHERE json_extract(body,'$.published_revision') IS NOT NULL
```

它是本任务唯一的持久结构变更：不新增表、不新增字段、不动 `user_version`（仍为 9），
也不改共享合同。查询计划是纯索引搜索（`SEARCH life_diaries USING INDEX ...`），无 TEMP B-TREE，
VM 步数不随无关行数增长。

续页不是一条行值比较，而是同一索引上的两段范围：先取**同一 day 且 `id < 游标 id`**，
不足一页时再取**更早的 day**（`id` 不受限），两段合计不超过 `limit + 1` 条候选，顺序仍是
`(day DESC, id DESC)`，同一只读事务内完成。这样代价只跟返回的行数有关，与游标已经翻过的
同角色同日历史无关：同一天 100 行与 10,000 行、游标固定、同样返回 11 行时，VM 步数都是 **130**
（旧的行值写法分别是 1,061 与 109,961），计划分别是
`(conversation_id=? AND <expr>=? AND id<?)` 与 `(conversation_id=? AND <expr><?)`。

打开数据库时先比对 `sqlite_master` 里的同名索引定义：定义不同就拒绝启动（不自动替换、不静默
改名），且不会留下新的备份。缺少该索引时，必须在任何新 DDL 之前把当前数据库（含 WAL）用 SQLite
备份到同目录唯一文件 `<database>.pre-life-read-index-<uuid>.bak`；备份失败即中止本次打开，
不执行新 DDL。备份只在“确有旧事实且确实要改结构”时产生：

- 新空库或 `:memory:` 不备份；
- 索引已正确存在时重开不备份、不改事实、不改 `source_head`；
- v1..v8 升级本来就会在本次打开的结构 DDL 前保存一份完整备份，此时复用那一份，不再多造第二份；
- 索引创建在既有初始化事务内，事务失败则索引与版本一起回滚。

恢复流程与既有迁移一致：停进程、确认无 owner、隔离当前 DB/WAL/SHM，再把所选备份复制到目标
路径后重启。备份可能含私密事实，按数据库同级保护。运行中的请求路径不会创建索引或备份。

## 验证

```bash
.venv/Scripts/python.exe -m pytest tests/test_life_read.py tests/test_life_read_queries.py tests/test_life_read_index.py -q
TIANSHU_TLS_PYTHON=<具有cryptography的解释器绝对路径> .venv/Scripts/python.exe -m pytest tests/test_life_read_https.py -q -s
.venv/Scripts/python.exe -m pytest -q
.venv/Scripts/python.exe -m ruff format --check src integrations tests scripts
.venv/Scripts/python.exe -m ruff check src integrations tests scripts
.venv/Scripts/python.exe -m compileall -q src integrations tests scripts
```

- `tests/test_life_read.py`：授权映射、身份来源、不泄露、投影、游标、版本先行、捕获证明
  （列表与正文用同一处内容寻址校验）、媒体类型边界、预算与“读操作零写入”。
  其中一个用例用真实 `Life` 链（本地模型替身）生成并发布日记，再用本端口读回。
- `tests/test_life_read_queries.py`：只读事务（BEGIN/COMMIT、无任何写语句）、异常回滚、
  索引计划（含两段续页范围）、同日深页步数恒定、跨日补齐、删游标续页、末页、keyset 不漏不重。
- `tests/test_life_read_index.py`：同名错误定义拒绝启动、缺索引先备份再建、备份含 WAL、
  备份失败不留新 DDL、v8 复用同一份备份、建索引在初始化事务内、重开不新增备份。
- `tests/test_life_read_https.py`：真实临时证书下的真实 Core 进程，覆盖四路由、身份 401/403、
  404/409、请求与响应预算 400/429、媒体类型 400（错误/缺失/重复 `Content-Type`，charset 合法）、
  客户端中途断开（无写入、服务继续可用）与进程退出；
  远端服务是记录型合成替身，断言其零请求，并断言请求前后全部事实行与 `source_head` 逐字节一致。

未验证：真实账号、真实渠道、真实模型、真实 Platform/Memory、生产部署与前端页面。本端口只读，
不提供网页 UI，也没有写入/发布能力；日记仍是虚构素材生成，不是现实对话总结。
