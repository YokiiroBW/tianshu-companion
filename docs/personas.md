# 角色人格版本管理与对话快照（TS-075）

## 人格档案编辑增量（2026-09-29）

`Personas.manage` 现有七个编辑操作：`author_catalog`、`author_view`、
`create_profile`、`save_profile`、`apply_profile`、`save_role`、`apply_role`。
可复用档案在同一个 `persona_personas` / `persona_revisions` 库中以
`persona-profile:<uuid>` 标识；名称、简介是元数据，不进入提示词，也不是角色、
来源、模型或发送权限。创建与保存只写草稿。已登记角色可以直接保存草稿或明确应用。

`apply_profile` / `apply_role` 在单个 SQLite 事务中写不可变角色修订、显式批准、
发布指针及操作幂等账本；`expected`（档案版本）、`target_expected`（目标角色版本）
都在写入前比较。相同 `request_id` 和规范化请求重放返回首次结果，版本冲突不覆盖。
编辑正文、语气、风格、称呼四个字段时，原修订的其他标量字段继续保留。新的发布
只作用于下一次准备的轮次；已有轮次保持原快照。部署导入仍不能覆盖已发布角色，
档案不在角色导入名单中。此增量不新增表、索引或 `user_version`；部署前仍需按既有
v9 备份、停写和恢复流程保护权威 SQLite 数据库。
应用档案时，目标角色原有的四个编辑字段由档案完整替换；档案留空的可选字段会清除，
目标角色的非编辑扩展字段继续保留。Core 从每轮钉住的修订组装 system 文本：正文、
非空语气、非空风格、非空称呼；名称/简介不进入模型请求。旧版只有正文时的 system
文本保持原样。档案摘要还记录上次应用产生的目标发布修订，供网页与当前发布指针核对。

隔离验证：`python -m pytest tests/test_persona_authoring.py tests/test_personas.py
tests/test_persona_queries.py tests/test_persona_chain.py -q`。线上管理仍只认独立
`personas.admin_token_env`；普通聊天、来源或桥接凭据不能写人格。

本模块把"角色人格"从一个可随手改写的配置字段，变成**版本化、需显式批准、可追溯回退、
且每轮对话都钉住不可变快照**的受管数据。人格文本永远不是权限、不是来源、不是模型绑定，
也不是发送资格——它只是一段被钉住的内容。

新增模块 `tianshu_companion.personas`（领域 + 应用用例）与
`tianshu_companion.persona_cli`（本地/在线维护适配器）；`core.py` 只在既有"准备边界"
取一次快照，`store.py` 只加表、索引与迁移，`app.py` 只加一条受鉴权的管理路由。

TS-076 在同一入口上补**只读浏览与版本比较**：新增
`tianshu_companion.persona_queries`（纯规则：页大小、游标绑定、字节预算、四字段比较），
`personas.py` 增加 `catalog`/`history_page`/`revision`/`compare` 四个只读操作与**匹配派生索引的
keyset 范围查询**，`persona_cli.py` 增加同语义命令，`store.py` 只在既有 persona 索引区新增四个
`(conversation_id, position, id)` 派生分页索引。`app.py`/`core.py` 未改：既有
`POST /internal/v1/persona/manage` 已经做了鉴权与分发，不需要第二条端口。

## 职责与依赖表（高内聚低耦合符合性）

| 单元 | 拥有的职责 | 只允许依赖 | 明确不拥有 |
| --- | --- | --- | --- |
| `personas.Personas`（领域+用例） | 草稿/修订不可变、批准、发布指针、回退为**新修订**、撤权、导入游标、快照钉取与校验、**操作幂等账本**、**有界页查询（SQL `LIMIT` + 匹配索引的键集范围 seek）**、`manage(request)` 唯一应用入口 | `Store` 的通用连接/事务/读写、`digest`/`canonical`、`persona_queries` 的纯规则 | 角色权限与登记、来源登记、模型绑定、发送资格、Memory 关系数据、HTTP、CLI 参数、Direct 发送顺序 |
| `persona_queries`（纯规则，TS-076 新增） | 页大小校验、**游标结构/编码/校验**（ASCII 与 base64url 形状先于密码学）、**页字节预算**、**四字段比较** | `contracts.canonical`、标准库 | 任何表名、任何 Store、任何 SQL、任何人格事实、HTTP/CLI/权限 |
| `core.Core` | 在**既有准备边界**调用 `personas.pin(actor)` 取快照并把 `config_version` 写进 turn；每次模型调用前 `personas.verify(role)`；把 `PersonaError` 映射为本模块既有 `Fault` | `personas` 的四个公开方法：`pin`/`verify`/`recover`/`manage`/`import_config` | 草稿、批准、发布、回退、历史、浏览与比较等业务规则；persona 表名（`test_boundaries.py` 结构断言禁止） |
| `persona_cli`（适配器） | 参数解析 → 一个操作文档 → `Personas.manage`；离线用 `--database` 自持 owner 锁，在线用 `--url` 走管理端口；`--request-id` 只做透传，缺失时生成一次性 id | `personas.manage`、`personas.deployment`（部署形状唯一规则）、`Store` | 任何人格规则、任何 SQL、任何表名、任何直接写入、绕过单所有者、自行判断"是否重复请求" |
| `app.create_app`（HTTP 适配器） | 只做鉴权 + 分发：`POST /internal/v1/persona/manage` → `core.manage_persona(service, body)` | `core.manage_persona` | 业务规则（`manage_persona` 本身只是"人设是否启用/是否 `persona_admin`"、把已鉴权服务记为授权域三件事的适配器） |
| `store.Store` | 连接、事务、owner 锁、结构迁移与恢复备份、**可重入事务**（内层 `with transaction()` 加入外层，只有最外层提交/回滚）、**既有 persona 索引区**（含四个派生分页索引，`IF NOT EXISTS`） | `sqlite3` | 人格语义；`PERSONA_TABLES` 只出现在 `store.py` 的表清单里 |
| 角色/来源/绑定/发送（既有模块） | 权限、来源登记、模型绑定、发送资格 | 不变 | 人格文本永远改不动它们 |
| Memory / Direct | 关系数据、功能指令与出站排序 | 不变 | 人格管理不写 Memory 关系数据、不碰 TS-024 顺序 |

模块边界由测试直接断言（`tests/test_boundaries.py::PersonaBoundaryTests`）：
除 `personas.py`/`store.py` 外**没有任何模块写出 persona 表名**；
`personas.py` 不 import `core`/`app`；`core.py` 只有一处 `from .personas import`，
且不再出现 `"published_revision"`/`"draft_revision"`/`"expected"`/`"operator"`/`"approve"`/`"publish"`
这些用例词汇；`persona_cli.py` 不出现 `store.get/put/list` 且只调用一次 `.manage(`；
`direct.py` 不 import `personas`。

## 唯一入口与两个适配器

```python
# 唯一应用入口（离线 CLI 与在线端口都提交同一种文档）
personas.manage({
    "operation": "publish",          # list/import/get/history/capabilities/draft/approve/
    "subject": "actor:companion",    # reject/publish/rollback/retire/restore
    "operator": "admin:1",           # 每次写入都必须点名操作者
    "reason": "语气过于生硬",         # 有界理由，随事实一起留存
    "expected": 7,                   # 乐观并发：写入者看到的 persona 版本
    "revision_id": "<sha256>",
    "request_id": "release-2026-09-17-1",  # 写入必填：这次操作的唯一身份
    "scope": "persona_cli",          # 可选：签发该请求的授权域，默认 "local"
})
```

离线维护（单所有者保护）：

```bash
python -m tianshu_companion.persona_cli --database .runtime/companion.db list
python -m tianshu_companion.persona_cli --database .runtime/companion.db \
    --subject actor:companion --operator admin:1 --reason "语气调整" --expected 3 \
    --request-id release-2026-09-17-1 --content persona.json draft
```

在线维护（自带独立管理凭据，与聊天/入站/桥接凭据分离）：

```bash
export TIANSHU_PERSONA_ADMIN_TOKEN=...
python -m tianshu_companion.persona_cli --url http://127.0.0.1:8765 list
```

- 服务正在运行并持有该数据库时，离线命令返回 `service_running` 并且**不写任何东西**；
  补救办法是停服，或改用 `--url` 走已鉴权的管理端口。
- 没有任何 `callers` 凭据能到达人格写入：管理端口只认 `personas.admin_token_env`
  指定的那一个凭据（服务 `persona_admin`）；未配置时该端口不是"开放"，而是不可用（503）。

## 操作幂等（请求身份与结果账本）

**内容寻址只去重"修订正文"，不能替代"操作去重"。** 同一份文本再次提交是重放一条修订；
但"这次请求是否已经执行过"必须由操作身份回答，否则重发会变成第二次事实、丢失的响应永远
无法安全重试。规则：

- **身份**：`request_id`（有界、必填）与签发它的授权域 `scope`（默认 `local`，管理端口记
  已鉴权的服务名，CLI 记 `persona_cli`）共同构成一次写入的身份
  `operation_key = digest({scope, request, operation})`。**每个写操作都要身份**（`draft`/
  `approve`/`reject`/`publish`/`rollback`/`retire`/`restore`）；读操作（`list`/`get`/
  `history`/`capabilities`）不需要。缺少身份时写入被拒（`invalid_input`），不会退化成
  "执行但不记账"。
- **绑定**：身份绑定一个**规范化请求摘要**（`canonical` 序列化的
  `{operation, subject, content, from_config, revision_id, operator, expected, reason, note}`）。
  键相同而摘要不同 = 另一个请求冒用同一身份 → 拒绝（`invalid_input`），并且**不产生任何写入**。
  键相同且摘要相同 = 重放：返回**首次执行时记录的结果**，不再执行业务规则，也不再写任何事实。
- **原子**：业务写入与账本行在**同一个 SQLite 事务**里提交（`Store.transaction()` 可重入，
  内层加入外层，只有最外层提交/回滚），因此不存在"写了事实但没记结果"或反之的窗口。
- **持久**：账本在 `persona_operations` 表里，重启后同一请求仍然重放同一结果；两把凭据
  的两个授权域互不干扰（一个域不能重放或阻塞另一个域的请求）。
- **不替代 CAS**：重放命中账本才跳过 `expected` 检查；**不同**请求带着过期 `expected`
  依然返回 `version_conflict`，不会被"见过这个身份"变成成功。
- **导入例外（既有语义，明确界定）**：`import` 的幂等键是**部署文档指纹**
  （`persona_imports` 游标），同一文档重放返回 `skipped`，文档变化则是新导入；它不参与
  `request_id` 账本，也不被账本覆盖。
- **批准推进版本**：`approve` 改变可见状态（`state` 由 `draft` 变 `approved`、写入
  `approved_at`/`approved_by`），因此**和其他写入一样推进 persona 版本**：`expected`
  覆盖审批状态，两个操作者不能凭同一次旧读取各自批准同一份待决草稿（后者得
  `version_conflict`）。决定行仍然只追加，不回写历史。

## 不变量

1. **修订不可变、内容寻址**：修订 id 由 `(subject, content, parent)` 派生。
   字节相同且父修订相同 → 同一条修订（重放，不复制）；同键异内容或父修订不同 → 拒绝写入。
2. **未批准不进模型**：发布要求对该修订 id 的**显式批准**，且该批准未被后续拒绝撤回；
   批准只由被鉴权的操作者签发，聊天文本与模型输出无任何路径可以批准。
3. **发布只前进**：发布把 persona 指针移到新修订并记录 `supersedes`；
   再次发布当前线上修订是同一次发布的重放（幂等），不产生第二条发布事实。
4. **回退可追溯**：回退不"恢复旧指针"，而是把旧内容**重放为新修订**并发布，
   父修订指向回退前的线上修订；历史因此始终线性、可端到端解释。
5. **快照钉在准备边界**：turn 在既有准备边界取一次 `{actor_id, persona, content,
   revision_id, revision_version, fingerprint, pinned_at}` 并写入 turn。
   已准备/生成/发送中的轮次保持自己的快照；排队未准备的轮次取发布后的新快照。
   发布**绝不就地改写**已存在的 turn 对象。
6. **每次模型调用前复核**：`personas.verify(role)` 重新计算指纹并比对权威修订行；
   被篡改或损坏的快照让该轮以 `dependency_unavailable` 失败，而不是照它生成。
7. **人格文本永远不是权限**：回退旧文本不会复活被撤下的角色登记；人格模块不读也不写
   角色权限、来源、绑定、发送资格、Memory 关系数据。
8. **撤权不复活**：`retire` 让该角色不再参与新的准备；已准备的轮次不受影响；
   `restore` 是唯一恢复方式。
9. **写入按操作去重**：一次写入 = 一个 `request_id` + 一个规范化请求摘要 + 一条原子记录的
   结果。重放返回原结果、不重复落事实；同键异请求被拒；过期 `expected` 仍是
   `version_conflict`（见上一节）。
10. **审批状态也在乐观并发内**：任何改变可见状态的写入（含 `approve`）都推进 persona
    版本号，因此"我看到的那一版"同时覆盖指针与审批状态。
11. **浏览是读，而且必须是有界的**：`catalog`/`history_page`/`revision`/`compare` 不带
    `request_id`，不写事实、不写账本、不推进版本、不动指针。有界是**两件事**：页由 SQL
    `LIMIT` 限定，且该查询必须走**匹配索引的范围 seek** —— 只有 `LIMIT` 而没有匹配索引时
    SQLite 会用临时 B-tree 排序并先遍历该角色的整段历史才可能返回一个短页，
    禁止把整段历史读进内存再在 Python 里切片。

## 有界浏览与版本比较（TS-076）

同一管理端口新增四个**只读**操作，`app.py`/`core.py` 不需要改：凭据、请求上限与错误映射
全部沿用既有那一套，未配置 `personas` 段时与写入一样不可用。

```json
{"operation": "catalog",      "limit": 20, "cursor": "<不透明游标>"}
{"operation": "history_page", "subject": "actor:companion", "kind": "revisions", "limit": 20, "cursor": "..."}
{"operation": "revision",     "subject": "actor:companion", "revision_id": "<sha256>"}
{"operation": "compare",      "subject": "actor:companion", "left": "<sha256>", "right": "<sha256>"}
```

| 操作 | 回答的问题 | 关键规则 |
| --- | --- | --- |
| `catalog` | 有哪些角色、当前指针停在哪 | **实时 keyset 目录**：按 subject 升序，以最后已返回的 subject 为下一页界限，响应带 `consistency="live_keyset"`；**不承诺跨页快照**（每个条目的指针是"读这一页那一刻"的值），翻页期间注册在游标之前的角色只会出现在重新打开的目录里，已返回过的角色不会重复。无模糊搜索、无任意排序、无 offset |
| `history_page` | 一个角色的某一类历史（`revisions`/`publications`/`approvals`/`rollbacks`） | 响应**自带读取基准**：`subject`、`kind`、`persona_version` 与 `consistency="version_bound"`，首/中/末/空页一律给出，末页即使 `next_cursor` 为 `null` 也照样给出；客户端不需要解码不透明游标去猜版本。响应元数据、页记录与修订/审批投影在**同一个短只读事务**内形成，之后只做序列化、字节预算与游标编码。后续请求先验证当前版本，不一致返回 `version_conflict` 并指示重开第一页；游标绑定 `subject`/`kind`/`version`/`limit`/`last_key`，跨角色、跨种类、跨操作、跨页大小一律 `invalid_input`；页内条目按 `(position, id)` 递增，无遗漏无重复 |
| `revision` | 某一条**不可变**修订的正文与它此刻的定位 | 必填 `subject` + `revision_id`，先核对所属角色才返回正文；未知修订 `not_found`、异角色修订 `invalid_input`，都不泄露别的角色正文。响应含 `fingerprint`、当前 `persona_version`、`state`、`published_revision`/`draft_revision` 与 `is_published`/`is_draft`，指针与正文同一读事务 |
| `compare` | 同一角色的两个不可变版本之间到底改了什么 | 比较域固定为 **`persona`/`tone`/`style`/`address`**（响应标 `comparison_scope`）；每字段给 `presence.left/right`、`change`（`added`/`removed`/`modified`/`unchanged`）与**完整左右值**；缺字段与 `null` 明确区分。正文允许的其他 scalar 字段只按**规范化摘要**比较（`additional_fields_present`/`additional_fields_changed` + 有界字段名列表），不出泛化 diff |

- **身份不由四个字段推断**：`identical_revision` 只在两个 `revision_id` 相同时为真，
  `content_identical` 只按全正文 `fingerprint` 相同为真。四个字段都没变但扩展字段变了时，
  响应明确给出 `additional_fields_changed=true`，绝不报告"人格完全相同"。
- **只陈述差异**：不调用任何模型、不做质量打分、不自动批准；审批与指针变化不改写不可变正文。
  草稿/批准/发布/回退仍然只能由原来的写操作完成，客户端用读回的 `persona_version` 与
  `revision_id` 走既有写链。
- **字节预算**：页响应按**实际 UTF-8 JSON 字节**上限 256 KiB，单条修订与比较上限 1 MiB。
  达预算只返回**已经完整编码**的条目并给准确 `next_cursor`（`has_more` 同时为真），
  单条自身超预算明确 `invalid_input`，绝不发不完整 JSON、也不"截正文后当完整"；
  `compare` 超限只拒绝不截断，两端修订可用 `revision` 分别读全。
- **游标**：不透明、最多 2048 字符、用**进程随机密钥** HMAC 签名。**结构先于密码学**：
  整串必须是 ASCII，且是「一段非空 base64url + 一个 `.` + 一段非空 base64url」，任何
  非 ASCII（如 `中.x`、`abc.中`）、控制字符、空白、填充 `=`、第二个点、越界长度都在解码、
  签名与比较**之前**就被判为 `invalid_input`；因此畸形游标永远是有界拒绝，不会变成
  `UnicodeEncodeError`/`TypeError` 这类领域无法映射的异常，也不会在线上变成 500。
  篡改、跨 subject/kind/operation/limit 复用同样 `invalid_input`；密钥不落库、不新增存储表，
  因此**进程重启后旧游标失效**，重开第一页即可。游标不携带任何正文或凭据。
- **匹配索引与范围 seek**：四类历史各有一个派生索引
  `persona_<kind>_page(conversation_id, position, id)`（在 `store.py` 既有 persona 索引区
  `IF NOT EXISTS` 创建，已存在的 v9 库下次打开即补上，事实/字段/`user_version`/锁/事务都不变）。
  页查询用**行值**做键集续读：`WHERE conversation_id=? AND (position,id)>(?,?) ORDER BY position,id LIMIT ?`。
  行值同时覆盖"位置更大"与"位置相同而 id 更大"两种情形，且与索引键完全匹配，因此查询计划是
  `SEARCH <表> USING INDEX persona_<kind>_page (conversation_id=? AND (position,id)>(?,?))`，
  **没有** `USE TEMP B-TREE FOR ORDER BY`：深页直接 seek 到游标处，不会从头扫过先前记录。
  目录同理：首屏按主键索引顺序取 `LIMIT` 条即停，后续页 `SEARCH ... (id>?)`，无排序、无 offset。

```bash
python -m tianshu_companion.persona_cli --database .runtime/companion.db --limit 20 catalog
python -m tianshu_companion.persona_cli --database .runtime/companion.db \
    --subject actor:companion --kind revisions --limit 20 history_page
python -m tianshu_companion.persona_cli --database .runtime/companion.db \
    --subject actor:companion --revision <sha256> revision
python -m tianshu_companion.persona_cli --url http://127.0.0.1:8765 \
    --subject actor:companion --left <sha256> --right <sha256> compare
```

四个命令在离线与在线两种形状上完全一致，都是一次 `Personas.manage` 调用；`persona_cli`
里没有任何 SQL 或表名。管理端口的凭据规则不变：只认 `personas.admin_token_env` 指定的
那个凭据，普通 platform/bridge/聊天凭据读不到也写不了。

## 数据模型

| 表 | 作用 |
| --- | --- |
| `persona_personas` | 每个角色的当前指针：`published_revision`、`draft_revision`、state、`imported`（部署声明的版本，仅作待决身份）、`version`（乐观并发用） |
| `persona_revisions` | 不可变修订：`content`、`fingerprint`、`parent`、`source`、`operator`、`sequence` |
| `persona_approvals` | 仅追加的批准/拒绝决定（同一修订后一条决定覆盖前一条） |
| `persona_publications` | 仅追加的发布事实：`revision_id`、`supersedes`、`kind`(seed/publish/rollback)、`operator`、`reason` |
| `persona_rollbacks` | 回退溯源：`target_revision`（被回退到的旧修订）与 `restored_revision`（新修订） |
| `persona_imports` | 按部署文档指纹的导入游标（同一部署重放为 `skipped`） |
| `persona_access` | 读取授权登记（谁可以读某个角色的历史） |
| `persona_operations` | **操作账本**：一次写入的身份（`scope` + `request_id` + `operation` 的摘要、主键）它绑定的规范化请求摘要，以及首次执行时提交的结果文档；与业务写入同事务，重启后仍可重放 |

派生索引（TS-076 返修新增，只在既有 persona 索引区 `IF NOT EXISTS` 创建，不改任何表/字段）：

| 索引 | 作用 |
| --- | --- |
| `persona_revisions_page` / `persona_publications_page` / `persona_approvals_page` / `persona_rollbacks_page` | 各 `(conversation_id, position, id)`：让 `history_page` 的键集范围 seek 与 `ORDER BY position,id` 都由索引直接满足，避免临时 B-tree 与整段历史遍历。既有 `persona_*_queue(conversation_id,status,position)` 与 `persona_approvals_revision` 等索引保留不动 |

## 部署配置

```json
{
  "config_version": 12,
  "roles": {
    "actor:companion": {"version": 3, "persona": "温和、简洁。没有来源时坦诚说明无法核对往事。"}
  },
  "personas": {"admin_token_env": "TIANSHU_PERSONA_ADMIN_TOKEN"}
}
```

- `roles` 是角色声明处（`personas.deployment` 是唯一形状规则）；`config_version` 是导入的
  `source_ref`，同一文档重复导入为 `skipped`。
- 部署里声明的 `version` 只被记录为 `imported`，**永远不会覆盖已发布内容**：
  线上已有修订时，配置变更只能由操作者草稿 + 批准 + 发布。
- 未设 `personas` 段 ⇒ 人格管理整体不启用（没有 `Personas` 实例，管理端口返回 503），
  旧行为不变。

## 数据库与迁移

数据库 `user_version=9`。打开低于 v9 的库且文件不是 `:memory:` 时，先做一次完整视图
（含 WAL）的 SQLite backup，再在同一事务内创建缺失的 `persona_*` 表（含操作账本）与索引
并提升版本号；失败时回滚 DDL 与版本、释放 owner 锁。备份标签取**本次打开所跨越的最高结构
步骤**：从 v8 打开得到 `<database>.pre-persona-ops-v9-<random>.bak`，从 v7 打开得到
`<database>.pre-persona-v8-<random>.bak`（v8 是它跨越的最高人格步骤）。
既有 `pre-source-v2`/`pre-life-v3`/`pre-images-v4`/`pre-writing-v5`/`pre-proactive-v6`/
`pre-routing-v7` 流程不变，旧对话事实与 `source_head` 不变。

四个派生分页索引走的是**每次打开都会执行的既有索引创建路径**（不是迁移步骤）：因此一个
**已经是 v9 的库**下次打开就会补上它们，不产生备份、不改 `user_version`、不动任何事实，
重复打开是幂等的。

**跨多个版本的一次升级只取一份恢复备份**（落在最高的那个结构步骤上，例如
v5→v9 得到 `pre-persona-ops-v9`，不会额外为 v6/v7/v8 再各写一份）。这是本产品既有备份流程的
既有语义（每次打开最多一份、标签取本次跳升的最高步骤），本轮复用而未改动；
需要某一中间版本的可恢复点时应从对应版本分步升级。

恢复必须先停 Core、确认无 owner、隔离保存当前 DB/WAL/SHM，再复制所选完整备份到目标路径；
禁止运行中覆盖数据库或单独拼接旧 WAL。本次验证只用临时合成数据库，未操作生产数据。

## 本轮未做（边界）

- 没有平台网页、没有新跨产品合同、没有真实模型费用或真实渠道发送。
- 人格管理不读 Memory 数据库、不读其他产品数据库、不写 Memory 关系数据。
- 跨进程并发不在范围内：`Store` 是单进程所有者（owner 锁），乐观并发在**进程内**
  由 `expected` CAS 保证；两个 OS 进程同时写同一库由 owner 锁整体拒绝。
- 人格文本不保证文案质量、语气或人设一致性；本轮不做模型改写、不做人格评分。
- TS-076 的浏览与比较是**同产品管理面**，不是面向浏览器的账户体系：没有普通网页账户、
  没有 CORS 授权、没有伪造的平台登录，也不把请求里的 `scope` 当授权事实。未来平台页面
  需要另行冻结同源连接器、身份映射与页面位置，本卡不设计。
- TS-076 没有新增表或迁移：只新增四个**派生**分页索引（`store.py` 既有索引区，
  `IF NOT EXISTS`），不改事实/字段/`user_version`/备份规则/锁/事务。
- 读操作的拒绝只表达"这次读不成立"，不承诺重试语义：`version_conflict` 需重开第一页，
  `invalid_input` 需修正游标或参数。
- 有界性的证据是**查询计划与 SQLite VM 步骤数**，不是语句条数：语句条数恒定**不能**证明
  行扫描有界（同一份语句在没有匹配索引时照样遍历整段历史）。因此文档与测试都不再以
  "语句数固定"作为有界性论据。

## 实际验证

- `python -m pytest tests/test_persona_queries.py tests/test_personas.py tests/test_persona_chain.py -q`：
  领域与用例、快照时序、损坏快照、跨角色隔离、回退、撤权、恢复报告、边界形态、
  迁移与失败恢复，**操作身份**（重放返回记录结果且不重复落事实、同键异内容被拒、
  缺身份被拒、批准重试不追加决定、过期版本不能靠身份变成成功、身份跨重启与跨授权域），
  以及 TS-076 的**有界浏览与比较**：纯规则（页大小、游标绑定/篡改/**非 ASCII 与控制字符
  畸形游标**、字节预算、四字段比较）、目录逐页无遗漏无重复、游标跨 subject/kind/operation/limit
  被拒、版本变化后旧页 `version_conflict`、**首/中/末/空页都自带 subject/kind/persona_version/
  version_bound 基准**、单修订与跨角色隔离、中文/换行/相同版/多字段/缺字段/回退派生版/大正文比较、
  读操作前后事实与账本计数不变，以及**四 kind 首/末页的 `EXPLAIN QUERY PLAN` + 深页 VM 步骤数**
  与**已存在 v9 库补建索引的幂等性/事实不变**。
- `python -m pytest tests/test_persona_chain.py -q`：**真实 CLI 子进程**（独立解释器 +
  隔离数据库）走完 draft→approve→publish→history，服务持锁时拒绝写入，
  重发同一 `--request-id` 命中账本、换内容复用同一 id 被拒；
  **真实回环服务**上的管理端口 + 独立凭据 + 草案/批准两次重放 + 离线复读同一状态；
  TS-076 的**全链**：真实 uvicorn 进程上 catalog→history_page→revision→compare→
  draft/approve/publish→新版读取→rollback→再次比较→旧游标被拒，普通 platform/bridge
  凭据与无凭据请求在同一真实服务上被拒（403/401），停机后第三个解释器核对库内只有四次写入；
  以及**真实 HTTP 上的畸形游标回归**：非 ASCII payload/tag、控制字符、错误形状、越界长度、
  非字符串与跨请求复用一律 `400 invalid_input`（不是 500），同一进程继续服务且有效游标照常续页。
- `python -m pytest tests/test_boundaries.py -q -k PersonaBoundary`：职责与依赖结构断言
  （含 `persona_queries.py` 不含表名/不含 SQL/不依赖 store·personas·core·app，
  `personas.py` 只一处 `from .persona_queries import`，带 `LIMIT` 与行值 seek 的页查询在
  `personas.py`，派生分页索引与使用它的 seek 必须同时存在）。
- `python -m pytest -q`：完整组件套件见交接记录。
- `python -m ruff format --check src integrations tests scripts`、
  `python -m ruff check src integrations tests scripts`、
  `python -m compileall -q src integrations tests scripts`：通过。
- `.runtime/dsh-delivery/verify_read_bounds.py`：四 kind 首/末页查询计划、目录计划、
  深页 VM 步骤数（16 条 vs 512 条历史）与"无索引的旧形状"对照，输出为脱敏日志。

全部使用合成人格文本、合成角色、临时 SQLite 与虚构标识。
