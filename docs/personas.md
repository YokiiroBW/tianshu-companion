# 角色人格版本管理与对话快照（TS-075）

本模块把"角色人格"从一个可随手改写的配置字段，变成**版本化、需显式批准、可追溯回退、
且每轮对话都钉住不可变快照**的受管数据。人格文本永远不是权限、不是来源、不是模型绑定，
也不是发送资格——它只是一段被钉住的内容。

新增模块 `tianshu_companion.personas`（领域 + 应用用例）与
`tianshu_companion.persona_cli`（本地/在线维护适配器）；`core.py` 只在既有"准备边界"
取一次快照，`store.py` 只加表与迁移，`app.py` 只加一条受鉴权的管理路由。

## 职责与依赖表（高内聚低耦合符合性）

| 单元 | 拥有的职责 | 只允许依赖 | 明确不拥有 |
| --- | --- | --- | --- |
| `personas.Personas`（领域+用例） | 草稿/修订不可变、批准、发布指针、回退为**新修订**、撤权、导入游标、快照钉取与校验、`manage(request)` 唯一应用入口 | `Store` 的通用连接/事务/读写、`digest`/`canonical` | 角色权限与登记、来源登记、模型绑定、发送资格、Memory 关系数据、HTTP、CLI 参数、Direct 发送顺序 |
| `core.Core` | 在**既有准备边界**调用 `personas.pin(actor)` 取快照并把 `config_version` 写进 turn；每次模型调用前 `personas.verify(role)`；把 `PersonaError` 映射为本模块既有 `Fault` | `personas` 的四个公开方法：`pin`/`verify`/`recover`/`manage`/`import_config` | 草稿、批准、发布、回退、历史等业务规则；persona 表名（`test_boundaries.py` 结构断言禁止） |
| `persona_cli`（适配器） | 参数解析 → 一个操作文档 → `Personas.manage`；离线用 `--database` 自持 owner 锁，在线用 `--url` 走管理端口 | `personas.manage`、`personas.deployment`（部署形状唯一规则）、`Store` | 任何人格规则、任何表名、任何直接写入、绕过单所有者 |
| `app.create_app`（HTTP 适配器） | 只做鉴权 + 分发：`POST /internal/v1/persona/manage` → `core.manage_persona(service, body)` | `core.manage_persona` | 业务规则（`manage_persona` 本身只是"人设是否启用/是否 `persona_admin`"两件事的适配器） |
| `store.Store` | 连接、事务、owner 锁、结构迁移与恢复备份 | `sqlite3` | 人格语义；`PERSONA_TABLES` 只出现在 `store.py` 的表清单里 |
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
})
```

离线维护（单所有者保护）：

```bash
python -m tianshu_companion.persona_cli --database .runtime/companion.db list
python -m tianshu_companion.persona_cli --database .runtime/companion.db \
    --subject actor:companion --operator admin:1 --reason "语气调整" --expected 3 \
    --content persona.json draft
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

数据库 `user_version=8`。打开低于 v8 的库且文件不是 `:memory:` 时，先做一次完整视图
（含 WAL）的 SQLite backup 到 `<database>.pre-persona-v8-<random>.bak`，再在同一事务内
创建 7 张 `persona_*` 表与索引并提升版本号；失败时回滚 DDL 与版本、释放 owner 锁。
既有 `pre-source-v2`/`pre-life-v3`/`pre-images-v4`/`pre-writing-v5`/`pre-proactive-v6`/
`pre-routing-v7` 流程不变，旧对话事实与 `source_head` 不变。

**跨多个版本的一次升级只取一份恢复备份**（落在最高的那个结构步骤上，例如
v5→v8 得到 `pre-persona-v8`，不会额外为 v6/v7 再各写一份）。这是本产品既有备份流程的
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

## 实际验证

- `python -m pytest tests/test_personas.py -q`：领域与用例、快照时序、损坏快照、
  跨角色隔离、回退、撤权、恢复报告、边界形态、迁移与失败恢复。
- `python -m pytest tests/test_persona_chain.py -q`：**真实 CLI 子进程**（独立解释器 +
  隔离数据库）走完 draft→approve→publish→history，服务持锁时拒绝写入，
  以及**真实回环服务**上的管理端口 + 独立凭据 + 离线复读同一状态。
- `python -m pytest tests/test_boundaries.py -q -k PersonaBoundary`：职责与依赖结构断言。
- `python -m pytest -q`：完整组件套件见交接记录。
- `python -m ruff format --check src integrations tests scripts`、
  `python -m ruff check src integrations tests scripts`、
  `python -m compileall -q src integrations tests scripts`：通过。

全部使用合成人格文本、合成角色、临时 SQLite 与虚构标识。
