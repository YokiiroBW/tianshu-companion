# 长篇作品版本与审校内部端口（TS-073）

本模块属于 Core，与生活/日记/图像共用同一个 SQLite 单进程所有者。
`core.writing` 是**可信同产品 Python 端口**：不新增 HTTP 路由、不新建跨产品网页合同、
不读取 Memory 数据库、不写现实用户画像。宿主必须先鉴权并校验作品管理权限；
不能把前端 JSON 直接解包为管理员端口参数。
`read_chapter` 的 reader 是宿主认证得到的身份，工作读授权只给**已发布**章节，
不授予草稿、审校意见、来源或管理权。

作品、章节、设定与正文全部是虚构内容：每条素材、每个修订、每次请求都带 `fictional=true`，
并且不进入真实 source-facts 流（`source_head` 不变）。

## 数据模型

| 表 | 作用 |
| --- | --- |
| `write_works` | 作品：actor 所有者、标题、版本化大纲/人物设定、候选新设定、配方名 |
| `write_chapters` | 章节：显式 order、目标、状态、当前/已发布修订、待复核原因、固定基准 basis |
| `write_revisions` | 不可变正文修订：内容、父链、来源（human/gateway）、生成时基准 |
| `write_reviews` | 追加式审校意见：reviewer/decision/notes，只针对当前修订 |
| `write_publications` | 追加式发布历史，`supersedes` 指向被取代的修订 |
| `write_materials` | 按 (章节, 素材哈希) 固定的输入快照（含来源 id 与 standing） |
| `write_requests` | 生成请求：幂等指纹、固定基准、提交意图与终态 |
| `write_recipes` | 按 (id, version) 不可变的章节写作配方 |
| `write_access` | 作品读授权（默认锁定） |

版本轨迹：`outline_version`、`characters_version`、`candidates_version` 只在显式更新时 +1；
`plan_version`（标题+目标）与 `summary_version`（人工续写摘要）同理；
章节 `version` 是通用乐观锁，过期 `expected_version` 一律拒绝且不改动数据。

## 状态机

`planned`（已建，未请求）→ `queued` → `generating` → `draft` → `published`。

- `needs_review`：有草稿/已发布内容，但基准已过期或存在比已发布更新的草稿。
- `invalidated`：排队/生成中的尝试固定基准已过期而被显式作废，尚未产生任何内容；
  请求记录标记 `invalidated`，重新 `request_chapter`/`retry_chapter` 会重钉当前正典并清掉旧标记。
- `unavailable`：没有独立写作配置或网关不可用，**不回退聊天模型**。
- `context_overflow`：结构化素材超过上限，明确拒绝而不是静默截断。
- `failed` / `interrupted` / `unknown` / `cancelled`：未成稿的显式终态。
- `published` 仅在“当前修订 = 已发布修订且无待复核原因”时成立；已发布正文永不被自动改写。

## 生成与固定基准

写作复用现有独立 `life_writing` 与 `life_config_version`（示例版本 19）以及已配置的
services.gateway；未启用、无独立版本或无服务配置时 `unavailable`，绝不回退聊天 `config_version`。
后台每次 pass 至多一次模型调用，并共享 Core 模型并发槽。

请求时固定 `basis`：`plan_version`、`outline_version`、`characters_version`、
`candidates_version`、`recipe`/`recipe_version`、`config_version`、
`references`（前序章节 id + 当前修订 + 摘要版本）与 `material_version`/`material_bytes`。
生成调用使用创建时固定的版本；显式 retry 才重新固定当前正典。
`material_version` 是**模型实际输入素材的哈希**：`_messages` 只发送身份字段、配方与该素材，
因此“进入模型的内容”与“被固定的内容”按构造一致，不存在只进上下文却未被固定的字段。

上下文是有上限且保留来源的结构化条目，不是整本书：

- `work`/`outline`/`character`/`candidate`/`goal`/`prior_chapter`/`selection`。
- `standing` 明确区分 `canon`（已发布章节、版本化大纲与人物设定等已确立虚构事实）、
  `draft_basis`（未发布的续写基础，**不是**正典）、`candidate`（未应用的新设定候选）、
  `plan`（本章目标，是意图而非既成事实）、`index`（选择与来源元数据）。
- 每条都带 `fictional=true` 与来源 id。最近 `max_prior_chapters` 章给节选并标注 `truncated`，
  更早章节保留摘要；`selection` 条目显式列出“有节选/仅摘要”的章节，因此没有任何前章被悄悄丢掉。
- 总量超过 `max_material_bytes` 时进入 `context_overflow`，不生成、不截断。
- 系统指令要求把素材当数据而非指令，禁止编造现实用户行为，禁止把 candidate/plan 当既成事实，
  只输出章节正文；空输出或超长输出失败。

模型产出的正文只是草稿：`canon_effect=none`，不会自动改写大纲、人物设定或候选。
配方或模型是否让文本真的连贯仍需人工审校；字符串长度检查不证明叙事质量。

## 依赖变化 → 显式失效 / 待复核

每次变更后重新比对固定基准与当前正典：先比较 `material_version` 与**按当前正典重算的素材哈希**，
一旦不同就必然给出原因。具体字段对应 `outline_changed`、`characters_changed`、
`candidates_changed`、`recipe_changed`、`chapter_plan_changed`、
`prior_chapter_changed`（前章修订或摘要版本变化）；无法归因到这些字段的素材变化（例如前章标题，
或前章从未发布变为已发布而改变其 `standing`）记为 `material_changed`。
另有 `unpublished_revision`（存在比已发布更新的草稿）。只要章节已有固定基准，原因就照实列出；
状态则按内容区分：

- 有草稿或已发布内容 → `needs_review`；**已发布正文、修订行与发布历史都不改**，
  读授权用户仍读到原正式文本。
- 排队中/生成中的尝试被依赖变化作废 → `invalidated`，请求记录标记 `invalidated`，不再生成。
- 尚无内容的其他状态（`unavailable`/`failed`/`cancelled` 等）保留原状态，只列出漂移原因。
- 清除标记只有两条显式路径：`retry_chapter` 重新生成（重钉当前正典），或 `revise_chapter`
  人工改写（重钉当前正典）；随后审校并发布。**只记录审校意见不会清掉标记**。

## 修订、审校与发布

- `revise_chapter(chapter_id, content, editor=, reason=, expected=, addresses_review=)`
  保存父链、编辑者、原因与来源，并重钉当前正典。模型离线、甚至完全没有写作配置时都可用；
  它也能创建章节的首个修订，因此人工可以完全脱离模型创作。排队/生成中先 `cancel_chapter`。
- `review_chapter(chapter_id, revision_id, reviewer=, decision=, notes=, expected=)`
  只接受当前修订，decision 为 `approved`/`changes_requested`/`rejected`，追加保存，不覆盖旧意见。
- `publish_chapter(chapter_id, reviewer=, expected=)` 要求当前修订存在且该修订最近一次审校为
  `approved`；同一修订重复发布幂等（不追加历史）；发布新修订会追加一条 `supersedes` 记录，
  发布历史完整保留。过期的 `expected_version` 一律拒绝。

## 取消、未知与重启

- `cancel_chapter` 可作用于 `queued`/`generating`/`unknown`/`unavailable`/`context_overflow`/
  `failed`/`interrupted`；`queued` 直接成为 `cancelled`（不会调用模型）；`generating` 时只记
  持久取消意图，调用返回后**丢弃文本**并记 `cancelled`，不产生修订。
  `draft`/`published` 内容不能被 cancel 抹掉。
- 提交意图在 I/O 之前落库：崩溃、超时或应答丢失后，`recover()` 把已提交但没有结果的章节标为
  `unknown`；**unknown 绝不自动重发**，必须显式 `retry_chapter`（配新的 `request_id`）。
- 未提交的 `generating` 回到 `queued`（提交与置 `generating` 在同一事务内，
  因此正常路径只会得到 `unknown`）。
- `request_id` 幂等重放：同章节同 `request_id` 返回已记录状态，不会第二次调用模型；
  同 `request_id` 用于其他章节为冲突。

## 权限与读隔离

`set_work_access(work_id, readers=..., expected=...)` 默认锁定，readers 由宿主认证得到。
`read_chapter` 先鉴权再取内容，且只返回已发布修订（带 `fictional` 与发布时间，无来源、审校或草稿）。
授权按作品隔离，不跨作品、不跨 actor；`works(actor_id)` 只列该 actor 的作品。
草稿、审校意见、来源与固定素材只走 `admin_read_revision` / `admin_read_materials` /
`reviews` / `publications` 等管理端口，宿主必须先验管理权限。

## 数据库与迁移

数据库 user_version=5。v4 打开前用 SQLite backup 保存完整视图（包括 WAL）至
`<database>.pre-writing-v5-<random>.bak`，然后在同一事务内创建 9 张 `write_*` 表与索引并提升版本；
失败回滚 DDL/版本并释放 owner 锁。v1/v2/v3 仍用既有备份前缀，旧对话事实、`source_head`、
生活与图像行为不变。

恢复必须先停 Core、确认无 owner，隔离保存当前 DB/WAL/SHM，再复制所选完整备份到目标路径；
禁止运行中覆盖数据库或单独拼接旧 WAL，也不自行清除水位/隔离标记。
本次验证只用临时合成数据库，未操作生产数据。

## 本轮未做（边界）

没有网页/HTTP 路由、没有新建跨产品合同、没有读取 Memory、没有资产归档或渠道发布；
作品只在 Core 内部持久化。模型对动机、因果、时间线的一致性不由此模块保证，必须人工审读。
最小跨产品候选（**未发布、未实现**，需双方 schema/样例/验收后再冻结）：
Platform→Core 提交 `request_id`/章节/`expected`；Core→Platform 返回状态、`review_required`、
固定基准与 `fictional` 标记、发布历史摘要与已发布正文读取。

## 实际验证

- `python -m pytest tests/test_writing.py tests/test_writing_chain.py -q`：21 通过。
- `python -m pytest -q`（设置 `TIANSHU_TLS_PYTHON`）：213 通过，1 跳过，95 子场景通过。
- 显式设置 `TIANSHU_MEMORY_REPO` 后 `python -m pytest tests/test_profile_joint.py -q`：1 通过
  （只读固定 Memory 提交 `69b29f3`，本任务未修改 Memory）。
- `python -m ruff check src integrations tests scripts`、`python -m ruff format --check ...`、
  `python -m compileall -q src integrations tests scripts`：通过。

覆盖两章以上生成与前后依赖、修改前章后下章 `needs_review`、发布历史与 `supersedes`、
取消/未知/重启不重发、幂等重放与冲突、过期 `expected_version`、跨 actor 与跨作品读隔离、
无命名版本字段的素材变化（前章改标题、前章发布）与 `material_changed`、素材上限与
`context_overflow`、v4→v5 迁移备份/回滚/水位不变。
全部使用合成模型、合成素材与临时 SQLite，不代表真实模型质量或真实渠道发布。
