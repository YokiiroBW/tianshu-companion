# 角色生活与日记内部端口（TS-071）

本模块属于 Core，同一个 SQLite 单进程所有者维护共享世界、房间、各角色生活和日记。
`core.life` 是**可信同产品 Python 端口**；不向现有 HTTP dispatch 开放。
调用宿主必须先鉴权、校验 actor 管理权限；不能把前端 JSON 直接解包为管理员端口参数。
虚构好感不授予管理权。真实设备、图片生成、模型资产和跨产品网页接线不在这里实现。

## 初始化与时间

宿主在已获得的 Core 实例上调用 `create_world`、`create_room`、`configure_actor`。
例如（均为虚构标识，需在授权宿主内执行）：

```python
life = core.life
life.create_world("world:home", timezone_name="+08:00", setting="虚构小屋")
life.create_room("room:study", "world:home")
life.configure_actor(
    "actor:a", "room:study", personality_version=1, outfit_ref="outfit:reading-v1",
    schedule=[
        {"minute": 0, "activity": "sleeping", "controls": {"desk_light": 0}},
        {"minute": 420, "activity": "reading", "controls": {"desk_light": 1}},
        {"minute": 1320, "activity": "sleeping", "controls": {"desk_light": 0}},
    ],
)
snapshot = life.snapshot("actor:a")
```

时刻均为 UTC epoch seconds，日程是配置时区的 civil minute。支持 UTC、明确固定偏移
（如 +08:00）及宿主具备 tzdb 的 IANA 名称；Windows 无 tzdb 时 IANA 配置报错，绝不偷偷改为 UTC。
IANA 夏令时跳过不存在的阶段；重复小时按同一 civil phase 去重。不是天文/实时天气数据。
`update_world` 需要 expected 版本，变更世界设定/时区后重新结算；已获知事件的归属日保留原时区，
不随以后时区变更重写历史。

角色引用人格版本、当前活动、短期心情和稳定 outfit_ref；图片引用不代表已经生成图片。
同一房间所有角色读取同一份 controls。每项包含 value、from_value、mode、hold_until、changed_at；
window/sheer/curtain/desk_light/ceiling_light 都是 0..1 的虚构语义状态。
逐帧插值在浏览器，服务器只给变更起点。snapshot 读取真实当前时刻；没有能触发业务的时间预览入口。

`set_room(..., expected=version)` 默认为手动保持：hold_until=None 为永久保持，显式 UTC 到期后恢复自动。
`resume_room(..., keys, expected=version)` 显式恢复指定控件。
角色活动也支持 `set_activity` / `resume_actor`。过期 expected 拒绝，避免两个浏览器丢失更新。
自动房间冲突规则为 actor id 升序，后者对同一控件的建议优先；手动保持总是优先。

现有 Core tick 每秒至多结算一次生活；生活工作器每2秒处理有界生成任务及昨日记草稿。
网页关闭不影响运行。重启直接落到当前活动，每个角色至多写一条当前结算事件，不模拟一夜动画。
同一日程 phase 的事件幂等；事务失败时下次重算，状态和获知关系一同提交。
时间回退不补演旧阶段。服务须维持可靠系统时间；无后台服务运行期间不会逐刻生成经历。

## 世界事件与素材

`record_event` 只接可信模拟生产者的虚构事件，包含 participants、visible_to 和 UTC occurred_at。
参与者/可见者获知，其他角色不会因共享世界自动知道。`tell(event_id, source_actor, recipient)`
要求说者已知该事件，记录 told_by 与学习时刻；不是任意角色读取全部世界历史。
首次获知按当时本地日期归档，同一事件/角色只记录一次。

素材只读取该角色当日 life_known 对应的完整事件，最多 64 条、12,000 UTF-8 字节。
超过边界明确 material_overflow，不能把静默截断的清单说成完整素材；不足配方 min_events 为 skipped。
没有读取 turns、physicals、Memory 私密表的日记路径。现实聊天缺当前来源/访问证明，明确 excluded，
因此当前能力**不是现实对话总结日记**。虚构 life 表不会增加真实 source-facts 水位或发 Memory 事实事件。
角色聊天只附带小份当前 fictional_life 摘要，共用原有 16 KiB 上下文预算，装不下整体省略；不附事件史。
未配置生活角色时原聊天上下文结构不变。

## 独立模型配置与草稿

### 自主生活（DQ06/07/08）

Core启动恢复与RoleRuntime启用自动登记缺失生活实体；新角色使用中文基础作息与
`life_timezone`（默认`+08:00`）。已有世界、日程、房间及手动保持不被覆盖。角色启用与对话能力
分别管理：没有对话、QQ绑定或打开网页，服务仍按时间结算；停用暂停，重新启用对齐现在。
停机跨过的阶段标记`skipped`，跨日先完成前日计划，不补造过去的经历。

每日计划与阶段体验复用现有Gateway和模型并发槽，任务持久化在metadata，成功体验进入原有
life_events→life_known→素材/日记链。计划生成选择符合人格的活动名与具体内容，保持既定civil分钟；
阶段细化独立执行，计划失败不阻塞阶段。每pass最多一个计划、一个阶段与一次意图提取，时钟只结算一次。
人格正文通过Personas.pin捕获、verify复核；不是仅向模型传版本号。未配模型仍有基础作息，生成状态
明确`unavailable`；只有成功Gateway回包才能标记`completed`及`generated_by=gateway`。

部署未写`life_writing`时，自主生活生成默认开启；显式`false`会关闭所有生活AI调用。优先使用现役
Provider默认模型选择服务，按角色在管理页选定的模型取得发布版本租约，不依赖dialogue开关，也不另建
一份模型配置。未部署选择服务时使用`life_config_version`，或已有`config_version`默认绑定；Gateway
未配仍不可生成。wire workload沿用已发布的`companion.text`；选择scope为角色独占的
`life:<actor摘要>` / `person:life:<actor摘要>`、`self_private`，不代表真实对话/人物。
任务调用前固定版本与有效租约，返回后再校验租约；Gateway仍验证原生响应与相应route_receipt。
有明确结果的失败至多三次退避；未知网络结果、取消、重启中断不会自动重发，可信宿主可用
`retry_generation(task_id, expected=version)`显式重试当前任务。

已受理、归属该角色的对话可影响当天当前/后续活动内容，不能成为建立计划或推进时钟的门槛。
现役来源与受众授权在意图提取前后复核，Gateway把开放主题/建议/约束提取为角色自己的虚构活动意图，
比如研究天体物理纪录片；未配模型时有限兴趣词可作为基础影响。内部保留来源引用用于撤回，生活事件、
计划与日记不复制私聊原文或声称真实用户行为。来源更新/撤回取消旧意图和在途计划/阶段回包；历史已发生
体验保持不变。每天意图和模型上下文有界，跨日不把临时兴趣永久写为人格。

以下旧日记/长篇草稿的显式独立配置规则保留：自主生活默认生成不会暗中启用长篇或自动发布日记。

部署显式设置 `life_writing: true` 和独立 `life_config_version: 19`（示例版本，必须实际发布）；
还必须配置现有 services.gateway URL/凭据。未启用、无独立版本或无服务配置均 unavailable，绝不回退
config_version 的聊天默认路由。不在源码填模型或供应商凭据。

发布 model 合同目前把 workload 固定为 companion.text，故写作请求仍使用该 wire 名称，
**独立引用已发布配置版本**中的绑定来指定写作模型。任务保存创建时版本，执行时固定使用该版本；
显式 retry 才重新选择当前写作配置。Gateway 返回真实原生响应并独立核验 route_receipt；
未知/撤销配置由 Gateway 拒绝，不生成固定成功文本。候选扩展为 companion.diary workload，
需 Platform/Gateway/Core 联合冻结，当前未改发布合同。

`put_recipe` 保存不可变的命名/版本配方：素材、视角、风格、禁止编造事实、篇幅、最低素材与质量约束。
每日后台只请求昨天，长停机不补全月；显式 `request_diary(actor_id, day)` 也可处理选定日。
同 actor/date/recipe/material hash 请求幂等；每个后台 pass 至多调用一次模型，并共享 Core 模型并发槽。
材料以数据提供并明确 fictional，系统指令禁止捏造真实用户行为；空或超长输出失败。
模型是否遵守事实/文风的语义质量仍需人工检查，不宣称字符串长度检查证明内容真实。

生成进入 draft，绝不自动发布。`revise_diary` 要求 editor/reason/expected，保存 parent 和来源；
`publish_diary` 由已获系统权限且完成内容审查的宿主调用，要求 reviewer/expected。
同素材任务只发布一次；之后编辑仍可保留新草稿，但不会覆写原正式版本。
中断任务变 interrupted，服务/模型故障为 unavailable 或 failed，显式 retry 后才重试；
避免不明网络结果导致后台重复计费。route_receipt 和修改来源只用于内部审查。

## 读锁

`diary_metadata` 不含正文或素材。它另外给出捕获标记：`fictional`（生活日记始终是虚构条目）、
`config_version`（创建时固定的写作配置版本）、`material_version`（素材哈希）以及
`recipe_id`/`recipe_version`。`read_diary(..., reader=...)` 的正式内容同样带 `fictional` 和
`material_version`，读者能判定这条内容对应哪次素材快照，不会把旧草稿当成当前状态。
`set_diary_access(actor_id, readers=..., expected=...)`
是 Core 管理的剧情读授权；readers 是宿主认证得到的身份，不接受用户自报身份作为权限。
默认锁定。`read_diary(..., reader=...)` 在读取 revision 内容前检查授权和正式版本存在，
有剧情授权也不能看草稿。`admin_read_revision` 是单独的系统管理员端口；宿主必须先验管理权限。
当前没有网络日记 API，前端不会先下载全文再盖锁。

## SQLite 迁移与恢复

数据库 user_version=3。v2 打开前使用 SQLite backup 保存完整视图（包括 WAL）至
`<database>.pre-life-v3-<random>.bak`，然后一个事务创建生活表/索引并提升版本。
v1 仍沿用 pre-source-v2 备份前缀，包含升级前原始事实；保留原来源迁移流程。
生活表与旧事实表分开，迁移不重写旧对话事实和 source_head。失败回滚生活 DDL/版本、释放 owner 锁。

恢复必须先停 Core，确认无 owner，再把当前 DB/WAL/SHM 隔离保存，复制所选备份到目标 DB 路径，
重启后允许重新迁移。禁止运行中覆盖 DB 或单独拼接旧 WAL；备份可能含私密事实，按数据库同级保护，
不入库。这里只用临时合成数据库验证，未操作生产数据。

实际验证命令：
- `.venv/Scripts/python.exe -m pytest tests/test_life.py tests/test_life_chain.py -q`
- `.venv/Scripts/python.exe -m ruff format --check src integrations tests scripts`
- `.venv/Scripts/python.exe -m ruff check src integrations tests scripts`
- `.venv/Scripts/python.exe -m compileall -q src integrations tests scripts`
- 设置 TIANSHU_TLS_PYTHON 后 `.venv/Scripts/python.exe -m pytest -q`

## 跨模块贯通（TS-070）

`tests/test_life_chain.py` 用一个合成场景覆盖完整链：共享世界事件 → 各角色分别获知
（participated/witnessed/told_by）→ 当日活动与手动保持 → 穿搭快照 → 日记素材和草稿 →
图像任务快照，并包含重启后的证据。断言要点：

- 只有参与者/可见者/被讲述者获知；未获知角色既不能转述，其素材与日记请求里也不出现该事件。
- 状态版本一致：聊天上下文 `fictional_life` 摘要、房间控件与图像任务读数来自同一生活状态；
  已捕获的图像任务保留创建时的 actor/room/world/outfit/工作流版本，之后的生活变化不改写它。
- 手动保持只由显式 `resume_*`/到期解除，日程与另一角色的建议都不会覆盖它。
- 重启后日程结算不重复写事件、已提交图像任务不重发、同素材日记任务不重复入队、正式版不重复发布。
- 生活与图像数据都不进入真实 source-facts 流（source_head 不变），日记仍标记真实聊天来源 excluded。

该场景只使用虚构标识、合成事件、本地 stub ComfyUI 与模型替身，不代表真实 GPU、真实渠道或现实对话总结。

网页候选：snapshot actor/world/room 版本读取；expected_version 控件写入/恢复；日记metadata及判锁内容读取。
另已审阅 TS014 网页会话快照候选并回报边界，尚未冻结，不在本模块私造网络 wire。

角色配置更新（人格、心情、日程及迁移房间/世界）保留已有manual活动、到期时间及活动变化起点。
只有显式resume_actor或保持到期才恢复新日程；配置更新不会暗中释放永久保持。
retry_diary入口要求expected为正整数，None/bool/0及旧版本均拒绝且不修改任务。
