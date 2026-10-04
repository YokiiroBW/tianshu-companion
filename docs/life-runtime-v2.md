# 角色生活运行链

本轮在既有 Life、Images、Writing、Proactive、Direct 和 replies 队列上演进。`runtime.py` 接管理与授权投影，`runtime_execution.py` 执行角色活动与主动表达，`role_actions.py` 提供模型能实际调用的领域操作。`delivery.py` 唯一拥有出站、分段和回执规则。没有第二套生活引擎、来源账本或正文库。

## 入口与配置

正式 POST 入口：`/internal/v2/life/manage`、`read`、`conversation/ensure`、`proactive/control`、`media/read`、`content/read`。请求与闭合投影来自协调仓库 `life-runtime/v2`。`content.acquire` 的管理回执包含真实 `content_ref`；带 `reading_id` 的 `content/read` 成功后权威推进该会话位置和实际覆盖。`image_jobs` 投影原任务的状态、版本、错误和真实原件引用；图像请求受理不等于生成完成。

保持既有 `services.<name>={url,token_env,ca_file}` 形状。独立生产进程需配置：

- `services.memory`：身份、长期记忆、关系和来源校验。
- `services.knowledge`：Knowledge 独立原件 owner 的 acquire/read/original；不连接 Memory DB。未给该段时保留已有合并 HTTP 服务部署的地址复用。
- `services.gateway`：原生 Chat Completions SSE、工具续答、视觉和执行回执。
- `services.platform_sender`：同一 Platform v2 出站队列。普通网页回复也使用该队列；已选择的 BOT 绑定仍沿 `bot_platform_bindings`，动态 BOT 登记沿既有 `bot_binding_management_enabled`。
- `services.image_credentials`：可选专用窄凭据 resolve 客户端；缺省复用 Platform 客户端。配置只存 `credential_ref`，真正生成前调用 Platform `/internal/v1/service-credentials/resolve`，原始密钥不进入读投影。

保持已注册 `callers.platform` 的真实 Platform issuer、HTTPS resolver 和 CA；独立只读 `platform_life` 不充当消息 publisher，只在自身已核 reader grant 下复用该明确注册的 resolver。`life_readers.platform_life` 为网页普通 reader；管理当前章节等操作需独立 `life_readers.platform` 管理 reader（可用 `runtime_roles:true`）。两个 reader 都沿已存在的 `life_access` 登记，不以管理 token 推导任意人物私有正文权限。

Memory caller 必须登记正式 context query/propose/receipt/batch/association 操作及既有 source-sync/check；自然纠正还调用 Platform proof/issue。Knowledge 角色 reader 需 `content_acquire/content_read/content_original`、`runtime_content:true` 与实时 RoleGrants（`allow_runtime_roles:true` 或部署实际 allowed_actors），operation_ref 来自真实持久活动/阅读会话。上传、授权管理仍归 Platform/Knowledge，不给普通模型 grant 工具。

网关使用 `/v1/chat/completions`、现有 ClientGrant 与 `companion.text` workload 选择，必须允许原生 stream/tools/vision。这条 Chat 路径与可选 `/v1/responses` 的 `native_enabled/native_clients` 配置不同。视觉联合夹具将 `max_request_bytes` 设置为 4 MiB；真实部署需相应预算。工具续答保留 assistant.tool_calls、role:tool、tool_call_id，视觉保留原生 image_url，不让 Gateway 执行工具或下载图。

## 生活、来源和权限

全天计划表达意图；活动 checkpoint 与真实执行进度独立。跨日和重启保留 running/paused 的活动，停机时间不伪造完成经历。开放事项与认识片段保存自身来源和版本；实际经验才生成生活事件。短期情绪衰减独立于 Memory 好感账本。

主动候选有持久动机与精确来源；派发前核角色 epoch、订阅、原 receiver 和来源。历史 Memory 来源沿 source-sync/check 校当前原 sources 与 scope_version，不用过期入站 origin 或模糊 topN 检索判撤回。需要新读历史上下文时，由 Platform bot-delivery/context 在实际订阅/channel 授权下签发当前 origin。

真实 ACK 才记录已送达次数。已送主动联系等待实际回应；到期一次记录“暂未收到回应，原因未知”的中性短情绪，之后实际入站一次记录恢复。沉默不扣好感、不推断反感。49 对象 fanout 采用持久游标每批16，重启后继续，不永久丢弃第17名以后的对象。

scope=null 是角色自有生活，仍需真实 origin 与已有 reader/role grant；不代表匿名公开。私有事项必须精确 scope，关联读取复用 Memory 当前精确 scope_checks 与关联版本。`chapter_view=current` 还需可信 Platform 管理 caller；默认只读已发布正文。身份/衣物参考保存源 scope，保存和实际生成都重读 owner 原件；另一 life reader 不获得私域参考。未配置身份参考的真实默认版本为1，第一次配置按读回版本CAS，写成2。

Knowledge 是链接、上传及媒体正文唯一 owner。Companion 只保存 refs、实际读覆盖和会话进度。read 保留 representations、gaps 和真实帧时刻；部分视频采样不能当通读。QQ 发送前在原 authority 下物化完整 bytes/ref/type/hash，Platform 复核原 entry/connection/receiver，再由 SDK 真实 ACK；QQ 附件送达不自动授予网页原件读取权。

ComfyUI 后端可在管理页配置/检查，身份、衣物及原图编辑用真实上传原件。请求本次 outfit 覆盖不自动改变当前穿搭。未配置、模型未布置、原件撤回、未知提交都准确显示对应状态，不自动安装 GPU 服务。生成原件保存在既有 staging 目录，persistent album 只保存引用。Writing 的完整章节原文与不可变修订仍归现有 Writing；续写不以摘要替代原文。

## 出站、取消与恢复

同一 expression 的 append 固定 segment_id/sequence；重复请求返回原回执，内容冲突拒绝，final 后不追加。中断保留已受理片段，unknown 先 query 原 expression，不整段重发。partial 中仍未知的片段继续对账；超时封账不删除原 ACK，晚回执只更新原投递证据。取消先请求 Gateway 现执行 owner，再关闭消费者连接，已送片段不会回退。Gateway 已启动上游后取消终态为 execution.state=cancelled，outcome 仍可能 unknown，不冒称上游副作用已撤销。

## 迁移与复验

Store 启动自动从非空 v1..v9 升级到 v10，DDL 前以 SQLite backup（包含 WAL）写 `<database>.pre-life-runtime-v10-<uuid>.bak`。旧数据、来源水位及原 image_jobs/artifacts 保留。旧 completed 图像按有界持久游标核实际文件、大小和 hash 后纳入媒体/相册；缺失原件标 unavailable，旧 completion 时间未知保留 null，不凭迁移时间编造经历。

回退需停止单 owner 并恢复成对的 DB 备份及对应原件目录。旧 v9 程序拒绝 user_version10；不能直接把旧备份覆盖升级后的新写入，须由部署协调者处理新事实。不得在运行中覆盖 DB/WAL/SHM。

最小组件检查为 `python -m pytest tests/test_runtime_v2.py tests/test_delivery_v2.py -q`。迁移/装配改变按项目要求运行完整 `python -m pytest -q`。

真实五 HTTP owners 联合入口复用 Platform `tests/backend/run_c4_joint.ps1`；它配置真实 Core、Memory、独立 Knowledge、Gateway、Platform 的 TLS、隔离账号与 DB，仅外部付费模型和 QQ SDK 用记录型合成上游。从 Platform 检出执行：

```powershell
tests/backend/run_c4_joint.ps1 -Tests test_c3_joint.CompanionJoint
```

对应模块覆盖上传→实读→进度→重启→当前草稿、普通回复→真实来源→自然纠正→Memory回执、两个语义负例、真实图片原件→native视觉与工具续答、主动→原队列→实际HTTP SDK ACK、held SSE→网页取消→网关执行终态→重启不重发。默认组件 pytest 未加载跨产品夹具时明确跳过联合模块。
