# TS-022 来源接线与恢复

实现基线为Core `05c35db54df146bf2997eab6cd516c0948dfdc40`，合同发布根提交 `154f068`。只使用source-sync/v1 1.0.0（manifest按UTF-8/LF哈希 `178d0ce66210bdfad4cfb85d8b5f0905b0b67f834e2a530efe5636ff0373633d`），并验证原text/profile两个发布包。没有修改wire/schema/hash，不导入历史candidate或用纯rules替代产品认证。

## 入站与来源事实

新 `POST /internal/v1/conversation/ingest-actors` 只接受认证platform/nonebot服务。Core用固定Platform服务客户端调用 `POST /internal/v1/source-access/read`，operation=input；核对完整请求/输入摘要、账号/渠道、原始source_input ref。逐actor context须来自platform issuer，caller与认证入口相同、receiver为companion、受众及Core binding相符；Core以这些actor origin调用Memory resolve/register，核对同一person/binding_version。身份通路不调用Core facts或Memory来源屏障。所有网络调用都在Core SQLite事务外；提交前重新核验截止时间和授权有效期。

P键包括namespace/binding/channel_conversation/thread/message_id，版本按message revision。正文摘要包括完整physical_input，targets只属于路由意图。A键为P+actor，版本受理有独立receipt/scope/binding/origin/accepted_at。一个Core事务保存P、成功A、collector、回执、命令结果及水位。只授权A而请求A/B时，B明确forbidden且receipt/admission为null；不存在默默吞掉B的“全成功”。没有合法actor时可以只受理P，不创建person。相同P分送A/B共有一个conversation、两个活跃槽及全局段序。

命令幂等键是认证服务+操作+idempotency_key，完整语义含input与原targets。空targets第一次解析的默认集合/routing_version持久冻结，重试不扩展集合并重验旧集合权限；已撤权角色不返回历史私密回执。先前forbidden不会靠同key重试升级为accepted，更改路由意图需要新key。A+revision重复不会吞掉B的首次受理。

新回应同时带原子inline admission与receipt。Platform受信prepare/confirm应用流程使用它们回填person/channel；薄桥提供 `confirm_admissions(request, result)` 接点，必须真实确认返回True才把本地待处理标记为accepted。该流程不增加HTTP映射RPC，不等待committed_event，不循环查询Core/Memory来源。响应丢失或确认失败重试原key取原回执；Core首次映射期间保留原有至多5秒、有deadline上界的503等待。

`POST /internal/v1/source-facts/read`只接受独立memory服务凭据。snapshot在一个Core事务内返回明确selectors的全部admission/missing、去重P当前版本/墓碑，以及请求turn的owner事实；不能用A补B的missing。include_content=false时P正文为null。head请求必须空selectors/turn_ids且不带内容。该接口只读Core状态，不回调Memory。上限256 selectors、32 turns、1 MiB完整响应；超大完整快照503，不截断、不分页拼快照。

physical/edit/retract/classification、admission、轮次和投递事实事务推进持久generation/sequence；同事务只加一次，读快照及内部投递重试计数不推进。普通重启不重置；v2缺失持久head直接拒绝启动，不能自行生成新水位冒充恢复完成。Memory负责执行Core C1→Platform P1→Core C2与本地事务屏障，本项目不能替它宣称屏障或L0已完成。

## 失效与兼容

P编辑使所有已受理actor旧版本失效，仅当前获准actor取得编辑版本新receipt；其他actor保留旧receipt作历史，不能正向复活。P撤回是全actor墓碑，不产生新actor receipt，后续更高revision也不能复活。新retract不允许actor targets；旧retract可返回旧信封要求的control receipt，但不会进入admission、input bundle或可召回来源。

classification由Core binding中显式 `classification` 配置保存，形状为已发布shared#classification。例如 `{"value":"real","basis":"registered_input_mode","policy_ref":"input-mode:ordinary","policy_version":1}` 表示登记的输入情境，**不证明陈述为真**。无配置为unclassified。`Core.reclassify_source(key, expected_revision, classification)`是Core受信应用内部端口，无HTTP路由或模型入口；核对当前revision并向所有actor只传播失效。正式reviewed_source配置/操作必须由真实受信应用承担，不能从聊天正文授予。

旧 `ingest` 保留旧actor-origin resolve与Memory identity的单actor兼容，空targets只取原授权actor，群内仍沿旧观察行为。它不拥有新source_input精确证明。Core内部inbox/admissions另存authorization.kind及实际ingress/origin：新路径为source_input_authority、旧在线路径为legacy_actor_origin、历史迁移为legacy_original_request；这些是本地证据元数据，不扩充wire。Platform若没有对应legacy admission历史，source.current明确503，Memory不能伪造精确证明继续服务。

普通回复cancel不撤回输入。发送前文字/画像两域探针、跨作者context_checks继承和TS-021不可上调的续接来源版本保留。输入编辑/撤回/分类改变使所有角色相关候选停止，已发送事实仍保留。终态缺scope_version由publisher调用Memory `/internal/v1/memory/source-sync/check`（只有服务身份，没有旧用户origin）；失败保留blocked_scope与last_error，权威返回后再次核对本地turn/input再发布原event_id。

## 部署配置

`contracts_path`仍指向text-dialogue/v1；同时部署同根profile-memory/v1和source-sync/v1。`services`的url是固定HTTPS服务**基础地址**，客户端追加正式路径，不接受payload URL、重定向或关闭证书校验。每服务token由token_env指定的环境变量读取；可选ca_file须为现有绝对文件路径，缺省使用系统受信CA。

[完整最小配置样例](config.source-sync.example.json)展示NoneBot群内两个actor与Memory来源读取所需字段，域名均为不可用占位值，无凭据；相对路径按启动工作目录解析。部署时须换成正式合同路径、隔离数据库路径、真实HTTPS基础地址/受信CA、独立环境凭据、Platform登记的binding/actor及发布config_version。样例config_version=null和unclassified有意不能宣称已生成或记住真实内容；分类与人格只能按实际已登记策略替换。平台直接入站可另加callers.platform及对应binding.service=platform，不能复用nonebot凭据。

| 配置 | 责任 |
| --- | --- |
| callers.nonebot / callers.platform | 独立入站token_env；新路由issuer=platform，origin_service指向Platform服务配置 |
| callers.memory | 独立仅来源读取token_env，不需要origin_service；该身份不能调用ingest-actors |
| services.platform（或origin_service所选名字） | 固定Platform HTTPS基础地址、Core服务凭据、可选ca_file；Platform principal必须service=companion且允许source.input |
| services.memory | 固定Memory HTTPS基础地址及Core服务凭据；允许identity/selection/profiles/turn_commits/check_sources正式操作 |
| services.gateway / services.nonebot | 原有真实网关、渠道发送客户端；未配置明确失败 |
| bindings | namespace、认证入口service、audience、允许actor_ids及显式classification策略 |

Memory→Core必须使用callers.memory对应凭据；Memory→Platform current使用其独立service=memory/source.current身份。在线viewer仍是独立companion→memory的完整scope origin，不能被传输服务身份覆盖。没有真实服务配置/账号/设备时，保留明确不可用，不填固定成功默认值。

已只读核对集成Platform `a94d34534ba0b6002bdcdab9db1d5dd899a06a16` 和Memory `ba0e50d56d6a4e816267d710c41c6b0c49035431` 的运行文档：Memory的source_sync.core/platform.url使用完整正式端点，与Core自身基础地址配置不同；Memory后台consume/check还要求event_scopes登记实际映射后的精确actor/person/audience/conversation，空列表拒绝。这些配置必须由联合接线明确准备，Core不能用旧origin或宽泛默认scope越过；此处文档核对不是实际联合通过记录。

## 迁移、异常与恢复

1. 只在隔离数据库验证并停止旧Core，确保单进程owner。首次打开user_version=1时，使用SQLite backup API（包含已提交WAL）生成唯一 `数据库路径.pre-source-v2-随机值.bak`，不覆盖旧备份；然后建立v2表与持久head。
2. Core迁移在单事务中读取旧原始request、receipt、bundle/scope进行交叉证明。原请求必须明确单actor；collector只能佐证，不能单独推断。原receipt值保留，另外生成物理receipt。旧hardcoded real不作分类证据，迁为unclassified。混组、空目标缺证据、归属矛盾或旧墓碑后又出现输入的会话保留原行并标source_quarantined；入站/来源查询503，调度不运行它们。其他可证明会话可继续。
3. 迁移中断/存储错误回滚本次P/A/索引转换与迁移标记，并释放owner锁。已经建立的v2表可保留，但不存在部分成功admission；修复磁盘/权限后再启动会从未完成标记继续迁移。完成标记存在则不会重做、更换physical receipt或重置head。
4. 恢复必须先停Core并保留当前DB、WAL、SHM、备份及诊断证据到独立恢复目录，再用已验证备份建立隔离恢复数据库。不要在运行中覆盖、删除WAL/SHM或清除source_head/source_quarantined来强行启动。备份schema=1可供旧版本离线检查，不能拿旧版本读取v2库。
5. 恢复旧备份可能让同generation序号回退；Memory应拒绝。无法延续序号时必须由协调的受信恢复流程分配新generation并核验双方水位/墓碑/消费状态。本任务不提供自动批准或重置命令，不宣称恢复旧备份后可以直接继续线上来源同步。

## 验证区分

安装与命令见项目AGENTS。普通来源测试使用显式合成依赖；迁移测试构造v1形状临时SQLite、核验真实backup、混组503、故障注入回滚、owner释放和重启水位。NoneBot测试经真实ASGI客户端验证inline确认失败后重试、receipt交换拒绝和W=0首次映射等待。

`test_source_https.py`启动实际Core/uvicorn子进程和真实TLS套接字，临时证书做完整CA/主机名验证；Platform/Memory是本测试内合成HTTP服务。覆盖首次resolve/register、1P/2A、facts ACL/正文、Memory check回调Core facts、blocked_scope恢复及两条committed_event。故意不模拟成功模型回复；不能把该结果称为真实Platform/Memory联合、真实渠道送达或完整L0。旧画像固定提交ASGI联合测试在未显式指定Memory仓库时保持跳过，不读其他工作者可变目录。
