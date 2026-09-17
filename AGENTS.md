# 项目开发约定

遵循天枢主工作区的 V2 总稿、当前任务卡及已发布的 contracts 版本。任务 worktree 的 .runtime/workspace-context.json 记录主工作区与明确基线。

只在分配的目录内实施，不改其他项目或共享合同；公共入口、依赖锁和迁移主线单人负责。禁止把未配置服务显示成成功。

当前 Python >=3.12 模块化后端，SQLite/WAL 单进程所有者；真实记忆、网关、渠道、PostgreSQL 与多进程尚未验收。依赖替身只允许放 tests/。合同从已发布目录加载并校验 manifest/文件哈希，禁止复制或更改共享 schema。

实际命令（项目根执行，Windows 用 `.venv/Scripts/python.exe`；其他平台用 `.venv/bin/python`）：

- 初始化：`python -m venv .venv`；安装锁定开发依赖：`python -m pip install -r requirements-dev.txt`；登记两个包：`python -m pip install --no-deps -e .`（保留默认构建隔离）。
- 格式检查：`python -m ruff format --check src integrations tests scripts`；静态检查：`python -m ruff check src integrations tests scripts`；语法：`python -m compileall -q src integrations tests scripts`。
- 调度定向：`python -m pytest tests/test_core.py tests/test_edges.py -q`；边界定向：`python -m pytest tests/test_boundaries.py tests/test_gateway.py tests/test_bootstrap.py -q`；完整组件套件：`python -m pytest -q`。
- 方案依赖定向：`python -m pytest tests/test_plan_dependencies.py -q`；依赖前序查询使用精确actor/person/audience/conversation索引和LIMIT 1，不筛掉未送达/失效的最近候选，不回退更旧方案；变更时同时验证调度、短期上下文与画像继承检查。
- 短期上下文定向：`python -m pytest tests/test_short_context.py tests/test_continuation_revision.py -q`；改变共享状态/来源读取时同时验证调度套件。
- 画像定向：`python -m pytest tests/test_profile_context.py tests/test_short_context.py tests/test_continuation_revision.py -q`。联合用例先显式设置 `TIANSHU_MEMORY_REPO` 为 Memory 仓库，再运行 `python -m pytest tests/test_profile_joint.py -q`；只读固定提交69b29f3，使用ASGI与合成来源，未设置时跳过，不能称完整联合验证通过。
- 来源定向：`python -m pytest tests/test_source_sync.py tests/test_source_migration.py tests/test_source_bridge.py -q`。来源、身份、collector、watermark改动涉及共享状态，再跑完整组件套件；已有旧混actor断言和手动blocked_scope断言已按TS-022新行为更新。
- 实际TLS回环：先设置 `TIANSHU_TLS_PYTHON` 为具有cryptography的解释器绝对路径，再运行 `python -m pytest tests/test_source_https.py -q -s`。该解释器仅生成临时单日回环证书，Core进程和测试仍用本项目虚拟环境；没有新增锁依赖或落库凭据。当前机器可用 `C:/Users/Administrator/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe`。未设置时明确跳过；远端Platform/Memory为合成HTTP替身，不是L0。
- 查询传输定向：同样设置 `TIANSHU_TLS_PYTHON` 后运行 `python -m pytest tests/test_query_transport.py -q -s`；覆盖真实TLS空闲关闭恢复、并发隔离、预算/取消、写入无响应不重发及unknown封账。JsonService属共享调用链，变更后跑完整组件套件；查询白名单和限制见 `docs/query-transport.md`。
- 合成持久轨迹：`python tests/trace_scenario.py --output .runtime/local-trace.json`。
- 回环 HTTP 进程启动/关闭检查：`python tests/smoke_server.py`（不加载外部服务配置）。
- 本地启动：`python -m uvicorn tianshu_companion.app:create_app --factory --host 127.0.0.1 --port 8765 --workers 1`；未配置业务请求返回 503。
- 本地状态检查：`python scripts/inspect_state.py .runtime/companion.db`；`--include-context` 仅供明确需要内容的本机调试。

测试通过 `TIANSHU_CONTRACTS` 或本任务 `.runtime/workspace-context.json` 查找协调仓库合同。主合同验证器不是产品测试。变更稳定后先审查完整 diff，再跑相关检查；已通过且输入未变的检查无需重复。

合同部署须同时保留 `contracts/text-dialogue/v1`、同根的 `contracts/profile-memory/v1` 和 `contracts/source-sync/v1` 1.0.0发布包；路径配置仍指向文字包，不复制schema到产品。source manifest LF SHA256固定为 `178d0ce66210bdfad4cfb85d8b5f0905b0b67f834e2a530efe5636ff0373633d`，不加载candidate/rules作产品认证。

数据库当前user_version=2。结构迁移前用SQLite backup保存 `数据库路径.pre-source-v2-随机值.bak`；随后Core在单事务中迁移可由原请求证明的actor，混组或缺原始归属的会话隔离503。迁移失败回滚事实并释放owner锁，修复故障后重试；恢复备份须停进程并隔离当前DB/WAL/SHM，不能在运行中覆盖或自行清除水位/隔离标记。完整步骤与边界见 `docs/source-sync.md`。

本地隔离开发和提交用于审查；不自动推送、部署或操作真实设备。交付短记录 docs/handoffs/<任务编号>.md，含实际变更、验证、风险及下一步。

TS-071：数据库现为user_version=3；v2结构升级前SQLite backup至pre-life-v3随机备份，恢复须停机隔离DB/WAL/SHM。生活、日记及可信同产品端口见docs/life.md；定向命令：python -m pytest tests/test_life.py -q。日记必须显式life_writing及独立life_config_version，不回退聊天配置；当前只用虚构素材，不是现实对话总结日记。共享生命周期/迁移改动后跑完整组件套件。

网页快照接线：另需正式web-conversation/v1 1.0.0（manifest LF SHA256 e493a1b5d0f4cec8d55995553faf84042f4c33a59365d15423e57f4dc70a6c09）。内部platform bearer读取，Core到Platform使用services.platform_sender独立companion凭据；web不回退NoneBot。定向python -m pytest tests/test_web_snapshot.py -q；配置/来源展示限制见docs/web-conversation.md。

TS-072：数据库现为user_version=4，v3升级前pre-images-v4随机备份；图像/衣橱可信内部端口、原工作流只读审阅与候选见docs/images.md。定向：python -m pytest tests/test_images.py tests/test_life.py -q；迁移/生命周期改动后跑完整组件套件。禁止把合成HTTP测试称为真实GPU验证。

TS-073：数据库现为user_version=5，v4升级前pre-writing-v5随机备份；作品/章节顺序/版本化大纲与人物设定/不可变章节修订、审校与发布历史见docs/writing.md。定向：python -m pytest tests/test_writing.py tests/test_writing_chain.py -q；写作复用独立life_writing与life_config_version及现有网关，未配置明确不可生成，不回退聊天模型；生成前固定素材/前章引用/配方/模型配置版本，回包按attempt归属（旧回包/失败/取消只落旧请求，不写新稿），依赖变化只标needs_review/invalidated且不改写已发布正文，unknown不自动重发；审校绑定所审阅修订与固定素材，发布要求无漂移且批准覆盖当前基准，重审需显式acknowledge_drift。迁移/生命周期改动后跑完整组件套件。

TS-074：数据库现为user_version=6，v5升级前pre-proactive-v6随机备份，恢复须停机隔离DB/WAL/SHM；显式订阅/角色目标/提醒候选与静默调度见docs/proactive.md。定向：python -m pytest tests/test_proactive.py -q。主动联系默认关闭，只有显式登记（consent basis为explicit_user_request/explicit_admin_registration）才可能产生候选；普通聊天或模型文本从不是同意，也不抓取健康数据或猜现实经历。文案只用不可变显式模板（占位符限summary/due_time/due_date/timezone），不调用模型、不回退聊天线路。决策按角色/接收者/会话权限/时区静默窗口/冷却/每日额度/未回复抑制/到期时间给出延后、抑制、过期或取消；所有时刻为绝对UTC，跨午夜与夏令时缺口/回拨在tick内重算。事项与候选两处扫描都必须保持有界且公平：当前occurrence已有候选的事项不占名额，持久轮转游标按(deadline,id)前进并回绕，每tick至多max_subjects（每类）/max_candidates行，禁止改成无界全扫或放大上限；登记不可用只记blocked_reason并退出扫描。派发前在同一事务重读来源、授权、候选版本与取消；额度与提交意图同事务，unknown不自动重发，晚回包只落旧attempt。已发布text-dialogue/v1无主动发送文档（send_request以已有turn为键），因此生产默认无dispatcher：候选保持ready并返回contract_gap，不写attempt、不耗额度，delivered仅在adapter="contract"回执下为真——不声称任何主动消息已送达。

TS-024：数据库现为user_version=7，v6升级前pre-routing-v7随机备份，恢复须停机隔离DB/WAL/SHM；功能指令登记/分流/单一执行与唯一回复责任见docs/routing.md。定向：python -m pytest tests/test_routing.py -q。只有首token完全等于已登记命令名、且登记覆盖该平台/受众、且reply_to="bridge"的文本才走命令快路；其余（含reply_to="core"的裸命令文本）继续走陪伴链，禁止让功能指令依赖聊天模型或渠道可达。参数形态由登记决定、永不猜测，非法参数用登记推导的usage行回复且不执行不调模型。命令快路不排聊天防抖、不建collection、不占两个聊天轮次名额、不调聊天模型；执行在独立direct_worker。命令入口与core.capability带同一message_key时共用一次执行（entries记录两个来源），同键异参报idempotency_conflict，同message_id更高revision取代旧版本。回复责任唯一：reply_to="bridge"时Core返回result/reply均为null、投递复用既有出站管线（不新建第二条消息出口，reply_id=digest([request_id,"reply"])稳定去重，turn_id为该直接请求自身）；reply_to="core"时请求停awaiting_core且适配器回包即收口unknown——禁止同时双答。授权只用本地事实（会话未隔离、该会话已受理入站建立的actor绑定、登记范围与actor_allowlist、角色与binding），persona/好感度/前端字段不是权限，capability字段集封闭；派发前同事务复核，失败即cancelled+authorization_revoked且不调用不投递。unknown不自动重发，redeliver仅在最近投递failed且回执retry_safe=true（且该回执的reply_id/segment_sequence确属本次投递文档）时允许，晚回包只落旧attempt，重启中断记unknown。功能回执与聊天回复共用同一出站出口，而Bridge.send只接受严格递增的turn_sequence*100+segment_sequence：会话出站序号由Core统一发放（conversations.send_band/send_band_owner），单元自己的号>send_band（水位以上按构造从未发出过）或号==send_band且send_band_owner就是本单元才沿用（同一轮所有分段共用一个号，整轮身份不变），号更低或相等但归属别人一律取max(turn_sequence,send_band)+1的新号——封盘号与出站号是两套计数器，功能回复取号不封盘，之后新入站聊天可能拿到已被功能回复用掉的号，相等必须核对持久归属，只按数字判"是自己的"会让整轮聊天被Bridge当旧位置拒绝；功能回复投递前若该号仍被已开始发送的陪伴轮次持有，就不抢号，留在completed+ready_to_deliver并记deferred_reason="outbound_band_busy"/deferred_on，轮次终态后由Core.tick唤起direct worker按下一个号发出——等待只发生在合法消息边界，未开始发送的轮次不持有边界，功能回复永不排队等聊天模型；禁止让功能回执夺走在途聊天轮次的分段（那会让该轮后续分段被拒绝而丢失）。投递前先落completed+submitting与冻结投递文档，重启后recover认领这些行：标unknown+unresolved+blocked_reason="interrupted_delivery"，不重跑功能、不伪造未送达、不盲目重发，只用同一份投递证据reconcile出sent/failed/仍未知，晚到旧attempt回包不改写结论。所有投递与结果都带证据标签，未配置即如实失败（delivery_port_unavailable/plugin_unavailable/conversation_unmapped/conversation_mismatch），合成适配器result_verified恒为false，禁止称为真实GsCore结果。命令表以Core为唯一来源（core.commands供桥接构造matcher），桥接侧不复制。

TS-075：数据库现为user_version=9，v8升级前pre-persona-ops-v9随机备份、v7升级前pre-persona-v8（一次打开最多一份，标签取本次跳升的最高结构步骤，例如v5→v9只写pre-persona-ops-v9），恢复须停机隔离DB/WAL/SHM；角色人格版本管理、显式批准、发布指针、可追溯回退、对话快照与操作幂等账本见docs/personas.md。定向：python -m pytest tests/test_personas.py tests/test_persona_chain.py -q；职责与依赖结构断言：python -m pytest tests/test_boundaries.py -q -k PersonaBoundary。人格业务规则只在src/tianshu_companion/personas.py（唯一应用入口Personas.manage）；core只在既有准备边界调用personas.pin取一次不可变快照、每次模型调用前personas.verify复核，禁止把草稿/批准/发布/回退规则或persona表名搬进core/app/cli（除personas.py/store.py外任何模块不得写出persona_*表名）；persona_cli只解析参数并提交同一use case，离线--database自持Store单所有者，服务在跑返回service_running且不写入，在线--url只认personas.admin_token_env指定的独立凭据（服务persona_admin），聊天/入站/桥接凭据永远到不了人格写入；未配置personas段则整体不启用（管理端口503）。人格文本永远不是权限，改不动角色登记、来源、模型绑定、发送资格与Memory关系数据，回退旧文本不复活已撤下的角色。修订按(subject,content,parent)内容寻址（同键同内容为重放，异内容/异父拒绝），发布要求对该修订的显式批准，回退是把旧内容重放为新修订以保持历史线性可追溯；已准备/生成/发送中的轮次保持自己的快照，排队未准备的轮次取新快照，发布不得就地改写已有turn。内容寻址只去重正文，不替代操作去重：每个写操作（draft/approve/reject/publish/rollback/retire/restore）都必须带request_id，身份=digest({scope,request_id,operation})并绑定规范化请求摘要，业务写入与结果账本行在同一个SQLite事务提交（Store.transaction可重入，内层加入外层、只最外层提交）；同键同摘要重放返回首次记录的结果且不重复落事实，同键异摘要拒绝（invalid_input），缺request_id的写入拒绝，导入仍按部署文档指纹幂等（不参与该账本）；账本不清除CAS——不同请求带过期expected仍返回version_conflict；approve改变可见状态故与其它写入一样推进persona版本，expected覆盖审批状态，两个操作者不能凭同一次旧读取各自批准同一草稿。适配器不复制这些规则：CLI只透传--request-id（未给则生成一次性id），HTTP端口把已鉴权服务记为scope。迁移/生命周期改动后跑完整组件套件。
