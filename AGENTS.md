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

TS-073：数据库现为user_version=5，v4升级前pre-writing-v5随机备份；作品/章节顺序/版本化大纲与人物设定/不可变章节修订、审校与发布历史见docs/writing.md。定向：python -m pytest tests/test_writing.py tests/test_writing_chain.py -q；写作复用独立life_writing与life_config_version及现有网关，未配置明确不可生成，不回退聊天模型；生成前固定素材/前章引用/配方/模型配置版本，依赖变化只标needs_review且不改写已发布正文，unknown不自动重发。迁移/生命周期改动后跑完整组件套件。
