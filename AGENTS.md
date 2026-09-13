# 项目开发约定

遵循天枢主工作区的 V2 总稿、当前任务卡及已发布的 contracts 版本。任务 worktree 的 .runtime/workspace-context.json 记录主工作区与明确基线。

只在分配的目录内实施，不改其他项目或共享合同；公共入口、依赖锁和迁移主线单人负责。禁止把未配置服务显示成成功。

当前 Python >=3.12 模块化后端，SQLite/WAL 单进程所有者；真实记忆、网关、渠道、PostgreSQL 与多进程尚未验收。依赖替身只允许放 tests/。合同从已发布目录加载并校验 manifest/文件哈希，禁止复制或更改共享 schema。

实际命令（项目根执行，Windows 用 `.venv/Scripts/python.exe`；其他平台用 `.venv/bin/python`）：

- 初始化：`python -m venv .venv`；安装锁定开发依赖：`python -m pip install -r requirements-dev.txt`；登记两个包：`python -m pip install --no-deps -e .`（保留默认构建隔离）。
- 格式检查：`python -m ruff format --check src integrations tests scripts`；静态检查：`python -m ruff check src integrations tests scripts`；语法：`python -m compileall -q src integrations tests scripts`。
- 调度定向：`python -m pytest tests/test_core.py tests/test_edges.py -q`；边界定向：`python -m pytest tests/test_boundaries.py tests/test_gateway.py tests/test_bootstrap.py -q`；完整组件套件：`python -m pytest -q`。
- 短期上下文定向：`python -m pytest tests/test_short_context.py -q`；改变共享状态/来源读取时同时验证调度套件。
- 合成持久轨迹：`python tests/trace_scenario.py --output .runtime/local-trace.json`。
- 回环 HTTP 进程启动/关闭检查：`python tests/smoke_server.py`（不加载外部服务配置）。
- 本地启动：`python -m uvicorn tianshu_companion.app:create_app --factory --host 127.0.0.1 --port 8765 --workers 1`；未配置业务请求返回 503。
- 本地状态检查：`python scripts/inspect_state.py .runtime/companion.db`；`--include-context` 仅供明确需要内容的本机调试。

测试通过 `TIANSHU_CONTRACTS` 或本任务 `.runtime/workspace-context.json` 查找协调仓库合同。主合同验证器不是产品测试。变更稳定后先审查完整 diff，再跑相关检查；已通过且输入未变的检查无需重复。

本地隔离开发和提交用于审查；不自动推送、部署或操作真实设备。交付短记录 docs/handoffs/<任务编号>.md，含实际变更、验证、风险及下一步。
