# 陪伴核心

可恢复的文字陪伴核心与 NoneBot 薄桥，当前实现 TS-020 C1/C2 的本地组件切片。

已实现标准入站、来源/账号核验、首次 memory 登记、持久收件和去重、分人静默窗口、结构化消息组、会话共享两个活跃轮次、按需记忆、一次主生成、逐段有序发送、回执与事务 outbox。核心 HTTP 和实际客户端可运行；生产服务未配置时明确不可用。

本项目有独立 Git；协调检出不供并发写入，任务在主工作区 worktrees 中进行。工作目录上下文见 .runtime/workspace-context.json，或回到主工作区 docs/development/CURRENT.md。

安装、启动和验证命令以 [AGENTS.md](AGENTS.md) 为唯一清单。依赖锁为 `requirements-dev.txt`，项目及两个包的清单为 `pyproject.toml`。系统 Python 不在 PATH 时，先用本机已安装的 Python >=3.12 路径创建虚拟环境。

不设 `TIANSHU_COMPANION_CONFIG` 即可启动检查空配置行为：`GET /healthz` 只表示进程存活，业务 POST 返回 `dependency_unavailable`。指定该变量为一个服务端 JSON 文件路径才创建数据库；[本地空配置样例](docs/config.local.example.json) 不含绑定、凭据或可访问服务，不能当真实可用配置。相对路径按启动工作目录解析。

实际接入时，由协调者配置 `services`（memory、gateway、nonebot 和 issuer 的固定 HTTPS `url`、读取凭据的 `token_env`）、`callers`（认证服务名到 `token_env`、`issuer`、`origin_service`）、`bindings`（绑定 ID 到 namespace、service、audience、actor_ids），以及平台已发布的 `config_version`。不同调用服务用独立凭据，不能复用相同 token。入口应由可信 TLS 终止代理保护，应用仅监听回环；不要让浏览器直接提交内部来源引用。

只暴露已实现的 `POST /internal/v1/conversation/ingest` 和 `POST /internal/v1/conversation/cancel`。模型正文走原生 `/v1/chat/completions`，配置版本/轮次放内部头，独立读取真实路由回执。缺配置/超时/无权限/版本变化均不会返回固定假回复。记忆只发一次有预算的选择，问候为零预算；后续版本核对也为零预算，不重新注入正文；没有额外规划/反思模型。

运行限制、状态表、来源边界与验收层级见 [实现说明](docs/implementation.md)，薄桥接入边界见 [NoneBot 说明](integrations/nonebot/README.md)。真实 QQ/TG、memory/gateway 联合链路和 PostgreSQL 仍未验收；生活、日记、工具执行与网页快照/SSE 留后续任务。
