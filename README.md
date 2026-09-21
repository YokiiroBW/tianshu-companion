# 陪伴核心

可恢复的文字陪伴核心与 NoneBot 薄桥，当前实现文字链、短期语境及群画像消费的本地组件切片。

已实现标准入站、物理来源P/角色受理A、来源/账号核验、首次 memory 登记、逐角色回执与静默窗口、结构化消息组、会话共享两个活跃轮次、按需记忆、一次主生成、逐段有序发送、回执与事务 outbox。相同消息分送A/B共用物理回执，但各有角色receipt/scope/collector；物理编辑撤回向所有角色传播失效。核心 HTTP 和实际客户端可运行；生产服务未配置时明确不可用。

普通连续聊天会按相同人物、角色、会话和受众装配近期完整输入组及已确认 sent 的回复，默认最多 4 轮、8 KiB、30 分钟。短期窗口来自核心持久记录，重启后可恢复；不依赖长期记忆提炼。来源编辑/撤回、权限或记忆范围版本变化会保守失效旧窗口，已发送事实仍保留在回执记录中。

群聊可纳入同一受众中重新核验过的其他作者完整输入组，并查询当前群主题/风格及有界窗口内人物的获准兴趣/风格。稳定身份不按昵称合并。普通记忆、画像、短期语境和必要依赖共用16 KiB附加预算，超额保留完整组或明确拒绝；画像独立版本域在发送前复核，失效依赖继续传递到衍生历史。须同时部署文字与画像两个正式合同包，详见安装约定。

本项目有独立 Git；协调检出不供并发写入，任务在主工作区 worktrees 中进行。工作目录上下文见 .runtime/workspace-context.json，或回到主工作区 docs/development/CURRENT.md。

安装、启动和验证命令以 [AGENTS.md](AGENTS.md) 为唯一清单。依赖锁为 `requirements-dev.txt`，项目及两个包的清单为 `pyproject.toml`。系统 Python 不在 PATH 时，先用本机已安装的 Python >=3.12 路径创建虚拟环境。

不设 `TIANSHU_COMPANION_CONFIG` 即可启动检查空配置行为：`GET /healthz` 只表示进程存活，业务 POST 返回 `dependency_unavailable`。指定该变量为一个服务端 JSON 文件路径才创建数据库；[本地空配置样例](docs/config.local.example.json) 不含绑定、凭据或可访问服务，不能当真实可用配置。相对路径按启动工作目录解析。

实际接入时，由协调者配置 `services`（memory、gateway、nonebot 和 issuer 的固定 HTTPS `url`、读取凭据的 `token_env`）、`callers`（认证服务名到 `token_env`、`issuer`、`origin_service`）、`bindings`（绑定 ID 到 namespace、service、audience、actor_ids），以及平台已发布的 `config_version`。不同调用服务用独立凭据，不能复用相同 token。入口应由可信 TLS 终止代理保护，应用仅监听回环；不要让浏览器直接提交内部来源引用。

HTTP入口为 `POST /internal/v1/conversation/ingest-actors`、`/internal/v1/source-facts/read`、兼容的 `/internal/v1/conversation/ingest` 和 `/internal/v1/conversation/cancel`。新入站只给platform/nonebot；facts的snapshot/head只给独立memory服务凭据，读取Core自有一致快照且不回调Memory。普通模型正文走原生 `/v1/chat/completions`，配置版本/轮次放内部头，独立读取真实路由回执。缺配置/超时/无权限/版本变化均不会返回固定假回复；文字/画像两域发送前探针继续保留。

新生产接入使用ingest-actors，先由Platform登记精确source_input，再逐actor授权；首次person由Memory身份接口返回，Platform用Core原子返回的inline admission+receipt回填，不循环查来源。旧ingest只保留原actor-origin认证适配，空targets不扩默认集合，不宣称具备新input-authority精确证明。Platform不能证明的旧admission，其Memory来源同步仍503。classification须显式绑定输入情境策略，无配置为unclassified，不能默认real或把情境分类当事实真伪。

运行时已接Memory `/internal/v1/memory/source-sync/check` 修复blocked_scope；过期用户origin不会替代后台服务身份。三份发布合同、配置、迁移备份/恢复与覆盖区分见 [来源接线说明](docs/source-sync.md)。已通过真实Core进程TLS回环（远端为合成HTTP替身）；真实Platform/Memory/网关/渠道完整L0仍未验收。

运行限制、状态表、来源边界与验收层级见 [实现说明](docs/implementation.md)，薄桥接入边界见 [NoneBot 说明](integrations/nonebot/README.md)。真实 QQ/TG、memory/gateway 联合链路和 PostgreSQL 仍未验收；生活、日记、工具执行与网页快照/SSE 留后续任务。

角色生活与日记内部端口、独立写作配置和SQLite v3恢复说明：[docs/life.md](docs/life.md)。日记的对外读取只经由 `life_readers` 部署的四个只读路由，凭据只在服务端。

网页会话快照与独立Platform发送端：[docs/web-conversation.md](docs/web-conversation.md)。

Core 图像与衣橱内部端口、ComfyUI 工作流审阅、SQLite v4 恢复及尚未发布的图像候选：[docs/images.md](docs/images.md)。

角色生活与**已发布日记**的授权只读内部端口（`life_readers` 部署、四个只读路由、派生索引与恢复点）：[docs/life-read.md](docs/life-read.md)。
