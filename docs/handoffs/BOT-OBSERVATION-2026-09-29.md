# 机器人账号观察 · Companion/宿主交接

## 目标与基线

- 基线 `e85987147b1109c8a41b3782e6fc3ccc33e0e249`；分支 `codex/bot-observation-policy-20260929`，本交接随实现提交固定。
- 真实在线 SDK 消息按账号被动观察；只有当前策略许可且 Platform 短租约有效时，宿主回复钩子才认领消息。

## 变更

- NoneBot 与 AstrBot 插件新增 v2 观察能力、策略应用、持久事件队列、轮询、ACK、状态与发送端再次核准。共享 RPC 与两份插件内副本字节一致；v1 精确绑定不改语义。
- 消息观察钩子不阻断普通宿主处理器。回复认领钩子把决定持久写盘，轮询先到时等待决定，逾时视为未认领；已认领重放保持归属。平台轮询租约失效后新消息不认领；发送还需匹配当前修订与名单。
- Companion 独立 SQLite inbox 路径为 `database_path + '.observations.sqlite'`，持久接收后才向宿主 ACK；向 Memory 归档失败可重试。`archive_epoch` 仅用于 Companion/Platform 历史隔离，转发给 Memory 严格四字段来源时剥离；旧 epoch 查询不可见。
- 显式配置 `bot_observation_enabled: true`，要求已登记 Platform caller。NoneBot wheel 与 AstrBot ZIP 均升为 `0.3.0`。

## 实际验证

- Ruff 通过；宿主/观察测试共 10 通过、1 AstrBot 真框架测试因当前环境未安装 AstrBot 而跳过。NoneBot 真实 2.5/OneBot 2.4 框架测试新增普通 priority 3 处理器：仅观察消息流经普通处理器，许可回复认领消息被阻断。
- 真实 TLS/HTTP 三产品联合测试使用实际 Companion `build_runtime` 配置初始化、Platform 认证调用和 Memory configured_app；故障待归档、恢复、撤销均通过。
- `uv run --no-sync python integrations/build_adapter_releases.py` 成功，ZIP SHA-256 `b66c8c20c167d543e916d4f45197bdbec5e235a2ad5d32704ccd666a7a9e9f6e`，wheel SHA-256 `ba008b1091eb3055c044ee93a84f4a2193aa55bb7cceede053da43f55b2c1822`。产物在 `.runtime/adapter-artifacts`，未入 Git。

## 部署与验收接口

- Companion 私有配置添加 `bot_observation_enabled: true`；`callers.platform.issuer` 必须为 `platform`，服务凭据可认证 v2 ingest/query。Memory 服务 URL、token 与 CA 使用既有 `services.memory`，对应 Memory caller 必须含 `observe_ingest`、`observe_query`。
- 隔离宿主环境安装新 wheel 或 ZIP，按 `integrations/nonebot/ADAPTER.md`、`integrations/astrbot/ADAPTER.md` 加载。宿主 SQLite 原路径保留，插件 v1 原配置保留；先检查 v2 capabilities 和在线账号，再由 Platform 管理员启用默认只观察账号。
- Linux 镜像预检：配置 JSON 指向实际契约和数据库，Companion 初始化后只出现单独 `.observations.sqlite`，v2 ingest/query 未授权返回 401，已授权观察事件可 durable inbox 入队；确认后台 flush 与 Memory TLS CA 可达。生产更新时明确备份宿主/Companion SQLite 及 WAL，并按维护窗口管理进程。

## 未完成与风险

- AstrBot 真宿主加载本机未验，须在对应宿主预生产环境补验；真实 QQ/NAS 和生产设备未操作。
- 短租约只表示近期 Platform 正常轮询；宿主无法瞬时感知远端故障。已认领事件可能暂时无回复，不能把宿主 ACK 解释成逐消息 Platform 成功处理承诺。队列容量耗尽时会计数丢弃并在管理页显示。

## 引用

- 根候选合同 `contracts/observation-source/candidate-v1`；Platform 与 Memory 同名交接文件。
