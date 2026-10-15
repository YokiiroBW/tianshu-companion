# 查询连接恢复

TS-023 在 JsonService 中只允许以下调用恢复一次；并非按 GET/POST 或路径中的 read/select 字样推断安全性。

| 调用 | 已发布语义与决定 |
| --- | --- |
| POST /internal/v1/identity/resolve | text-dialogue/v1 semantics §5 明确只读；未登记仍由独立 register 命令处理。 |
| POST /internal/v1/origins/resolve | text-dialogue/v1 semantics §1 解析已签发引用，复核当前权限/期限/撤销；不新签发输入授权。 |
| POST /internal/v1/memory/select | text-dialogue/v1 semantics §5 与 source-sync/v1 semantics §5；重新查询仍执行来源屏障和版本校验。 |
| POST /internal/v1/memory/profiles/select | profile-memory/v1 只读选择与 source-sync/v1 屏障；原 scope/target/known version 不变。 |
| POST /internal/v1/memory/source-sync/check | source-sync/v1 interfaces.json memory.check 与 semantics §5；重新检查同一范围和来源。 |
| GET /internal/v1/model-requests/model:{32位小写hex} | text-dialogue/v1 semantics §7 的授权回执查询；仅覆盖当前 Gateway.generate 生成的 uid("model")，不重新生成模型结果。 |

Memory 查询与 check 的来源屏障可能提交水位/失效记录。因此“查询恢复”不表示无数据库写入或第一次未执行。重发原查询仍需重新建立屏障，现有 schema、请求关联、范围、known_scope_version、有效期检查继续执行；明确 scope_changed/version_conflict 不自动重试。

所有 JsonService.call 调用点已核对：Origins.input_access/resolve、Memory.identity/check_sources/select/profiles/commit、Gateway.generate（生成及回执）、Sender.send，以及 NoneBot Bridge.flush 的 ingest-actors。source-access/read 的 operation=input 可能冻结路由集合或签发 actor context，明确排除；即使名字含 read，也不属于本次恢复范围。register、turn-commits/consume、revise、ingest、ingest-actors、渠道 send 和 chat/completions 均不重放；未来新路径默认不重试。

获取响应头之前的 HTTPX RemoteProtocolError、ReadError、WriteError 可以触发一次连接恢复；第二次仍失败则沿用 dependency_unavailable。既有查询白名单在 HTTP 429/503 且响应明确为 `execution_state=not_started`、`retryable=true`、错误为 `queue_full` 或 `dependency_unavailable` 时退避重试。TS-160 将这层尝试从四次降到两次，减少与服务端 barrier、持久准备队列叠加后的重复工作。其余 HTTP 错误/拒绝/重定向、JSON/schema/业务校验失败、响应体中途断开、连接建立/证书错误、连接/读取/连接池超时均不重试。

准备工作全局受既有 model_slots 约束，按会话游标轮转，保留每会话两个活动轮次和既有队列上限。开始准备及模型前复用 Platform 的原回复 context 续办：仍核对当前角色、连接、策略、来源和精确账户/会话，不能本地延长授权。历史上下文保留 source_turn_id，原发言人的租约不足时续办原授权；不会拿当前提问者的授权冒充另一人。完整上下文组装后统一做模型前 preflight，各读取自身仍验证权限与版本。

收到语义未变的投递回执只保留原状态，不因新 request_id/observed_at 推进 Core 来源水位；实际发送、取消、失败及未知→已送达仍正常持久化。入站确认恢复和出站重发保持不同语义，见 [ingest-receipt](ingress-receipts.md)。

TS-165：上下文准备继续受上述全局工作数限制，但读取与组装期间不占用模型槽位。取得模型槽位后重新执行完整 preflight，排队期间的撤权、来源变更或版本变化仍阻止生成。`context_prepared_at` 和 `model_slot_wait_ms` 分别记录上下文准备结束和模型排队时长。

一次 `_prepare_context` 内，依赖和近期对话继承的完全相同核验按完整签名合并，成功或失败只在这次准备中复用；下一次准备、模型前和每次表达发送前都重新核验。不存在跨轮次或按时间失效的授权缓存。不同来源轮次仍逐项核对作者、范围、渠道、绑定版本及当前来源；需要续办时只续办该历史作者的原租约。

聊天、自动抽取和生活任务共用原模型并发总数（默认 4）。后台任务最多占一个槽位，空槽优先交给等候聊天；后台可运行且持续等候时，最多再授予三次聊天后安排一次后台，避免整理长期饿死。同类任务保持排队顺序；已发出的模型请求不中断、不重放，取消排队会归还已授予但尚未使用的容量。消息合并窗口与模型选择配置不随此优化改变。

同一 JsonService 查询并发最多四个，等待槽位与所有尝试共享原来的 15 秒总预算。业务准备仅在尚未调用模型、没有开始表达且收到上述可重试只读错误时重新入队，最多五次、总计不超过首次准备后的 30 秒及来源授权实际有效期；已接收的命令截止时间不再错误地截断排队后的处理。入站 SDK 的持久回执只有明确未执行且可重试才回到 pending，沿用同一事件与消息 ID，并由 Platform 重新核对当前连接权限；unknown、永久拒绝和已接收均不盲目重发。

请求只序列化一次，保留方法、URL、正文、关联头、凭据及固定 CA/TLS 配置。查询从发送到响应体读取共用 15 秒 wall-clock 上限，两次不重置预算；HTTPX 原有每阶段 15 秒限制不变。外部取消原样传播。没有新依赖、配置开关、全局禁用 keepalive 或延长超时；HTTPX/httpcore 自行释放失败连接，不关闭或替换共享 client。非白名单命令仍单次发送，原消费者继续处理不确定结果，例如 Core 发送响应丢失进入 reconciling，再到 closed_unknown，不能再发一次。

验证命令（项目根）：先显式设置 `TIANSHU_TLS_PYTHON` 为具有 cryptography 的解释器，运行 `.venv/Scripts/python.exe -m pytest tests/test_query_transport.py -q -s`。真实 TLS 用临时单日回环证书；Uvicorn 原默认 5 秒空闲关闭，HTTPX 正式 trace hook 在已复用连接的 send_request_headers.started 暂停 5.2 秒。断言第一次 RemoteProtocolError 且无响应头、随后新 TLS 握手、目标查询只入站一次、并发 T2 保持运行直至释放并正常完成。另用 TLS 服务收到完整写正文后断连，证明传输层不重发、聊天发送仍 unknown 封账；不信任临时 CA 的 client 必须失败。

TS-118：候选记忆事件由 outbox 业务层按退避重放同一不可变事件，以 Memory 消费账本取回遗失回执；不改变 JsonService 的写入单次发送规则，也不重发渠道消息。候选提议先持久化，结果不明时先读取处理回执，详见 `memory-learning.md`。TLS 用例断言重启后候选重放正文完全相同。

这都是合成数据、真实传输的组件测试，不是 Platform/Memory/Gateway 三方 L0。原协调 W5 未记录底层异常，根因仍仅高置信推断；TS-050 独立受控复现已确认竞态，历史成功/失败证据均不在本任务修改。集成后须由协调者安排 TS-050 固定提交的真实三方 W5 重验。
