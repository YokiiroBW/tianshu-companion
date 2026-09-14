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

只有获取响应头之前的 HTTPX RemoteProtocolError、ReadError、WriteError 可以触发恢复；第二次仍失败则沿用 dependency_unavailable。明确 HTTP 错误/拒绝/重定向、JSON/schema/业务校验失败、响应体中途断开、连接建立/证书错误、连接/读取/连接池超时均不重试。这个有意保守的范围解决已确认的复用连接关闭竞态，不是通用可用性重试策略。

请求只序列化一次，保留方法、URL、正文、关联头、凭据及固定 CA/TLS 配置。查询从发送到响应体读取共用 15 秒 wall-clock 上限，两次不重置预算；HTTPX 原有每阶段 15 秒限制不变。外部取消原样传播。没有新依赖、配置开关、全局禁用 keepalive 或延长超时；HTTPX/httpcore 自行释放失败连接，不关闭或替换共享 client。非白名单命令仍单次发送，原消费者继续处理不确定结果，例如 Core 发送响应丢失进入 reconciling，再到 closed_unknown，不能再发一次。

验证命令（项目根）：先显式设置 `TIANSHU_TLS_PYTHON` 为具有 cryptography 的解释器，运行 `.venv/Scripts/python.exe -m pytest tests/test_query_transport.py -q -s`。真实 TLS 用临时单日回环证书；Uvicorn 原默认 5 秒空闲关闭，HTTPX 正式 trace hook 在已复用连接的 send_request_headers.started 暂停 5.2 秒。断言第一次 RemoteProtocolError 且无响应头、随后新 TLS 握手、目标查询只入站一次、并发 T2 保持运行直至释放并正常完成。另用 TLS 服务收到完整写正文后断连，证明无重发与 Core unknown 封账；不信任临时 CA 的 client 必须失败。

这都是合成数据、真实传输的组件测试，不是 Platform/Memory/Gateway 三方 L0。原协调 W5 未记录底层异常，根因仍仅高置信推断；TS-050 独立受控复现已确认竞态，历史成功/失败证据均不在本任务修改。集成后须由协调者安排 TS-050 固定提交的真实三方 W5 重验。
