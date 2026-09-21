# 运行事件日志（contracts/diagnostics/v1）

本文件说明陪伴服务写出的运行事件日志：写什么、不写什么、写到哪里、写不进去时怎么办。
合同本身是 `contracts/diagnostics/v1`（版本 `1.0.0`，状态
`development_frozen_pending_joint_acceptance`）；本文件只描述本产品的实现，不改变合同。

实现只有一个文件：`src/tianshu_companion/observability.py`。它是宿主注入的端口，不认识
任何业务模块，也不开数据库、不做业务判断。

## 1. 一条记录的形状

一行一个 UTF-8 JSON 文档加一个 LF，单行最多 4096 字节，字段封闭为 11 个：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 固定 `1.0.0` |
| `timestamp` | UTC，毫秒精度，`Z` 结尾 |
| `service` | 固定 `companion` |
| `instance_id` | 每次进程启动生成的 UUID |
| `sequence` | 本进程内从 1 递增，只有被接受的记录才占用序号 |
| `event_id` | 每条记录的 UUID |
| `level` | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` |
| `event` | 已登记的事件名（见第 3 节） |
| `outcome` | `started` / `succeeded` / `failed` / `cancelled` / `unknown` / `rejected` / `degraded` |
| `correlation_id` | 32 位小写十六进制，或 `null` |
| `duration_ms` | 非负数，或 `null` |
| `error_code` | 已登记的错误码，或 `null` |

记录里没有 message、没有 extra、没有堆栈、没有 URL、没有请求头、没有请求或响应正文。
唯一的自由文本位置是事件名和错误码，而两者都必须先在代码里的登记表中出现，因此调用方
字符串不可能变成事件名或错误码，秘密也不可能被顺手写进日志。

## 2. 秘密与关联号

- 事件名、错误码、`level`、`outcome` 都是登记过的常量；`correlation_id` 只接受
  `^[a-f0-9]{32}$`。
- 异常文本永远不写日志。失败只用从**异常类型**查表得到的固定标签
  （`timeout` / `connect_error` / `read_error` / `write_error` / `protocol_error` /
  `transport_error` / `os_error` / `cancelled` / `invalid_input` / `fault`），
  因为 `str(exception)` 可能引用正文或令牌。
- 关联号由 `secrets.token_hex(16)` 生成，**不**来自用户身份、账号、会话或消息内容。
- HTTP 入站只接受合法的 `X-Tianshu-Correlation-Id`；非法值（包括看起来像令牌的字符串）
  一律替换为新号，且**不回显**给调用方。
- 同一个关联号在内部转发到下游服务时原样带上，请求体与鉴权头不变。

## 3. 事件覆盖表

`EVENTS` 是完整的静态登记表（`observability.py`）。下表列出全部已登记事件、它们的发出点，
以及它们记录的是"发生了什么"还是"开始/结束"。

**终态列**标记的是 `TERMINAL_EVENTS`：这些事件结束一次请求、轮次、尝试、投递或进程本身，
合同要求它们**持久**落盘（不只是写进内核），因此写者在写完这类记录后额外做一次
`fsync`——在**写者自己的线程**上做，业务事件接缝（含持有权威事务的代码）永远不等待磁盘。
其余事件只保证"已写入"，需要静默点时用 `flush()` / `await_flush()` 等一次真实排空。

| 事件 | 发出点 | outcome | 终态 | 说明 |
| --- | --- | --- | --- | --- |
| `runtime.started` | `app.create_app` lifespan 启动 | `succeeded` | | 进程完成装配并开始服务 |
| `runtime.stopping` | lifespan 关闭开始 | `started` | | 收到停止请求，开始收尾 |
| `runtime.stopped` | lifespan 关闭结束 | `succeeded` | ✔ | 后台任务已取消、库已关闭、owner 锁已释放 |
| `runtime.log_probe` | `LogAdapter.probe()` | `succeeded` / `failed` | | 运维显式发起的日志通道恢复探针（见第 5 节） |
| `runtime.background_work` | `app.run_loop` 每一轮真实做功 | `succeeded` | | 本轮确实处理了工作 |
| `runtime.background_failed` | `app.run_loop` 每一轮失败 | `failed` | | 每次失败都记，不采样、不去重，带失败标签 |
| `service.request.started` | `RuntimeEvents` 中间件 | `started` | | 入站请求开始，带关联号 |
| `service.request.authenticated` | 入站鉴权通过处 | `succeeded` | | 凭据被接受（不写凭据本身） |
| `service.request.finished` | `RuntimeEvents` 中间件 | `succeeded` / `rejected` | ✔ | 请求结束，带 `duration_ms`；容量耗尽时为 `rejected` + `log_capacity_exhausted` |
| `peer.call.started` | `clients.JsonService.call` | `started` | | 调用既有内部对端开始，带固定 `peer` 标签 |
| `peer.call.finished` | `clients.JsonService.call` | `succeeded` / `failed` | | 调用结束，带 `duration_ms` 与失败标签 |
| `turn.prepared` | `core` 准备轮次 | `succeeded` | | 快照已固定 |
| `turn.queued` | `core` 入队 | `succeeded` | | 轮次进入调度队列 |
| `turn.generation.started` | `core` 调用模型前 | `started` | | 开始生成 |
| `turn.generation.finished` | `core` 模型返回后 | `succeeded` / `failed` / `unknown` | | 失败带 `error.code`（Fault）或失败标签 |
| `turn.cancelled` | `core` 取消 | `cancelled` | | 轮次被取消 |
| `turn.delivery.started` | `core` 投递开始 | `started` | | 开始出站投递 |
| `turn.delivery.finished` | `core` 投递结束 | `succeeded` / `failed` / `unknown` | ✔ | 投递结果 |
| `outbox.flush` | `core` 出箱扫描 | `succeeded` | | 一次出箱处理 |
| `direct.request.queued` | `direct` 功能指令入队 | `succeeded` | | 指令请求进入独立执行单元 |
| `direct.request.cancelled` | `direct` 取消 | `cancelled` | | 指令请求被取消 |
| `direct.attempt.started` | `direct` 执行开始 | `started` | | 一次执行尝试开始 |
| `direct.attempt.finished` | `direct` 执行结束 | `succeeded` / `failed` / `unknown` | ✔ | 执行结果 |
| `direct.delivery.started` | `direct` 投递开始 | `started` | | 指令回复开始投递 |
| `direct.delivery.finished` | `direct` 投递结束 | `succeeded` / `failed` / `unknown` | ✔ | 指令回复投递结果 |
| `direct.delivery.deferred` | `direct` 让号等待 | `degraded` | | 出站序号仍被在途轮次持有，按 `outbound_band_busy` 延后 |

`direct.request.finished` 登记在 `EVENTS` 里，但**当前没有发出点**：功能指令请求的结束由
`direct.attempt.finished` / `direct.delivery.finished` 记录，请求层没有单独的收口事件。它保留
在登记表中是为了不改变已发布的封闭枚举；这是"登记了但未发出"，不是"已覆盖"。

`runtime.stopping` 是收尾的**开始**而不是终态，所以它不要求额外 `fsync`；进程真正的终态是
`runtime.stopped`。

**未覆盖项（明确写出，不假装覆盖）**

- 健康探针本身：`/health/live`、`/health/ready`、`/healthz` 不写任何记录。探针必须纯粹
  只读，"只读"包括不留记录；中间件对这三个路径直接放行（`health.PROBE_PATHS`）。
- 空闲轮次：后台循环什么都没做时不写记录。空转不是结果，按 0.05 秒/0.5 秒的间隔写空转
  记录会把预算塞满噪声。
- 循环的启动与停止：不为 7 个循环各写一条。循环启动不是结果，而且这些记录会写在进程
  开始服务**之前**——在通道有硬上限时（管道写满），噪声会变成启动卡死。
- 请求/响应的正文、消息内容、角色人格文本、模型提示词：一律不写。
- 数据库行级变化：本卡不新增审计表，事件只记录操作边界，不记录行内容。

## 4. 持久化、轮转与预算

| 变量 | 默认 | 范围 | 说明 |
| --- | --- | --- | --- |
| `TIANSHU_LOG_DIR` | 未设置 | 绝对路径 | **显式**开启文件输出；部署用 `/var/log/tianshu` |
| `TIANSHU_LOG_SEGMENT_BYTES` | 64 MiB | 正整数 | 单个分段上限，超出即轮转 |
| `TIANSHU_LOG_DIRECTORY_BYTES` | 1 GiB | 32 MiB – 64 GiB | 目录预算，超出即判定容量耗尽 |

- 未设置 `TIANSHU_LOG_DIR` 时是**非持久**通道：记录写到标准错误，`/health/ready` 报
  `logs=non_durable`，`status=not_ready`。不配置就等于不持久，绝不假装 `ok`。
- **标准错误也由独立写线程写**，与分段文件同一个理由：`emit` 跑在事件循环上，而"没人读的
  标准错误管道"（被 `subprocess.PIPE` 捕获、容器运行时不再读取）写满后会永久阻塞写入方。
  一旦在事件循环里同步写，整个进程就停在那里——请求、探针、后台轮次全部不再推进，直到有人
  排空那条管道（这正是 TS-101 返修中复现的 5 处超时的根因）。因此该通道同样是有界队列 +
  单写线程：写不进去的记录计为 `dropped`，**从不阻塞调用方**。
- 非持久通道的队列写满时**丢弃并计数**，不拒绝业务：该通道本身已经让进程 `not_ready`，它的
  承诺从来不是持久性，把"没人读的调试流"升级成服务中断并不更诚实。持久通道仍执行严格规则：
  写不进去的记录拒绝业务（见下条）。
- 文件按实例命名，轮转到 `.jsonl.1`、`.jsonl.2` ……，目录预算是所有分段之和。
- **容量基准在首次受理之前建立**（第二次返修 R1）。适配器在构造时（启动路径，不是请求路径）
  就扫描既有分段，把它们的字节数记为基准；`submit` 用它加"待写字节"加新记录长度一起判预算，
  写者在追加前再用同一条预算复核一次。之前的顺序是"`submit` 先按 0 记账、写者到 `_open` 才
  扫目录"，于是一个已经写满的目录在重启后**第一次受理仍然成功**，文件越过自己声明的预算
  （实测 +356 字节）。现在预满目录的第一次 `admit` 就是 `false`，副作用 0，且既有分段一字节
  不动。度量不会创建目录——只读探针构造适配器时不会在文件系统上留下任何东西。
- 容量耗尽（预算写满）时：**拒绝新业务**（业务请求 503 + `x-tianshu-log-capacity-exhausted:
  exhausted`），`/health/ready` 报 `logs=failed`、`status=not_ready`，普通事件被丢弃并计数。
  选择拒绝业务而不是"再宽容几条"，是因为宽容会让文件超过它自己声明的预算。
- 写入失败（IO 错误）时：**不向业务抛异常**，记录被丢弃、`log_unavailable` 留在内存、
  标准错误只写一条固定告警。恢复必须靠一次真正成功的写入。
- **单写者、单期限关闭**（第二次返修 R2）。分段句柄只由写者线程持有和释放。关闭是**一个**
  有界动作，覆盖排队、等写者、送停止哨兵、join 与释放句柄；超时后返回"未确认"，**不**清零
  仍在途的计数、**不**把句柄从写者手里抢过来关掉、**不**丢掉写者引用——记录要么被写者写完
  并由它自己释放句柄，要么被如实计入 `dropped`。`aclose` 取消只停止**等待**，它启动的那一次
  关闭仍会跑完，不会产生第二个 owner（之前 `aclose` 先 `_join_writer` 再同步 `close`，两次
  等待后又关闭了活跃句柄，写者随后把 `_pending` 减成 -1）。
- **静默点 = 已写入**（第二次返修 R3）。`flush()` / `await_flush()` 等到的是"每条被接受的记录
  都已被写者处理完"，不是"队列看起来是空的"。待写计数在记录**入队之前**发布，写者处理完才
  清除；因此一条仍在写者手里的记录永远不会被报成静默点，而一条写失败的记录会被计为
  `dropped`，并在 `admit` 上如实返回 `false`。
- 写者在任何单条记录的异常上都不会退出：一条记录失败只记一次 `dropped` 并置 `log_unavailable`，
  线程继续跑。否则一次瞬时 IO 错误会让写者死掉，此后每次受理都变成静默丢弃。

## 5. 探针与恢复

`LogAdapter.probe()` 是**运维显式发起**的恢复动作：清空失败标记、以 fsync 写一条
`runtime.log_probe`，若仍失败则恢复原有标记。它只由运维动作触发，不会因为后来某条普通
事件恰好写成功就悄悄恢复。

## 6. 不因日志失败而重发

日志失败与业务事实是两件事：

- 事件写不进去时，已经在途的副作用（已发出的消息、已提交的轮次）**不会**因为日志失败而
  重发；业务代码只看到"日志没写成功"，看不到"业务要重做"。
- 容量耗尽时中间件在**受理之前**拒绝请求：没有入队、没有调用模型、没有发送，因此不存在
  需要重试的东西。
- 探针请求（第 3 节未覆盖项）不写日志、不 tick、不迁移、不创建目录，所以日志目录在探针
  前后逐字节一致。

## 7. 健康检查

- `GET /health/live`：公开，只回答 `{"status":"alive"}`。它只说明进程在跑，**不**说明任何
  依赖可用，也不该被用来驱动重启策略。
- `GET /healthz`：既有地址，返回 `alive` / `configured` / `external_dependencies`。
- `GET /health/ready`：需要专用凭据 `TIANSHU_DIAGNOSTICS_TOKEN`（`Authorization: Bearer`）。
  缺凭据或凭据错误 401；未部署该凭据 503。正文封闭为
  `{status, service, checks}`，`checks` 的键固定为
  `configuration` / `logs` / `runtime` / `dependencies`，取值只能是
  `ok` / `failed` / `not_configured` / `not_verified` / `non_durable`。

| 检查 | 何时 `ok` |
| --- | --- |
| `configuration` | 已配置专用诊断凭据 |
| `logs` | 持久通道可用且未耗尽 |
| `runtime` | 已装配、owner 锁仍有效、能用一次有界只读语句读到一行 |
| `dependencies` | **本批恒为 `not_verified`**：本批没有真实 Memory、网关或渠道 |

`status=ready` 的条件是每个检查都处于"本条件成立"的取值；`dependencies=not_verified`
算成立，因为把未验证的远端当成失败会让进程永久 not_ready，那是另一种谎。

## 8. 容器健康检查

镜像的 `HEALTHCHECK` 只读 `/health/live`，从不读 `/health/ready`：用就绪状态驱动重启，
会让一个日志目录写满但本身健康的进程被反复重启，把容量问题变成重启循环。
检查脚本是 `scripts/container_healthcheck.py`，只用标准库、不依赖本包、不写任何东西。

它的输出是一组**封闭的静态类别**（第二次返修 R4），成功时 `healthy`，失败时非零退出并写
`unhealthy: <类别>`：

| 类别 | 含义 |
| --- | --- |
| `tls_verification_failed` | 证书链或主机名不被信任（`urllib` 会把它包在 `URLError.reason` 里，脚本按根因归类） |
| `unreachable` | 连不上：拒绝连接、超时、解析失败等 |
| `unexpected_status` | 连上了但不是 200 |
| `unexpected_payload` | 连上了、200，但正文不是 `{"status":"alive"}` |
| `invalid_timeout` | `TIANSHU_HEALTHCHECK_TIMEOUT` 不是合法秒数 |

异常原文、响应正文、URL 与路径**一律不输出**：这段标准错误会被容器运行时收集并被运维阅读，
而一个异常消息或响应体可能携带证书主题、主机名或与存活无关的 payload。类别说明"哪一类失败"，
细节刻意不复述。证书与主机名校验始终开启，`TIANSHU_HEALTHCHECK_CA` 只是**增加**信任锚，
没有任何跳过开关。

