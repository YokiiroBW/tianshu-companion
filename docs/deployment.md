# 部署基础（TS-101）

本文件说明如何启动陪伴服务的运行进程、需要挂载什么、如何判断它是否可用，以及**本批尚未
验证**的部分。运行事件日志的字段与事件表见 `docs/runtime-events.md`。

## 1. 本批已验证与未验证（先读这一节）

| 项目 | 状态 |
| --- | --- |
| 进程入口、绑定校验、TLS 规则、单 owner 锁 | 已在回环地址上真实跑过（见 `tests/test_runtime_cli.py`） |
| 健康检查端点、专用凭据、封闭检查集 | 已用 ASGI 真实请求验证（见 `tests/test_health.py`） |
| 运行事件日志：字段、轮转、预算、降级、关联号 | 已用真实文件系统验证（见 `tests/test_observability.py`） |
| `Dockerfile` / `.dockerignore` / 容器健康检查脚本 | **未构建镜像**。本批没有执行 `docker build`，因此"镜像能构建""容器能启动"都**不成立**；健康检查脚本本身用真实回环 TLS 监听验证过（见 `tests/test_runtime_cli.py`） |
| 真实 Memory / 网关 / 渠道 / PostgreSQL | **未验收**。`/health/ready` 的 `dependencies` 恒为 `not_verified` |
| 部署到任何真实主机或 NAS | **未发生**。本批只用合成数据与隔离测试 |

`Dockerfile` 存在不等于构建通过。任何"镜像可用"的说法都必须先有一次真实的构建与启动记录。

## 2. 进程入口

```bash
python -m tianshu_companion.runtime_cli \
  --config /config/companion.json \
  --contracts /contracts \
  --database /data/companion.db \
  --log-dir /var/log/tianshu \
  --host 127.0.0.1 --port 8765
```

入口只做四件事：解析部署路径、在开 socket **之前**拒绝不安全绑定、用既有工厂装配应用、
把终止信号变成有界优雅关闭（释放 SQLite 单 owner 锁）。业务规则一条也不在这里。

### 路径解析顺序

显式参数 → 同名环境变量 → 平台默认值。

| 参数 | 环境变量 | 容器默认 | 开发默认 |
| --- | --- | --- | --- |
| `--config` | `TIANSHU_COMPANION_CONFIG` | `/config/companion.json` | `.runtime/companion.json` |
| `--contracts` | `TIANSHU_CONTRACTS` | `/contracts` | `.runtime/contracts` |
| `--database` | `TIANSHU_COMPANION_DATABASE` | `/data/companion.db` | `.runtime/companion.db` |
| `--log-dir` | `TIANSHU_LOG_DIR` | `/var/log/tianshu` | 未设置（非持久） |
| `--host` | — | `127.0.0.1` | `127.0.0.1` |
| `--port` | — | `8765` | `8765` |

`--print-config` 打印解析结果后**不监听**即退出；输出只有路径与绑定，不含任何凭据。
`workers` 固定为 1：SQLite 只有一个所有者，调度状态只在一个进程内，第二个 worker 会被
拒绝而不是悄悄破坏两者。

## 3. 绑定与 TLS

- 回环地址（`127.0.0.1`、`::1`、`localhost`）不需要 TLS。
- **非回环绑定必须显式给出 `--tls-cert` 与 `--tls-key`**，且两者必须是存在、非空、绝对
  路径。缺少任一、文件不存在或为空，都在开 socket 之前以退出码 2 拒绝。
- 没有"临时关闭 TLS"的开关：那正是部署悄悄丢掉传输保护的方式。

## 4. 挂载

| 容器路径 | 内容 | 权限 |
| --- | --- | --- |
| `/config/companion.json` | 部署文档（含 `contracts_path`） | 只读 |
| `/contracts` | 已发布合同包（如 `contracts/text-dialogue/v1`） | 只读 |
| `/data` | SQLite 数据库与其 `.owner` 锁文件 | 读写，仅 10001 |
| `/var/log/tianshu` | 运行事件日志分段 | 读写，仅 10001 |

合同包从只读挂载加载并校验 manifest 与文件哈希；镜像里**不**烘焙合同副本，避免镜像内
的旧副本被误用。凭据一律通过环境变量注入，**不写入镜像、不写入部署文档**。

## 5. 健康检查

| 地址 | 凭据 | 用途 |
| --- | --- | --- |
| `GET /health/live` | 无 | 容器 `HEALTHCHECK`：进程是否在回答 |
| `GET /healthz` | 无 | 既有地址：`alive` / `configured` / `external_dependencies` |
| `GET /health/ready` | `TIANSHU_DIAGNOSTICS_TOKEN` | 现在是否可以服务业务 |

镜像的 `HEALTHCHECK` **只**读 `/health/live`。用就绪状态驱动重启，会让日志目录写满但进程
本身健康的情况变成重启循环。`scripts/container_healthcheck.py` 只用标准库、不依赖本包、
不写任何东西，因此应用导入失败时它仍然能给出答案。

它探测的地址与 TLS 参数与部署一致，且全部可覆盖：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `TIANSHU_HEALTHCHECK_URL` | 无 | 完整地址，给出后覆盖下面四项 |
| `TIANSHU_HEALTHCHECK_SCHEME` | `http` | 非回环或已启用 TLS 时设为 `https` |
| `TIANSHU_HEALTHCHECK_HOST` | `127.0.0.1` | 探测主机 |
| `TIANSHU_HEALTHCHECK_PORT` | `8765` | 探测端口，必须与 `--port` 一致 |
| `TIANSHU_HEALTHCHECK_CA` | 无 | 私有 CA 的 PEM 路径，用于校验服务端证书 |
| `TIANSHU_HEALTHCHECK_TIMEOUT` | `3.0` | 单次探测超时（秒） |

- TLS 校验**始终开启**：`https` 下要求 `CERT_REQUIRED` 且 `check_hostname` 恒为真，没有
  "跳过校验"的开关。自签或私有 CA 部署必须给出 `TIANSHU_HEALTHCHECK_CA`，否则探测失败——
  这是对的：一个连证书都不校验的健康检查只会把错误配置报成健康。
- 探测请求不带任何凭据，也**从不**读 `/health/ready`：它只回答"进程是否在回答"。
- 探测地址与端口写错（例如容器里监听 8765 却探测 8080）时，健康检查会失败而不是碰巧通过，
  所以 `TIANSHU_HEALTHCHECK_PORT` 必须与入口的 `--port` 保持一致。
- 失败输出只有**静态类别**（`tls_verification_failed` / `unreachable` / `unexpected_status` /
  `unexpected_payload` / `invalid_timeout`），非零退出。异常原文、响应正文、URL 与路径都不
  输出：这段标准错误会被容器运行时收集，而异常消息或响应体可能携带证书主题、主机名或与存活
  无关的 payload。类别说明哪一类失败，细节刻意不复述。类别表见 `docs/runtime-events.md` 第 8 节。

就绪回答封闭为 `{status, service, checks}`，检查键固定为
`configuration` / `logs` / `runtime` / `dependencies`，取值限
`ok` / `failed` / `not_configured` / `not_verified` / `non_durable`。探针不 tick 调度、
不写快照、不迁移、不建目录、不写日志记录。

## 6. 运行事件日志

- `TIANSHU_LOG_DIR` 显式开启文件输出（部署用 `/var/log/tianshu`）；未设置即非持久，就绪
  报 `logs=non_durable`。
- 单分段 64 MiB；目录预算默认 1 GiB，可用 `TIANSHU_LOG_DIRECTORY_BYTES` 在 32 MiB –
  64 GiB 之间调整。
- **重启不会买到容量**：适配器在受理任何业务之前先扫描既有分段，把它们的字节计入同一条预算；
  一个已经写满的目录在重启后第一次 `admit` 就是拒绝，且既有分段一字节不删、不改。
- 预算写满 ⇒ 拒绝新业务（503 + `x-tianshu-log-capacity-exhausted: exhausted`），就绪报
  `logs=failed`。这是刻意的：宁可拒绝业务，也不让文件超过它自己声明的预算。
- 写入失败 ⇒ 不向业务抛异常，内存中降级，标准错误一条固定告警；恢复只能靠一次真正成功
  的写入，或运维显式调用 `LogAdapter.probe()`。
- 请求/轮次/尝试/投递的**结束事件**、后台每一轮的**真实工作与失败**、取消/延后/鉴权通过与
  进程停止，都由写者在自己的线程上补一次 `fsync` 后再确认（合同要求终态持久）；业务接缝与
  权威事务都不等待磁盘。完整终态清单见 `docs/runtime-events.md` 第 3 节的"终态"列。
- **不要用 `subprocess.PIPE` 捕获运行进程的标准错误却不读取它。** 无人读取的管道写满后
  会永久阻塞写入方，所以适配器把标准错误也交给自己的写线程、并把它设为非阻塞：写不进去
  的记录计为 `dropped`，绝不阻塞事件循环。生产部署应让标准错误落到容器运行时可读的地方
  （默认即如此），或用 `TIANSHU_LOG_DIR` 走文件通道。

细节与完整事件表见 `docs/runtime-events.md`。

## 7. 容器镜像

`Dockerfile` 是 `python:3.12-slim`、非 root `10001:10001`、exec 形式入口、固定单 worker、
不烘焙凭据、不下载模型。`.dockerignore` 排除版本控制、缓存、`.runtime`、`tests` 与
`contracts`（合同走挂载）。

```bash
# 尚未在本批执行过，此处只是预期用法，不构成"已验证"
docker build -t tianshu-companion:ts-101 .
docker run --rm \
  -v /srv/tianshu/config:/config:ro \
  -v /srv/tianshu/contracts:/contracts:ro \
  -v /srv/tianshu/data:/data \
  -v /srv/tianshu/logs:/var/log/tianshu \
  -e TIANSHU_DIAGNOSTICS_TOKEN=... \
  tianshu-companion:ts-101
```

**上面两条命令在本批没有运行过。** 未构建的镜像就是未验证的镜像。

## 8. 停止

向容器主进程发送 `SIGTERM`（Windows 上是控制台控制事件）。入口把信号转成
`server.should_exit`，应用 lifespan 依次取消后台任务、关闭 Core、关闭客户端、关闭日志，
最后释放 SQLite owner 锁，并写出 `runtime.stopping` 与 `runtime.stopped`。
日志关闭是**一个有界动作**：一个总期限覆盖排队、停止请求、join 与释放句柄；句柄只由写者
线程持有和释放，超时时返回"未确认"并保留仍在途的计数，而不是把句柄从写者手里抢过来关掉。
停止意图是一份**状态**而不是队列里的哨兵，所以即使队列已满（哨兵塞不进去）写者也会自己
退出——IO 恢复后不需要任何人手工补一个信号。写者关闭文件属于 IO，它在状态锁**外**执行，
因此关闭方的 30 ms 期限不会被一次慢 `close()` 拖成 250 ms。关闭对**同一个** owner 引用做
判断与 join（写者退出时会清空自己在适配器上的引用，两次解引用会在退出窗口里读到 `None`）。
锁释放后可立即重启；锁没释放时第二个进程会以 `Database already has a running owner` 拒绝
启动，而不是同时写同一个库。

## 9. 已知边界

- 本批不新增数据库表、字段或迁移（`user_version=9` 不变）。
- 就绪的 `dependencies` 在本批恒为 `not_verified`：没有真实依赖可验证，就不写 `ok`。
- 空闲的后台轮次与探针不写日志记录；理由见 `docs/runtime-events.md` 第 3 节。
- 未配置业务请求返回 503；未配置的子系统明确不可用，绝不显示成成功。

## 10. TS-108 首版能力停用接口（产品内 v1）

部署 JSON 顶层字段 `automatic_memory_candidates` 只接受布尔值。省略为 `true`，
保留自动产生并提交对话候选的已有行为；首次隔离文字试部署必须显式写 `false`。
这是启动配置，变更需正常停机后重启，不支持运行时热切换。禁用只影响对话结束的
长期记忆候选事件，不关闭身份、来源验证、记忆读取、短期上下文或聊天投递。

公开只读入口：`GET /internal/v1/runtime/capabilities`，使用独立
`Authorization: Bearer <TIANSHU_DIAGNOSTICS_TOKEN>`。缺部署凭据 503，
缺/错请求凭据 401，Core 未装配 503；正常读取 200，**200 仅表示读取成功**。
该入口不属于跨产品 `diagnostics/v1` 健康/事件文档，不扩展其词汇；
`/health/ready` 与运行事件的字段、语义保持原合同。

禁用且没有历史候选时返回：

```json
{
  "schema_version": 1,
  "service": "companion",
  "automatic_memory_candidates": {
    "enabled": false,
    "generation": "disabled",
    "submission": "paused",
    "backlog_policy": "preserve",
    "retained_outbox": {
      "pending": false,
      "blocked_scope": false,
      "submitting": false,
      "unknown": false
    },
    "memory_write_verification": "not_verified"
  },
  "chat_audit": {"enabled": false, "state": "not_integrated"}
}
```

启用时 `enabled=true`、`generation="enabled"`、`submission="enabled"`；其余键不变。
`retained_outbox` 各布尔值表示该状态是否仍有记录（索引 LIMIT 1，只读，不是全表计数）；
禁用不修改/清除任何旧候选、不修复其 scope、不启动积压提交，也不为新轮次创建待消费
事件。再次显式启用才恢复原 `pending` / `blocked_scope` 处理，`unknown` 永不自动重发。
`submitting` 表示已记提交意图但尚未取得可信结果：禁用重启保持原字节；启用恢复时转为
`unknown`，不假定未执行。正常接受候选也不等于提炼/写入长期记忆，故这里不宣称成功。
Chat Audit 尚未接入，恒声明未启用；不生成归档成功回执。

状态入口只反映当前进程实际生效配置与本地持久事实，不写数据库、日志或调用对端，
也不作为容器重启探针。发布组合应同时校验配置 `false`、运行状态 `disabled/paused`，
以及超过 256 轮合成对话仍没有新增待消费候选；不能仅凭配置文件认定停用已生效。
