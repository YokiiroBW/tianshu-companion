# 部署基础（TS-101）

本文件说明如何启动陪伴服务的运行进程、需要挂载什么、如何判断它是否可用，以及**本批尚未
验证**的部分。运行事件日志的字段与事件表见 `docs/runtime-events.md`。

## 1. 本批已验证与未验证（先读这一节）

| 项目 | 状态 |
| --- | --- |
| 进程入口、绑定校验、TLS 规则、单 owner 锁 | 已在回环地址上真实跑过（见 `tests/test_runtime_cli.py`） |
| 健康检查端点、专用凭据、封闭检查集 | 已用 ASGI 真实请求验证（见 `tests/test_health.py`） |
| 运行事件日志：字段、轮转、预算、降级、关联号 | 已用真实文件系统验证（见 `tests/test_observability.py`） |
| `Dockerfile` / `.dockerignore` / 容器健康检查脚本 | **未构建镜像**。本批没有执行 `docker build`，因此"镜像能构建""容器能启动"都**不成立** |
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

就绪回答封闭为 `{status, service, checks}`，检查键固定为
`configuration` / `logs` / `runtime` / `dependencies`，取值限
`ok` / `failed` / `not_configured` / `not_verified` / `non_durable`。探针不 tick 调度、
不写快照、不迁移、不建目录、不写日志记录。

## 6. 运行事件日志

- `TIANSHU_LOG_DIR` 显式开启文件输出（部署用 `/var/log/tianshu`）；未设置即非持久，就绪
  报 `logs=non_durable`。
- 单分段 64 MiB；目录预算默认 1 GiB，可用 `TIANSHU_LOG_DIRECTORY_BYTES` 在 32 MiB –
  64 GiB 之间调整。
- 预算写满 ⇒ 拒绝新业务（503 + `x-tianshu-log-capacity-exhausted: exhausted`），就绪报
  `logs=failed`。这是刻意的：宁可拒绝业务，也不让文件超过它自己声明的预算。
- 写入失败 ⇒ 不向业务抛异常，内存中降级，标准错误一条固定告警；恢复只能靠一次真正成功
  的写入，或运维显式调用 `LogAdapter.probe()`。

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
`server.should_exit`，应用 lifespan 依次取消后台任务、关闭 Core、关闭客户端、fsync 关闭
日志，最后释放 SQLite owner 锁，并写出 `runtime.stopping` 与 `runtime.stopped`。
锁释放后可立即重启；锁没释放时第二个进程会以 `Database already has a running owner` 拒绝
启动，而不是同时写同一个库。

## 9. 已知边界

- 本批不新增数据库表、字段或迁移（`user_version=9` 不变）。
- 就绪的 `dependencies` 在本批恒为 `not_verified`：没有真实依赖可验证，就不写 `ok`。
- 空闲的后台轮次与探针不写日志记录；理由见 `docs/runtime-events.md` 第 3 节。
- 未配置业务请求返回 503；未配置的子系统明确不可用，绝不显示成成功。
