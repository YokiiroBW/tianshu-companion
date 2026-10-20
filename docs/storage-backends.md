# Companion 与 NoneBot 可选存储

未填写后端配置时继续使用 SQLite。Companion 主库、被动观察分库与独立 NoneBot Journal/RPC 使用固定版本 `tianshu-storage==0.1.0`；产品自己的 schema、JSON 查询、索引、事务和恢复规则仍在各自仓库。安装共享 wheel 后再安装本产品：

```powershell
python -m pip install vendor/tianshu_storage-0.1.0-py3-none-any.whl
python -m pip install -r requirements-dev.txt
python -m pip install --no-deps -e .
```

随产品分发的共享 wheel SHA256 为 `e7c8ef910a43a5e2d1f426948fe19e4ea3942e1ff0618a35ecfae03bfedc1482`，不依赖未发布 PyPI 或开发 worktree 路径。

服务 JSON 可填写：

```json
{
  "database_path": "/data/companion.db",
  "storage_backends": {
    "postgresql_url": "postgresql://USER:PASSWORD@HOST:5432/DATABASE",
    "redis_url": "",
    "namespace": "tianshu"
  }
}
```

非空 `TIANSHU_POSTGRES_URL`、`TIANSHU_REDIS_URL`、`TIANSHU_STORAGE_NAMESPACE` 优先于文件字段。首次 PG 不可用时保持当前 SQLite；PG 连接和迁移校验成功后自动使用 PG。第一次激活以后，断线、移除 URL、命名空间不一致或未完成迁移都会明确拒绝使用旧 SQLite，防止产生两套事实。当前长持主连接断线时不透明续事务或重放请求：恢复连接后重新启动服务，重新取得 owner，再按已有恢复流程对账。

`database_path` 始终是原有本地路径锚点，不替换为 URL。主库的控制文件位于 `<database_path>.storage/`，大载荷位于 `<database_path>.files/`，原 owner 文件仍是 `<database_path>.owner`。图像暂存目录和配置中的其他文件根保持原语义。PG 按 `namespace + service + logical database` 分 schema；同一数据库上的独立安装/机器人实例应使用各自命名空间，不能把新安装指向另一安装已占用的目标 schema。

Companion 启动把 `main` 和启用时的 `observations` 作为同一激活集合。迁移读取原 SQLite 一致快照，保留文档、rowid 顺序、schema 版本、source_head 和所有幂等/回执状态，完成后才激活。原 SQLite 保留；应用新写入只去 PG，不双写。PG 主库用 session advisory lock 保持单运行时，写事务保留最外层提交和 source_head 推进，日记读取使用独立只读一致快照。原生 jsonb 路径、数字/布尔类型、数组展开、表达式及部分索引由 `storage_postgres.py` 明确拥有；索引定义不一致拒绝启动。

共享绑定已 `ready` 时，四种逻辑库均在任何 CREATE 之前验证产品必需表集及版本。关键表、版本表或整组产品表丢失都拒绝启动，不能建空表掩盖回执/幂等丢失。共享 0.1 迁移合同允许产品在 prepare 中只读绑定状态（`initializing` 为新库、`ready` 为已接续）；产品不修改共享绑定。恢复应补回一致 SQL/文件/控制副本后重试。

`replies.v2_request` 中的 base64 媒体先以内容摘要命名写入文件，SQL 保存文件引用和原完整请求摘要。读取时核对文件哈希并重建相同 wire，不能读取时明确不可用；unknown 仍只查询回执，不自动发送。大于等于 64 KiB 的 `write_materials.material` 同样外置；日记、人格文本、人工原创章节及其修订继续放 SQL。文件提交先于 SQL 引用提交，没有自动删除引用文件。已有内联载荷会在受控启动中外置，SQLite 首次转换前保留独立备份；表示方式转换不推进 source_head。

NoneBot 的独立进程可以使用不同 PG 地址和命名空间。两种已有接入方式保留：pull 插件的 `Journal` 和主动 RPC 插件分别登记 `journal` / `rpc`，不合并账本。NoneBot 配置支持同形 `storage_backends`；独立 OneBot 主机也可通过 `--storage-config /config/storage.json` 读取包含上述字段的 JSON，或直接使用环境变量。`python -m tianshu_nonebot.onebot --show-key --data-dir /data` 和 `python -m tianshu_nonebot.rpc show-key /data/adapter.sqlite3` 只读当前后端；配置文件可带 `--storage-config`，不会打开旧 SQLite 作为 PG 的替代。事件排序、连接绑定、pending/inflight/unknown、claimed→unknown、SDK 回执和 ACK 保持原规则。共享 RPC 源与 NoneBot/AstrBot 现有分发副本继续同步，AstrBot 默认账本没有切换后端。

此产品识别可选 Redis 设置，公共存储状态可用 `python -m tianshu_storage --config /config/companion.json --service companion status --state-directory /data/companion.db.storage` 只读查看。业务事实仍不进入 Redis；模型槽位、同检查点核验与短期上下文保持本地语义。首批 Redis 业务缓存由 Platform 提供。

备份/恢复必须同时包含 SQL、存储绑定、原加密/配置控制文件以及 `<database_path>.files/`。恢复文件后核对 SQL 中引用的大小与 SHA256，不删除旧恢复证据。PG 备份使用一致性逻辑备份并保持 schema/owner/版本；不能把旧 SQLite 覆盖回来当作 PG 恢复。容器两个 Dockerfile 从 `vendor/` 安装固定共享 wheel；本任务未构建或运行生产容器，也没有 NAS/真实 QQ/真实模型验证。

隔离 PG 专项：显式设置 `TS233_POSTGRES_URL` 指向可丢弃测试库，再运行 `python -B -m pytest tests/test_storage_backends.py -q`；每个用例使用随机 `ts233_` 命名空间。没有该变量则明确跳过 PG 用例，不能将 SQLite 通过算为 PG 通过。整个组件按 AGENTS 运行，合同使用与 Q12 基线匹配的发布快照。

原生实现依据：[PostgreSQL JSON 操作](https://www.postgresql.org/docs/current/functions-json.html)、[会话与事务 advisory locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)、[事务只读属性](https://www.postgresql.org/docs/current/sql-set-transaction.html)。
