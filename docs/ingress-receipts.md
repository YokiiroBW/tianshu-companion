# 入站持久回执查询

`POST /internal/v1/conversation/ingest-receipt` 是内部只读端口，仅接受已有 platform / nonebot 服务身份。它读取该调用服务在原 `ingest-actors` 命令账本中的精确记录，不受理消息、不运行模型、不改变角色或权限。

请求为封闭对象：`schema_version: 1`、`request_id`、原 `idempotency_key`、`semantic_digest`。摘要沿用原 Core 命令规则：对去掉 command 的整个 fanout request 作 canonical JSON / SHA-256；因此 input、actor 列表和 schema_version 都参与绑定。已存在同 key 但语义不同返回 idempotency_conflict。

响应为封闭对象：`schema_version: 1`、原 `request_id`、布尔 `found`，没有原文、来源凭据或其他调用方的回执内容。`found=false` 仅表示未查到，不能证明消息从未执行或当前没有在途请求。

Platform 只在 found=true 时用原输入、原消息 ID 和原幂等 key 重新经过当前来源授权，取回 Core 同一次 admission 并补齐映射/入站确认。Core 的既有事务与命令账本阻止重复 collection 和模型调用。查询失败或找不到回执时保持 unknown；不能把“查询本身未执行”当作“原命令未执行”。出站未知状态仍只查原投递回执，不重新发送 QQ。

此端口是新增的本机服务接口，原 source-sync/v1 与 fanout schema 没有修改，无数据库迁移。先部署 Core 再部署 Platform 可启用恢复；旧 Core 不支持此端口时 Platform 保留 unknown。验证覆盖精确语义、调用方隔离、只读、重启、原命令去重，以及查询拥塞不授予重放许可。
