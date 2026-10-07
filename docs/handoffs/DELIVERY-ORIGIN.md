# DELIVERY-ORIGIN — 2026-10-08

修复图片异步完成后原 origin 短租期过期导致 image-notice 永久 waiting；检查普通/流式回复、Direct、主动消息及未知回执路径。

Platform 在 bot-delivery/v2.1 context 接受 response/direct，重新验证既有原始记录、撤销位、注册摘要、owner、路由、scope/channel 与启用的连接后续接同一个 ref。沿用注册 TTL/绝对期限，不放宽撤销或换收件人，不创建子凭证。保留 v2 合同不变。

Companion 在原 preflight 与流式 finalize 前续接，PlatformBotSender 在 response/direct send 前续接；固定 ref 保持幂等语义。未知发送仍查询既有 expression，不重发。图片 notice 保存错误码/尝试时间并等待 30 秒重试，due 查询避免待重试记录阻挡后续。主动任务本已有实时 context；本次不改变其行为。legacy 非 Platform 通道无新的授权接口，保留原合同，当前部署使用 Platform v2。

验证：Platform bots/delivery/sources/web-console 53 项通过，额外实际 Companion 客户端→Platform HTTP 联测 1 项通过。Companion 全量 761 passed /26 skipped /8 failed/101 subtests；8 项缺少新检出的 runtime 路径配置，补齐后重跑 7 passed，余下 test_source_https 的旧夹具缺少 QQ 管理读取配置、在未改 app.build_runtime 校验处失败。末次生图/投递定向 8 passed，覆盖65秒生成、短暂 context 故障及恢复单次原图发送；格式及静态检查通过。新合同正例5/反例4通过；不声称真实模型/GPU/QQ测试通过。

未开始生产更新时的实现交接。实际发布版本、验证和后台恢复状态另见协调交付。无业务库迁移，生产更新使用新冷备且不重做已完成图片。

生产回读追加：原任务被关系投影的120秒缓存年龄拦截，隔离数据库副本上的实时owner校验通过。verify保留未来时间/版本/权限校验，去除在实时check之前阻止旧缓存的年龄上限；初次read仍校验120秒新鲜度。31项关系回归通过，覆盖600秒延迟后实时check、版本变化拒绝与撤权拒绝。
