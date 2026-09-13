# NoneBot 薄桥边界

`tianshu_nonebot` 随项目安装，核心不导入 NoneBot。归一化只消费已认证的 OneBot11/TG SDK 事件，保留不可变作者、binding、会话/线程、原时间、消息 ID、修订、引用、提及及角色目标。文本 SDK helper 为 `normalize_onebot` / `normalize_telegram`；媒体原生段尚无受管 asset_ref 时明确不可用，不下载 URL 或伪造资源。标准入站合同中的媒体引用在核心中原样保留。

`Bridge.capture` 先持久化事件及唯一责任方。明确命令匹配由部署传入（完整命令或命令加空格参数）；direct 只交功能插件，companion 只进入核心，不双重回复。NoneBot matcher 应读取该结果并停止已交出的匹配传播；本任务不注册具体 GsCore 命令，也不启动真实 matcher/机器人。

`Bridge.flush` 在 429/依赖故障时保留待重试记录、错误原因、下次时间；没有成功受理回执前不删除或宣称已交付。NoneBot 宿主应把 pending/last_error 显示为等待或背压。`refresh_origin` 只能由可信 issuer 从原已登记 SDK 事件续签，不能让用户 payload 自签。未安装续签器时过期来源持续拒绝。

成功受理后，薄桥从已认证核心响应验证 request_id 与 collection_key，再持久保存 conversation_id、原 channel_key 和核心 receipt_id。issuer 用 `channel_mapping(channel_key)` 取得该权威映射，后续 resolve 必须返回具体 conversation_id；首次 resolve/register 可为 null，不阻塞首次登记。既有非空映射冲突则拒绝更新。映射也可供下行目的地核验使用。W=0 的首次 select 可在映射保存前得到暂时 503，由核心有界等待处理；严禁把未知来源 scope 当任意会话授权。

下行 `Bridge.send` 需要部署注入 `verify_send`（认证服务及实际来源/目标/角色权限核验）、当前 conversation→destination 映射和真实 SDK `send_native`；缺任意关键依赖就不可用。服务端在 TLS 认证入口接收已发布 `POST /internal/v1/conversation/send` 后调用此方法；此任务提供方法边界和测试，不随意增加来源签发/目标授权 HTTP 路由。`send_onebot_text`、`send_telegram_text` 是 SDK 回执映射 helper，只有真实 message_id 才能 sent；文本按纯文本发送，不启用模型返回的富文本指令。

adapter 自己的数据库在 native 调用前保存 unknown attempt。重复 reply/key 不再次 native send；同键异 payload 冲突。`reconcile` 仅返回本地已确认回执；本地也未知时返回 None。真实 NapCat/TG 的查询机制留部署验收，不能用重发充当核对。SDK helpers 尚未在实际 NoneBot/OneBot/TG 版本组合验证；本任务没有安装或运行机器人适配器。

核心包和薄桥包的 `pip install --no-deps -e .` 应在两个源码包都存在后执行。测试验证归一化、目的地边界、责任持久性、429 后重试、发送 unknown 和去重；合成 native callback 仅在 tests 定义，不能作为生产成功证据。
