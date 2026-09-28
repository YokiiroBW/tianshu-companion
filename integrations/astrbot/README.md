# 天枢 AstrBot 连接器

此目录中的 `astrbot_plugin_tianshu/` 是可单独放入 AstrBot `data/plugins/` 的插件。它只连接天枢平台，不运行第二套模型、人格或记忆。首轮支持 **AstrBot 4.27.3 + aiocqhttp/OneBot11 的 QQ 私聊与群聊文本**。其他 AstrBot 平台适配器、媒体、编辑、撤回和引用消息没有实现，不会转成纯文本冒充支持。

## 安装和配置

1. 在天枢平台的部署配置中预登记 `bot_connections` 槽位及该会话的 Sources input entry、允许作者、角色和 Core binding。平台管理员从连接页面创建 `astrbot` 实例，明确启用并取得一次性连接 ID 和独立令牌。网页不负责安装插件。
2. 在 AstrBot 4.27.3 主机上，把整个 `astrbot_plugin_tianshu` 文件夹复制到 `data/plugins/astrbot_plugin_tianshu/`。在 AstrBot 插件页加载该目录。插件无需额外 pip 依赖；持久账本写在 AstrBot 标准 `data/plugin_data/astrbot_plugin_tianshu/`，不要放进插件源码目录。
3. 在插件配置页填写平台内部 API 的 HTTPS 根地址、`connection_id`、一次性 `token`、**aiocqhttp 平台实例 ID**（不是 AstrBot 镜像 ID）、该实例的机器人 QQ `self_id`，以及本地精确会话白名单，如 `private:123456`、`group:123456`。本地白名单应与平台槽位的外部会话键一致。默认空白名单和 `enabled=false`，不会接管消息或领取回复。
4. 默认只接管以 `天枢 ` 开头、位于本地白名单的普通文本，并向天枢提交去掉该前缀后的文本。`/` 开头的 AstrBot 指令始终放行。`capture_all_text=true` 只适用于明确划给天枢的专用会话；在这种模式下，该会话内其他普通文本插件可能无法收到已被本插件接管的消息。
5. 配置完成后启用平台连接实例和插件。平台连接状态中的“在线”仅代表心跳成功；需要分别检查入站受理、模型结果、领取回执与真实账号收发。本任务没有启用生产连接，也没有向真实会话发送消息。

若平台内部 HTTPS 使用自建 CA，将**签发服务端证书的 CA PEM** 只读挂载进 AstrBot 容器，例如 Compose 卷映射 `/srv/tianshu/certs/platform-ca.pem:/run/secrets/tianshu-platform-ca.pem:ro`，然后把插件的 `ca_file` 设为容器内绝对路径 `/run/secrets/tianshu-platform-ca.pem`。这里只挂载公开 CA 证书，服务端私钥留在平台侧。`base_url` 需填 AstrBot 容器可达的内部 API HTTPS 根地址，例如 `https://platform.internal:<TLS端口>`，主机名必须出现在服务端证书 SAN 中；插件不使用公开 `18446` HTTP 网页入口作为内部 API。平台尚未提供该 HTTPS 入口时，需先在平台侧配置 TLS 终止与内部 API 转发。CA 路径留空时使用系统信任库；不需要修改 AstrBot 宿主的全局 CA。证书链和主机名始终校验，不支持跳过验证。

令牌只给当前连接使用，不要填平台管理员令牌。插件只允许 HTTPS；本地隔离测试可在代码构造的 `allow_http_loopback=true` 下使用 `127.0.0.1`。日志只输出固定错误码，不打印令牌、正文或账号号值。`instance_id` 和入站/出站意图保存在 SQLite WAL 中，复制或恢复插件时要同时保留该数据目录。

## 收发语义

- 入站从 AstrBot 已认证 aiocqhttp 原事件获取 OneBot `message_id`、原始 `time`、作者、会话和完整文本段；与 AstrBot getter、配置的平台实例与机器人账号交叉核对。遇到媒体/引用/别人的提及、无稳定 ID 或未授权会话就交还 AstrBot 其他插件。
- 命中本插件的消息先持久记录摘要，随后调用平台 `bot/events`。本地重复事件不再次提交；网络结果未知只查询 `bot/events/status`，不重新发送原事件。平台负责来源登记、角色权限与 Core 调度。
- 回复由平台 `bot/replies/claim` 提供。插件在调用原 aiocqhttp `send_group_msg` / `send_private_msg` 前落 SQLite 意图；只有渠道 API 返回真实数字 `message_id` 才 ACK `sent`。超时、异常、缺回执和重启中断一律 ACK `unknown`，不自动再次调用 SDK。ACK 丢失只重放同一 attempt 的已持久回执。插件只在配置的 aiocqhttp 平台实例和精确会话上发送。
- 入站 `events.text` 最多 8000 字符。当前 Core 默认每次生成一段，单段最多 32768 UTF-8 字节；插件对这一区间的出站正文执行一次原生发送。超出 32768 字节的 claim 在调用 SDK 前 ACK `failed`，不会擅自分段；SDK 调用后的异常仍是 `unknown`。QQ 渠道实际上限尚待实机验收，不能由此推定所有合法段都必然送达。
- 管理员停用连接后，插件不能再受理或领取新消息；此前已领取且可能发出的回复仍用原有效连接凭据结算同一 attempt。轮换令牌后，旧凭据立即失效，需在插件配置中填入新令牌才能重放尚未确认的回执。

## 版本依据和未验边界

代码对照了 AstrBot [v4.27.3 事件实现](https://github.com/AstrBotDevs/AstrBot/blob/v4.27.3/astrbot/core/platform/sources/aiocqhttp/aiocqhttp_message_event.py)、[适配器事件转换](https://github.com/AstrBotDevs/AstrBot/blob/v4.27.3/astrbot/core/platform/sources/aiocqhttp/aiocqhttp_platform_adapter.py)、[Context 平台实例查找](https://github.com/AstrBotDevs/AstrBot/blob/v4.27.3/astrbot/core/star/context.py) 和 [插件开发指南](https://docs.astrbot.app/dev/star/plugin-new.html)。`event.send()` 在该版本返回 `None`，无法作为已发送回执；原生 aiocqhttp API 的数字 `message_id` 才能用作回执。元数据暂将插件限制在 `==4.27.3`，升级 AstrBot 后应复核 SDK 并调整版本范围。宿主实装、NapCat 网络回执及真实会话仍待总控授权的集成验收。

本地隔离测试：在产品根目录运行 `python -m unittest discover -s tests -p 'test_astrbot_*.py' -v`。
