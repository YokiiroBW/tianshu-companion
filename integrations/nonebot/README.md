# 天枢 NoneBot / OneBot v11 插件

此插件在现有 NoneBot 进程内运行。当前验证组合为 Python 3.12、NoneBot 2.5.0、`nonebot-adapter-onebot` 2.4.0；线上宿主版本未获知，安装前应先读取宿主 manifest 并在隔离环境中验证兼容性。首轮只支持 OneBot v11/NapCat 的私聊或群聊文本；群聊默认仅处理发给本机器人的消息。媒体、任意第三方 @、回复段、编辑、撤回、TG 均未启用。它不启动第二个陪伴 Core，也不使用 NoneBot `matcher.send` 回复。

## 部署前提

1. 平台部署方先登记对应 `bot_connections` 槽位：adapter=`nonebot`、`platform_id`、QQ 机器人 `self_id`、外部会话 `group:<群号>` 或 `private:<用户号>`、逐作者的 Sources input entry 和角色。未知作者会被平台拒绝。管理员在网页创建并启用连接，选择允许会话和角色，领取只展示一次的**连接专属**令牌。该令牌不是平台管理员或 Core service 凭据。
2. Core 部署的 `bindings` 中，对这些机器人来源登记 `service=platform`、`namespace=qq` 的 binding；仅将选中的 binding ID 放入 `bot_platform_bindings`，并配置 `services.platform_sender` 指向平台 HTTPS 地址与 **Core 专用** token。该 Companion service principal 需有 `source.input`、`origin.resolve`、`dialogue.send`，其 resolver 对齐 `caller=platform`、`purpose=dialogue`；平台内部登记者另需 `source.register`、`source.dispatch`、`mapping.prepare`。未选中的旧 qq/tg binding 仍走原 `services.nonebot`。跨产品来源登记由部署方核对，不靠网页临时创建。
3. 在运行 NoneBot/NapCat 的机器安装本仓库包。Python 3.12 隔离环境示例：

   ```powershell
   python -m pip install -r requirements-nonebot.txt
   python -m pip install --no-deps -e .
   ```

   发布安装可用 `python -m pip install '.[nonebot]'`；`requirements-nonebot.txt` 是本次实测环境的精确依赖版本。以上命令在 **Companion 项目根目录**执行。不要在生产宿主中盲目覆盖其已有 NoneBot 依赖版本。
4. 在宿主入口按原有方式注册 `nonebot.adapters.onebot.v11.Adapter`，然后调用 `nonebot.load_plugin("tianshu_nonebot.plugin")`。插件必须在 `nonebot.init()` 后、`nonebot.run()` 前加载。已有适配器不重复注册。插件启动后只监听显式允许的会话；插件本身不会远程安装或开启 NapCat。

## NoneBot 配置

在宿主的 `.env.prod` / 受控环境变量中设置（JSON 列表遵循 NoneBot 配置格式）：

```dotenv
TIANSHU_NONEBOT_CONNECTION_ID=网页创建的连接ID
TIANSHU_NONEBOT_PLATFORM_ID=部署登记的宿主实例ID
TIANSHU_NONEBOT_BOT_SELF_ID=机器人QQ号
TIANSHU_NONEBOT_ALLOWED_CONVERSATIONS=["group:已登记群号","private:已登记用户号"]
TIANSHU_NONEBOT_PLATFORM_URL=https://平台内部服务地址
TIANSHU_NONEBOT_TOKEN=网页一次展示的连接专属令牌
TIANSHU_NONEBOT_JOURNAL_PATH=/持久化私有目录/tianshu-nonebot.db
# 内部私有 CA 时：TIANSHU_NONEBOT_CA_FILE=/绝对路径/ca.pem
# 可选：TIANSHU_NONEBOT_PRIORITY=5
# 可选：TIANSHU_NONEBOT_GROUP_REQUIRES_MENTION=true
```

Windows 上把 journal 路径改成真实绝对路径。journal 是 SQLite/WAL，需为宿主进程独占且跨重启保留，目录仅授予宿主账号读取权限，不能放入 Git 或共享临时目录。一个连接实例使用一个 journal；多个 NoneBot 进程不能共用一个连接配置并抢占同一个会话。内部 API 地址必须是 HTTPS，且地址中的 DNS/IP 要与服务端证书 SAN 匹配；使用私有 CA 时将 CA PEM 只读挂载到宿主并填写绝对路径，插件仍验证证书链和主机名。公开的 `18446` HTTP 网页入口不能填作插件内部 API 地址。`TIANSHU_NONEBOT_GROUP_REQUIRES_MENTION=false` 只在管理员明确希望该连接接管白名单群内全部纯文本消息时使用。插件仅在本地白名单与平台授权同时匹配时认领事件；NoneBot matcher 以 `block=True` 阻断更低优先级插件，避免同一消息双答。宿主中更高或相同优先级的其他 matcher 仍需运维确认不会对同一会话发消息。

## 恢复与状态含义

入站事件先写入本地 journal 再请求平台。请求中断后本地标记 `unknown`，仅查平台 `/internal/v1/bot/events/status`；查不到时保持 unknown，不重发。平台返回 accepted 仅代表 Core admission，不代表生成或发送成功。出站先写 `claimed` 意图再调用真实 OneBot SDK；只有 SDK 返回真实 `message_id` 才写 `sent`。SDK 异常/超时、进程在调用中退出均记 `unknown`，重连只补发 ACK，绝不再调 SDK。平台领取同一 reply/attempt 的重复记录也不会重复发。平台禁用/撤权后拒绝新事件和新领取，已经执行的 SDK 发送无法撤回。

插件不会把消息文本、令牌、账号 ID 写入自己的日志；宿主 NoneBot/适配器已有事件日志仍需按其部署配置控制敏感内容。`/heartbeat` 只证明插件近期能认证访问平台；网页“在线”不等于 Core、模型或真实 QQ 发送已通过。当前仅完成本地合成测试，实机安装和收发要等用户指定账号及会话白名单。

历史的 `bridge.py` / `routing.py` 保留原 Core/NoneBot 直连薄桥与测试。新插件只用连接专属平台 HTTP 入口，不在机器人进程签发 Core origin，也不会复用旧桥中的已知授权宽口。
