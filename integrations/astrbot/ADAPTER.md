# 天枢 AstrBot 适配器候选 0.4.0

适用于 AstrBot **4.27.3** 的 aiocqhttp/OneBot v11 QQ 纯文本。插件默认运行 `tianshu.bot-adapter/v1`：平台主动读取事件、提交已授权回复。安装后没有绑定，不接管聊天或发送消息。

## 安装、监听与获取密钥

1. 从项目根运行 `python integrations/build_adapter_releases.py`。将 `.runtime/adapter-artifacts/astrbot_plugin_tianshu-0.4.0.zip` 通过 AstrBot 插件上传页安装，或解压成 `data/plugins/astrbot_plugin_tianshu/`。在插件配置页保持旧 `enabled=false`。
2. 在 AstrBot 插件配置页设置 `adapter_port` 和 `adapter_listen_mode`，保存并重载插件：

   | 模式 | 监听地址 | 使用场景 |
   | --- | --- | --- |
   | `loopback`（默认） | `127.0.0.1` | 平台与 AstrBot 同机，或已有私网转发 |
   | `lan` | `adapter_lan_host` 指定的宿主私网 IPv4 | 两端位于可互通的私有局域网 |
   | `container` | 容器内 `0.0.0.0` | 由部署者把容器端口只发布到 NAS 私网 |

   `lan` 只接受 `10/8`、`172.16/12`、`192.168/16` 地址，不接受公网、回环或链路本地地址。RPC 对来源地址再做相同的私网或回环校验，且所有请求仍需独立 Bearer 密钥。`container` 模式应只将端口发布到可信私网；不要向公网发布。两端私网可直接使用 HTTP，平台需允许私有 HTTP，不要求另建 HTTPS 代理。`loopback` 的 `127.0.0.1` 地址只在平台与插件同机时可直接填写。
3. 在 AstrBot Dashboard 登录配置的管理员账号，打开插件详情页中的 `connection` Page，点击“显示连接密钥”。Page 通过 Dashboard 已认证插件 API 按需读取密钥，隐藏操作或关闭页面会清除页面里的文本。密钥首次加载时保存在 `data/plugin_data/astrbot_plugin_tianshu/adapter.sqlite3`，重启不变；不要复制到聊天或普通日志。若 Dashboard Page 不可用，可在宿主本机 AstrBot 工作目录运行：

   ```text
   python data/plugins/astrbot_plugin_tianshu/rpc.py show-key data/plugin_data/astrbot_plugin_tianshu/adapter.sqlite3
   ```

4. 天枢网页添加 AstrBot 时，填写 `http://<插件宿主私网地址>:<adapter_port>` 和密钥；容器模式填写实际发布到 NAS 私网的宿主地址与端口。网页探测真实在线 QQ 账号后，管理员选择账号、群或私聊、允许作者、角色并启用。离线时账号列表为空。插件只接管完全匹配的会话和作者；输入最多 8000 字符，回复最多 32768 UTF-8 字节。

AstrBot 4.27.3 的 `Context.register_web_api()` 固定映射到 Dashboard `/api/plug/...` 并经 `require_dashboard_user` 验证；插件 Page 通过 Dashboard 桥接调用该 API，另外核对当前 Dashboard 用户名等于配置的管理员用户名。平台 RPC 使用单独的 Bearer 监听端口，不能把 Dashboard 地址当作适配器地址。[固定版 Context](https://github.com/AstrBotDevs/AstrBot/blob/v4.27.3/astrbot/core/star/context.py)、[Dashboard 路由](https://github.com/AstrBotDevs/AstrBot/blob/v4.27.3/astrbot/dashboard/api/plugins.py)、[Plugin Pages](https://github.com/AstrBotDevs/AstrBot/blob/v4.27.3/docs/en/dev/star/guides/plugin-pages.md)。

消息队列、ACK 与 SDK 发送意图保存在 SQLite；同一个 `reply_id`/`attempt_id` 只调用一次原生 SDK。结果不明为 `unknown`，绝不自动重发；只有 aiocqhttp 返回真实 `message_id` 才为 `sent`。停用后不新增收发，未 ACK 队列被清除。队列与历史有硬上限，满后拒绝新工作。不能多进程共用数据目录。

本插件只在入队成功后调用 `stop_event()`；其他优先运行的回复插件仍需由宿主运维核对会话范围。若多个 aiocqhttp 实例同时连接同一 QQ self_id，该账号被视为不可用，以免发往不确定实例。

旧 pull 连接器仍在包内：只有在 AstrBot 插件配置页把旧 `enabled=true` 才运行，且新版 RPC 同时关闭。旧配置与行为见 [README.md](README.md)。切换时先停用旧平台连接，避免其他进程重复答复。

隔离验收：`python -m unittest tests.test_adapter_astrbot_host tests.test_adapter_rpc -v`，需 AstrBot 4.27.3 与 `httpx`。真实 `PluginManager` 加载/重载/卸载、Dashboard 登录鉴权、容器与可用 LAN 地址的 HTTP 回环已测；真实 NapCat、QQ 会话、NAS 与端口发布尚未测试。

## 持续运行与历史保留

入站容量只计算尚未 ACK 的事件，上限 10000。ACK 后立即清除传输正文，保留紧凑事件身份，旧 SDK 重放不会重新入队；升级启动也清除旧 ACK 行的正文。发送与绑定操作的幂等回执保留用于重试和状态核对，不计入终身发送次数配额；unknown 回执不会自动重发。紧凑身份和回执随处理量增长，需随适配器数据库备份，不通过清空账本释放容量。观察模式仍沿用已有 30 天及 20000 条传输历史保留策略，未 ACK 事件不清理；正文权威归档位于归档服务。
