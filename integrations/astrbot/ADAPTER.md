# 天枢 AstrBot 适配器候选 0.2.0

适用于 AstrBot **4.27.3** 的 aiocqhttp/OneBot v11 QQ 纯文本。插件默认运行 `tianshu.bot-adapter/v1`：平台主动读取事件、提交已授权回复。安装后没有绑定，不接管聊天或发送消息。

## 安装与获取密钥

1. 从项目根运行 `python integrations/build_adapter_releases.py`。将 `.runtime/adapter-artifacts/astrbot_plugin_tianshu-0.2.0.zip` 通过 AstrBot 插件上传页安装，或解压成 `data/plugins/astrbot_plugin_tianshu/`。在插件配置页保持旧 `enabled=false`。
2. 首次加载会在 `data/plugin_data/astrbot_plugin_tianshu/adapter.sqlite3` 生成稳定随机密钥。在宿主本机、AstrBot 工作目录下运行：

   ```text
   python data/plugins/astrbot_plugin_tianshu/rpc.py show-key data/plugin_data/astrbot_plugin_tianshu/adapter.sqlite3
   ```

   命令只在当前终端显示密钥。不要将终端输出、SQLite 文件或真实聊天数据复制到仓库或普通日志；重启后密钥不变。不需人工编辑配置文件。
3. RPC **只绑定 `127.0.0.1:18765`**。可在 AstrBot 插件配置 UI 改 `adapter_port` 并重载。若平台在其他机器，宿主运维需用私有网络 HTTPS 反向代理将 `/tianshu/adapter/v1/*` 原样转发至此回环端口，并限制平台来源。不可直接改绑公网。网页填写代理 HTTPS 地址及密钥；同机可填 `http://127.0.0.1:18765`，但平台必须显式允许私有 HTTP。
4. 网页探测真实在线 QQ 账号后，管理员选择账号、群或私聊、允许作者、角色并启用。离线时账号列表为空。插件只接管完全匹配的会话和作者；输入最多 8000 字符，回复最多 32768 UTF-8 字节。

AstrBot 4.27.3 的 `Context.register_web_api()` 由 Dashboard 固定映射到 `/api/plug/...`，先经 `require_dashboard_user` 验证 Dashboard 会话；它没有独立 Bearer RPC 的公开挂载/卸载接口。[固定版 Context](https://github.com/AstrBotDevs/AstrBot/blob/v4.27.3/astrbot/core/star/context.py)、[Dashboard 路由](https://github.com/AstrBotDevs/AstrBot/blob/v4.27.3/astrbot/dashboard/api/plugins.py)。因此本插件只开带 Bearer 的本机 HTTP，受控 HTTPS 发布属于宿主部署工作。

消息队列、ACK 与 SDK 发送意图保存在 SQLite；同一个 `reply_id`/`attempt_id` 只调用一次原生 SDK。结果不明为 `unknown`，绝不自动重发；只有 aiocqhttp 返回真实 `message_id` 才为 `sent`。停用后不新增收发，未 ACK 队列被清除。队列与历史有硬上限，满后拒绝新工作。不能多进程共用数据目录。

本插件只在入队成功后调用 `stop_event()`；其他优先运行的回复插件仍需由宿主运维核对会话范围。若多个 aiocqhttp 实例同时连接同一 QQ self_id，该账号被视为不可用，以免发往不确定实例。

旧 pull 连接器仍在包内：只有在 AstrBot 插件配置页把旧 `enabled=true` 才运行，且新版 RPC 同时关闭。旧配置与行为见 [README.md](README.md)。切换时先停用旧平台连接，避免其他进程重复答复。

隔离验收：`python -m unittest tests.test_adapter_astrbot_host tests.test_adapter_rpc -v`，需 AstrBot 4.27.3 与 `httpx`。真实 `PluginManager` 加载/重载/卸载、合成 SDK 与本机 HTTP 回环已测；真实 NapCat、QQ 会话、NAS 和私有 HTTPS 代理尚未测试。
