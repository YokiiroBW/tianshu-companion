# 天枢 NoneBot 适配器候选 0.2.0

适用于 NoneBot **2.5.0**、FastAPI driver、OneBot v11 适配器 **2.4.0**。默认没有机器人、会话或作者白名单，安装后不接管消息。网页配置只需插件地址和一次本机显示的连接密钥；平台内置固定 `tianshu.bot-adapter/v1` 协议。

## 安装与获取密钥

1. 从项目根运行 `python integrations/build_adapter_releases.py`，取得 `.runtime/adapter-artifacts/tianshu_nonebot_adapter-0.2.0-py3-none-any.whl`。在 NoneBot 宿主的隔离环境安装 wheel，不覆盖宿主框架依赖。通过宿主正常插件加载机制加载 `tianshu_nonebot.adapter_plugin`，须在 `nonebot.init()` 和 OneBot v11 Adapter 注册之后、`nonebot.run()` 之前。此为一次插件安装，不需为每个机器人编辑环境变量或配置文件。
2. 首次加载会在宿主工作目录 `data/tianshu_nonebot/adapter.sqlite3` 生成稳定随机密钥。在宿主本机同一工作目录运行：

   ```text
   python -m tianshu_nonebot.rpc show-key data/tianshu_nonebot/adapter.sqlite3
   ```

   只在当前终端显示密钥；普通日志、RPC 返回和网页回显均不包含它。保持 SQLite 数据目录私有且跨重启保存。
3. RPC 挂在 NoneBot 现有 FastAPI driver 的监听端口，路径 `/tianshu/adapter/v1/*`。网页填写该宿主的受控 HTTPS 地址和密钥；本机/局域网 HTTP 需平台显式允许私有 HTTP。插件不启动第二端口或第二套 Core。
4. 网页探测从在线 OneBot SDK `get_login_info()` 读取真实 QQ 账号；离线列表为空。管理员选择账号、群或私聊、明确作者、角色，保存后再启用。群没有隐含“所有作者”权限。

入口 `tianshu_nonebot.adapter_plugin` 与历史 `tianshu_nonebot.plugin` 互斥，同一进程同时加载会拒绝第二个。旧 pull 模式及配置/API 保留，见 [README.md](README.md)；切换时先停用旧连接。NoneBot 2.5.0 没有通用插件热卸载 API，本插件路由在 driver shutdown 时移除，宿主重启用于切换插件集合。

入站只接受 OneBot v11 真实私聊/群聊纯文本与 bot 自身 @；媒体、第三方 @、引用及匿名消息放行其他插件。消息先持久入队后才由 matcher 阻断下游。发送先持久意图、再调用 SDK 一次；`unknown` 不重试，`sent` 需要真实 `message_id`。停用后不新收发。SQLite 队列与发送历史有硬上限，满后拒绝新工作；不要多进程共用数据目录。

本插件 matcher 默认优先级 5；宿主上优先级更高或相同的其他插件仍可能处理同一消息，安装时需检查既有回复插件的会话范围。

插件自身不打印密钥或正文。NoneBot/OneBot 默认事件日志可能记录消息正文和作者，宿主运维需按其日志配置控制这些数据。

隔离验收：`python -m unittest tests.test_adapter_nonebot_host tests.test_adapter_rpc -v`，需上述框架版本、`uvicorn` 和 `httpx`。真实 NoneBot 加载、路由注册、关停及本机 HTTP 回环已测；真实 NapCat、QQ 会话和 NAS 尚未测试。
