# 天枢 NoneBot 适配器候选 0.6.0

适用于 NoneBot **2.5.0**、FastAPI driver、OneBot v11 适配器 **2.4.0**。默认没有机器人、会话或作者白名单，安装后不接管消息。网页配置只需插件地址和一次本机显示的连接密钥；平台内置固定 `tianshu.bot-adapter/v1` 协议。

## 安装与获取密钥

1. 从项目根运行 `python integrations/build_adapter_releases.py`，取得 `.runtime/adapter-artifacts/` 内的 `tianshu_nonebot_adapter-0.6.0-py3-none-any.whl` 和 `tianshu_storage-0.1.3-py3-none-any.whl`。将两个 wheel 一起带到 NoneBot 宿主的隔离环境，先安装共享 storage wheel，再安装 adapter wheel，不覆盖宿主框架依赖。通过宿主正常插件加载机制加载 `tianshu_nonebot.adapter_plugin`，须在 `nonebot.init()` 和 OneBot v11 Adapter 注册之后、`nonebot.run()` 之前。此为一次插件安装，不需为每个机器人编辑环境变量或配置文件。
2. 首次加载会在宿主工作目录 `data/tianshu_nonebot/adapter.sqlite3` 生成稳定随机密钥。在宿主本机同一工作目录运行：

   ```text
   python -m tianshu_nonebot.rpc show-key data/tianshu_nonebot/adapter.sqlite3
   ```

   只在当前终端显示密钥；普通日志、RPC 返回和网页回显均不包含它。保持数据目录私有且跨重启保存。默认 SQLite；可选 PG、环境变量及配置文件见[存储后端说明](../../docs/storage-backends.md)，激活 PG 后命令读取当前绑定账本。
3. RPC 挂在 NoneBot 现有 FastAPI driver 的监听端口，路径 `/tianshu/adapter/v1/*`。网页填写该宿主的私网 HTTP 地址和密钥，平台需允许私有 HTTP；若宿主已经使用受控 HTTPS，也可填写其地址。插件不启动第二端口或第二套 Core。宿主仍需把现有监听端口绑定或发布到平台可访问的私网，且不要向公网发布。
4. 网页探测从在线 OneBot SDK `get_login_info()` 读取真实 QQ 账号；离线列表为空。管理员选择账号、群或私聊、明确作者、角色，保存后再启用。群没有隐含“所有作者”权限。

入口 `tianshu_nonebot.adapter_plugin` 与历史 `tianshu_nonebot.plugin` 互斥，同一进程同时加载会拒绝第二个。旧 pull 模式及配置/API 保留，见 [README.md](README.md)；切换时先停用旧连接。NoneBot 2.5.0 没有通用插件热卸载 API，本插件路由在 driver shutdown 时移除，宿主重启用于切换插件集合。NoneBot 没有 AstrBot 式统一插件详情 Page：安装仍需宿主现有插件加载机制；密钥仍由宿主本机命令显示，这是当前候选的两个操作限制。

入站接受 OneBot v11 真实私聊/群聊文本、引用消息和精确 @ 账号；引用与 @ 作为结构化元数据交给天枢，不能改变作者或权限。媒体、@全体和匿名消息继续放行其他插件。消息先持久入队后才由 matcher 阻断下游。发送先持久意图、再调用 SDK 一次；`unknown` 不重试，`sent` 需要真实 `message_id`。停用后不新收发。SQLite 队列与发送历史有硬上限，满后拒绝新工作；不要多进程共用数据目录。

本插件 matcher 默认优先级 5；宿主上优先级更高或相同的其他插件仍可能处理同一消息，安装时需检查既有回复插件的会话范围。

插件自身不打印密钥或正文。NoneBot/OneBot 默认事件日志可能记录消息正文和作者，宿主运维需按其日志配置控制这些数据。

隔离验收：`python -m unittest tests.test_adapter_nonebot_host tests.test_adapter_rpc -v`，需上述框架版本、`uvicorn` 和 `httpx`。真实 NoneBot 加载、路由注册、关停及本机 HTTP 回环已测；真实 NapCat、QQ 会话和 NAS 尚未测试。

## 持续运行与历史保留

入站容量只计算尚未 ACK 的事件，上限 10000。ACK 后立即清除传输正文，保留紧凑事件身份，旧 SDK 重放不会重新入队；升级启动也清除旧 ACK 行的正文。发送与绑定操作的幂等回执保留用于重试和状态核对，不计入终身发送次数配额；unknown 回执不会自动重发。紧凑身份和回执随处理量增长，需随适配器数据库备份，不通过清空账本释放容量。观察模式仍沿用已有 30 天及 20000 条传输历史保留策略，未 ACK 事件不清理；正文权威归档位于归档服务。

## 直接接入 OneBot

同一安装包提供 `python -m tianshu_nonebot.onebot` 独立入口，只加载天枢适配器。模型、人格和上下文由天枢负责，启动和令牌配置见 [直接接入说明](../../docs/onebot-direct.md)。平台类型选 OneBot / NapCat，地址填写该接入服务的 HTTP 地址。

## 普通群消息参与候选（AL11）

适配器在原观察账本内保存宿主归属，普通非 @ 文本不会因此阻断其他处理器，也不会直接生成回复。只有上述独立入口实际构造的 NoneBot 2.5.0 / OneBot 2.4.0 宿主，且其处理器、规则、前后 hook 和任务集合保持受控，才支持自动领取。原观察捕获、显式回复未接管、精确路由无 owner、处理闭合四份成功收据齐全才为 `free`。其他 matcher 实际进入即 `handled_elsewhere`；未知注册图、异常、取消、缺少收据或旧记录只能为 `ownership_unknown`，不能用“没有发送”推断空闲。普通插件加载入口不授予此能力，修改配置字符串也不能开启。

新的内部操作位于原认证路径 `/tianshu/adapter/v1/participation/`：`capabilities`（请求 `account_id`）、`ownership`（请求 `native`）、`candidate`、`decision`、`claim`、`register`、`bind`。除 claim 直接返回领取结果外，后三类记录及 candidate/decision 均返回同名字段的原记录。`available` 只说明协议可用；实际支持还须 `claim_supported` 和当前 ownership。发行包内置封闭 DTO 检查器，精确候选 manifest 为 `347b60a1dde6967adf559597e5bd7dfaa948f5b268d158d04ea3e3ad78b7c826`，运行不依赖本机 workspace 或在线 schema。

同一原生消息跨角色共用唯一领取槽。领取同参重放先返回不可变结果，再给当前有效性；ACK 不释放领取。观察保留清理同时清掉正文关联，最小 native 摘要及参与 reply/attempt 身份保留，迟到 SDK 重放不能重新 free，迟到发送不能降级普通发送。新操作没有第二个 sender：派发前登记和原 reply 绑定完成、当前来源已确认后，仍调用原 messages/send/status，在 SDK 发送前复核当前 claim；unknown 和历史 sent 只恢复原回执。

升级必须同时交付构建脚本产出的 adapter wheel 与 `tianshu_storage-0.1.3-py3-none-any.whl`。保持原 `adapter.sqlite3`、instance、访问密钥和数据目录；若宿主挂载旧 `/app/tianshu_nonebot`，安装新 wheel 后应核实际 import 路径，避免挂载继续覆盖新代码。普通参与还需要平台 AL11 接线及 AL12 语义决定消费者；没有消费者时保持安静。以上是本地候选能力，不代表真实 QQ 已验证。
