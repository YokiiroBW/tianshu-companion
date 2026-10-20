# 天枢 OneBot 直接接入

天枢接入服务直接连接 NapCat 等 OneBot v11 实现，使用已有 NoneBot 适配器收发协议。服务只加载天枢桥接插件；模型、人格、上下文与长期记忆由天枢管理。

## 本地运行

使用 Python 3.12，先安装随产品或适配器产物提供的 `tianshu_storage-0.1.0-py3-none-any.whl`，再安装项目现有 `nonebot` extra 或构建出的 `tianshu_nonebot_adapter-0.6.0` wheel。可选 PG 配置与固定共享 wheel 校验见[存储后端说明](storage-backends.md)。设置 `TIANSHU_ONEBOT_TOKEN`，或让 `TIANSHU_ONEBOT_TOKEN_FILE` 指向可读的本地令牌文件，然后启动：

```text
python -m tianshu_nonebot.onebot --host 127.0.0.1 --port 18765 --data-dir ./data/tianshu_onebot
```

NapCat 使用反向 WebSocket 连接 `ws://<接入服务地址>:18765/onebot/v11/ws`，Access Token 与上面的 OneBot 令牌一致。容器或另一台主机连接时，根据部署网络设置监听地址；该端口同时承载 SDK WebSocket 和天枢适配器 HTTP。

首次启动后，可在接入服务本机显示独立的天枢连接密钥：

```text
python -m tianshu_nonebot.onebot --data-dir ./data/tianshu_onebot --show-key
```

在天枢机器人设置选择 **OneBot / NapCat**，填写接入服务的 HTTP 地址和天枢连接密钥。这里填写接入服务地址，不是 NapCat 管理页面地址。账号探测成功后复用现有会话、作者、角色及观察设置。安装与在线不会自动启用任何会话。

`--print-config` 只打印非秘密的接入信息。`/health/live` 表示进程运行；`/health/ready` 只表示至少一个 OneBot SDK 连接在线，不能作为模型或真实 QQ 全链路验收。

## 容器

项目提供 `Dockerfile.onebot`。构建后持久挂载 `/data`，通过只读令牌文件或环境变量提供 OneBot 令牌。容器以 UID/GID 10001 运行，挂载目录应允许该身份写入。

```text
docker build -f Dockerfile.onebot -t tianshu-onebot:local .
```

本轮只验证本地进程和合成协议对端；容器构建、NAS 安装、真实 NapCat 登录与 QQ 收发以实际发布记录为准。

## 上下文和消息结构

SDK 原始消息链中的引用 ID 和精确 @ 账号会进入既有持久队列，再由 Platform 转换为 Core 的 `reply_refs` 和 `mentioned_accounts`。引用只在同一连接中匹配已经受理的消息；未知引用不补查其他房间内容。昵称、群名片和 @ 都不改变作者身份或管理员权限。

普通记忆新增和重复证据合并保留有效近期对话；遗忘、修订、当前事实替换和撤权继续使相关历史失效。发送仍使用原有回执，结果不明不自动重发。

TS-165：由平台管理接口登记的动态 QQ 绑定明确使用现实聊天输入模式（`input-mode:platform-qq`）。原先缺少该标记会使新消息被记为 unclassified，候选 outbox 因来源分类不足而丢弃；开着自动记忆也不会实际抽取。修复只为之后的新输入提供模式证明，不宣称聊天内容必然真实、不重标历史来源；静态绑定和虚构模式保持原配置。模型提议仍须经过 Memory 的来源、授权与内容核验。隔离测试同时检查发送、候选回执和抽取提交，不只查看启用开关。

观察模式与对话接管继续遵守已有范围设置；被动观察不会擅自获得回复权限。当前入站媒体仍按 `unsupported` 处理，不能把文字片段冒充完整媒体消息。AstrBot 和 NoneBot 插件仍可用于需要其其他插件的场景，同一会话只启用一个回复负责人。
