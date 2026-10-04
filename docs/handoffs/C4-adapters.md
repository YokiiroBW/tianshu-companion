# C4 渠道原件发送

基线 `a5725d04aba00762d66306ae4821a40b70f7e2ed`；分支 `codex/companion-complete-adapters-20261004`。仅修改 shared、NoneBot、AstrBot 适配器及直接测试，不修改 Companion Core。

Platform 既有发送队列把已授权、已物化的媒体原件随文本段传入适配器。共享 RPC 验证引用、类型、SHA256、严格 base64 和 32 MiB/最多四项预算；仅发送路由允许 45 MiB JSON。适配器不取私有 URL，不保存原件日志。NoneBot/OneBot 和 AstrBot aiocqhttp 使用原生 image/record/video 消息段；只有 SDK 返回真实 message_id 后才标记 sent。拿到引用、物化成功、SDK 超时均不冒称送达。未知回执和原有重启去重继续沿已有 ledger。

实际验证（隔离账号/loopback，无真实 QQ 网络）：

- 现有 pytest：`python -m pytest tests/test_adapter_rpc.py tests/test_adapter_capacity.py tests/test_nonebot_sdk.py tests/test_astrbot_connector.py tests/test_observation_adapter.py -q`：29 passed，1 环境跳过；该 HTTPS CA 场景随后配置 `TIANSHU_TLS_PYTHON` 为 Platform Python，`python -m unittest test_astrbot_connector.HTTPSCATests`：1 passed。
- 已安装宿主 Python `worktrees/ADAPTER-H/tianshu-companion/.runtime/adapter-venv/Scripts/python.exe`，设置 `PYTHONPATH=本检出/tests;本检出/integrations/nonebot`，`python -m unittest test_adapter_nonebot_host test_adapter_astrbot_host -v`：2 passed。实际加载 NoneBot 2.5.0/OneBot 2.4.0、AstrBot 4.27.3 的原生宿主；3.1 MiB 合成 PNG 的原始 bytes/SHA 和原生 message segment 一致；缺媒体物化拒绝、SDK ACK、重启与重放均覆盖。
- `python integrations/build_adapter_releases.py` 构建成功；三个 RPC 源副本一致；`git diff --check` 通过。

构建产物位于 ignored `.runtime/adapter-artifacts/`：AstrBot `astrbot_plugin_tianshu-0.4.0.zip` SHA256 `c83117bb0d71163ceeaf78f630e0b70f6a730c2abb01ec15692a3e98322556bf`；NoneBot `tianshu_nonebot-0.4.0-py3-none-any.whl` SHA256 `93672e8f4a4ce44f62ee5e9ca72f9ca144d1419429500626960183d4fd8b4295`。

部署只更新既有插件包，复用既有连接/access key/SDK 账号与 QQ 策略；不重置配置。32 MiB/四项按 expression 的累计限制由 Platform 唯一队列负责，append 重放不重复计数。QQ 本身的媒体/大小限制仍由实际 SDK 拒绝或未知回执明确呈现；未执行真实 QQ 发送或生产插件升级。音频/视频已接原生形状并校验类型，本次实际宿主大原件验证使用 PNG。
