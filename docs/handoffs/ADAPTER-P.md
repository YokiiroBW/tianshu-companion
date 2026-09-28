# ADAPTER-P · Core 动态 Bot binding 交接

基线：`ad180ca34c36c657f4a8acccb1e731cd434c3e03`。本任务只改 Core `src`、定向测试与本交接；提交为当前任务分支 HEAD（固定 SHA 在总交接中记录）。

## 交付

- 显式 `bot_binding_management_enabled:true` 才组装动态管理器；必须有 `callers.platform.issuer=platform` 且 `platform_sender` 真实配置。未开启时新增端点返回 503；非 platform 已认证调用者返回 403。
- `POST /internal/v1/bot-bindings/{apply,status}` 只接受受限 Bot binding 字段，不接受 principal、origin、来源证明或任意数据库字段。apply 校验 actor 存在于当前 roles、binding ID 为 `binding:bot:<连接ID>`、不能覆盖静态 binding、修订连续且同 request_id/语义幂等。
- 动态行在既有 Core `metadata` 表持久化，不改主 schema。启用时映射到既有 Core ingest/来源/Memory 授权链；停用从 Core 权限映射撤下。SenderRouter 永久记住该动态 ID 应走 Platform 发送端；停用或重启后旧待处理投递不会回落旧 NoneBot 出口。
- 若已持久动态行在下次部署失去角色或与新静态 binding 冲突，Core 静态路径仍能启动；动态有效状态报告 disabled，Platform 不会把连接恢复为 ready。

## 部署 bootstrap 示例（总控统一装配）

在既有 Core 部署 JSON 中加入 `"bot_binding_management_enabled": true`。原有字段必须满足：

```json
{
  "bot_binding_management_enabled": true,
  "callers": {
    "platform": {
      "issuer": "platform",
      "token_env": "TS_PLATFORM_TO_CORE_TOKEN",
      "origin_service": "platform_origin"
    }
  },
  "services": {
    "platform_sender": {
      "url": "https://platform.example.internal:8443",
      "token_env": "TS_CORE_TO_PLATFORM_TOKEN"
    }
  },
  "roles": {"actor:a": {"...": "现有已批准角色配置"}}
}
```

这里只展示相关字段，`roles` 仍须是部署已有的真实完整角色定义；示例字符串不可当作可运行角色。两个 token 环境变量属于不同方向，不复用。无需让网页用户填写 Core 地址、binding 或服务 token。

## 验证与边界

```powershell
$env:TIANSHU_CONTRACTS='C:/YOKI/Codex/tianshu-peiban-bot/contracts/text-dialogue/v1'
.runtime/venv/Scripts/python.exe -m pytest tests/test_bot_bindings.py tests/test_bot_sender.py -q
```

定向覆盖持久重启、角色不在批准表、静态 ID 冲突、关闭开关、非 platform 调用者、issuer 不符、缺 platform_sender 与旧发送路由不回退。Platform 交接中的 `TS_ADAPTER_JOINT_CORE=1` 会通过真实 Core ASGI/TLS 管理端点与录制插件 HTTP 对端联合创建/启停。尚未以真实宿主插件、QQ 账号或 NAS 部署验证；不将合成对端称为真实 SDK 验收。

完整 `pytest -q` 首轮实际为 611 passed / 22 skipped / 6 failed；6 项直接读取独立工作树缺失的忽略文件 `.runtime/workspace-context.json`。在该工作树补本地 `{"workspace":"C:/YOKI/Codex/tianshu-peiban-bot"}` 后，仅重跑这 6 项所属 `test_persona_chain.py` 与 profile 定向，共 10 passed。对当前改动相关 `test_bot_bindings/test_bot_sender/test_bootstrap/test_source_sync/test_web_snapshot/test_core/test_edges` 另做定向，73 passed、4 subtests passed。未重复已通过的 611 项；全量首次报告不改写成通过。
