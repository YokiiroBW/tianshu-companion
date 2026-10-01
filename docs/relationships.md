# TS-115 关系背景与候选事件

2026-10-01。Companion 是表达消费者；Memory 是 `(actor_id, person_id)` 关系类型、称呼、分数、阶段、冻结和事件账本的唯一权威。没有新增评分算法、关系表、人格权限或聊天入口。

## 装配与所有权

`relationships/contract.py` 校验显式外部候选；`client.py` 调用已有 Memory 服务；`projection.py` 装配表达与版本；`outbox.py` 使用已有轮次 outbox。`app.py`、`core.py`、`clients.py` 只做必要装配与调用。未改 Store schema、生活、人格和 QQ 身份/权限规则。

本地候选配置示例：

```json
{
  "relationships": {
    "enabled": true,
    "candidate_schema_path": "/contracts/role-relationship/candidate-v1/schema.json",
    "max_bytes": 2048,
    "timeout_seconds": 5
  }
}
```

未配置或 `enabled=false` 不装配消费者。该配置只拥有开关、投影预算与请求时限；地址、服务凭据和 CA 复用既有 `services.memory`。`score`、`relationship_type`、`bindings`、权限等配置一律拒绝。角色—人物绑定和冻结由 Memory 的管理端口拥有，Companion 不维护第二套状态，也不根据聊天称呼选择人物。

候选 schema 由绝对路径读取，不复制进产品。LF 归一化 SHA256 固定为 `f3b588591411f1ed4b8aa7c9003d201530644d4dfc02294bdd9e9d7f847214a3`；根 CRLF 原始文件 SHA256 为 `481a610729539005c42553c20336580acc9068b3729e43bdf575340698627bb1`。哈希不匹配启动失败。本批没有正式发布根合同，生产启用需要协调发布与对应版本固定。

## Memory 接口合同

四个 Memory 端口及管理规则见 TS-114 `docs/relationships.md`。本消费者只调用：

| 端口 | 请求业务字段 | 返回及核对 |
| --- | --- | --- |
| POST /internal/v1/relationships/read | 当前可信 pair，既有 origin | projection；候选 Private/PublicProjection、精确 pair、预算、新鲜度 |
| POST /internal/v1/relationships/check | 同一 pair、expected_version、同一 origin | check 恰为 `{version,current:true}`，版本必须等于 pin |
| POST /internal/v1/relationships/settle | 同一 origin、AffinityEventCandidate | SettlementResult；event_id/pair 精确一致，未知写入不作成功 |

信封恰为 schema_version/request_id/origin 和上述业务字段；响应恰为 schema_version/request_id 与一个结果字段。request_id 必须匹配。每次调用有总期限（0.01–10秒），投影预算256–8192字节，响应信封上限16KiB；复用 JsonService 的流式上限、TLS、拒绝重定向和固定错误码。read/check 允许一次同请求传输重试；settle 不自动传输重试。

## 表达、版本和隐私

当前身份链得出 actor/person/audience 后读取。私聊仅注入 view/pair/relationship_type/display_label/stage，分数、冻结游标、管理原因及权限不进入模型；群聊仅接受 PublicProjection 的表达提示，服务误返 PrivateProjection 会被丢弃。关系文字是表达数据，不是系统身份或工具授权；静态人格没有写入关系状态。

pin 放入既有 context_checks（scope/origin/版本域/关系版本/checked_at/schema_hash），不保存权威分数。prepare、生成前及每段发送前仍复核原有来源、角色、账号与范围，再检查关系版本。关系配置、来源或授权变更不能凭旧回执继续发送。此前回复的关系 pin 也随近期上下文/显式依赖继承；陈旧的普通历史整组省略，陈旧显式依赖或当前回复拒绝。

投影最多120秒；允许1秒有界时钟舍入差，不能接受任意未来时间。新鲜度以响应到达后时钟计算。初次读取超时、无权限或依赖故障时不使用关系背景，不伪造分数0/默认关系，普通聊天继续且不生成好感候选。如果生成已经使用了投影，其检查失败时停止旧发送。冻结不控制生活模拟或短期情绪。

## 事件与持久交付

只有 `sent` 的真实私聊轮次、所有回复确认为 sent 且有渠道消息ID、可信直接当前用户文字、当前来源和角色/账号权限仍有效时，才允许 conversation_completed 候选。引用、媒体、群聊、模型自述、历史材料、失败/部分/未知发送不构成候选来源。仅有模型回复不表示成功互动。

event_id 为 turn_id 与固定 kind 的稳定摘要；source_ref 为现有 admission selector 摘要（包含 actor），不是物理消息key；source_revision 与 committed event 的 occurred_at 固定。候选没有 delta，也不提交未经可信规则核实的负向/修复行为。

Memory 接受 committed turn 后，原 outbox 保存 `relationship_pending` 与候选；读后再次核对角色、账号、来源版本和持久轮次，再保存 submitting 意图后调用 settle。成功保存结算回执，失败发送不会创建该候选。没有新增表或第二执行库。

明确未开始的依赖故障按有界退避保留 pending；可能已经执行的超时/断连/取消或进程中断标为 relationship_unknown，保留相同候选，不自动重放、不宣称已接受。重启恢复 submitting 为 unknown，pending 原样验证后继续。Memory 本身的事件幂等保证相同候选重复提交不重复结算；未知结果的显式核对/恢复工具不在本批公共入口内。

## 本地验收边界

已实际通过40项专项：30项消费者边界、10项固定 Memory `0b82242590c35c5299721ad165bd5f5e9aa0f77e` 的真实回环TLS/SQLite联合。联合使用实际 Companion 来源事实读取，Platform origin/access、模型与发送渠道是明确合成替身。覆盖隔离、schema、成功/失败/重复、冻结不追补、群隐私、在途版本/撤权、超时、来源撤回和重启 outbox。不是实际 Platform、QQ、NAS或模型验收。

完整回归与基线 lint 等结果见 `docs/handoffs/TS-115.md`。TS-116 页面、场景表达/事件历史的进一步合同以及真实三模块联合尚需完成；不把单模块完成称为全功能上线。
