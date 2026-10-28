# 会话工作窗口与滚动压缩

Companion 在原聊天准备边界装配一份有界摘要和近期完整原文。窗口由规范化 conversation_id、actor_id、audience 隔离；群内多个发言人共用同角色窗口，私聊还核对 person_id。摘要是派生对话材料，不是 system 指令、人格、永久画像或 Memory 学习输入。原 turns、replies、归档与其删除权限保持原 owner；压缩不删除原文。

## 配置

原运行配置新增 `conversation_window`，省略时使用下列默认值。修改后按原运行配置加载方式重启；不新增自动 token 预检。

```json
{
  "conversation_window": {
    "text_bytes": 12288,
    "summary_bytes": 3072,
    "retain_turns": 4,
    "max_turns": 24,
    "max_messages": 96,
    "soft_percent": 70,
    "hard_percent": 82,
    "idle_seconds": 180,
    "cooldown_seconds": 600,
    "aged_seconds": 1800,
    "minimum_turns": 4,
    "minimum_bytes": 2048,
    "input_bytes": 49152
  }
}
```

`text_bytes` 衡量尚未移出原文与当前摘要的 UTF-8 文本量；百分比是这个配置量的比例，**不是供应商模型的实测 token 占比**。消息数包括已封盘输入及确认 sent 的回复，轮数按 Core turn 幂等记账。正常阶段不扫描历史全文算 token；首次历史恢复按旧到新每次最多32轮分页，后台每次最多读16个窗口状态。

`summary_bytes` 是摘要输出上限，`input_bytes` 是一次完整旧段的输入预算。`retain_turns` 保护最新若干轮不被移出；实际注入的近期原文仍服从原 `short_context` 的轮数、字节、时限与整组来源验证，且摘要和其它材料共同受现 TurnContext 16KiB 限额约束。需要展示更多近期原文时同时明确配置原 `short_context`，不能把它的历史默认值当作新窗口压缩阈值。

参数必须为整数。文本窗口范围2048–65536字节，摘要128–8192字节且小于窗口，近期轮数1–16，硬轮数2–256且大于近期轮数，消息2–2048，软比例小于硬比例；空闲/冷却0–86400秒、旧段年龄0–604800秒，最小新增1–128轮或1–65536字节，单次输入1024–262144字节且大于摘要上限。非法配置直接使用原启动配置错误语义，不静默改值。

## 触发与恢复

- 硬文本、轮数或消息数阈值在前台生成拿槽前处理。新完整段达到硬阈值时可以越过主动尝试的时间冷却；一次前台请求不会循环压缩。
- 后台每2秒沿原应用生命周期检查。必须没有实际前台需求、该会话无正在收件的 collection/活动 turn，并经过真实空闲时间。软水位，或最早未覆盖内容达到年龄且有足够积累，才能提前压缩；时间本身不触发。
- 所有触发仍要求完整可移出的旧前缀，以及最小轮数或最小文本量。冷却按最近一次尝试限制提前整理；同一失败输入范围至少等待 `max(30, cooldown_seconds)`，不会每 tick 再调模型。空摘要、超过上限、未缩短或检查到的部分中文引号锚点无法在输入中找到时，不替换摘要，也不推进覆盖位置；不会用字符串截断强行凑上限。这不表示自动核实了全部引号或语义。
- 一窗最多一个持久 pending。原文末尾正常追加不改变冻结旧前缀；编辑、撤回、来源/权限变化及 clear 使旧结果不可提交。完整 continuation 链一起处理，不切开未闭合片段。过大单个完整段无法装入时保留积压与原文，聊天继续使用有界近期材料。
- 模型执行 ID 在发送前落盘，调用后先保存正文和实际 receipt，再独立核验提交。重启发现持久正文时重新核验；只有 succeeded receipt 没有正文不能恢复摘要，记录 `execution_finished_without_persisted_text`，相同范围不自动重发。unknown 沿同一执行 ID 读回执，不能伪造成功或换 ID 重发。
- 回执查询暂时失败保留原执行 ID 并退避；已持久正文的临时来源校验失败保留 result 待重验，确切来源失效/权限拒绝才清理。模型选择与来源核验完成后、实际发送前还会再次让位给新到的前台收件。
- 回执恢复写回前重新读取当前窗口并核对 generation/旧前缀；查询期间的正常新尾部保留，clear 后的旧查询结果不能恢复被清空状态。
- 成功摘要保留一份精简 `last_execution`（执行 ID、配置版本、实际 receipt、覆盖位置），不复制正文到诊断记录。状态全部放原 metadata，无新增数据库表/结构迁移；历史与副本继续由既有备份机制负责。

硬触发与 idle 均复用 `ModelSlots.background`；idle 取得槽后再次确认无前台需求，尚未派发时让位。独立 background Gateway 已配置时使用它自己的授权线路，不能借前台 grant；没有独立端口则沿现 Life 装配复用正常线路。generate-only 的旧嵌入式端口不能执行可恢复摘要，会记 `dependency_unavailable` 并让聊天继续。单槽环境已经派发的背景调用不可抢占，仍可能使新前台短暂等待。

## 来源和内容边界

压缩提示要求保留人物归属、名字、具体时间/数字/单位、否定更正、条件、指代、未完问题/承诺和当前互动意图；计划与完成、猜测/转述与事实分开。仅真实 sent 且有渠道消息回执的助手文本可候选；存在外部工具来源、旧来源不完整、依赖核验不通过或摘要 generation 已失效的助手回复会保守省略。人类原文有自己的精确来源检查，不因助手回复被省略就一同丢弃。

旧摘要保存每个实际参与 scope 的历史权威快照，按 scope/history 域去重；明细不进入模型 prompt，也不设64个发言人的窗口门槛。沿原 Memory history/input-history 的撤销 epoch 与原 `_verify_checks` 重新核验，普通追加不会冒充来源撤销。参与者非常多时核验成本仍会增加，现有依赖预算继续有效。不可用时不注入未核验摘要，保留原文范围；首版没有自创摘要授权或逐句依赖协议。

这些确定性检查不等于摘要语义绝对保真。本次仅用合成模型端口验证流程，没有真实付费模型、多轮语义评分或 QQ 实机验收。

## 本地管理入口

`POST /internal/v1/conversation/window` 复用现有 `platform` bearer 身份，只允许实际已有窗口的精确 scope。该单产品管理 wire 尚无 Platform 页面消费，**不是已提供 QQ `/reset` 命令**。

```json
{
  "schema_version": 1,
  "operation": "status",
  "scope": {
    "actor_id": "actor:example",
    "person_id": null,
    "audience": "group",
    "conversation_id": "conv:example"
  }
}
```

字段严格；私聊使用 `self_private` 与真实 person_id，群聊 person_id 必须为 null。`status` 只返回轮数/消息数/文本字节、覆盖序号、版本、最后触发原因/错误/时刻、pending 状态和摘要字节数，不返回摘要正文。`last_reason` 为 `hard_limit`、`idle_high_water` 或 `idle_aged_prefix`。

把 operation 改成 `clear` 清除工作摘要和 pending，提高 generation，并把原文保留起点设为当前会话 turn_sequence；不会删原始记录，不会让清空前的旧执行回包复活摘要。其它群、角色或私聊不受影响。此操作不替代 Memory 的遗忘/删除功能。
