# 逐群四态与兴趣参与

Platform 持有版本化群规则：`disabled` 停止新采集、回复与自动整理领取；`observe` 仅观察；`reply` 保留观察并响应真实 @ 或显式配置的字面子串；`active` 另允许对普通话题作有界兴趣判断。私聊沿原 policy 的 observe/名单，入口也先检查当前启用状态与租约。未绑定角色只归档，不编造角色或人物身份。

SDK 在正文规范化前检查真实账号和会话状态。只有 active 群进入 NoneBot 普通消息闭包证明；AstrBot 缺普通消息 claim 能力时明确不可主动参与。关键词不修改事件的 `mentioned`。共享策略和 RPC 模块随原发布构建同步到两个宿主。

AL12 复用 MemoryLearning 单自动阶段调度和 ModelSlots.background；独立 Gateway/selector 的实际 caller 为 companion_background，功能为 participation。未配置独立后台模型时不借前台身份。候选与所有权仍由 Platform/SDK 的 AL11 账本拥有，Companion metadata 只记实际模型尝试、同一精确决定及未知回执恢复。模型只能决定 engage 或 silent，不能选择地址、账号或权限。每群从实际开始判断起合并同一短窗口的后续消息，silent/无效/unknown 也不会逐条再调模型；候选原 60 秒期限不延长。engage 仅提交原 decision，由 Platform 原 claim/register/真实 Core 入站和发送链执行。

`/internal/v2/observation-source/processing` 是自动工作当前资格提示，不能代替来源证明。停止整理以正常让出保留 job/window/output/attempt，不写 blocked，不阻住已暂停的 capture_wait/commit_wait 所在前台。未确认物理会话或真实 channel 的旧材料保留并暂停，不猜 UUID。历史读取、删除、来源完整 AND 和 receipt-first 不变；已发 RPC/模型照原收口，模型输出先持久后等候。停用不删除历史，不补采停用期间消息，也不补发旧回复。

Platform 可用原管理凭据只读 POST `/internal/v1/bot-bindings/participation-status`、body `{}`，查看 consumer 的实际后台配置与错误；该读操作不触发模型、业务或持久写。active 是期望模式，宿主 claim 不可用、后台未配置或来源不可用时仍保持安静。
