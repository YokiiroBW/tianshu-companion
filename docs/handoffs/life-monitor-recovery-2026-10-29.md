# 日程与自主目标巡检修复候选

本批只做隔离候选与必要验证，不合产品 main、不推送、不部署、不重试生产任务。监控主聊天接力交“记忆系统隔离测试接续”统一装配与发布。

## 基线与归属

实际只读核对 .230 为 R28，image `sha256:6d777fefab004de926bad3cce4f7ba1e581eb76cd5e83077692e0ba13f9d35fa`，owner `0a21e7276ef8b37d95773db32d38674dc6a97ee2eb28b20ae09085f28e8945e9`。Companion 运行及远程 main 均为 `5430300bc37e1ff17002c25e764acdd4689b9f24`；Platform 519d5f0、Memory 57a1a14、Gateway ed631d8、application 5badd01。

独立 worktree：`C:/Users/Administrator/.codex/worktrees/tianshu-life-monitor-recovery/tianshu-companion`，分支 `codex/life-monitor-recovery-20261029`。仅改 life_daily.py、life_goals.py、两个对应测试及本交接。不改 app.py/core.py/clients.py/model_selection 或 memory-learning/observation；根 index、旧工作树和 TS306/307 保持。没有新增依赖、合同、schema 或用户对话记忆语义变化。

## 实查与修复

- 匿名 role-2c3cee1d1d63 是既有静态 `actor:household` / world:household，没有对应 Platform roles 行。其当日日程是 R27 前遗留 interrupted/Fault、attempt=1；原 generation_turn_id/config_version 存在，但 model_request_id、generation_stage 和 receipt 缺失。现存 Gateway 回执未找到该配置版本的对应记录；缺记录不能证明未执行，原确切上游失败不能倒推。新启动恢复对当前有效的此类任务记录 `manual_retry_required` / `legacy_request_identity_missing`，保持原状态、次数、内容和任务身份；不自动重放，也不把历史过期计划变成当前计划。
- 既有显式 retry 仍是唯一人工恢复通道。恢复诊断进入原 attempt_history，新尝试清除旧恢复标签；已完成日程不重发。仅在隔离用例中验证了显式恢复、历史保留和完成，生产没有执行重试。
- household 当前自主规划的准备阶段有真实 query/403 forbidden。记忆窗口确认这是现有角色授权拒绝，不扩大 grants、不自动登记静态角色。候选沿原 failed/forbidden 终态保留拒绝，对静态未登记角色补 `configuration_required` / `legacy_role_authorization_denied` 诊断，不伪装成功。
- 澄汐近期仍有模型后 invalid_input，对应 DS 模型回执是 HTTP200/completed。旧实现直到整个决策应用完成才持久模型回执，失败时没有留下具体被拒字段，因此不能证明历史所有 invalid_input 的确切原因。候选复用既有 Gateway request_id 能力，在提交前留原请求号，模型返回即保存真实 receipt；区分 preparation/model/verification/choice_decoding/choice_validation/application，并记录校验字段。通过校验的决策另存 validated_choice，只有事务应用成功才是 completed/choice，不把未应用的意图当成事实。返回后的上下文核验超时及重启不会再误报模型未返回 unknown；原未返回请求仍 unknown。
- 可复现的本地语义缺陷：休息、继续或暂缓已有目标并不创建新意图，却被统一要求提供非空 title/goal。候选使 title/goal 仅在 new 时必填且非空；其他决策允许省略/null/空串。新目标文本上限、理由、整数延迟、严格枚举、精确 concern ID、来源复验和已有承诺优先级保持，未知 ID/非法输入仍拒绝。提示与实际校验对齐。此修复不等于已还原那次历史模型正文。
- 智乃旧活动 paused / step_unavailable / operation unknown 保持原保护。其 marker 缺原模型请求身份，本批不修改 checkpoint、清除未知标志、重放工具或伪造完成；仅复用原保护回归。澄汐、智乃已经 completed 的日程未操作。

## 必要验证

解释器使用现有 `projects/tianshu-memory/.venv/Scripts/python.exe`；PYTHONUTF8=1、PYTHONPATH 指本 worktree/src，TIANSHU_CONTRACTS 指协调工作树 `.runtime/al-core-verification/contracts-cp-v2/text-dialogue/v1`。全部模型、时钟、网络和数据是既有隔离夹具，没有真实模型或 QQ 测试。

累计 **18 个不同定向节点通过**。首轮 14 passed in 2.30s；仅恢复字段收口和休息场景变化后执行 8 passed in 1.73s（与首轮重合 5，另含原终态回执恢复 3）；最后静态权限诊断新增节点 1 passed in 0.89s。初次权限测试误假定夹具具有 role_runtime 属性，修正为其实际静态角色夹具后通过，非产品失败。未将重复节点相加为总数。

覆盖的新行为：休息/继续/暂缓不产生虚假新目标；新意图缺文本、浮点延迟、未知 concern ID 仍拒绝；提交前模型请求落盘、校验失败留 receipt/字段、后置核验超时及重启；旧无身份日程不重放、诊断不重复写、显式恢复归档；静态角色权限失败不发模型。复用并执行的受影响旧用例包括成功目标复盘延迟、原未知模型退避、显式恢复不重放未知工具、未知 Platform 不滚动新目标、日程未知回执三次上限，以及三种终态回执恢复。

完整修改 diff 已审阅，`git diff --check` 通过。没有全套、PG/SQLite 双矩阵、浏览器、负载或部署测试；本地没有 ruff 模块，未安装或修改共享环境。DailyLife 仍使用既有 metadata 字段和 status 文本条件，无共享方言/数值类型或 DDL 修改。生产只做既有正式存储及回执 SELECT，没有恢复写入。

## 统一发布交接

候选基于上述 C main，适合与记忆窗口的 memory_learning.py 和 app.py 恢复参数小块串行装配；本候选不占这些文件。发布者复用本定向结果，仅核装配差异与受影响入口，不分层重跑。

当前线上尚未安装本候选。household 的授权与旧请求缺失问题不能靠自动扩权或猜回执解决；若没有已授权管理入口，保持明确配置诊断。发布后若已跨日，优先让原日程时钟建立新日计划，不补写旧日离线经历、不重发 completed 计划。澄汐规划应依自然业务观察新阶段/字段诊断，不为验收另发真实模型测试；历史具体输出丢失不能伪称已经完全定位。所有旧活动和记忆数据保持。

只读私密证据保存在本 worktree `.runtime/life-monitor-20261029/read-life.remote-*.json`、`read-original-receipt.remote-*.json` 与 `read-legacy-config-receipt.remote-*.json`，未入库。最终候选 SHA 由本次 Git 提交及聊天交接给出。
