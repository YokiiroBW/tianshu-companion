# 记忆回执阶段受控恢复

基于 R28 Companion `5430300bc37e1ff17002c25e764acdd4689b9f24`，独立分支 `codex/observation-memory-recovery-20261029`。本批仅 memory_learning.py、app.py 中 memory-learning-recover 校验块及对应测试；日程/活动/目标由另一窗口独占，不改其文件，原 TS307 dirty 副本保持。

Memory 的空非文本材料/可选上下文导致首次合法 window 未准备，Companion 原 receipt 读取最终 blocked。生产元数据抽查未见这些 job 的模型提交记录，旧恢复 API 仅支持 model 阶段，不能合法接续。

沿同一 owner recovery API 允许 model_request_id=null，但领域强制为 blocked receipt、无 output、无 model_request 证据，actor/原 job、attempts/failure_count CAS 和请求消重保持。先核原 Memory 回执：完成/失效则封账；仍 pending 才沿原身份冻结并 pin 合法 window，给一次原 owner_recovery 授权。失败历史和计数不清，unknown model 不接受此分支，现有 model 阶段恢复不变。未新增接口或权限。

验证：新的 receipt 恢复、未知模型拒绝及真实本地 HTTP 的 platform 身份限制，加原 model 恢复共 4 passed；原取消/二次失败/HTTP 检查的未变有效结果复用；actor CAS 与原回执优先另 1 passed。首轮新增测试插入位置导致 NameError 已更正，恢复原测试全部断言后复验通过，不削减原行为。E4/E7/E9/F 检查与改动模块 I/格式、diff 检查通过；app.py 原有 import 排序提示保留，未顺手重排共享块。无模型/QQ 测试调用，无生产写入、全量或双后端矩阵。

发布后由唯一协调者核原任务身份、模型提交证据和 Memory 权威账本，使用同一合法 API 一次恢复并保存去正文收据。正常业务产生的真实调用另计，不制造测试消息或测试模型请求；发送未知仍保持待核。本批不单独推 main 或部署，等待日程与观测最终候选统一装配。

## 2026-10-30 余项诊断接续

R29已部署。原16 job恢复中10有resumed回执，Gateway核实10个原自然请求各一条completed回执；9个原Memory job各有一条账本，无重复，另1个proposal校验拒绝。6个503命令与无job capture仍受保护，无重放未知模型、清次数或新造任务。

本增量复用JsonService现有error_response钩子，在Memory候选语义客户端只接固定代码位置/原source owner、operation和HTTP状态，写回原材料/整理job的first_failure与last_failure。原schema/引用关系校验给ProposalStructureError具体字段路径，原接受谓词、预算和拒绝标准均保持；没有保留模型正文或把未应用输出写成记忆。旧丢失正文不能倒推出具体失败字段。

新增2定向、4个原协议节点及15 subtests、1个原恢复历史节点均通过（7个不同节点，复用未变成功结果），无真实模型/QQ测试。R30仅M/C wheels，其他产品、前端、root运行文件与配置保持。

### 结果未知写入的诊断保留

R30 的错误钩子会替换 `Fault.cause`，而 JsonService 的未知写入包装仍引用钩子执行前的局部字典，导致材料提交和分段创建丢失已返回的机器位置。改为沿用处理后的 `error.cause`，保持 unknown、原请求及不重放行为。隔离子用例先复现写入丢失，再修复通过；诊断文件 2 节点/2 子用例及原传输原因、不重放 2 节点/20 子用例通过。无真实模型或 QQ 测试；此修复只恢复诊断，不能据此宣布上游 503 根因已解决。
