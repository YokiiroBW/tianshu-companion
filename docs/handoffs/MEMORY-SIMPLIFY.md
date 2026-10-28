# Companion 自动记忆退役收口（2026-10-28）

状态：当前分支 `codex/memory-streamlining-20261028` 的本地 diff，未提交、未推送、未部署。起点为保全原群 SDK 改动并合入当前 main 会话窗口的 `da0cd33`；本块没有修改 SDK、`conversation_windows.py` 或压缩模块。用户明确不需要旧自动提取协议或旧数据兼容。

## 已退役与保留

- 自动入口只走 materials → conversation segment → schema 6。删除客户端旧 turn-commits、schema 2/3/4/5 协商/恢复/提交、旧事实提示/解析与语义补抽模块，未搬入新 legacy 文件。
- SummaryWindows 唯一生产调用方已使用共享群上下文，去掉仅为旧单人任务保留的 owner-only 开关和无调用的 context_changed 裁剪方法；保留当前群/私聊身份与 BOT 来源证明。`consolidation/window/check` 名称虽旧，当前 schema 6 材料校验仍依赖其 manifest，不属于可删除入口。
- 老 pending/blocked 条目分批标记 retired，每轮最多 64 条，不远程读写、不调用模型、不假称 completed；当前段选择、前序阻挡和能力展示排除旧协议。清理完成后本进程停止探查。保留当前 materials 回执的有界 backfill。
- 保留明确手工记忆、更正/遗忘、来源/版本验证、当前回执优先恢复、后台模型授权与静态配置、既有容量和调度；不是把所有结构化接口删除。
- 删除仅覆盖旧自动协议的 5 个测试文件及旧 TLS joint 启动脚本；通用 Harness、送达/来源/unknown/删除测试改用当前 materials/schema 6。保留 6 个通用 Memory joint 行为，使用 Memory 新材料/分段夹具。
- 当前使用说明改写为 `docs/memory-learning.md`，历史 handoff 不追写或抹除。

## 实际验证

解释器（以下 `python`）：`C:/YOKI/Codex/tianshu-peiban-bot/projects/tianshu-memory/.venv/Scripts/python.exe`。本产品 cwd；除 collect-only 外环境为：

```powershell
$env:PYTHONPATH='src'
$env:TIANSHU_CONTRACTS='C:/Users/Administrator/.codex/worktrees/context-capacity-20261028/tianshu-peiban-bot/.runtime/TS-400-owner/contracts/text-dialogue/v1'
```

1. 首轮当前协议与后台配置：

```text
python -B -m pytest tests/test_memory_segments.py tests/test_generation_context.py -q --tb=short -p no:cacheprovider
```

结果：54 passed、36 subtests passed、1 failed，10.18 秒。唯一失败是新增退役用例把旧任务放在真实 chat/flush 前，导致已被 worker 提前退休，错误预期为仅 64；已把夹具改为先建立当前段、再预置旧条目并重启。未将此轮整体称为通过。

2. 退役、受影响入口、unknown/遗忘/召回和传输关键点：

```text
python -B -m pytest tests/test_memory_segments.py::SegmentLearningTests::test_obsolete_jobs_retire_in_bounded_pages_without_blocking_current_segment tests/test_memory_segments.py::SegmentLearningTests::test_segment_context_uses_actual_group_authors tests/test_memory_candidates.py tests/test_memory_v2.py tests/test_core.py::CoreTests::test_outbox_retry_schema_and_no_duplicate_state_commit tests/test_core.py::CoreTests::test_partial_send_unknown_closes_without_replay_and_late_receipt tests/test_edges.py::EdgeTests::test_memory_unavailable_after_acceptance_does_not_hold_slot tests/test_query_transport.py::QueryTransportTests::test_exact_queries_retry_once_with_identical_wire_request tests/test_query_transport.py::QueryTransportTests::test_failure_limit_and_nonquery_paths_never_replay tests/test_query_transport.py::QueryTransportTests::test_memory_reads_leave_one_shared_slot_for_identity -k "not 300" -vv --tb=short -p no:cacheprovider
```

结果：33 passed、78 subtests passed、2 deselected，6.45 秒。

3. 最终只补验“清完即停止探查”和 schema helper 收口受影响项：

```text
python -B -m pytest tests/test_memory_segments.py::SegmentLearningTests::test_obsolete_jobs_retire_in_bounded_pages_without_blocking_current_segment tests/test_memory_segments.py::SegmentLearningTests::test_segment_context_uses_actual_group_authors tests/test_memory_segments.py::SegmentProtocolTests::test_model_output_schema_is_fully_resolved_and_keeps_memory_constraints tests/test_memory_segments.py::SegmentProtocolTests::test_shared_messages_keep_authors_once_and_do_not_mutate_portrait_basis -q --tb=short -p no:cacheprovider
```

结果：4 passed、5 subtests passed，1.11 秒。前述未变输入的有效结果复用，各轮有重叠，数字不直接相加。

4. 通用跨产品夹具仅加载检查：

```powershell
$env:PYTHONPATH='src;C:/YOKI/Codex/tianshu-peiban-bot/worktrees/TS-306/tianshu-memory/src'
$env:TIANSHU_CONTRACTS='C:/YOKI/Codex/tianshu-peiban-bot/.runtime/memory-streamlining/memory-owner/contracts/text-dialogue/v1'
$env:TS307_MEMORY_V2_REPO='C:/YOKI/Codex/tianshu-peiban-bot/worktrees/TS-306/tianshu-memory'
python -B -m pytest tests/test_memory_v2_joint.py --collect-only -q -p no:cacheprovider
```

结果：12 tests collected，exit 0（6 个行为 × 私聊/群聊）。没有执行这些联合用例，不能称联合验收通过。

5. 必要静态检查：

```powershell
$taskPython = @(git diff --name-only --diff-filter=AM -- '*.py')
& 'C:/YOKI/Codex/tianshu-peiban-bot/.runtime/storage-venv/Scripts/python.exe' -m ruff check --select E4,E7,E9,F,I @taskPython
git diff --check
```

均 exit 0。活动 src/tests/scripts 中已无旧自动协议入口、旧模块导入或旧测试夹具引用；保留的 `legacy_check` 是当前来源证明适配，不是旧自动提取器。

一个中间宽文件命令误选了既有 300 轮用例，已只终止该验证进程及其解释器 launcher，结果不计通过；随后换为上述明确排除 300 的定向命令。未运行完整产品、双后端、压力、真实 QQ 或付费模型；真实模型调用增量为 0。

## 集成边界

### 协调者随后完成的联合验证

协调者修正联合测试的 owner 合同选择：Consumer 继续用当前 CP 合同，Producer 可由 `TIANSHU_CONTRACT_DIRECTORY` 指定当前 Memory 合同。Memory-only 合同缺 Consumer 的 `web-conversation` 包，原首次执行在业务前失败；未修改任何发布合同。分别指定两产品合同后实际运行 `tests/test_memory_v2_joint.py -q -x --tb=short -p no:cacheprovider`：**11 passed / 1 skipped**，12.81 秒。跳过项是私聊夹具不具备群历史域；此结果覆盖前文“仅 collect、未执行”的阶段限制。0 真实模型调用。

Memory 生产端必须一起使用当前 schema 6 退役版本。旧模型输出不再恢复或迁移，新客户端拒绝非 6 且不发请求。没有清理真实数据库、生产存储或其他服务；本次只是代码与本地历史工作条目的明确退休。生产清空/发布由协调者按既有授权与实际范围串行安排。
