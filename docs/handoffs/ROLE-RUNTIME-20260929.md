# 多角色运行时交接：Companion

## 目标、基线与提交

- 任务：`C:/YOKI/Codex/tianshu-peiban-bot/docs/development/role-runtime-plan-2026-09-29.md`。
- 基线：`c24230216c41818c425d0b0a61f2f23ba0dfe578`。
- 固定提交：本文件所在提交（SHA 随总控交接消息提供）；未合并、未推送、未部署。

## 变更

- `RoleRuntime` 用现有 Companion DB `metadata` 表保存角色状态和操作回执；由已认证的 Platform 服务凭据调用管理端点。`expected_version` 与请求 ID 防并发覆盖和不同内容重放；重启恢复角色和网页 actor binding。旧启用请求在后续停用后重放只返回旧回执，不恢复运行状态。
- 可复用 Persona profile 显式复制/发布到独立角色 subject 并固定修订；每轮在入站/准备边界持久固定角色运行能力与人格快照。profile 后续修改不自动改变既有角色。原有部署角色可明确接管，`profile_id/profile_version=null` 保留已发布人格、actor ID、原 binding 和历史。
- 入站、排队准备、直接对话、记忆读取/写入及异步写入检查当前角色能力；停用拒绝新轮次和未提交动作。读取禁用时不注入该角色近期上下文或长期记忆。保留原出站 unknown/幂等语义。
- 候选接口 schema/实例见 `docs/contracts-candidates/role-runtime/v1/`，与 Platform/Memory 任务检出同版；未发布根合同。
- 返修：同一 Platform `application_id`、操作主体和配置签名才可将暂停的角色启用，源 profile 升版不改变已批准的暂停快照。新动态角色先注册为 unpublished，再使用现有 `apply_profile` 一次事务完成草稿、审批、发布、profile 回链、运行事实和操作回执；记录已认证控制台 principal 为操作主体，继承目标扩展字段，清除源档案未提供的可选文本。停用复用已固定修订，不读取当前可删除/升版的 profile。

## 实际验证

- `tests/test_role_runtime.py`：7 通过，覆盖 profile 固定/编辑/冲突、重放/重启/停用、完整 Core 重启后原有静态角色仍停用且 B 正常、生成中停用不发送且不阻碍 B、memory.read 在准备前撤销不读取旧快照、pending/blocked 记忆 outbox 在 memory.write 关闭和重启后不提交、同一作者两角色提示和 memory.write 限制。
- Core/model selector/Persona/Role 回归：40 通过；另执行短上下文、日记读限制、Life 独立 actor 与记忆候选相关 31 项及 10 个子测试通过；Ruff 通过。
- Platform 联合 HTTPS 测试真实调用本产品端点、人格准备和模型请求：`C:/YOKI/Codex/role-runtime-worktrees/platform/tests/backend/test_role_joint.py`，1 通过；录制上游与 BOT 回执见 Platform 运行证据。未使用真实聊天或模型。
- 返修验证：`tests/test_role_runtime.py` 8 通过，受影响 Core/model/Persona/Role 41 通过，Ruff 通过。真实 HTTPS 新增 Memory 回执丢失后源档案编辑、启用沿用一次审批/发布修订及控制台 principal 归因，以及 active 角色在 profile 升版后停用和旧 enable 回执不复活；Platform 联合三项通过。

## 部署与恢复准备

- 需要启用已有 `personas` 部署配置，保留原 `roles` 和 `bindings`、已有 origin/sender/Memory/Gateway 凭据。Platform caller 必须是单独已登记服务，管理端点只允许 `platform`；角色模型选择依赖已配置 `provider_self_service`/`provider_selector`。
- 维护窗口内与 Platform/Memory 成组备份 Companion 主 DB。该改动使用既有 `metadata` 表，不执行破坏性迁移；重启后角色元数据从 DB 读取，并以原部署 roles/bindings 为旧角色基线。回滚必须与 Platform/Memory 的 sidecar 一起恢复。
- 不自动接管 household。显式接管保留当前 Persona 已发布修订；如果操作者后续选择新档案，则需要明确应用，旧人格历史仍在 Persona revision 记录中。

## 未完成/风险与下一步

- Platform 失败矩阵 `docs/handoffs/ROLE-RUNTIME-FAILURE-MATRIX.md` 对 12 项场景逐一列出已测及缺口。queued/preparing/外部发送每个精确交界尚未全部故障注入；生成中停用、记忆 outbox 权限关闭和静态角色重启拒绝已验证。外部调用已开始后的结果仍以既有 unknown 机制核对，不能视作可撤销发送。
- 生活日记/短上下文跨角色专项、共享模型容量压力和真实 BOT/生产环境未在本任务验证。总控需审查三产品候选合同并组织部署演练。
