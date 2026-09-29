# QQ-IDENTITY-20260929 · Companion

- 目标：在 QQ 会话读资料前、生成前和发送前核验稳定身份与管理版本；把固定规则和每条作者来源投影送入真实 Gateway 消息，模型输出只能走现有文本发送。
- 基线：`644929cd404b106a90abbf0ebe01a049f30a0521`；协调者后续多角色候选 `9863800f0fcca67f59aa285ae14802d391f77e50` 待本任务提交后对齐。交付提交为包含本文件的本地 `codex/qq-identity-20260929` HEAD。
- 变更：`qq_identity.py` 管理只读核验与服务生成投影；生产 QQ composition 必须有 `services.qq_admin` HTTPS 专用凭据，查验故障关闭；管理员版本固定到 turn 并在前置检查/生成/发送前比较。NoneBot、AstrBot 新适配器、旧 pull、观察及两个发送出口拒绝非规范 QQ 号/冲突来源；CQ 样式输出仍为单一文本段。三份 vendored RPC 字节一致，候选插件版本 `0.4.0`。
- 合同：`docs/contracts-candidates/qq-identity/v1` 为候选。Platform check 返回仅 `identity.explain`，不会开通工具、管理写入或跨平台关联。
- 实际验证：录制 Core 进入 Gateway `messages` 与 Gateway native body，验证伪 system/管理员正文仍在 user 数据、投影在 system 数据、接收目标未改；生成期撤销阻止发送、成员身份不继承、缺失 reader 启动拒绝、无效服务回执拒绝。NoneBot SDK 输出伪 CQ 文本经 OneBot 字符转义；AstrBot 保持纯文本 segment。受影响 Core、角色、适配器与观察测试，以及候选插件构建一致性检查已执行；Platform 工作树中的隔离 HTTPS NoneBot 宿主联合用本源码通过。
- 部署前配置：`services.qq_admin={url,token_env,ca_file}` 指向 Platform `/internal/v1/qq-admin/check`，对应 Platform 独立 `service=companion`、`qq.admin.check` principal，不复用发送/来源/网页登录令牌。现有授权链仍负责 memory.read/write、角色停用、会话/群私权限和送达。
- 未完成/风险：待与多角色新固定候选重基并对冲突重新验证；完整 AstrBot 4.27.3 宿主未在本环境安装验证；未使用真实模型、QQ、NAS、生产设置。直接构造 Core 的历史合成测试保留 `qq_identity_required=False`，实际 composition 的 QQ 准入必须开启且无 reader 会拒绝启动。下一步协调者审查并合入三个固定提交，发布根合同后才准备部署。
