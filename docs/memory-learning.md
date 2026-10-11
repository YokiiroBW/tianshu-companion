# 自动记忆闭环

正常对话沿用原有来源记录和候选 outbox。候选受理后，`MemoryLearning` 在现有 metadata 持久队列中登记候选 job，独立后台循环每秒取一条到期工作，不在聊天投递锁内等待模型。既有已受理候选通过有界 rowid 游标补入，每轮最多检查 64 条。

处理顺序：读取 Memory 回执/当前来源 → 选择 `function_id=memory` 模型 → 提炼带精确引文的事实 → 持久保存原提议 → Memory 再核验与提交 → 保存回执。模型选择复用原功能配置与默认回退，未启用动态选择的部署沿用固定配置版本。抽取与聊天使用独立 provider 会话，避免提炼提示或 JSON 污染聊天历史。模型输出仅为 inferred，不生成权限、身份绑定或关系分数。

现有 `automatic_memory_candidates=false` 同时暂停候选投递和整理，不删除积压。来源和授权由 Memory 在读取及提交时核验。失败指数退避，上限 300 秒；单轮总期限 45 秒。停止/重启不丢待处理任务。写入无响应时保留原提议，先查询回执；只有 Memory 明确 rejected 才重新生成。候选受理 outbox 的 unknown 也按相同不可变事件重放，复用服务端幂等，不重发聊天消息。

完成的事实由现有 memory-context 自动召回，普通问题无需“你记得吗”等触发词。纠正、遗忘及抑制旧来源重新出现继续由 Memory 现有修订与 proof 机制负责。本次没有另建第二套用户画像或长期存储。

内部运行能力端口增加 `extraction` 与 `retained_learning`（pending/completed/invalidated 的有无），不包含原文、人物标识或模型输出。completed 包括 committed/skipped，不能单凭任务完成就声称产生了事实；`memory_write_verification` 不自动升级为真实环境已验收。

跨产品联合用例：`tests/run_memory_learning_joint.ps1`，所有依赖路径均为必传，创建独立 `.runtime/memory-learning-joint` 数据、临时证书及回环端口；只有外部模型与渠道是合成替身，Platform、Companion、Memory、Gateway 均使用实际代码。用例保存否定/条件偏好、重启 Companion、关闭短期邻接，再验证普通问题的实际模型请求中包含长期记忆。它不证明真实模型提炼质量或 QQ 收发效果。

与 TS-117 成对集成，先 Memory 后 Companion，无新增数据库迁移。Platform 的功能模型选择服务本来已支持 memory，因此无需新增模型配置 API；现有 Platform 功能目录的 connected 静态文案尚未随本批修改，部署界面说明应由 Platform 集成时同步。
