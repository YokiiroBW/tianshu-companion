# MODEL-FUNCTIONS 陪伴交付

2026-10-10。本地已验证，未提交、集成、推送或部署。

基线 a5725d04aba00762d66306ae4821a40b70f7e2ed，分支 codex/model-functions-20261010。修改集中于 model_selection.py、life.py；复用既有 selector、固定版本租约、模型槽和 Gateway。

普通聊天请求保持原形状；日常/生活意图请求指定 writing 功能。日记沿用原开关，在已有 selector 时读取 writing 绑定；静态旧部署及长篇作品不改变原配置方式。等待后检查租约，显式日记重试使用新生成 ID。

model selection / life / 新 function selection 共 40 项通过，daily life 在补齐匹配的 life-read 合同后 28 项通过，合计 68 项，无跳过。Ruff 与 git diff --check 通过。测试模型均为合成替身，未调用真实供应商。

协议、配套平台、夹具版本与初始失败记录详见同批 platform/docs/handoffs/MODEL-FUNCTIONS.md；产品用法见 [模型分工](../model-functions.md)。需要先更新平台再更新陪伴；自动工具/搜索/代码分派及两次回复未实现。
