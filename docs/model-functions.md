# 陪伴功能模型选择

2026-10-10，本地候选。

SelectionRequest 增加 function_id（默认 chat）。HttpDefaultModelSelector 在普通聊天时仍发送原请求形状；日常计划、阶段经历、生活意图以及日记生成明确传 writing。平台选择对应功能绑定，返回已有固定版本租约；Gateway 保持原协议和调用方式。

日记沿用 life_writing 开关。配置动态 selector 时可选择功能模型，无需额外静态 life_config_version；没有 selector 的旧部署继续沿用原显式静态版本。长篇作品保留独立的静态写作配置。日记在实际发起前保存生成状态和选择版本，等待模型槽后检查租约；失败/重启不自动重放，显式重试使用新的生成 ID。

本轮不自动触发代码/搜索/工具/记忆整理任务，不增加两阶段聊天编排。角色人格和记忆权威保持不变。

配套平台协议说明：MODEL-FUNCTIONS/platform/docs/platform/model-functions.md。部署需先更新平台的可选 function_id 接口，再更新陪伴；没有协议支持时生成会报失败，不静默切换模型。

验证：设置 TIANSHU_CONTRACTS 为完整且哈希匹配的发布目录，PYTHONPATH=src;tests；运行 pytest 的 test_function_selection.py、test_model_selection.py、test_life.py、test_daily_life.py。数据及模型均为隔离合成夹具，不能当作真实模型或 NAS 验收。
