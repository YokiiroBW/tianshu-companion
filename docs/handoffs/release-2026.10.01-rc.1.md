# 2026.10.01-rc.1 正式关系合同绑定

固定交付 ea5c044719bfa76de384b0b69bbfd7ed1439996b，根契约发布 37b086f66ca8521ec065578b9126111d97d0f112，Memory 联合归档提交 1f3121c9758faeb31fc9d0fe2a54974c72505d55。DOMAIN 改为 role-relationship/v1，schema LF SHA256 改为 e96397bac2b6ad8ff9d23c023d7d3c5ba0701734b27053a05b9d0f65a7ff8ee6。既有配置键和内部类名保留；无自动发现或候选回退。候选 pin 不能继续作为正式域的 pin。

2026-10-01 实际执行 tests/test_relationships.py 与 tests/test_relationship_joint.py：40 passed / 2 warnings，13.54s，无跳过。包含真实 Memory 固定 Git 归档的 10 项 HTTPS/SQLite 联合测试；源代码必须来自该归档而不是同机可变检出。Platform origin/access、模型及渠道仍为明确合成组件；不等同于三产品全链或真实 QQ/模型使用。

本轮只改变发布元数据、固定测试 SHA 与文档，未改变关系结算或发消息行为。Ruff check/format 与 diff 检查通过。此前完整回归的基线失败与跳过保留于 TS-115 交接和协调发布清单，本轮没有重跑全量、没有删例掩盖问题。GitHub 推送、生产迁移、恢复和真实使用验收尚未完成。
