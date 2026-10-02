# DQ02 适配器持续运行修复

基线：25778436a5e36895c60f0d7beb35fa5ba060fc30。分支：codex/quality-life-20261003。

入站只对未 ACK 事件执行 10000 条容量限制；ACK 清除传输正文，持久保留事件身份。启动也清理既有 ACK 正文。旧消息重放先查身份，即使待处理队列满也不会重新占位。使用部分索引避免容量查询随全部历史增长。

发送和绑定保留原有紧凑幂等回执，移除把历史总数当永久调用次数配额的判断。未知发送结果仍保持 unknown，重启和重复请求均不重发。观察入站已有的 30 天及 20000 条历史清理不变。身份/回执元数据随总处理量增长，这是保持长期幂等所需的保留，不承诺数据库总字节恒定；已 ACK 的聊天正文不再累积。

复用 shared/tianshu_adapter_rpc.py，通过同字节分发到 NoneBot/AstrBot rpc.py，未新增队列、框架、服务或依赖。

验证：新增超过 20000 条完成历史的三个场景，涵盖待处理满额、重复 ACK、旧事件重放、普通/观察发送、冲突、unknown 与重启。连同原 RPC 和观察测试共 12 passed（0.94s）；ruff check/format、git diff --check 通过。既有发布构建器成功生成 ZIP 和 wheel 并检查分发源码一致。产物仅本地候选，未在真实宿主安装或发送 QQ。

命令：`python -m pytest tests/test_adapter_capacity.py tests/test_adapter_rpc.py tests/test_observation_adapter.py -q`；`python integrations/build_adapter_releases.py`。使用既有 `.runtime/nas-a1-r1-venv`，没有改依赖锁。宿主集成和最终共享组件回归由本轮整体交付继续记录，不将当前合成用例称为实机验收。
