# TS-400：真实群基础目录与正文前停用边界

基线 `398803b3bad8ba267154576bcd7bf3cf793f2be0`，分支 `codex/group-directory-20261028`。本卡只改 SDK、共享 RPC 发行副本及定向测试；Core、Memory、配置、依赖和生产未改，未推送或部署。

两宿主收到实际群消息后先核宿主/self/group身份，在四态正文门之前登记账号＋群ID。目录复用 AdapterService 的 `meta` 表，不保存正文、作者或消息，不创建观察/模型任务。群资料从原 SDK `get_group_info` 异步补充，最多4个在途任务、每群300秒节流、单次3秒；首见立即可读未知字段，失败不清旧资料，不拉全群或成员。NoneBot原 lifecycle drain、AstrBot close 都在关库前收尾刷新。

原认证 RPC 新增 `/tianshu/adapter/v1/observation/groups`，请求 `{account_id,cursor,limit}`，仅接受在线账号；不要求观察启用、角色绑定或观察lease。按账号和群ID持久去重、keyset分页，完整行还须落在原64KiB响应界内。仅返回约定的群ID/name/counts/时间与next_cursor；observation capabilities增加 `group_directory_version:1`。

共享 group_policy 的未知群默认停用：normalize复制default后只将mode置disabled，保default.actor_id/keywords和显式groups；legacy仅保显式list规则，effective即使收到未normalize旧default也不会启用未知群。私聊分支不变，已有精确binding及明确groups保持原资格。RPC及policy均同步到两宿主发行副本。

## 实际验证

- 定向 R3：**33 passed、0 skipped、0 failed/error**，2.73秒。仅运行新 `test_group_directory.py`、原 `test_adapter_rpc.py`、`test_group_modes.py`、`test_observation_adapter.py` 及 `test_onebot_host_assembly.py::test_completion_lease_survives_waiter_timeout_and_cancellation`。覆盖真实NoneBot SDK入口和ASGI新路由、AstrAdapter停用正文/作者访问抛错、伪self/group与私聊不登记、目录重启/分页/UTF8预算、异步节流/失败保值/有界drain、未知群与显式群及私聊对照。没有执行完整宿主或Core套件。
- 使用现成 `.runtime/q21-venv/Scripts/python.exe`，PYTHONPATH优先本worktree源、SDK和根storage源，另复用根 `.runtime/storage-venv/Lib/site-packages`；显式SQLite，未安装依赖。R1缺redis、R2缺aiohttp均在collection退出，0业务节点执行，原日志/XML保留。
- 定向 Ruff check通过；`git diff --check`通过。原 `integrations/build_adapter_releases.py` 成功核对共享副本并离线构建ZIP/wheel，没有下载或安装。
- 日志/XML：`.runtime/ts400-r3.log`、`.runtime/ts400-r3.xml`；XML SHA256 `087bcc114aace6a50514ca7483709c255eda35578bbb159f57489bb24b81a857`。
- 最终 AstrBot ZIP SHA256 `c13fa0ae298d0f0623a92b92fc7f687dfff088069dc03085904515e8c4f383c9`；NoneBot wheel SHA256 `6d8f119845167ac7a02b0768dbceb3f30ef4eb11c786d80e6c6545c9ddf6658f`，均在 `.runtime/adapter-artifacts/`。

SDK未真实连接QQ；Platform分页拉取、群页空档案及原位四态由TS399负责，本卡不冒称页面联合或正式发布通过。失败的群资料刷新需后续真实群消息在节流期后再尝试，不另加后台扫描。已见基础目录不授予正文观察、归档、Memory或发送权限。

后续定向对齐：legacy `observe_only` 的 list 无名单语义，normalize不生成groups、effective不启用其中群；只保whitelist/blacklist显式名单，private分支不变。新增该负例并将原观察调用方的显式观察夹具改为blacklist中明确列出的群，保原业务断言；只复测受影响政策与该调用方，7 passed/0 skipped（`.runtime/ts400-policy-followup.xml`），未重跑其余已过节点。Ruff/format及三份policy同字节通过；上述ZIP/wheel hash仍对应61e首轮，本次小修未重建发行包，交协调者按新提交构建。
