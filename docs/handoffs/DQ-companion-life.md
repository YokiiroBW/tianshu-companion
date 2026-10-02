# DQ04/06/07/08/12、Q10 Companion本地交接

日期：2026-10-03。状态：本地实现及隔离验证完成，尚未合入产品main、推送或部署。
工作分支：codex/quality-life-20261003。
基线：25778436a5e36895c60f0d7beb35fa5ba060fc30。
同分支父级DQ02适配器先行提交：7374424a5f0c5eb718318f52c8ef5fa6f615f895，本批不修改其路径。
本交接随实现提交；精确实现SHA可用`git log -1 --format=%H -- docs/handoffs/DQ-companion-life.md`取得。

## 实现与复用

- DQ04：BotBindings单一reconcile_actor恢复已持久绑定，RoleRuntime停用/启用同步，完全相同revision重放安装当前可用状态；停用后重启再启用无需重新修改QQ绑定。
- DQ06：Core恢复及RoleRuntime启用自动登记角色生活。启用与dialogue能力独立，零对话、零QQ、网页关闭仍有中文基础作息。已有世界/日程/房间/手动保持保留；停用暂停，恢复对齐现在，不回演缺口。
- DQ07：life_daily拥有日计划、阶段细化与持久任务；每天AI根据实际人格正文生成适合角色的活动名/内容，时间约束不变。计划失败不阻塞阶段；Gateway成功内容进入既有life_events/life_known/素材/日记链。每阶段去重，晚回包按计划/阶段/人格/内容版本/角色epoch/attempt拒绝。跨日完成前日计划，未发生阶段skipped，不制造停机期间经历。
- DQ08：life_influences复用已受理来源/角色/受众约束，Gateway提取开放主题、建议和约束为角色自己的活动意图；明确具体主题用例为天体物理纪录片。来源撤回取消旧影响及在途计划/阶段回包，历史经历不改写。模型上下文有界，不复制私聊原文至计划、公开经历或日记，不声称真实用户行为。
- DQ08读取：today/timeline沿既有LifeRead、life_access和独立读凭据，formal life-read/v1出入站校验。只读读取持久状态，不tick、不生成、不做DDL；时间线是已有position/known_id索引seek分页，角色获知日与发生日分别保留。
- DQ12：RoleRuntime经Personas公开窄接口读取role/profile版本，不调用其私有读方法；Core仅消费不透明人格标记与pin/verify。人格规则仍由Personas拥有。
- Q10：Life.work只tick一次，日记准备不再逐角色重复全量tick。5/25角色SQL增长定向断言保持线性，生成查询LIMIT 1且使用due/历史派生索引。

生命周期与时钟仍归既有Life，新增能力按职责拆入life_daily、life_influences；未另建调度器或记忆账本。现役生活worker由旧30秒调整为2秒，生成每pass最多一次意图提取、一个日计划、一个当前阶段与一个日记，复用Core模型并发槽，失败deadline退避。

## 一次部署配置及权限

life-read/v1固定manifest LF SHA256：7be7507d58f897a739b269de3c096ba948c92b25266888a91fa50130d342c551。协调者拥有正式合同；产品不复制schema。

新增管理角色的生活读归属可一次配置，示例全部是环境变量名，不含真实凭据：

```json
{
  "callers": {
    "platform": {"token_env": "PLATFORM_ROLE_TOKEN", "issuer": "platform"},
    "platform_life": {"token_env": "PLATFORM_LIFE_READ_TOKEN"}
  },
  "life_readers": {
    "platform_life": {
      "reader_id": "reader:platform-life", "actor_ids": [], "runtime_roles": true
    }
  }
}
```

固定reader_id由部署给既有独立read服务选定，HTTP不能自报。runtime_roles默认false；true才枚举持久Platform RoleRuntime角色，并在首次启用/启动接线时给缺失life_access安装该reader。已有life_access不覆盖，包括显式readers=[]撤回，重启、角色重放与读取均不恢复撤回。静态角色仍需actor_ids和剧情grant，管理/读token各自独立；动态+静态候选最多64，超预算明确拒绝。停用角色可以读取已有授权历史和暂停状态。

自主生活未显式设置life_writing时默认允许生成；显式false继续禁止全部生活AI。优先复用现役角色管理所选模型与DefaultModelSelector发布租约，caps=[]角色同样可用；不新增模型配置。请求workload沿用companion.text，scope是actor独占虚构life/person:life摘要，audience=self_private。每次持久request_attempt递增，selector.turn_id与Gateway.X-Tianshu-Turn-ID完全一致；重试不复用旧attempt授权。调用前65秒租约预算、回包及所有后置await授权后再次检查未到期，Gateway仍核验实际版本receipt。

未部署模型选择服务时，自主生活可使用life_config_version或既有config_version默认绑定；没有Gateway/模型则基础作息照常且明确unavailable。原日记/长篇仍要求显式life_writing=true与独立life_config_version，不因自主生活默认开启而自动启用长篇或发布日记。

失败/unavailable可至多三次已知结果退避；取消、重启、timeout未知结果为interrupted，不自动重新收费。实际可达POST `/internal/v1/life-generation/retry`只接受Platform管理服务：actor_id/plan_id/phase_id/expected_version，null phase重试计划，非空只限当前阶段，CAS用today.plan.version。回执queued/unavailable仅表示受理；Platform页面随后经独立读服务刷新。过期409、角色/计划不存在404、不可重试400、读凭据403。formal retry_request/retry_response已校验。

## 实际验证

隔离Python3.12.14 venv使用项目requirements-dev.txt及requirements-nonebot.txt既有锁定依赖；为已有AstrBot适配器import补aiohttp3.14.0及其依赖（本机现役同版本），仅ignored .venv，无产品依赖/锁文件改动。TIANSHU_CONTRACTS指本轮协调合同，TIANSHU_TLS_PYTHON指已安装cryptography解释器；ignored workspace-context仅指本轮coordination。所有DB、文本、角色、模型响应、证书和token均是临时合成夹具。

- 新生活定向：26 passed（0.99秒）。覆盖零聊天基础/AI生活、人格A/B实际传入及活动投影一致、开放主题/撤回/隔离、日计划与阶段独立失败、timeout显式retry、same-persona停启旧回包、来源在日计划await撤回、影响后置授权await租约到期、真实Gateway类native header/receipt与exact grant身份、RoleRuntime HTTP新建→启用→动态actors/today、管理retry CAS与只读凭据拒绝、跨日/停机/幂等/手动保持、只读事实字节不变、深页索引、缺派生索引前备份、Q10线性查询。
- 首次完整组件：701 passed、14 failed、25 skipped、101 subtests passed、2个既有websockets弃用警告，121.20秒。没有把失败/skip说成全绿。
- 归属/环境补测：tests/test_boundaries.py、test_life_read.py、test_daily_life.py、test_life_read_https.py、test_persona_chain.py、test_profile_context.py、test_source_https.py、test_astrbot_connector.py：138 passed、1个下述已证实基线失败、13 subtests passed，23.48秒。修复本次2个兼容问题：Personas标记归属与旧reader解析shape；补齐context/aiohttp使原环境失败得到实际执行。
- 2秒worker及最终生命周期定向：health、observability、bootstrap、daily_life共104 passed，4.30秒。依协调者指令复用其余已通过701项结果，没有再重复完整套件。
- ruff check全部src/integrations/tests/scripts通过；owned 16个Python文件format check通过；compileall全部通过；git diff --check通过。全仓format check仍有6个基线未格式化文件：两adapter runtime、test_adapter_astrbot_host/test_adapter_rpc/test_optional_persona/test_role_runtime，非本批所属，保留并明确记录。
- 首完整25 skip中的observability.RecordTests十项因ignored context缺失而找不到diagnostics合同，补context后已在104项定向中全部通过；剩余15项为Memory/profile/relationship未指定固定peer、Platform joint未指定peer、AstrBot真实host SDK未安装。另以仅收集不执行的skip归属核对15项，未以读取其他产品可变工作目录绕过既有联合门槛。
- Platform写者实际四服务新建角色RoleJoint：1 passed，12.77秒。浏览器选择模型B、enabled=true/caps=[]、零对话→独立platform_life动态actors/today→实际Selector/Gateway/合成模型计划失败→管理页retry→独立授权读取→模型日计划与阶段体验落库及页面展示→停用paused。没有seed actor/grant/tick；桌面/手机截图由其交接保存。这是隔离HTTPS联合，不是真实供应商模型或生产L0。

四个剩余失败均在只读git archive固定基线2577843中用相同测试环境精确复现；不是本批改动造成，也未为过旧断言修改产品：

1. persona_authoring::test_core_without_personas_keeps_legacy_model_request，旧startswith(Role A)断言与已有身份投影前缀冲突。
2. persona_authoring::test_core_model_request_uses_all_four_fields_from_each_pinned_revision，同样是旧人格前缀断言。
3. web_snapshot::WebSnapshotTests::test_restart_preserves_web_delivery_and_current_viewer，WebHarness.new_core重启没有重新提供web绑定，forbidden。
4. source_https::HttpsSourceTests::test_core_https_first_identity_fanout_facts_and_background_check，旧配置夹具缺现役QQ管理员reader，启动明确拒绝。

前三项基线定向3 failed/0 passed（0.66秒），第四项基线1 failed（2.88秒），错误一致。补环境后的事实是原14失败有10项已经通过、4项保留已证实基线失败，原25skip中10项实际补执行通过；按节点归并为721项有通过证据、4项基线失败、15项未执行。相关分批结果不冒称为最终代码重跑全量全绿。

## 限制及下一步

未读写真实数据库/聊天/凭据，未发QQ、调用真实模型、访问NAS、生产迁移、推送或部署。AI内容的语义质量不能由字符串/schema测试保证。停机缺口不补造，unknown须正常管理页重试；read不会救活时钟。实际Platform/Gateway/Memory/Companion四服务HTTPS隔离联合证据及浏览器页面由Platform交接引用，不能称生产L0。

协调者按产品边界串行集成并处理上述既有测试夹具；部署时挂载正式life-read/v1，登记独立生活read映射与现役角色模型选择服务，再验真实环境。参见docs/life.md、docs/life-read.md及根quality-life开发队列。
