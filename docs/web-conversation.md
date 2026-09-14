# Core 网页会话快照与独立发送端

本功能是 TS-071 生活块之后的独立接线，依赖正式 `web-conversation/v1` 1.0.0。
manifest（UTF-8 LF）固定 SHA256：
`e493a1b5d0f4cec8d55995553faf84042f4c33a59365d15423e57f4dc70a6c09`。
Core启动时校验manifest、依赖与全部文件hash，加载同根正式包；不使用候选或产品内复制schema。
部署必须同时携带text-dialogue、profile-memory、source-sync与web-conversation正式包。

## 路由和配置

- `POST /internal/v1/conversation/web-snapshot` 仅接受已配置的 platform 服务bearer。
- body严格使用发布snapshot_request：query、deadline_at、actor_id、conversation_id、before_turn_sequence、limit。
- 网页Cookie/CSRF由Platform处理，不能替代Core内部服务认证。请求不得自报person_id或scope。
- 当前viewer actor origin必须核验为web/self_private及精确conversation/actor；source_input凭证不能冒充actor origin。

Core运行配置示意（示例版本/域名/环境变量名需由部署者实际提供）：

```json
{
  "services": {
    "platform_sender": {
      "url": "https://platform.example.invalid",
      "token_env": "CORE_TO_PLATFORM_WEB_SENDER_TOKEN",
      "ca_file": "C:/deployment/ca.pem"
    }
  }
}
```

该token是Core作为companion调用Platform的专用服务凭据，与Platform调用Core的bearer、网页Cookie分别管理。
Platform需登记companion服务的dialogue.send权限，以及actor route：caller=companion、receiver=platform、purpose=dialogue。
web namespace只走此发送端，缺配置则dependency_unavailable；绝不退到NoneBot。
非web沿原Sender保持QQ/TG等现有路径。发送仍复用已发布conversation/send_request和send_receipt。
Core在请求发出前持久保存attempted_at/request；只有合法sent回执才显示正文。Platform持久存储是其产品责任。

## 当前查看者与来源边界

请求开始和投影前都读取当前origin及Memory身份映射；比较当前account/channel/actor/person/scope/binding。
还验证该conversation确由当前账号/person的持久收集记录拥有。错误用户、角色、渠道或绑定/范围改变拒绝。
SQL先按精确actor/person/audience/conversation/channel/author过滤，再有界加载记录。
第二次授权后没有异步切换；构造/校验输出后再次检查deadline与当前origin有效期。

历史不复用旧入站origin作为查看授权；旧origin自然过期或无关Memory全局scope_version变化不等于来源撤回。
来源展示检查Core当前physical/admission及revision，编辑旧版/撤回清旧输入正文；
显式permission_revoked/classification_changed保存局部display_invalidated，清对应输入和回复正文。
本地短期历史、续段和方案依赖递归检查可证明来源，最多64节点，失败时派生正文不可展示。
仅取消回复不撤销仍合法的用户输入，也不能把partial轮已sent段改成未送达。

明确限制：Core尚无“当前查看者展示权限下的远端Memory单项素材版本”检查端口。
尚未传入Core的远端画像/证据撤销不能由这里证明；不根据全域Memory版本猜测已撤回范围。
此限制已报协调。该接口是历史投影，不作为模型召回授权/现实日记素材证明。

## 有界完整投影

collectors≤32、每组messages≤256；active≤64；history≤50并按turn_sequence降序，before严格排除游标及以上。
active独立于history分页，二者互斥；只在确有更旧终态时返回下一游标。分页不是全历史事件订阅。
完整响应≤1,048,576 UTF-8字节；任意字段、数组或整体预算不符都budget_exceeded，绝不静默截断完整组。
首版只投影text；媒体附件标unavailable、parts=[]，不泄露媒体定位符。
不返回origin、来源证明、模型路由回执、person_id或内部凭据。

reply.state保留sent/failed/unknown/pending/sending事实；content_state独立。
只有sent且有确认回执并且来源仍可展示时available/text非空；其他状态正文null。
已经失效的sent仍然sent，只将content_state置unavailable；partial同理。

只有state=cancelled且attempted_at/request/receipt全部为空的未尝试草稿可以不投影。
若出现cancelled但带发送尝试/回执的矛盾状态，返回dependency_unavailable，不吞掉事实或猜映射。
实际发送中取消、晚到sent回执和partial保留都已由Core产品测试覆盖。

## 验证与交付状态

- 定向：`python -m pytest tests/test_web_snapshot.py -q`。
- 调度相关：`python -m pytest tests/test_web_snapshot.py tests/test_core.py tests/test_edges.py -q`。
- 静态：沿AGENTS的ruff format/check与compileall命令。
- 共享合同/发送路径修改后跑完整套件；TLS及固定Memory archive联合须显式设置各自环境变量。

本批使用合成账号、临时数据库、ASGI或MockTransport；真实Sender适配器及正式schema实际参与测试。
Platform真实产品的持久sender与网页消费尚待对固定Core/Platform提交联合验证；未宣称网页端到端或真实模型通过。
SQLite仍是v3，无新迁移、无跨产品数据库访问、无生产/真实设备操作。
