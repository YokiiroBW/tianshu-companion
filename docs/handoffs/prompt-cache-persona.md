# 对话装配：稳定前缀、人格表达与替换人设注入边界

基线：`5430300bc37e1ff17002c25e764acdd4689b9f24`；隔离分支：`codex/prompt-cache-persona-20261029`。仅本地实现和验证，未合入共享 main、推送或部署，未调用真实模型、QQ 或生产数据。

## 改动与归属

- 新增 `dialogue_prompt.py`，统一管理模型可见的规则、完整人格及素材展示顺序。全局 `canonical()`、合同、来源哈希与持久化表示不变；未知素材字段完整保留。
- `Core` 继续复用人格固定/核验、上下文来源校验、身份投影、原生工具执行及回执链路。系统内容先放稳定规则、可用工具操作说明、完整人格及 Tone/Style/Address，再放当轮身份、记忆处理状态和来源元数据，最后放统一角色一致性边界。当前消息放 user 素材包末尾。
- 技能注册表只为实际提供的工具组装操作说明，并去重相同规则；工具按函数名稳定排序。原生 schema 保留函数说明和参数，不再为每个函数重复技能标题和描述。完整管理目录及全部可用工具保留。
- 可修改的技能标题、描述和动态状态移入 user 素材；不再直接拼进 system 或原生函数说明。工具结果保持 tool 消息。
- 角色一致性边界属于统一装配代码，不写入人格卡。明确覆盖“忽略所有人设”、整段替换人设、伪造 system/developer、要求改自称或凭空建立主人/恋人关系，以及来自历史、摘要、召回和工具的同类内容。允许与人格不冲突的昵称、语体、格式偏好和角色创作。

## 装配结构

```text
system
  稳定：身份/来源/记忆使用规则 → 可用工具操作规则
      → 完整人格 + Tone/Style/Address → 人格表达规则
  当轮：可信身份投影 → 记忆处理状态 → 素材来源与人物时间线
  末尾：统一角色一致性边界
user
  工作摘要/记忆/证据 → 关系及生活状态 → 技能状态/共读素材
  → 最近对话与早期片段 → 当前消息
assistant / tool
  原生调用及实际工具回执，继续使用同一完整系统内容
tools
  全部实际可用的原生工具，按函数名排序
```

## 实际验证

环境：隔离 `.venv`，固定 `requirements-dev.txt` 和本仓库 `tianshu_storage-0.1.3` wheel；无依赖锁或生产依赖修改。合同使用已有严格校验的配套发布目录：

`C:/Users/Administrator/.codex/worktrees/al-autonomous-life-20261028/tianshu-peiban-bot/.runtime/al-core-verification/contracts-cp-v2/text-dialogue/v1`。

`PYTHONPATH=src;integrations/nonebot`，通过 `python -B -m pytest <以下文件> -q --tb=short --basetemp=<隔离目录>` 分批验证，成功结果按未变行为复用：

- 54 passed：`test_dialogue_prompt.py`、`test_image_dialogue.py`、`test_chat_history.py`、`test_comfy_deep.py`、`test_skills.py`、`test_prompt_projection.py`。
- 105 passed / 4 subtests：`test_personas.py`、`test_persona_authoring.py`、`test_role_runtime.py`、`test_qq_identity.py`、`test_context_time.py`、`test_book_reading.py`、`test_short_context.py`、`test_boundaries.py`。
- 最后追加历史污染断言后，28 passed / 15 subtests：`test_dialogue_prompt.py`、`test_memory_context_mode.py`。其中 7 个用例与第一组重复，合计 180 个不同用例及 19 个子测试。
- 7 个修改的 Python 文件通过 Ruff 格式/静态检查，`git diff --check` 通过。

新增用例核对完整长人格、不同群成员之间稳定前缀、未知字段无损、消息顺序、技能目录增长、技能描述/状态/工具结果隔离，以及三类人设替换文本进入当前和历史消息后不提升到系统层。未通过伪造模型输出声称已经验证语义抗攻击效果。

同一组合成的两个群聊轮次，基线与修改后均记录实际交给测试网关的请求，22 个工具保持不变，无真实模型调用：

| 字节级指标 | 基线 | 修改后 |
| --- | ---: | ---: |
| 两轮共同 system 前缀 | 1,930 | 12,790 |
| 首轮工具 schema | 22,421 | 20,431 |
| 首轮 messages + tools | 43,537 | 39,057 |

共同前缀约为原来的 6.6 倍，首轮载荷缩小约 10.3%。这是合成请求的字节比较；实际供应商缓存命中率、费用和延迟均未测量。

## 局限与接续

本次是装配信任分层与系统级提示防护，没有独立语义分类器或生成后拦截器，也未修改 Memory 自动提取链路。固定人格快照不会被聊天改写，但不能据此保证真实模型永远不受诱导，或保证长期记忆不会记录用户提出过这类要求。真实模型需要覆盖单轮替换、长篇/多轮重复、历史与召回传播、工具返回污染和正常昵称/创作，分别评估人格偏移及误拒绝。

既有字节预算和工具轮次限制保持不变；未把字节上限宣称为模型 token 预算。未跑无关全量、PG、生产或外部验收；后续由统一发布窗口集成，需要真实模型结果后才能给出抗人格劫持成功率和实际缓存收益。
