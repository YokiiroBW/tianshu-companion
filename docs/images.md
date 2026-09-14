# Core 图像与衣橱内部端口（TS-072）

`core.images` 是可信同产品 Python 端口；调用宿主须先鉴权并校验 actor 管理权限。
没有新的网络路由，没有修改 web-conversation 或 text-dialogue。普通聊天不调用 ComfyUI。
所有生成物标识 fictional，表示虚构角色渲染，不是现实生活照片或当前穿搭自动更新。

## 实际工作流审阅（2026-09-14，只读）

实际目录为 `C:/YOKI/ComfyUI_Anime/ComfyUI-good-anima-test/user/default/workflows`，
不是最外层 `ComfyUI_Anime/user/default/workflows`。

- `GoodAnima_AdultNSFW_WebUI_Workflow.json`，SHA256 `c2447a4adfc8494118cc0b2eab7ac5228aad1345b27263f105eb3d526b8c7101`。
- `GoodAnima_NaturalLanguage_ArtistMixer_WebUI_Workflow.json`，SHA256 `8933d229187a37f01ef3934141dcfcc4ef328480f6c903642f8f94090a74225a`。

两份都是 UI 节点/连线/控件格式，**不作为 API 图直接提交，也未进行猜测转换**。
节点 3/4 是 `LoraLoaderModelOnly`，模型为 anima-highres-aesthetic-boost 与
anima-base-1-masterpiece-v51；没有观察到可确认为独立角色 LoRA 的节点。
因此尚不能宣称找到了用户指定的完整固定角色配置。没有读取权重内容、修改原文件、下载模型或运行 GPU。

普通版 6 为 CLIPTextEncode，7 为负提示；8 为 EmptyLatentImage，9 为 FLS_SamplerV4，
12 为 SaveImage。ArtistMixer 版 6 为 AnimaArtistPack，13/14 为画师选项与 CrossAttn。
本地 `custom_nodes/Anima-Artist-Mixer/anima_mixer/nodes_core.py` 确认 6 的
`artist_chain` 为固定画师输入，`base_prompt` 为可选主提示；映射只允许后者。
本地 `custom_nodes/ComfyUI-BSS_FLSampler/nodes/node_fls.py` 确认 `seed` 与 `steps`。
两份图中的加载器、LoRA、画师链、选项、CrossAttn、采样其他参数都须保持不变。
参考图映射仅支持经审查图中确实存在的 `LoadImage.image`；这两份图没有该节点，
因此不能凭空为它们添加 reference 能力。

下一步需要用户确认所用版本并提供/导出 API-format 图，再审查节点输入及模型/角色绑定。
这里的测试图是独立的合成夹具，不能当作真实 Anima 接入通过。

## 配置和调用

默认 images 未配置。可信宿主可向 Core 的 `image_options` 传入 `ComfyUI`、`Workflow` 和
专用 staging 目录；此种嵌入方式由宿主关闭 transport。应用配置支持显式 `images` 对象：
`base_url`、可选 `token_env`、`api_graph`、`bindings`、`outputs`、`staging`。
启用配置会让后台处理可信宿主已创建的任务，因此真实环境启用/请求须另获实际生成授权。
本任务没有写入任何真实连接配置。

固定 origin 禁止 URL 凭据/路径/查询、跟随重定向与环境代理；非回环必须 HTTPS。
认证只从 token_env 引用读取，不落库；数据库只保存 origin/认证引用的哈希。
API 图必须经过管理员审阅，图内节点仍可执行任意已安装扩展能力，结构校验不是沙箱。
不能把用户上传的 workflow 或 bindings 原样作为配置。Core 校验图格式、链接、实际节点类型、
字面量类型、映射白名单及数值范围；完整节点语义和模型存在性仍由真实 ComfyUI 验证。

`Workflow(api_graph, bindings, outputs)` 中每项映射包含 node/class_type/input；
seed/width/height/steps 另要求 min/max，且实际模板值也须在范围内。
positive 支持 CLIPTextEncode.text 或 AnimaArtistPack.base_prompt，追加生活提示并保留原固定前缀；
negative 只支持 CLIPTextEncode.text，reference 只支持 LoadImage.image；
seed/steps 支持 KSampler 或 FLS_SamplerV4，尺寸仅 EmptyLatentImage。
未绑定节点及字段原样深复制。没有 NAI/GPT Image 适配。

衣橱条目 `put_outfit(id, description=..., prompt=..., reference=..., activities=..., expected=...)`
含版本、显式提示、可选预先部署的参考文件名、适用活动描述；更新要求旧版本。
reference 不是 URL，也不自动上传素材。activities 是说明性元数据，不自动换装。
`select_outfit(actor, outfit, expected=actor_version)` 修改同一 life actor 的 outfit_ref。
`request(request_id, actor_id, parameters=...)` 从当前 life 读取完整 actor/room/world 和衣橱版本，
冻结完整图、工作流哈希及 prompt_id。用户参数只允许已绑定的 seed/尺寸/steps/negative。
新生活状态只影响新请求；同 request_id 和参数重试返回旧任务，参数冲突拒绝。
完成图片不会覆盖 actor 穿搭。未绑定的衣橱/参考或未知参数明确拒绝。

## 状态、恢复和取消

状态 queued/running/completed/failed/unknown/cancelled。
本地 queued 尚未提交时 cancelled 即确定取消；提交前把 submitted 与随机 prompt_id 持久保存。
本机 `server.py:915–968` 的 POST /prompt 支持传入 prompt_id，但没有幂等去重保证，
所以 **提交意图一旦落库就绝不自动重复 POST**，包括进程在发包前崩溃的保守未知状态。
同一请求重试不会换 prompt_id。HTTP 400 明确拒绝为 failed；超时、离线、响应丢失或异常为 unknown。
重启已提交 queued/running 转 unknown；随后仅 GET /history/{已保存prompt_id} 与 GET /queue 核对。
历史无记录不证明从未提交，不能据此重发。服务更换时不向新 origin 查询旧任务。
历史被清除的未知任务可能长期保留，须运维审查，不能直接用新请求 ID 当自动重试。

取消已提交任务保留 cancel_requested；只对观察到仍在队列的本任务 POST /queue delete。
再读队列/历史确认消失才标 cancelled；若已开始执行则保持运行直至成功/失败。
没有调用全局 /interrupt。虽然本地版本新增按 prompt_id 请求中断支持，本适配器首版保守不使用，
更不声称强制停止 GPU。最终 completed 可以与 cancel_requested 同时存在，UI 必须如实展示。

应用每两秒独立工作 pass，单并发，每 pass 最多一个任务，单 pass 30 秒总预算；
每 HTTP 10 秒，JSON 2 MB，默认待办最多 16，文件最多 4 个、每个 8 MB、暂存总量 128 MB。
每个任务轮流轮询。预算以真实 GPU 模板的管理员审查补充：服务端图可执行更重节点，
不能把 HTTP 字节预算称作 GPU 耗时硬限制。图片错误与离线不会阻塞聊天调度或模型槽。

## 暂存与迁移

仅从本任务 history 中显式 outputs 白名单内 SaveImage 的 images 下载；只允许 output 类型。
拒绝目录穿越、绝对路径、Windows 分隔符与盘符。返回 filename 只作为受约束 /view 查询参数，
从不当作本地文件名。按本机 folder_paths.annotated_filepath 的大小写敏感 endswith 语义，
拒绝末尾 `[input]`、`[temp]`、`[output]`，无论前面是否有空格；不剥后缀再读其他文件。
`/view` 仅发送 filename/subfolder/type 三字段，history 扩展键（包括 preview/channel）不转发。
Core 专用目录内随机文件名排他创建，失败批次清理自身新文件，不覆盖现有文件。
PNG 首版校验签名/IHDR、尺寸上限、完整 chunk/CRC/IEND；不解码图像语义，也不支持 JPEG/WebP 输出。
保存哈希、字节数、MIME、尺寸、staging_name、archived=false。未提供跨产品下载 URL。
进程崩溃可能留下未登记暂存文件；其大小仍计入预算，停机后由管理员核对清理，不自动删除未知文件。
目录须由 Core 独占并限制本机写权限，不能指向外部共享可变目录。
AssetLibrary 当前只有读取服务，**没有归档成功声明**。

SQLite user_version=4 新增 image_outfits/image_jobs；v3 升级前 SQLite backup 至
`<database>.pre-images-v4-<random>.bak`，在同一事务建表/索引并提升版本。
v1/v2 仍保留既有备份前缀。失败回滚并释放 owner；source_head/原聊天事实不改。
恢复必须先停进程，隔离当前 DB/WAL/SHM，再恢复完整备份，不运行中覆盖数据库。

## 最小跨产品候选（未发布、未实现）

- Platform→Core：认证 actor 管理者后提交 request_id/actor_id/expected生活与衣橱版本/允许参数，返回任务 ID；衣橱管理写入应有 expected_version。
- Core→Platform：任务状态、fictional=true、捕获时间及 actor/room/world/outfit 版本、取消意图、产物受控引用。前端标记“生成时状态”，不能展示成实时照片。
- Core→AssetLibrary：未来独立幂等写入/提交暂存产物协议，携带 hash/type/size/来源快照；只有接收方确认后才能记录 archive receipt。
- 渠道图片下行：需独立能力/产物授权与真实 sender 联验；现有 text send 继续只收文本，不添加假 image 字段。

这些只在任务 docs 提议，不扩根 contracts，需双方 schema/样例/验收后冻结。
