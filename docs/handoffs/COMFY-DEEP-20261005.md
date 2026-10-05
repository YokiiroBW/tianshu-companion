# Companion ComfyUI deep integration — local handoff

Date: 2026-10-05. Branch: `codex/comfy-deep-20261005`. Base: `91d82ddad9bb1f2eb140d0dc6615ee84dd6b4eb9` from the deployed Companion release checkout. Final local commit is the commit containing this handoff; use `git log -1`. Integration, build and deployment remain with the coordinator.

## Delivered behavior

- Reused `Images`, `image_jobs`, existing metadata, references, original staging, album and delivery. Separated immutable Comfy workflow rendering, validated UI conversion, semantic compiler/catalogue and provider transport. Only ComfyUI is implemented. The provider result supports an opaque server handle or immediate completion; synchronous and asynchronous synthetic providers use the same original ledger.
- Added Platform-authenticated image backend read/manage/compile endpoints through the existing dispatch. Global connection CAS and per-actor workflow CAS remain separate. Credentials resolve through the existing directory; projections contain no token. Existing `standard_sd` configuration and old job fingerprints remain compatible.
- Real ComfyUI userdata discovery excludes explicit backups. Workflow inspection maps the actual composer/sampler connections, exposes typed candidate bindings, and freezes the selected API graph for each actor. Model-assisted analysis can select a real catalogue entry, inspect only that graph and suggest exact existing candidates. Both analysis model calls share one 120-second deadline.
- UI-format conversion uses verified node serializers and live `object_info`. ResolutionMaster reads its saved named properties and checks consistency; it does not zip its reordered widgets. Unknown list layouts, bypass/subgraphs, missing nodes/links or bindings outside the supported typed fields are explicitly unsupported. Known Anima selectors/composer, Simple String/AstrBot Router, sampler and standard loader/encoder/decoder/save nodes are supported. This is not a universal custom-node converter.
- Structured image intent becomes readable prompts instead of canonical JSON. Fixed graph identity, quality, artist, LoRA and topology are retained. AstrBot Router receives its documented envelope at the upstream Simple String input. Missing clothing retains the original envelope's clothing; a plain template is retained as context when no clothing override exists. No wardrobe item is required for ordinary photography intent.
- Deep-configured roles translate dynamic scene/outfit intent automatically unless `assist_model:false` is explicit. Stable identity is excluded from model translation. Job preparation persists the compiled values, graph and model receipt once. An interrupted preparation reports unknown without another model charge. Native tools reuse their held model slot; synchronous preparation markers avoid a model/preparation lock inversion.
- Width/height use live node limits plus 64–4096 product bounds, steps 1–150 and 4,194,304 maximum pixels. Camera text uses horizontal/vertical/square canvas. Preview returns the exact compiled graph, changed inputs, preserved nodes, dimensions, hash and model receipt.
- Proactive expression can decide on a structured photo intent. Its original sourced candidate waits for a stable Images job, then attaches the real original to its original delivery queue. Sources and subscription are rechecked before submission and dispatch. Failure cancels the candidate without a success claim. Restart resumes the same job and compiled values. Completion does not recursively create a photo or duplicate the same subscription's motive. No fake conversation turn is created.
- Provider handles persist before artifact download; completion clears the current failure field. Durable cancellation survives that intermediate persistence. A disabled deep connection stays disabled after restart even when older standard configuration exists.

## Published contracts

The coordinator's root contract commit is `725113bcc5676972226d0c73ae2543cdc8495085`. Candidate schema files were delivered for review and are omitted from this product commit after publication.

- `image-backend/v1`: `62e71dd2f42fb5b1376c439c362a555619da41cb7f7ae26d07acc1182b80ffbd`
- `life-runtime/v2`: `85aff438f91cb96876e259204b5a198e2fedaead56db64a2265aa520a59ea7e3`
- `bot-delivery/v2`: `edec27d83b8b9427656096d45818d9db3665d8d7bd82cc796805e21ba35b757b`
- `knowledge-content/v1`: `75d210454102af5af505ec1f72d69cc7dfd344550f7cf90e70125c082fdf4cd7`

The API implementation details are in `docs/comfy-deep-api-draft.md`. The consumer must use the new published closure; unchanged Memory/Gateway/NoneBot use the coordinator's old closure snapshot, as coordinated separately.

## Verification actually performed

Python: existing `worktrees/quality-life-20261003/companion/.venv/Scripts/python.exe`. `PYTHONPATH=src;integrations/nonebot`. Contract root: `worktrees/quality-life-20261003/coordination/contracts/text-dialogue/v1`.

- `pytest tests/test_images.py tests/test_runtime_v2.py tests/test_proactive.py tests/test_life.py tests/test_delivery_v2.py tests/test_bootstrap.py tests/test_boundaries.py tests/test_comfy_deep.py -q`: **138 passed, 21.17s** before the final local recovery fixes.
- After durable provider-handle/cancellation, disabled restore, single-slot concurrent preparation and completion failure clearing fixes: `pytest tests/test_images.py tests/test_comfy_deep.py -q`: **51 passed, 17.83s**. Deep tests include 16 synthetic cases covering discovery, conversion, preserved identity/style, CAS, dry-run/no POST, pixels, model selection/adaptation, native tool translation with one model slot, duplicate preparation, no outfit, provider handles/restart and proactive success/restart/failure/revocation/recursion prevention.
- After the final canvas wording change, its catalogue/compile test: **1 passed, 0.24s**. Ruff on every changed Python module/script/test and `git diff --check`: passed.
- Earlier collection failed when NoneBot's integration directory was missing from PYTHONPATH; the corrected command above passed. An async-provider test initially expected the business job id instead of the internal submission UUID; that assertion was corrected. An intermediate handle-persistence change exposed cancellation being overwritten; durable cancellation merge fixed it and the 51-case rerun passed. No remaining test failure.
- Real read-only GET `/system_stats`, `/object_info` and `/userdata` on local port **8188**; actual inspected and compiled workflows: `角色/澄汐/澄汐-分类测试版.json`, `角色/澄汐/澄汐_网页编辑版.json`, `小说插图/小说插图_ANIMA_yoki画风_质量版.json`, all ready. Actual named ResolutionMaster and AstrBot Router sources were read locally. Private graph/prompt evidence stays in ignored runtime files, not the commit.
- `scripts/comfy_acceptance.py` default read-only mode ran successfully, including reuse of the same output directory: ready at 1024×1536. It uses real Core/Catalog/LifeRuntime/Images/Comfy adapter with an isolated Store and disconnected production clients. Model translation was explicitly not called; the intent is English.
- The coordinator explicitly ran **one** real GPU job with `--generate`. It completed through this same pipeline: prompt `7277d72e-f0d2-42c1-8efd-18aafa9c35b6`, PNG **1024×1536**, **2,023,498 bytes**, SHA-256 `c8e801c61406ac6b7430a80a9b23e03e6b290d34507c4a12373d75b3d408f4af`. A transient ReadTimeout entered unknown, then the original prompt reconciled to completed without another submission. The coordinator visually inspected the white/blue character and morning ocean scene. Evidence is in root `.runtime/comfy-deep-20261005/real-generation/acceptance.json` and originals; it is not committed.

## Acceptance command and remaining limits

Run from this Companion checkout:

```powershell
$env:PYTHONPATH='src;integrations/nonebot'
& 'C:/YOKI/Codex/tianshu-peiban-bot/worktrees/quality-life-20261003/companion/.venv/Scripts/python.exe' -X utf8 scripts/comfy_acceptance.py --contracts C:/YOKI/Codex/tianshu-peiban-bot/worktrees/quality-life-20261003/coordination/contracts/text-dialogue/v1 --output C:/YOKI/Codex/tianshu-peiban-bot/.runtime/comfy-deep-20261005/real-generation
```

Omit `--generate` for read-only compilation. The coordinator already generated the stable job in that output directory. If explicitly generating/reconciling, reuse exactly the same output and inputs; the script reuses the existing admission and never automatically resubmits unknown work. It writes job/prompt/state/submitted/failure and artifact receipt after each poll; timeout/unknown/failure exits 2. No QQ delivery is enabled.

The actual image did not fully follow the requested full-body composition; it was closer to an upper-body portrait. Inspection found `full body` in the submitted dynamic prompt, no old upper-body/close-up/bust/waist-up positive conflict, and `cropped` only in the negative prompt. The final compiler now uses `vertical canvas` instead of the potentially ambiguous portrait orientation term. No additional GPU image was generated for this wording change; generation quality is not guaranteed by schema/compilation.

Real Gateway/model assistance and actual QQ delivery were not exercised by this worker. Assistance used explicitly synthetic receipts in tests; a real model check is scheduled by the coordinator through the legitimate Platform caller after role configuration. No NAS write, deployment, production migration, paid model, remote push or merge was performed by this worker. No NAI/online provider implementation is included.
