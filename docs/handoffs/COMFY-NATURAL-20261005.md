# Ordinary dialogue image requests

Base: `41393d9f559e6ca98a082bc9aa39b22fc4bb1bae`. Product fix: `f1936defebc765e366f3b1469734149ff4ce6bfc`. The later commit containing this handoff adds only the isolated acceptance script and its test; it does not change the product image used for the build.

The real incident reached Gateway successfully but selected no tool. The model extended an unconfirmed relationship expression boundary into a ban on ordinary sleepwear pictures and confused absent wardrobe history with absent image capability. No image job was created. The pinned persona had no clothing or photography ban.

Companion now projects actor-scoped configuration and current visible outfit into ordinary native dialogue. Configuration is explicitly separate from the last network observation. The image tool exposes dynamic intent and optional dimensions; Companion supplies stable job identity, version and trusted conversation scope. Missing wardrobe records allow clothing chosen for this image without changing persistent outfit state or inventing historical wear. Ordinary modest clothing does not acquire a relationship grant requirement; explicit persona limits remain applicable. The existing LifeRuntime, Images ledger, translation and automatic original delivery continue to own execution.

Local verification: **33 affected checks passed** across the native image/Comfy and dialogue/delivery paths, including five new dialogue checks. The full ingest test covers automatic native tool selection by a synthetic Gateway, no current outfit, translated queued work, one provider submission, then one original delivered to the original scope through a recording sender. Other checks cover disabled/unconfigured capabilities, actor isolation and the visible current outfit. Static checks and diff whitespace checks passed. Actual model interpretation of the private persona is not claimed by these substitutes.

`scripts/comfy_dialogue_acceptance.py` provides a single ordinary natural-language acceptance entry. It imports only the existing pytest-free `tests/support.py` and production modules. Required input fields are `{actor_id, role, relationship_background, config_version, request_text}`; the complete pinned role and actual configuration version are retained. Relationship expression and version checking use the production projection with an explicitly isolated snapshot client. Identity/Memory and outbound are isolated fixtures; Gateway is the existing registered service. Its credential is resolved from the current host's configured `token_env`, never written to the receipt.

The coordinator can run inside its isolated candidate container:

```sh
PYTHONPATH=src python scripts/comfy_dialogue_acceptance.py \
  --contracts /contracts/text-dialogue/v1 \
  --settings /config/settings.json \
  --private-input /acceptance/private-dialogue-input.json \
  --output /acceptance-output \
  --run-dialogue
```

Paths are supplied by the coordinator's wrapper. Default mode only discovers/compiles the selected real workflow. `--run-dialogue` explicitly permits one actual Gateway dialogue with automatic tool choice; neither mode invokes `Images.work` or submits GPU generation. `--generate` is a separate explicit option for polling/submitting only an existing single image job. No real channel sender exists in this entry. The original input, assistant/tool receipts, queued image IDs, failure states and optional artifact hashes are written to the private output receipt. Reusing the same output directory does not repeat the model dialogue, including interrupted attempts.

The script's single regression check passed (**1 passed, 0.53s**) after fixing its missing relationship context pin. It confirms tool selection, a queued unsubmitted job, no Comfy POST, and no second dialogue on repeated invocation. The clock is initialized to actual time before any configuration or ingest. No actual Gateway/GPU/QQ call, NAS write, push or deployment was performed for this local fix; the coordinator owns those next steps.
