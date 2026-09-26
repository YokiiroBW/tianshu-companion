# PROVIDER-C1 — opt-in default model selection

Baseline: `e94b609099365f75ca933d9fed03cdfbc83ec235`; branch `codex/provider-companion-20260926`.

## Implemented

`Core(default_model_selector=...)` explicitly injects a trusted selector. Omitting it preserves static `config_version`. Dynamic admission reserves the existing preparing slot with no selected version, then resolves before the first Memory call. The selector runs outside SQLite transactions with a five-second timeout. Only a valid result with at least 65 seconds remaining is pinned in the existing turn JSON. Generation rechecks the remaining lease immediately before committing its submission intent. No new database schema or migration.

Selection uses the authenticated, persisted turn scope. Tick's existing per-turn jobs prevent duplicate preparation; after selection the transaction rereads the turn, checks cancellation and current inputs, and writes only if `config_version` is still null. Once pinned, dependency retries and later ticks preserve the version. Startup retains the existing rule that interrupted preparation/generation fails; it never reselects or replays uncertain generation. New turns after restart select the current default. Cancellation/retraction wins over late selection replies, including failures. Selector exceptions become a fixed dependency failure, with no static fallback.

Persona, memory, origin checks and gateway routing continue through the existing chain. A selector result is not a grant: the gateway must independently enforce authorization and revocation when executing the pinned version. Tests use synthetic collaborators and real SQLite, not a real model or NAS.

## Internal port and coordinator requirements

`DefaultModelSelector.select(SelectionRequest) -> ModelSelection` is an in-process port, not a proposed wire contract.

- Request: turn_id, actor_id, person_id, audience, conversation_id, caller_service=`companion`, workload=`companion.text`. Scope comes from the server-side turn, never browser-provided model settings. No messages, credentials, or provider URL are needed.
- Result: positive integer config_version, finite UTC epoch expires_at, revoked boolean, caller_service/workload binding. `revoked=False` is only structural input validation; an authenticated platform authority must actually prove the binding is published, enabled and authorized, and the gateway remains authoritative for execution-time revocation.
- Production adapter must use the dedicated companion service credential, enforce its own response/transport bounds and cancellation, and project the coordinator's frozen response into this DTO. Platform selection/renewal must complete the exact version's publication and grant before returning it; no all-version wildcard grant. A late default change applies only to future selections.
- Existing Gateway.generate sends `X-Tianshu-Config-Version`, `X-Tianshu-Workload: companion.text`, `X-Tianshu-Turn-ID`; its bearer token identifies companion. It validates the returned route receipt's version and caller. Dynamic gateway authority must accept only the selected published version/workload and reject withdrawn versions, without redirecting old turns to latest.
- Minimal future assembly change: an explicitly enabled selector adapter passed to Core, with authenticated platform transport and lease semantics defined by the root contract. No new app configuration keys, HTTP routes, production transport, or grant widening in this task.
- Expired pinned selections fail honestly; this implementation does not renew or substitute an old turn's version. Sustainable default publication/renewal belongs to platform and the coordinated protocol.

## Verification

New `tests/test_model_selection.py`: 10 passing tests for in-flight/default changes, delayed competing turns without an open SQLite transaction, cancellation and retraction, revoked and insufficient leases, timeout, exception sanitization, trusted identity context, disk restart, model-slot lease consumption, and dependency retry pinning.

Targeted unittest run covering the then-eight new cases plus core/edges/gateway/bootstrap: 35 passed. Final ten-case file: `pytest tests/test_model_selection.py -q`, 10 passed. `ruff format --check` and `ruff check` on all changed Python files passed; `git diff --check` passed.

Interpreter: root `.runtime/nas-a1-r1-venv/Scripts/python.exe`, with PYTHONPATH=`src;integrations/nonebot` and TIANSHU_CONTRACTS pointing to root `contracts/text-dialogue/v1`. Installed the exact project-declared pytest 9.0.2 and ruff 0.15.6 into that existing development environment; manifests unchanged.

Full component run: 581 passed, 18 skipped, 101 subtests passed; six existing tests failed solely because this manually created worktree lacked ignored `.runtime/workspace-context.json`. Added local development context with the root workspace path and fixed baseline, then reran exactly the six failures via `pytest --lf -q`: 6 passed. The two additional new tests added after full-suite collection are included in the separate final 10/10 result above. No remaining observed failures. TLS and external Memory tests without their opt-in environment remain skipped, not accepted as real integration. Logs are in ignored `.runtime/provider-full-tests.log` and `.runtime/provider-recheck.log`. No production configuration, real provider calls, deployment or NAS acceptance performed.

## 2026-09-26 backend wiring continuation

`HttpDefaultModelSelector` now calls the platform's authenticated internal select route through the existing `JsonService`. `build_runtime` assembles it only when `provider_self_service` is enabled and `services.provider_selector` supplies a configured platform URL/token; the previous static path remains available. The earlier “no production transport or app configuration key” statement describes the first C1 delivery only.

The 10 model selection tests, five bootstrap tests, one gateway test, and four root platform-gateway-companion joint tests passed with isolated fixtures. The joint test runs the real Core turn chain against a local HTTP test transport and a recorded TLS upstream; production internal HTTPS/certificate and NAS wiring remain to be verified. See the root `docs/handoffs/PROVIDER-BACKEND-2026-09-26.md`.

### Review repair continuation

Selection now occurs before the first input's accepting transaction. The exact version and preassigned turn ID are saved on its collection, then copied into the queued turn on seal. A queued turn therefore cannot consult a switched default during background tick; failed selection refuses the input before it is accepted. Existing static mode and bounded lease checks remain. The updated targeted model suite has 11 passing cases, including a queued-before-switch race; the root six-case HTTP/TLS suite checks the same sequence against real platform and gateway service routes. Since `source_sync` is shared, a complete `pytest -q` run passed 600 tests with 8 skips and 101 subtests (107.40 seconds). No production HTTPS certificate or NAS validation was performed.
