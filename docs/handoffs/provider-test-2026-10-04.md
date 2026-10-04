# Stable provider session metadata — local candidate

Base: `11e6cc37057d60e389c8837926fe61352065a605`.

The existing gateway client derives an opaque stable session from the turn's actor and conversation. New turn IDs do not change it, while actors and conversations remain separate. It sends this only as internal routing metadata; gateway authentication, scope and execution receipts remain authoritative.

Chat already has those fields. Four existing background callers now pass their existing actor and logical conversation: diary per actor, daily life / influences per actor's life session, and chapter generation per work. Diary's stored `conversation_id` is the actor ID set by `_prepare_diary`; no identity inference or database migration is introduced.

Verification: 88 tests in `test_gateway`, `test_daily_life`, `test_life`, `test_life_chain`, `test_writing` and `test_writing_chain` passed. Gateway test records stable headers across turns and isolation across actors/conversations. Targeted Ruff format/check and `git diff --check` passed. The three-product local joint suite passed separately. The first test invocation failed collection because this pre-existing venv lacked the editable source path; setting `PYTHONPATH=src;tests` resolved collection without dependency changes.

No real model, QQ output, provider default, deployment or migration was performed here. The coordinator owns deployment together with the matching gateway candidate. Legacy minimal test turns without actor/conversation emit no metadata; actual Chat and affected background callers now provide it.
