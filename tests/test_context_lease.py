import asyncio
import copy

import pytest

from support import Harness
from tianshu_companion.clients import utc
from tianshu_companion.context import scope_check
from tianshu_companion.contracts import Fault


def test_group_history_renews_original_human_lease_and_revocation_still_denies():
    async def run():
        h = Harness(silence_ms=0)
        try:
            await h.ingest(account="1001", channel="group:123", group=True, text="周末约在公园门口")
            await h.cycles()
            previous = h.turns()[0]
            await h.ingest(account="1002", channel="group:123", group=True, text="刚才约在哪里？")
            h.core._seal_due(h.clock())
            current = h.turns()[-1]
            old_ref = previous["origin"]["assertion_ref"]
            h.origins.values[old_ref]["expires_at"] = utc(h.clock() - 1)
            previous["source_deadline"] = h.clock() - 1
            h.core.store.put("turns", previous)
            renewed = []

            class Sender:
                def expression_available(self, channel):
                    return True

                async def expression_context(self, origin, scope, channel):
                    ref = origin["origin"]["assertion_ref"]
                    assert ref == old_ref and scope == previous["scope"]
                    assert scope["person_id"] != current["scope"]["person_id"]
                    context = h.origins.values[ref]
                    if context.get("revoked"):
                        raise Fault("forbidden")
                    context["expires_at"] = utc(h.clock() + 60)
                    renewed.append(ref)
                    return dict(
                        origin=origin["origin"],
                        scope=scope,
                        channel=channel,
                        expires_at=context["expires_at"],
                    )

            h.core.sender = Sender()
            await h.core._verify_checks(current, [scope_check(previous)])
            assert renewed == [old_ref]
            forged = copy.deepcopy(scope_check(previous))
            forged["origin"] = current["origin"]
            with pytest.raises(Fault):
                await h.core._verify_checks(current, [forged])
            assert renewed == [old_ref]
            h.origins.values[old_ref]["revoked"] = True
            with pytest.raises(Fault):
                await h.core._verify_checks(current, [scope_check(previous)])
            assert renewed == [old_ref]
        finally:
            await h.core.close()

    asyncio.run(run())
