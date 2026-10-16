"""The input-only port retains the same authenticated history snapshot contract."""

import asyncio
import copy
import json

import httpx
import pytest

from support import Harness
from tianshu_companion.clients import JsonService, Memory, utc
from tianshu_companion.contracts import Fault


@pytest.mark.parametrize(
    "change", [None, "scope", "request", "association", "version", "expiry", "known"]
)
def test_input_history_client_validates_authority_snapshot_and_lease(change):
    async def run():
        h = Harness(silence_ms=0)
        try:
            await h.ingest()
            await h.cycles()
            turn = h.turns()[0]
            expected = {
                k: turn["preparation"][k]
                for k in ("scope_version", "association_version", "scope_checks")
            }
            snapshot = await h.memory.history(turn["origin"], turn["scope"], expected=expected)

            async def handler(request):
                assert request.url.path == "/internal/v1/memory/context/input-history"
                body = json.loads(request.content)
                assert body["include_associated"] is True
                result = dict(
                    schema_version=1,
                    request_id=body["query"]["request_id"],
                    effective_scope=turn["scope"],
                    scope_version=expected["scope_version"],
                    snapshot=copy.deepcopy(snapshot),
                    verified_at=utc(h.clock()),
                    valid_until=utc(h.clock() + 30),
                )
                if change == "scope":
                    result["effective_scope"] = dict(turn["scope"], person_id="person:other")
                elif change == "request":
                    result["request_id"] = "request:other"
                elif change == "association":
                    result["snapshot"]["association_version"] += 1
                elif change == "version":
                    result["scope_version"] += 1
                elif change == "expiry":
                    result["valid_until"] = utc(h.clock() - 1)
                elif change == "known":
                    result["snapshot"]["history_checks"][0]["history_version"] += 1
                return httpx.Response(200, json=result)

            client = JsonService(
                "https://synthetic.invalid", "fixture", transport=httpx.MockTransport(handler)
            )
            try:
                memory = Memory(h.contracts, client)
                if change:
                    with pytest.raises(Fault):
                        await memory.input_history(
                            turn["origin"], turn["scope"], expected=expected, known=snapshot
                        )
                else:
                    assert (
                        await memory.input_history(
                            turn["origin"], turn["scope"], expected=expected, known=snapshot
                        )
                        == snapshot
                    )
            finally:
                await client.close()
        finally:
            await h.core.close()

    asyncio.run(run())
