import asyncio
import copy

import httpx

from test_source_sync import SourceHarness
from tianshu_companion.app import create_app
from tianshu_companion.contracts import digest


def test_receipt_query_is_exact_caller_scoped_read_only_and_restart_durable(tmp_path):
    async def run():
        h = SourceHarness(tmp_path / "core.db", silence_ms=0)
        request = h.fanout(targets=["actor:a"])
        query = dict(
            schema_version=1,
            request_id="receipt:test",
            idempotency_key=request["command"]["idempotency_key"],
            semantic_digest=digest({k: v for k, v in request.items() if k != "command"}),
        )

        async def check(body, token="bridge"):
            app = create_app(
                core=h.core,
                tokens={"nonebot": "bridge", "platform": "platform", "memory": "memory"},
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="https://fixture"
            ) as client:
                return await client.post(
                    "/internal/v1/conversation/ingest-receipt",
                    json=body,
                    headers={"Authorization": "Bearer " + token},
                )

        try:
            before = h.core.store.db.total_changes
            assert (await check(query)).json()["found"] is False
            assert h.core.store.db.total_changes == before
            await h.submit(request)
            await h.cycles()
            before = h.core.store.db.total_changes
            assert (await check(query)).json()["found"] is True
            assert (await check(query, "platform")).json()["found"] is False
            assert (await check(query, "memory")).status_code == 403
            assert (await check(dict(query, semantic_digest="0" * 64))).status_code == 409
            assert h.core.store.db.total_changes == before
            await h.core.close()
            h.core = h.new_core()
            h.core.recover()
            assert (await check(query)).json()["found"] is True
            repeat = copy.deepcopy(request)
            repeat["command"]["request_id"] = "request:again"
            await h.submit(repeat)
            await h.cycles()
            assert len(h.gateway.calls) == 1
        finally:
            await h.core.close()

    asyncio.run(run())
