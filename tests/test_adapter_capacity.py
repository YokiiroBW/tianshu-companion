"""A busy adapter must keep working without forgetting completed delivery identities."""

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "integrations" / "shared"))
import tianshu_adapter_rpc as rpc  # noqa: E402


async def accounts():
    return [{"id": "42", "platform": "qq", "label": "synthetic"}]


async def post(service, route, body):
    return await service.handle(
        rpc.PREFIX + route, "Bearer " + service.access_key, rpc._json(body).encode()
    )


def binding(request_id="binding-1", revision=1):
    return dict(
        request_id=request_id,
        connection_id="conn",
        revision=revision,
        account_id="42",
        conversation={"kind": "private", "id": "7"},
        allowed_authors=["7"],
        enabled=True,
    )


def test_completed_events_release_payload_budget_but_keep_replay_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(rpc, "MAX_PENDING", 2)

    async def run():
        async def send(*args):
            return "sent"

        path = tmp_path / "adapter.db"
        service = rpc.AdapterService(path, "nonebot", accounts, send)
        assert (await post(service, "/bindings/apply", binding()))[0] == 200
        # Simulate an existing installation whose completed history exceeds the
        # old lifetime quota. Payload cleanup also applies on upgrade/restart.
        with service.db:
            service.db.execute("BEGIN")
            service.db.executemany(
                "INSERT INTO events VALUES(?,?,?,?,?,?,?)",
                (
                    (f"old-{i}", "conn", f"native-{i}", "7", '{"text":"old"}', 1, i)
                    for i in range(rpc.MAX_HISTORY + 1)
                ),
            )
        service.close()
        service = rpc.AdapterService(path, "nonebot", accounts, send)
        try:
            assert (
                service.db.execute(
                    "SELECT COUNT(*) FROM events WHERE acked=1 AND payload<>'{}'"
                ).fetchone()[0]
                == 0
            )
            for native in ("new-1", "new-2"):
                assert await service.capture(
                    "42", "private:7", "7", native, "2026-10-03T00:00:00Z", "new"
                )
            assert not await service.capture(
                "42", "private:7", "7", "new-3", "2026-10-03T00:00:00Z", "new"
            )
            # Replays are accepted even when the *pending* capacity is full.
            assert await service.capture(
                "42", "private:7", "7", "native-0", "2026-10-03T00:00:00Z", "old"
            )
            assert await service.capture(
                "42", "private:7", "7", "new-1", "2026-10-03T00:00:00Z", "new"
            )
            status, result = await post(
                service, "/events/poll", {"connection_id": "conn", "limit": 20}
            )
            assert status == 200
            assert len(result["events"]) == 2
            ids = [event["id"] for event in result["events"]]
            request = {"connection_id": "conn", "event_ids": ids}
            assert (await post(service, "/events/ack", request))[1]["acknowledged"] == ids
            assert (await post(service, "/events/ack", request))[1]["acknowledged"] == ids
            assert await service.capture(
                "42", "private:7", "7", "new-3", "2026-10-03T00:00:00Z", "new"
            )
            assert (
                service.db.execute("SELECT COUNT(*) FROM events WHERE payload<>'{}'").fetchone()[0]
                == 1
            )
        finally:
            service.close()

    asyncio.run(run())


@pytest.mark.parametrize("observation", [False, True])
def test_completed_receipts_do_not_exhaust_send_or_binding_capacity(tmp_path, observation):
    async def run():
        calls = []

        async def send(*args):
            calls.append(args)
            if args[-1] == "timeout":
                raise TimeoutError
            return "sent-1"

        path = tmp_path / "adapter.db"
        service = rpc.AdapterService(path, "nonebot", accounts, send)
        try:
            with service.db:
                service.db.execute("BEGIN")
                service.db.executemany(
                    "INSERT INTO deliveries VALUES(?,?,?,?,?)",
                    (
                        (
                            "conn",
                            f"old-{i}",
                            "attempt",
                            "digest",
                            rpc._json(
                                {
                                    "reply_id": f"old-{i}",
                                    "attempt_id": "attempt",
                                    "state": "sent",
                                    "channel_message_ids": [str(i)],
                                }
                            ),
                        )
                        for i in range(rpc.MAX_HISTORY + 1)
                    ),
                )
                service.db.executemany(
                    "INSERT INTO apply_requests VALUES(?,?,?)",
                    ((f"old-{i}", "digest", "{}") for i in range(rpc.MAX_HISTORY + 1)),
                )
            assert (await post(service, "/bindings/apply", binding()))[0] == 200
            delivery = dict(
                reply_id="new-reply",
                attempt_id="attempt",
                namespace="qq",
                conversation_id="private:7",
                thread_id=None,
                text="hello",
                turn_id="turn",
                segment_sequence=1,
            )
            route = "/messages/send"
            body = {"connection_id": "conn", "delivery": delivery}
            if observation:
                policy = {"observe": True, "mode": "whitelist", "list": ["7"]}
                status, result = await post(
                    service,
                    "/observation/apply",
                    dict(
                        request_id="observe",
                        account_id="42",
                        revision=1,
                        enabled=True,
                        group_policy=policy,
                        private_policy=policy,
                    ),
                )
                assert status == 200, result
                body.update(account_id="42", policy_revision=1)
                route = "/observation/messages/send"
            status, receipt = await post(service, route, body)
            assert status == 200, receipt
            assert receipt["state"] == "sent"
            assert (await post(service, route, body)) == (200, receipt)
            conflict = {**body, "delivery": {**delivery, "text": "changed"}}
            assert (await post(service, route, conflict))[0] == 409
            unknown = {**body, "delivery": {**delivery, "reply_id": "uncertain", "text": "timeout"}}
            assert (await post(service, route, unknown))[1]["state"] == "unknown"
            assert len(calls) == 2
            service.close()
            service = rpc.AdapterService(path, "nonebot", accounts, send)
            assert (await post(service, route, body)) == (200, receipt)
            assert (await post(service, route, unknown))[1]["state"] == "unknown"
            assert len(calls) == 2
            status, result = await post(
                service,
                "/messages/status",
                dict(connection_id="conn", reply_id="old-0", attempt_id="attempt"),
            )
            assert status == 200, result
            assert result["receipt"]["state"] == "sent"
        finally:
            service.close()

    asyncio.run(run())
