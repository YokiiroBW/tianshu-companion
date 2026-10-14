"""Actual SDK normalization and durable RPC metadata, without QQ access."""

import asyncio
import json
from pathlib import Path
import tempfile

from tianshu_adapter_rpc import AdapterService, PREFIX, onebot_input
from tianshu_companion.contracts import digest
from tianshu_companion.observation import Observations


def segments():
    return [
        dict(type="reply", data={"id": "earlier:7"}),
        dict(type="at", data={"qq": "123"}),
        dict(type="text", data={"text": " 你说的那本呢？"}),
        dict(type="at", data={"qq": "42"}),
    ]


def test_mentions_and_replies_are_structured_and_media_is_not_silently_removed():
    value = onebot_input(segments(), "42", group=True)
    assert value["text"] == " 你说的那本呢？"
    assert value["mentioned"] is True
    assert value["message_context"] == {
        "reply_ids": ["earlier:7"],
        "mentioned_accounts": ["123", "42"],
    }
    unsupported = onebot_input(
        segments() + [dict(type="image", data={"file": "unused"})], "42", group=True
    )
    assert unsupported["content_state"] == "unsupported" and unsupported["text"] == ""


def test_durable_event_preserves_metadata_after_reopen():
    async def run():
        async def accounts():
            return [{"id": "42", "platform": "qq", "label": "fixture"}]

        async def send(*args):
            return "confirmed:1"

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "adapter.db"
            service = AdapterService(path, "onebot", accounts, send)
            token = service.access_key

            async def call(operation, body):
                return await service.handle(
                    PREFIX + operation, "Bearer " + token, json.dumps(body).encode()
                )

            try:
                status, result = await call(
                    "/bindings/apply",
                    dict(
                        request_id="apply:one",
                        connection_id="conn",
                        revision=1,
                        account_id="42",
                        conversation=dict(kind="group", id="9"),
                        allowed_authors=["123"],
                        enabled=True,
                    ),
                )
                assert status == 200, result
                value = onebot_input(segments(), "42", group=True)
                assert await service.capture(
                    "42",
                    "group:9",
                    "123",
                    "event:8",
                    "2026-10-14T00:00:00Z",
                    value["text"],
                    context=value["message_context"],
                )
                service.close()
                service = AdapterService(path, "onebot", accounts, send)
                status, result = await call("/events/poll", dict(connection_id="conn", limit=20))
                assert status == 200
                event = result["events"][0]["event"]
                assert event["schema_version"] == 3
                assert event["message_context"] == value["message_context"]
                assert service.access_key == token
                assert len(result["events"]) == 1

                status, result = await call(
                    "/observation/apply",
                    dict(
                        request_id="observe:one",
                        account_id="42",
                        revision=1,
                        enabled=True,
                        group_policy=dict(observe=True, mode="observe_only", list=[]),
                        private_policy=dict(observe=False, mode="observe_only", list=[]),
                    ),
                )
                assert status == 200, result
                assert await service.capture_observation(
                    "42",
                    "group:9",
                    "123",
                    "event:9",
                    "2026-10-14T00:00:01Z",
                    value["text"],
                    True,
                    context=value["message_context"],
                )
                _, observed = await call("/observation/poll", dict(account_id="42", limit=20))
                event = observed["events"][0]["event"]
                assert event["schema_version"] == 4
                transferred = []

                class MemoryRecorder:
                    async def call(self, endpoint, body, **options):
                        transferred.append(body)
                        return dict(
                            source_ref=body["source_ref"],
                            state="accepted",
                            archive_state="archived",
                        )

                payload = dict(
                    event=event,
                    source_ref="observed:9",
                    source_digest=digest(event),
                    scope_version=1,
                    archive_epoch=1,
                )
                inbox_path = Path(directory) / "core-observations.db"
                inbox = Observations(inbox_path, MemoryRecorder())
                assert inbox.ingest("platform", payload)["state"] == "accepted"
                restarted = Observations(inbox_path, MemoryRecorder())
                assert restarted.ingest("platform", payload)["state"] == "duplicate"
                assert await restarted.flush()
                assert len(transferred) == 1
                assert transferred[0]["event"]["message_context"] == value["message_context"]
            finally:
                service.close()

    asyncio.run(run())
