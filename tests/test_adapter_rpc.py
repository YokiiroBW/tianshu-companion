"""Durable RPC behavior, including restart and uncertain native outcomes."""

import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "integrations" / "shared"))
from tianshu_adapter_rpc import AdapterService, MAX_RESPONSE, PREFIX, _json  # noqa: E402


class AdapterRpcTests(unittest.TestCase):
    def test_message_revision_is_independent_of_binding_revision(self):
        from jsonschema import Draft202012Validator, FormatChecker
        from referencing import Registry, Resource

        contracts = next((parent / "contracts" for parent in Path(__file__).resolve().parents
                          if (parent / "contracts" / "bot-connection" / "v1" / "schemas" /
                              "bot-connection.json").is_file()), None)
        self.assertIsNotNone(contracts, "published bot-connection contract is missing")
        bot_schema = json.loads((contracts / "bot-connection" / "v1" / "schemas" /
                                 "bot-connection.json").read_text(encoding="utf-8"))
        common = json.loads((contracts / "text-dialogue" / "v1" / "schemas" /
                             "common.json").read_text(encoding="utf-8"))
        event_schema = {"$schema": bot_schema["$schema"], "$id": bot_schema["$id"],
                        "$defs": bot_schema["$defs"], **bot_schema["$defs"]["event_request"]}
        registry = Registry().with_resource(common["$id"], Resource.from_contents(common))
        validator = Draft202012Validator(event_schema, registry=registry,
                                         format_checker=FormatChecker())

        async def run():
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
                async def accounts():
                    return [{"id": "42", "platform": "qq", "label": "42"}]
                async def send(*args):
                    return "1"
                service = AdapterService(Path(directory) / "db", "astrbot", accounts, send)
                async def post(route, data):
                    return await service.handle(PREFIX + route, "Bearer " + service.access_key,
                                                _json(data).encode())
                binding = dict(connection_id="conn", account_id="42",
                               conversation={"kind": "private", "id": "7"},
                               allowed_authors=["7"], enabled=True)
                for revision in (2, 4):
                    status, result = await post("/bindings/apply", {
                        **binding, "request_id": f"revision-{revision}", "revision": revision,
                    })
                    self.assertEqual(status, 200, result)
                    self.assertTrue(await service.capture("42", "private:7", "7",
                        f"event-{revision}", "2026-09-28T00:00:00Z", "hello"))
                    status, result = await post("/events/poll", {"connection_id": "conn", "limit": 20})
                    self.assertEqual(status, 200, result)
                    self.assertEqual(len(result["events"]), 1)
                    event = result["events"][0]
                    validator.validate(event["event"])
                    self.assertEqual(event["event"]["revision"], 1)
                    await post("/events/ack", {"connection_id": "conn", "event_ids": [event["id"]]})
                status, result = await post("/bindings/status", {"connection_id": "conn"})
                self.assertEqual(status, 200)
                self.assertEqual(result["binding"]["revision"], 4)
                service.close()
        asyncio.run(run())

    def test_large_chinese_events_poll_in_bounded_ackable_batches(self):
        async def run():
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
                async def accounts():
                    return [{"id": "42", "platform": "qq", "label": "42"}]
                async def send(*args):
                    return "1"
                service = AdapterService(Path(directory) / "db", "nonebot", accounts, send)
                async def post(route, data):
                    return await service.handle(PREFIX + route, "Bearer " + service.access_key,
                                                _json(data).encode())
                status, result = await post("/bindings/apply", dict(
                    request_id="r1", connection_id="conn", revision=1, account_id="42",
                    conversation={"kind": "private", "id": "7"},
                    allowed_authors=["7"], enabled=True,
                ))
                self.assertEqual(status, 200, result)
                for index in range(6):
                    self.assertTrue(await service.capture("42", "private:7", "7",
                        f"event-{index}", "2026-09-28T00:00:00Z", "界" * 8000))
                expected = [row[0] for row in service.db.execute(
                    "SELECT id FROM events WHERE connection_id='conn' ORDER BY created,id")]
                seen, batches = [], []
                while True:
                    status, result = await post("/events/poll", {"connection_id": "conn", "limit": 20})
                    self.assertEqual(status, 200, result)
                    self.assertLessEqual(len(_json(result).encode("utf-8")), MAX_RESPONSE)
                    ids = [item["id"] for item in result["events"]]
                    if not ids:
                        break
                    seen.extend(ids)
                    batches.append(len(ids))
                    self.assertEqual(seen, expected[:len(seen)])
                    status, ack = await post("/events/ack", {"connection_id": "conn",
                                                   "event_ids": ids})
                    self.assertEqual(status, 200, ack)
                    self.assertEqual(ack["acknowledged"], ids)
                self.assertEqual(seen, expected)
                self.assertGreater(len(batches), 1)
                self.assertEqual(service.db.execute(
                    "SELECT COUNT(*) FROM events WHERE acked=0").fetchone()[0], 0)
                service.close()
        asyncio.run(run())

    def test_durable_binding_events_send_and_restart(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "adapter.db"
                sends = []

                async def accounts():
                    return [{"id": "42", "platform": "qq", "label": "Test account"}]

                async def send(self_id, conversation, text):
                    sends.append((self_id, conversation, text))
                    return 876

                service = AdapterService(path, "nonebot", accounts, send)
                key, instance = service.access_key, service.instance_id
                displayed = subprocess.run(
                    [sys.executable, str(Path(__file__).resolve().parents[1] / "integrations" /
                                      "shared" / "tianshu_adapter_rpc.py"), "show-key", str(path)],
                    capture_output=True, text=True, check=True,
                )
                self.assertEqual(displayed.stdout.strip(), key)

                async def post(route, value, token=key, owner=service):
                    import json
                    return await owner.handle(PREFIX + route, "Bearer " + token,
                                              json.dumps(value, ensure_ascii=False).encode())

                self.assertEqual((await post("/capabilities", {}, "wrong"))[0], 401)
                self.assertEqual((await post("/capabilities", {}))[1]["protocol"],
                                 "tianshu.bot-adapter/v1")
                self.assertEqual((await service.handle("/tianshu/adapter/v2/capabilities",
                                  "Bearer " + key, b"{}"))[0], 404)
                binding = dict(request_id="req1", connection_id="conn", revision=1,
                               account_id="42", conversation={"kind": "group", "id": "99"},
                               allowed_authors=["7"], enabled=False)
                self.assertEqual((await post("/bindings/apply", binding))[1]["enabled"], False)
                self.assertEqual((await post("/bindings/apply", binding))[0], 200)
                binding.update(request_id="req2", revision=2, enabled=True)
                self.assertEqual((await post("/bindings/apply", binding))[0], 200)
                self.assertEqual((await post("/bindings/apply", {**binding, "enabled": False}))[1]["code"],
                                 "idempotency_conflict")
                self.assertFalse(await service.capture("42", "group:99", "8", "1", "2026-01-01Z", "no"))
                self.assertTrue(await service.capture("42", "group:99", "7", "1", "2026-01-01Z", "hello"))
                self.assertTrue(await service.capture("42", "group:99", "7", "1", "2026-01-01Z", "hello"))
                events = (await post("/events/poll", {"connection_id": "conn", "limit": 20}))[1]["events"]
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0]["event"]["platform_id"], instance)
                event_id = events[0]["id"]
                self.assertEqual((await post("/events/ack", {"connection_id": "other", "event_ids": [event_id]}))[1]["acknowledged"], [])
                self.assertEqual((await post("/events/ack", {"connection_id": "conn", "event_ids": [event_id]}))[1]["acknowledged"], [event_id])

                delivery = dict(reply_id="reply1", attempt_id="attempt1", namespace="qq",
                                conversation_id="group:99", thread_id=None, text="reply",
                                turn_id="turn1", segment_sequence=1)
                receipt = (await post("/messages/send", {"connection_id": "conn", "delivery": delivery}))[1]
                self.assertEqual(receipt["state"], "sent")
                self.assertEqual(receipt["channel_message_ids"], ["876"])
                self.assertEqual((await post("/messages/send", {"connection_id": "conn", "delivery": delivery}))[1], receipt)
                self.assertEqual(len(sends), 1)
                self.assertEqual((await post("/messages/send", {"connection_id": "conn", "delivery": {**delivery, "text": "changed"}}))[1]["code"], "idempotency_conflict")
                too_long = {**delivery, "attempt_id": "attempt2", "text": "汉" * 11000}
                self.assertEqual((await post("/messages/send", {"connection_id": "conn", "delivery": too_long}))[1]["state"], "failed")
                self.assertEqual(len(sends), 1)
                service.close()

                recovered = AdapterService(path, "nonebot", accounts, send)
                self.assertEqual((recovered.access_key, recovered.instance_id), (key, instance))
                self.assertEqual((await post("/messages/send", {"connection_id": "conn", "delivery": delivery}, owner=recovered))[1], receipt)
                self.assertEqual(len(sends), 1)
                recovered.db.execute("INSERT INTO deliveries VALUES(?,?,?,?,?)",
                                     ("conn", "reply2", "attempt1", "hash",
                                      '{"reply_id":"reply2","attempt_id":"attempt1","state":"inflight","channel_message_ids":[]}'))
                recovered.close()
                recovered = AdapterService(path, "nonebot", accounts, send)
                status = (await post("/messages/status", {"connection_id": "conn", "reply_id": "reply2", "attempt_id": "attempt1"}, owner=recovered))[1]
                self.assertEqual(status["receipt"]["state"], "unknown")
                binding.update(request_id="req3", revision=3, enabled=False)
                self.assertEqual((await post("/bindings/apply", binding, owner=recovered))[0], 200)
                self.assertFalse(await recovered.capture("42", "group:99", "7", "2", "2026-01-01Z", "hello"))
                self.assertEqual((await post("/messages/send", {"connection_id": "conn", "delivery": {**delivery, "attempt_id": "attempt3"}}, owner=recovered))[0], 403)
                recovered.close()
        asyncio.run(run())

    def test_unknown_send_is_never_repeated(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                calls = []

                async def accounts():
                    return [{"id": "42", "platform": "qq", "label": "42"}]

                async def send(*args):
                    calls.append(args)
                    raise TimeoutError()

                service = AdapterService(Path(directory) / "db", "astrbot", accounts, send)
                key = service.access_key

                async def post(route, data):
                    import json
                    return await service.handle(PREFIX + route, "Bearer " + key,
                                                json.dumps(data, ensure_ascii=False).encode())

                await post("/bindings/apply", dict(request_id="r1", connection_id="c", revision=1,
                    account_id="42", conversation={"kind": "private", "id": "7"},
                    allowed_authors=["7"], enabled=True))
                delivery = dict(reply_id="r", attempt_id="a", namespace="qq",
                                conversation_id="private:7", thread_id=None, text="hello",
                                turn_id="t", segment_sequence=1)
                first = (await post("/messages/send", {"connection_id": "c", "delivery": delivery}))[1]
                second = (await post("/messages/send", {"connection_id": "c", "delivery": delivery}))[1]
                self.assertEqual(first, second)
                self.assertEqual(first["state"], "unknown")
                self.assertEqual(len(calls), 1)
                service.close()
        asyncio.run(run())

    def test_utf8_byte_limit_and_input_character_limit(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                calls = []
                async def accounts():
                    return [{"id": "42", "platform": "qq", "label": "42"}]
                async def send(*args):
                    calls.append(args)
                    return "991"
                service = AdapterService(Path(directory) / "db", "nonebot", accounts, send)
                import json
                async def post(route, data):
                    return await service.handle(PREFIX + route, "Bearer " + service.access_key,
                                                json.dumps(data, ensure_ascii=False).encode())
                await post("/bindings/apply", dict(request_id="r1", connection_id="c", revision=1,
                    account_id="42", conversation={"kind": "private", "id": "7"},
                    allowed_authors=["7"], enabled=True))
                self.assertTrue(await service.capture("42", "private:7", "7", "1", "2026-01-01Z", "界" * 8000))
                self.assertFalse(await service.capture("42", "private:7", "7", "2", "2026-01-01Z", "界" * 8001))
                base = dict(reply_id="r", namespace="qq", conversation_id="private:7",
                            thread_id=None, turn_id="t", segment_sequence=1)
                exact = {**base, "attempt_id": "a1", "text": "界" * 10922 + "ab"}
                self.assertEqual(len(exact["text"].encode()), 32768)
                self.assertEqual((await post("/messages/send", {"connection_id": "c", "delivery": exact}))[1]["state"], "sent")
                excess = {**base, "attempt_id": "a2", "text": exact["text"] + "c"}
                self.assertEqual((await post("/messages/send", {"connection_id": "c", "delivery": excess}))[1]["state"], "failed")
                self.assertEqual(len(calls), 1)
                service.close()
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
