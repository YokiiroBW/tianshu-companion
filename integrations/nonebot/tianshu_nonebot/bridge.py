"""Only authenticated SDK events may enter this module; no public event-upload route."""

import asyncio
import time

from tianshu_companion.clients import command, uid, utc
from tianshu_companion.contracts import Fault, digest


def _request(
    namespace,
    binding_id,
    conversation,
    thread,
    account,
    message_id,
    text,
    sent_at,
    origin,
    *,
    targets=(),
    references=(),
    revision=1,
    kind="message",
):
    channel = dict(
        namespace=namespace,
        binding_id=binding_id,
        channel_conversation_id=str(conversation),
        thread_id=str(thread) if thread is not None else None,
    )
    message_key = dict(channel=channel, message_id=str(message_id), revision=revision)
    return dict(
        command=command(origin, "ingest:" + digest([message_key, list(targets)]), time.time()),
        message_key=message_key,
        author=dict(namespace=namespace, immutable_account_id=str(account)),
        sent_at=utc(sent_at),
        kind=kind,
        parts=[] if kind == "retract" else [dict(kind="text", text=text)],
        reply_refs=list(references),
        mentioned_accounts=[],
        target_actor_ids=list(targets),
    )


def normalize_onebot(event, binding_id, origin, *, targets=(), revision=1):
    if event.get("post_type") != "message" or event.get("message_type") not in {"private", "group"}:
        raise Fault("invalid_input")
    if str(event["user_id"]) == str(event["self_id"]):
        raise Fault("invalid_input")
    parts, mentions, refs = [], [], []
    conversation = (
        ("group:" + str(event["group_id"]))
        if event["message_type"] == "group"
        else "private:" + str(event["user_id"])
    )
    result = _request(
        "qq",
        binding_id,
        conversation,
        None,
        event["user_id"],
        event["message_id"],
        "placeholder",
        event["time"],
        origin,
        targets=targets,
        revision=revision,
    )
    for segment in event["message"]:
        if segment["type"] == "text":
            if segment["data"]["text"]:
                parts.append(dict(kind="text", text=segment["data"]["text"]))
        elif segment["type"] == "at":
            if str(segment["data"]["qq"]) != "all":
                mentions.append(
                    dict(namespace="qq", immutable_account_id=str(segment["data"]["qq"]))
                )
        elif segment["type"] == "reply":
            refs.append(
                dict(
                    channel=result["message_key"]["channel"],
                    message_id=str(segment["data"]["id"]),
                    revision=1,
                )
            )
        else:
            # Asset ingestion is not configured in this text slice. Never turn a
            # private download URL into a trusted asset_ref or silently drop it.
            raise Fault("dependency_unavailable")
    if not parts:
        raise Fault("invalid_input")
    result.update(parts=parts, mentioned_accounts=mentions, reply_refs=refs)
    return result


def normalize_telegram(update, binding_id, origin, *, targets=(), revision=1):
    message = update.get("message", update.get("edited_message"))
    if not message or message.get("from", {}).get("is_bot") or "text" not in message:
        raise Fault("invalid_input")
    if "edited_message" in update and revision <= 1:
        raise Fault("invalid_input")
    result = _request(
        "tg",
        binding_id,
        message["chat"]["id"],
        message.get("message_thread_id"),
        message["from"]["id"],
        message["message_id"],
        message["text"],
        message["date"],
        origin,
        targets=targets,
        revision=revision,
        kind="edit" if "edited_message" in update else "message",
    )
    if "reply_to_message" in message:
        result["reply_refs"].append(
            dict(
                channel=result["message_key"]["channel"],
                message_id=str(message["reply_to_message"]["message_id"]),
                revision=1,
            )
        )
    result["mentioned_accounts"] = [
        dict(namespace="tg", immutable_account_id=str(e["user"]["id"]))
        for e in message.get("entities", [])
        if e["type"] == "text_mention"
    ]
    return result


class Bridge:
    def __init__(
        self,
        store,
        contracts,
        core_client,
        *,
        destinations,
        send_native=None,
        verify_send=None,
        refresh_origin=None,
        confirm_admissions=None,
        clock=time.time,
    ):
        self.store, self.contracts, self.core_client = store, contracts, core_client
        self.destinations, self.send_native = destinations, send_native
        self.verify_send, self.refresh_origin, self.clock = verify_send, refresh_origin, clock
        self.confirm_admissions = confirm_admissions
        self.locks = {}

    def capture(self, request, *, direct_commands=()):
        """Call once from a registered NoneBot matcher before handing off responsibility."""
        fanout = "input" in request
        self.contracts.check(
            "sources#fanout_request" if fanout else "conversation#ingest_request", request
        )
        data = request["input"] if fanout else request
        key = digest([data["message_key"], request["target_actor_ids"]])
        semantic = digest({k: v for k, v in request.items() if k != "command"})
        text = "".join(p["text"] for p in data["parts"] if p["kind"] == "text").strip()
        owner = (
            "direct"
            if any(text == cmd or text.startswith(cmd + " ") for cmd in direct_commands)
            else "companion"
        )
        with self.store.transaction():
            existing = self.store.get("inbox", key)
            if existing:
                if existing["signature"] != semantic:
                    raise Fault("idempotency_conflict")
                return existing["owner"]
            self.store.put(
                "inbox",
                dict(
                    id=key,
                    owner=owner,
                    signature=semantic,
                    request=request,
                    state="direct_owned" if owner == "direct" else "pending",
                    sequence=int(self.clock() * 1000),
                    attempts=0,
                    deadline=self.clock(),
                    receipt=None,
                    fanout=fanout,
                ),
            )
        return owner

    async def flush(self):
        for item in self.store.list("inbox", states=["pending"]):
            if item["deadline"] > self.clock():
                continue
            try:
                request = item["request"]
                if self.refresh_origin:
                    # Issuer must renew from the persisted, authenticated SDK event.
                    request["command"]["origin"] = await self.refresh_origin(item)
                request["command"] = command(
                    request["command"]["origin"],
                    request["command"]["idempotency_key"],
                    self.clock(),
                )
                fanout = item.get("fanout", False)
                data = request["input"] if fanout else request
                if fanout and not self.confirm_admissions:
                    raise Fault("dependency_unavailable")
                result = await self.core_client.call(
                    "/internal/v1/conversation/ingest-actors"
                    if fanout
                    else "/internal/v1/conversation/ingest",
                    request,
                )
                self.contracts.check(
                    "sources#fanout_response" if fanout else "conversation#ingest_response", result
                )
                if result["request_id"] != request["command"]["request_id"]:
                    raise Fault("invalid_input")
                if fanout:
                    self._check_admissions(request, result, item.get("routing_record"))
                    # Persist the first route before confirmation or any further
                    # remote call. A failed mapping retry cannot expand defaults.
                    item["routing_record"] = dict(
                        effective_actor_ids=result["effective_actor_ids"],
                        routing_version=result["routing_version"],
                    )
                    self.store.put("inbox", item)
                    # Trusted Platform prepare/confirm application callback uses
                    # the inline facts, with no recursive Core/Memory source read.
                    if await self.confirm_admissions(request, result) is not True:
                        raise Fault("dependency_unavailable")
                elif result["collection_key"] != dict(
                    channel=data["message_key"]["channel"], author=data["author"]
                ):
                    raise Fault("forbidden")
                channel = data["message_key"]["channel"]
                mapping = self.channel_mapping(channel)
                if mapping is not None and mapping != result["conversation_id"]:
                    raise Fault("scope_changed")
                with self.store.transaction():
                    self.store.put(
                        "conversations",
                        dict(
                            id=digest(channel),
                            channel=channel,
                            conversation_id=result["conversation_id"],
                            core_receipt_id=result["physical_receipt_id"]
                            if fanout
                            else result["receipt_id"],
                        ),
                    )
                item.update(state="accepted", receipt=result)
            except (Fault, OSError) as error:
                item["last_error"] = (
                    error.code if isinstance(error, Fault) else "dependency_unavailable"
                )
                item["deadline"] = self.clock() + min(60, 2 ** min(item["attempts"], 6))
            item["attempts"] += 1
            with self.store.transaction():
                self.store.put("inbox", item)

    def _check_admissions(self, request, result, frozen):
        data = request["input"]
        expected = (
            []
            if data["kind"] == "retract"
            else sorted(
                frozen["effective_actor_ids"]
                if frozen
                else request["target_actor_ids"] or result["effective_actor_ids"]
            )
        )
        if (
            result["request_digest"] != digest(request)
            or result["effective_actor_ids"] != expected
            or result["routing_state"] != ("routed" if expected else "unrouted")
            or (frozen and result["routing_version"] != frozen["routing_version"])
            or sorted(o["actor_id"] for o in result["outcomes"]) != expected
        ):
            raise Fault("forbidden")
        receipts = set()
        for outcome in result["outcomes"]:
            if outcome["state"] == "forbidden":
                continue
            actor, receipt, admission = (
                outcome["actor_id"],
                outcome["receipt"],
                outcome["admission"],
            )
            scope = admission["scope"]
            if (
                receipt["receipt_id"] in receipts
                or receipt["request_id"] != result["request_id"]
                or receipt["deduplicated"] != (outcome["state"] == "duplicate")
                or receipt["collection_key"]
                != dict(channel=data["message_key"]["channel"], author=data["author"])
                or receipt["conversation_id"] != result["conversation_id"]
                or receipt["person_id"] != result["person_id"]
                or admission["selector"]
                != dict(
                    actor_id=actor,
                    key={k: v for k, v in data["message_key"].items() if k != "revision"},
                )
                or scope["actor_id"] != actor
                or scope["person_id"] != result["person_id"]
                or scope["conversation_id"] != result["conversation_id"]
                or admission["physical_receipt_id"] != result["physical_receipt_id"]
                or admission["source"]
                != dict(
                    message_key=data["message_key"],
                    receipt_id=receipt["receipt_id"],
                    archive_state="pending",
                    locator=None,
                )
                or admission["accepted_at"] != receipt["accepted_at"]
            ):
                raise Fault("forbidden")
            receipts.add(receipt["receipt_id"])

    def channel_mapping(self, channel):
        """Issuer reads only mappings learned from authenticated core ingest receipts."""
        mapping = self.store.get("conversations", digest(channel))
        return mapping["conversation_id"] if mapping else None

    async def send(self, service, request):
        self.contracts.check("conversation#send_request", request)
        if request["segment_sequence"] > request["segment_count"] or request["segment_count"] > 16:
            raise Fault("invalid_input")
        if not self.verify_send or not self.send_native:
            raise Fault("dependency_unavailable")
        await self.verify_send(service, request)
        destination = self.destinations.get(request["conversation_id"])
        if destination is None:
            mapped = self.store.list("conversations", request["conversation_id"])
            destination = mapped[0]["channel"] if len(mapped) == 1 else None
        if destination != request["destination"]:
            raise Fault("forbidden")
        semantic = digest({k: v for k, v in request.items() if k != "command"})
        lock = self.locks.setdefault(request["conversation_id"], asyncio.Lock())
        async with lock:
            command_key = digest([service, "send", request["command"]["idempotency_key"]])
            prior_command = self.store.get("commands", command_key)
            if prior_command and prior_command["signature"] != semantic:
                raise Fault("idempotency_conflict")
            previous = self.store.get("replies", request["reply_id"])
            if previous:
                if previous["signature"] != semantic:
                    raise Fault("idempotency_conflict")
                with self.store.transaction():
                    self.store.put(
                        "commands",
                        dict(id=command_key, signature=semantic, reply_id=request["reply_id"]),
                    )
                return {**previous["receipt"], "request_id": request["command"]["request_id"]}
            from tianshu_companion.clients import epoch

            if epoch(request["command"]["deadline_at"]) <= self.clock():
                raise Fault("timeout")
            position = request["turn_sequence"] * 100 + request["segment_sequence"]
            earlier = self.store.list("replies", request["conversation_id"])
            if earlier and position <= max(r["sequence"] for r in earlier):
                raise Fault("version_conflict")
            receipt = dict(
                schema_version=1,
                request_id=request["command"]["request_id"],
                reply_id=request["reply_id"],
                segment_sequence=request["segment_sequence"],
                attempt_id=uid("attempt"),
                state="unknown",
                channel_message_ids=[],
                observed_at=utc(self.clock()),
                retry_safe=False,
            )
            record = dict(
                id=request["reply_id"],
                conversation_id=request["conversation_id"],
                sequence=position,
                state="unknown",
                signature=semantic,
                request=request,
                receipt=receipt,
            )
            with self.store.transaction():
                self.store.put("replies", record)
                self.store.put(
                    "commands",
                    dict(id=command_key, signature=semantic, reply_id=request["reply_id"]),
                )
            try:
                ids = await asyncio.wait_for(
                    self.send_native(destination, request["text"]), timeout=15
                )
                if (
                    not isinstance(ids, list)
                    or not ids
                    or any(not isinstance(i, str) or not i for i in ids)
                ):
                    raise Fault("result_unknown", unknown=True)
                receipt.update(state="sent", channel_message_ids=ids, observed_at=utc(self.clock()))
            except Exception:
                # An exception after the durable attempt is not proof of non-delivery.
                pass
            record.update(state=receipt["state"], receipt=receipt)
            with self.store.transaction():
                self.store.put("replies", record)
            return receipt

    async def reconcile(self, request):
        record = self.store.get("replies", request["reply_id"])
        return record["receipt"] if record and record["state"] != "unknown" else None


async def send_onebot_text(bot, destination, text):
    """Use an authenticated NoneBot OneBot11 Bot; its API result supplies the real ID."""
    target = destination["channel_conversation_id"]
    from nonebot.adapters.onebot.v11 import Message, MessageSegment

    message = Message(MessageSegment.text(text))
    if target.startswith("group:"):
        result = await bot.call_api("send_group_msg", group_id=int(target[6:]), message=message)
    elif target.startswith("private:"):
        result = await bot.call_api("send_private_msg", user_id=int(target[8:]), message=message)
    else:
        raise Fault("forbidden")
    return [_message_id(result["message_id"])]


async def send_telegram_text(bot, destination, text):
    arguments = dict(chat_id=destination["channel_conversation_id"], text=text)
    if destination["thread_id"] is not None:
        arguments["message_thread_id"] = int(destination["thread_id"])
    result = await bot.call_api("send_message", **arguments)
    message_id = result["message_id"] if isinstance(result, dict) else result.message_id
    return [_message_id(message_id)]


def _message_id(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).strip():
        raise Fault("result_unknown", unknown=True)
    return str(value)
