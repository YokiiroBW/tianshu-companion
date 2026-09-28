"""One connection, one explicit session allowlist, and one durable native send owner."""

import asyncio
import contextlib
import time
from uuid import uuid4

from .journal import Conflict, Journal
from .platform import PlatformError, PlatformPort
from .sdk import UnsupportedEvent, send_text, text_event


class BotRuntime:
    def __init__(
        self,
        *,
        connection_id,
        platform_id,
        bot_self_id,
        allowed_conversations,
        journal_path,
        platform_url,
        token,
        get_bot,
        port=None,
        poll_seconds=2,
        group_requires_mention=True,
        ca_file=None,
    ):
        if not connection_id or not platform_id or not bot_self_id or not allowed_conversations:
            raise ValueError("Connection, platform ID, bot self ID and conversations are required")
        self.connection_id = connection_id
        self.platform_id = platform_id
        self.bot_self_id = str(bot_self_id)
        self.allowed = frozenset(allowed_conversations)
        self.group_requires_mention = group_requires_mention
        self.instance_id = uuid4().hex
        self.journal = Journal(journal_path)
        self.journal.bind(connection_id, platform_id, bot_self_id)
        self.port = port or PlatformPort(platform_url, token, ca_file=ca_file)
        self.get_bot = get_bot
        self.poll_seconds = poll_seconds
        self.task = None
        self._events_lock = asyncio.Lock()
        self._last_heartbeat = 0.0

    def eligible(self, bot, event):
        if str(bot.self_id) != self.bot_self_id:
            return False
        if (
            self.group_requires_mention
            and getattr(event, "message_type", None) == "group"
            and not event.is_tome()
        ):
            return False
        try:
            value = text_event(bot, event)
        except (UnsupportedEvent, ValueError, KeyError):
            return False
        return value.conversation_id in self.allowed

    async def capture(self, bot, event):
        if not self.eligible(bot, event):
            return False
        body = text_event(bot, event).platform_body(
            self.connection_id, self.platform_id, self.bot_self_id
        )
        self.journal.capture(body)
        return True

    async def flush_events(self):
        async with self._events_lock:
            for key, body in self.journal.pending_events():
                if not self.journal.start_event(key):
                    continue
                try:
                    response = await self.port.event(body)
                except PlatformError:
                    # The write may have crossed the wire. Only status can resolve it;
                    # never submit this event a second time.
                    self.journal.event_unknown(key)
                    break
                else:
                    self.journal.accepted(key, response)

    async def check_unknown_events(self):
        for key, body in self.journal.unknown_events():
            try:
                status = await self.port.event_status(body)
            except PlatformError:
                return
            if status["found"] and status["state"] in {"accepted", "not_started"}:
                self.journal.resolve_event(key, status)

    async def start(self):
        if self.task is not None:
            return
        self.journal.recover_events()
        self.task = asyncio.create_task(self._loop(), name="tianshu-nonebot-connection")

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task
            self.task = None
        await self.port.close()
        self.journal.close()

    async def _loop(self):
        while True:
            try:
                await self.check_unknown_events()
                await self.flush_acks()
                bot = self.get_bot(self.bot_self_id)
                if bot is not None:
                    now = time.monotonic()
                    if now - self._last_heartbeat >= 15:
                        await self.port.heartbeat(self.connection_id, self.instance_id)
                        self._last_heartbeat = now
                    await self.flush_events()
                    for delivery in await self.port.claim(self.connection_id, self.instance_id):
                        await self.deliver(bot, delivery)
                    await self.flush_acks()
            except (PlatformError, OSError, Conflict):
                pass  # Keep the ledger; the next poll retries only safe reads and ACKs.
            await asyncio.sleep(self.poll_seconds)

    async def deliver(self, bot, delivery):
        first = self.journal.claim(delivery)
        if not first:
            return
        if (
            str(bot.self_id) != self.bot_self_id
            or delivery["namespace"] != "qq"
            or delivery["conversation_id"] not in self.allowed
        ):
            self.journal.settle(delivery["reply_id"], "failed")
            return
        try:
            ids = await send_text(bot, delivery)
        except UnsupportedEvent:
            self.journal.settle(delivery["reply_id"], "failed")
        except Exception:
            # A timeout, cancellation or SDK exception cannot prove no send happened.
            self.journal.settle(delivery["reply_id"], "unknown")
        else:
            self.journal.settle(delivery["reply_id"], "sent", ids)

    async def flush_acks(self):
        for receipt in self.journal.unacked():
            receipt["connection_id"] = self.connection_id
            try:
                await self.port.ack(receipt)
            except PlatformError:
                return
            self.journal.acked(receipt["reply_id"])
