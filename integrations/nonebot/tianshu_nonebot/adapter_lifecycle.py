"""Own the adapter's admitted RPCs and SDK event scopes until their final writes."""

import asyncio
from contextlib import asynccontextmanager


class AdapterLifecycle:
    def __init__(self, service):
        self._instance_id = service.instance_id
        self.accepting_events = True
        self.closing = False
        self.closed = False
        self.pending = set()

    @property
    def instance_id(self):
        return self._instance_id

    @asynccontextmanager
    async def operation(self, *, event=False):
        if self.closing or event and not self.accepting_events:
            yield False
            return
        # A NoneBot dependency may enter in a short-lived preprocessor task and
        # exit in handle_event's AsyncExitStack. Track completion, not that task.
        finished = asyncio.get_running_loop().create_future()
        self.pending.add(finished)
        try:
            yield True
        finally:
            self.pending.discard(finished)
            finished.set_result(None)

    async def quiesce(self):
        """Stop SDK event admission; keep existing events, sends and receipts live."""
        self.accepting_events = False

    async def drain(self, timeout=20):
        """Return false on the caller's budget without cancelling work or closing SQL."""
        await self.quiesce()
        try:
            async with asyncio.timeout(timeout):
                while self.pending:
                    # Cancellation of a waiting host must not complete a lease.
                    await asyncio.gather(*(asyncio.shield(item) for item in tuple(self.pending)))
        except TimeoutError:
            return False
        return True
