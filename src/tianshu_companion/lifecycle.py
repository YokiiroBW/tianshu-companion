"""Stop admission and producers before releasing the existing domain owners."""

import asyncio

from .contracts import Fault


# These are root commands. Source/lease/receipt reads and cancellation remain
# available while an admitted turn or another module finishes its callbacks.
ROOT_COMMANDS = frozenset(
    {
        "ingest",
        "ingest-actors",
        "direct-command",
        "capability",
        "persona",
        "role-runtime",
        "life-generation-retry",
        "life-runtime-manage",
        "life-runtime-conversation",
        "life-runtime-control",
        "skills-manage",
        "image-backend-manage",
        "image-backend-compile",
        "bot-binding-apply",
        "conversation-window",
        "observation-ingest",
    }
)


class RuntimeLifecycle:
    def __init__(self, core, stopping, workers):
        self.core, self.stopping, self.workers = core, stopping, workers
        self.producers = []
        self._requests = set()
        self._drain_task = None

    def admit(self, operation):
        if operation in ROOT_COMMANDS:
            if self.stopping.is_set():
                raise Fault("dependency_unavailable")
            self._requests.add(asyncio.current_task())

    def release(self, operation):
        if operation in ROOT_COMMANDS:
            self._requests.discard(asyncio.current_task())

    async def quiesce(self):
        """Stop new work; leave active passes, clients, stores and callbacks alive."""
        self.workers.stopping = True
        self.stopping.set()

    async def drain(self, timeout=30):
        """Settle admitted work without closing its dependencies; false means cancelled."""
        await self.quiesce()
        if self._drain_task is None:
            self._drain_task = asyncio.create_task(self._drain(timeout))
        return await asyncio.shield(self._drain_task)

    async def _drain(self, timeout):
        async def settle():
            # A pass or already admitted root command can still create a domain
            # task. Finish those producers before asking domain owners to drain.
            await asyncio.gather(*self.producers, *tuple(self._requests), return_exceptions=True)
            if self.core is not None:
                await self.core.drain()

        try:
            async with asyncio.timeout(timeout):
                await settle()
            return True
        except TimeoutError:
            # gather propagates cancellation into actual work and waits for its
            # cleanup. The lifespan's original close remains the final owner.
            if self.core is not None:
                await self.core.stop_work()
            return False
