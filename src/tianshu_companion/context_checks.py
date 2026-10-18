"""Bounded reads and exact-authority sharing within one context verification checkpoint."""

import asyncio

from .context import merge_checks
from .contracts import Fault, digest


def _start(read):
    # Python 3.12: synchronous local checks finish inline; only actual waits
    # enter the loop. Do not change the application's global task factory.
    return asyncio.eager_task_factory(asyncio.get_running_loop(), read)


async def _finish(tasks, *, cancel=False):
    pending = [task for task in tasks if not task.done()]
    if cancel:
        for task in pending:
            task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    for task in tasks:
        if not task.cancelled():
            task.exception()


class _Checkpoint:
    def __init__(self):
        self.reads = {}

    async def once(self, key, read):
        signature = digest(key)
        if signature not in self.reads:
            self.reads[signature] = _start(read())
        return await self.reads[signature]

    async def close(self):
        await _finish(list(self.reads.values()), cancel=True)


async def read_parallel(reads):
    """Independent prompt reads, bounded to four and drained on cancellation."""
    slots = asyncio.Semaphore(4)

    async def run(read):
        async with slots:
            return await read()

    tasks = [_start(run(read)) for read in reads]
    try:
        await _finish(tasks)
        return [task.result() for task in tasks]
    finally:
        await _finish(tasks, cancel=True)


async def verify_checks(checks, verify, *, verified=None):
    # Callers may share exact results only while assembling this one prompt.
    # No authority task or result survives a later preflight/delivery checkpoint.
    verified = {} if verified is None else verified
    pending = []
    for check in merge_checks(checks):
        signature = digest(check)
        if signature in verified:
            if verified[signature] is not None:
                raise verified[signature]
        else:
            pending.append((signature, check))
    if not pending:
        return
    checkpoint = _Checkpoint()
    slots = asyncio.Semaphore(4)

    async def run(signature, check):
        async with slots:
            try:
                await verify(check, checkpoint)
            except Fault as error:
                verified[signature] = error
                raise
            verified[signature] = None

    tasks = [_start(run(*item)) for item in pending]
    try:
        await _finish(tasks)
        for task in tasks:
            task.result()
    finally:
        await _finish(tasks, cancel=True)
        await checkpoint.close()
