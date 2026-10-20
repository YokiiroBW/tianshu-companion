"""Bounded model capacity with foreground preference and progress for background work."""

import asyncio
from collections import deque


class ModelSlots:
    """Use spare capacity for background work, with dialogue priority under load.

    Work already sent to a model is never preempted or replayed. At a free slot,
    waiting dialogue has preference for up to three grants before a waiting
    background job gets a turn. A one-slot deployment shares that same rule.
    """

    def __init__(self, capacity):
        if type(capacity) is not int or capacity < 1:
            raise ValueError("Model capacity must be positive")
        self._capacity = self._available = capacity
        self.background_limit = max(1, capacity // 2)
        self._foreground = deque()
        self._background = deque()
        self._background_active = 0
        self._foreground_streak = 0
        self.background = _BackgroundSlots(self)

    async def _acquire(self, background):
        future = asyncio.get_running_loop().create_future()
        queue = self._background if background else self._foreground
        queue.append(future)
        self._wake()
        try:
            await future
        except asyncio.CancelledError:
            if future.done() and not future.cancelled():
                # Cancellation can race with a granted slot before its task resumes.
                self._release(background)
            else:
                if future in queue:
                    queue.remove(future)
                self._wake()
            raise
        return True

    def _wake(self):
        while self._available:
            background_ready = (
                self._background
                and self._background_active < self.background_limit
                and (not self._foreground or not self._background_active)
            )
            if self._foreground and (not background_ready or self._foreground_streak < 3):
                queue, background = self._foreground, False
            elif background_ready:
                queue, background = self._background, True
            else:
                return
            future = queue.popleft()
            if future.cancelled():
                continue
            self._available -= 1
            if background:
                self._background_active += 1
                self._foreground_streak = 0
            else:
                self._foreground_streak = (
                    min(3, self._foreground_streak + 1) if background_ready else 0
                )
            future.set_result(True)

    def _release(self, background):
        if self._available >= self._capacity:
            raise ValueError("Model slot released without acquisition")
        if background:
            if not self._background_active:
                raise ValueError("Background slot released without acquisition")
            self._background_active -= 1
        self._available += 1
        self._wake()

    async def acquire(self):
        return await self._acquire(False)

    def release(self):
        self._release(False)

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, *_):
        self.release()


class _BackgroundSlots:
    def __init__(self, owner):
        self.owner = owner

    async def __aenter__(self):
        await self.owner._acquire(True)

    async def __aexit__(self, *_):
        self.owner._release(True)
