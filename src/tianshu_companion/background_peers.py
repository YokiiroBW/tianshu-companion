"""Runtime transport admission for existing life/proactive jobs; no business results."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
import random
import time

_BACKGROUND = ContextVar("companion_background_peer_calls", default=False)


@contextmanager
def background_calls():
    token = _BACKGROUND.set(True)
    try:
        yield
    finally:
        _BACKGROUND.reset(token)


def is_background():
    return _BACKGROUND.get()


class BackgroundPeers:
    def __init__(self, *, clock=time.monotonic, sleep=asyncio.sleep, jitter=None):
        self.clock, self.sleep = clock, sleep
        self.jitter = jitter or (lambda: random.uniform(1.0, 1.2))
        self._states = {}

    async def admit(self, key):
        if not is_background():
            return None
        while state := self._states.get(key):
            delay = state["until"] - self.clock()
            if delay > 0:
                # Waiting is before transport admission, outside its execution budget.
                # It never invents a rejection or consumes the owner's failure attempts.
                timer = asyncio.create_task(self.sleep(delay))
                changed = asyncio.create_task(state["settled"].wait())
                try:
                    await asyncio.wait((timer, changed), return_when=asyncio.FIRST_COMPLETED)
                finally:
                    timer.cancel()
                    changed.cancel()
                    await asyncio.gather(timer, changed, return_exceptions=True)
            elif state["probe"]:
                await state["settled"].wait()
            else:
                if state["failures"] >= 5:
                    state["probe"] = True
                    state["settled"].clear()
                break
        return key

    def finished(self, key, code):
        if key is None:
            return
        previous = self._states.get(key)
        if previous:
            previous["probe"] = False
            previous["settled"].set()
        if code is None:
            self._states.pop(key, None)
            return
        if code == "cancelled":
            return
        failures = previous["failures"] if previous and previous["code"] == code else 0
        state = dict(
            failures=failures + 1, until=0.0, code=code, probe=False, settled=asyncio.Event()
        )
        if state["failures"] >= 5:
            state["until"] = self.clock() + min(
                1800.0, 30.0 * 2 ** min(state["failures"] - 5, 6) * self.jitter()
            )
        self._states[key] = state

    def snapshot(self):
        now = self.clock()
        return [
            dict(
                peer=key[0],
                route=key[1],
                principal=key[2],
                error_code=value["code"],
                consecutive_failures=value["failures"],
                cooldown_seconds=max(0.0, value["until"] - now),
                probing=value["probe"],
            )
            for key, value in sorted(self._states.items())
            if value["failures"] >= 5
        ]
