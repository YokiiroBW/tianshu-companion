"""Local lifecycle facts; health reads this projection without running any work."""

import time


class WorkerHealth:
    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.workers = {}
        self.stopping = False

    def register(self, name, interval, task):
        self.workers[name] = dict(
            task=task,
            last_success=None,
            failed=False,
            # The slowest built-in pass has a 30s transport budget. Allow scheduling
            # and its normal sleep, but never accept a worker stalled indefinitely.
            stale_after=max(60.0, interval + 60.0),
        )

    def completed(self, name, *, failed):
        item = self.workers[name]
        item["failed"] = failed
        if not failed:
            item["last_success"] = self.clock()

    def healthy(self):
        now = self.clock()
        return (
            bool(self.workers)
            and not self.stopping
            and all(
                not item["task"].done()
                and not item["failed"]
                and item["last_success"] is not None
                and now - item["last_success"] <= item["stale_after"]
                for item in self.workers.values()
            )
        )
