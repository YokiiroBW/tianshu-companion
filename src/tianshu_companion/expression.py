"""Natural text boundaries for an incrementally produced expression."""

import re


class Segments:
    def __init__(self):
        self.pending = ""

    def feed(self, text):
        self.pending += text
        results = []
        while True:
            boundary = re.search(r"[。！？!?](?:[”’\"']?)(?:\s|$)|\n", self.pending)
            if boundary and (boundary.end() >= 40 or len(self.pending) >= 200):
                end = boundary.end()
            elif len(self.pending) >= 400:
                end = max(self.pending.rfind("，", 0, 400), self.pending.rfind(" ", 0, 400)) + 1
                end = end if end > 100 else 400
            else:
                break
            value, self.pending = self.pending[:end], self.pending[end:]
            if value.strip():
                results.append(value)
        return results

    def finish(self):
        value, self.pending = self.pending, ""
        return [value] if value.strip() else []
