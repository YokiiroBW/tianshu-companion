"""Optional local relationship consumer; Memory owns every relationship decision."""

import math

from . import outbox, projection
from .client import RelationshipClient
from .contract import CandidateContract


class Relationships:
    def __init__(self, client):
        self.client = client

    async def prepare(self, core, turn, context):
        return await projection.prepare(self, core, turn, context)

    async def verify(self, check, now):
        await projection.verify(self, check, now)

    def queue(self, core, item, turn):
        outbox.queue(self, core, item, turn)

    def recover(self, core):
        outbox.recover(core)

    async def flush(self, core):
        await outbox.flush(self, core)


def assemble(config, memory_service):
    if config is None:
        return None
    allowed = {"enabled", "candidate_schema_path", "max_bytes", "timeout_seconds"}
    if type(config) is not dict or set(config) - allowed or type(config.get("enabled")) is not bool:
        raise ValueError("Invalid relationship consumer configuration")
    maximum, timeout = config.get("max_bytes", 2048), config.get("timeout_seconds", 5)
    if (
        type(maximum) is not int
        or not 256 <= maximum <= 8192
        or type(timeout) not in (int, float)
        or not math.isfinite(timeout)
        or not 0.01 <= timeout <= 10
    ):
        raise ValueError("Invalid relationship consumer budget")
    if not config["enabled"]:
        return None
    return Relationships(
        RelationshipClient(
            memory_service,
            CandidateContract(config["candidate_schema_path"]),
            max_bytes=maximum,
            timeout_seconds=timeout,
        )
    )
