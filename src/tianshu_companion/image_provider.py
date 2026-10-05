"""Provider-independent job transport result. Compilation happens before admission."""

from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class ImageResult:
    state: str
    artifacts: list[dict] = field(default_factory=list)
    error_code: str | None = None
    handle: object = None


class ImageProvider(Protocol):
    identity: str

    async def submit(self, submission_id: str, plan: dict) -> ImageResult: ...
    async def poll(self, handle: object, plan: dict) -> ImageResult: ...
    async def cancel_pending(self, handle: object, plan: dict) -> ImageResult: ...
    async def image(self, descriptor: dict, maximum: int) -> tuple[bytes, int, int]: ...
    async def upload(self, data: bytes, name: str) -> str: ...


class ImageRejected(ValueError):
    """Provider has explicit proof that it rejected the submission before execution."""
