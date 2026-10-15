"""Durable, bounded recovery for peer congestion before a model or delivery starts."""

from .clients import epoch
from .contracts import Fault


def retry_preparation(turn, error, now):
    if (
        not isinstance(error, Fault)
        or error.unknown
        or not (error.retryable_read or error.code == "queue_full")
        or turn["model_calls"]
        or turn.get("expression_id")
    ):
        return False
    attempts = turn.get("preparation_retries", 0)
    started = turn.get("timings", {}).get("started_at") or turn["created_at"]
    deadline = min(turn["source_deadline"], epoch(started) + 30)
    delay = min(2, 0.2 * 2**attempts)
    if attempts >= 5 or now + delay >= deadline:
        return False
    turn.update(
        preparation_retries=attempts + 1,
        retry_at=now + delay,
        preparation_last_error=error.code,
        failure=None,
        phase="preparing",
    )
    if turn.get("bootstrap_mapping"):
        turn["bootstrap_until"] = deadline
    return True
