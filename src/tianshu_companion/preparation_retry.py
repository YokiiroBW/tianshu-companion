"""Durable, bounded recovery for peer congestion before a model or delivery starts."""

from .contracts import Fault


def read_retry(state, error, now, source_deadline):
    """One bounded policy for a logical preparation or delivery authority read.

    Transport performs no semantic replay. The durable preparation owner and
    the live delivery checkpoint use this policy at mutually exclusive model
    boundaries; neither can replay a model, tool command or send.
    """
    if (
        not isinstance(error, Fault)
        or error.unknown
        or not (error.retryable_read or error.code == "queue_full" and not error.cause)
        or (error.cause or {}).get("operation") in {"command", "stream"}
    ):
        return None
    attempts = state.get("attempts", 0)
    started = state.get("started_at", now)
    deadline = min(source_deadline, started + 30)
    delay = max(min(2, 0.2 * 2**attempts), getattr(error, "retry_after_seconds", None) or 0)
    if attempts >= 5 or now + delay >= deadline:
        return None
    state.update(started_at=started, attempts=attempts + 1, deadline=deadline)
    return delay


def retry_preparation(turn, error, now):
    if turn["model_calls"] or turn.get("expression_id"):
        return False
    state = dict(attempts=turn.get("preparation_retries", 0))
    if "preparation_retry_started_at" in turn:
        state["started_at"] = turn["preparation_retry_started_at"]
    delay = read_retry(state, error, now, turn["source_deadline"])
    if delay is None:
        return False
    turn.update(
        preparation_retries=state["attempts"],
        preparation_retry_started_at=state["started_at"],
        retry_at=now + delay,
        preparation_last_error=error.code,
        failure=None,
        phase="preparing",
    )
    if turn.get("bootstrap_mapping"):
        turn["bootstrap_until"] = state["deadline"]
    return True
