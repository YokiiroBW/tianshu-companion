"""Durable, bounded preparation recovery before a model or delivery starts."""

from .contracts import Fault


def recovery_deadline(started_at, *, source_deadline=None):
    """Recovery horizon, optionally bounded by a source lease before another read.

    A running preparation can renew its source with fresh proof; callers that
    own that work keep the recovery horizon rather than pinning its old lease.
    """
    deadline = started_at + 30
    return min(deadline, source_deadline) if source_deadline is not None else deadline


def read_retry(state, error, now, source_deadline, *, selection_changed=False):
    """One bounded policy for a logical preparation or delivery authority read.

    Transport performs no semantic replay. The durable preparation owner and
    the live delivery checkpoint use this policy at mutually exclusive model
    boundaries; neither can replay a model, tool command or send.
    """
    if (
        not isinstance(error, Fault)
        or error.unknown
        or not (
            error.retryable_read
            or error.code == "queue_full"
            and not error.cause
            or selection_changed
            and error.code == "scope_changed"
        )
        or (error.cause or {}).get("operation") in {"command", "stream"}
    ):
        return None
    attempts = state.get("attempts", 0)
    started = state.get("started_at", now)
    deadline = recovery_deadline(started, source_deadline=source_deadline)
    delay = max(min(2, 0.2 * 2**attempts), getattr(error, "retry_after_seconds", None) or 0)
    if attempts >= 5 or now + delay >= deadline:
        return None
    state.update(started_at=started, attempts=attempts + 1, deadline=deadline)
    return delay


def retry_preparation(turn, error, now):
    if turn["model_calls"] or turn.get("expression_id") or turn.get("native_execution"):
        return False
    selection_changed = getattr(error, "memory_selection_invalid", False)
    state = dict(attempts=turn.get("preparation_retries", 0))
    if "preparation_retry_started_at" in turn:
        state["started_at"] = turn["preparation_retry_started_at"]
    delay = read_retry(
        state, error, now, turn["source_deadline"], selection_changed=selection_changed
    )
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
    if selection_changed:
        # A retry of the old proof can never validate a changed candidate domain.
        # Preserve input authority and explicit dependencies; replace all read
        # artifacts together through the normal query/prepare/preflight path.
        turn.update(
            preparation=None,
            memory_context=None,
            history_snapshot=None,
            input_history_snapshot=None,
            short_context=None,
            context_checks=[],
            profile_checks=[],
            context_budget_used=0,
        )
        turn.get("timings", {}).pop("context_prepared_at", None)
    return True


async def verify_prepared_memory(memory, turn, checks):
    """Identify an invalid current selection without confusing optional old checks."""
    args = turn["origin"], turn["scope"], turn["memory_context"]["bundle"]
    try:
        return await memory.verify_context(*args, checks)
    except Fault as error:
        if (
            error.code != "scope_changed"
            or error.unknown
            or turn["model_calls"]
            or turn.get("expression_id")
            or turn.get("native_execution")
        ):
            raise
        if checks:
            try:
                await memory.verify_context(*args, [])
            except Fault as current_error:
                if current_error.code == "scope_changed" and not current_error.unknown:
                    current_error.memory_selection_invalid = True
                raise
            # The current selection is valid; keep the old-material fallback.
            raise
        error.memory_selection_invalid = True
        raise
