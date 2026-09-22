"""Product-local, read-only deployment facts; not a diagnostics/v1 event or probe."""

PATH = "/internal/v1/runtime/capabilities"
OUTBOX_STATES = ("pending", "blocked_scope", "submitting", "unknown")


def candidates_enabled(value=True):
    if type(value) is not bool:
        raise ValueError("automatic_memory_candidates must be boolean")
    return value


def read_capabilities(core):
    """Bounded indexed existence reads, with no content, mutation or remote verification."""
    enabled = core.automatic_memory_candidates
    retained = {
        state: core.store.db.execute(
            "SELECT 1 FROM outbox WHERE status=? LIMIT 1", (state,)
        ).fetchone()
        is not None
        for state in OUTBOX_STATES
    }
    return {
        "schema_version": 1,
        "service": "companion",
        "automatic_memory_candidates": {
            "enabled": enabled,
            "generation": "enabled" if enabled else "disabled",
            "submission": "enabled" if enabled else "paused",
            "backlog_policy": "preserve",
            "retained_outbox": retained,
            "memory_write_verification": "not_verified",
        },
        "chat_audit": {"enabled": False, "state": "not_integrated"},
    }
