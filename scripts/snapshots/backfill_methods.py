from __future__ import annotations


class BackfillNotEnabledError(RuntimeError):
    """Raised when a simulated backfill method is called during phase 1."""


def controlled_backfill_linear(*args, **kwargs):
    raise BackfillNotEnabledError(
        "Simulated backfill is intentionally disabled in phase 1. "
        "Use only actual_monthly_aggregate, cumulative_from_event_history, or actual_pipeline_audit."
    )


def controlled_backfill_stepwise(*args, **kwargs):
    raise BackfillNotEnabledError(
        "Simulated backfill is intentionally disabled in phase 1. "
        "Use only actual_monthly_aggregate, cumulative_from_event_history, or actual_pipeline_audit."
    )
