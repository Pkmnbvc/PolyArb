from dataclasses import dataclass
from datetime import datetime, timezone, timedelta


@dataclass(frozen=True)
class WindowState:
    now: datetime
    start: datetime
    end: datetime
    elapsed_seconds: float
    remaining_seconds: float
    progress: float
    phase: str


def parse_utc(timestamp: str) -> datetime:
    """
    Convert an ISO-8601 UTC timestamp from the API into
    a timezone-aware Python datetime.
    """

    return datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )


def get_window_state(
    window_start: str,
    window_end: str,
    now: datetime | None = None,
) -> WindowState:

    if now is None:
        now = datetime.now(timezone.utc)

    start = parse_utc(window_start)
    end = parse_utc(window_end)

    # --------------------------------------------------------------
    # VALIDATE WINDOW
    # --------------------------------------------------------------

    if end <= start:
        raise ValueError(
            "window_end must be later than window_start"
        )

    total_seconds = (
        end - start
    ).total_seconds()

    # --------------------------------------------------------------
    # ELAPSED TIME
    # --------------------------------------------------------------

    elapsed_seconds = max(
        0.0,
        min(
            total_seconds,
            (now - start).total_seconds(),
        ),
    )

    # --------------------------------------------------------------
    # REMAINING TIME
    # --------------------------------------------------------------

    remaining_seconds = max(
        0.0,
        min(
            total_seconds,
            (end - now).total_seconds(),
        ),
    )

    # --------------------------------------------------------------
    # WINDOW PROGRESS
    # --------------------------------------------------------------

    progress = (
        elapsed_seconds / total_seconds
    )

    # --------------------------------------------------------------
    # SETTLEMENT PHASE
    # --------------------------------------------------------------

    settlement_minute_start = (
        end - timedelta(seconds=60)
    )

    if now < start:
        phase = "NOT_STARTED"

    elif now >= end:
        phase = "CLOSED"

    elif now >= settlement_minute_start:
        phase = "FINAL_SETTLEMENT_MINUTE"

    else:
        phase = "TRADING_WINDOW"

    # --------------------------------------------------------------
    # RESULT
    # --------------------------------------------------------------

    return WindowState(
        now=now,
        start=start,
        end=end,
        elapsed_seconds=elapsed_seconds,
        remaining_seconds=remaining_seconds,
        progress=progress,
        phase=phase,
    )