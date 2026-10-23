from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from polyarb.state_features import (
    StateFeatureEngine,
    StateSnapshot,
)


REPLAY_VERSION = "0.1"


@dataclass
class ReplayStats:
    total_records: int = 0
    malformed_records: int = 0

    coinbase_ticks: int = 0
    btc_impulses: int = 0
    btc_up_impulses: int = 0
    btc_down_impulses: int = 0

    pm_lite_ticks: int = 0
    pm_book_ticks: int = 0

    pm_repricings: int = 0
    pm_up_repricings: int = 0
    pm_down_repricings: int = 0

    one_sided_pm_lite: int = 0

    first_timestamp: float | None = None
    last_timestamp: float | None = None

    @property
    def duration_seconds(self) -> float | None:
        if (
            self.first_timestamp is None
            or self.last_timestamp is None
        ):
            return None

        return (
            self.last_timestamp
            - self.first_timestamp
        )


@dataclass(frozen=True)
class ReplayEvent:
    """
    One causally replayed recorder event.

    pre_state:
        State immediately BEFORE this event, evaluated at the
        event's local receive timestamp.

    post_state:
        State immediately AFTER this event.

    This distinction will matter later:

        state eligibility -> usually PRE event
        trigger information -> current event
        resulting causal state -> POST event
    """

    record_number: int
    sequence: int | None

    timestamp: float
    event_type: str | None

    btc_impulse_created: bool
    pm_repricing_direction: int | None

    pre_state: StateSnapshot | None
    post_state: StateSnapshot | None


@dataclass
class ReplayResult:
    engine: StateFeatureEngine
    stats: ReplayStats


def parse_timestamp(value: str) -> float:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    return datetime.fromisoformat(
        value
    ).timestamp()


def _as_float(value) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value) -> int | None:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _received_timestamp(
    record: dict,
    data: dict,
) -> float:
    """
    The replay clock is the SAME clock used in our causal research:
    local receive time.

    Source timestamps are deliberately ignored for cross-feed ordering.
    """

    raw = (
        record.get("event_received_at")
        or data.get("received_at")
    )

    if not raw:
        raise ValueError(
            "Record has no local receive timestamp"
        )

    return parse_timestamp(raw)


def replay_jsonl(
    path: str | Path,
    *,
    engine: StateFeatureEngine | None = None,
    on_event: Callable[[ReplayEvent], None] | None = None,
    capture_pre_state: bool = False,
    capture_post_state: bool = False,
) -> ReplayResult:
    """
    Replay one PolyArb causal JSONL recording through the production
    StateFeatureEngine.

    IMPORTANT
    ---------
    The JSONL file must already be in recorder causal order.

    We do NOT resort by:
        - exchange source timestamp
        - wall-clock metadata
        - event type

    The recorder's sequence is the historical information path PolyArb
    actually observed.

    State snapshots are optional because constructing them for every
    event is more expensive than simple replay.
    """

    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(path)

    if engine is None:
        engine = StateFeatureEngine()

    stats = ReplayStats()

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:

        for record_number, line in enumerate(
            file,
            start=1,
        ):
            if not line.strip():
                continue

            stats.total_records += 1

            try:
                record = json.loads(line)

                event = record.get(
                    "event",
                    {},
                )

                data = event.get(
                    "data",
                    {},
                )

                event_type = event.get(
                    "type"
                )

                timestamp = _received_timestamp(
                    record,
                    data,
                )

            except (
                json.JSONDecodeError,
                TypeError,
                ValueError,
            ):
                stats.malformed_records += 1
                continue

            if (
                stats.first_timestamp is None
                or timestamp < stats.first_timestamp
            ):
                stats.first_timestamp = timestamp

            if (
                stats.last_timestamp is None
                or timestamp > stats.last_timestamp
            ):
                stats.last_timestamp = timestamp

            pre_state = None

            if capture_pre_state:
                pre_state = engine.snapshot(
                    timestamp
                )

            btc_impulse_created = False
            pm_repricing_direction = None

            # ==================================================
            # Coinbase
            # ==================================================

            if event_type == "coinbase_tick":
                price = _as_float(
                    data.get("price")
                )

                if price is not None:
                    stats.coinbase_ticks += 1

                    btc_impulse_created = (
                        engine.update_coinbase_tick(
                            timestamp,
                            price,
                        )
                    )

                    if btc_impulse_created:
                        stats.btc_impulses += 1

                        post_direction = (
                            engine.snapshot(
                                timestamp
                            ).btc_last_impulse_direction
                        )

                        if post_direction == 1:
                            stats.btc_up_impulses += 1

                        elif post_direction == -1:
                            stats.btc_down_impulses += 1

            # ==================================================
            # Polymarket LITE
            # ==================================================

            elif event_type == "polymarket_lite":
                bid = _as_float(
                    data.get("best_bid")
                )

                ask = _as_float(
                    data.get("best_ask")
                )

                stats.pm_lite_ticks += 1

                if (
                    bid is None
                    or ask is None
                ):
                    stats.one_sided_pm_lite += 1

                pm_repricing_direction = (
                    engine.update_pm_lite(
                        timestamp,
                        bid,
                        ask,
                    )
                )

                if pm_repricing_direction is not None:
                    stats.pm_repricings += 1

                    if pm_repricing_direction == 1:
                        stats.pm_up_repricings += 1

                    elif pm_repricing_direction == -1:
                        stats.pm_down_repricings += 1

            # ==================================================
            # Polymarket BOOK
            # ==================================================

            elif event_type == "polymarket_book":
                bid_levels = _as_int(
                    data.get("bid_levels")
                )

                offer_levels = _as_int(
                    data.get("offer_levels")
                )

                stats.pm_book_ticks += 1

                engine.update_pm_book(
                    timestamp,
                    bid_levels=bid_levels,
                    offer_levels=offer_levels,
                )

            # Unknown recorder event types are deliberately ignored by
            # the state engine rather than guessed at.

            post_state = None

            if capture_post_state:
                post_state = engine.snapshot(
                    timestamp
                )

            if on_event is not None:
                on_event(
                    ReplayEvent(
                        record_number=record_number,
                        sequence=record.get(
                            "sequence"
                        ),
                        timestamp=timestamp,
                        event_type=event_type,
                        btc_impulse_created=(
                            btc_impulse_created
                        ),
                        pm_repricing_direction=(
                            pm_repricing_direction
                        ),
                        pre_state=pre_state,
                        post_state=post_state,
                    )
                )

    return ReplayResult(
        engine=engine,
        stats=stats,
    )