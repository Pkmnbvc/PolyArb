from __future__ import annotations

import argparse
import bisect
import json
import statistics
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


VERSION = "0.1"

# Frozen primary structural bins for this research phase.
# Interpretation:
# [0,25), [25,50), [50,100), [100,250), [250,500] ms
LAG_BINS_MS = [
    (0.0, 25.0),
    (25.0, 50.0),
    (50.0, 100.0),
    (100.0, 250.0),
    (250.0, 500.0),
]

MAX_HORIZON_MS = 500.0


@dataclass(frozen=True)
class SignedEvent:
    timestamp: float
    direction: int       # +1 = UP, -1 = DOWN
    value: Decimal
    sequence: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PolyArb signed continuous-time lag explorer."
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Causal PolyArb JSONL recording.",
    )
    parser.add_argument(
        "--btc-threshold",
        type=Decimal,
        default=Decimal("1.00"),
        help="Minimum absolute tick-to-tick Coinbase move in USD. Default: 1.00",
    )
    return parser.parse_args()


def parse_timestamp(value: str) -> float:
    """
    Convert ISO-8601 timestamp to epoch seconds.

    Cross-feed analysis uses local receive timestamps only.
    """
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    return datetime.fromisoformat(value).timestamp()


def parse_decimal(value) -> Decimal | None:
    if value is None:
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def event_receive_time(record: dict, data: dict) -> float:
    """
    Prefer the recorder's canonical causal event_received_at.
    Fall back to the normalized event data's received_at.
    """
    raw = record.get("event_received_at") or data.get("received_at")

    if not raw:
        raise ValueError("event has no receive timestamp")

    return parse_timestamp(raw)


def midpoint(
    best_bid: Decimal | None,
    best_ask: Decimal | None,
) -> Decimal | None:
    """
    Only construct a directional PM price when both sides exist.

    We intentionally do not invent a midpoint for one-sided books.
    """
    if best_bid is None or best_ask is None:
        return None

    return (best_bid + best_ask) / Decimal("2")


def bin_index(lag_ms: float) -> int | None:
    """
    Primary bins:
      [0,25)
      [25,50)
      [50,100)
      [100,250)
      [250,500]
    """
    if lag_ms < 0.0 or lag_ms > MAX_HORIZON_MS:
        return None

    for index, (low, high) in enumerate(LAG_BINS_MS):
        if index == len(LAG_BINS_MS) - 1:
            if low <= lag_ms <= high:
                return index
        elif low <= lag_ms < high:
            return index

    return None


def bin_label(index: int) -> str:
    low, high = LAG_BINS_MS[index]
    return f"{int(low)}-{int(high)}"


def direction_name(direction: int) -> str:
    return "UP" if direction > 0 else "DOWN"


def load_signed_events(
    path: Path,
    btc_threshold: Decimal,
):
    btc_events: list[SignedEvent] = []
    pm_events: list[SignedEvent] = []

    total_records = 0
    malformed_records = 0

    coinbase_ticks = 0
    polymarket_lite_ticks = 0

    prev_btc_price: Decimal | None = None

    prev_pm_bid: Decimal | None = None
    prev_pm_ask: Decimal | None = None
    prev_pm_mid: Decimal | None = None

    raw_pm_bbo_changes = 0
    spread_only_or_mid_unchanged_changes = 0
    one_sided_pm_lite = 0
    two_sided_baseline_resets = 0

    for raw_line in path.open("r", encoding="utf-8"):
        if not raw_line.strip():
            continue

        total_records += 1

        try:
            record = json.loads(raw_line)
            event = record.get("event", {})
            data = event.get("data", {})
            event_type = event.get("type")
            sequence = record.get("sequence")

            received_at = event_receive_time(record, data)

        except (json.JSONDecodeError, ValueError, TypeError, KeyError):
            malformed_records += 1
            continue

        # --------------------------------------------------------------
        # Coinbase directional impulses
        # --------------------------------------------------------------
        if event_type == "coinbase_tick":
            price = parse_decimal(data.get("price"))

            if price is None:
                continue

            coinbase_ticks += 1

            if prev_btc_price is not None:
                delta = price - prev_btc_price

                if abs(delta) >= btc_threshold:
                    direction = 1 if delta > 0 else -1

                    btc_events.append(
                        SignedEvent(
                            timestamp=received_at,
                            direction=direction,
                            value=price,
                            sequence=sequence,
                        )
                    )

            prev_btc_price = price
            continue

        # --------------------------------------------------------------
        # Polymarket canonical directional repricing
        #
        # Use LITE only so BOOK + LITE do not double-count the same
        # repricing event.
        # --------------------------------------------------------------
        if event_type == "polymarket_lite":
            polymarket_lite_ticks += 1

            bid = parse_decimal(data.get("best_bid"))
            ask = parse_decimal(data.get("best_ask"))

            bbo_changed = (
                prev_pm_bid is not None
                or prev_pm_ask is not None
            ) and (
                bid != prev_pm_bid
                or ask != prev_pm_ask
            )

            if bbo_changed:
                raw_pm_bbo_changes += 1

            current_mid = midpoint(bid, ask)

            # A one-sided book is valid market data, but not enough
            # information to assign a reliable midquote direction.
            if current_mid is None:
                one_sided_pm_lite += 1
                prev_pm_mid = None

                prev_pm_bid = bid
                prev_pm_ask = ask
                continue

            # If the preceding observation was one-sided, establish
            # a new two-sided baseline instead of comparing across
            # the missing-midpoint gap.
            if prev_pm_mid is None:
                if prev_pm_bid is not None or prev_pm_ask is not None:
                    two_sided_baseline_resets += 1

                prev_pm_mid = current_mid
                prev_pm_bid = bid
                prev_pm_ask = ask
                continue

            delta_mid = current_mid - prev_pm_mid

            if delta_mid > 0:
                pm_events.append(
                    SignedEvent(
                        timestamp=received_at,
                        direction=1,
                        value=current_mid,
                        sequence=sequence,
                    )
                )

            elif delta_mid < 0:
                pm_events.append(
                    SignedEvent(
                        timestamp=received_at,
                        direction=-1,
                        value=current_mid,
                        sequence=sequence,
                    )
                )

            elif bbo_changed:
                # Bid/ask changed but their midpoint did not.
                # This is book/spread activity, not directional price movement.
                spread_only_or_mid_unchanged_changes += 1

            prev_pm_mid = current_mid
            prev_pm_bid = bid
            prev_pm_ask = ask

    btc_events.sort(key=lambda e: e.timestamp)
    pm_events.sort(key=lambda e: e.timestamp)

    diagnostics = {
        "total_records": total_records,
        "malformed_records": malformed_records,
        "coinbase_ticks": coinbase_ticks,
        "polymarket_lite_ticks": polymarket_lite_ticks,
        "raw_pm_bbo_changes": raw_pm_bbo_changes,
        "spread_only_or_mid_unchanged_changes":
            spread_only_or_mid_unchanged_changes,
        "one_sided_pm_lite": one_sided_pm_lite,
        "two_sided_baseline_resets": two_sided_baseline_resets,
    }

    return btc_events, pm_events, diagnostics


def analyze_signed_lags(
    btc_events: list[SignedEvent],
    pm_events: list[SignedEvent],
):
    pm_times = [event.timestamp for event in pm_events]

    pair_keys = [
        (1, 1),     # BTC_UP -> PM_UP
        (1, -1),    # BTC_UP -> PM_DOWN
        (-1, -1),   # BTC_DOWN -> PM_DOWN
        (-1, 1),    # BTC_DOWN -> PM_UP
    ]

    # --------------------------------------------------------------
    # Structural analysis:
    # all PM directional events occurring after each BTC trigger
    # inside the 500 ms runway.
    # --------------------------------------------------------------
    all_event_bins = {
        pair: [0] * len(LAG_BINS_MS)
        for pair in pair_keys
    }

    all_event_lags = {
        pair: []
        for pair in pair_keys
    }

    # --------------------------------------------------------------
    # Execution-oriented analysis:
    # only the FIRST PM directional repricing after each BTC trigger.
    # --------------------------------------------------------------
    first_outcomes = {
        1: Counter(),
        -1: Counter(),
    }

    first_event_bins = {
        pair: [0] * len(LAG_BINS_MS)
        for pair in pair_keys
    }

    first_event_lags = {
        pair: []
        for pair in pair_keys
    }

    for btc in btc_events:
        start_index = bisect.bisect_right(pm_times, btc.timestamp)

        # ----------------------------------------------------------
        # First-next-PM-event outcome
        # ----------------------------------------------------------
        if start_index >= len(pm_events):
            first_outcomes[btc.direction]["NO_MOVE"] += 1

        else:
            first_pm = pm_events[start_index]
            first_lag_ms = (
                first_pm.timestamp - btc.timestamp
            ) * 1000.0

            if 0.0 < first_lag_ms <= MAX_HORIZON_MS:
                pair = (btc.direction, first_pm.direction)

                first_outcomes[btc.direction][first_pm.direction] += 1
                first_event_lags[pair].append(first_lag_ms)

                idx = bin_index(first_lag_ms)

                if idx is not None:
                    first_event_bins[pair][idx] += 1

            else:
                first_outcomes[btc.direction]["NO_MOVE"] += 1

        # ----------------------------------------------------------
        # Full structural cross-event distribution
        # ----------------------------------------------------------
        index = start_index

        while index < len(pm_events):
            pm = pm_events[index]

            lag_ms = (
                pm.timestamp - btc.timestamp
            ) * 1000.0

            if lag_ms > MAX_HORIZON_MS:
                break

            if lag_ms > 0.0:
                pair = (btc.direction, pm.direction)
                idx = bin_index(lag_ms)

                if idx is not None:
                    all_event_bins[pair][idx] += 1
                    all_event_lags[pair].append(lag_ms)

            index += 1

    return {
        "all_event_bins": all_event_bins,
        "all_event_lags": all_event_lags,
        "first_outcomes": first_outcomes,
        "first_event_bins": first_event_bins,
        "first_event_lags": first_event_lags,
    }


def print_bin_header():
    labels = [bin_label(i) for i in range(len(LAG_BINS_MS))]

    print(
        f"{'PAIR':<24}"
        + "".join(f"{label + ' ms':>12}" for label in labels)
        + f"{'TOTAL':>10}"
    )


def print_bin_rows(bin_counts: dict):
    pair_order = [
        (1, 1),
        (1, -1),
        (-1, -1),
        (-1, 1),
    ]

    for pair in pair_order:
        btc_dir, pm_dir = pair
        label = (
            f"BTC_{direction_name(btc_dir)}"
            f" -> PM_{direction_name(pm_dir)}"
        )

        counts = bin_counts[pair]
        total = sum(counts)

        print(
            f"{label:<24}"
            + "".join(f"{count:>12}" for count in counts)
            + f"{total:>10}"
        )


def print_latency_summary(lag_data: dict):
    pair_order = [
        (1, 1),
        (1, -1),
        (-1, -1),
        (-1, 1),
    ]

    print(
        f"{'PAIR':<24}"
        f"{'N':>8}"
        f"{'MEDIAN':>14}"
        f"{'MEAN':>14}"
    )

    for pair in pair_order:
        btc_dir, pm_dir = pair

        label = (
            f"BTC_{direction_name(btc_dir)}"
            f" -> PM_{direction_name(pm_dir)}"
        )

        lags = lag_data[pair]

        if lags:
            median = statistics.median(lags)
            mean = statistics.fmean(lags)

            print(
                f"{label:<24}"
                f"{len(lags):>8}"
                f"{median:>11.1f} ms"
                f"{mean:>11.1f} ms"
            )
        else:
            print(
                f"{label:<24}"
                f"{0:>8}"
                f"{'n/a':>14}"
                f"{'n/a':>14}"
            )


def print_first_outcomes(
    btc_events: list[SignedEvent],
    first_outcomes: dict,
):
    totals = Counter(event.direction for event in btc_events)

    print(
        f"{'BTC TRIGGER':<14}"
        f"{'N':>8}"
        f"{'PM UP':>10}"
        f"{'PM DOWN':>10}"
        f"{'NO MOVE':>10}"
        f"{'RESP %':>10}"
        f"{'ALIGN %':>11}"
    )

    for direction in (1, -1):
        outcomes = first_outcomes[direction]

        total = totals[direction]
        up = outcomes[1]
        down = outcomes[-1]
        no_move = outcomes["NO_MOVE"]

        responded = up + down

        response_rate = (
            100.0 * responded / total
            if total else 0.0
        )

        aligned = (
            up if direction == 1
            else down
        )

        alignment_rate = (
            100.0 * aligned / responded
            if responded else 0.0
        )

        print(
            f"{'BTC_' + direction_name(direction):<14}"
            f"{total:>8}"
            f"{up:>10}"
            f"{down:>10}"
            f"{no_move:>10}"
            f"{response_rate:>9.1f}%"
            f"{alignment_rate:>10.1f}%"
        )


def main():
    args = parse_args()

    if not args.file.exists():
        raise SystemExit(
            f"File not found: {args.file}"
        )

    print("=" * 88)
    print(
        f"POLYARB SIGNED CONTINUOUS-TIME LAG EXPLORER v{VERSION}"
    )
    print("Foundation / descriptive phase")
    print("=" * 88)

    print(f"File:              {args.file}")
    print(f"BTC threshold:     ${args.btc_threshold}")
    print(f"Maximum runway:    {MAX_HORIZON_MS:.0f} ms")
    print(
        "Primary bins:      "
        + ", ".join(
            f"{bin_label(i)} ms"
            for i in range(len(LAG_BINS_MS))
        )
    )

    print()
    print("TIMESTAMP POLICY")
    print(
        "Cross-feed lag calculations use LOCAL RECEIVE TIME only."
    )
    print(
        "Coinbase source timestamps and Polymarket book timestamps "
        "are not used for cross-venue latency."
    )

    btc_events, pm_events, diagnostics = load_signed_events(
        args.file,
        args.btc_threshold,
    )

    btc_counts = Counter(
        event.direction
        for event in btc_events
    )

    pm_counts = Counter(
        event.direction
        for event in pm_events
    )

    print()
    print("=" * 88)
    print("EVENT CLASSIFICATION")
    print("=" * 88)

    print(
        f"Ordered JSONL records:              "
        f"{diagnostics['total_records']}"
    )
    print(
        f"Malformed/skipped records:           "
        f"{diagnostics['malformed_records']}"
    )
    print(
        f"Coinbase ticks:                      "
        f"{diagnostics['coinbase_ticks']}"
    )
    print(
        f"Polymarket LITE ticks:               "
        f"{diagnostics['polymarket_lite_ticks']}"
    )

    print()
    print(
        f"BTC_UP impulses:                     "
        f"{btc_counts[1]}"
    )
    print(
        f"BTC_DOWN impulses:                   "
        f"{btc_counts[-1]}"
    )
    print(
        f"Total signed BTC impulses:           "
        f"{len(btc_events)}"
    )

    print()
    print(
        f"PM_UP midpoint repricings:           "
        f"{pm_counts[1]}"
    )
    print(
        f"PM_DOWN midpoint repricings:         "
        f"{pm_counts[-1]}"
    )
    print(
        f"Total directional PM repricings:     "
        f"{len(pm_events)}"
    )

    print()
    print(
        f"Raw LITE BBO changes observed:       "
        f"{diagnostics['raw_pm_bbo_changes']}"
    )
    print(
        f"Spread-only / same-mid changes:      "
        f"{diagnostics['spread_only_or_mid_unchanged_changes']}"
    )
    print(
        f"One-sided PM LITE observations:      "
        f"{diagnostics['one_sided_pm_lite']}"
    )
    print(
        f"Two-sided baseline resets:           "
        f"{diagnostics['two_sided_baseline_resets']}"
    )

    results = analyze_signed_lags(
        btc_events,
        pm_events,
    )

    print()
    print("=" * 88)
    print("A. FULL SIGNED CROSS-EVENT DISTRIBUTION")
    print("All PM directional repricings occurring after each BTC trigger.")
    print("Counts are descriptive; multiple PM events may follow one trigger.")
    print("=" * 88)

    print_bin_header()
    print_bin_rows(
        results["all_event_bins"]
    )

    print()
    print("FULL-DISTRIBUTION LATENCY SUMMARY")
    print_latency_summary(
        results["all_event_lags"]
    )

    print()
    print("=" * 88)
    print("B. FIRST-NEXT-PM-EVENT EXECUTION VIEW")
    print(
        "For each BTC trigger, only the first future PM directional "
        "repricing inside 500 ms is used."
    )
    print("=" * 88)

    print_first_outcomes(
        btc_events,
        results["first_outcomes"],
    )

    print()
    print("FIRST-EVENT SIGNED LATENCY BINS")
    print_bin_header()
    print_bin_rows(
        results["first_event_bins"]
    )

    print()
    print("FIRST-EVENT LATENCY SUMMARY")
    print_latency_summary(
        results["first_event_lags"]
    )

    print()
    print("=" * 88)
    print("IMPORTANT")
    print("=" * 88)
    print(
        "This version is DESCRIPTIVE ONLY."
    )
    print(
        "No timing-null or directional-mark null has been applied yet."
    )
    print(
        "Do not interpret a large aligned count or alignment percentage "
        "as statistical evidence or tradable alpha."
    )
    print(
        "The next phase adds local-activity timestamp nulls and "
        "fixed-timestamp directional mark-shuffle nulls."
    )


if __name__ == "__main__":
    main()