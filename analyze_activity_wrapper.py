from __future__ import annotations

import argparse
import bisect
import json
import math
import random
import statistics
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


VERSION = "0.1"

HORIZONS_MS = [50, 100, 250, 500]

# Frozen sensitivity set from our research specification.
BLOCK_WIDTHS_MS = [500, 1000, 2000, 5000]

DEFAULT_ITERATIONS = 1000
DEFAULT_SEED = 42


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "PolyArb Local Activity / Volatility Wrapper Test. "
            "Tests whether BTC->PM short-lag timing survives a "
            "local activity-preserving null."
        )
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
        help="Minimum absolute tick-to-tick BTC move. Default: 1.00",
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_ITERATIONS,
        help=f"Null simulations per block width. Default: {DEFAULT_ITERATIONS}",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed. Default: {DEFAULT_SEED}",
    )

    return parser.parse_args()


def parse_timestamp(value: str) -> float:
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
    Cross-feed ordering uses only the causal local receive clock.

    Never use Coinbase source_timestamp or Polymarket book_timestamp
    for cross-venue timing.
    """
    raw = record.get("event_received_at") or data.get("received_at")

    if not raw:
        raise ValueError("Missing local receive timestamp")

    return parse_timestamp(raw)


def load_events(
    path: Path,
    btc_threshold: Decimal,
):
    """
    Reproduce the SAME event definitions used by lead/lag v0.2:

      BTC event:
          tick-to-tick Coinbase price move >= threshold

      PM event:
          canonical Polymarket LITE BBO change

    Direction is deliberately irrelevant in this test.
    """

    btc_times: list[float] = []
    pm_times: list[float] = []

    previous_btc_price: Decimal | None = None

    previous_pm_bid: Decimal | None = None
    previous_pm_ask: Decimal | None = None
    seen_pm_lite = False

    total_records = 0
    malformed = 0
    coinbase_ticks = 0
    pm_lite_ticks = 0

    first_time: float | None = None
    last_time: float | None = None

    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue

            total_records += 1

            try:
                record = json.loads(line)

                event = record.get("event", {})
                data = event.get("data", {})
                event_type = event.get("type")

                received_at = event_receive_time(record, data)

            except (
                json.JSONDecodeError,
                ValueError,
                TypeError,
                KeyError,
            ):
                malformed += 1
                continue

            if first_time is None or received_at < first_time:
                first_time = received_at

            if last_time is None or received_at > last_time:
                last_time = received_at

            # --------------------------------------------------
            # Coinbase impulses
            # --------------------------------------------------
            if event_type == "coinbase_tick":
                price = parse_decimal(data.get("price"))

                if price is None:
                    continue

                coinbase_ticks += 1

                if previous_btc_price is not None:
                    delta = price - previous_btc_price

                    if abs(delta) >= btc_threshold:
                        btc_times.append(received_at)

                previous_btc_price = price
                continue

            # --------------------------------------------------
            # Polymarket canonical BBO changes
            #
            # LITE only, avoiding BOOK + LITE double counting.
            # --------------------------------------------------
            if event_type == "polymarket_lite":
                pm_lite_ticks += 1

                bid = parse_decimal(data.get("best_bid"))
                ask = parse_decimal(data.get("best_ask"))

                if seen_pm_lite:
                    if (
                        bid != previous_pm_bid
                        or ask != previous_pm_ask
                    ):
                        pm_times.append(received_at)

                previous_pm_bid = bid
                previous_pm_ask = ask
                seen_pm_lite = True

    btc_times.sort()
    pm_times.sort()

    diagnostics = {
        "total_records": total_records,
        "malformed": malformed,
        "coinbase_ticks": coinbase_ticks,
        "pm_lite_ticks": pm_lite_ticks,
        "first_time": first_time,
        "last_time": last_time,
    }

    return btc_times, pm_times, diagnostics


def next_event_lags_ms(
    btc_times: list[float],
    pm_times: list[float],
) -> list[float | None]:
    """
    For each BTC impulse, find ONLY the next PM BBO change.

    Returns the forward lag in milliseconds.
    """
    lags: list[float | None] = []

    for btc_time in btc_times:
        index = bisect.bisect_right(
            pm_times,
            btc_time,
        )

        if index >= len(pm_times):
            lags.append(None)
            continue

        lag_ms = (
            pm_times[index] - btc_time
        ) * 1000.0

        lags.append(lag_ms)

    return lags


def summarize_horizon(
    lags: list[float | None],
    horizon_ms: int,
):
    matched = [
        lag
        for lag in lags
        if lag is not None
        and 0.0 < lag <= horizon_ms
    ]

    rate = (
        len(matched) / len(lags)
        if lags
        else 0.0
    )

    median = (
        statistics.median(matched)
        if matched
        else None
    )

    return {
        "matched": len(matched),
        "rate": rate,
        "median": median,
    }


def local_circular_shift_pm(
    pm_times: list[float],
    origin: float,
    block_width_ms: int,
    rng: random.Random,
) -> list[float]:
    """
    Local activity-preserving null.

    PM events never leave their original local block.

    Inside each block, every PM timestamp receives the SAME random
    circular offset. This preserves:

      - exact PM event count in every local block
      - the PM event cluster structure inside that block
        (up to the circular boundary)
      - coarse local activity / volatility regime

    But it destroys:

      - the original precise BTC -> PM millisecond alignment.
    """

    width_sec = block_width_ms / 1000.0

    blocks: dict[int, list[float]] = defaultdict(list)

    for timestamp in pm_times:
        block_index = math.floor(
            (timestamp - origin) / width_sec
        )

        blocks[block_index].append(timestamp)

    shifted: list[float] = []

    for block_index, timestamps in blocks.items():
        block_start = (
            origin
            + block_index * width_sec
        )

        shift = rng.uniform(
            0.0,
            width_sec,
        )

        for timestamp in timestamps:
            relative = timestamp - block_start

            new_relative = (
                relative + shift
            ) % width_sec

            shifted.append(
                block_start + new_relative
            )

    shifted.sort()

    return shifted


def empirical_upper_p(
    real_value: float,
    null_values: list[float],
) -> float:
    """
    One-sided test:
    unusually HIGH real value.
    """
    extreme = sum(
        value >= real_value
        for value in null_values
    )

    return (
        extreme + 1
    ) / (
        len(null_values) + 1
    )


def empirical_lower_p(
    real_value: float,
    null_values: list[float],
) -> float:
    """
    One-sided test:
    unusually LOW real value.
    """
    extreme = sum(
        value <= real_value
        for value in null_values
    )

    return (
        extreme + 1
    ) / (
        len(null_values) + 1
    )


def percentile(
    values: list[float],
    probability: float,
) -> float:
    ordered = sorted(values)

    if not ordered:
        raise ValueError("Cannot percentile empty list")

    rank = math.ceil(
        probability * len(ordered)
    ) - 1

    rank = max(
        0,
        min(rank, len(ordered) - 1),
    )

    return ordered[rank]


def format_p(value: float) -> str:
    if value < 0.001:
        return "<0.001"

    return f"{value:.3f}"


def main():
    args = parse_args()

    if not args.file.exists():
        raise SystemExit(
            f"File not found: {args.file}"
        )

    if args.iterations <= 0:
        raise SystemExit(
            "--iterations must be positive"
        )

    print("=" * 88)
    print(
        f"POLYARB LOCAL ACTIVITY / VOLATILITY WRAPPER TEST v{VERSION}"
    )
    print("=" * 88)

    print(f"File:              {args.file}")
    print(f"BTC threshold:     ${args.btc_threshold}")
    print(
        "Horizons:          "
        + ", ".join(
            f"{value} ms"
            for value in HORIZONS_MS
        )
    )
    print(
        "Block widths:       "
        + ", ".join(
            f"{value} ms"
            for value in BLOCK_WIDTHS_MS
        )
    )
    print(f"Null runs/block:    {args.iterations}")
    print(f"Random seed:        {args.seed}")

    print()
    print("NULL HYPOTHESIS")
    print(
        "BTC and PM only appear close because both are active "
        "inside the same local market regime."
    )
    print()
    print(
        "PM events stay inside their original activity blocks, "
        "but their precise millisecond alignment is circularly shifted."
    )

    btc_times, pm_times, diagnostics = load_events(
        args.file,
        args.btc_threshold,
    )

    if not btc_times:
        raise SystemExit(
            "No qualifying BTC impulses found."
        )

    if not pm_times:
        raise SystemExit(
            "No Polymarket BBO changes found."
        )

    session_start = diagnostics["first_time"]
    session_end = diagnostics["last_time"]

    assert session_start is not None
    assert session_end is not None

    duration = session_end - session_start

    print()
    print("=" * 88)
    print("DATASET")
    print("=" * 88)

    print(
        f"Session duration:       "
        f"{duration:.3f} sec"
    )
    print(
        f"Ordered records:        "
        f"{diagnostics['total_records']}"
    )
    print(
        f"Malformed/skipped:      "
        f"{diagnostics['malformed']}"
    )
    print(
        f"Coinbase ticks:         "
        f"{diagnostics['coinbase_ticks']}"
    )
    print(
        f"Polymarket LITE ticks:  "
        f"{diagnostics['pm_lite_ticks']}"
    )

    print(
        f"BTC impulses:           "
        f"{len(btc_times)}"
    )
    print(
        f"PM BBO changes:         "
        f"{len(pm_times)}"
    )

    real_lags = next_event_lags_ms(
        btc_times,
        pm_times,
    )

    real_results = {
        horizon: summarize_horizon(
            real_lags,
            horizon,
        )
        for horizon in HORIZONS_MS
    }

    print()
    print("=" * 88)
    print("REAL TIMELINE")
    print("=" * 88)

    for horizon in HORIZONS_MS:
        result = real_results[horizon]

        median_text = (
            f"{result['median']:.1f} ms"
            if result["median"] is not None
            else "n/a"
        )

        print(
            f"{horizon:>4} ms | "
            f"matched={result['matched']:>3}/{len(btc_times)} | "
            f"response={100 * result['rate']:>5.1f}% | "
            f"median={median_text}"
        )

    print()
    print("Running local-activity null simulations...")

    master_rng = random.Random(
        args.seed
    )

    for block_width_ms in BLOCK_WIDTHS_MS:

        rate_nulls = {
            horizon: []
            for horizon in HORIZONS_MS
        }

        median_nulls = {
            horizon: []
            for horizon in HORIZONS_MS
        }

        for iteration in range(
            args.iterations
        ):
            # Independent deterministic child seed.
            child_seed = master_rng.randrange(
                0,
                2**63,
            )

            rng = random.Random(
                child_seed
            )

            shifted_pm = local_circular_shift_pm(
                pm_times,
                session_start,
                block_width_ms,
                rng,
            )

            shifted_lags = next_event_lags_ms(
                btc_times,
                shifted_pm,
            )

            for horizon in HORIZONS_MS:
                summary = summarize_horizon(
                    shifted_lags,
                    horizon,
                )

                rate_nulls[horizon].append(
                    summary["rate"]
                )

                if summary["median"] is not None:
                    median_nulls[horizon].append(
                        summary["median"]
                    )

        print()
        print("-" * 88)
        print(
            f"LOCAL BLOCK WIDTH: {block_width_ms} ms"
        )
        print("-" * 88)

        if block_width_ms <= max(HORIZONS_MS):
            print(
                "CAUTION: horizons approaching the block width "
                "are not strong micro-alignment tests."
            )

        print(
            f"{'HORIZON':>8} "
            f"{'REAL RATE':>11} "
            f"{'NULL MEAN':>11} "
            f"{'NULL 99%':>11} "
            f"{'RATE p':>9} "
            f"{'REAL MED':>11} "
            f"{'NULL MED':>11} "
            f"{'LAT p':>9} "
            f"{'RESULT':>10}"
        )

        for horizon in HORIZONS_MS:
            real = real_results[horizon]

            null_rates = rate_nulls[horizon]
            null_medians = median_nulls[horizon]

            null_rate_mean = statistics.fmean(
                null_rates
            )

            null_rate_99 = percentile(
                null_rates,
                0.99,
            )

            rate_p = empirical_upper_p(
                real["rate"],
                null_rates,
            )

            if (
                real["median"] is not None
                and null_medians
            ):
                null_median_mean = (
                    statistics.fmean(
                        null_medians
                    )
                )

                latency_p = empirical_lower_p(
                    real["median"],
                    null_medians,
                )

                real_med_text = (
                    f"{real['median']:.1f}"
                )

                null_med_text = (
                    f"{null_median_mean:.1f}"
                )

                latency_p_text = (
                    format_p(latency_p)
                )

            else:
                real_med_text = "n/a"
                null_med_text = "n/a"
                latency_p_text = "n/a"

            breach = (
                real["rate"]
                > null_rate_99
            )

            result_text = (
                "BREACH"
                if breach
                else "inside"
            )

            print(
                f"{horizon:>6}ms "
                f"{100 * real['rate']:>10.1f}% "
                f"{100 * null_rate_mean:>10.1f}% "
                f"{100 * null_rate_99:>10.1f}% "
                f"{format_p(rate_p):>9} "
                f"{real_med_text:>9}ms "
                f"{null_med_text:>9}ms "
                f"{latency_p_text:>9} "
                f"{result_text:>10}"
            )

    print()
    print("=" * 88)
    print("INTERPRETATION")
    print("=" * 88)

    print(
        "BREACH means the real response rate exceeded the "
        "99th percentile of this local-activity null."
    )

    print(
        "That supports excess micro-timing structure beyond "
        "coarse local co-activity under THIS null."
    )

    print()
    print(
        "It does NOT establish causation, directionality, "
        "tradability, or profitability."
    )

    print()
    print(
        "Primary evidence should be stable across multiple "
        "reasonable block widths and independent market windows."
    )

    print(
        "The 500 ms block / 500 ms horizon combination should "
        "not be treated as primary evidence."
    )


if __name__ == "__main__":
    main()