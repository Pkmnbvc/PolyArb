from __future__ import annotations

import argparse
import bisect
import json
import math
import random
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


VERSION = "0.1"

HORIZONS_MS = [50, 100, 250, 500]

# Includes our previously specified local widths plus 10 s as a
# mobility sensitivity check for sparse signed PM repricings.
BLOCK_WIDTHS_MS = [500, 1000, 2000, 5000, 10000]

DEFAULT_ITERATIONS = 5000
DEFAULT_SEED = 42


@dataclass(frozen=True)
class SignedEvent:
    timestamp: float
    direction: int  # +1 UP, -1 DOWN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "PolyArb Directional Sign-Inversion Test. "
            "PM timestamps remain fixed; only local PM UP/DOWN marks "
            "are permuted."
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
        help="Minimum absolute Coinbase tick move. Default: 1.00",
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_ITERATIONS,
        help=f"Null permutations per block width. Default: {DEFAULT_ITERATIONS}",
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
    Cross-feed inference uses only the recorder's local causal clock.
    """
    raw = record.get("event_received_at") or data.get("received_at")

    if not raw:
        raise ValueError("Missing receive timestamp")

    return parse_timestamp(raw)


def midpoint(
    bid: Decimal | None,
    ask: Decimal | None,
) -> Decimal | None:
    if bid is None or ask is None:
        return None

    return (bid + ask) / Decimal("2")


def load_signed_events(
    path: Path,
    btc_threshold: Decimal,
):
    btc_events: list[SignedEvent] = []
    pm_events: list[SignedEvent] = []

    previous_btc_price: Decimal | None = None
    previous_pm_mid: Decimal | None = None

    total_records = 0
    malformed = 0
    coinbase_ticks = 0
    pm_lite_ticks = 0
    one_sided_pm = 0

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

                received_at = event_receive_time(
                    record,
                    data,
                )

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
            # Coinbase signed impulses
            # --------------------------------------------------
            if event_type == "coinbase_tick":
                price = parse_decimal(
                    data.get("price")
                )

                if price is None:
                    continue

                coinbase_ticks += 1

                if previous_btc_price is not None:
                    delta = (
                        price
                        - previous_btc_price
                    )

                    if abs(delta) >= btc_threshold:
                        btc_events.append(
                            SignedEvent(
                                timestamp=received_at,
                                direction=(
                                    1
                                    if delta > 0
                                    else -1
                                ),
                            )
                        )

                previous_btc_price = price
                continue

            # --------------------------------------------------
            # Polymarket signed midpoint repricing
            #
            # LITE only: prevents BOOK + LITE double counting.
            # --------------------------------------------------
            if event_type == "polymarket_lite":
                pm_lite_ticks += 1

                bid = parse_decimal(
                    data.get("best_bid")
                )
                ask = parse_decimal(
                    data.get("best_ask")
                )

                current_mid = midpoint(
                    bid,
                    ask,
                )

                if current_mid is None:
                    one_sided_pm += 1

                    # Do not compare across a one-sided gap.
                    previous_pm_mid = None
                    continue

                if previous_pm_mid is None:
                    previous_pm_mid = current_mid
                    continue

                delta_mid = (
                    current_mid
                    - previous_pm_mid
                )

                if delta_mid > 0:
                    pm_events.append(
                        SignedEvent(
                            timestamp=received_at,
                            direction=1,
                        )
                    )

                elif delta_mid < 0:
                    pm_events.append(
                        SignedEvent(
                            timestamp=received_at,
                            direction=-1,
                        )
                    )

                previous_pm_mid = current_mid

    btc_events.sort(
        key=lambda event: event.timestamp
    )

    pm_events.sort(
        key=lambda event: event.timestamp
    )

    diagnostics = {
        "total_records": total_records,
        "malformed": malformed,
        "coinbase_ticks": coinbase_ticks,
        "pm_lite_ticks": pm_lite_ticks,
        "one_sided_pm": one_sided_pm,
        "first_time": first_time,
        "last_time": last_time,
    }

    return (
        btc_events,
        pm_events,
        diagnostics,
    )


def build_claims(
    btc_events: list[SignedEvent],
    pm_events: list[SignedEvent],
):
    """
    For every BTC trigger, find the first future signed PM repricing.

    The PM timestamp/index is fixed forever after this point.
    Null simulations only alter the PM direction mark.
    """
    pm_times = [
        event.timestamp
        for event in pm_events
    ]

    claims = []

    for btc in btc_events:
        index = bisect.bisect_right(
            pm_times,
            btc.timestamp,
        )

        if index >= len(pm_events):
            claims.append(
                (
                    btc.direction,
                    None,
                    None,
                )
            )
            continue

        lag_ms = (
            pm_events[index].timestamp
            - btc.timestamp
        ) * 1000.0

        claims.append(
            (
                btc.direction,
                index,
                lag_ms,
            )
        )

    return claims


def compute_stats(
    claims,
    pm_directions: list[int],
    horizon_ms: int,
):
    """
    Primary sign-inversion statistic:

        P(PM_UP | BTC_UP)
        -
        P(PM_UP | BTC_DOWN)

    evaluated only for first PM repricings inside the horizon.

    Positive values mean the PM sign changes with the BTC sign
    in the hypothesized direction.
    """

    uu = 0  # BTC_UP -> PM_UP
    ud = 0  # BTC_UP -> PM_DOWN
    du = 0  # BTC_DOWN -> PM_UP
    dd = 0  # BTC_DOWN -> PM_DOWN

    for (
        btc_direction,
        pm_index,
        lag_ms,
    ) in claims:

        if (
            pm_index is None
            or lag_ms is None
            or lag_ms <= 0
            or lag_ms > horizon_ms
        ):
            continue

        pm_direction = (
            pm_directions[pm_index]
        )

        if btc_direction > 0:
            if pm_direction > 0:
                uu += 1
            else:
                ud += 1

        else:
            if pm_direction > 0:
                du += 1
            else:
                dd += 1

    btc_up_n = uu + ud
    btc_down_n = du + dd
    matched = btc_up_n + btc_down_n

    p_pm_up_given_btc_up = (
        uu / btc_up_n
        if btc_up_n
        else float("nan")
    )

    p_pm_up_given_btc_down = (
        du / btc_down_n
        if btc_down_n
        else float("nan")
    )

    inversion_contrast = (
        p_pm_up_given_btc_up
        - p_pm_up_given_btc_down
        if btc_up_n and btc_down_n
        else float("nan")
    )

    up_alignment = (
        uu / btc_up_n
        if btc_up_n
        else float("nan")
    )

    down_alignment = (
        dd / btc_down_n
        if btc_down_n
        else float("nan")
    )

    overall_alignment = (
        (uu + dd) / matched
        if matched
        else float("nan")
    )

    balanced_alignment = (
        (
            up_alignment
            + down_alignment
        ) / 2.0
        if btc_up_n and btc_down_n
        else float("nan")
    )

    return {
        "uu": uu,
        "ud": ud,
        "du": du,
        "dd": dd,
        "matched": matched,
        "btc_up_n": btc_up_n,
        "btc_down_n": btc_down_n,
        "p_up_given_up":
            p_pm_up_given_btc_up,
        "p_up_given_down":
            p_pm_up_given_btc_down,
        "contrast":
            inversion_contrast,
        "up_alignment":
            up_alignment,
        "down_alignment":
            down_alignment,
        "overall_alignment":
            overall_alignment,
        "balanced_alignment":
            balanced_alignment,
    }


def build_blocks(
    pm_events: list[SignedEvent],
    origin: float,
    block_width_ms: int,
):
    width_sec = (
        block_width_ms / 1000.0
    )

    blocks: dict[int, list[int]] = (
        defaultdict(list)
    )

    for index, event in enumerate(pm_events):
        block_index = math.floor(
            (
                event.timestamp
                - origin
            )
            / width_sec
        )

        blocks[block_index].append(
            index
        )

    return list(blocks.values())


def mobility_stats(
    blocks: list[list[int]],
    original_directions: list[int],
):
    """
    Marks can only actually move in a block that contains
    at least two events AND both signs.
    """
    mixed_blocks = 0
    mutable_marks = 0

    for indices in blocks:
        directions = {
            original_directions[index]
            for index in indices
        }

        if (
            len(indices) >= 2
            and len(directions) >= 2
        ):
            mixed_blocks += 1
            mutable_marks += len(indices)

    total_marks = len(
        original_directions
    )

    mobility = (
        mutable_marks / total_marks
        if total_marks
        else 0.0
    )

    return {
        "mixed_blocks": mixed_blocks,
        "mutable_marks": mutable_marks,
        "mobility": mobility,
    }


def shuffled_directions(
    blocks: list[list[int]],
    original_directions: list[int],
    rng: random.Random,
):
    """
    Keep PM event timestamps EXACTLY fixed.

    Within each local block, permute only UP/DOWN marks.

    Therefore we preserve:
      - every PM event time
      - every BTC event time
      - exact local PM event count
      - exact UP/DOWN composition of each block

    We destroy:
      - which particular timestamp received which PM sign
    """
    result = list(
        original_directions
    )

    for indices in blocks:
        if len(indices) < 2:
            continue

        labels = [
            original_directions[index]
            for index in indices
        ]

        rng.shuffle(labels)

        for index, label in zip(
            indices,
            labels,
        ):
            result[index] = label

    return result


def percentile(
    values: list[float],
    probability: float,
):
    ordered = sorted(values)

    rank = (
        math.ceil(
            probability
            * len(ordered)
        )
        - 1
    )

    rank = max(
        0,
        min(
            rank,
            len(ordered) - 1,
        ),
    )

    return ordered[rank]


def empirical_upper_p(
    real_value: float,
    null_values: list[float],
):
    extreme = sum(
        value >= real_value
        for value in null_values
    )

    return (
        extreme + 1
    ) / (
        len(null_values) + 1
    )


def format_p(value: float) -> str:
    if value < 0.001:
        return "<0.001"

    return f"{value:.3f}"


def percent(value: float) -> str:
    if math.isnan(value):
        return "n/a"

    return f"{100 * value:.1f}%"


def main():
    args = parse_args()

    if not args.file.exists():
        raise SystemExit(
            f"File not found: {args.file}"
        )

    print("=" * 94)
    print(
        f"POLYARB DIRECTIONAL SIGN-INVERSION TEST v{VERSION}"
    )
    print("=" * 94)

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
        "Mark blocks:        "
        + ", ".join(
            f"{value} ms"
            for value in BLOCK_WIDTHS_MS
        )
    )

    print(
        f"Null runs/block:    {args.iterations}"
    )

    print(
        f"Random seed:        {args.seed}"
    )

    print()
    print("NULL HYPOTHESIS")
    print(
        "Given the observed PM event times and local directional "
        "composition, Coinbase sign contains no information about "
        "which PM event receives the UP or DOWN mark."
    )

    print()
    print("NULL CONSTRUCTION")
    print(
        "All BTC timestamps remain fixed."
    )
    print(
        "All PM timestamps remain fixed."
    )
    print(
        "Only PM UP/DOWN labels are shuffled inside local time blocks."
    )

    (
        btc_events,
        pm_events,
        diagnostics,
    ) = load_signed_events(
        args.file,
        args.btc_threshold,
    )

    if not btc_events:
        raise SystemExit(
            "No signed BTC impulses found."
        )

    if not pm_events:
        raise SystemExit(
            "No signed PM repricings found."
        )

    btc_counts = Counter(
        event.direction
        for event in btc_events
    )

    pm_counts = Counter(
        event.direction
        for event in pm_events
    )

    session_start = min(
        event.timestamp
        for event in (
            btc_events
            + pm_events
        )
    )

    print()
    print("=" * 94)
    print("DATASET")
    print("=" * 94)

    print(
        f"Ordered records:       "
        f"{diagnostics['total_records']}"
    )

    print(
        f"Coinbase ticks:        "
        f"{diagnostics['coinbase_ticks']}"
    )

    print(
        f"PM LITE ticks:         "
        f"{diagnostics['pm_lite_ticks']}"
    )

    print()
    print(
        f"BTC_UP impulses:       "
        f"{btc_counts[1]}"
    )

    print(
        f"BTC_DOWN impulses:     "
        f"{btc_counts[-1]}"
    )

    print(
        f"Total BTC impulses:    "
        f"{len(btc_events)}"
    )

    print()
    print(
        f"PM_UP repricings:      "
        f"{pm_counts[1]}"
    )

    print(
        f"PM_DOWN repricings:    "
        f"{pm_counts[-1]}"
    )

    print(
        f"Total signed PM moves: "
        f"{len(pm_events)}"
    )

    claims = build_claims(
        btc_events,
        pm_events,
    )

    original_directions = [
        event.direction
        for event in pm_events
    ]

    real_stats = {
        horizon: compute_stats(
            claims,
            original_directions,
            horizon,
        )
        for horizon in HORIZONS_MS
    }

    print()
    print("=" * 94)
    print("REAL TIMELINE — FIRST SIGNED PM MOVE")
    print("=" * 94)

    print(
        f"{'HORIZON':>8}"
        f"{'MATCHED':>10}"
        f"{'UP→UP':>9}"
        f"{'UP→DOWN':>10}"
        f"{'DOWN→DOWN':>12}"
        f"{'DOWN→UP':>10}"
        f"{'ALIGN':>10}"
        f"{'CONTRAST':>11}"
    )

    for horizon in HORIZONS_MS:
        stats = real_stats[horizon]

        print(
            f"{horizon:>6}ms"
            f"{stats['matched']:>10}"
            f"{stats['uu']:>9}"
            f"{stats['ud']:>10}"
            f"{stats['dd']:>12}"
            f"{stats['du']:>10}"
            f"{percent(stats['overall_alignment']):>10}"
            f"{percent(stats['contrast']):>11}"
        )

    print()
    print("SIGN-INVERSION DEFINITION")
    print(
        "contrast = P(PM_UP | BTC_UP, response)"
    )
    print(
        "         - P(PM_UP | BTC_DOWN, response)"
    )
    print()
    print(
        "Positive contrast means changing the BTC sign changes "
        "the future PM sign in the hypothesized direction."
    )

    master_rng = random.Random(
        args.seed
    )

    for block_width_ms in BLOCK_WIDTHS_MS:

        blocks = build_blocks(
            pm_events,
            session_start,
            block_width_ms,
        )

        mobility = mobility_stats(
            blocks,
            original_directions,
        )

        null_contrasts = {
            horizon: []
            for horizon in HORIZONS_MS
        }

        null_balanced_alignment = {
            horizon: []
            for horizon in HORIZONS_MS
        }

        null_overall_alignment = {
            horizon: []
            for horizon in HORIZONS_MS
        }

        for _ in range(args.iterations):
            child_seed = (
                master_rng.randrange(
                    0,
                    2**63,
                )
            )

            rng = random.Random(
                child_seed
            )

            shuffled = shuffled_directions(
                blocks,
                original_directions,
                rng,
            )

            for horizon in HORIZONS_MS:
                stats = compute_stats(
                    claims,
                    shuffled,
                    horizon,
                )

                null_contrasts[
                    horizon
                ].append(
                    stats["contrast"]
                )

                null_balanced_alignment[
                    horizon
                ].append(
                    stats[
                        "balanced_alignment"
                    ]
                )

                null_overall_alignment[
                    horizon
                ].append(
                    stats[
                        "overall_alignment"
                    ]
                )

        print()
        print("-" * 94)
        print(
            f"LOCAL MARK BLOCK: {block_width_ms} ms"
        )
        print("-" * 94)

        print(
            f"Mixed-sign movable blocks: "
            f"{mobility['mixed_blocks']}"
        )

        print(
            f"Movable PM marks:          "
            f"{mobility['mutable_marks']}"
            f"/{len(pm_events)} "
            f"({100 * mobility['mobility']:.1f}%)"
        )

        if mobility["mobility"] < 0.25:
            print(
                "CAUTION: low mark mobility. "
                "This block width produces a nearly frozen null."
            )

        print()
        print(
            f"{'HORIZON':>8}"
            f"{'REAL Δ':>10}"
            f"{'NULL Δ':>10}"
            f"{'NULL99':>10}"
            f"{'p':>9}"
            f"{'REAL BAL':>11}"
            f"{'NULL BAL':>11}"
            f"{'RESULT':>10}"
        )

        for horizon in HORIZONS_MS:
            real = real_stats[horizon]

            contrasts = [
                value
                for value in null_contrasts[
                    horizon
                ]
                if not math.isnan(value)
            ]

            balanced = [
                value
                for value in null_balanced_alignment[
                    horizon
                ]
                if not math.isnan(value)
            ]

            real_contrast = (
                real["contrast"]
            )

            null_contrast_mean = (
                statistics.fmean(
                    contrasts
                )
            )

            null_99 = percentile(
                contrasts,
                0.99,
            )

            p_value = empirical_upper_p(
                real_contrast,
                contrasts,
            )

            null_bal_mean = (
                statistics.fmean(
                    balanced
                )
            )

            breach = (
                real_contrast
                > null_99
            )

            result = (
                "BREACH"
                if breach
                else "inside"
            )

            print(
                f"{horizon:>6}ms"
                f"{percent(real_contrast):>10}"
                f"{percent(null_contrast_mean):>10}"
                f"{percent(null_99):>10}"
                f"{format_p(p_value):>9}"
                f"{percent(real['balanced_alignment']):>11}"
                f"{percent(null_bal_mean):>11}"
                f"{result:>10}"
            )

    print()
    print("=" * 94)
    print("INTERPRETATION")
    print("=" * 94)

    print(
        "A BREACH means the observed BTC-sign / PM-sign inversion "
        "contrast exceeded the 99th percentile of the local "
        "fixed-timestamp mark-shuffle null."
    )

    print()
    print(
        "Because PM timestamps never move, this test does NOT ask "
        "whether Coinbase causes more PM activity."
    )

    print(
        "It asks only whether Coinbase sign contains information "
        "about WHICH WAY the already-observed future PM repricing goes."
    )

    print()
    print(
        "Low-mobility block widths are diagnostic only and should "
        "not be treated as strong evidence."
    )

    print()
    print(
        "A directional result must replicate across independent markets "
        "and later survive timing/jitter ordering perturbations."
    )


if __name__ == "__main__":
    main()