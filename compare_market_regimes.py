from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


VERSION = "0.1"


@dataclass(frozen=True)
class Tick:
    timestamp: float
    value: Decimal


@dataclass(frozen=True)
class PMQuote:
    timestamp: float
    bid: Decimal | None
    ask: Decimal | None


@dataclass(frozen=True)
class PMBook:
    timestamp: float
    bid_levels: int
    offer_levels: int


@dataclass(frozen=True)
class SignedImpulse:
    timestamp: float
    direction: int
    magnitude: float


@dataclass(frozen=True)
class SignedPMMove:
    timestamp: float
    direction: int
    pre_spread: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare PolyArb market regimes across multiple causal recordings."
        )
    )

    parser.add_argument(
        "files",
        nargs="+",
        type=Path,
        help="Causal JSONL recordings to compare.",
    )

    parser.add_argument(
        "--btc-threshold",
        type=Decimal,
        default=Decimal("1.00"),
        help="Minimum absolute BTC tick move. Default: 1.00",
    )

    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Optional path to save the fingerprint table as CSV.",
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


def receive_time(record: dict, data: dict) -> float:
    raw = record.get("event_received_at") or data.get("received_at")
    if not raw:
        raise ValueError("Missing receive timestamp")
    return parse_timestamp(raw)


def label_from_filename(path: Path) -> str:
    match = re.search(r"-(\d{4})z_", path.name)
    if match:
        hhmm = match.group(1)
        return f"{hhmm[:2]}:{hhmm[2:]}"
    return path.stem[:18]


def percentile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")

    ordered = sorted(values)

    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)

    if lower == upper:
        return ordered[lower]

    weight = position - lower

    return (
        ordered[lower] * (1.0 - weight)
        + ordered[upper] * weight
    )


def weighted_quantile(
    values_and_weights: list[tuple[float, float]],
    q: float,
) -> float:
    usable = [
        (value, weight)
        for value, weight in values_and_weights
        if weight > 0
    ]

    if not usable:
        return float("nan")

    usable.sort(key=lambda item: item[0])

    total_weight = sum(weight for _, weight in usable)
    target = q * total_weight

    cumulative = 0.0

    for value, weight in usable:
        cumulative += weight
        if cumulative >= target:
            return value

    return usable[-1][0]


def safe_mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else float("nan")


def safe_median(values: list[float]) -> float:
    return statistics.median(values) if values else float("nan")


def fraction(condition_values: list[bool]) -> float:
    if not condition_values:
        return float("nan")
    return sum(condition_values) / len(condition_values)


def midpoint(
    bid: Decimal | None,
    ask: Decimal | None,
) -> Decimal | None:
    if bid is None or ask is None:
        return None
    return (bid + ask) / Decimal("2")


def spread(
    bid: Decimal | None,
    ask: Decimal | None,
) -> float | None:
    if bid is None or ask is None:
        return None
    return float(ask - bid)


def build_btc_impulses(
    ticks: list[Tick],
    threshold: Decimal,
) -> list[SignedImpulse]:
    ticks = sorted(ticks, key=lambda item: item.timestamp)

    impulses: list[SignedImpulse] = []

    previous_price: Decimal | None = None

    for tick in ticks:
        if previous_price is not None:
            delta = tick.value - previous_price

            if abs(delta) >= threshold:
                impulses.append(
                    SignedImpulse(
                        timestamp=tick.timestamp,
                        direction=1 if delta > 0 else -1,
                        magnitude=float(abs(delta)),
                    )
                )

        previous_price = tick.value

    return impulses


def build_pm_moves(
    quotes: list[PMQuote],
) -> tuple[list[SignedPMMove], int, int]:
    quotes = sorted(quotes, key=lambda item: item.timestamp)

    moves: list[SignedPMMove] = []

    previous_mid: Decimal | None = None
    previous_spread: float | None = None

    bbo_changes = 0
    one_sided = 0

    previous_bid: Decimal | None = None
    previous_ask: Decimal | None = None
    seen_quote = False

    for quote in quotes:
        current_spread = spread(
            quote.bid,
            quote.ask,
        )

        current_mid = midpoint(
            quote.bid,
            quote.ask,
        )

        if seen_quote and (
            quote.bid != previous_bid
            or quote.ask != previous_ask
        ):
            bbo_changes += 1

        if current_mid is None:
            one_sided += 1
            previous_mid = None
            previous_spread = None

            previous_bid = quote.bid
            previous_ask = quote.ask
            seen_quote = True
            continue

        if previous_mid is None:
            previous_mid = current_mid
            previous_spread = current_spread

            previous_bid = quote.bid
            previous_ask = quote.ask
            seen_quote = True
            continue

        delta_mid = current_mid - previous_mid

        if delta_mid > 0:
            moves.append(
                SignedPMMove(
                    timestamp=quote.timestamp,
                    direction=1,
                    pre_spread=previous_spread,
                )
            )

        elif delta_mid < 0:
            moves.append(
                SignedPMMove(
                    timestamp=quote.timestamp,
                    direction=-1,
                    pre_spread=previous_spread,
                )
            )

        previous_mid = current_mid
        previous_spread = current_spread

        previous_bid = quote.bid
        previous_ask = quote.ask
        seen_quote = True

    return moves, bbo_changes, one_sided


def compute_time_weighted_spread(
    quotes: list[PMQuote],
):
    quotes = sorted(quotes, key=lambda item: item.timestamp)

    weighted: list[tuple[float, float]] = []

    total_valid_time = 0.0
    time_le_1c = 0.0
    time_le_2c = 0.0
    time_ge_3c = 0.0

    for current, following in zip(quotes, quotes[1:]):
        current_spread = spread(
            current.bid,
            current.ask,
        )

        if current_spread is None:
            continue

        duration = following.timestamp - current.timestamp

        if duration <= 0:
            continue

        weighted.append(
            (current_spread, duration)
        )

        total_valid_time += duration

        if current_spread <= 0.0100001:
            time_le_1c += duration

        if current_spread <= 0.0200001:
            time_le_2c += duration

        if current_spread >= 0.0299999:
            time_ge_3c += duration

    if total_valid_time <= 0:
        return {
            "spread_tw_median": float("nan"),
            "spread_tw_p90": float("nan"),
            "spread_time_1c": float("nan"),
            "spread_time_le_2c": float("nan"),
            "spread_time_ge_3c": float("nan"),
        }

    return {
        "spread_tw_median": weighted_quantile(weighted, 0.50),
        "spread_tw_p90": weighted_quantile(weighted, 0.90),
        "spread_time_1c": time_le_1c / total_valid_time,
        "spread_time_le_2c": time_le_2c / total_valid_time,
        "spread_time_ge_3c": time_ge_3c / total_valid_time,
    }


def interarrival_ms(
    timestamps: list[float],
) -> list[float]:
    ordered = sorted(timestamps)

    return [
        (later - earlier) * 1000.0
        for earlier, later in zip(
            ordered,
            ordered[1:],
        )
        if later > earlier
    ]


def run_lengths(
    directions: list[int],
) -> list[int]:
    if not directions:
        return []

    runs = []

    current_direction = directions[0]
    current_length = 1

    for direction in directions[1:]:
        if direction == current_direction:
            current_length += 1
        else:
            runs.append(current_length)
            current_direction = direction
            current_length = 1

    runs.append(current_length)

    return runs


def analyze_file(
    path: Path,
    btc_threshold: Decimal,
) -> dict:
    btc_ticks: list[Tick] = []
    pm_quotes: list[PMQuote] = []
    pm_books: list[PMBook] = []

    total_records = 0
    malformed = 0

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

                timestamp = receive_time(
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

            if first_time is None or timestamp < first_time:
                first_time = timestamp

            if last_time is None or timestamp > last_time:
                last_time = timestamp

            if event_type == "coinbase_tick":
                price = parse_decimal(
                    data.get("price")
                )

                if price is not None:
                    btc_ticks.append(
                        Tick(
                            timestamp=timestamp,
                            value=price,
                        )
                    )

            elif event_type == "polymarket_lite":
                pm_quotes.append(
                    PMQuote(
                        timestamp=timestamp,
                        bid=parse_decimal(
                            data.get("best_bid")
                        ),
                        ask=parse_decimal(
                            data.get("best_ask")
                        ),
                    )
                )

            elif event_type == "polymarket_book":
                bid_levels = data.get(
                    "bid_levels"
                )
                offer_levels = data.get(
                    "offer_levels"
                )

                if (
                    isinstance(bid_levels, int)
                    and isinstance(offer_levels, int)
                ):
                    pm_books.append(
                        PMBook(
                            timestamp=timestamp,
                            bid_levels=bid_levels,
                            offer_levels=offer_levels,
                        )
                    )

    if first_time is None or last_time is None:
        raise ValueError(
            f"No usable events in {path}"
        )

    duration = last_time - first_time

    btc_impulses = build_btc_impulses(
        btc_ticks,
        btc_threshold,
    )

    pm_moves, bbo_changes, one_sided = (
        build_pm_moves(pm_quotes)
    )

    spread_stats = compute_time_weighted_spread(
        pm_quotes
    )

    # ----------------------------------------------------------
    # Coinbase regime features
    # ----------------------------------------------------------
    btc_directions = [
        impulse.direction
        for impulse in btc_impulses
    ]

    btc_magnitudes = [
        impulse.magnitude
        for impulse in btc_impulses
    ]

    btc_intervals = interarrival_ms(
        [
            impulse.timestamp
            for impulse in btc_impulses
        ]
    )

    btc_runs = run_lengths(
        btc_directions
    )

    if len(btc_directions) >= 2:
        same_sign_pairs = [
            left == right
            for left, right in zip(
                btc_directions,
                btc_directions[1:],
            )
        ]

        same_sign_continuation = fraction(
            same_sign_pairs
        )

        alternation_rate = (
            1.0 - same_sign_continuation
        )

    else:
        same_sign_continuation = float("nan")
        alternation_rate = float("nan")

    btc_counts = Counter(
        btc_directions
    )

    btc_signed_imbalance = (
        (
            btc_counts[1]
            - btc_counts[-1]
        )
        / len(btc_impulses)
        if btc_impulses
        else float("nan")
    )

    # ----------------------------------------------------------
    # Polymarket directional activity
    # ----------------------------------------------------------
    pm_directions = [
        move.direction
        for move in pm_moves
    ]

    pm_counts = Counter(
        pm_directions
    )

    pm_signed_imbalance = (
        (
            pm_counts[1]
            - pm_counts[-1]
        )
        / len(pm_moves)
        if pm_moves
        else float("nan")
    )

    pm_intervals = interarrival_ms(
        [
            move.timestamp
            for move in pm_moves
        ]
    )

    pre_move_spreads = [
        move.pre_spread
        for move in pm_moves
        if move.pre_spread is not None
    ]

    # ----------------------------------------------------------
    # Book-level-count proxies
    #
    # IMPORTANT:
    # These are NOT queue sizes and NOT true OBI.
    # ----------------------------------------------------------
    bid_level_counts = [
        float(book.bid_levels)
        for book in pm_books
    ]

    ask_level_counts = [
        float(book.offer_levels)
        for book in pm_books
    ]

    level_imbalance_proxy = []

    for book in pm_books:
        total = (
            book.bid_levels
            + book.offer_levels
        )

        if total > 0:
            level_imbalance_proxy.append(
                (
                    book.bid_levels
                    - book.offer_levels
                )
                / total
            )

    return {
        "label": label_from_filename(path),
        "file": str(path),

        "duration_sec": duration,
        "total_records": total_records,
        "malformed": malformed,

        # Coinbase
        "coinbase_ticks": len(btc_ticks),
        "btc_impulses": len(btc_impulses),
        "btc_impulses_per_sec": (
            len(btc_impulses) / duration
            if duration > 0 else float("nan")
        ),
        "btc_up": btc_counts[1],
        "btc_down": btc_counts[-1],
        "btc_signed_imbalance": btc_signed_imbalance,

        "btc_move_median": safe_median(
            btc_magnitudes
        ),
        "btc_move_p90": percentile(
            btc_magnitudes,
            0.90,
        ),
        "btc_move_p99": percentile(
            btc_magnitudes,
            0.99,
        ),
        "btc_move_max": (
            max(btc_magnitudes)
            if btc_magnitudes
            else float("nan")
        ),

        "btc_interval_median_ms": safe_median(
            btc_intervals
        ),
        "btc_interval_p10_ms": percentile(
            btc_intervals,
            0.10,
        ),
        "btc_frac_lt_50ms": fraction(
            [
                value < 50.0
                for value in btc_intervals
            ]
        ),
        "btc_frac_lt_100ms": fraction(
            [
                value < 100.0
                for value in btc_intervals
            ]
        ),
        "btc_frac_lt_250ms": fraction(
            [
                value < 250.0
                for value in btc_intervals
            ]
        ),

        "btc_same_sign_continuation":
            same_sign_continuation,
        "btc_alternation_rate":
            alternation_rate,
        "btc_run_mean": safe_mean(
            [float(value) for value in btc_runs]
        ),
        "btc_run_max": (
            max(btc_runs)
            if btc_runs
            else float("nan")
        ),

        # Polymarket
        "pm_lite_ticks": len(pm_quotes),
        "pm_book_ticks": len(pm_books),
        "pm_bbo_changes": bbo_changes,
        "pm_signed_moves": len(pm_moves),
        "pm_moves_per_sec": (
            len(pm_moves) / duration
            if duration > 0 else float("nan")
        ),
        "pm_up": pm_counts[1],
        "pm_down": pm_counts[-1],
        "pm_signed_imbalance": pm_signed_imbalance,

        "pm_move_interval_median_ms":
            safe_median(pm_intervals),

        "pm_move_frac_lt_50ms": fraction(
            [
                value < 50.0
                for value in pm_intervals
            ]
        ),
        "pm_move_frac_lt_100ms": fraction(
            [
                value < 100.0
                for value in pm_intervals
            ]
        ),
        "pm_move_frac_lt_250ms": fraction(
            [
                value < 250.0
                for value in pm_intervals
            ]
        ),

        "one_sided_pm_lite": one_sided,

        # Spread
        **spread_stats,

        "pre_move_spread_median":
            safe_median(pre_move_spreads),
        "pre_move_spread_p90":
            percentile(
                pre_move_spreads,
                0.90,
            ),

        # Level-count proxies
        "bid_levels_median":
            safe_median(bid_level_counts),
        "ask_levels_median":
            safe_median(ask_level_counts),

        "bid_levels_p10":
            percentile(
                bid_level_counts,
                0.10,
            ),
        "ask_levels_p10":
            percentile(
                ask_level_counts,
                0.10,
            ),

        "level_imbalance_median":
            safe_median(
                level_imbalance_proxy
            ),
        "level_imbalance_abs_median":
            safe_median(
                [
                    abs(value)
                    for value in level_imbalance_proxy
                ]
            ),
    }


def fmt_number(
    value,
    decimals: int = 2,
):
    if isinstance(value, int):
        return str(value)

    if value is None:
        return "n/a"

    try:
        if math.isnan(float(value)):
            return "n/a"
    except (TypeError, ValueError):
        return str(value)

    return f"{float(value):.{decimals}f}"


def fmt_pct(value):
    try:
        if math.isnan(float(value)):
            return "n/a"
    except (TypeError, ValueError):
        return "n/a"

    return f"{100 * float(value):.1f}%"


def print_table(
    title: str,
    rows: list[tuple[str, str, str]],
    results: list[dict],
):
    """
    rows:
        (display name, result key, format type)
    """
    print()
    print("=" * 104)
    print(title)
    print("=" * 104)

    labels = [
        result["label"]
        for result in results
    ]

    print(
        f"{'FEATURE':<42}"
        + "".join(
            f"{label:>18}"
            for label in labels
        )
    )

    print("-" * 104)

    for display, key, kind in rows:
        values = []

        for result in results:
            value = result[key]

            if kind == "pct":
                text = fmt_pct(value)

            elif kind == "int":
                text = (
                    str(int(value))
                    if not math.isnan(float(value))
                    else "n/a"
                )

            elif kind == "ms":
                text = (
                    f"{value:.1f} ms"
                    if not math.isnan(float(value))
                    else "n/a"
                )

            elif kind == "usd":
                text = (
                    f"${value:.2f}"
                    if not math.isnan(float(value))
                    else "n/a"
                )

            elif kind == "prob":
                text = (
                    f"{value:.4f}"
                    if not math.isnan(float(value))
                    else "n/a"
                )

            else:
                text = fmt_number(
                    value,
                    3,
                )

            values.append(text)

        print(
            f"{display:<42}"
            + "".join(
                f"{value:>18}"
                for value in values
            )
        )


def main():
    args = parse_args()

    if len(args.files) < 2:
        raise SystemExit(
            "Provide at least two recordings to compare."
        )

    for path in args.files:
        if not path.exists():
            raise SystemExit(
                f"File not found: {path}"
            )

    print("=" * 104)
    print(
        f"POLYARB MARKET REGIME FINGERPRINT v{VERSION}"
    )
    print("=" * 104)

    print(
        f"BTC impulse threshold: ${args.btc_threshold}"
    )

    print()
    print("TIMING POLICY")
    print(
        "All cross-feed/event sequencing uses local received_at time."
    )

    print()
    print("LIQUIDITY WARNING")
    print(
        "Current recordings contain bid_levels / offer_levels counts, "
        "NOT resting quantity."
    )
    print(
        "Level-count asymmetry below is a structural proxy only. "
        "It is NOT true queue depth or true order-book imbalance."
    )

    results = [
        analyze_file(
            path,
            args.btc_threshold,
        )
        for path in args.files
    ]

    # ----------------------------------------------------------
    # Sanity
    # ----------------------------------------------------------
    print_table(
        "A. DATASET / SANITY",
        [
            ("Session duration", "duration_sec", "number"),
            ("Coinbase ticks", "coinbase_ticks", "int"),
            ("BTC impulses >= threshold", "btc_impulses", "int"),
            ("PM LITE ticks", "pm_lite_ticks", "int"),
            ("PM BOOK ticks", "pm_book_ticks", "int"),
            ("PM BBO changes", "pm_bbo_changes", "int"),
            ("Signed PM midpoint moves", "pm_signed_moves", "int"),
        ],
        results,
    )

    # ----------------------------------------------------------
    # BTC fingerprint
    # ----------------------------------------------------------
    print_table(
        "B. COINBASE IMPULSE REGIME",
        [
            ("BTC impulses / second", "btc_impulses_per_sec", "number"),
            ("BTC UP impulses", "btc_up", "int"),
            ("BTC DOWN impulses", "btc_down", "int"),
            ("BTC signed imbalance", "btc_signed_imbalance", "pct"),

            ("Median impulse magnitude", "btc_move_median", "usd"),
            ("90th pct impulse magnitude", "btc_move_p90", "usd"),
            ("99th pct impulse magnitude", "btc_move_p99", "usd"),
            ("Maximum impulse magnitude", "btc_move_max", "usd"),

            ("Median inter-impulse interval", "btc_interval_median_ms", "ms"),
            ("10th pct inter-impulse interval", "btc_interval_p10_ms", "ms"),

            ("Inter-impulse < 50 ms", "btc_frac_lt_50ms", "pct"),
            ("Inter-impulse < 100 ms", "btc_frac_lt_100ms", "pct"),
            ("Inter-impulse < 250 ms", "btc_frac_lt_250ms", "pct"),

            ("Same-sign continuation", "btc_same_sign_continuation", "pct"),
            ("Alternation rate", "btc_alternation_rate", "pct"),
            ("Mean same-sign run length", "btc_run_mean", "number"),
            ("Max same-sign run length", "btc_run_max", "int"),
        ],
        results,
    )

    # ----------------------------------------------------------
    # PM activity fingerprint
    # ----------------------------------------------------------
    print_table(
        "C. POLYMARKET REPRICING REGIME",
        [
            ("Signed midpoint moves / second", "pm_moves_per_sec", "number"),
            ("PM UP moves", "pm_up", "int"),
            ("PM DOWN moves", "pm_down", "int"),
            ("PM signed imbalance", "pm_signed_imbalance", "pct"),

            ("Median inter-repricing interval", "pm_move_interval_median_ms", "ms"),
            ("Inter-repricing < 50 ms", "pm_move_frac_lt_50ms", "pct"),
            ("Inter-repricing < 100 ms", "pm_move_frac_lt_100ms", "pct"),
            ("Inter-repricing < 250 ms", "pm_move_frac_lt_250ms", "pct"),

            ("One-sided PM LITE observations", "one_sided_pm_lite", "int"),
        ],
        results,
    )

    # ----------------------------------------------------------
    # Spread
    # ----------------------------------------------------------
    print_table(
        "D. POLYMARKET SPREAD / MARKET-MAKER STATE",
        [
            ("Time-weighted median spread", "spread_tw_median", "prob"),
            ("Time-weighted 90th pct spread", "spread_tw_p90", "prob"),

            ("Time at <= 1c spread", "spread_time_1c", "pct"),
            ("Time at <= 2c spread", "spread_time_le_2c", "pct"),
            ("Time at >= 3c spread", "spread_time_ge_3c", "pct"),

            ("Median spread before PM move", "pre_move_spread_median", "prob"),
            ("90th pct spread before PM move", "pre_move_spread_p90", "prob"),
        ],
        results,
    )

    # ----------------------------------------------------------
    # Level-count proxies
    # ----------------------------------------------------------
    print_table(
        "E. BOOK STRUCTURE — LEVEL-COUNT PROXIES ONLY",
        [
            ("Median bid level count", "bid_levels_median", "number"),
            ("Median ask level count", "ask_levels_median", "number"),
            ("10th pct bid level count", "bid_levels_p10", "number"),
            ("10th pct ask level count", "ask_levels_p10", "number"),

            (
                "Median signed level-count imbalance",
                "level_imbalance_median",
                "pct",
            ),
            (
                "Median absolute level-count imbalance",
                "level_imbalance_abs_median",
                "pct",
            ),
        ],
        results,
    )

    print()
    print("=" * 104)
    print("RESEARCH STATUS")
    print("=" * 104)
    print(
        "This comparison is EXPLORATORY because the candidate regime "
        "features were chosen after observing that 03:15 behaved differently."
    )
    print(
        "Use the comparison to generate a regime hypothesis, not to claim "
        "independent confirmation."
    )
    print()
    print(
        "Next step after identifying candidate differentiators: "
        "Influence / Outlier Purge on 03:15, followed by genuinely new "
        "out-of-sample markets."
    )

    if args.csv is not None:
        fieldnames = list(results[0].keys())

        with args.csv.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames,
            )

            writer.writeheader()
            writer.writerows(results)

        print()
        print(
            f"Saved CSV: {args.csv}"
        )


if __name__ == "__main__":
    main()