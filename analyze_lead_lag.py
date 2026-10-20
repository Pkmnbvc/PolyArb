import argparse
import bisect
import json
import random
import statistics

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path


DATA_DIR = Path("data") / "recordings"

DEFAULT_HORIZONS_MS = [
    50,
    100,
    250,
    500,
    1000,
    2000,
]

DEFAULT_ITERATIONS = 5000
DEFAULT_SEED = 42


# ---------------------------------------------------------------------
# DATA OBJECTS
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class CoinbaseMove:
    timestamp: datetime
    price: Decimal
    delta: Decimal
    remaining_seconds: float | None
    distance_to_strike: Decimal | None


@dataclass(frozen=True)
class PolymarketMove:
    timestamp: datetime

    old_bid: Decimal | None
    old_ask: Decimal | None

    new_bid: Decimal | None
    new_ask: Decimal | None

    old_mid: Decimal | None
    new_mid: Decimal | None

    remaining_seconds: float | None


@dataclass(frozen=True)
class LeadLagResult:
    total: int
    matched: int
    latencies_ms: list[float]
    direction_total: int
    direction_agree: int

    @property
    def match_rate(self):
        if self.total == 0:
            return None

        return self.matched / self.total

    @property
    def median_latency_ms(self):
        if not self.latencies_ms:
            return None

        return statistics.median(
            self.latencies_ms
        )

    @property
    def direction_rate(self):
        if self.direction_total == 0:
            return None

        return (
            self.direction_agree
            / self.direction_total
        )


# ---------------------------------------------------------------------
# BASIC HELPERS
# ---------------------------------------------------------------------

def parse_timestamp(value):
    return datetime.fromisoformat(
        value.replace(
            "Z",
            "+00:00",
        )
    )


def decimal_or_none(value):
    if value is None:
        return None

    return Decimal(
        str(value)
    )


def quote_mid(
    bid,
    ask,
):
    if (
        bid is None
        or ask is None
    ):
        return None

    return (
        bid + ask
    ) / Decimal("2")


def sign(value):
    if value > 0:
        return 1

    if value < 0:
        return -1

    return 0


def percentage(value):
    if value is None:
        return "n/a"

    return f"{value * 100:.1f}%"


def milliseconds(value):
    if value is None:
        return "n/a"

    return f"{value:.1f} ms"


def mean_or_none(values):
    if not values:
        return None

    return statistics.mean(
        values
    )


def median_or_none(values):
    if not values:
        return None

    return statistics.median(
        values
    )


def empirical_p_value(
    observed,
    null_values,
    direction,
):
    """
    Empirical one-sided p-value.

    direction="high":
        How often is the null >= observed?

    direction="low":
        How often is the null <= observed?
    """

    if (
        observed is None
        or not null_values
    ):
        return None

    if direction == "high":
        extreme = sum(
            1
            for value in null_values
            if value >= observed
        )

    elif direction == "low":
        extreme = sum(
            1
            for value in null_values
            if value <= observed
        )

    else:
        raise ValueError(
            "direction must be 'high' or 'low'"
        )

    return (
        extreme + 1
    ) / (
        len(null_values) + 1
    )


def format_p(value):
    if value is None:
        return "n/a"

    if value < 0.001:
        return "<0.001"

    return f"{value:.3f}"


# ---------------------------------------------------------------------
# COMMAND-LINE HORIZONS
# ---------------------------------------------------------------------

def parse_horizons(value):
    try:
        horizons = [
            float(item.strip())
            for item in value.split(",")
            if item.strip()
        ]

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Horizons must be comma-separated numbers."
        ) from exc

    if not horizons:
        raise argparse.ArgumentTypeError(
            "At least one horizon is required."
        )

    if any(
        horizon <= 0
        for horizon in horizons
    ):
        raise argparse.ArgumentTypeError(
            "Horizons must be positive."
        )

    return sorted(
        set(horizons)
    )


# ---------------------------------------------------------------------
# FILE DISCOVERY
# ---------------------------------------------------------------------

def newest_recording():
    files = list(
        DATA_DIR.glob(
            "realtime_causal_btc_15m_*.jsonl"
        )
    )

    if not files:
        return None

    return max(
        files,
        key=lambda path: (
            path.stat().st_mtime
        ),
    )


# ---------------------------------------------------------------------
# LOAD RECORDS
# ---------------------------------------------------------------------

def load_records(path):
    records = []

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:

        for line_number, line in enumerate(
            file,
            start=1,
        ):
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(
                    line
                )

            except json.JSONDecodeError:
                print(
                    f"Skipping malformed JSON "
                    f"at line {line_number}."
                )

                continue

            timestamp_raw = record.get(
                "event_received_at"
            )

            if not timestamp_raw:
                continue

            record["_timestamp"] = (
                parse_timestamp(
                    timestamp_raw
                )
            )

            records.append(
                record
            )

    records.sort(
        key=lambda record: (
            record["_timestamp"]
        )
    )

    return records


# ---------------------------------------------------------------------
# EXTRACT COINBASE IMPULSES
# ---------------------------------------------------------------------

def extract_coinbase_moves(
    records,
    threshold,
):
    moves = []

    previous_price = None

    for record in records:
        event = record.get(
            "event",
            {},
        )

        if (
            event.get("type")
            != "coinbase_tick"
        ):
            continue

        data = event.get(
            "data",
            {},
        )

        price_raw = data.get(
            "price"
        )

        if price_raw is None:
            continue

        price = Decimal(
            str(price_raw)
        )

        if previous_price is None:
            previous_price = price
            continue

        delta = (
            price
            - previous_price
        )

        previous_price = price

        if abs(delta) < threshold:
            continue

        market = record.get(
            "market",
            {},
        )

        strike_raw = market.get(
            "price_to_beat"
        )

        strike = (
            Decimal(
                str(strike_raw)
            )
            if strike_raw is not None
            else None
        )

        distance_to_strike = (
            price - strike
            if strike is not None
            else None
        )

        window = record.get(
            "window",
            {},
        )

        moves.append(
            CoinbaseMove(
                timestamp=record[
                    "_timestamp"
                ],
                price=price,
                delta=delta,
                remaining_seconds=(
                    window.get(
                        "remaining_seconds"
                    )
                ),
                distance_to_strike=(
                    distance_to_strike
                ),
            )
        )

    return moves


# ---------------------------------------------------------------------
# EXTRACT REAL POLYMARKET BBO CHANGES
# ---------------------------------------------------------------------

def extract_polymarket_moves(
    records,
):
    moves = []

    previous_bid = None
    previous_ask = None

    initialized = False

    for record in records:
        event = record.get(
            "event",
            {},
        )

        # LITE is our canonical BBO stream.
        # BOOK often reflects the same repricing and would double-count.
        if (
            event.get("type")
            != "polymarket_lite"
        ):
            continue

        data = event.get(
            "data",
            {},
        )

        bid = decimal_or_none(
            data.get(
                "best_bid"
            )
        )

        ask = decimal_or_none(
            data.get(
                "best_ask"
            )
        )

        if not initialized:
            previous_bid = bid
            previous_ask = ask
            initialized = True
            continue

        if (
            bid == previous_bid
            and ask == previous_ask
        ):
            continue

        old_mid = quote_mid(
            previous_bid,
            previous_ask,
        )

        new_mid = quote_mid(
            bid,
            ask,
        )

        window = record.get(
            "window",
            {},
        )

        moves.append(
            PolymarketMove(
                timestamp=record[
                    "_timestamp"
                ],

                old_bid=previous_bid,
                old_ask=previous_ask,

                new_bid=bid,
                new_ask=ask,

                old_mid=old_mid,
                new_mid=new_mid,

                remaining_seconds=(
                    window.get(
                        "remaining_seconds"
                    )
                ),
            )
        )

        previous_bid = bid
        previous_ask = ask

    return moves


# ---------------------------------------------------------------------
# BTC -> NEXT POLYMARKET BBO CHANGE
# ---------------------------------------------------------------------

def analyze_btc_to_polymarket(
    btc_moves,
    pm_moves,
    horizon_ms,
):
    if not btc_moves:
        return LeadLagResult(
            total=0,
            matched=0,
            latencies_ms=[],
            direction_total=0,
            direction_agree=0,
        )

    pm_times = [
        move.timestamp
        for move in pm_moves
    ]

    latencies = []

    matched = 0
    direction_total = 0
    direction_agree = 0

    for btc in btc_moves:
        index = bisect.bisect_right(
            pm_times,
            btc.timestamp,
        )

        if index >= len(
            pm_moves
        ):
            continue

        pm = pm_moves[
            index
        ]

        latency_ms = (
            pm.timestamp
            - btc.timestamp
        ).total_seconds() * 1000

        if latency_ms > horizon_ms:
            continue

        matched += 1

        latencies.append(
            latency_ms
        )

        if (
            pm.old_mid is not None
            and pm.new_mid is not None
        ):
            pm_delta = (
                pm.new_mid
                - pm.old_mid
            )

            if pm_delta != 0:
                direction_total += 1

                if (
                    sign(btc.delta)
                    == sign(pm_delta)
                ):
                    direction_agree += 1

    return LeadLagResult(
        total=len(
            btc_moves
        ),
        matched=matched,
        latencies_ms=latencies,
        direction_total=direction_total,
        direction_agree=direction_agree,
    )


# ---------------------------------------------------------------------
# POLYMARKET BBO CHANGE -> MOST RECENT BTC MOVE
# ---------------------------------------------------------------------

def analyze_polymarket_after_btc(
    btc_moves,
    pm_moves,
    horizon_ms,
):
    if not pm_moves:
        return LeadLagResult(
            total=0,
            matched=0,
            latencies_ms=[],
            direction_total=0,
            direction_agree=0,
        )

    btc_times = [
        move.timestamp
        for move in btc_moves
    ]

    latencies = []

    matched = 0
    direction_total = 0
    direction_agree = 0

    for pm in pm_moves:
        index = (
            bisect.bisect_left(
                btc_times,
                pm.timestamp,
            )
            - 1
        )

        if index < 0:
            continue

        btc = btc_moves[
            index
        ]

        latency_ms = (
            pm.timestamp
            - btc.timestamp
        ).total_seconds() * 1000

        if latency_ms > horizon_ms:
            continue

        matched += 1

        latencies.append(
            latency_ms
        )

        if (
            pm.old_mid is not None
            and pm.new_mid is not None
        ):
            pm_delta = (
                pm.new_mid
                - pm.old_mid
            )

            if pm_delta != 0:
                direction_total += 1

                if (
                    sign(btc.delta)
                    == sign(pm_delta)
                ):
                    direction_agree += 1

    return LeadLagResult(
        total=len(
            pm_moves
        ),
        matched=matched,
        latencies_ms=latencies,
        direction_total=direction_total,
        direction_agree=direction_agree,
    )


# ---------------------------------------------------------------------
# CIRCULAR TIME-SHIFT NULL MODEL
# ---------------------------------------------------------------------

def circular_shift_btc_moves(
    btc_moves,
    session_start,
    session_end,
    offset_seconds,
):
    """
    Shift the complete Coinbase impulse sequence through time while
    preserving:

    - number of BTC impulses
    - spacing between impulses
    - move magnitudes
    - move directions
    - clustering

    The timeline wraps around at the session boundary.

    This destroys the original Coinbase/Polymarket alignment while
    preserving much of the internal Coinbase event structure.
    """

    duration_seconds = (
        session_end
        - session_start
    ).total_seconds()

    shifted = []

    for move in btc_moves:
        relative_seconds = (
            move.timestamp
            - session_start
        ).total_seconds()

        shifted_seconds = (
            relative_seconds
            + offset_seconds
        ) % duration_seconds

        shifted_timestamp = (
            session_start
            + timedelta(
                seconds=shifted_seconds
            )
        )

        shifted.append(
            CoinbaseMove(
                timestamp=shifted_timestamp,
                price=move.price,
                delta=move.delta,
                remaining_seconds=(
                    move.remaining_seconds
                ),
                distance_to_strike=(
                    move.distance_to_strike
                ),
            )
        )

    shifted.sort(
        key=lambda move: (
            move.timestamp
        )
    )

    return shifted


def choose_null_offset(
    rng,
    duration_seconds,
    max_horizon_ms,
):
    """
    Avoid tiny shifts that would leave the real alignment nearly intact.
    """

    max_horizon_seconds = (
        max_horizon_ms / 1000
    )

    minimum_shift = (
        max_horizon_seconds
        * 1.5
    )

    # For very short recordings, use one quarter of the session instead.
    if (
        minimum_shift * 2
        >= duration_seconds
    ):
        minimum_shift = (
            duration_seconds
            * 0.25
        )

    low = minimum_shift

    high = (
        duration_seconds
        - minimum_shift
    )

    if high <= low:
        return (
            duration_seconds
            / 2
        )

    return rng.uniform(
        low,
        high,
    )


# ---------------------------------------------------------------------
# NULL SIMULATION
# ---------------------------------------------------------------------

def run_null_simulation(
    btc_moves,
    pm_moves,
    session_start,
    session_end,
    horizons_ms,
    iterations,
    seed,
):
    rng = random.Random(
        seed
    )

    duration_seconds = (
        session_end
        - session_start
    ).total_seconds()

    max_horizon_ms = max(
        horizons_ms
    )

    results = {}

    for horizon in horizons_ms:
        results[horizon] = {
            "btc_match_rate": [],
            "btc_median_latency": [],
            "btc_direction_rate": [],

            "pm_match_rate": [],
            "pm_median_latency": [],
            "pm_direction_rate": [],
        }

    for _ in range(
        iterations
    ):
        offset = choose_null_offset(
            rng=rng,
            duration_seconds=duration_seconds,
            max_horizon_ms=max_horizon_ms,
        )

        shifted_btc = (
            circular_shift_btc_moves(
                btc_moves=btc_moves,
                session_start=session_start,
                session_end=session_end,
                offset_seconds=offset,
            )
        )

        for horizon in horizons_ms:
            btc_result = (
                analyze_btc_to_polymarket(
                    btc_moves=shifted_btc,
                    pm_moves=pm_moves,
                    horizon_ms=horizon,
                )
            )

            pm_result = (
                analyze_polymarket_after_btc(
                    btc_moves=shifted_btc,
                    pm_moves=pm_moves,
                    horizon_ms=horizon,
                )
            )

            if (
                btc_result.match_rate
                is not None
            ):
                results[horizon][
                    "btc_match_rate"
                ].append(
                    btc_result.match_rate
                )

            if (
                btc_result.median_latency_ms
                is not None
            ):
                results[horizon][
                    "btc_median_latency"
                ].append(
                    btc_result.median_latency_ms
                )

            if (
                btc_result.direction_rate
                is not None
            ):
                results[horizon][
                    "btc_direction_rate"
                ].append(
                    btc_result.direction_rate
                )

            if (
                pm_result.match_rate
                is not None
            ):
                results[horizon][
                    "pm_match_rate"
                ].append(
                    pm_result.match_rate
                )

            if (
                pm_result.median_latency_ms
                is not None
            ):
                results[horizon][
                    "pm_median_latency"
                ].append(
                    pm_result.median_latency_ms
                )

            if (
                pm_result.direction_rate
                is not None
            ):
                results[horizon][
                    "pm_direction_rate"
                ].append(
                    pm_result.direction_rate
                )

    return results


# ---------------------------------------------------------------------
# DISPLAY
# ---------------------------------------------------------------------

def print_comparison(
    label,
    observed,
    null_values,
    better,
    formatter,
):
    null_mean = mean_or_none(
        null_values
    )

    p_value = empirical_p_value(
        observed=observed,
        null_values=null_values,
        direction=better,
    )

    print(
        f"{label:<24}"
        f"real={formatter(observed):>10}  "
        f"null={formatter(null_mean):>10}  "
        f"p={format_p(p_value):>6}"
    )


def print_horizon_results(
    horizon,
    btc_real,
    pm_real,
    null,
):
    print(
        "-" * 72
    )

    print(
        f"HORIZON: {horizon:.0f} ms"
    )

    print()

    print(
        "BTC MOVE -> NEXT POLYMARKET BBO CHANGE"
    )

    print(
        f"Matched: "
        f"{btc_real.matched}/"
        f"{btc_real.total}"
    )

    print_comparison(
        label="Response rate",
        observed=btc_real.match_rate,
        null_values=null[
            "btc_match_rate"
        ],
        better="high",
        formatter=percentage,
    )

    print_comparison(
        label="Median latency",
        observed=(
            btc_real.median_latency_ms
        ),
        null_values=null[
            "btc_median_latency"
        ],
        better="low",
        formatter=milliseconds,
    )

    print(
        f"Directional samples: "
        f"{btc_real.direction_total}"
    )

    print_comparison(
        label="Direction agreement",
        observed=(
            btc_real.direction_rate
        ),
        null_values=null[
            "btc_direction_rate"
        ],
        better="high",
        formatter=percentage,
    )

    print()

    print(
        "POLYMARKET BBO CHANGE -> RECENT BTC MOVE"
    )

    print(
        f"Matched: "
        f"{pm_real.matched}/"
        f"{pm_real.total}"
    )

    print_comparison(
        label="Preceded rate",
        observed=pm_real.match_rate,
        null_values=null[
            "pm_match_rate"
        ],
        better="high",
        formatter=percentage,
    )

    print_comparison(
        label="Median latency",
        observed=(
            pm_real.median_latency_ms
        ),
        null_values=null[
            "pm_median_latency"
        ],
        better="low",
        formatter=milliseconds,
    )

    print(
        f"Directional samples: "
        f"{pm_real.direction_total}"
    )

    print_comparison(
        label="Direction agreement",
        observed=(
            pm_real.direction_rate
        ),
        null_values=null[
            "pm_direction_rate"
        ],
        better="high",
        formatter=percentage,
    )

    print()


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "PolyArb Coinbase/Polymarket "
            "lead-lag explorer with a circular "
            "time-shift null model."
        )
    )

    parser.add_argument(
        "path",
        nargs="?",
        help=(
            "Causal recorder JSONL path. "
            "Defaults to newest recording."
        ),
    )

    parser.add_argument(
        "--btc-threshold",
        type=Decimal,
        default=Decimal("1.00"),
        help=(
            "Minimum absolute tick-to-tick "
            "Coinbase move in USD. "
            "Default: 1.00"
        ),
    )

    parser.add_argument(
        "--horizons",
        type=parse_horizons,
        default=DEFAULT_HORIZONS_MS,
        help=(
            "Comma-separated horizons in ms. "
            "Default: 50,100,250,500,1000,2000"
        ),
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_ITERATIONS,
        help=(
            "Number of circular-shift null "
            "simulations. Default: 5000"
        ),
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=(
            "Random seed for reproducibility. "
            "Default: 42"
        ),
    )

    args = parser.parse_args()

    if args.iterations <= 0:
        print(
            "Iterations must be positive."
        )
        return

    if args.path:
        path = Path(
            args.path
        )

    else:
        path = newest_recording()

    if (
        path is None
        or not path.exists()
    ):
        print(
            "No causal recording found."
        )
        return

    print(
        "=" * 72
    )

    print(
        "POLYARB LEAD/LAG EXPLORER v0.2"
    )

    print(
        "Circular Time-Shift Null Test"
    )

    print(
        "=" * 72
    )

    print(
        f"File:          {path}"
    )

    print(
        f"BTC threshold: "
        f"${args.btc_threshold}"
    )

    print(
        "Horizons:      "
        + ", ".join(
            f"{value:.0f} ms"
            for value in args.horizons
        )
    )

    print(
        f"Null runs:     "
        f"{args.iterations}"
    )

    print(
        f"Random seed:   "
        f"{args.seed}"
    )

    print()

    records = load_records(
        path
    )

    if len(records) < 2:
        print(
            "Not enough usable events."
        )
        return

    session_start = records[
        0
    ][
        "_timestamp"
    ]

    session_end = records[
        -1
    ][
        "_timestamp"
    ]

    session_duration = (
        session_end
        - session_start
    ).total_seconds()

    if session_duration <= 0:
        print(
            "Invalid session duration."
        )
        return

    btc_moves = (
        extract_coinbase_moves(
            records=records,
            threshold=(
                args.btc_threshold
            ),
        )
    )

    pm_moves = (
        extract_polymarket_moves(
            records
        )
    )

    coinbase_events = sum(
        1
        for record in records
        if record.get(
            "event",
            {},
        ).get(
            "type"
        )
        == "coinbase_tick"
    )

    polymarket_lite_events = sum(
        1
        for record in records
        if record.get(
            "event",
            {},
        ).get(
            "type"
        )
        == "polymarket_lite"
    )

    print(
        "DATASET"
    )

    print(
        f"Session duration:     "
        f"{session_duration:.3f} sec"
    )

    print(
        f"Total ordered events: "
        f"{len(records)}"
    )

    print(
        f"Coinbase ticks:       "
        f"{coinbase_events}"
    )

    print(
        f"Polymarket LITE:      "
        f"{polymarket_lite_events}"
    )

    print(
        f"BTC impulses >= "
        f"${args.btc_threshold}: "
        f"{len(btc_moves)}"
    )

    print(
        f"Actual BBO changes:   "
        f"{len(pm_moves)}"
    )

    print()

    if not btc_moves:
        print(
            "No qualifying Coinbase moves."
        )
        return

    if not pm_moves:
        print(
            "No Polymarket BBO changes."
        )
        return

    print(
        "NULL MODEL"
    )

    print(
        "The entire Coinbase impulse sequence is "
        "circularly shifted through the session."
    )

    print(
        "BTC clustering, spacing, magnitudes, and "
        "directions are preserved; the original "
        "BTC/Polymarket alignment is destroyed."
    )

    print()

    print(
        "Running null simulations..."
    )

    null_results = run_null_simulation(
        btc_moves=btc_moves,
        pm_moves=pm_moves,
        session_start=session_start,
        session_end=session_end,
        horizons_ms=args.horizons,
        iterations=args.iterations,
        seed=args.seed,
    )

    print(
        "Done."
    )

    print()

    for horizon in args.horizons:
        btc_real = (
            analyze_btc_to_polymarket(
                btc_moves=btc_moves,
                pm_moves=pm_moves,
                horizon_ms=horizon,
            )
        )

        pm_real = (
            analyze_polymarket_after_btc(
                btc_moves=btc_moves,
                pm_moves=pm_moves,
                horizon_ms=horizon,
            )
        )

        print_horizon_results(
            horizon=horizon,
            btc_real=btc_real,
            pm_real=pm_real,
            null=null_results[
                horizon
            ],
        )

    print(
        "=" * 72
    )

    print(
        "INTERPRETATION RULE"
    )

    print(
        "A low p-value means the real alignment was "
        "unusual relative to these circularly shifted "
        "timelines."
    )

    print(
        "It does NOT by itself prove causation, "
        "tradability, or profitability."
    )

    print()

    print(
        "This recording is still a very small sample. "
        "Treat all results as exploratory until the "
        "same pattern appears across many complete "
        "15-minute markets."
    )


if __name__ == "__main__":
    main()