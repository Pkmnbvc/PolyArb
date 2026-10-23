from __future__ import annotations

import argparse
import re
from pathlib import Path

from polyarb.replay import replay_jsonl
from polyarb.state_features import (
    StateFeatureEngine,
)


VERSION = "0.1"


EXPECTED = {
    "03:00": {
        "records": 21654,
        "coinbase_ticks": 4077,

        "btc_impulses": 511,
        "btc_up": 268,
        "btc_down": 243,

        "pm_lite": 6923,

        "pm_repricings": 566,
        "pm_up": 311,
        "pm_down": 255,

        "one_sided": 1015,
    },

    "03:15": {
        "records": 26953,
        "coinbase_ticks": 3811,

        "btc_impulses": 471,
        "btc_up": 243,
        "btc_down": 228,

        "pm_lite": 8415,

        "pm_repricings": 778,
        "pm_up": 368,
        "pm_down": 410,

        "one_sided": 348,
    },

    "03:30": {
        "records": 22216,
        "coinbase_ticks": 3688,

        "btc_impulses": 464,
        "btc_up": 254,
        "btc_down": 210,

        "pm_lite": 6753,

        "pm_repricings": 266,
        "pm_up": 160,
        "pm_down": 106,

        "one_sided": 1096,
    },
}


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Validate StateFeatureEngine historical parity "
            "against previously validated PolyArb analyses."
        )
    )

    parser.add_argument(
        "files",
        nargs="+",
        type=Path,
    )

    return parser.parse_args()


def market_label(
    path: Path,
) -> str:
    match = re.search(
        r"-(\d{4})z_",
        path.name,
    )

    if not match:
        return path.stem

    value = match.group(1)

    return (
        f"{value[:2]}:"
        f"{value[2:]}"
    )


def check(
    name: str,
    actual,
    expected,
):
    passed = (
        actual == expected
    )

    status = (
        "PASS"
        if passed
        else "FAIL"
    )

    print(
        f"{name:<28}"
        f"{actual:>10}"
        f"{expected:>10}"
        f"{status:>10}"
    )

    return passed


def main():
    args = parse_args()

    print("=" * 72)
    print(
        f"POLYARB STATE ENGINE HISTORICAL PARITY v{VERSION}"
    )
    print("=" * 72)

    all_passed = True

    for path in args.files:
        label = market_label(
            path
        )

        print()
        print("-" * 72)
        print(
            f"MARKET {label}"
        )
        print("-" * 72)

        if label not in EXPECTED:
            print(
                "No frozen reference values exist "
                "for this recording."
            )
            print(
                "Replay will run, but it cannot receive "
                "a parity PASS/FAIL verdict."
            )

            result = replay_jsonl(
                path,
                engine=StateFeatureEngine(
                    btc_impulse_threshold=1.00
                ),
            )

            print(
                f"Records:       "
                f"{result.stats.total_records}"
            )

            print(
                f"BTC impulses:  "
                f"{result.stats.btc_impulses}"
            )

            print(
                f"PM repricings: "
                f"{result.stats.pm_repricings}"
            )

            continue

        expected = EXPECTED[
            label
        ]

        result = replay_jsonl(
            path,
            engine=StateFeatureEngine(
                btc_impulse_threshold=1.00
            ),
        )

        stats = result.stats

        print(
            f"{'FEATURE':<28}"
            f"{'ACTUAL':>10}"
            f"{'EXPECTED':>10}"
            f"{'RESULT':>10}"
        )

        market_passed = True

        market_passed &= check(
            "Ordered records",
            stats.total_records,
            expected["records"],
        )

        market_passed &= check(
            "Coinbase ticks",
            stats.coinbase_ticks,
            expected["coinbase_ticks"],
        )

        market_passed &= check(
            "BTC impulses",
            stats.btc_impulses,
            expected["btc_impulses"],
        )

        market_passed &= check(
            "BTC UP impulses",
            stats.btc_up_impulses,
            expected["btc_up"],
        )

        market_passed &= check(
            "BTC DOWN impulses",
            stats.btc_down_impulses,
            expected["btc_down"],
        )

        market_passed &= check(
            "PM LITE ticks",
            stats.pm_lite_ticks,
            expected["pm_lite"],
        )

        market_passed &= check(
            "PM signed repricings",
            stats.pm_repricings,
            expected["pm_repricings"],
        )

        market_passed &= check(
            "PM UP repricings",
            stats.pm_up_repricings,
            expected["pm_up"],
        )

        market_passed &= check(
            "PM DOWN repricings",
            stats.pm_down_repricings,
            expected["pm_down"],
        )

        market_passed &= check(
            "One-sided PM LITE",
            stats.one_sided_pm_lite,
            expected["one_sided"],
        )

        print()

        if market_passed:
            print(
                f"{label}: HISTORICAL PARITY PASS"
            )
        else:
            print(
                f"{label}: HISTORICAL PARITY FAILURE"
            )

        all_passed &= market_passed

    print()
    print("=" * 72)

    if all_passed:
        print(
            "OVERALL: STATE ENGINE PARITY PASS"
        )

        print(
            "The production feature engine reproduces "
            "the frozen research event semantics."
        )

    else:
        print(
            "OVERALL: STATE ENGINE PARITY FAILURE"
        )

        print(
            "Do NOT build state-filter logic until "
            "the mismatch is explained."
        )

    print("=" * 72)


if __name__ == "__main__":
    main()