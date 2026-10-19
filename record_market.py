import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from polymarket_us import PolymarketUS
from polymarket_us.errors import NotFoundError

from polyarb.settlement import get_window_state


SAMPLE_INTERVAL_SECONDS = 5

DATA_DIR = Path("data") / "recordings"


def find_current_btc_15m(client):
    now = datetime.now(timezone.utc)

    window_minute = (now.minute // 15) * 15

    expected_window_start = now.replace(
        minute=window_minute,
        second=0,
        microsecond=0,
    )

    slug = (
        "cpc-btc-updown-15m-"
        f"{expected_window_start:%Y-%m-%d-%H%M}z"
    )

    try:
        response = client.markets.retrieve_by_slug(slug)

    except NotFoundError:
        return None

    market = response.get(
        "market",
        response,
    )

    terms = market.get(
        "assetPriceTerms"
    ) or {}

    window_start = terms.get(
        "windowStart"
    )

    window_end = terms.get(
        "windowEnd"
    )

    if not window_start or not window_end:
        return None

    return market


def decimal_value(quote):
    if not quote:
        return None

    value = quote.get("value")

    if value is None:
        return None

    return Decimal(value)


def normalize_levels(levels):
    """
    Convert SDK book levels into JSON-friendly structures.
    """

    normalized = []

    for level in levels:

        px = level.get("px") or {}

        normalized.append(
            {
                "price": px.get("value"),
                "quantity": level.get("qty"),
            }
        )

    return normalized


def create_snapshot(client, market):
    terms = market["assetPriceTerms"]
    slug = market["slug"]

    response = client.markets.book(slug)
    book = response["marketData"]

    bids = book.get("bids", [])
    offers = book.get("offers", [])

    if not bids or not offers:
        return None

    # --------------------------------------------------------------
    # Sort executable depth
    # --------------------------------------------------------------

    bids = sorted(
        bids,
        key=lambda level: Decimal(
            level["px"]["value"]
        ),
        reverse=True,
    )

    offers = sorted(
        offers,
        key=lambda level: Decimal(
            level["px"]["value"]
        ),
    )

    best_bid = decimal_value(
        bids[0]["px"]
    )

    best_ask = decimal_value(
        offers[0]["px"]
    )

    if best_bid is None or best_ask is None:
        return None

    spread = best_ask - best_bid

    # --------------------------------------------------------------
    # Window state
    # --------------------------------------------------------------

    state = get_window_state(
        terms["windowStart"],
        terms["windowEnd"],
    )

    captured_at = datetime.now(
        timezone.utc
    )

    # --------------------------------------------------------------
    # Snapshot
    # --------------------------------------------------------------

    snapshot = {
        "captured_at": captured_at.isoformat(),

        "market": {
            "slug": slug,
            "title": market.get("title"),
            "index": terms.get("indexSymbol"),
            "market_type": terms.get("marketType"),
            "horizon": terms.get("horizon"),
            "price_to_beat": (
                terms.get("priceToBeat") or {}
            ).get("value"),
            "window_start": terms.get("windowStart"),
            "window_end": terms.get("windowEnd"),
        },

        "window": {
            "elapsed_seconds": state.elapsed_seconds,
            "remaining_seconds": state.remaining_seconds,
            "progress": state.progress,
            "phase": state.phase,
        },

        "book": {
            "transact_time": book.get("transactTime"),
            "state": book.get("state"),

            "best_bid": str(best_bid),
            "best_ask": str(best_ask),
            "spread": str(spread),

            "best_bid_quantity": bids[0].get("qty"),
            "best_ask_quantity": offers[0].get("qty"),

            "bid_depth": len(bids),
            "offer_depth": len(offers),

            "bids": normalize_levels(bids),
            "offers": normalize_levels(offers),
        },
    }

    return snapshot


def output_path():
    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    today = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d")

    return DATA_DIR / f"btc_15m_{today}.jsonl"


def save_snapshot(snapshot):
    path = output_path()

    with path.open(
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(
                snapshot,
                separators=(",", ":"),
            )
        )

        file.write("\n")

    return path


def print_snapshot(snapshot):
    market = snapshot["market"]
    window = snapshot["window"]
    book = snapshot["book"]

    print(
        f"{snapshot['captured_at']} | "
        f"{market['slug']} | "
        f"T-{window['remaining_seconds']:.1f}s | "
        f"{window['phase']} | "
        f"bid={book['best_bid']} | "
        f"ask={book['best_ask']} | "
        f"spread={book['spread']}"
    )


def main():
    print("=" * 78)
    print("POLYARB RECORDER v0.1")
    print("BTC 15-Minute Market Research Recorder")
    print("=" * 78)

    print(
        f"Sampling approximately every "
        f"{SAMPLE_INTERVAL_SECONDS} seconds."
    )

    print(
        "Press Ctrl+C to stop safely."
    )

    client = PolymarketUS()

    last_slug = None

    try:

        while True:

            cycle_started = time.monotonic()

            try:
                market = find_current_btc_15m(
                    client
                )

                if market is None:
                    print(
                        "No current BTC 15m market. "
                        "Waiting..."
                    )

                else:
                    slug = market["slug"]

                    if slug != last_slug:

                        print()
                        print("-" * 78)
                        print(
                            f"NEW MARKET: {slug}"
                        )

                        terms = market[
                            "assetPriceTerms"
                        ]

                        price_to_beat = (
                            terms.get(
                                "priceToBeat"
                            ) or {}
                        ).get("value")

                        print(
                            f"Price to beat: "
                            f"${price_to_beat}"
                        )

                        print(
                            f"Window: "
                            f"{terms.get('windowStart')} "
                            f"-> "
                            f"{terms.get('windowEnd')}"
                        )

                        print("-" * 78)

                        last_slug = slug

                    snapshot = create_snapshot(
                        client,
                        market,
                    )

                    if snapshot is None:
                        print(
                            "Book unavailable or empty; "
                            "snapshot skipped."
                        )

                    else:
                        path = save_snapshot(
                            snapshot
                        )

                        print_snapshot(
                            snapshot
                        )

            except NotFoundError:
                print(
                    "Market/book unavailable during "
                    "rollover; skipping this sample."
                )

            except Exception as exc:
                print(
                    f"Recorder error: "
                    f"{type(exc).__name__}: {exc}"
                )

            # ------------------------------------------------------
            # Maintain approximate sampling cadence.
            # ------------------------------------------------------

            cycle_elapsed = (
                time.monotonic()
                - cycle_started
            )

            sleep_for = max(
                0,
                SAMPLE_INTERVAL_SECONDS
                - cycle_elapsed,
            )

            time.sleep(
                sleep_for
            )

    except KeyboardInterrupt:
        print()
        print("Recorder stopped by user.")

    finally:
        client.close()

        print(
            f"Data directory: "
            f"{DATA_DIR.resolve()}"
        )


if __name__ == "__main__":
    main()