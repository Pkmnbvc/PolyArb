import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from polymarket_us import PolymarketUS
from polymarket_us.errors import NotFoundError

from polyarb.settlement import get_window_state
from polyarb.feeds.coinbase import CoinbaseBTCFeed


SAMPLE_INTERVAL_SECONDS = 5

DATA_DIR = (
    Path("data")
    / "recordings"
)


# ---------------------------------------------------------------------
# CURRENT BTC 15-MINUTE MARKET
# ---------------------------------------------------------------------

def find_current_btc_15m(client):
    now = datetime.now(
        timezone.utc
    )

    window_minute = (
        now.minute // 15
    ) * 15

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
        response = (
            client
            .markets
            .retrieve_by_slug(slug)
        )

    except NotFoundError:
        return None

    market = response.get(
        "market",
        response,
    )

    terms = (
        market.get(
            "assetPriceTerms"
        )
        or {}
    )

    window_start = (
        terms.get(
            "windowStart"
        )
    )

    window_end = (
        terms.get(
            "windowEnd"
        )
    )

    if not window_start or not window_end:
        return None

    start_dt = (
        datetime.fromisoformat(
            window_start.replace(
                "Z",
                "+00:00",
            )
        )
    )

    end_dt = (
        datetime.fromisoformat(
            window_end.replace(
                "Z",
                "+00:00",
            )
        )
    )

    if start_dt <= now < end_dt:
        return market

    return None


# ---------------------------------------------------------------------
# BOOK HELPERS
# ---------------------------------------------------------------------

def decimal_value(quote):
    if not quote:
        return None

    value = quote.get(
        "value"
    )

    if value is None:
        return None

    return Decimal(
        value
    )


def normalize_levels(levels):
    normalized = []

    for level in levels:

        px = (
            level.get(
                "px"
            )
            or {}
        )

        normalized.append(
            {
                "price": px.get(
                    "value"
                ),

                "quantity": level.get(
                    "qty"
                ),
            }
        )

    return normalized


# ---------------------------------------------------------------------
# CREATE COMBINED SNAPSHOT
# ---------------------------------------------------------------------

def create_snapshot(
    client,
    market,
    coinbase,
):
    terms = market[
        "assetPriceTerms"
    ]

    slug = market[
        "slug"
    ]

    # --------------------------------------------------------------
    # POLYMARKET BOOK
    # --------------------------------------------------------------

    response = (
        client
        .markets
        .book(slug)
    )

    book = response[
        "marketData"
    ]

    bids = book.get(
        "bids",
        [],
    )

    offers = book.get(
        "offers",
        [],
    )

    if not bids or not offers:
        return None

    bids = sorted(
        bids,
        key=lambda level: Decimal(
            level[
                "px"
            ][
                "value"
            ]
        ),
        reverse=True,
    )

    offers = sorted(
        offers,
        key=lambda level: Decimal(
            level[
                "px"
            ][
                "value"
            ]
        ),
    )

    best_bid = decimal_value(
        bids[0][
            "px"
        ]
    )

    best_ask = decimal_value(
        offers[0][
            "px"
        ]
    )

    if (
        best_bid is None
        or best_ask is None
    ):
        return None

    spread = (
        best_ask
        - best_bid
    )

    # --------------------------------------------------------------
    # LOCAL CAPTURE TIME
    # --------------------------------------------------------------

    captured_at = datetime.now(
        timezone.utc
    )

    # --------------------------------------------------------------
    # SETTLEMENT WINDOW STATE
    # --------------------------------------------------------------

    state = get_window_state(
        terms[
            "windowStart"
        ],
        terms[
            "windowEnd"
        ],
        now=captured_at,
    )

    # --------------------------------------------------------------
    # COINBASE REFERENCE
    # --------------------------------------------------------------

    coinbase_tick = (
        coinbase.latest()
    )

    coinbase_data = None

    if coinbase_tick is not None:

        price_to_beat_raw = (
            terms.get(
                "priceToBeat"
            )
            or {}
        ).get(
            "value"
        )

        price_to_beat = None

        if price_to_beat_raw:

            price_to_beat = Decimal(
                price_to_beat_raw
            )

        tick_received_at = (
            datetime.fromisoformat(
                coinbase_tick.received_at
            )
        )

        tick_age_ms = (
            captured_at
            - tick_received_at
        ).total_seconds() * 1000

        distance_from_target = None

        if price_to_beat is not None:

            distance_from_target = (
                coinbase_tick.price
                - price_to_beat
            )

        coinbase_data = {
            "product_id": (
                coinbase_tick.product_id
            ),

            "price": str(
                coinbase_tick.price
            ),

            "source_timestamp": (
                coinbase_tick.source_timestamp
            ),

            "received_at": (
                coinbase_tick.received_at
            ),

            "tick_age_ms": (
                tick_age_ms
            ),

            "proxy_distance_from_price_to_beat": (
                str(
                    distance_from_target
                )
                if distance_from_target
                is not None
                else None
            ),

            "warning": (
                "Coinbase BTC-USD is a spot proxy, "
                "not the BRTI settlement benchmark."
            ),
        }

    # --------------------------------------------------------------
    # COMPLETE RESEARCH SNAPSHOT
    # --------------------------------------------------------------

    snapshot = {
        "captured_at": (
            captured_at.isoformat()
        ),

        "market": {
            "slug": slug,

            "title": market.get(
                "title"
            ),

            "index": terms.get(
                "indexSymbol"
            ),

            "market_type": terms.get(
                "marketType"
            ),

            "horizon": terms.get(
                "horizon"
            ),

            "price_to_beat": (
                terms.get(
                    "priceToBeat"
                )
                or {}
            ).get(
                "value"
            ),

            "window_start": terms.get(
                "windowStart"
            ),

            "window_end": terms.get(
                "windowEnd"
            ),
        },

        "window": {
            "elapsed_seconds": (
                state.elapsed_seconds
            ),

            "remaining_seconds": (
                state.remaining_seconds
            ),

            "progress": (
                state.progress
            ),

            "phase": (
                state.phase
            ),
        },

        "reference": {
            "coinbase": (
                coinbase_data
            ),
        },

        "book": {
            "transact_time": (
                book.get(
                    "transactTime"
                )
            ),

            "state": (
                book.get(
                    "state"
                )
            ),

            "best_bid": str(
                best_bid
            ),

            "best_ask": str(
                best_ask
            ),

            "spread": str(
                spread
            ),

            "best_bid_quantity": (
                bids[0].get(
                    "qty"
                )
            ),

            "best_ask_quantity": (
                offers[0].get(
                    "qty"
                )
            ),

            "bid_depth": len(
                bids
            ),

            "offer_depth": len(
                offers
            ),

            "bids": normalize_levels(
                bids
            ),

            "offers": normalize_levels(
                offers
            ),
        },
    }

    return snapshot


# ---------------------------------------------------------------------
# SAVE
# ---------------------------------------------------------------------

def output_path():
    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    today = datetime.now(
        timezone.utc
    ).strftime(
        "%Y%m%d"
    )

    return (
        DATA_DIR
        / f"btc_15m_{today}.jsonl"
    )


def save_snapshot(snapshot):
    path = output_path()

    with path.open(
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(
                snapshot,
                separators=(
                    ",",
                    ":",
                ),
            )
        )

        file.write(
            "\n"
        )

    return path


# ---------------------------------------------------------------------
# CONSOLE OUTPUT
# ---------------------------------------------------------------------

def print_snapshot(snapshot):
    market = snapshot[
        "market"
    ]

    window = snapshot[
        "window"
    ]

    book = snapshot[
        "book"
    ]

    coinbase = (
        snapshot
        .get(
            "reference",
            {},
        )
        .get(
            "coinbase"
        )
    )

    if coinbase:

        print(
            f"{snapshot['captured_at']} | "
            f"T-{window['remaining_seconds']:.1f}s | "
            f"BTC=${coinbase['price']} | "
            f"Δ=${coinbase['proxy_distance_from_price_to_beat']} | "
            f"UP={book['best_bid']}/{book['best_ask']} | "
            f"BTC age={coinbase['tick_age_ms']:.0f}ms"
        )

    else:

        print(
            f"{snapshot['captured_at']} | "
            f"{market['slug']} | "
            f"T-{window['remaining_seconds']:.1f}s | "
            f"BTC=WAITING | "
            f"UP={book['best_bid']}/{book['best_ask']}"
        )


# ---------------------------------------------------------------------
# MAIN RECORDER
# ---------------------------------------------------------------------

def main():
    print(
        "=" * 78
    )

    print(
        "POLYARB RECORDER v0.2"
    )

    print(
        "BTC 15-Minute Market + Coinbase Research Recorder"
    )

    print(
        "=" * 78
    )

    print(
        f"Sampling approximately every "
        f"{SAMPLE_INTERVAL_SECONDS} seconds."
    )

    print(
        "Coinbase BTC-USD is a proxy; "
        "BRTI remains the settlement benchmark."
    )

    print(
        "Press Ctrl+C to stop safely."
    )

    polymarket = (
        PolymarketUS()
    )

    coinbase = (
        CoinbaseBTCFeed()
    )

    last_slug = None

    try:

        # ----------------------------------------------------------
        # START COINBASE WEBSOCKET
        # ----------------------------------------------------------

        print()

        print(
            "Starting Coinbase BTC-USD reference feed..."
        )

        coinbase.start()

        # Give the stream a moment to receive
        # its first message.

        started = (
            time.monotonic()
        )

        while (
            coinbase.latest()
            is None
            and (
                time.monotonic()
                - started
            ) < 5
        ):
            time.sleep(
                0.1
            )

        first_tick = (
            coinbase.latest()
        )

        if first_tick is None:

            print(
                "Coinbase connected, but no BTC tick "
                "has arrived yet."
            )

        else:

            print(
                f"Coinbase feed ready: "
                f"BTC-USD ${first_tick.price}"
            )

        print()

        # ----------------------------------------------------------
        # RECORDING LOOP
        # ----------------------------------------------------------

        while True:

            cycle_started = (
                time.monotonic()
            )

            try:

                market = (
                    find_current_btc_15m(
                        polymarket
                    )
                )

                if market is None:

                    print(
                        "No current BTC 15m market. "
                        "Waiting..."
                    )

                else:

                    slug = market[
                        "slug"
                    ]

                    if slug != last_slug:

                        print()

                        print(
                            "-" * 78
                        )

                        print(
                            f"NEW MARKET: "
                            f"{slug}"
                        )

                        terms = market[
                            "assetPriceTerms"
                        ]

                        price_to_beat = (
                            terms.get(
                                "priceToBeat"
                            )
                            or {}
                        ).get(
                            "value"
                        )

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

                        print(
                            "-" * 78
                        )

                        last_slug = (
                            slug
                        )

                    snapshot = (
                        create_snapshot(
                            polymarket,
                            market,
                            coinbase,
                        )
                    )

                    if snapshot is None:

                        print(
                            "Book unavailable or empty; "
                            "snapshot skipped."
                        )

                    else:

                        save_snapshot(
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
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )

            # ------------------------------------------------------
            # APPROXIMATE FIVE-SECOND CADENCE
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

        print(
            "Recorder stopped by user."
        )

    finally:

        try:
            coinbase.stop()

        except Exception:
            pass

        polymarket.close()

        print(
            f"Data directory: "
            f"{DATA_DIR.resolve()}"
        )


# ---------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()