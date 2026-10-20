from datetime import datetime, timezone
from decimal import Decimal
import time

from polymarket_us import PolymarketUS
from polymarket_us.errors import NotFoundError

from polyarb.settlement import get_window_state
from polyarb.feeds.coinbase import CoinbaseBTCFeed


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def quote_value(quote):
    if not quote:
        return None

    return quote.get("value")


def wait_for_coinbase_tick(coinbase, timeout_seconds=5):
    """
    Wait briefly for the Coinbase WebSocket to receive its first BTC tick.
    """

    started = time.monotonic()

    while (
        time.monotonic() - started
        < timeout_seconds
    ):
        tick = coinbase.latest()

        if tick is not None:
            return tick

        time.sleep(0.1)

    return None


def wait_for_order_book(
    client,
    slug,
    attempts=10,
    delay=3,
):
    """
    Retrieve one coherent full-order-book snapshot.

    Best bid and best ask are calculated from the SAME book response.
    """

    for attempt in range(
        1,
        attempts + 1,
    ):

        try:
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

                print(
                    f"Book exists but has no usable depth "
                    f"({attempt}/{attempts})"
                )

                if attempt < attempts:
                    time.sleep(delay)

                continue

            best_bid = max(
                Decimal(
                    level["px"]["value"]
                )
                for level in bids
            )

            best_ask = min(
                Decimal(
                    level["px"]["value"]
                )
                for level in offers
            )

            print(
                f"Order book ready on attempt {attempt}."
            )

            return (
                book,
                best_bid,
                best_ask,
            )

        except NotFoundError:

            print(
                f"Order book not ready for {slug} "
                f"({attempt}/{attempts})"
            )

            if attempt < attempts:
                time.sleep(delay)

    return (
        None,
        None,
        None,
    )


# ---------------------------------------------------------------------
# CURRENT BTC 15-MINUTE MARKET
# ---------------------------------------------------------------------

def find_current_btc_15m(client):
    now = datetime.now(
        timezone.utc
    )

    # Examples:
    #
    # 23:17 UTC -> 23:15 window
    # 23:43 UTC -> 23:30 window

    window_minute = (
        now.minute // 15
    ) * 15

    expected_window_start = (
        now.replace(
            minute=window_minute,
            second=0,
            microsecond=0,
        )
    )

    slug = (
        "cpc-btc-updown-15m-"
        f"{expected_window_start:%Y-%m-%d-%H%M}z"
    )

    print(
        f"Looking for current market: {slug}"
    )

    try:
        response = (
            client
            .markets
            .retrieve_by_slug(slug)
        )

    except NotFoundError as exc:

        print(
            f"Current market not available yet: "
            f"{exc}"
        )

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

    window_start_raw = (
        terms.get(
            "windowStart"
        )
    )

    window_end_raw = (
        terms.get(
            "windowEnd"
        )
    )

    if (
        not window_start_raw
        or not window_end_raw
    ):

        print(
            "Market exists but window timing "
            "metadata is missing."
        )

        return None

    start_dt = (
        datetime.fromisoformat(
            window_start_raw.replace(
                "Z",
                "+00:00",
            )
        )
    )

    end_dt = (
        datetime.fromisoformat(
            window_end_raw.replace(
                "Z",
                "+00:00",
            )
        )
    )

    if start_dt <= now < end_dt:
        return market

    return None


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    print("=" * 72)
    print("POLYARB v0.2")
    print("BTC 15-Minute Live Market Monitor")
    print("=" * 72)

    polymarket = PolymarketUS()
    coinbase = CoinbaseBTCFeed()

    try:

        # =============================================================
        # 1. START COINBASE BTC REFERENCE FEED
        # =============================================================

        print(
            "\nStarting Coinbase BTC-USD reference feed..."
        )

        coinbase.start()

        coinbase_tick = wait_for_coinbase_tick(
            coinbase
        )

        if coinbase_tick is None:

            print(
                "Coinbase feed connected, but no BTC "
                "tick arrived yet."
            )

        else:

            print(
                f"Coinbase BTC-USD: "
                f"${coinbase_tick.price}"
            )

        # =============================================================
        # 2. FIND CURRENT POLYMARKET BTC 15-MINUTE MARKET
        # =============================================================

        print()

        market = find_current_btc_15m(
            polymarket
        )

        if not market:

            print(
                "No active BTC 15-minute market found."
            )

            return

        terms = market[
            "assetPriceTerms"
        ]

        slug = market[
            "slug"
        ]

        price_to_beat = Decimal(
            terms[
                "priceToBeat"
            ][
                "value"
            ]
        )

        # =============================================================
        # 3. MARKET INFORMATION
        # =============================================================

        print()

        print(
            f"MARKET:        "
            f"{market['title']}"
        )

        print(
            f"SLUG:          "
            f"{slug}"
        )

        print(
            f"INDEX:         "
            f"{terms['indexSymbol']}"
        )

        print(
            f"PRICE TO BEAT: "
            f"${price_to_beat:,.2f}"
        )

        print(
            f"WINDOW START:  "
            f"{terms['windowStart']}"
        )

        print(
            f"WINDOW END:    "
            f"{terms['windowEnd']}"
        )

        # =============================================================
        # 4. WINDOW / SETTLEMENT CLOCK
        # =============================================================

        state = get_window_state(
            terms[
                "windowStart"
            ],
            terms[
                "windowEnd"
            ],
        )

        print(
            "\nWINDOW STATE"
        )

        print(
            "-" * 72
        )

        print(
            f"Elapsed:       "
            f"{state.elapsed_seconds:.1f} seconds"
        )

        print(
            f"Remaining:     "
            f"{state.remaining_seconds:.1f} seconds"
        )

        print(
            f"Progress:      "
            f"{state.progress * 100:.1f}%"
        )

        print(
            f"Phase:         "
            f"{state.phase}"
        )

        # =============================================================
        # 5. COINBASE REFERENCE SNAPSHOT
        # =============================================================

        coinbase_tick = (
            coinbase.latest()
        )

        print(
            "\nBTC REFERENCE"
        )

        print(
            "-" * 72
        )

        if coinbase_tick is None:

            print(
                "Coinbase BTC-USD: waiting for tick"
            )

        else:

            captured_at = datetime.now(
                timezone.utc
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

            proxy_distance = (
                coinbase_tick.price
                - price_to_beat
            )

            print(
                f"Coinbase BTC:  "
                f"${coinbase_tick.price:,.2f}"
            )

            print(
                f"Price to beat: "
                f"${price_to_beat:,.2f}"
            )

            print(
                f"Proxy delta:   "
                f"${proxy_distance:+,.2f}"
            )

            print(
                f"Tick age:      "
                f"{tick_age_ms:.0f} ms"
            )

            print(
                f"Source time:   "
                f"{coinbase_tick.source_timestamp}"
            )

            print(
                f"Received at:   "
                f"{coinbase_tick.received_at}"
            )

            print(
                "NOTE: Coinbase BTC-USD is a proxy; "
                "Polymarket settles using BRTI."
            )

        # =============================================================
        # 6. RETRIEVE ONE COHERENT ORDER-BOOK SNAPSHOT
        # =============================================================

        print(
            "\nWAITING FOR ORDER BOOK"
        )

        print(
            "-" * 72
        )

        (
            book,
            best_bid,
            best_ask,
        ) = wait_for_order_book(
            polymarket,
            slug,
        )

        if book is None:

            print()

            print(
                "Market metadata exists, but the "
                "trading book is not live yet."
            )

            print(
                "PolyArb will skip this market "
                "rather than crash."
            )

            return

        # =============================================================
        # 7. COHERENT TOP-OF-BOOK DATA
        # =============================================================

        spread = (
            best_ask
            - best_bid
        )

        print(
            "\nCOHERENT BOOK SNAPSHOT"
        )

        print(
            "-" * 72
        )

        print(
            f"Best bid:      "
            f"{best_bid}"
        )

        print(
            f"Best ask:      "
            f"{best_ask}"
        )

        print(
            f"Spread:        "
            f"{spread}"
        )

        print(
            f"Book time:     "
            f"{book.get('transactTime')}"
        )

        print(
            f"Book state:    "
            f"{book.get('state')}"
        )

        # =============================================================
        # 8. COMBINED RESEARCH SNAPSHOT
        # =============================================================

        coinbase_tick = (
            coinbase.latest()
        )

        print(
            "\nCOMBINED RESEARCH SNAPSHOT"
        )

        print(
            "-" * 72
        )

        print(
            f"Time remaining: "
            f"{state.remaining_seconds:.1f}s"
        )

        print(
            f"UP market:      "
            f"{best_bid} / {best_ask}"
        )

        if coinbase_tick is not None:

            proxy_distance = (
                coinbase_tick.price
                - price_to_beat
            )

            print(
                f"BTC proxy:      "
                f"${coinbase_tick.price:,.2f}"
            )

            print(
                f"BRTI start:     "
                f"${price_to_beat:,.2f}"
            )

            print(
                f"Proxy distance: "
                f"${proxy_distance:+,.2f}"
            )

        else:

            print(
                "BTC proxy:      unavailable"
            )

        # =============================================================
        # 9. FULL ORDER BOOK
        # =============================================================

        print(
            "\nORDER BOOK"
        )

        print(
            "-" * 72
        )

        # -------------------------------------------------------------
        # BIDS
        # -------------------------------------------------------------

        print(
            "\nBIDS"
        )

        bids = sorted(
            book.get(
                "bids",
                [],
            ),
            key=lambda level: Decimal(
                level[
                    "px"
                ][
                    "value"
                ]
            ),
            reverse=True,
        )

        for level in bids:

            price = quote_value(
                level.get(
                    "px"
                )
            )

            qty = level.get(
                "qty"
            )

            print(
                f"  {price:>8}   "
                f"{qty:>10} shares"
            )

        # -------------------------------------------------------------
        # OFFERS
        # -------------------------------------------------------------

        print(
            "\nOFFERS"
        )

        offers = sorted(
            book.get(
                "offers",
                [],
            ),
            key=lambda level: Decimal(
                level[
                    "px"
                ][
                    "value"
                ]
            ),
        )

        for level in offers:

            price = quote_value(
                level.get(
                    "px"
                )
            )

            qty = level.get(
                "qty"
            )

            print(
                f"  {price:>8}   "
                f"{qty:>10} shares"
            )

    finally:

        # Stop the Coinbase WebSocket first.
        try:
            coinbase.stop()

        except Exception:
            pass

        # Then close the Polymarket client.
        polymarket.close()


# ---------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()