from datetime import datetime, timezone
from decimal import Decimal
import time

from polymarket_us import PolymarketUS
from polymarket_us.errors import NotFoundError

from polyarb.settlement import get_window_state


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def quote_value(quote):
    if not quote:
        return None

    return quote.get("value")


def wait_for_order_book(client, slug, attempts=10, delay=3):
    """
    Retrieve one coherent full-order-book snapshot.

    We intentionally calculate the best bid and best ask from the SAME
    book response instead of combining separate REST responses.
    """

    for attempt in range(1, attempts + 1):

        try:
            response = client.markets.book(slug)
            book = response["marketData"]

            bids = book.get("bids", [])
            offers = book.get("offers", [])

            if not bids or not offers:
                print(
                    f"Book exists but has no usable depth "
                    f"({attempt}/{attempts})"
                )

                if attempt < attempts:
                    time.sleep(delay)

                continue

            best_bid = max(
                Decimal(level["px"]["value"])
                for level in bids
            )

            best_ask = min(
                Decimal(level["px"]["value"])
                for level in offers
            )

            print(
                f"Order book ready on attempt {attempt}."
            )

            return book, best_bid, best_ask

        except NotFoundError:
            print(
                f"Order book not ready for {slug} "
                f"({attempt}/{attempts})"
            )

            if attempt < attempts:
                time.sleep(delay)

    return None, None, None


# ---------------------------------------------------------------------
# CURRENT BTC 15-MINUTE MARKET
# ---------------------------------------------------------------------

def find_current_btc_15m(client):
    now = datetime.now(timezone.utc)

    # Examples:
    #
    # 23:17 UTC -> 23:15 window
    # 23:43 UTC -> 23:30 window
    #
    # Floor the current time to the beginning of
    # the current 15-minute interval.

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

    print(
        f"Looking for current market: {slug}"
    )

    try:
        response = client.markets.retrieve_by_slug(slug)

    except NotFoundError as exc:
        print(
            f"Current market not available yet: {exc}"
        )

        return None

    market = response.get(
        "market",
        response,
    )

    terms = market.get(
        "assetPriceTerms"
    ) or {}

    window_start_raw = terms.get(
        "windowStart"
    )

    window_end_raw = terms.get(
        "windowEnd"
    )

    # Do not continue if Polymarket returned
    # incomplete market timing metadata.

    if not window_start_raw or not window_end_raw:
        print(
            "Market exists but window timing "
            "metadata is missing."
        )

        return None

    start_dt = datetime.fromisoformat(
        window_start_raw.replace(
            "Z",
            "+00:00",
        )
    )

    end_dt = datetime.fromisoformat(
        window_end_raw.replace(
            "Z",
            "+00:00",
        )
    )

    # Final safety check:
    # only return the market if NOW is actually
    # inside this contract's time window.

    if start_dt <= now < end_dt:
        return market

    return None


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    print("=" * 72)
    print("POLYARB v0.1")
    print("BTC 15-Minute Live Market Monitor")
    print("=" * 72)

    client = PolymarketUS()

    try:

        # =============================================================
        # 1. FIND CURRENT 15-MINUTE BTC MARKET
        # =============================================================

        market = find_current_btc_15m(
            client
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

        # =============================================================
        # 2. MARKET INFORMATION
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
            "PRICE TO BEAT: "
            f"${float(terms['priceToBeat']['value']):,.2f}"
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
        # 3. WINDOW / SETTLEMENT CLOCK
        # =============================================================

        state = get_window_state(
            terms["windowStart"],
            terms["windowEnd"],
        )

        print("\nWINDOW STATE")
        print("-" * 72)

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
        # 4. RETRIEVE ONE COHERENT ORDER-BOOK SNAPSHOT
        # =============================================================

        print("\nWAITING FOR ORDER BOOK")
        print("-" * 72)

        book, best_bid, best_ask = wait_for_order_book(
            client,
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
        # 5. COHERENT TOP-OF-BOOK DATA
        # =============================================================

        spread = (
            best_ask - best_bid
        )

        print("\nCOHERENT BOOK SNAPSHOT")
        print("-" * 72)

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
        # 6. FULL ORDER BOOK
        # =============================================================

        print("\nORDER BOOK")
        print("-" * 72)

        # -------------------------------------------------------------
        # BIDS
        # -------------------------------------------------------------

        print("\nBIDS")

        bids = sorted(
            book.get(
                "bids",
                [],
            ),
            key=lambda level: Decimal(
                level["px"]["value"]
            ),
            reverse=True,
        )

        for level in bids:

            price = quote_value(
                level.get("px")
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

        print("\nOFFERS")

        offers = sorted(
            book.get(
                "offers",
                [],
            ),
            key=lambda level: Decimal(
                level["px"]["value"]
            ),
        )

        for level in offers:

            price = quote_value(
                level.get("px")
            )

            qty = level.get(
                "qty"
            )

            print(
                f"  {price:>8}   "
                f"{qty:>10} shares"
            )

    finally:
        client.close()


# ---------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()