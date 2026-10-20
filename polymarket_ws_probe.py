import asyncio
import os
from datetime import datetime, timezone
from decimal import Decimal

from polymarket_us import PolymarketUS
from polymarket_us.errors import NotFoundError


def current_btc_15m_slug():
    now = datetime.now(
        timezone.utc
    )

    window_minute = (
        now.minute // 15
    ) * 15

    window_start = now.replace(
        minute=window_minute,
        second=0,
        microsecond=0,
    )

    return (
        "cpc-btc-updown-15m-"
        f"{window_start:%Y-%m-%d-%H%M}z"
    )


def quote_value(quote):
    if quote is None:
        return None

    if isinstance(quote, dict):
        return quote.get("value")

    return quote


def best_bid_from_levels(levels):
    prices = []

    for level in levels:
        px = quote_value(
            level.get("px")
        )

        if px is not None:
            prices.append(
                Decimal(str(px))
            )

    if not prices:
        return None

    return max(prices)


def best_ask_from_levels(levels):
    prices = []

    for level in levels:
        px = quote_value(
            level.get("px")
        )

        if px is not None:
            prices.append(
                Decimal(str(px))
            )

    if not prices:
        return None

    return min(prices)


async def main():
    print("=" * 78)
    print("POLYARB")
    print("Polymarket US Markets WebSocket Probe")
    print("=" * 78)

    key_id = os.environ.get(
        "POLYMARKET_KEY_ID"
    )

    secret_key = os.environ.get(
        "POLYMARKET_SECRET_KEY"
    )

    if not key_id or not secret_key:
        print(
            "Missing POLYMARKET_KEY_ID or "
            "POLYMARKET_SECRET_KEY."
        )
        return

    slug = current_btc_15m_slug()

    print(
        f"Target market: {slug}"
    )

    client = PolymarketUS(
        key_id=key_id,
        secret_key=secret_key,
    )

    # --------------------------------------------------------------
    # Confirm the market actually exists before opening WebSocket.
    # --------------------------------------------------------------

    try:
        response = (
            client
            .markets
            .retrieve_by_slug(slug)
        )

    except NotFoundError:
        print(
            "Current BTC 15-minute market "
            "not available yet."
        )

        client.close()
        return

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

    print(
        f"Price to beat: "
        f"${(terms.get('priceToBeat') or {}).get('value')}"
    )

    print(
        f"Window: "
        f"{terms.get('windowStart')} -> "
        f"{terms.get('windowEnd')}"
    )

    markets_ws = (
        client.ws.markets()
    )

    # --------------------------------------------------------------
    # CALLBACKS
    # --------------------------------------------------------------

    def on_open():
        print(
            "Polymarket WebSocket connected."
        )

    def on_close():
        print(
            "Polymarket WebSocket closed."
        )

    def on_error(error):
        print(
            f"[ERROR] {error}"
        )

    def on_heartbeat():
        print(
            "[HEARTBEAT]"
        )

    def on_market_data(data):
        received_at = datetime.now(
            timezone.utc
        ).isoformat()

        market_data = (
            data.get(
                "marketData"
            )
            or {}
        )

        bids = market_data.get(
            "bids",
            [],
        )

        offers = market_data.get(
            "offers",
            [],
        )

        best_bid = best_bid_from_levels(
            bids
        )

        best_ask = best_ask_from_levels(
            offers
        )

        print(
            f"[BOOK] "
            f"receive={received_at} | "
            f"market={market_data.get('marketSlug')} | "
            f"book_time={market_data.get('transactTime')} | "
            f"bid={best_bid} | "
            f"ask={best_ask} | "
            f"bids={len(bids)} | "
            f"offers={len(offers)}"
        )

    def on_market_data_lite(data):
        received_at = datetime.now(
            timezone.utc
        ).isoformat()

        lite = (
            data.get(
                "marketDataLite"
            )
            or {}
        )

        print(
            f"[LITE] "
            f"receive={received_at} | "
            f"market={lite.get('marketSlug')} | "
            f"bid={lite.get('bestBid')} | "
            f"ask={lite.get('bestAsk')} | "
            f"last={lite.get('lastTradePx')}"
        )

    def on_trade(data):
        received_at = datetime.now(
            timezone.utc
        ).isoformat()

        trade = (
            data.get(
                "trade"
            )
            or {}
        )

        print(
            f"[TRADE] "
            f"receive={received_at} | "
            f"market={trade.get('marketSlug')} | "
            f"price={trade.get('price')} | "
            f"qty={trade.get('quantity')}"
        )

    markets_ws.on(
        "open",
        on_open,
    )

    markets_ws.on(
        "close",
        on_close,
    )

    markets_ws.on(
        "error",
        on_error,
    )

    markets_ws.on(
        "heartbeat",
        on_heartbeat,
    )

    markets_ws.on(
        "market_data",
        on_market_data,
    )

    markets_ws.on(
        "market_data_lite",
        on_market_data_lite,
    )

    markets_ws.on(
        "trade",
        on_trade,
    )

    try:
        print(
            "Connecting to Polymarket Markets WebSocket..."
        )

        await markets_ws.connect()

        print(
            "Subscribing to full market data..."
        )

        await markets_ws.subscribe(
            "btc15-book",
            "SUBSCRIPTION_TYPE_MARKET_DATA",
            [slug],
        )

        print(
            "Subscribing to lightweight market data..."
        )

        await markets_ws.subscribe(
            "btc15-lite",
            "SUBSCRIPTION_TYPE_MARKET_DATA_LITE",
            [slug],
        )

        print(
            "Subscribing to trades..."
        )

        await markets_ws.subscribe(
            "btc15-trades",
            "SUBSCRIPTION_TYPE_TRADE",
            [slug],
        )

        print()
        print(
            "Listening for real-time market events..."
        )

        print(
            "Press Ctrl+C to stop."
        )

        while True:
            await asyncio.sleep(
                1
            )

    except KeyboardInterrupt:
        print()
        print(
            "Stopping Polymarket WebSocket..."
        )

    finally:
        try:
            await markets_ws.close()

        except Exception:
            pass

        client.close()

        print(
            "Polymarket WebSocket stopped."
        )


if __name__ == "__main__":
    try:
        asyncio.run(
            main()
        )
    except KeyboardInterrupt:
        print(
            "\nStopped by user."
        )