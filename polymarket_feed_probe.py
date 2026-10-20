import asyncio
import os
from datetime import datetime, timezone

from polymarket_us import PolymarketUS
from polymarket_us.errors import NotFoundError

from polyarb.feeds.polymarket import (
    PolymarketMarketFeed,
)


def current_btc_15m_slug():
    now = datetime.now(
        timezone.utc
    )

    minute = (
        now.minute // 15
    ) * 15

    start = now.replace(
        minute=minute,
        second=0,
        microsecond=0,
    )

    return (
        "cpc-btc-updown-15m-"
        f"{start:%Y-%m-%d-%H%M}z"
    )


async def main():
    print(
        "=" * 78
    )

    print(
        "POLYARB"
    )

    print(
        "Reusable Polymarket Feed Probe"
    )

    print(
        "=" * 78
    )

    key_id = os.environ.get(
        "POLYMARKET_KEY_ID"
    )

    secret_key = os.environ.get(
        "POLYMARKET_SECRET_KEY"
    )

    if not key_id or not secret_key:

        print(
            "Polymarket environment "
            "credentials are missing."
        )

        return

    slug = current_btc_15m_slug()

    print(
        f"Market: {slug}"
    )

    # Confirm market exists.
    check_client = PolymarketUS()

    try:
        check_client.markets.retrieve_by_slug(
            slug
        )

    except NotFoundError:

        print(
            "Current market not available."
        )

        check_client.close()
        return

    check_client.close()

    feed = PolymarketMarketFeed(
        key_id=key_id,
        secret_key=secret_key,
        market_slug=slug,
    )

    try:

        print(
            "Starting Polymarket feed..."
        )

        await feed.start()

        print(
            "Feed started."
        )

        print(
            "Press Ctrl+C to stop."
        )

        while True:

            event = await feed.events.get()

            event_type = event[
                "type"
            ]

            data = event[
                "data"
            ]

            if event_type == "polymarket_lite":

                print(
                    f"[LITE] "
                    f"{data.received_at} | "
                    f"{data.best_bid} / "
                    f"{data.best_ask} | "
                    f"last={data.last_trade}"
                )

            elif event_type == "polymarket_book":

                print(
                    f"[BOOK] "
                    f"{data.received_at} | "
                    f"{data.best_bid} / "
                    f"{data.best_ask} | "
                    f"levels="
                    f"{data.bid_levels}/"
                    f"{data.offer_levels}"
                )

            elif event_type == "polymarket_trade":

                print(
                    f"[TRADE] "
                    f"{data.received_at} | "
                    f"price={data.price} | "
                    f"qty={data.quantity}"
                )

            elif event_type == "polymarket_error":

                print(
                    f"[ERROR] {data}"
                )

    finally:

        await feed.stop()

        print(
            "Feed stopped."
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