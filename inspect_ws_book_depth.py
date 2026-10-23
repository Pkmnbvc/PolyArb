from __future__ import annotations

import asyncio
import json
import os

from polymarket_us import PolymarketUS


def find_live_btc_15m_market(
    client: PolymarketUS,
) -> dict:
    result = client.search.query(
        {
            "query": "bitcoin",
        }
    )

    for event in result.get(
        "events",
        [],
    ):
        if not event.get("live"):
            continue

        if event.get("ended"):
            continue

        if (
            event.get("seriesSlug")
            != "btc-updown-15m"
        ):
            continue

        for market in event.get(
            "markets",
            [],
        ):
            terms = market.get(
                "assetPriceTerms",
                {},
            )

            if (
                terms.get("marketType")
                == "ASSET_PRICE_MARKET_TYPE_UP_DOWN"
                and terms.get("horizon")
                == "15m"
                and market.get("status")
                == "MARKET_STATUS_OPEN"
            ):
                return market

    raise RuntimeError(
        "No live BTC Up/Down 15m market found."
    )


async def main() -> None:
    key_id = os.environ.get(
        "POLYMARKET_KEY_ID"
    )

    secret_key = os.environ.get(
        "POLYMARKET_SECRET_KEY"
    )

    if not key_id or not secret_key:
        raise SystemExit(
            "POLYMARKET_KEY_ID and "
            "POLYMARKET_SECRET_KEY must be set."
        )

    # Public discovery client.
    discovery_client = PolymarketUS()

    market = find_live_btc_15m_market(
        discovery_client
    )

    slug = market["slug"]

    print("=" * 80)
    print(
        "POLYARB WEBSOCKET BOOK DEPTH SCHEMA PROBE"
    )
    print("=" * 80)

    print(f"Market: {slug}")

    terms = market.get(
        "assetPriceTerms",
        {},
    )

    print(
        f"Window: "
        f"{terms.get('windowStart')} "
        f"-> {terms.get('windowEnd')}"
    )

    print(
        f"Price to beat: "
        f"{terms.get('priceToBeat')}"
    )

    client = PolymarketUS(
        key_id=key_id,
        secret_key=secret_key,
    )

    ws = client.ws.markets()

    received = asyncio.Event()

    def on_market_data(
        data: dict,
    ) -> None:
        if received.is_set():
            return

        market_data = data.get(
            "marketData",
            {},
        )

        if (
            market_data.get("marketSlug")
            != slug
        ):
            return

        bids = market_data.get(
            "bids",
            [],
        )

        offers = market_data.get(
            "offers",
            [],
        )

        print()
        print("-" * 80)
        print("WEBSOCKET MARKET DATA")
        print("-" * 80)

        print(
            "Keys:",
            sorted(
                market_data.keys()
            ),
        )

        print(
            f"State: "
            f"{market_data.get('state')}"
        )

        print(
            f"Transact time: "
            f"{market_data.get('transactTime')}"
        )

        print(
            f"Bid levels: "
            f"{len(bids)}"
        )

        print(
            f"Offer levels: "
            f"{len(offers)}"
        )

        print()
        print(
            "FIRST 3 WS BIDS"
        )

        for index, level in enumerate(
            bids[:3]
        ):
            print(
                f"\nBID[{index}]"
            )

            print(
                json.dumps(
                    level,
                    indent=2,
                    default=str,
                )
            )

        print()
        print(
            "FIRST 3 WS OFFERS"
        )

        for index, level in enumerate(
            offers[:3]
        ):
            print(
                f"\nOFFER[{index}]"
            )

            print(
                json.dumps(
                    level,
                    indent=2,
                    default=str,
                )
            )

        if bids:
            print()
            print(
                "Bid keys:",
                (
                    sorted(
                        bids[0].keys()
                    )
                    if isinstance(
                        bids[0],
                        dict,
                    )
                    else type(
                        bids[0]
                    ).__name__
                ),
            )

        if offers:
            print(
                "Offer keys:",
                (
                    sorted(
                        offers[0].keys()
                    )
                    if isinstance(
                        offers[0],
                        dict,
                    )
                    else type(
                        offers[0]
                    ).__name__
                ),
            )

        received.set()

    def on_error(
        error,
    ) -> None:
        print(
            f"[WebSocket error] "
            f"{error}"
        )

    ws.on(
        "market_data",
        on_market_data,
    )

    ws.on(
        "error",
        on_error,
    )

    print()
    print(
        "Connecting to markets WebSocket..."
    )

    await ws.connect()

    try:
        await ws.subscribe(
            "polyarb-depth-probe",
            "SUBSCRIPTION_TYPE_MARKET_DATA",
            [slug],
        )

        print(
            "Subscribed. Waiting for "
            "first full book update..."
        )

        await asyncio.wait_for(
            received.wait(),
            timeout=20.0,
        )

    except asyncio.TimeoutError:
        print(
            "No full market_data event "
            "received within 20 seconds."
        )

    finally:
        await ws.close()


if __name__ == "__main__":
    asyncio.run(main())