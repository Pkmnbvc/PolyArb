from __future__ import annotations

import json

from polymarket_us import PolymarketUS


def find_live_btc_15m_market(client: PolymarketUS) -> dict:
    result = client.search.query(
        {
            "query": "bitcoin",
        }
    )

    for event in result.get("events", []):
        if not event.get("live"):
            continue

        if event.get("ended"):
            continue

        if event.get("seriesSlug") != "btc-updown-15m":
            continue

        for market in event.get("markets", []):
            terms = market.get(
                "assetPriceTerms",
                {},
            )

            if (
                terms.get("marketType")
                == "ASSET_PRICE_MARKET_TYPE_UP_DOWN"
                and terms.get("horizon") == "15m"
                and market.get("status")
                == "MARKET_STATUS_OPEN"
            ):
                return market

    raise RuntimeError(
        "No live BTC Up/Down 15m market found."
    )


def main():
    client = PolymarketUS()

    market = find_live_btc_15m_market(
        client
    )

    slug = market["slug"]

    print("=" * 80)
    print("POLYARB LIVE BTC 15M BOOK DEPTH PROBE")
    print("=" * 80)

    print(f"Market slug:   {slug}")
    print(f"Market ID:     {market.get('id')}")

    terms = market.get(
        "assetPriceTerms",
        {},
    )

    print(
        f"Window start:  "
        f"{terms.get('windowStart')}"
    )

    print(
        f"Window end:    "
        f"{terms.get('windowEnd')}"
    )

    print(
        f"Price to beat: "
        f"{terms.get('priceToBeat')}"
    )

    print()
    print("Fetching book...")

    response = client.markets.book(
        slug
    )

    print()
    print("TOP-LEVEL RESPONSE KEYS")
    print("-" * 80)

    print(
        sorted(response.keys())
        if isinstance(response, dict)
        else type(response).__name__
    )

    market_data = response.get(
        "marketData",
        response,
    )

    print()
    print("MARKET DATA KEYS")
    print("-" * 80)

    print(
        sorted(market_data.keys())
        if isinstance(market_data, dict)
        else type(market_data).__name__
    )

    bids = market_data.get(
        "bids",
        [],
    )

    offers = market_data.get(
        "offers",
        [],
    )

    print()
    print(f"Bid levels:   {len(bids)}")
    print(f"Offer levels: {len(offers)}")

    print()
    print("-" * 80)
    print("FIRST 5 BID LEVELS — RAW")
    print("-" * 80)

    for index, level in enumerate(
        bids[:5]
    ):
        print(f"\nBID[{index}]")
        print(
            json.dumps(
                level,
                indent=2,
                default=str,
            )
        )

    print()
    print("-" * 80)
    print("FIRST 5 OFFER LEVELS — RAW")
    print("-" * 80)

    for index, level in enumerate(
        offers[:5]
    ):
        print(f"\nOFFER[{index}]")
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
            "Bid level keys:",
            sorted(bids[0].keys())
            if isinstance(bids[0], dict)
            else type(bids[0]).__name__,
        )

    if offers:
        print(
            "Offer level keys:",
            sorted(offers[0].keys())
            if isinstance(offers[0], dict)
            else type(offers[0]).__name__,
        )


if __name__ == "__main__":
    main()