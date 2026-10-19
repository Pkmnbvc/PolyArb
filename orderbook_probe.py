from pprint import pprint

from polymarket_us import PolymarketUS


SLUG = "cpc-btc-above-day-2026-10-20-82000"


def main():
    print("=" * 60)
    print("POLYARB v0.1")
    print("Live Order Book Probe")
    print("=" * 60)

    client = PolymarketUS()

    try:
        print(f"\nMarket:\n{SLUG}\n")

        print("BEST BID / OFFER")
        print("-" * 60)

        bbo_response = client.markets.bbo(SLUG)
        bbo = bbo_response["marketData"]

        pprint(bbo)

        print("\nFULL ORDER BOOK")
        print("-" * 60)

        book_response = client.markets.book(SLUG)
        book = book_response["marketData"]

        pprint(book)

    finally:
        client.close()


if __name__ == "__main__":
    main()