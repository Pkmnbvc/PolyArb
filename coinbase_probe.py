import json
import time
from datetime import datetime, timezone

from coinbase.websocket import WSClient


PRODUCT_ID = "BTC-USD"


def on_message(message):
    received_at = datetime.now(
        timezone.utc
    ).isoformat()

    try:
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message

    except json.JSONDecodeError as exc:
        print(
            f"{received_at} | "
            f"JSON decode error: {exc}"
        )
        return

    # Ignore subscription acknowledgements,
    # heartbeats, status messages, etc.
    if data.get("channel") != "ticker":
        return

    source_timestamp = data.get(
        "timestamp"
    )

    events = data.get(
        "events",
        [],
    )

    for event in events:

        tickers = event.get(
            "tickers",
            [],
        )

        for ticker in tickers:

            if (
                ticker.get("product_id")
                != PRODUCT_ID
            ):
                continue

            price = ticker.get(
                "price"
            )

            if price is None:
                continue

            print(
                f"receive={received_at} | "
                f"source={source_timestamp} | "
                f"{PRODUCT_ID} | "
                f"price=${price}"
            )


def main():
    print("=" * 72)
    print("POLYARB")
    print("Coinbase BTC-USD WebSocket Probe")
    print("=" * 72)

    print(
        "Listening for BTC-USD ticker updates..."
    )

    print(
        "Press Ctrl+C to stop."
    )

    client = WSClient(
        on_message=on_message
    )

    try:
        client.open()

        client.ticker(
            product_ids=[
                PRODUCT_ID
            ]
        )

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print()
        print(
            "Stopping Coinbase feed..."
        )

    finally:
        try:
            client.ticker_unsubscribe(
                product_ids=[
                    PRODUCT_ID
                ]
            )
        except Exception:
            pass

        client.close()

        print(
            "Coinbase feed stopped."
        )


if __name__ == "__main__":
    main()