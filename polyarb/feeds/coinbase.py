import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from coinbase.websocket import WSClient


PRODUCT_ID = "BTC-USD"


@dataclass(frozen=True)
class CoinbaseTick:
    product_id: str
    price: Decimal
    source_timestamp: str | None
    received_at: str


class CoinbaseBTCFeed:
    """
    Maintains the latest live Coinbase BTC-USD ticker value.

    This is a research proxy for BTC spot movement.

    It is NOT the BRTI settlement benchmark used by Polymarket.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._latest_tick = None

        self._client = WSClient(
            on_message=self._on_message
        )

    def _on_message(self, message):
        received_at = datetime.now(
            timezone.utc
        ).isoformat()

        try:
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message

        except Exception:
            return

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

                price_raw = ticker.get(
                    "price"
                )

                if price_raw is None:
                    continue

                try:
                    price = Decimal(
                        price_raw
                    )

                except Exception:
                    continue

                tick = CoinbaseTick(
                    product_id=PRODUCT_ID,
                    price=price,
                    source_timestamp=source_timestamp,
                    received_at=received_at,
                )

                with self._lock:
                    self._latest_tick = tick

    def start(self):
        self._client.open()

        self._client.ticker(
            product_ids=[
                PRODUCT_ID
            ]
        )

    def stop(self):
        try:
            self._client.ticker_unsubscribe(
                product_ids=[
                    PRODUCT_ID
                ]
            )

        except Exception:
            pass

        try:
            self._client.close()

        except Exception:
            pass

    def latest(self):
        with self._lock:
            return self._latest_tick