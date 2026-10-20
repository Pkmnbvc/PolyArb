import json
import queue
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
    Real-time Coinbase BTC-USD feed.

    Coinbase is used as a BTC spot proxy for research.
    It is NOT the BRTI settlement benchmark used by Polymarket.

    The feed provides:

    - latest(): newest Coinbase tick
    - events: thread-safe queue containing every received BTC tick
    """

    def __init__(self):
        self._lock = threading.Lock()

        self._latest_tick: CoinbaseTick | None = None

        # Coinbase's SDK callback may run outside our asyncio loop,
        # so use Python's thread-safe queue.Queue here.
        self.events = queue.Queue(
            maxsize=10000
        )

        self._client = WSClient(
            on_message=self._on_message
        )

    # -----------------------------------------------------------------
    # EVENT QUEUE
    # -----------------------------------------------------------------

    def _queue_tick(
        self,
        tick,
    ):
        """
        Store every tick for the research recorder.

        If the queue somehow fills, discard the oldest tick rather than
        allowing the market-data callback to fail.
        """

        try:
            self.events.put_nowait(
                tick
            )

        except queue.Full:

            try:
                self.events.get_nowait()

            except queue.Empty:
                pass

            try:
                self.events.put_nowait(
                    tick
                )

            except queue.Full:
                pass

    # -----------------------------------------------------------------
    # WEBSOCKET CALLBACK
    # -----------------------------------------------------------------

    def _on_message(
        self,
        message,
    ):
        received_at = datetime.now(
            timezone.utc
        ).isoformat()

        try:

            if isinstance(
                message,
                str,
            ):
                data = json.loads(
                    message
                )

            else:
                data = message

        except Exception:
            return

        # Ignore heartbeat/status/subscription messages.
        if data.get(
            "channel"
        ) != "ticker":
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
                    ticker.get(
                        "product_id"
                    )
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

                self._queue_tick(
                    tick
                )

    # -----------------------------------------------------------------
    # START / STOP
    # -----------------------------------------------------------------

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

    # -----------------------------------------------------------------
    # LATEST STATE
    # -----------------------------------------------------------------

    def latest(self):
        with self._lock:
            return self._latest_tick