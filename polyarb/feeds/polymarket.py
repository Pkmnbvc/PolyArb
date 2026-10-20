import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import threading

from polymarket_us import PolymarketUS


# ---------------------------------------------------------------------
# DATA OBJECTS
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class PolymarketLiteTick:
    market_slug: str | None
    best_bid: Decimal | None
    best_ask: Decimal | None
    last_trade: Decimal | None
    received_at: str


@dataclass(frozen=True)
class PolymarketTradeTick:
    market_slug: str | None
    price: Decimal | None
    quantity: Decimal | None
    received_at: str


@dataclass(frozen=True)
class PolymarketBookTick:
    market_slug: str | None
    best_bid: Decimal | None
    best_ask: Decimal | None
    book_timestamp: str | None
    bid_levels: int
    offer_levels: int
    received_at: str


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def quote_decimal(value):
    """
    Convert Polymarket quote structures such as:

        {"value": "0.7400", "currency": "USD"}

    into Decimal("0.7400").
    """

    if value is None:
        return None

    if isinstance(value, dict):
        value = value.get("value")

    if value is None:
        return None

    try:
        return Decimal(
            str(value)
        )

    except Exception:
        return None


def best_bid_from_levels(levels):
    prices = []

    for level in levels:
        price = quote_decimal(
            level.get("px")
        )

        if price is not None:
            prices.append(
                price
            )

    if not prices:
        return None

    return max(
        prices
    )


def best_ask_from_levels(levels):
    prices = []

    for level in levels:
        price = quote_decimal(
            level.get("px")
        )

        if price is not None:
            prices.append(
                price
            )

    if not prices:
        return None

    return min(
        prices
    )


# ---------------------------------------------------------------------
# REAL-TIME POLYMARKET FEED
# ---------------------------------------------------------------------

class PolymarketMarketFeed:
    """
    Real-time Polymarket US market feed.

    Maintains the latest:
    - lightweight bid/ask state
    - full-book top of book
    - trade

    It also places every normalized event into an asyncio queue so
    PolyArb can later record every event instead of polling.
    """

    def __init__(
        self,
        key_id,
        secret_key,
        market_slug,
    ):
        self.market_slug = (
            market_slug
        )

        self._lock = (
            threading.Lock()
        )

        self._latest_lite = None
        self._latest_book = None
        self._latest_trade = None

        self.events = (
            asyncio.Queue(
                maxsize=10000
            )
        )

        self._client = (
            PolymarketUS(
                key_id=key_id,
                secret_key=secret_key,
            )
        )

        self._ws = (
            self._client
            .ws
            .markets()
        )

        self._register_callbacks()

    # -----------------------------------------------------------------
    # CALLBACK REGISTRATION
    # -----------------------------------------------------------------

    def _register_callbacks(
        self,
    ):
        self._ws.on(
            "market_data_lite",
            self._on_market_data_lite,
        )

        self._ws.on(
            "market_data",
            self._on_market_data,
        )

        self._ws.on(
            "trade",
            self._on_trade,
        )

        self._ws.on(
            "error",
            self._on_error,
        )

    # -----------------------------------------------------------------
    # EVENT QUEUE
    # -----------------------------------------------------------------

    def _queue_event(
        self,
        event,
    ):
        """
        Add an event without allowing a full queue to crash the feed.

        For now we drop the oldest event if the queue becomes full.
        """

        try:
            self.events.put_nowait(
                event
            )

        except asyncio.QueueFull:

            try:
                self.events.get_nowait()

            except asyncio.QueueEmpty:
                pass

            try:
                self.events.put_nowait(
                    event
                )

            except asyncio.QueueFull:
                pass

    # -----------------------------------------------------------------
    # LITE MARKET DATA
    # -----------------------------------------------------------------

    def _on_market_data_lite(
        self,
        data,
    ):
        received_at = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        lite = (
            data.get(
                "marketDataLite"
            )
            or {}
        )

        tick = PolymarketLiteTick(
            market_slug=lite.get(
                "marketSlug"
            ),

            best_bid=quote_decimal(
                lite.get(
                    "bestBid"
                )
            ),

            best_ask=quote_decimal(
                lite.get(
                    "bestAsk"
                )
            ),

            last_trade=quote_decimal(
                lite.get(
                    "lastTradePx"
                )
            ),

            received_at=received_at,
        )

        with self._lock:
            self._latest_lite = tick

        self._queue_event(
            {
                "type": "polymarket_lite",
                "data": tick,
            }
        )

    # -----------------------------------------------------------------
    # FULL BOOK
    # -----------------------------------------------------------------

    def _on_market_data(
        self,
        data,
    ):
        received_at = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

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

        tick = PolymarketBookTick(
            market_slug=market_data.get(
                "marketSlug"
            ),

            best_bid=best_bid_from_levels(
                bids
            ),

            best_ask=best_ask_from_levels(
                offers
            ),

            book_timestamp=market_data.get(
                "transactTime"
            ),

            bid_levels=len(
                bids
            ),

            offer_levels=len(
                offers
            ),

            received_at=received_at,
        )

        with self._lock:
            self._latest_book = tick

        self._queue_event(
            {
                "type": "polymarket_book",
                "data": tick,
            }
        )

    # -----------------------------------------------------------------
    # TRADES
    # -----------------------------------------------------------------

    def _on_trade(
        self,
        data,
    ):
        received_at = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        trade = (
            data.get(
                "trade"
            )
            or {}
        )

        tick = PolymarketTradeTick(
            market_slug=trade.get(
                "marketSlug"
            ),

            price=quote_decimal(
                trade.get(
                    "price"
                )
            ),

            quantity=quote_decimal(
                trade.get(
                    "quantity"
                )
            ),

            received_at=received_at,
        )

        with self._lock:
            self._latest_trade = tick

        self._queue_event(
            {
                "type": "polymarket_trade",
                "data": tick,
            }
        )

    # -----------------------------------------------------------------
    # ERRORS
    # -----------------------------------------------------------------

    def _on_error(
        self,
        error,
    ):
        self._queue_event(
            {
                "type": "polymarket_error",
                "data": str(
                    error
                ),
            }
        )

    # -----------------------------------------------------------------
    # START / STOP
    # -----------------------------------------------------------------

    async def start(
        self,
    ):
        await self._ws.connect()

        await self._ws.subscribe(
            "polyarb-book",
            "SUBSCRIPTION_TYPE_MARKET_DATA",
            [
                self.market_slug
            ],
        )

        await self._ws.subscribe(
            "polyarb-lite",
            "SUBSCRIPTION_TYPE_MARKET_DATA_LITE",
            [
                self.market_slug
            ],
        )

        await self._ws.subscribe(
            "polyarb-trades",
            "SUBSCRIPTION_TYPE_TRADE",
            [
                self.market_slug
            ],
        )

    async def stop(
        self,
    ):
        try:
            await self._ws.close()

        except Exception:
            pass

        try:
            self._client.close()

        except Exception:
            pass

    # -----------------------------------------------------------------
    # LATEST STATE
    # -----------------------------------------------------------------

    def latest_lite(
        self,
    ):
        with self._lock:
            return self._latest_lite

    def latest_book(
        self,
    ):
        with self._lock:
            return self._latest_book

    def latest_trade(
        self,
    ):
        with self._lock:
            return self._latest_trade