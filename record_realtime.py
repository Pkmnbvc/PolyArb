import asyncio
import heapq
import json
import os
import queue as thread_queue
import time

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from polymarket_us import PolymarketUS
from polymarket_us.errors import NotFoundError

from polyarb.feeds.coinbase import CoinbaseBTCFeed
from polyarb.feeds.polymarket import PolymarketMarketFeed


DATA_DIR = Path("data") / "recordings"

# Briefly hold events so Coinbase and Polymarket can be placed in
# chronological order using their LOCAL received_at timestamps.
REORDER_WINDOW_SECONDS = 0.250

RECORDER_VERSION = "0.3"


# ---------------------------------------------------------------------
# JSON SERIALIZATION
# ---------------------------------------------------------------------

def json_default(value):
    if isinstance(value, Decimal):
        return str(value)

    if is_dataclass(value):
        return asdict(value)

    raise TypeError(
        f"Cannot serialize {type(value).__name__}"
    )


# ---------------------------------------------------------------------
# CURRENT BTC 15-MINUTE MARKET
# ---------------------------------------------------------------------

def current_btc_15m_slug():
    now = datetime.now(timezone.utc)

    minute = (now.minute // 15) * 15

    start = now.replace(
        minute=minute,
        second=0,
        microsecond=0,
    )

    return (
        "cpc-btc-updown-15m-"
        f"{start:%Y-%m-%d-%H%M}z"
    )


def get_market_metadata():
    """
    REST is used only for market discovery and static metadata.

    Real-time market movement comes from the WebSocket.
    """

    slug = current_btc_15m_slug()

    client = PolymarketUS()

    try:
        response = client.markets.retrieve_by_slug(
            slug
        )

    except NotFoundError:
        return None

    finally:
        client.close()

    market = response.get(
        "market",
        response,
    )

    terms = (
        market.get("assetPriceTerms")
        or {}
    )

    return {
        "slug": market.get("slug"),
        "title": market.get("title"),
        "index": terms.get("indexSymbol"),
        "horizon": terms.get("horizon"),
        "market_type": terms.get("marketType"),

        "price_to_beat": (
            terms.get("priceToBeat")
            or {}
        ).get("value"),

        "window_start": terms.get(
            "windowStart"
        ),

        "window_end": terms.get(
            "windowEnd"
        ),
    }


# ---------------------------------------------------------------------
# TIME HELPERS
# ---------------------------------------------------------------------

def parse_timestamp(timestamp):
    return datetime.fromisoformat(
        timestamp.replace(
            "Z",
            "+00:00",
        )
    )


def event_received_at(event):
    """
    Get the local timestamp assigned by the feed callback when the
    event reached this computer.

    Coinbase and Polymarket both use the same local system clock for
    received_at, making these timestamps suitable for ordering.
    """

    data = event.get("data")

    received_at = getattr(
        data,
        "received_at",
        None,
    )

    if received_at:
        return received_at

    # Fallback for unusual/error events.
    return datetime.now(
        timezone.utc
    ).isoformat()


def event_epoch(event):
    return parse_timestamp(
        event_received_at(event)
    ).timestamp()


def seconds_remaining_at(
    market,
    timestamp,
):
    end = parse_timestamp(
        market["window_end"]
    )

    now = parse_timestamp(
        timestamp
    )

    remaining = (
        end - now
    ).total_seconds()

    return max(
        0.0,
        remaining,
    )


def current_seconds_remaining(
    market,
):
    end = parse_timestamp(
        market["window_end"]
    )

    remaining = (
        end
        - datetime.now(timezone.utc)
    ).total_seconds()

    return max(
        0.0,
        remaining,
    )


# ---------------------------------------------------------------------
# RECORDING SESSION
# ---------------------------------------------------------------------

def create_session_id():
    return datetime.now(
        timezone.utc
    ).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )


def output_path(session_id):
    """
    Every recorder run gets its own file.

    This prevents sequence numbers from restarting inside a file left
    over from an earlier test session.
    """

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    return (
        DATA_DIR
        / f"realtime_causal_btc_15m_{session_id}.jsonl"
    )


def write_record(
    file,
    record,
):
    file.write(
        json.dumps(
            record,
            default=json_default,
            separators=(",", ":"),
        )
    )

    file.write("\n")
    file.flush()


# ---------------------------------------------------------------------
# COINBASE EVENT BRIDGE
# ---------------------------------------------------------------------

async def consume_coinbase(
    coinbase,
    incoming_events,
):
    """
    Coinbase's SDK callback uses a thread-safe queue.

    Bridge its ticks into the recorder's asyncio event stream.
    """

    while True:
        try:
            tick = await asyncio.to_thread(
                coinbase.events.get,
                True,
                0.5,
            )

        except thread_queue.Empty:
            await asyncio.sleep(0)
            continue

        await incoming_events.put(
            {
                "source": "coinbase",
                "type": "coinbase_tick",
                "data": tick,
            }
        )


# ---------------------------------------------------------------------
# POLYMARKET EVENT BRIDGE
# ---------------------------------------------------------------------

async def consume_polymarket(
    polymarket,
    incoming_events,
):
    while True:
        event = await polymarket.events.get()

        await incoming_events.put(
            {
                "source": "polymarket",
                "type": event["type"],
                "data": event["data"],
            }
        )


# ---------------------------------------------------------------------
# CAUSAL RECORDER STATE
# ---------------------------------------------------------------------

def empty_causal_state():
    """
    State owned exclusively by the recorder.

    Historical records are never created by asking the live feeds for
    their current latest() values.
    """

    return {
        "coinbase": None,
        "polymarket_lite": None,
        "polymarket_book": None,
        "polymarket_trade": None,
    }


def apply_event_to_state(
    state,
    event,
):
    """
    Advance recorder-owned state using only the event currently being
    processed.
    """

    event_type = event["type"]
    data = event["data"]

    if event_type == "coinbase_tick":
        state["coinbase"] = data

    elif event_type == "polymarket_lite":
        state["polymarket_lite"] = data

    elif event_type == "polymarket_book":
        state["polymarket_book"] = data

    elif event_type == "polymarket_trade":
        state["polymarket_trade"] = data


# ---------------------------------------------------------------------
# BUILD ONE CAUSAL RECORD
# ---------------------------------------------------------------------

def build_record(
    market,
    event,
    state,
    sequence,
    session_id,
):
    event_time = event_received_at(
        event
    )

    event_dt = parse_timestamp(
        event_time
    )

    recorded_dt = datetime.now(
        timezone.utc
    )

    recorded_at = (
        recorded_dt.isoformat()
    )

    processing_delay_ms = round(
        (
            recorded_dt
            - event_dt
        ).total_seconds()
        * 1000,
        3,
    )

    return {
        "session_id": session_id,
        "sequence": sequence,

        "recorder": {
            "version": RECORDER_VERSION,
            "reorder_window_ms": (
                REORDER_WINDOW_SECONDS
                * 1000
            ),
        },

        "event_received_at": event_time,
        "recorded_at": recorded_at,

        "processing_delay_ms": (
            processing_delay_ms
        ),

        "market": market,

        "event": {
            "source": event["source"],
            "type": event["type"],
            "data": event["data"],
        },

        # This state contains only events already processed at or before
        # this event's local received_at timestamp.
        "state_as_of_event": {
            "coinbase": state["coinbase"],

            "polymarket_lite": (
                state["polymarket_lite"]
            ),

            "polymarket_book": (
                state["polymarket_book"]
            ),

            "polymarket_trade": (
                state["polymarket_trade"]
            ),
        },

        "window": {
            "remaining_seconds": (
                seconds_remaining_at(
                    market,
                    event_time,
                )
            ),
        },
    }


# ---------------------------------------------------------------------
# PROCESS ONE ORDERED EVENT
# ---------------------------------------------------------------------

def process_event(
    file,
    market,
    state,
    event,
    sequence,
    session_id,
):
    """
    Causal processing rule:

        event
          ↓
        update recorder-owned state
          ↓
        save state as of that event
    """

    apply_event_to_state(
        state,
        event,
    )

    record = build_record(
        market=market,
        event=event,
        state=state,
        sequence=sequence,
        session_id=session_id,
    )

    write_record(
        file,
        record,
    )


# ---------------------------------------------------------------------
# HEAP / ORDERING HELPERS
# ---------------------------------------------------------------------

def push_event_to_heap(
    reorder_heap,
    event,
    heap_counter,
):
    """
    Push an event into the reorder heap.

    heap_counter breaks ties when two events have identical timestamps.
    """

    heap_counter += 1

    heapq.heappush(
        reorder_heap,
        (
            event_epoch(event),
            heap_counter,
            event,
        ),
    )

    return heap_counter


def release_ready_events(
    file,
    market,
    state,
    reorder_heap,
    event_count,
    session_id,
):
    """
    Release events that have remained in the reorder buffer long enough
    to be safely processed in timestamp order.
    """

    cutoff = (
        datetime.now(
            timezone.utc
        ).timestamp()
        - REORDER_WINDOW_SECONDS
    )

    while (
        reorder_heap
        and reorder_heap[0][0]
        <= cutoff
    ):
        (
            _,
            _,
            ordered_event,
        ) = heapq.heappop(
            reorder_heap
        )

        event_count += 1

        process_event(
            file=file,
            market=market,
            state=state,
            event=ordered_event,
            sequence=event_count,
            session_id=session_id,
        )

    return event_count


def flush_reorder_heap(
    file,
    market,
    state,
    reorder_heap,
    event_count,
    session_id,
):
    """
    Write every remaining buffered event in timestamp order.
    """

    while reorder_heap:
        (
            _,
            _,
            ordered_event,
        ) = heapq.heappop(
            reorder_heap
        )

        event_count += 1

        process_event(
            file=file,
            market=market,
            state=state,
            event=ordered_event,
            sequence=event_count,
            session_id=session_id,
        )

    return event_count


# ---------------------------------------------------------------------
# DRAIN PENDING EVENTS DURING SHUTDOWN
# ---------------------------------------------------------------------

def drain_pending_events(
    incoming_events,
    coinbase,
    polymarket,
    reorder_heap,
    heap_counter,
):
    """
    Collect events already sitting in any of our queues before shutdown.

    This minimizes data loss when Ctrl+C occurs during the 250 ms reorder
    window.
    """

    # Events already bridged into the unified asyncio queue.
    while True:
        try:
            event = (
                incoming_events
                .get_nowait()
            )

        except asyncio.QueueEmpty:
            break

        heap_counter = push_event_to_heap(
            reorder_heap,
            event,
            heap_counter,
        )

    # Coinbase ticks that reached its callback queue but were not yet
    # bridged into incoming_events.
    while True:
        try:
            tick = (
                coinbase.events
                .get_nowait()
            )

        except thread_queue.Empty:
            break

        event = {
            "source": "coinbase",
            "type": "coinbase_tick",
            "data": tick,
        }

        heap_counter = push_event_to_heap(
            reorder_heap,
            event,
            heap_counter,
        )

    # Polymarket events received by its feed but not yet bridged.
    while True:
        try:
            pm_event = (
                polymarket.events
                .get_nowait()
            )

        except asyncio.QueueEmpty:
            break

        event = {
            "source": "polymarket",
            "type": pm_event["type"],
            "data": pm_event["data"],
        }

        heap_counter = push_event_to_heap(
            reorder_heap,
            event,
            heap_counter,
        )

    return heap_counter


# ---------------------------------------------------------------------
# STATUS DISPLAY
# ---------------------------------------------------------------------

def print_status(
    market,
    state,
    event_count,
    buffered_count,
):
    btc = state["coinbase"]
    lite = state["polymarket_lite"]

    if btc is None:
        btc_price = "WAITING"

    else:
        btc_price = str(
            btc.price
        )

    if lite is None:
        bid = "WAITING"
        ask = "WAITING"

    else:
        bid = (
            str(lite.best_bid)
            if lite.best_bid is not None
            else "None"
        )

        ask = (
            str(lite.best_ask)
            if lite.best_ask is not None
            else "None"
        )

    remaining = (
        current_seconds_remaining(
            market
        )
    )

    print(
        f"{datetime.now(timezone.utc).isoformat()} | "
        f"events={event_count} | "
        f"buffered={buffered_count} | "
        f"BTC=${btc_price} | "
        f"UP={bid}/{ask} | "
        f"T-{remaining:.1f}s"
    )


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

async def main():
    print(
        "=" * 78
    )

    print(
        "POLYARB REAL-TIME RECORDER v0.3"
    )

    print(
        "Causally Ordered Coinbase + Polymarket Timeline"
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
            "Missing Polymarket environment credentials."
        )

        return False

    # -----------------------------------------------------------------
    # MARKET
    # -----------------------------------------------------------------

    market = get_market_metadata()

    if market is None:
        print(
            "Current BTC 15-minute market "
            "is not available."
        )

        return False

    session_id = create_session_id()
    path = output_path(
        session_id
    )

    print()

    print(
        f"Session:       {session_id}"
    )

    print(
        f"Market:        {market['slug']}"
    )

    print(
        f"Index:         {market['index']}"
    )

    print(
        f"Price to beat: "
        f"${market['price_to_beat']}"
    )

    print(
        f"Window:        "
        f"{market['window_start']} "
        f"-> "
        f"{market['window_end']}"
    )

    print(
        f"Reorder buffer: "
        f"{REORDER_WINDOW_SECONDS * 1000:.0f} ms"
    )

    print()

    # -----------------------------------------------------------------
    # FEEDS / STATE
    # -----------------------------------------------------------------

    coinbase = CoinbaseBTCFeed()

    polymarket = (
        PolymarketMarketFeed(
            key_id=key_id,
            secret_key=secret_key,
            market_slug=market["slug"],
        )
    )

    incoming_events = asyncio.Queue(
        maxsize=20000
    )

    causal_state = empty_causal_state()

    reorder_heap = []

    heap_counter = 0
    event_count = 0

    coinbase_task = None
    polymarket_task = None

    coinbase_started = False
    polymarket_started = False

    last_status = time.monotonic()

    shutdown_requested = False

    try:
        # -------------------------------------------------------------
        # START COINBASE
        # -------------------------------------------------------------

        print(
            "Starting Coinbase WebSocket..."
        )

        coinbase.start()
        coinbase_started = True

        started = time.monotonic()

        while (
            coinbase.latest() is None
            and (
                time.monotonic()
                - started
            ) < 5
        ):
            await asyncio.sleep(
                0.05
            )

        if coinbase.latest():
            print(
                f"Coinbase ready: "
                f"BTC-USD "
                f"${coinbase.latest().price}"
            )

        else:
            print(
                "Coinbase connected, but no "
                "tick has arrived yet."
            )

        # -------------------------------------------------------------
        # START POLYMARKET
        # -------------------------------------------------------------

        print(
            "Starting Polymarket WebSocket..."
        )

        await polymarket.start()
        polymarket_started = True

        print(
            "Polymarket feed ready."
        )

        print()

        print(
            "Recording to:"
        )

        print(
            path.resolve()
        )

        print()

        print(
            "Press Ctrl+C to stop."
        )

        print()

        # -------------------------------------------------------------
        # START EVENT BRIDGES
        # -------------------------------------------------------------

        coinbase_task = asyncio.create_task(
            consume_coinbase(
                coinbase,
                incoming_events,
            )
        )

        polymarket_task = asyncio.create_task(
            consume_polymarket(
                polymarket,
                incoming_events,
            )
        )

        # -------------------------------------------------------------
        # RECORD EVENT STREAM
        # -------------------------------------------------------------

        with path.open(
            "w",
            encoding="utf-8",
            buffering=1,
        ) as file:

            try:
                while True:
                    if (
                        current_seconds_remaining(
                            market
                        )
                        <= 0
                    ):
                        print()
                        print(
                            "Market window ended."
                        )
                        break

                    # ---------------------------------------------
                    # RECEIVE NEW EVENT
                    # ---------------------------------------------

                    try:
                        event = (
                            await asyncio.wait_for(
                                incoming_events.get(),
                                timeout=0.05,
                            )
                        )

                        heap_counter = (
                            push_event_to_heap(
                                reorder_heap,
                                event,
                                heap_counter,
                            )
                        )

                    except asyncio.TimeoutError:
                        pass

                    # ---------------------------------------------
                    # RELEASE ORDERED EVENTS
                    # ---------------------------------------------

                    event_count = (
                        release_ready_events(
                            file=file,
                            market=market,
                            state=causal_state,
                            reorder_heap=reorder_heap,
                            event_count=event_count,
                            session_id=session_id,
                        )
                    )

                    # ---------------------------------------------
                    # STATUS ONCE PER SECOND
                    # ---------------------------------------------

                    now_monotonic = (
                        time.monotonic()
                    )

                    if (
                        now_monotonic
                        - last_status
                        >= 1
                    ):
                        print_status(
                            market=market,
                            state=causal_state,
                            event_count=event_count,
                            buffered_count=len(
                                reorder_heap
                            ),
                        )

                        last_status = (
                            now_monotonic
                        )

            except asyncio.CancelledError:
                shutdown_requested = True

                print()
                print(
                    "Shutdown requested; "
                    "flushing pending events..."
                )

            finally:
                # ---------------------------------------------
                # STOP EVENT BRIDGES FIRST
                # ---------------------------------------------

                tasks = [
                    task
                    for task in (
                        coinbase_task,
                        polymarket_task,
                    )
                    if task is not None
                ]

                for task in tasks:
                    task.cancel()

                if tasks:
                    await asyncio.gather(
                        *tasks,
                        return_exceptions=True,
                    )

                coinbase_task = None
                polymarket_task = None

                # ---------------------------------------------
                # COLLECT EVENTS ALREADY RECEIVED
                # ---------------------------------------------

                heap_counter = (
                    drain_pending_events(
                        incoming_events=(
                            incoming_events
                        ),
                        coinbase=coinbase,
                        polymarket=polymarket,
                        reorder_heap=(
                            reorder_heap
                        ),
                        heap_counter=(
                            heap_counter
                        ),
                    )
                )

                # ---------------------------------------------
                # FLUSH REMAINING EVENTS IN TIMESTAMP ORDER
                # ---------------------------------------------

                buffered_before_flush = (
                    len(reorder_heap)
                )

                event_count = (
                    flush_reorder_heap(
                        file=file,
                        market=market,
                        state=causal_state,
                        reorder_heap=(
                            reorder_heap
                        ),
                        event_count=(
                            event_count
                        ),
                        session_id=(
                            session_id
                        ),
                    )
                )

                file.flush()

                if buffered_before_flush:
                    print(
                        f"Flushed "
                        f"{buffered_before_flush} "
                        f"pending events."
                    )

    finally:
        # -----------------------------------------------------------------
        # SAFETY: CANCEL BRIDGES IF STARTUP/LOOP FAILED EARLY
        # -----------------------------------------------------------------

        tasks = [
            task
            for task in (
                coinbase_task,
                polymarket_task,
            )
            if task is not None
        ]

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        # -----------------------------------------------------------------
        # STOP FEEDS
        # -----------------------------------------------------------------

        if coinbase_started:
            coinbase.stop()

        if polymarket_started:
            await polymarket.stop()

        print()

        print(
            f"Recorded ordered events: "
            f"{event_count}"
        )

        print(
            f"Data file: "
            f"{path.resolve()}"
        )

        print(
            "Feeds stopped."
        )

    return shutdown_requested


# ---------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    try:
        stopped_by_user = asyncio.run(
            main()
        )

        if stopped_by_user:
            print(
                "\nStopped by user."
            )

    except KeyboardInterrupt:
        print(
            "\nStopped immediately by user."
        )