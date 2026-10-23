import argparse
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

REORDER_WINDOW_SECONDS = 0.250
DISCOVERY_RETRY_SECONDS = 1.0
RECORDER_VERSION = "continuous-0.2"


# ---------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------

def json_default(value):
    if isinstance(value, Decimal):
        return str(value)

    if is_dataclass(value):
        return asdict(value)

    raise TypeError(
        f"Cannot serialize {type(value).__name__}"
    )


def write_record(file, record):
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
# TIME
# ---------------------------------------------------------------------

def parse_timestamp(timestamp):
    return datetime.fromisoformat(
        timestamp.replace(
            "Z",
            "+00:00",
        )
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


def event_received_at(event):
    data = event.get(
        "data"
    )

    received_at = getattr(
        data,
        "received_at",
        None,
    )

    if received_at:
        return received_at

    return datetime.now(
        timezone.utc
    ).isoformat()


def event_epoch(event):
    return parse_timestamp(
        event_received_at(event)
    ).timestamp()


def current_seconds_remaining(market):
    end = parse_timestamp(
        market["window_end"]
    )

    remaining = (
        end
        - datetime.now(
            timezone.utc
        )
    ).total_seconds()

    return max(
        0.0,
        remaining,
    )


def seconds_remaining_at(
    market,
    timestamp,
):
    end = parse_timestamp(
        market["window_end"]
    )

    event_time = parse_timestamp(
        timestamp
    )

    return max(
        0.0,
        (
            end - event_time
        ).total_seconds(),
    )


# ---------------------------------------------------------------------
# MARKET DISCOVERY
# ---------------------------------------------------------------------

def get_current_market_metadata():
    slug = current_btc_15m_slug()

    client = PolymarketUS()

    try:
        response = (
            client
            .markets
            .retrieve_by_slug(
                slug
            )
        )

    except NotFoundError:
        return None

    except Exception as exc:
        print(
            f"Market discovery error: {exc}"
        )

        return None

    finally:
        try:
            client.close()

        except Exception:
            pass

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

    window_start = terms.get(
        "windowStart"
    )

    window_end = terms.get(
        "windowEnd"
    )

    if (
        not market.get("slug")
        or not window_start
        or not window_end
    ):
        return None

    return {
        "slug": market.get("slug"),
        "title": market.get("title"),
        "index": terms.get(
            "indexSymbol"
        ),
        "horizon": terms.get(
            "horizon"
        ),
        "market_type": terms.get(
            "marketType"
        ),

        "price_to_beat": (
            terms.get(
                "priceToBeat"
            )
            or {}
        ).get(
            "value"
        ),

        "window_start": window_start,
        "window_end": window_end,
    }


async def wait_for_current_market():
    last_slug = None

    while True:
        slug = current_btc_15m_slug()

        if slug != last_slug:
            print()
            print(
                f"Looking for market: {slug}"
            )

            last_slug = slug

        market = (
            get_current_market_metadata()
        )

        if market is not None:
            if (
                current_seconds_remaining(
                    market
                )
                > 0
            ):
                return market

        await asyncio.sleep(
            DISCOVERY_RETRY_SECONDS
        )


# ---------------------------------------------------------------------
# RECORDING FILE
# ---------------------------------------------------------------------

def create_session_id():
    return datetime.now(
        timezone.utc
    ).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )


def output_path(
    market,
    session_id,
):
    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    market_suffix = (
        market["slug"]
        .replace(
            "cpc-btc-updown-15m-",
            "",
        )
    )

    return (
        DATA_DIR
        / (
            "realtime_causal_btc_15m_"
            f"{market_suffix}_"
            f"{session_id}.jsonl"
        )
    )


# ---------------------------------------------------------------------
# FEED BRIDGES
# ---------------------------------------------------------------------

async def consume_coinbase(
    coinbase,
    incoming_events,
):
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


async def consume_polymarket(
    polymarket,
    incoming_events,
):
    while True:
        event = (
            await polymarket.events.get()
        )

        await incoming_events.put(
            {
                "source": "polymarket",
                "type": event["type"],
                "data": event["data"],
            }
        )


# ---------------------------------------------------------------------
# CAUSAL STATE
# ---------------------------------------------------------------------

def empty_causal_state():
    return {
        "coinbase": None,
        "polymarket_lite": None,
        "polymarket_book": None,
        "polymarket_trade": None,
    }

def compact_polymarket_book_state(
    book,
):
    """
    Compact causal state representation.

    The complete px/qty ladder remains in the polymarket_book EVENT.

    We intentionally do NOT copy the full ladder into
    state_as_of_event for every subsequent Coinbase/LITE/trade event.
    """

    return {
        "market_slug": getattr(
            book,
            "market_slug",
            None,
        ),

        "best_bid": getattr(
            book,
            "best_bid",
            None,
        ),

        "best_ask": getattr(
            book,
            "best_ask",
            None,
        ),

        "book_timestamp": getattr(
            book,
            "book_timestamp",
            None,
        ),

        "market_state": getattr(
            book,
            "market_state",
            None,
        ),

        "bid_levels": getattr(
            book,
            "bid_levels",
            None,
        ),

        "offer_levels": getattr(
            book,
            "offer_levels",
            None,
        ),

        "received_at": getattr(
            book,
            "received_at",
            None,
        ),
    }

def apply_event_to_state(
    state,
    event,
):
    event_type = event[
        "type"
    ]

    data = event[
        "data"
    ]

    if event_type == "coinbase_tick":
        state["coinbase"] = data

    elif event_type == "polymarket_lite":
        state[
            "polymarket_lite"
        ] = data

    elif event_type == "polymarket_book":
        state[
            "polymarket_book"
        ] = compact_polymarket_book_state(
            data
        )
    elif event_type == "polymarket_trade":
        state[
            "polymarket_trade"
        ] = data


# ---------------------------------------------------------------------
# EVENT FILTERING
# ---------------------------------------------------------------------

def event_belongs_to_market(
    event,
    market,
):
    """
    Keep events whose local received_at timestamp falls within this
    market's window.

    Polymarket events are additionally checked against the market slug.
    """

    timestamp = parse_timestamp(
        event_received_at(event)
    )

    start = parse_timestamp(
        market["window_start"]
    )

    end = parse_timestamp(
        market["window_end"]
    )

    if (
        timestamp < start
        or timestamp > end
    ):
        return False

    if (
        event["source"]
        == "polymarket"
    ):
        data = event.get(
            "data"
        )

        event_slug = getattr(
            data,
            "market_slug",
            None,
        )

        if (
            event_slug is not None
            and event_slug
            != market["slug"]
        ):
            return False

    return True


# ---------------------------------------------------------------------
# RECORD BUILDING
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

    processing_delay_ms = round(
        (
            recorded_dt - event_dt
        ).total_seconds()
        * 1000,
        3,
    )

    return {
        "session_id": session_id,

        "sequence": sequence,

        "recorder": {
            "version": (
                RECORDER_VERSION
            ),

            "reorder_window_ms": (
                REORDER_WINDOW_SECONDS
                * 1000
            ),
        },

        "event_received_at": (
            event_time
        ),

        "recorded_at": (
            recorded_dt.isoformat()
        ),

        "processing_delay_ms": (
            processing_delay_ms
        ),

        "market": market,

        "event": {
            "source": event[
                "source"
            ],

            "type": event[
                "type"
            ],

            "data": event[
                "data"
            ],
        },

        "state_as_of_event": {
            "coinbase": (
                state["coinbase"]
            ),

            "polymarket_lite": (
                state[
                    "polymarket_lite"
                ]
            ),

            "polymarket_book": (
                state[
                    "polymarket_book"
                ]
            ),

            "polymarket_trade": (
                state[
                    "polymarket_trade"
                ]
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


def process_event(
    file,
    market,
    state,
    event,
    sequence,
    session_id,
):
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
# REORDER BUFFER
# ---------------------------------------------------------------------

def push_event_to_heap(
    reorder_heap,
    event,
    heap_counter,
):
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


def flush_heap(
    file,
    market,
    state,
    reorder_heap,
    event_count,
    session_id,
):
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
# SHUTDOWN DRAIN
# ---------------------------------------------------------------------

def drain_pending_events(
    incoming_events,
    coinbase,
    polymarket,
    reorder_heap,
    heap_counter,
    market,
):
    """
    Pull anything already received out of all three queues.

    Events outside the current market window are ignored.
    """

    while True:
        try:
            event = (
                incoming_events
                .get_nowait()
            )

        except asyncio.QueueEmpty:
            break

        if event_belongs_to_market(
            event,
            market,
        ):
            heap_counter = (
                push_event_to_heap(
                    reorder_heap,
                    event,
                    heap_counter,
                )
            )

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

        if event_belongs_to_market(
            event,
            market,
        ):
            heap_counter = (
                push_event_to_heap(
                    reorder_heap,
                    event,
                    heap_counter,
                )
            )

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

        if event_belongs_to_market(
            event,
            market,
        ):
            heap_counter = (
                push_event_to_heap(
                    reorder_heap,
                    event,
                    heap_counter,
                )
            )

    return heap_counter


# ---------------------------------------------------------------------
# STATUS
# ---------------------------------------------------------------------

def print_status(
    market,
    state,
    event_count,
    buffered_count,
):
    btc = state["coinbase"]

    lite = state[
        "polymarket_lite"
    ]

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
            if lite.best_bid
            is not None
            else "None"
        )

        ask = (
            str(lite.best_ask)
            if lite.best_ask
            is not None
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
# RECORD ONE MARKET
# ---------------------------------------------------------------------

async def record_market(
    market,
    key_id,
    secret_key,
    market_number,
):
    session_id = (
        create_session_id()
    )

    path = output_path(
        market,
        session_id,
    )

    print()
    print(
        "=" * 78
    )

    print(
        f"MARKET #{market_number}"
    )

    print(
        "=" * 78
    )

    print(
        f"Market:        "
        f"{market['slug']}"
    )

    print(
        f"Index:         "
        f"{market['index']}"
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
        f"Remaining:     "
        f"{current_seconds_remaining(market):.1f}s"
    )

    print(
        f"Reorder:       "
        f"{REORDER_WINDOW_SECONDS * 1000:.0f} ms"
    )

    print()

    coinbase = (
        CoinbaseBTCFeed()
    )

    polymarket = (
        PolymarketMarketFeed(
            key_id=key_id,
            secret_key=secret_key,
            market_slug=market[
                "slug"
            ],
        )
    )

    incoming_events = (
        asyncio.Queue(
            maxsize=20000
        )
    )

    causal_state = (
        empty_causal_state()
    )

    reorder_heap = []

    heap_counter = 0
    event_count = 0

    coinbase_task = None
    polymarket_task = None

    coinbase_started = False
    polymarket_started = False

    interrupted = False

    last_status = (
        time.monotonic()
    )

    try:
        # -------------------------------------------------------------
        # START COINBASE
        # -------------------------------------------------------------

        print(
            "Starting Coinbase..."
        )

        coinbase.start()

        coinbase_started = True

        started = (
            time.monotonic()
        )

        while (
            coinbase.latest()
            is None
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
                f"${coinbase.latest().price}"
            )

        else:
            print(
                "Coinbase connected; "
                "waiting for first tick."
            )

        # -------------------------------------------------------------
        # START POLYMARKET
        # -------------------------------------------------------------

        print(
            "Starting Polymarket..."
        )

        await polymarket.start()

        polymarket_started = True

        print(
            "Polymarket ready."
        )

        print()

        print(
            f"Recording: "
            f"{path.resolve()}"
        )

        print()

        # -------------------------------------------------------------
        # BRIDGE TASKS
        # -------------------------------------------------------------

        coinbase_task = (
            asyncio.create_task(
                consume_coinbase(
                    coinbase,
                    incoming_events,
                )
            )
        )

        polymarket_task = (
            asyncio.create_task(
                consume_polymarket(
                    polymarket,
                    incoming_events,
                )
            )
        )

        # -------------------------------------------------------------
        # RECORD
        # -------------------------------------------------------------

        with path.open(
            "w",
            encoding="utf-8",
            buffering=1,
        ) as file:

            try:
                while (
                    current_seconds_remaining(
                        market
                    )
                    > 0
                ):
                    try:
                        event = (
                            await asyncio.wait_for(
                                incoming_events.get(),
                                timeout=0.05,
                            )
                        )

                        if event_belongs_to_market(
                            event,
                            market,
                        ):
                            heap_counter = (
                                push_event_to_heap(
                                    reorder_heap,
                                    event,
                                    heap_counter,
                                )
                            )

                    except asyncio.TimeoutError:
                        pass

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

                print()
                print(
                    "Market window ended."
                )

            except asyncio.CancelledError:
                interrupted = True

                print()
                print(
                    "Shutdown requested; "
                    "flushing pending events..."
                )

            finally:
                # -----------------------------------------------------
                # STOP BRIDGE TASKS
                # -----------------------------------------------------

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

                # -----------------------------------------------------
                # DRAIN RECEIVED EVENTS
                # -----------------------------------------------------

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
                        market=market,
                    )
                )

                pending = len(
                    reorder_heap
                )

                event_count = (
                    flush_heap(
                        file=file,
                        market=market,
                        state=causal_state,
                        reorder_heap=reorder_heap,
                        event_count=event_count,
                        session_id=session_id,
                    )
                )

                file.flush()

                if pending:
                    print(
                        f"Flushed "
                        f"{pending} "
                        f"pending events."
                    )

    finally:
        # -------------------------------------------------------------
        # SAFETY CLEANUP
        # -------------------------------------------------------------

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

        if coinbase_started:
            coinbase.stop()

        if polymarket_started:
            await polymarket.stop()

    print()

    print(
        f"Completed market: "
        f"{market['slug']}"
    )

    print(
        f"Ordered events:  "
        f"{event_count}"
    )

    print(
        f"Saved:           "
        f"{path.resolve()}"
    )

    return interrupted


# ---------------------------------------------------------------------
# CONTINUOUS MAIN LOOP
# ---------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(
        description=(
            "Continuously record successive "
            "Polymarket BTC 15-minute markets."
        )
    )

    parser.add_argument(
        "--max-markets",
        type=int,
        default=0,
        help=(
            "Stop after N markets. "
            "0 means run until Ctrl+C."
        ),
    )

    args = parser.parse_args()

    if args.max_markets < 0:
        print(
            "--max-markets cannot be negative."
        )

        return False

    key_id = os.environ.get(
        "POLYMARKET_KEY_ID"
    )

    secret_key = os.environ.get(
        "POLYMARKET_SECRET_KEY"
    )

    if (
        not key_id
        or not secret_key
    ):
        print(
            "Missing Polymarket environment credentials."
        )

        return False

    print(
        "=" * 78
    )

    print(
        "POLYARB CONTINUOUS RESEARCH RECORDER"
    )

    print(
        "BTC 15-Minute Markets"
    )

    print(
        "=" * 78
    )

    print(
        "Mode: READ-ONLY RESEARCH"
    )

    print(
        f"Reorder buffer: "
        f"{REORDER_WINDOW_SECONDS * 1000:.0f} ms"
    )

    if args.max_markets:
        print(
            f"Maximum markets: "
            f"{args.max_markets}"
        )

    else:
        print(
            "Maximum markets: unlimited"
        )

    print()

    print(
        "Press Ctrl+C to stop."
    )

    market_count = 0

    try:
        while True:
            market = (
                await wait_for_current_market()
            )

            market_count += 1

            interrupted = (
                await record_market(
                    market=market,
                    key_id=key_id,
                    secret_key=secret_key,
                    market_number=market_count,
                )
            )

            if interrupted:
                print()
                print(
                    "Continuous recorder stopped "
                    "by user."
                )

                return True

            if (
                args.max_markets
                and market_count
                >= args.max_markets
            ):
                print()
                print(
                    "Requested market limit reached."
                )

                return False

            print()
            print(
                "Rolling to next "
                "15-minute market..."
            )

            await asyncio.sleep(
                0.25
            )

    except asyncio.CancelledError:
        print()
        print(
            "Continuous recorder shutdown requested."
        )

        return True


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