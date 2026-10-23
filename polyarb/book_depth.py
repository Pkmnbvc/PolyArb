from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


DEPTH_FEATURE_VERSION = "0.1"

ONE_CENT = Decimal("0.01")
TWO_CENTS = Decimal("0.02")
FIVE_CENTS = Decimal("0.05")


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True)
class DepthSnapshot:
    version: str

    bid_level_count: int
    ask_level_count: int

    best_bid_price: Decimal | None
    best_bid_quantity: Decimal | None

    best_ask_price: Decimal | None
    best_ask_quantity: Decimal | None

    spread: Decimal | None
    midpoint: Decimal | None
    microprice_top1: Decimal | None

    bid_depth_top1: Decimal
    ask_depth_top1: Decimal

    bid_depth_top5: Decimal
    ask_depth_top5: Decimal

    bid_depth_top10: Decimal
    ask_depth_top10: Decimal

    obi_top1: Decimal | None
    obi_top5: Decimal | None
    obi_top10: Decimal | None

    bid_depth_within_1c: Decimal
    ask_depth_within_1c: Decimal
    obi_within_1c: Decimal | None

    bid_depth_within_2c: Decimal
    ask_depth_within_2c: Decimal
    obi_within_2c: Decimal | None

    bid_depth_within_5c: Decimal
    ask_depth_within_5c: Decimal
    obi_within_5c: Decimal | None

    bid_top1_share_of_top5: Decimal | None
    ask_top1_share_of_top5: Decimal | None


def _decimal(value) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid decimal value: {value!r}"
        ) from exc

    if not result.is_finite():
        raise ValueError(
            f"Non-finite decimal value: {value!r}"
        )

    return result


def parse_sdk_level(
    raw: dict,
) -> BookLevel:
    """
    Parse current Polymarket US book level:

        {
            "px": {
                "value": "0.7200",
                "currency": "USD"
            },
            "qty": "2666.0100"
        }

    Fail loudly on schema drift rather than silently guessing.
    """

    if not isinstance(raw, dict):
        raise ValueError(
            f"Book level must be dict, got {type(raw).__name__}"
        )

    px = raw.get("px")

    if not isinstance(px, dict):
        raise ValueError(
            "Book level missing px object"
        )

    if "value" not in px:
        raise ValueError(
            "Book level px missing value"
        )

    if "qty" not in raw:
        raise ValueError(
            "Book level missing qty"
        )

    price = _decimal(
        px["value"]
    )

    quantity = _decimal(
        raw["qty"]
    )

    if price < 0:
        raise ValueError(
            "Book price cannot be negative"
        )

    if quantity < 0:
        raise ValueError(
            "Book quantity cannot be negative"
        )

    return BookLevel(
        price=price,
        quantity=quantity,
    )


def parse_sdk_book(
    bids_raw: list[dict],
    offers_raw: list[dict],
) -> tuple[
    tuple[BookLevel, ...],
    tuple[BookLevel, ...],
]:
    bids = tuple(
        sorted(
            (
                parse_sdk_level(level)
                for level in bids_raw
            ),
            key=lambda level: level.price,
            reverse=True,
        )
    )

    asks = tuple(
        sorted(
            (
                parse_sdk_level(level)
                for level in offers_raw
            ),
            key=lambda level: level.price,
        )
    )

    return bids, asks


def _sum_top(
    levels: tuple[BookLevel, ...],
    count: int,
) -> Decimal:
    return sum(
        (
            level.quantity
            for level in levels[:count]
        ),
        Decimal("0"),
    )


def _depth_within(
    levels: tuple[BookLevel, ...],
    width: Decimal,
    *,
    side: str,
) -> Decimal:
    if not levels:
        return Decimal("0")

    best = levels[0].price

    total = Decimal("0")

    for level in levels:
        if side == "bid":
            distance = (
                best
                - level.price
            )

        elif side == "ask":
            distance = (
                level.price
                - best
            )

        else:
            raise ValueError(
                f"Unknown side: {side}"
            )

        if distance <= width:
            total += level.quantity

    return total


def _obi(
    bid_depth: Decimal,
    ask_depth: Decimal,
) -> Decimal | None:
    total = (
        bid_depth
        + ask_depth
    )

    if total == 0:
        return None

    return (
        bid_depth
        - ask_depth
    ) / total


def _share(
    numerator: Decimal,
    denominator: Decimal,
) -> Decimal | None:
    if denominator == 0:
        return None

    return (
        numerator
        / denominator
    )


def summarize_book(
    bids: tuple[BookLevel, ...],
    asks: tuple[BookLevel, ...],
) -> DepthSnapshot:

    best_bid = (
        bids[0]
        if bids
        else None
    )

    best_ask = (
        asks[0]
        if asks
        else None
    )

    spread = None
    midpoint = None
    microprice = None

    if (
        best_bid is not None
        and best_ask is not None
    ):
        spread = (
            best_ask.price
            - best_bid.price
        )

        midpoint = (
            best_bid.price
            + best_ask.price
        ) / Decimal("2")

        top_total_qty = (
            best_bid.quantity
            + best_ask.quantity
        )

        if top_total_qty > 0:
            # Standard top-of-book microprice:
            # more bid quantity pulls fair price toward ask,
            # more ask quantity pulls it toward bid.
            microprice = (
                (
                    best_ask.price
                    * best_bid.quantity
                )
                +
                (
                    best_bid.price
                    * best_ask.quantity
                )
            ) / top_total_qty

    bid_top1 = _sum_top(
        bids,
        1,
    )

    ask_top1 = _sum_top(
        asks,
        1,
    )

    bid_top5 = _sum_top(
        bids,
        5,
    )

    ask_top5 = _sum_top(
        asks,
        5,
    )

    bid_top10 = _sum_top(
        bids,
        10,
    )

    ask_top10 = _sum_top(
        asks,
        10,
    )

    bid_1c = _depth_within(
        bids,
        ONE_CENT,
        side="bid",
    )

    ask_1c = _depth_within(
        asks,
        ONE_CENT,
        side="ask",
    )

    bid_2c = _depth_within(
        bids,
        TWO_CENTS,
        side="bid",
    )

    ask_2c = _depth_within(
        asks,
        TWO_CENTS,
        side="ask",
    )

    bid_5c = _depth_within(
        bids,
        FIVE_CENTS,
        side="bid",
    )

    ask_5c = _depth_within(
        asks,
        FIVE_CENTS,
        side="ask",
    )

    return DepthSnapshot(
        version=DEPTH_FEATURE_VERSION,

        bid_level_count=len(bids),
        ask_level_count=len(asks),

        best_bid_price=(
            best_bid.price
            if best_bid is not None
            else None
        ),

        best_bid_quantity=(
            best_bid.quantity
            if best_bid is not None
            else None
        ),

        best_ask_price=(
            best_ask.price
            if best_ask is not None
            else None
        ),

        best_ask_quantity=(
            best_ask.quantity
            if best_ask is not None
            else None
        ),

        spread=spread,
        midpoint=midpoint,
        microprice_top1=microprice,

        bid_depth_top1=bid_top1,
        ask_depth_top1=ask_top1,

        bid_depth_top5=bid_top5,
        ask_depth_top5=ask_top5,

        bid_depth_top10=bid_top10,
        ask_depth_top10=ask_top10,

        obi_top1=_obi(
            bid_top1,
            ask_top1,
        ),

        obi_top5=_obi(
            bid_top5,
            ask_top5,
        ),

        obi_top10=_obi(
            bid_top10,
            ask_top10,
        ),

        bid_depth_within_1c=bid_1c,
        ask_depth_within_1c=ask_1c,

        obi_within_1c=_obi(
            bid_1c,
            ask_1c,
        ),

        bid_depth_within_2c=bid_2c,
        ask_depth_within_2c=ask_2c,

        obi_within_2c=_obi(
            bid_2c,
            ask_2c,
        ),

        bid_depth_within_5c=bid_5c,
        ask_depth_within_5c=ask_5c,

        obi_within_5c=_obi(
            bid_5c,
            ask_5c,
        ),

        bid_top1_share_of_top5=_share(
            bid_top1,
            bid_top5,
        ),

        ask_top1_share_of_top5=_share(
            ask_top1,
            ask_top5,
        ),
    )


def summarize_sdk_market_data(
    market_data: dict,
) -> DepthSnapshot:
    bids, asks = parse_sdk_book(
        market_data.get(
            "bids",
            [],
        ),
        market_data.get(
            "offers",
            [],
        ),
    )

    return summarize_book(
        bids,
        asks,
    )