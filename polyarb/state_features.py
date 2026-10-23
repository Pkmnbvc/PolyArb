from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
import math
from typing import Deque

from polyarb.book_depth import (
    summarize_sdk_market_data,
)


STATE_FEATURE_VERSION = "0.1"

DEFAULT_BTC_IMPULSE_THRESHOLD = 1.00
MAX_HISTORY_SECONDS = 60.0

EPSILON = 1e-12


class CausalOrderError(ValueError):
    """
    Raised when events are fed into the state engine out of local
    receive-time order.

    PolyArb's state engine is intentionally causal. Future events must
    never be allowed to mutate state belonging to an earlier timestamp.
    """


@dataclass(frozen=True)
class _BTCImpulse:
    timestamp: float
    direction: int
    magnitude: float


@dataclass(frozen=True)
class _PMRepricing:
    timestamp: float
    direction: int


@dataclass(frozen=True)
class _QuoteSegment:
    start: float
    end: float
    two_sided: bool
    spread: float | None


@dataclass(frozen=True)
class StateSnapshot:
    """
    Causally observable PolyArb state at timestamp.

    IMPORTANT
    ---------
    `pm_level_count_imbalance` is based ONLY on the number of populated
    bid/ask price levels.

    It is NOT:
        - volume OBI
        - queue depth
        - executable liquidity
        - order-flow imbalance
        - inventory pressure
    """

    version: str
    timestamp: float

    # ----------------------------------------------------------
    # Data freshness
    # ----------------------------------------------------------

    btc_tick_age_ms: float | None
    pm_quote_age_ms: float | None
    pm_book_age_ms: float | None

    # ----------------------------------------------------------
    # Coinbase state
    # ----------------------------------------------------------

    btc_price: float | None

    btc_last_impulse_direction: int | None
    btc_last_impulse_magnitude: float | None
    btc_previous_inter_impulse_ms: float | None

    btc_impulse_rate_1s: float
    btc_impulse_rate_5s: float
    btc_impulse_rate_10s: float

    btc_signed_imbalance_1s: float | None
    btc_signed_imbalance_5s: float | None
    btc_signed_imbalance_10s: float | None

    btc_same_sign_continuation_5s: float | None
    btc_same_sign_continuation_10s: float | None

    btc_current_same_sign_run: int | None

    # ----------------------------------------------------------
    # Polymarket quote state
    # ----------------------------------------------------------

    pm_best_bid: float | None
    pm_best_ask: float | None
    pm_current_two_sided: bool

    pm_spread: float | None

    pm_spread_median_5s: float | None
    pm_spread_p90_10s: float | None

    pm_two_sided_fraction_1s: float | None
    pm_two_sided_fraction_5s: float | None
    pm_two_sided_fraction_10s: float | None

    # ----------------------------------------------------------
    # Polymarket repricing state
    # ----------------------------------------------------------

    pm_repricing_rate_1s: float
    pm_repricing_rate_5s: float
    pm_repricing_rate_10s: float

    pm_signed_move_imbalance_5s: float | None
    pm_signed_move_imbalance_10s: float | None

    # ----------------------------------------------------------
    # Polymarket level-count structure
    #
    # These are occupancy descriptors only.
    # ----------------------------------------------------------

    pm_bid_level_count: int | None
    pm_ask_level_count: int | None

    pm_level_count_imbalance: float | None
    pm_abs_level_count_imbalance: float | None

    # ----------------------------------------------------------
    # Future depth-capable fields.
    #
    # Current recorder cannot establish these.
    # They remain explicitly UNKNOWN rather than inferred.
    # ----------------------------------------------------------

    # ----------------------------------------------------------
    # Quantity-aware book state.
    #
    # These fields are populated ONLY when actual price/quantity
    # ladder data was observed.
    # ----------------------------------------------------------

    pm_depth_observed: bool

    pm_best_bid_quantity: float | None
    pm_best_ask_quantity: float | None

    pm_bid_depth_top5: float | None
    pm_ask_depth_top5: float | None

    pm_bid_depth_top10: float | None
    pm_ask_depth_top10: float | None

    pm_obi_top1: float | None
    pm_obi_top5: float | None
    pm_obi_top10: float | None

    pm_bid_depth_within_1c: float | None
    pm_ask_depth_within_1c: float | None
    pm_obi_within_1c: float | None

    pm_bid_depth_within_2c: float | None
    pm_ask_depth_within_2c: float | None
    pm_obi_within_2c: float | None

    pm_bid_depth_within_5c: float | None
    pm_ask_depth_within_5c: float | None
    pm_obi_within_5c: float | None

    pm_microprice_top1: float | None

    pm_bid_top1_share_of_top5: float | None
    pm_ask_top1_share_of_top5: float | None

    # ----------------------------------------------------------
    # Market context
    # ----------------------------------------------------------

    seconds_remaining: float | None

    # This is explicitly Coinbase-vs-priceToBeat distance.
    # It must NOT be interpreted as exact BRTI settlement distance.
    coinbase_distance_to_price_to_beat_usd: float | None
    coinbase_distance_to_price_to_beat_pct: float | None

    def as_dict(self) -> dict:
        return asdict(self)


class StateFeatureEngine:
    """
    Online causal feature engine for PolyArb.

    Events MUST be submitted in non-decreasing LOCAL receive-time order.

    Typical use:

        engine = StateFeatureEngine()

        engine.set_market_context(
            window_end=...,
            price_to_beat=...,
        )

        engine.update_coinbase_tick(t, price)
        engine.update_pm_lite(t, bid, ask)
        engine.update_pm_book(t, bid_levels, ask_levels)

        state = engine.snapshot(t)

    The engine never attempts to explain WHY a state exists.
    It only describes what PolyArb actually observed.
    """

    def __init__(
        self,
        *,
        btc_impulse_threshold: float = DEFAULT_BTC_IMPULSE_THRESHOLD,
    ):
        if btc_impulse_threshold <= 0:
            raise ValueError(
                "btc_impulse_threshold must be positive"
            )

        self.btc_impulse_threshold = float(
            btc_impulse_threshold
        )

        self._last_global_timestamp: float | None = None

        # Highest causal time PolyArb has either ingested OR evaluated.
        # Once this watermark advances, older information cannot enter state.
        self._causal_watermark: float | None = None

        # Coinbase
        self._last_btc_tick_ts: float | None = None
        self._btc_price: float | None = None
        self._previous_btc_price: float | None = None

        self._btc_impulses: Deque[_BTCImpulse] = deque()

        # PM quote
        self._last_pm_quote_ts: float | None = None

        self._pm_best_bid: float | None = None
        self._pm_best_ask: float | None = None

        self._pm_current_two_sided = False
        self._pm_current_spread: float | None = None

        self._previous_pm_mid: float | None = None

        self._quote_segments: Deque[_QuoteSegment] = deque()

        # PM directional midpoint changes
        self._pm_repricings: Deque[_PMRepricing] = deque()

        # PM book level counts
        self._last_pm_book_ts: float | None = None

        self._pm_bid_level_count: int | None = None
        self._pm_ask_level_count: int | None = None

        # Market context
        self._window_end: float | None = None
        self._price_to_beat: float | None = None

                # Quantity-aware book state.
        #
        # Remains UNKNOWN until an actual px/qty ladder is observed.
        self._pm_depth_observed = False

        self._pm_best_bid_quantity: float | None = None
        self._pm_best_ask_quantity: float | None = None

        self._pm_bid_depth_top5: float | None = None
        self._pm_ask_depth_top5: float | None = None

        self._pm_bid_depth_top10: float | None = None
        self._pm_ask_depth_top10: float | None = None

        self._pm_obi_top1: float | None = None
        self._pm_obi_top5: float | None = None
        self._pm_obi_top10: float | None = None

        self._pm_bid_depth_within_1c: float | None = None
        self._pm_ask_depth_within_1c: float | None = None
        self._pm_obi_within_1c: float | None = None

        self._pm_bid_depth_within_2c: float | None = None
        self._pm_ask_depth_within_2c: float | None = None
        self._pm_obi_within_2c: float | None = None

        self._pm_bid_depth_within_5c: float | None = None
        self._pm_ask_depth_within_5c: float | None = None
        self._pm_obi_within_5c: float | None = None

        self._pm_microprice_top1: float | None = None

        self._pm_bid_top1_share_of_top5: float | None = None
        self._pm_ask_top1_share_of_top5: float | None = None
    # ==========================================================
    # PUBLIC INPUT API
    # ==========================================================

    def set_market_context(
        self,
        *,
        window_end: float | None = None,
        price_to_beat: float | None = None,
    ) -> None:
        """
        Set immutable-ish market metadata/context.

        window_end:
            Local epoch timestamp corresponding to market window end.

        price_to_beat:
            Official market threshold.

        Note:
            Coinbase price is only a proxy relative to this threshold.
            Settlement may use BRTI rather than Coinbase.
        """

        if window_end is not None:
            self._window_end = float(window_end)

        if price_to_beat is not None:
            self._price_to_beat = float(price_to_beat)

    def update_coinbase_tick(
        self,
        timestamp: float,
        price: float,
    ) -> bool:
        """
        Consume one Coinbase tick.

        Returns True if this tick created a >= threshold BTC impulse.
        """

        timestamp = float(timestamp)
        price = float(price)

        self._assert_causal(timestamp)

        self._last_btc_tick_ts = timestamp

        impulse_created = False

        if self._previous_btc_price is not None:
            delta = price - self._previous_btc_price

            if abs(delta) >= self.btc_impulse_threshold:
                direction = 1 if delta > 0 else -1

                self._btc_impulses.append(
                    _BTCImpulse(
                        timestamp=timestamp,
                        direction=direction,
                        magnitude=abs(delta),
                    )
                )

                impulse_created = True

        self._previous_btc_price = price
        self._btc_price = price

        self._purge(timestamp)

        return impulse_created

    def update_pm_lite(
        self,
        timestamp: float,
        best_bid: float | None,
        best_ask: float | None,
    ) -> int | None:
        """
        Consume one Polymarket LITE BBO update.

        Returns:
            +1 if this update creates an UP midpoint repricing
            -1 if it creates a DOWN midpoint repricing
            None otherwise

        One-sided observations reset the midpoint comparison baseline.
        We do not manufacture a midpoint across a one-sided gap.
        """

        timestamp = float(timestamp)

        bid = (
            float(best_bid)
            if best_bid is not None
            else None
        )

        ask = (
            float(best_ask)
            if best_ask is not None
            else None
        )

        self._assert_causal(timestamp)

        # Close the prior quote-state interval at this timestamp.
        self._close_quote_segment(timestamp)

        self._last_pm_quote_ts = timestamp

        self._pm_best_bid = bid
        self._pm_best_ask = ask

        two_sided = (
            bid is not None
            and ask is not None
        )

        self._pm_current_two_sided = two_sided

        if not two_sided:
            self._pm_current_spread = None
            self._previous_pm_mid = None

            self._purge(timestamp)

            return None

        spread = ask - bid

        self._pm_current_spread = spread

        current_mid = (bid + ask) / 2.0

        if self._previous_pm_mid is None:
            self._previous_pm_mid = current_mid

            self._purge(timestamp)

            return None

        delta_mid = current_mid - self._previous_pm_mid

        direction: int | None = None

        if delta_mid > EPSILON:
            direction = 1

        elif delta_mid < -EPSILON:
            direction = -1

        if direction is not None:
            self._pm_repricings.append(
                _PMRepricing(
                    timestamp=timestamp,
                    direction=direction,
                )
            )

        self._previous_pm_mid = current_mid

        self._purge(timestamp)

        return direction
    def update_pm_book(
        self,
        timestamp: float,
        *,
        bid_levels: int | None = None,
        ask_levels: int | None = None,
        offer_levels: int | None = None,
        bids_raw: list[dict] | None = None,
        offers_raw: list[dict] | None = None,
    ) -> None:
        """
        Consume a Polymarket BOOK event.

        Two valid modes exist:

        1. Historical/count-only mode:
               bid_levels / ask_levels

           This preserves compatibility with older PolyArb recordings.
           Quantity-derived features remain UNKNOWN.

        2. Quantity-aware mode:
               bids_raw / offers_raw

           Raw levels must contain actual px + qty observations.
           Scoped depth, OBI, concentration, and microprice are then
           calculated using polyarb.book_depth.

        We NEVER infer quantity from the number of populated levels.
        """

        timestamp = float(timestamp)

        self._assert_causal(timestamp)

        if ask_levels is None:
            ask_levels = offer_levels

        # ------------------------------------------------------
        # Quantity-aware event
        # ------------------------------------------------------

        has_bids_raw = bids_raw is not None
        has_offers_raw = offers_raw is not None

        if has_bids_raw != has_offers_raw:
            raise ValueError(
                "Quantity-aware PM book updates require "
                "both bids_raw and offers_raw."
            )

        if has_bids_raw and has_offers_raw:
            depth = summarize_sdk_market_data(
                {
                    "bids": bids_raw,
                    "offers": offers_raw,
                }
            )

            # If the event also supplied level counts, require them
            # to agree with the actual ladder.
            if (
                bid_levels is not None
                and int(bid_levels)
                != depth.bid_level_count
            ):
                raise ValueError(
                    "bid_levels disagrees with raw bid ladder"
                )

            if (
                ask_levels is not None
                and int(ask_levels)
                != depth.ask_level_count
            ):
                raise ValueError(
                    "ask_levels disagrees with raw ask ladder"
                )

            self._last_pm_book_ts = timestamp

            self._pm_bid_level_count = (
                depth.bid_level_count
            )

            self._pm_ask_level_count = (
                depth.ask_level_count
            )

            self._pm_depth_observed = True

            self._pm_best_bid_quantity = (
                float(depth.best_bid_quantity)
                if depth.best_bid_quantity is not None
                else None
            )

            self._pm_best_ask_quantity = (
                float(depth.best_ask_quantity)
                if depth.best_ask_quantity is not None
                else None
            )

            self._pm_bid_depth_top5 = float(
                depth.bid_depth_top5
            )

            self._pm_ask_depth_top5 = float(
                depth.ask_depth_top5
            )

            self._pm_bid_depth_top10 = float(
                depth.bid_depth_top10
            )

            self._pm_ask_depth_top10 = float(
                depth.ask_depth_top10
            )

            self._pm_obi_top1 = (
                float(depth.obi_top1)
                if depth.obi_top1 is not None
                else None
            )

            self._pm_obi_top5 = (
                float(depth.obi_top5)
                if depth.obi_top5 is not None
                else None
            )

            self._pm_obi_top10 = (
                float(depth.obi_top10)
                if depth.obi_top10 is not None
                else None
            )

            self._pm_bid_depth_within_1c = float(
                depth.bid_depth_within_1c
            )

            self._pm_ask_depth_within_1c = float(
                depth.ask_depth_within_1c
            )

            self._pm_obi_within_1c = (
                float(depth.obi_within_1c)
                if depth.obi_within_1c is not None
                else None
            )

            self._pm_bid_depth_within_2c = float(
                depth.bid_depth_within_2c
            )

            self._pm_ask_depth_within_2c = float(
                depth.ask_depth_within_2c
            )

            self._pm_obi_within_2c = (
                float(depth.obi_within_2c)
                if depth.obi_within_2c is not None
                else None
            )

            self._pm_bid_depth_within_5c = float(
                depth.bid_depth_within_5c
            )

            self._pm_ask_depth_within_5c = float(
                depth.ask_depth_within_5c
            )

            self._pm_obi_within_5c = (
                float(depth.obi_within_5c)
                if depth.obi_within_5c is not None
                else None
            )

            self._pm_microprice_top1 = (
                float(depth.microprice_top1)
                if depth.microprice_top1 is not None
                else None
            )

            self._pm_bid_top1_share_of_top5 = (
                float(
                    depth.bid_top1_share_of_top5
                )
                if depth.bid_top1_share_of_top5 is not None
                else None
            )

            self._pm_ask_top1_share_of_top5 = (
                float(
                    depth.ask_top1_share_of_top5
                )
                if depth.ask_top1_share_of_top5 is not None
                else None
            )

            self._purge(timestamp)
            return

        # ------------------------------------------------------
        # Historical/count-only event
        # ------------------------------------------------------

        if bid_levels is not None:
            bid_levels = int(
                bid_levels
            )

            if bid_levels < 0:
                raise ValueError(
                    "bid_levels cannot be negative"
                )

        if ask_levels is not None:
            ask_levels = int(
                ask_levels
            )

            if ask_levels < 0:
                raise ValueError(
                    "ask_levels cannot be negative"
                )

        self._last_pm_book_ts = timestamp

        self._pm_bid_level_count = (
            bid_levels
        )

        self._pm_ask_level_count = (
            ask_levels
        )

        # Critical safety behavior:
        #
        # A newer book event without quantity data invalidates any
        # previously cached depth state. We refuse to carry stale
        # quantity forward as if it described the latest book.
        self._clear_depth_state()

        self._purge(timestamp)

    def _clear_depth_state(
        self,
    ) -> None:
        self._pm_depth_observed = False

        self._pm_best_bid_quantity = None
        self._pm_best_ask_quantity = None

        self._pm_bid_depth_top5 = None
        self._pm_ask_depth_top5 = None

        self._pm_bid_depth_top10 = None
        self._pm_ask_depth_top10 = None

        self._pm_obi_top1 = None
        self._pm_obi_top5 = None
        self._pm_obi_top10 = None

        self._pm_bid_depth_within_1c = None
        self._pm_ask_depth_within_1c = None
        self._pm_obi_within_1c = None

        self._pm_bid_depth_within_2c = None
        self._pm_ask_depth_within_2c = None
        self._pm_obi_within_2c = None

        self._pm_bid_depth_within_5c = None
        self._pm_ask_depth_within_5c = None
        self._pm_obi_within_5c = None

        self._pm_microprice_top1 = None

        self._pm_bid_top1_share_of_top5 = None
        self._pm_ask_top1_share_of_top5 = None

    # ==========================================================
    # STATE SNAPSHOT
    # ==========================================================

    def snapshot(
        self,
        timestamp: float | None = None,
    ) -> StateSnapshot:
        """
        Return the causal state available at `timestamp`.

        `timestamp` may equal or exceed the most recently ingested event
        timestamp, but may never go backward.
        """

        if timestamp is None:
            if self._last_global_timestamp is None:
                raise ValueError(
                    "Cannot snapshot before receiving any events"
                )

            timestamp = self._last_global_timestamp

        timestamp = float(timestamp)

        self._assert_snapshot_time(timestamp)

        self._purge(timestamp)

        bid_count = self._pm_bid_level_count
        ask_count = self._pm_ask_level_count

        level_count_imbalance: float | None = None

        if (
            bid_count is not None
            and ask_count is not None
            and (bid_count + ask_count) > 0
        ):
            level_count_imbalance = (
                (bid_count - ask_count)
                / (bid_count + ask_count)
            )

        seconds_remaining: float | None = None

        if self._window_end is not None:
            seconds_remaining = max(
                0.0,
                self._window_end - timestamp,
            )

        distance_usd: float | None = None
        distance_pct: float | None = None

        if (
            self._btc_price is not None
            and self._price_to_beat is not None
        ):
            distance_usd = (
                self._btc_price
                - self._price_to_beat
            )

            if abs(self._price_to_beat) > EPSILON:
                distance_pct = (
                    distance_usd
                    / self._price_to_beat
                )

        last_impulse = (
            self._btc_impulses[-1]
            if self._btc_impulses
            else None
        )

        previous_inter_impulse_ms = (
            self._previous_inter_impulse_ms()
        )

        return StateSnapshot(
            version=STATE_FEATURE_VERSION,
            timestamp=timestamp,

            btc_tick_age_ms=self._age_ms(
                timestamp,
                self._last_btc_tick_ts,
            ),

            pm_quote_age_ms=self._age_ms(
                timestamp,
                self._last_pm_quote_ts,
            ),

            pm_book_age_ms=self._age_ms(
                timestamp,
                self._last_pm_book_ts,
            ),

            btc_price=self._btc_price,

            btc_last_impulse_direction=(
                last_impulse.direction
                if last_impulse is not None
                else None
            ),

            btc_last_impulse_magnitude=(
                last_impulse.magnitude
                if last_impulse is not None
                else None
            ),

            btc_previous_inter_impulse_ms=(
                previous_inter_impulse_ms
            ),

            btc_impulse_rate_1s=self._btc_rate(
                timestamp,
                1.0,
            ),

            btc_impulse_rate_5s=self._btc_rate(
                timestamp,
                5.0,
            ),

            btc_impulse_rate_10s=self._btc_rate(
                timestamp,
                10.0,
            ),

            btc_signed_imbalance_1s=(
                self._btc_signed_imbalance(
                    timestamp,
                    1.0,
                )
            ),

            btc_signed_imbalance_5s=(
                self._btc_signed_imbalance(
                    timestamp,
                    5.0,
                )
            ),

            btc_signed_imbalance_10s=(
                self._btc_signed_imbalance(
                    timestamp,
                    10.0,
                )
            ),

            btc_same_sign_continuation_5s=(
                self._btc_same_sign_continuation(
                    timestamp,
                    5.0,
                )
            ),

            btc_same_sign_continuation_10s=(
                self._btc_same_sign_continuation(
                    timestamp,
                    10.0,
                )
            ),

            btc_current_same_sign_run=(
                self._btc_current_run_length(
                    timestamp,
                    10.0,
                )
            ),

            pm_best_bid=self._pm_best_bid,
            pm_best_ask=self._pm_best_ask,

            pm_current_two_sided=(
                self._pm_current_two_sided
            ),

            pm_spread=self._pm_current_spread,

            pm_spread_median_5s=(
                self._time_weighted_spread_quantile(
                    timestamp,
                    5.0,
                    0.50,
                )
            ),

            pm_spread_p90_10s=(
                self._time_weighted_spread_quantile(
                    timestamp,
                    10.0,
                    0.90,
                )
            ),

            pm_two_sided_fraction_1s=(
                self._two_sided_fraction(
                    timestamp,
                    1.0,
                )
            ),

            pm_two_sided_fraction_5s=(
                self._two_sided_fraction(
                    timestamp,
                    5.0,
                )
            ),

            pm_two_sided_fraction_10s=(
                self._two_sided_fraction(
                    timestamp,
                    10.0,
                )
            ),

            pm_repricing_rate_1s=self._pm_rate(
                timestamp,
                1.0,
            ),

            pm_repricing_rate_5s=self._pm_rate(
                timestamp,
                5.0,
            ),

            pm_repricing_rate_10s=self._pm_rate(
                timestamp,
                10.0,
            ),

            pm_signed_move_imbalance_5s=(
                self._pm_signed_imbalance(
                    timestamp,
                    5.0,
                )
            ),

            pm_signed_move_imbalance_10s=(
                self._pm_signed_imbalance(
                    timestamp,
                    10.0,
                )
            ),

            pm_bid_level_count=bid_count,
            pm_ask_level_count=ask_count,

            pm_level_count_imbalance=(
                level_count_imbalance
            ),

            pm_abs_level_count_imbalance=(
                abs(level_count_imbalance)
                if level_count_imbalance is not None
                else None
            ),

            # Current recorder cannot establish these.
            pm_depth_observed=(
                self._pm_depth_observed
            ),

            pm_best_bid_quantity=(
                self._pm_best_bid_quantity
            ),

            pm_best_ask_quantity=(
                self._pm_best_ask_quantity
            ),

            pm_bid_depth_top5=(
                self._pm_bid_depth_top5
            ),

            pm_ask_depth_top5=(
                self._pm_ask_depth_top5
            ),

            pm_bid_depth_top10=(
                self._pm_bid_depth_top10
            ),

            pm_ask_depth_top10=(
                self._pm_ask_depth_top10
            ),

            pm_obi_top1=(
                self._pm_obi_top1
            ),

            pm_obi_top5=(
                self._pm_obi_top5
            ),

            pm_obi_top10=(
                self._pm_obi_top10
            ),

            pm_bid_depth_within_1c=(
                self._pm_bid_depth_within_1c
            ),

            pm_ask_depth_within_1c=(
                self._pm_ask_depth_within_1c
            ),

            pm_obi_within_1c=(
                self._pm_obi_within_1c
            ),

            pm_bid_depth_within_2c=(
                self._pm_bid_depth_within_2c
            ),

            pm_ask_depth_within_2c=(
                self._pm_ask_depth_within_2c
            ),

            pm_obi_within_2c=(
                self._pm_obi_within_2c
            ),

            pm_bid_depth_within_5c=(
                self._pm_bid_depth_within_5c
            ),

            pm_ask_depth_within_5c=(
                self._pm_ask_depth_within_5c
            ),

            pm_obi_within_5c=(
                self._pm_obi_within_5c
            ),

            pm_microprice_top1=(
                self._pm_microprice_top1
            ),

            pm_bid_top1_share_of_top5=(
                self._pm_bid_top1_share_of_top5
            ),

            pm_ask_top1_share_of_top5=(
                self._pm_ask_top1_share_of_top5
            ),

            seconds_remaining=seconds_remaining,

            coinbase_distance_to_price_to_beat_usd=(
                distance_usd
            ),

            coinbase_distance_to_price_to_beat_pct=(
                distance_pct
            ),
        )

    # ==========================================================
    # CAUSALITY / HISTORY MANAGEMENT
    # ==========================================================

   
    def _assert_causal(
        self,
        timestamp: float,
    )-> None:
        if (
        self._causal_watermark is not None
        and timestamp < (
            self._causal_watermark
            - EPSILON
        )
    ):
            raise CausalOrderError(
                "StateFeatureEngine received information "
                "earlier than its causal watermark. "
                "An event or state evaluation has already "
                "occurred at a later local receive time."
        )

        self._last_global_timestamp = timestamp

        if (
            self._causal_watermark is None
            or timestamp > self._causal_watermark
    ):
            self._causal_watermark = timestamp

    def _assert_snapshot_time(
        self,
        timestamp: float,
    ) -> None:
        if (
            self._causal_watermark is not None
            and timestamp < (
                self._causal_watermark
                - EPSILON
            )
        ):
            raise CausalOrderError(
                "Cannot evaluate state earlier than "
                "PolyArb's causal watermark."
            )

        if (
            self._causal_watermark is None
            or timestamp > self._causal_watermark
        ):
            self._causal_watermark = timestamp

    def _purge(
        self,
        timestamp: float,
    ) -> None:
        cutoff = (
            timestamp
            - MAX_HISTORY_SECONDS
        )

        while (
            self._btc_impulses
            and self._btc_impulses[0].timestamp < cutoff
        ):
            self._btc_impulses.popleft()

        while (
            self._pm_repricings
            and self._pm_repricings[0].timestamp < cutoff
        ):
            self._pm_repricings.popleft()

        while (
            self._quote_segments
            and self._quote_segments[0].end < cutoff
        ):
            self._quote_segments.popleft()

    # ==========================================================
    # QUOTE STATE
    # ==========================================================

    def _close_quote_segment(
        self,
        timestamp: float,
    ) -> None:
        if self._last_pm_quote_ts is None:
            return

        if timestamp <= self._last_pm_quote_ts:
            return

        self._quote_segments.append(
            _QuoteSegment(
                start=self._last_pm_quote_ts,
                end=timestamp,
                two_sided=self._pm_current_two_sided,
                spread=self._pm_current_spread,
            )
        )

    def _quote_segments_in_window(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> list[_QuoteSegment]:
        start = timestamp - window_seconds

        result: list[_QuoteSegment] = []

        for segment in self._quote_segments:
            clipped_start = max(
                start,
                segment.start,
            )

            clipped_end = min(
                timestamp,
                segment.end,
            )

            if clipped_end > clipped_start:
                result.append(
                    _QuoteSegment(
                        start=clipped_start,
                        end=clipped_end,
                        two_sided=segment.two_sided,
                        spread=segment.spread,
                    )
                )

        # Include causal duration of current quote state up to snapshot.
        if (
            self._last_pm_quote_ts is not None
            and timestamp > self._last_pm_quote_ts
        ):
            clipped_start = max(
                start,
                self._last_pm_quote_ts,
            )

            if timestamp > clipped_start:
                result.append(
                    _QuoteSegment(
                        start=clipped_start,
                        end=timestamp,
                        two_sided=self._pm_current_two_sided,
                        spread=self._pm_current_spread,
                    )
                )

        return result

    def _two_sided_fraction(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> float | None:
        segments = self._quote_segments_in_window(
            timestamp,
            window_seconds,
        )

        total_duration = 0.0
        two_sided_duration = 0.0

        for segment in segments:
            duration = (
                segment.end
                - segment.start
            )

            if duration <= 0:
                continue

            total_duration += duration

            if segment.two_sided:
                two_sided_duration += duration

        if total_duration <= EPSILON:
            return None

        return (
            two_sided_duration
            / total_duration
        )

    def _time_weighted_spread_quantile(
        self,
        timestamp: float,
        window_seconds: float,
        quantile: float,
    ) -> float | None:
        segments = self._quote_segments_in_window(
            timestamp,
            window_seconds,
        )

        weighted: list[tuple[float, float]] = []

        for segment in segments:
            if (
                not segment.two_sided
                or segment.spread is None
            ):
                continue

            duration = (
                segment.end
                - segment.start
            )

            if duration > 0:
                weighted.append(
                    (
                        segment.spread,
                        duration,
                    )
                )

        return self._weighted_quantile(
            weighted,
            quantile,
        )

    # ==========================================================
    # BTC FEATURES
    # ==========================================================

    def _btc_events(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> list[_BTCImpulse]:
        start = timestamp - window_seconds

        return [
            event
            for event in self._btc_impulses
            if start <= event.timestamp <= timestamp
        ]

    def _btc_rate(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> float:
        return (
            len(
                self._btc_events(
                    timestamp,
                    window_seconds,
                )
            )
            / window_seconds
        )

    def _btc_signed_imbalance(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> float | None:
        events = self._btc_events(
            timestamp,
            window_seconds,
        )

        if not events:
            return None

        return (
            sum(
                event.direction
                for event in events
            )
            / len(events)
        )

    def _btc_same_sign_continuation(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> float | None:
        events = self._btc_events(
            timestamp,
            window_seconds,
        )

        if len(events) < 2:
            return None

        same = 0

        pairs = len(events) - 1

        for earlier, later in zip(
            events,
            events[1:],
        ):
            if earlier.direction == later.direction:
                same += 1

        return same / pairs

    def _btc_current_run_length(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> int | None:
        events = self._btc_events(
            timestamp,
            window_seconds,
        )

        if not events:
            return None

        direction = events[-1].direction
        run = 0

        for event in reversed(events):
            if event.direction != direction:
                break

            run += 1

        return run

    def _previous_inter_impulse_ms(
        self,
    ) -> float | None:
        if len(self._btc_impulses) < 2:
            return None

        return (
            (
                self._btc_impulses[-1].timestamp
                - self._btc_impulses[-2].timestamp
            )
            * 1000.0
        )

    # ==========================================================
    # PM REPRICING FEATURES
    # ==========================================================

    def _pm_events(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> list[_PMRepricing]:
        start = timestamp - window_seconds

        return [
            event
            for event in self._pm_repricings
            if start <= event.timestamp <= timestamp
        ]

    def _pm_rate(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> float:
        return (
            len(
                self._pm_events(
                    timestamp,
                    window_seconds,
                )
            )
            / window_seconds
        )

    def _pm_signed_imbalance(
        self,
        timestamp: float,
        window_seconds: float,
    ) -> float | None:
        events = self._pm_events(
            timestamp,
            window_seconds,
        )

        if not events:
            return None

        return (
            sum(
                event.direction
                for event in events
            )
            / len(events)
        )

    # ==========================================================
    # HELPERS
    # ==========================================================

    @staticmethod
    def _age_ms(
        now: float,
        event_timestamp: float | None,
    ) -> float | None:
        if event_timestamp is None:
            return None

        return max(
            0.0,
            (now - event_timestamp) * 1000.0,
        )

    @staticmethod
    def _weighted_quantile(
        values_and_weights: list[tuple[float, float]],
        quantile: float,
    ) -> float | None:
        if not values_and_weights:
            return None

        if not 0.0 <= quantile <= 1.0:
            raise ValueError(
                "quantile must be between 0 and 1"
            )

        usable = [
            (value, weight)
            for value, weight in values_and_weights
            if weight > 0
        ]

        if not usable:
            return None

        usable.sort(
            key=lambda item: item[0]
        )

        total_weight = sum(
            weight
            for _, weight in usable
        )

        target = quantile * total_weight

        cumulative = 0.0

        for value, weight in usable:
            cumulative += weight

            if cumulative + EPSILON >= target:
                return value

        return usable[-1][0]