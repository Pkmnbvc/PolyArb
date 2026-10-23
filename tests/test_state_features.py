import unittest

from polyarb.state_features import (
    CausalOrderError,
    StateFeatureEngine,
)


class TestStateFeatureEngine(unittest.TestCase):

    def test_quantity_book_populates_scoped_depth(self):
        engine = StateFeatureEngine()

        bids = [
            {
                "px": {"value": "0.72"},
                "qty": "100",
            },
            {
                "px": {"value": "0.71"},
                "qty": "50",
            },
        ]

        offers = [
            {
                "px": {"value": "0.73"},
                "qty": "40",
            },
            {
                "px": {"value": "0.74"},
                "qty": "10",
            },
        ]

        engine.update_pm_book(
            100.0,
            bids_raw=bids,
            offers_raw=offers,
        )

        state = engine.snapshot(100.0)

        self.assertTrue(
            state.pm_depth_observed
        )

        self.assertEqual(
            state.pm_bid_level_count,
            2,
        )

        self.assertEqual(
            state.pm_ask_level_count,
            2,
        )

        self.assertAlmostEqual(
            state.pm_best_bid_quantity,
            100.0,
        )

        self.assertAlmostEqual(
            state.pm_best_ask_quantity,
            40.0,
        )

        self.assertAlmostEqual(
            state.pm_bid_depth_top5,
            150.0,
        )

        self.assertAlmostEqual(
            state.pm_ask_depth_top5,
            50.0,
        )

        self.assertAlmostEqual(
            state.pm_obi_top5,
            0.5,
        )

        self.assertIsNotNone(
            state.pm_microprice_top1
        )

    def test_count_only_book_invalidates_stale_depth(self):
        engine = StateFeatureEngine()

        engine.update_pm_book(
            100.0,
            bids_raw=[
                {
                    "px": {"value": "0.50"},
                    "qty": "100",
                }
            ],
            offers_raw=[
                {
                    "px": {"value": "0.51"},
                    "qty": "100",
                }
            ],
        )

        first = engine.snapshot(100.0)

        self.assertTrue(
            first.pm_depth_observed
        )

        # A newer book observation contains only level counts.
        # PolyArb must NOT keep stale quantity features.
        engine.update_pm_book(
            101.0,
            bid_levels=10,
            offer_levels=20,
        )

        second = engine.snapshot(101.0)

        self.assertFalse(
            second.pm_depth_observed
        )

        self.assertIsNone(
            second.pm_obi_top5
        )

        self.assertIsNone(
            second.pm_best_bid_quantity
        )

    def test_btc_impulse_is_causal(self):
        engine = StateFeatureEngine(
            btc_impulse_threshold=1.00
        )

        engine.update_coinbase_tick(
            100.0,
            100_000.0,
        )

        created = engine.update_coinbase_tick(
            100.5,
            100_003.0,
        )

        self.assertTrue(created)

        state = engine.snapshot(100.5)

        self.assertEqual(
            state.btc_last_impulse_direction,
            1,
        )

        self.assertAlmostEqual(
            state.btc_last_impulse_magnitude,
            3.0,
        )

        self.assertAlmostEqual(
            state.btc_impulse_rate_1s,
            1.0,
        )

    def test_out_of_order_event_rejected(self):
        engine = StateFeatureEngine()

        engine.update_coinbase_tick(
            100.0,
            100_000.0,
        )

        with self.assertRaises(
            CausalOrderError
        ):
            engine.update_pm_lite(
                99.0,
                0.50,
                0.51,
            )

    def test_one_sided_quote_resets_midpoint(self):
        engine = StateFeatureEngine()

        engine.update_pm_lite(
            100.0,
            0.50,
            0.51,
        )

        # One-sided gap.
        engine.update_pm_lite(
            101.0,
            0.50,
            None,
        )

        # New two-sided quote must NOT be compared
        # to the pre-gap midpoint.
        direction = engine.update_pm_lite(
            102.0,
            0.55,
            0.56,
        )

        self.assertIsNone(direction)

        state = engine.snapshot(102.0)

        self.assertEqual(
            state.pm_repricing_rate_10s,
            0.0,
        )

    def test_pm_directional_repricing(self):
        engine = StateFeatureEngine()

        engine.update_pm_lite(
            100.0,
            0.50,
            0.51,
        )

        up = engine.update_pm_lite(
            100.2,
            0.51,
            0.52,
        )

        down = engine.update_pm_lite(
            100.4,
            0.50,
            0.51,
        )

        self.assertEqual(up, 1)
        self.assertEqual(down, -1)

        state = engine.snapshot(100.4)

        self.assertAlmostEqual(
            state.pm_repricing_rate_1s,
            2.0,
        )

        self.assertAlmostEqual(
            state.pm_signed_move_imbalance_5s,
            0.0,
        )

    def test_level_counts_are_not_true_obi(self):
        engine = StateFeatureEngine()

        engine.update_pm_book(
            100.0,
            bid_levels=24,
            offer_levels=57,
        )

        state = engine.snapshot(100.0)

        expected = (
            (24 - 57)
            / (24 + 57)
        )

        self.assertAlmostEqual(
            state.pm_level_count_imbalance,
            expected,
        )

        # Critical scientific boundary:
        # counts are NOT quantity depth.
        self.assertFalse(
            state.pm_depth_observed
        )

        self.assertIsNone(
            state.pm_obi_top1
        )

        self.assertIsNone(
            state.pm_obi_top5
        )

        self.assertIsNone(
            state.pm_obi_top10
        )

        self.assertIsNone(
            state.pm_microprice_top1
        )

        self.assertIsNone(
            state.pm_best_bid_quantity
        )

        self.assertIsNone(
            state.pm_best_ask_quantity
        )

    def test_two_sided_fraction_is_time_weighted(self):
        engine = StateFeatureEngine()

        # Two-sided from t=100 to t=104.
        engine.update_pm_lite(
            100.0,
            0.50,
            0.51,
        )

        # One-sided from t=104 to t=106.
        engine.update_pm_lite(
            104.0,
            0.50,
            None,
        )

        state = engine.snapshot(106.0)

        # Window contains 4 seconds two-sided,
        # 2 seconds one-sided.
        self.assertAlmostEqual(
            state.pm_two_sided_fraction_10s,
            4.0 / 6.0,
        )

    def test_market_context_is_explicit_coinbase_proxy(self):
        engine = StateFeatureEngine()

        engine.set_market_context(
            window_end=200.0,
            price_to_beat=100_000.0,
        )

        engine.update_coinbase_tick(
            150.0,
            100_050.0,
        )

        state = engine.snapshot(150.0)

        self.assertAlmostEqual(
            state.seconds_remaining,
            50.0,
        )

        self.assertAlmostEqual(
            state.coinbase_distance_to_price_to_beat_usd,
            50.0,
        )

        self.assertAlmostEqual(
            state.coinbase_distance_to_price_to_beat_pct,
            0.0005,
        )

    def test_future_snapshot_advances_causal_watermark(self):
        engine = StateFeatureEngine()

        engine.update_coinbase_tick(
            100.0,
            100_000.0,
        )

        # PolyArb evaluates the world at t=105.
        engine.snapshot(105.0)

        # Information claiming to have arrived at t=103
        # can no longer be inserted afterward.
        with self.assertRaises(
            CausalOrderError
        ):
            engine.update_pm_lite(
                103.0,
                0.50,
                0.51,
            )


if __name__ == "__main__":
    unittest.main()