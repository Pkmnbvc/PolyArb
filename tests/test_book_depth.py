import unittest
from decimal import Decimal

from polyarb.book_depth import (
    parse_sdk_book,
    summarize_book,
)


class TestBookDepth(unittest.TestCase):

    def setUp(self):
        self.bids_raw = [
            {
                "px": {
                    "value": "0.7200",
                    "currency": "USD",
                },
                "qty": "2666.0100",
            },
            {
                "px": {
                    "value": "0.7100",
                    "currency": "USD",
                },
                "qty": "2128.0000",
            },
            {
                "px": {
                    "value": "0.7000",
                    "currency": "USD",
                },
                "qty": "1920.4400",
            },
            {
                "px": {
                    "value": "0.6900",
                    "currency": "USD",
                },
                "qty": "2821.0000",
            },
            {
                "px": {
                    "value": "0.6800",
                    "currency": "USD",
                },
                "qty": "543.0000",
            },
        ]

        self.asks_raw = [
            {
                "px": {
                    "value": "0.7300",
                    "currency": "USD",
                },
                "qty": "2581.2800",
            },
            {
                "px": {
                    "value": "0.7400",
                    "currency": "USD",
                },
                "qty": "2314.6400",
            },
            {
                "px": {
                    "value": "0.7500",
                    "currency": "USD",
                },
                "qty": "1514.0000",
            },
            {
                "px": {
                    "value": "0.7600",
                    "currency": "USD",
                },
                "qty": "1450.0000",
            },
            {
                "px": {
                    "value": "0.7700",
                    "currency": "USD",
                },
                "qty": "1425.0000",
            },
        ]

    def test_live_sample_parses_exactly(self):
        bids, asks = parse_sdk_book(
            self.bids_raw,
            self.asks_raw,
        )

        state = summarize_book(
            bids,
            asks,
        )

        self.assertEqual(
            state.best_bid_price,
            Decimal("0.7200"),
        )

        self.assertEqual(
            state.best_bid_quantity,
            Decimal("2666.0100"),
        )

        self.assertEqual(
            state.best_ask_price,
            Decimal("0.7300"),
        )

        self.assertEqual(
            state.best_ask_quantity,
            Decimal("2581.2800"),
        )

        self.assertEqual(
            state.spread,
            Decimal("0.0100"),
        )

    def test_top5_depth_uses_quantity_not_level_count(self):
        bids, asks = parse_sdk_book(
            self.bids_raw,
            self.asks_raw,
        )

        state = summarize_book(
            bids,
            asks,
        )

        self.assertEqual(
            state.bid_depth_top5,
            Decimal("10078.4500"),
        )

        self.assertEqual(
            state.ask_depth_top5,
            Decimal("9284.9200"),
        )

    def test_scoped_obi(self):
        bids, asks = parse_sdk_book(
            self.bids_raw,
            self.asks_raw,
        )

        state = summarize_book(
            bids,
            asks,
        )

        expected_top1 = (
            Decimal("2666.0100")
            - Decimal("2581.2800")
        ) / (
            Decimal("2666.0100")
            + Decimal("2581.2800")
        )

        self.assertEqual(
            state.obi_top1,
            expected_top1,
        )

        expected_top5 = (
            Decimal("10078.4500")
            - Decimal("9284.9200")
        ) / (
            Decimal("10078.4500")
            + Decimal("9284.9200")
        )

        self.assertEqual(
            state.obi_top5,
            expected_top5,
        )

    def test_microprice(self):
        bids, asks = parse_sdk_book(
            self.bids_raw,
            self.asks_raw,
        )

        state = summarize_book(
            bids,
            asks,
        )

        expected = (
            (
                Decimal("0.7300")
                * Decimal("2666.0100")
            )
            +
            (
                Decimal("0.7200")
                * Decimal("2581.2800")
            )
        ) / (
            Decimal("2666.0100")
            + Decimal("2581.2800")
        )

        self.assertEqual(
            state.microprice_top1,
            expected,
        )

    def test_level_count_can_disagree_with_quantity_depth(self):
        bids_raw = [
            {
                "px": {"value": "0.50"},
                "qty": "10000",
            },
        ]

        asks_raw = [
            {
                "px": {"value": "0.51"},
                "qty": "1",
            },
            {
                "px": {"value": "0.52"},
                "qty": "1",
            },
            {
                "px": {"value": "0.53"},
                "qty": "1",
            },
        ]

        bids, asks = parse_sdk_book(
            bids_raw,
            asks_raw,
        )

        state = summarize_book(
            bids,
            asks,
        )

        # Level-count shape points ask-heavy:
        level_count_imbalance = (
            len(bids)
            - len(asks)
        ) / (
            len(bids)
            + len(asks)
        )

        self.assertLess(
            level_count_imbalance,
            0,
        )

        # Actual top depth points overwhelmingly bid-heavy.
        self.assertGreater(
            state.obi_top5,
            Decimal("0"),
        )

    def test_schema_drift_fails_loudly(self):
        bad_bid = [
            {
                "px": {
                    "value": "0.50"
                }
                # qty deliberately absent
            }
        ]

        with self.assertRaises(
            ValueError
        ):
            parse_sdk_book(
                bad_bid,
                [],
            )


if __name__ == "__main__":
    unittest.main()