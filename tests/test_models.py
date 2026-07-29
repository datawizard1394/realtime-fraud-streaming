from __future__ import annotations

import unittest

from fraud_streaming.models import TransactionEvent, parse_timestamp

from helpers import event


class TransactionEventTests(unittest.TestCase):
    def test_json_round_trip_preserves_exact_money_and_time(self) -> None:
        original = event("evt-1", amount="10.10")
        restored = TransactionEvent.from_dict(original.to_dict())
        self.assertEqual(restored, original)
        self.assertEqual(original.to_dict()["amount"], "10.10")

    def test_naive_timestamp_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            parse_timestamp("2026-01-01T00:00:00")

    def test_negative_amount_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "negative"):
            event("evt-negative", amount="-0.01")


if __name__ == "__main__":
    unittest.main()

