from __future__ import annotations

import unittest

from fraud_streaming.simulator import SimulationConfig, TransactionSimulator


class TransactionSimulatorTests(unittest.TestCase):
    def test_same_seed_produces_identical_arrivals(self) -> None:
        config = SimulationConfig(seed=7, event_count=30)
        first = TransactionSimulator(config).generate()
        second = TransactionSimulator(config).generate()
        self.assertEqual(first, second)

    def test_different_seed_changes_ids_and_payloads(self) -> None:
        first = TransactionSimulator(SimulationConfig(seed=7, event_count=10)).generate()
        second = TransactionSimulator(SimulationConfig(seed=8, event_count=10)).generate()
        self.assertNotEqual(first, second)

    def test_controlled_duplicate_has_same_event_id(self) -> None:
        arrivals = TransactionSimulator(
            SimulationConfig(seed=7, event_count=18, duplicate_every=17)
        ).generate()
        ids = [item.event_id for item in arrivals]
        duplicate_ids = {event_id for event_id in ids if ids.count(event_id) > 1}
        self.assertEqual(len(arrivals), 19)
        self.assertEqual(len(duplicate_ids), 1)


if __name__ == "__main__":
    unittest.main()

