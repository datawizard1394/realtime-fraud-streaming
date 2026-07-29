from __future__ import annotations

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

from fraud_streaming.cli import run
from fraud_streaming.config import PipelineConfig
from fraud_streaming.pipeline import FraudPipeline
from fraud_streaming.simulator import SimulationConfig, TransactionSimulator


class FiniteStreamIntegrationTests(unittest.TestCase):
    def test_stream_accounting_and_alerts_are_reproducible(self) -> None:
        arrivals = TransactionSimulator(
            SimulationConfig(seed=42, event_count=100)
        ).generate()
        first = FraudPipeline(PipelineConfig.load("config/rules.json"))
        second = FraudPipeline(PipelineConfig.load("config/rules.json"))

        first_alerts = first.process_all(arrivals)
        second_alerts = second.process_all(arrivals)
        counters = first.metrics_snapshot()["counters"]

        self.assertEqual(
            counters["events_received"],
            counters["events_processed"]
            + counters["duplicates_dropped"]
            + counters["late_events_dropped"],
        )
        self.assertGreater(counters["out_of_order_received"], 0)
        self.assertGreater(len(first_alerts), 0)
        self.assertEqual(
            [alert.to_dict() for alert in first_alerts],
            [alert.to_dict() for alert in second_alerts],
        )

    def test_cli_writes_auditable_demo_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            summary = run(
                Namespace(
                    config="config/rules.json",
                    events=25,
                    seed=42,
                    accounts=8,
                    output_dir=temporary_directory,
                )
            )
            output_dir = Path(temporary_directory)
            metrics = json.loads((output_dir / "metrics.json").read_text())

            self.assertTrue((output_dir / "events.ndjson").is_file())
            self.assertTrue((output_dir / "alerts.ndjson").is_file())
            self.assertTrue(metrics["demo"]["synthetic_data"])
            self.assertFalse(metrics["demo"]["production_deployment"])
            self.assertEqual(summary["output_dir"], temporary_directory)


if __name__ == "__main__":
    unittest.main()

