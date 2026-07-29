from __future__ import annotations

import unittest
from dataclasses import replace
from decimal import Decimal

from fraud_streaming.config import (
    CountryHopRuleConfig,
    HighAmountRuleConfig,
    PipelineConfig,
    VelocityRuleConfig,
)
from fraud_streaming.pipeline import FraudPipeline

from helpers import event


def config_without_rules(*, lateness: int = 10) -> PipelineConfig:
    return PipelineConfig(
        allowed_lateness_seconds=lateness,
        state_retention_seconds=60,
        high_amount=HighAmountRuleConfig(enabled=False),
        velocity=VelocityRuleConfig(enabled=False),
        country_hop=CountryHopRuleConfig(enabled=False),
    )


class EventTimePipelineTests(unittest.TestCase):
    def test_watermark_reorders_events_by_event_time(self) -> None:
        pipeline = FraudPipeline(config_without_rules(lateness=10))

        pipeline.process(event("evt-00", seconds=0))
        pipeline.process(event("evt-20", seconds=20))
        pipeline.process(event("evt-15", seconds=15))
        pipeline.flush()

        self.assertEqual(
            pipeline.processed_event_ids,
            ["evt-00", "evt-15", "evt-20"],
        )
        self.assertEqual(
            pipeline.metrics_snapshot()["counters"]["out_of_order_received"],
            1,
        )

    def test_event_behind_watermark_is_dropped_and_counted(self) -> None:
        pipeline = FraudPipeline(config_without_rules(lateness=5))

        pipeline.process(event("evt-20", seconds=20))
        pipeline.process(event("evt-too-late", seconds=0))
        pipeline.flush()

        self.assertEqual(pipeline.processed_event_ids, ["evt-20"])
        counters = pipeline.metrics_snapshot()["counters"]
        self.assertEqual(counters["late_events"], 1)
        self.assertEqual(counters["late_events_dropped"], 1)

    def test_duplicate_event_is_idempotently_suppressed(self) -> None:
        pipeline = FraudPipeline(config_without_rules())
        duplicate = event("evt-same")

        pipeline.process(duplicate)
        pipeline.process(duplicate)
        pipeline.flush()

        self.assertEqual(pipeline.processed_event_ids, ["evt-same"])
        self.assertEqual(
            pipeline.metrics_snapshot()["counters"]["duplicates_dropped"],
            1,
        )

    def test_processing_after_flush_is_rejected(self) -> None:
        pipeline = FraudPipeline(config_without_rules())
        pipeline.flush()
        with self.assertRaisesRegex(RuntimeError, "after flush"):
            pipeline.process(event("evt-late-call"))


class FraudRuleTests(unittest.TestCase):
    def test_high_amount_emits_stable_alert(self) -> None:
        config = PipelineConfig(
            allowed_lateness_seconds=0,
            state_retention_seconds=60,
            alert_score_threshold=50,
            high_amount=HighAmountRuleConfig(
                enabled=True,
                threshold=Decimal("100"),
                score=60,
            ),
            velocity=VelocityRuleConfig(enabled=False),
            country_hop=CountryHopRuleConfig(enabled=False),
        )
        first = FraudPipeline(config)
        second = FraudPipeline(config)
        high_value_event = event("evt-high", amount="100.00")

        first_alerts = first.process_all([high_value_event])
        second_alerts = second.process_all([high_value_event])

        self.assertEqual(len(first_alerts), 1)
        self.assertEqual(first_alerts[0].signals, ("high_amount",))
        self.assertEqual(first_alerts[0].risk_score, 60)
        self.assertEqual(first_alerts[0].alert_id, second_alerts[0].alert_id)

    def test_velocity_rule_uses_event_time_window(self) -> None:
        config = PipelineConfig(
            allowed_lateness_seconds=0,
            state_retention_seconds=60,
            alert_score_threshold=50,
            high_amount=HighAmountRuleConfig(enabled=False),
            velocity=VelocityRuleConfig(
                enabled=True,
                window_seconds=20,
                count_threshold=3,
                amount_threshold=Decimal("100"),
                score=60,
            ),
            country_hop=CountryHopRuleConfig(enabled=False),
        )
        pipeline = FraudPipeline(config)

        alerts = pipeline.process_all(
            [
                event("evt-1", seconds=0, amount="40"),
                event("evt-2", seconds=5, amount="40"),
                event("evt-3", seconds=10, amount="40"),
            ]
        )

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].event_id, "evt-3")
        self.assertEqual(alerts[0].signals, ("velocity",))

    def test_velocity_rule_does_not_mix_currencies(self) -> None:
        config = PipelineConfig(
            allowed_lateness_seconds=0,
            state_retention_seconds=60,
            alert_score_threshold=50,
            high_amount=HighAmountRuleConfig(enabled=False),
            velocity=VelocityRuleConfig(
                enabled=True,
                window_seconds=20,
                count_threshold=3,
                amount_threshold=Decimal("100"),
                score=60,
            ),
            country_hop=CountryHopRuleConfig(enabled=False),
        )
        pipeline = FraudPipeline(config)
        usd_event = replace(event("evt-usd", seconds=5, amount="40"), currency="USD")

        alerts = pipeline.process_all(
            [
                event("evt-cad-1", seconds=0, amount="40"),
                usd_event,
                event("evt-cad-2", seconds=10, amount="40"),
            ]
        )

        self.assertEqual(alerts, ())

    def test_country_hop_uses_prior_account_state_only(self) -> None:
        config = PipelineConfig(
            allowed_lateness_seconds=0,
            state_retention_seconds=300,
            alert_score_threshold=50,
            high_amount=HighAmountRuleConfig(enabled=False),
            velocity=VelocityRuleConfig(enabled=False),
            country_hop=CountryHopRuleConfig(enabled=True, window_seconds=300, score=50),
        )
        pipeline = FraudPipeline(config)

        alerts = pipeline.process_all(
            [
                event("evt-ca", seconds=0, country="CA"),
                event("evt-gb", seconds=30, country="GB"),
            ]
        )

        self.assertEqual([alert.event_id for alert in alerts], ["evt-gb"])
        self.assertIn("country_hop", alerts[0].signals)


if __name__ == "__main__":
    unittest.main()
