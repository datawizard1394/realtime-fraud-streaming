from __future__ import annotations

import unittest
from pathlib import Path

from fraud_streaming.config import PipelineConfig, VelocityRuleConfig


class PipelineConfigTests(unittest.TestCase):
    def test_repository_config_loads(self) -> None:
        config = PipelineConfig.load(Path("config/rules.json"))
        self.assertEqual(config.allowed_lateness_seconds, 30)
        self.assertEqual(config.velocity.window_seconds, 60)

    def test_retention_must_cover_enabled_windows(self) -> None:
        with self.assertRaisesRegex(ValueError, "cover every enabled rule window"):
            PipelineConfig(
                state_retention_seconds=10,
                velocity=VelocityRuleConfig(window_seconds=60),
            )

    def test_unknown_late_policy_fails_fast(self) -> None:
        with self.assertRaisesRegex(ValueError, "late_event_policy"):
            PipelineConfig(late_event_policy="silently-accept")


if __name__ == "__main__":
    unittest.main()

