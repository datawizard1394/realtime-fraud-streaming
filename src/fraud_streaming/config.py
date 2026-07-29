"""Typed, validated configuration for pipeline semantics and fraud rules."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class HighAmountRuleConfig:
    enabled: bool = True
    threshold: Decimal = Decimal("1200")
    score: int = 55


@dataclass(frozen=True, slots=True)
class VelocityRuleConfig:
    enabled: bool = True
    window_seconds: int = 60
    count_threshold: int = 5
    amount_threshold: Decimal = Decimal("2500")
    score: int = 40


@dataclass(frozen=True, slots=True)
class CountryHopRuleConfig:
    enabled: bool = True
    window_seconds: int = 300
    score: int = 50


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """All behavior-changing values are explicit and validated."""

    allowed_lateness_seconds: int = 30
    late_event_policy: str = "drop"
    state_retention_seconds: int = 900
    alert_score_threshold: int = 50
    high_amount: HighAmountRuleConfig = HighAmountRuleConfig()
    velocity: VelocityRuleConfig = VelocityRuleConfig()
    country_hop: CountryHopRuleConfig = CountryHopRuleConfig()

    def __post_init__(self) -> None:
        if self.allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds cannot be negative")
        if self.late_event_policy not in {"drop"}:
            raise ValueError("this demo supports late_event_policy='drop' only")
        if not 1 <= self.alert_score_threshold <= 100:
            raise ValueError("alert_score_threshold must be in [1, 100]")
        if self.high_amount.threshold <= 0:
            raise ValueError("high_amount.threshold must be positive")
        if self.velocity.window_seconds <= 0:
            raise ValueError("velocity.window_seconds must be positive")
        if self.velocity.count_threshold < 2:
            raise ValueError("velocity.count_threshold must be at least 2")
        if self.velocity.amount_threshold <= 0:
            raise ValueError("velocity.amount_threshold must be positive")
        if self.country_hop.window_seconds <= 0:
            raise ValueError("country_hop.window_seconds must be positive")
        scores = (self.high_amount.score, self.velocity.score, self.country_hop.score)
        if any(score < 0 or score > 100 for score in scores):
            raise ValueError("each rule score must be in [0, 100]")
        longest_window = max(
            self.velocity.window_seconds if self.velocity.enabled else 0,
            self.country_hop.window_seconds if self.country_hop.enabled else 0,
        )
        if self.state_retention_seconds < longest_window:
            raise ValueError("state_retention_seconds must cover every enabled rule window")

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> PipelineConfig:
        rules = raw.get("rules", {})
        high_amount = rules.get("high_amount", {})
        velocity = rules.get("velocity", {})
        country_hop = rules.get("country_hop", {})
        return cls(
            allowed_lateness_seconds=int(raw.get("allowed_lateness_seconds", 30)),
            late_event_policy=str(raw.get("late_event_policy", "drop")),
            state_retention_seconds=int(raw.get("state_retention_seconds", 900)),
            alert_score_threshold=int(raw.get("alert_score_threshold", 50)),
            high_amount=HighAmountRuleConfig(
                enabled=bool(high_amount.get("enabled", True)),
                threshold=Decimal(str(high_amount.get("threshold", "1200"))),
                score=int(high_amount.get("score", 55)),
            ),
            velocity=VelocityRuleConfig(
                enabled=bool(velocity.get("enabled", True)),
                window_seconds=int(velocity.get("window_seconds", 60)),
                count_threshold=int(velocity.get("count_threshold", 5)),
                amount_threshold=Decimal(str(velocity.get("amount_threshold", "2500"))),
                score=int(velocity.get("score", 40)),
            ),
            country_hop=CountryHopRuleConfig(
                enabled=bool(country_hop.get("enabled", True)),
                window_seconds=int(country_hop.get("window_seconds", 300)),
                score=int(country_hop.get("score", 50)),
            ),
        )

    @classmethod
    def load(cls, path: str | Path) -> PipelineConfig:
        with Path(path).open(encoding="utf-8") as handle:
            raw = json.load(handle)
        if not isinstance(raw, dict):
            raise ValueError("pipeline config must be a JSON object")
        return cls.from_dict(raw)

