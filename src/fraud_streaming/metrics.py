"""Small dependency-free metric registry for testable pipeline observability."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class MetricRegistry:
    counters: Counter[str] = field(default_factory=Counter)
    gauges: dict[str, int | float] = field(default_factory=dict)

    def increment(self, name: str, value: int = 1) -> None:
        self.counters[name] += value

    def set_gauge(self, name: str, value: int | float) -> None:
        self.gauges[name] = value

    def snapshot(self) -> dict[str, Any]:
        return {
            "counters": dict(sorted(self.counters.items())),
            "gauges": dict(sorted(self.gauges.items())),
        }

