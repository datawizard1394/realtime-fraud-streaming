"""Pure rule evaluation over one event and event-time ordered account history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Sequence

from .config import PipelineConfig
from .models import TransactionEvent


@dataclass(frozen=True, slots=True)
class Signal:
    name: str
    score: int
    reason: str


def evaluate_rules(
    event: TransactionEvent,
    history: Sequence[TransactionEvent],
    config: PipelineConfig,
) -> tuple[Signal, ...]:
    """Evaluate enabled rules without side effects."""
    signals: list[Signal] = []

    if config.high_amount.enabled and event.amount >= config.high_amount.threshold:
        signals.append(
            Signal(
                name="high_amount",
                score=config.high_amount.score,
                reason=(
                    f"amount {event.amount} {event.currency} met threshold "
                    f"{config.high_amount.threshold}"
                ),
            )
        )

    if config.velocity.enabled:
        cutoff = event.event_time - timedelta(seconds=config.velocity.window_seconds)
        recent = [
            item
            for item in history
            if item.event_time >= cutoff and item.currency == event.currency
        ]
        count = len(recent) + 1
        total = sum((item.amount for item in recent), start=Decimal("0")) + event.amount
        if (
            count >= config.velocity.count_threshold
            and total >= config.velocity.amount_threshold
        ):
            signals.append(
                Signal(
                    name="velocity",
                    score=config.velocity.score,
                    reason=(
                        f"{count} transactions totaling {total} {event.currency} "
                        f"within {config.velocity.window_seconds}s"
                    ),
                )
            )

    if config.country_hop.enabled:
        cutoff = event.event_time - timedelta(seconds=config.country_hop.window_seconds)
        recent_countries = {
            item.country for item in history if item.event_time >= cutoff
        }
        other_countries = sorted(recent_countries - {event.country})
        if other_countries:
            signals.append(
                Signal(
                    name="country_hop",
                    score=config.country_hop.score,
                    reason=(
                        f"country changed from {','.join(other_countries)} to {event.country} "
                        f"within {config.country_hop.window_seconds}s"
                    ),
                )
            )

    return tuple(signals)
