"""Deterministic synthetic transaction-arrival generator."""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

from .models import TransactionEvent

_NAMESPACE = uuid.UUID("57f61eec-4d60-4f8a-8cd4-e96f242f6b8c")
_CENT = Decimal("0.01")


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    seed: int = 42
    event_count: int = 100
    account_count: int = 8
    duplicate_every: int = 17
    start_time: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def __post_init__(self) -> None:
        if self.event_count < 0:
            raise ValueError("event_count cannot be negative")
        if self.account_count <= 0:
            raise ValueError("account_count must be positive")
        if self.duplicate_every < 0:
            raise ValueError("duplicate_every cannot be negative")
        if self.start_time.tzinfo is None or self.start_time.utcoffset() is None:
            raise ValueError("start_time must be timezone-aware")


class TransactionSimulator:
    """Generate repeatable arrivals containing controlled edge cases."""

    def __init__(self, config: SimulationConfig) -> None:
        self.config = config

    def generate(self) -> tuple[TransactionEvent, ...]:
        rng = random.Random(self.config.seed)
        arrivals: list[TransactionEvent] = []

        for index in range(self.config.event_count):
            event_time = self.config.start_time + timedelta(seconds=index * 5)
            cycle_position = index % 50
            is_velocity_burst = 20 <= cycle_position <= 24
            is_injected_anomaly = index > 0 and index % 13 == 0

            account_index = 0 if is_velocity_burst else rng.randrange(self.config.account_count)
            account_id = f"acct-{account_index:03d}"
            amount = Decimal(str(rng.uniform(8, 420))).quantize(
                _CENT, rounding=ROUND_HALF_UP
            )
            country = "CA"
            device_id = f"device-{account_index:03d}-{rng.randrange(2)}"
            card_present = rng.random() < 0.55

            if is_velocity_burst:
                amount = Decimal("550.00")
                country = "CA" if cycle_position < 24 else "GB"
                device_id = f"burst-device-{cycle_position}"
                card_present = False
            if is_injected_anomaly:
                amount = (
                    Decimal("1500") + Decimal(str(rng.uniform(0, 700)))
                ).quantize(_CENT, rounding=ROUND_HALF_UP)
                country = "GB"
                device_id = f"unseen-device-{index}"
                card_present = False

            # Ingestion delays intentionally reorder some otherwise ordered events.
            delay_seconds = rng.choice((0, 1, 2, 3, 8, 18, 45))
            ingest_time = event_time + timedelta(seconds=delay_seconds)
            event_id = str(
                uuid.uuid5(
                    _NAMESPACE,
                    f"synthetic-fraud-stream:{self.config.seed}:{index}",
                )
            )
            event = TransactionEvent(
                event_id=event_id,
                account_id=account_id,
                merchant_id=f"merchant-{rng.randrange(12):03d}",
                event_time=event_time,
                ingest_time=ingest_time,
                amount=amount,
                currency="CAD",
                country=country,
                device_id=device_id,
                card_present=card_present,
            )
            arrivals.append(event)

            if (
                self.config.duplicate_every
                and index > 0
                and index % self.config.duplicate_every == 0
            ):
                arrivals.append(
                    replace(event, ingest_time=ingest_time + timedelta(seconds=1))
                )

        arrivals.sort(key=lambda item: (item.ingest_time, item.event_id))
        return tuple(arrivals)

