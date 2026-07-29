"""Domain models and stable serialization boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and require an explicit timezone."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp must be timezone-aware: {value!r}")
    return parsed


def format_timestamp(value: datetime) -> str:
    """Format an aware timestamp in a stable ISO-8601 representation."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class TransactionEvent:
    """One synthetic card transaction at the ingestion boundary."""

    event_id: str
    account_id: str
    merchant_id: str
    event_time: datetime
    ingest_time: datetime
    amount: Decimal
    currency: str
    country: str
    device_id: str
    card_present: bool

    def __post_init__(self) -> None:
        if not self.event_id or not self.account_id:
            raise ValueError("event_id and account_id are required")
        if self.event_time.tzinfo is None or self.event_time.utcoffset() is None:
            raise ValueError("event_time must be timezone-aware")
        if self.ingest_time.tzinfo is None or self.ingest_time.utcoffset() is None:
            raise ValueError("ingest_time must be timezone-aware")
        if self.amount < 0:
            raise ValueError("amount cannot be negative")
        if len(self.currency) != 3:
            raise ValueError("currency must be a three-letter code")
        if len(self.country) != 2:
            raise ValueError("country must be a two-letter code")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe record; money remains an exact decimal string."""
        return {
            "event_id": self.event_id,
            "account_id": self.account_id,
            "merchant_id": self.merchant_id,
            "event_time": format_timestamp(self.event_time),
            "ingest_time": format_timestamp(self.ingest_time),
            "amount": str(self.amount),
            "currency": self.currency,
            "country": self.country,
            "device_id": self.device_id,
            "card_present": self.card_present,
        }

    @classmethod
    def from_dict(cls, record: dict[str, Any]) -> TransactionEvent:
        """Validate and construct an event from a deserialized record."""
        return cls(
            event_id=str(record["event_id"]),
            account_id=str(record["account_id"]),
            merchant_id=str(record["merchant_id"]),
            event_time=parse_timestamp(str(record["event_time"])),
            ingest_time=parse_timestamp(str(record["ingest_time"])),
            amount=Decimal(str(record["amount"])),
            currency=str(record["currency"]).upper(),
            country=str(record["country"]).upper(),
            device_id=str(record["device_id"]),
            card_present=bool(record["card_present"]),
        )


@dataclass(frozen=True, slots=True)
class FraudAlert:
    """A deterministic alert derived from one event and its account state."""

    alert_id: str
    event_id: str
    account_id: str
    event_time: datetime
    risk_score: int
    signals: tuple[str, ...]
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "event_id": self.event_id,
            "account_id": self.account_id,
            "event_time": format_timestamp(self.event_time),
            "risk_score": self.risk_score,
            "signals": list(self.signals),
            "reasons": list(self.reasons),
        }

