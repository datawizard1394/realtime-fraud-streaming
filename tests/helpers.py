from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fraud_streaming.models import TransactionEvent

BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def event(
    event_id: str,
    *,
    seconds: int = 0,
    ingest_seconds: int | None = None,
    amount: str = "10.00",
    account_id: str = "acct-test",
    country: str = "CA",
) -> TransactionEvent:
    ingest_offset = seconds if ingest_seconds is None else ingest_seconds
    return TransactionEvent(
        event_id=event_id,
        account_id=account_id,
        merchant_id="merchant-test",
        event_time=BASE_TIME + timedelta(seconds=seconds),
        ingest_time=BASE_TIME + timedelta(seconds=ingest_offset),
        amount=Decimal(amount),
        currency="CAD",
        country=country,
        device_id="device-test",
        card_present=False,
    )

