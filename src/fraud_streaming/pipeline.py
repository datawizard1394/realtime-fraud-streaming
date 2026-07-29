"""Stateful event-time pipeline with watermarks, deduplication, and rules."""

from __future__ import annotations

import hashlib
import heapq
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Iterable

from .config import PipelineConfig
from .metrics import MetricRegistry
from .models import FraudAlert, TransactionEvent, format_timestamp
from .rules import evaluate_rules


class FraudPipeline:
    """
    A bounded reference implementation of event-time stream processing.

    The in-memory heap models an event-time reorder buffer. The per-account
    deques model keyed state. A real deployment would replace these with a
    durable stream processor and checkpointed state backend.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.metrics = MetricRegistry()
        self._buffer: list[tuple[datetime, int, TransactionEvent]] = []
        self._arrival_sequence = 0
        self._max_event_time: datetime | None = None
        self._watermark: datetime | None = None
        self._state: dict[str, deque[TransactionEvent]] = defaultdict(deque)
        self._seen_event_ids: set[str] = set()
        self._emitted_alert_ids: set[str] = set()
        self._closed = False

        self.alerts: list[FraudAlert] = []
        self.processed_event_ids: list[str] = []
        self.metrics.set_gauge("buffered_events", 0)
        self.metrics.set_gauge("active_accounts", 0)

    @property
    def watermark(self) -> datetime | None:
        return self._watermark

    def process(self, event: TransactionEvent) -> tuple[FraudAlert, ...]:
        """Accept one arrival and emit alerts made ready by watermark progress."""
        if self._closed:
            raise RuntimeError("cannot process events after flush")

        self.metrics.increment("events_received")
        if event.event_id in self._seen_event_ids:
            self.metrics.increment("duplicates_dropped")
            return ()
        self._seen_event_ids.add(event.event_id)

        previous_max = self._max_event_time
        if previous_max is not None and event.event_time < previous_max:
            self.metrics.increment("out_of_order_received")
        if previous_max is None or event.event_time > previous_max:
            self._max_event_time = event.event_time

        if self._max_event_time is None:  # pragma: no cover - defensive invariant
            raise RuntimeError("max event time was not initialized")
        self._watermark = self._max_event_time - timedelta(
            seconds=self.config.allowed_lateness_seconds
        )

        if event.event_time < self._watermark:
            self.metrics.increment("late_events")
            self.metrics.increment("late_events_dropped")
            return ()

        heapq.heappush(
            self._buffer,
            (event.event_time, self._arrival_sequence, event),
        )
        self._arrival_sequence += 1
        self.metrics.increment("events_buffered")
        self.metrics.set_gauge("buffered_events", len(self._buffer))
        return self._drain_ready()

    def process_all(self, events: Iterable[TransactionEvent]) -> tuple[FraudAlert, ...]:
        """Process a finite arrival stream and advance the final watermark."""
        emitted: list[FraudAlert] = []
        for event in events:
            emitted.extend(self.process(event))
        emitted.extend(self.flush())
        return tuple(emitted)

    def flush(self) -> tuple[FraudAlert, ...]:
        """Model an end-of-stream watermark and drain all accepted events."""
        if self._closed:
            return ()
        emitted: list[FraudAlert] = []
        while self._buffer:
            _, _, event = heapq.heappop(self._buffer)
            alert = self._evaluate(event)
            if alert is not None:
                emitted.append(alert)
        self.metrics.set_gauge("buffered_events", 0)
        self.metrics.increment("flushes")
        self._closed = True
        return tuple(emitted)

    def metrics_snapshot(self) -> dict[str, object]:
        snapshot = self.metrics.snapshot()
        snapshot["event_time"] = {
            "max_event_time": (
                format_timestamp(self._max_event_time) if self._max_event_time else None
            ),
            "watermark": format_timestamp(self._watermark) if self._watermark else None,
            "allowed_lateness_seconds": self.config.allowed_lateness_seconds,
        }
        snapshot["pipeline_closed"] = self._closed
        return snapshot

    def _drain_ready(self) -> tuple[FraudAlert, ...]:
        if self._watermark is None:
            return ()
        emitted: list[FraudAlert] = []
        while self._buffer and self._buffer[0][0] <= self._watermark:
            _, _, event = heapq.heappop(self._buffer)
            alert = self._evaluate(event)
            if alert is not None:
                emitted.append(alert)
        self.metrics.set_gauge("buffered_events", len(self._buffer))
        return tuple(emitted)

    def _evaluate(self, event: TransactionEvent) -> FraudAlert | None:
        history = self._state[event.account_id]
        retention_cutoff = event.event_time - timedelta(
            seconds=self.config.state_retention_seconds
        )
        while history and history[0].event_time < retention_cutoff:
            history.popleft()
            self.metrics.increment("state_records_evicted")

        signals = evaluate_rules(event, history, self.config)
        history.append(event)
        self.processed_event_ids.append(event.event_id)
        self.metrics.increment("events_processed")
        self.metrics.set_gauge("active_accounts", len(self._state))
        self.metrics.set_gauge(
            "state_records",
            sum(len(account_history) for account_history in self._state.values()),
        )

        risk_score = min(100, sum(signal.score for signal in signals))
        if risk_score < self.config.alert_score_threshold:
            return None

        signal_names = tuple(signal.name for signal in signals)
        stable_material = f"{event.event_id}:{','.join(sorted(signal_names))}"
        digest = hashlib.sha256(stable_material.encode("utf-8")).hexdigest()[:16]
        alert = FraudAlert(
            alert_id=f"alert-{digest}",
            event_id=event.event_id,
            account_id=event.account_id,
            event_time=event.event_time,
            risk_score=risk_score,
            signals=signal_names,
            reasons=tuple(signal.reason for signal in signals),
        )
        if alert.alert_id in self._emitted_alert_ids:
            self.metrics.increment("alerts_idempotently_suppressed")
            return None
        self._emitted_alert_ids.add(alert.alert_id)
        self.alerts.append(alert)
        self.metrics.increment("alerts_emitted")
        return alert
