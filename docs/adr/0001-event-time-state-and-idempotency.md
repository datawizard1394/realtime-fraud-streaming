# ADR 0001: Event-time state and idempotent outputs

- **Status:** Accepted for this synthetic demo
- **Date:** 2026-07-28
- **Scope:** Local reference implementation, not a production deployment

## Context

Fraud signals depend on when transactions occurred, not when a processor happened
to receive them. Network delay, retries, and partition recovery can reorder or
duplicate records. A naive arrival-order loop therefore produces non-repeatable
rolling-window results and duplicate alerts.

This demo needs semantics that are visible, deterministic, and testable without
an external broker or state store.

## Decision

1. Treat `event_time` as the ordering field and `ingest_time` as simulator metadata.
2. Track `max_observed_event_time` and define the watermark as:

   ```text
   watermark = max_observed_event_time - allowed_lateness
   ```

3. Buffer accepted records in an event-time heap. Evaluate records whose event
   time is at or before the current watermark.
4. Count and drop a new record when its event time is older than the watermark.
5. Maintain bounded per-account history and evict records older than the configured
   retention horizon.
6. Deduplicate on stable `event_id` before buffering.
7. Derive `alert_id` from the event ID and matched signal names so a deterministic
   replay produces the same output identity.
8. On finite input completion, model an end-of-stream watermark by draining the heap.

## Consequences

### Positive

- Rule results are independent of normal bounded arrival disorder.
- Replays are deterministic and alert sinks can use a stable idempotency key.
- Late-data loss and duplicate suppression are measurable rather than implicit.
- The algorithm can be tested without Kafka, wall-clock sleeps, or cloud services.

### Negative

- A larger lateness allowance increases the reorder buffer and output delay.
- The demo drops too-late records instead of recomputing historical state.
- Process-local sets and deques do not survive a crash.
- End-of-stream flush exists only for finite demos; an unbounded production stream
  advances through source watermarks and checkpoints.

## Alternatives considered

### Process in arrival order

Rejected because retry timing changes window membership and alert output.

### Sort the complete input before processing

Rejected because it cannot represent an unbounded stream or expose watermark tradeoffs.

### Add Kafka and Flink as mandatory local dependencies

Rejected for the reference path. Those systems supply durable distributed mechanics,
but would obscure the core semantics and make correctness tests slower and less
portable. An illustrative Redpanda profile remains as an integration seam.

### Update historical outputs for too-late records

Deferred. Retractions or upserts require a versioned sink contract, persisted state,
and downstream consumers that understand corrections.

## Production follow-up

A production design should decide and test:

- source partition key and per-partition watermark strategy;
- checkpoint and restore behavior under rebalance and failure;
- dedupe-state TTL based on maximum producer retry/replay horizon;
- transactional versus idempotent alert-sink guarantees;
- a dead-letter or correction workflow for too-late records;
- state-size estimates, skew controls, and backpressure limits;
- rule version in every decision and an auditable rollout/rollback process.

