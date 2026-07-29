# Operations and verification notes

> This is a synthetic local demo, not an operating production service. The notes
> below document how its guarantees would be monitored and extended.

## Reconciliation invariants

After a completed run with late-event policy `drop`:

```text
events_received
  = events_processed
  + duplicates_dropped
  + late_events_dropped
```

Additional invariants:

- `buffered_events == 0` after `flush`;
- each processed event ID is unique;
- each emitted alert ID is unique;
- `alerts_emitted` equals the number of alert records;
- state records are in nondecreasing event-time order for each account.

The integration test asserts the first invariant and deterministic alert replay.

## Metric interpretation

| Metric | Meaning | Production response |
|---|---|---|
| `events_received` | All arrivals presented to the operator | Compare with source offsets |
| `duplicates_dropped` | Previously seen logical IDs | Watch for producer retry spikes |
| `out_of_order_received` | Event time behind observed maximum | Track delay distribution |
| `late_events_dropped` | Event time behind current watermark | Review lateness budget and source health |
| `events_processed` | Records evaluated by rules | Reconcile against input outcomes |
| `alerts_emitted` | New stable alert IDs | Reconcile with sink acknowledgements |
| `state_records_evicted` | History removed by retention | Validate TTL assumptions |
| `buffered_events` | Current reorder-buffer size | Alert on sustained growth |
| `state_records` | Current retained keyed records | Capacity and skew signal |

## Failure-mode review

### Crash before output

The local process loses state. Production requires coordinated source offsets,
state checkpoints, and a sink guarantee. Stable alert IDs make an idempotent sink
possible but do not alone provide exactly-once processing.

### Hot account or partition

One high-volume account can concentrate state and compute. Production controls
include key-salting where semantics permit, per-key rate limits, skew dashboards,
and sizing tests based on worst-case windows.

### Watermark stalls

An idle or delayed partition can hold back progress. Define partition idleness,
measure per-partition delay, and document whether watermarks use the minimum or
another bounded strategy.

### Rule rollout changes decisions

Version rules, include the version in every alert, evaluate new versions in shadow,
measure decision deltas, require approval, and retain a tested rollback.

### Late-event spike

Do not silently discard. Page or ticket based on volume and value, preserve enough
metadata for replay, and define whether correction is an upsert, retraction, or
manual review.

## Production-readiness checklist

- [ ] Versioned schema with compatibility tests
- [ ] PII classification, minimization, encryption, retention, and access review
- [ ] Authenticated and authorized broker, state, metrics, and sink access
- [ ] Durable checkpoints with automated restore tests
- [ ] Idempotent/transactional sink contract and reconciliation job
- [ ] Load, skew, soak, and backpressure tests with stated hardware
- [ ] SLOs for end-to-end delay, availability, and late-event rate
- [ ] Dashboards, alerts, runbooks, and named ownership
- [ ] Rule/model governance, explainability, human review, and rollback
- [ ] Regional legal, financial, and fairness assessment
- [ ] Disaster recovery objectives and game-day evidence

## Local verification

```bash
make check
make demo
python -m json.tool artifacts/metrics.json
```

The `demo` provenance block must continue to report:

```json
{
  "production_deployment": false,
  "synthetic_data": true
}
```

