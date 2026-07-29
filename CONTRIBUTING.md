# Contributing

This repository is a synthetic portfolio reference implementation.

Before opening a change:

1. Keep all fixtures and examples synthetic.
2. Do not add credentials, customer data, or claims of production deployment.
3. Preserve deterministic behavior for a fixed seed and configuration.
4. Add a test for every semantic change to watermarks, state, deduplication, or rules.
5. Run `make check` and `make demo`.
6. Update the ADR when changing a correctness contract.

Small, auditable changes are preferred. Benchmark claims must include a reproducible
method, environment details, and raw evidence.

