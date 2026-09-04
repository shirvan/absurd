# Changelog

All notable changes to this project are documented here.

## 0.1.0 - 2026-09-03

Initial public release.

### Added

- Process-free `Absurd.Client` API for queues, task spawning, results, retries,
  cancellation, and events.
- `Absurd.Task` and `Absurd.Context` APIs for checkpointed steps, external
  idempotency, heartbeats, durable sleeps, event waits, and cross-queue child
  waits.
- Capacity-aware, OTP-supervised worker pools with graceful draining, lease
  watchdogs, rolling-deployment deferral, hooks, and bounded failure handling.
- Parameterized wrappers for the Absurd `0.5.0` SQL lifecycle and maintenance
  operations.
- Bounded SQL and task-execution telemetry.
- Credo, doctest, documentation-coverage, PostgreSQL integration, worker
  failure, and Elixir/OTP compatibility gates.
