# Changelog

All notable changes to this project are documented here.

## 0.2.1 - 2026-09-04

### Fixed

- Preserve ambiguous context, returned, and raised write errors without a
  competing completion or failure transition.
- Reject checkpoint occurrence collisions while retaining existing names.

## 0.2.0 - 2026-09-04

### Added

- Public architecture and human-focused contribution guides.
- A dependency-free worker throughput script for local comparisons.
- Regression coverage for supervision recovery and stale lease timers.
- CI validation that the release package can be built from repository contents.

### Changed

- Expanded internal documentation to explain the intent behind worker and
  lifecycle code.

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
