# SPEC TC-04: Signals, Updates and bounded recovery

## Problem Statement

The previous system implemented pause, cancel, answer, retries, recovery
episodes, leases and process takeover in application-owned SQLite. This created
many states that were difficult to reason about and made recovery a second
workflow engine.

## Solution

Use Temporal's native Signals, Updates, Queries, durable timers, Activity retry
policies and Worker heartbeats. Keep recovery decisions in the Workflow where
they are part of the durable state machine. Keep provider-specific diagnosis in
Activity results. Do not introduce a second lease or recovery database for the
new path.

## User Stories

1. As an operator, I want to pause a run at a safe stage point, so that no new Activity is scheduled after the pause request is accepted.
2. As an operator, I want to cancel a run, so that pending work is stopped or marked for bounded cleanup.
3. As an operator, I want to submit an answer exactly once for a question, so that duplicate messages have no side effect.
4. As an operator, I want status queries to work while a run is waiting, so that no worker process needs to own status state.
5. As an operator, I want a long external operation to heartbeat, so that Temporal can detect a lost Worker.
6. As a developer, I want transient Activity errors retried by policy, so that each Activity does not grow a private retry loop.
7. As a developer, I want nonretryable or unknown errors to stop safely, so that a new thread is not created indefinitely.
8. As an operator, I want a durable timer to wake a waiting run, so that service availability can be rechecked without a polling daemon.
9. As a maintainer, I want a Workflow replay to reproduce recovery decisions, so that failures are diagnosable from Temporal history.
10. As a maintainer, I want existing legacy runs to remain readable, so that migration does not destroy historical evidence.

## Implementation Decisions

- Signals are used for fire-and-forget control and answers; Updates are used
  for validated commands that need a direct result; Queries are read-only.
- The Workflow owns a small explicit state model: active, waiting for input,
  waiting for external observation, paused, cancelled, completed or failed.
- Activity retry policies distinguish transient, nonretryable and unknown
  external outcomes. Unknown outcomes never trigger blind duplication.
- Heartbeats carry bounded progress metadata, not secrets or full provider
  history.
- Durable timers replace service-wait polling threads.
- Cleanup is an Activity with its own operation ID. A cleanup failure leaves
  the Workflow in a cleanup-pending state and does not restart implementation.
- The legacy Spec Runner database is read-only compatibility input. Migration
  is an explicit Activity/command and is not part of the first normal run.

## Testing Decisions

- Temporal test-environment tests cover signal/update races, duplicate answer,
  pause before scheduling, cancellation, timer wakeup and replay.
- Activity tests cover heartbeat timeout, retry classification and unknown
  external outcome.
- Restart tests stop and restart a Worker while a Workflow is waiting and while
  an Activity is in progress.
- Legacy compatibility tests use copied historical databases read-only and
  verify that the new Workflow never mutates them.

## Out of Scope

- Arbitrary historical SDK-thread interruption.
- A custom cross-process owner lease.
- A custom recovery episode schema parallel to Temporal history.
- Deterministic tests being reported as live provider qualification.

## Further Notes

Temporal is the durable execution system. This SPEC exists to prevent the
application from rebuilding one beside it.
