# SPEC TC-01: Durable Run Workflow

## Problem Statement

The flow needs to survive Worker process exits, wait for external input and
continue from the same run. Implementing those properties in SQLite and custom
process recovery created most of the previous complexity.

## Solution

Implement a Temporal Workflow that owns the minimal Run state: requested
stages, current stage, stage results, status and pending input. Temporal
Workflow history is the durable source of truth. Activities perform one
bounded external operation and return a typed result. The Workflow schedules
the next Activity, waits for a signal/update, or completes.

## User Stories

1. As an operator, I want a run to keep its identity after a Worker restart, so that work does not restart from an empty local database.
2. As an operator, I want a run to report its current stage and status, so that I can understand progress without reading implementation logs.
3. As an operator, I want a run to wait durably for a business answer, so that a waiting process does not need to remain alive.
4. As an operator, I want to resume a waiting run, so that the next stage uses the saved answer.
5. As an operator, I want to cancel a run, so that no later stage is scheduled after cancellation.
6. As a developer, I want a stage result to identify success, waiting, failure or unknown external outcome, so that the Workflow does not infer completion from text.
7. As a developer, I want a bounded retry policy per Activity, so that transient failures do not become infinite application loops.
8. As a developer, I want the same Workflow definition to work with deterministic test Activities and production Activities, so that the interface is the test surface.
9. As a maintainer, I want to add a stage without changing persistence tables, so that stage growth does not recreate the previous Store design.

## Implementation Decisions

- The Workflow is the only normal Run orchestrator.
- The Workflow state is typed and serializable by the configured Temporal data
  converter. No SQLite schema is required for the new path.
- Stage execution is represented by an Activity call returning a typed
  `StageResult`. The result contains a stage key, outcome, evidence references
  and an optional pending-input description.
- A Signal carries asynchronous operator input. An Update is used where the
  caller needs a validated synchronous acceptance or rejection. Query methods
  expose read-only status.
- Timers use Temporal durable timers. Python/Go sleep, polling loops and wall
  clock calls are forbidden in Workflow code.
- Activity retry policies are explicit per operation. An Activity that may have
  performed an external side effect uses a readback Activity before a new
  attempt, rather than blind retry.
- Child Workflows are reserved for independent long-running SPEC deliveries;
  the first single-run path does not create a child for every trivial stage.

## Testing Decisions

- Replay tests verify deterministic Workflow behavior from captured histories.
- Tests cover normal completion, waiting, answer, cancellation, Worker restart,
  Activity timeout and unknown Activity outcome.
- Tests use the Temporal test environment and fake Activities. They assert
  Workflow results, signals and Update responses, not internal history layout.
- A smoke test runs the same Workflow through a local Temporal Server.

## Out of Scope

- Codex SDK behavior and GitHub API behavior.
- Reproducing legacy SQLite tables in the new path.
- Automatic source-thread migration.
- Exactly-once external side effects.

## Further Notes

The thin controller must remain understandable as a small state machine. If a
new requirement needs a large receipt or recovery subsystem, it belongs in its
Activity or a later extension SPEC.
