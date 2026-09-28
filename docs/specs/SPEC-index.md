# SPEC TC: temporalio-codex durable Codex delivery platform

## Problem Statement

The previous local runner rebuilt durable workflow execution, recovery, process ownership and external delivery coordination in application code. The result was a large controller with a wide interface and unclear ownership. This fork should provide a thin flow controller by using Temporal native Workflows, Activities, Signals, Updates, Queries, timers and Worker restart semantics.

## Solution

Build an external Codex Worker application in this Temporal fork. Keep the Temporal Server source on its upstream-compatible path and use its existing extension seams only when a Server concern truly needs customization. Put Codex SDK, Git, GitHub and migration behavior in Activities and optional child Workflows. Use Temporal history as the normal Run source of truth and retain only compact typed results in Workflow state.

The work is split into TC-00 through TC-05. They must be delivered in dependency order; a later SPEC cannot broaden the core Workflow interface to expose an adapter detail.

## User Stories

1. As a developer, I want a clean local Server and Worker setup, so that a new checkout can run one deterministic Workflow.
2. As an operator, I want a Run to survive Worker restart, wait for input and continue with the same identity.
3. As a user, I want Codex planning, implementation and independent review to run as explicit Activities with visible thread and turn observations.
4. As a developer, I want Git workspaces, candidate checks, review, CI and merge guarded by candidate identity and readback.
5. As an operator, I want GitHub response loss, queued merge and cleanup to remain durable without duplicate confirmed side effects.
6. As a maintainer, I want Temporal Signals, Updates, Queries, timers and retry policies to replace a second recovery engine.
7. As an existing user, I want selected legacy runs inspected and imported without inventing missing history.
8. As a release owner, I want deterministic, local, live SDK, live GitHub and OS evidence kept separate.
9. As an upstream maintainer, I want Server changes additive and independently buildable, so that upstream rebases remain practical.

## Implementation Decisions

- TC-00 establishes the external Worker application and deterministic vertical slice.
- TC-01 owns the thin Run Workflow and minimal durable state.
- TC-02 adds the Codex SDK Activity adapter.
- TC-03 adds local Git and GitHub delivery Activities.
- TC-04 adds operator controls and bounded recovery using Temporal primitives.
- TC-05 adds read-only legacy migration and release qualification.
- Workflow code performs no network, subprocess, filesystem or provider SDK work.
- External writes use stable operation IDs and readback after unknown outcomes.
- The application does not add a generic Server plugin registry or change Temporal history, matching or frontend semantics.

## Testing Decisions

Tests cross the highest seam: Workflow inputs, results, Signals, Updates and Queries. Temporal test environments and fake Activities cover deterministic behavior. Local Server smoke tests cover Worker integration. Opt-in live SDK, GitHub and Windows checks produce separate evidence and cannot promote mock results to live completion. Historical databases are read-only fixtures for migration tests.

## Out of Scope

Automatic migration of every old run, arbitrary historical SDK-thread interruption, a custom SQLite recovery engine, a detached process supervisor, changes to Temporal protocol or persistence semantics, and exactly-once claims for Codex or GitHub side effects.

## Further Notes

Canonical child documents are `TC-00-foundation.md` through `TC-05-migration-release.md`. The GitHub issue set mirrors those documents. The repository Issues are the active planning surface; this file is the versioned source of the plan.
