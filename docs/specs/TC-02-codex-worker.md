# SPEC TC-02: Codex SDK Activity adapter

## Problem Statement

The existing implementation mixes Codex thread/turn lifecycle, recovery and
workflow state. This makes a simple stage depend on SDK-specific identity,
provider failures and process behavior.

## Solution

Implement Codex as an external Temporal Activity adapter. The Activity receives
an immutable operation input and returns a typed observation containing thread
and turn identity, outcome, output references and uncertainty. The Workflow
stores only the compact result needed to choose the next stage. SDK calls never
run in deterministic Workflow code.

## User Stories

1. As a user, I want a planning stage to invoke the configured Codex SDK, so that a requirement can be turned into a plan.
2. As a user, I want an implementation stage to use a configured repository and scope, so that generated changes stay within the requested project.
3. As a user, I want a review stage to be a separate Codex operation, so that implementation output is not accepted as its own review.
4. As an operator, I want the thread and turn identities in the result, so that an interrupted call can be inspected.
5. As an operator, I want an SDK failure classified as failed or unknown, so that the Workflow does not claim completion from an ambiguous response.
6. As an operator, I want a pending business question returned as a waiting result, so that I can answer through a Temporal Signal or Update.
7. As a maintainer, I want the SDK version and capability facts recorded, so that unsupported interrupt or takeover operations are explicit.
8. As a maintainer, I want a fake Codex adapter for tests, so that deterministic tests do not require live credentials.
9. As a security owner, I want approvals and repository scope passed explicitly, so that the Worker cannot silently broaden permissions.

## Implementation Decisions

- The Codex adapter is a Worker-side Activity implementation with an injected
  SDK client factory. Production and deterministic test adapters are separate.
- Each Activity input contains a stable operation ID, run ID, stage key, thread
  identity when resuming, repository path and allowed scope.
- The adapter persists no second run database. It may write bounded SDK
  artifacts needed for diagnosis, while the Workflow retains the typed result.
- Unknown outcomes are represented explicitly. The Workflow invokes a readback
  Activity or waits for operator action; it does not create a new thread merely
  because a call timed out.
- Approval policy, model, effort, skill input and repository scope are explicit
  input fields and are recorded in the result metadata without secrets.
- SDK interruption is supported only when the current SDK process owns the
  interrupt handle. Historical arbitrary-thread interruption is reported as an
  unsupported capability.

## Testing Decisions

- Unit tests exercise the adapter interface with a fake SDK client.
- Activity tests cover completed turn, rejected call, timeout, stream
  disconnect, unknown acceptance and pending input.
- Workflow tests verify that each observation leads to the correct next state
  and that unknown results do not schedule duplicate implementation work.
- One opt-in live qualification uses the installed official SDK and records
  real thread/turn evidence separately from deterministic tests.

## Out of Scope

- Modifying the Temporal Server to understand Codex.
- Automatic migration of arbitrary historical Codex threads.
- Storing encrypted provider history in Workflow state.
- Treating a successful model response as proof of code correctness.

## Further Notes

Codex is an external system. Temporal provides durable orchestration, not
exactly-once provider execution. Stable operation IDs and readback are part of
this adapter's interface.
