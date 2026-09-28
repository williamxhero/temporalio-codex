# SPEC TC-00: Thin architecture and repository skeleton

## Problem Statement

The previous Spec Runner grew into a self-built durable workflow engine. Its
controller, persistence model, recovery policy, detached process handling and
delivery adapters became one product surface. The desired product is a thin
flow controller that delegates durability to Temporal and keeps domain work in
replaceable Activities.

This fork is also the Temporal Server source tree. Application code must not be
mixed into Server internals merely because the repository is a fork.

## Solution

Add a separately named Codex application area with its own language package,
dependency manifest, local development commands and documentation. Run it
against the unmodified Temporal Server using the supported SDK. Provide a
minimal deterministic Workflow and Worker before adding Codex or GitHub calls.

The first public interface is a Workflow input and result, not a large Python
or Go controller class. The server fork remains runnable using upstream
commands and configuration.

## User Stories

1. As a developer, I want to start the local Temporal Server with the existing upstream tooling, so that the fork remains recognizable and supportable.
2. As a developer, I want to start a Codex Worker from a separate application directory, so that application changes do not modify Server internals.
3. As a developer, I want a deterministic one-stage Workflow, so that the architecture can be tested without credentials or GitHub side effects.
4. As an operator, I want a stable Workflow ID, so that starting the same run is an explicit Temporal operation rather than an accidental duplicate.
5. As a maintainer, I want the application dependencies and commands documented, so that a fresh checkout can run the first vertical slice.
6. As a maintainer, I want the Server fork to remain buildable independently of the Codex Worker, so that an application dependency cannot break Temporal Server builds.
7. As a reviewer, I want the first slice to show the exact seam between Workflow code and Activities, so that later features cannot leak I/O into deterministic Workflow code.

## Implementation Decisions

- The application is housed in a clearly named top-level directory and has an
  independent dependency manifest. It is not imported by Server packages.
- The first Worker uses the official Temporal SDK for its language and the
  Temporal test environment for Workflow tests.
- Workflow code may use only deterministic Temporal APIs. Network calls,
  subprocesses, SDK clients, filesystem writes and wall-clock access run in
  Activities.
- The initial Workflow has a typed input, a typed stage result and one
  deterministic Activity. It supports a stable Workflow ID and a task queue.
- The Server fork is not renamed internally and no persistence or history
  protocol is changed.
- Existing Server extension seams are documented as optional integration
  points. They are not required for the Codex Worker.

## Testing Decisions

- A Workflow test uses a deterministic Activity implementation and asserts only
  the public result and state transitions.
- A Worker smoke test starts the local Temporal test server or supported local
  development server and completes one run.
- A separate Server build check proves that the application directory is not a
  dependency of Server packages.
- Tests must not inspect private Workflow fields or call helper functions that
  bypass the Workflow interface.

## Out of Scope

- Codex SDK calls, GitHub calls, Git worktrees and migration.
- A new Server plugin registry.
- SQLite, custom lease files or detached process code.
- Live external credentials in CI.

## Further Notes

This SPEC is the architecture gate. No later SPEC may add a core interface
method solely to expose an adapter detail.
