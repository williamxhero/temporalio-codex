# SPEC TC-05: Migration and release qualification

## Problem Statement

The existing `skills` repository contains a mature but overgrown Spec Runner
with a public CLI, SQLite records, deterministic tests and partial live
evidence. Moving to Temporal must not silently discard runs or claim that
deterministic evidence proves live SDK, GitHub or Windows behavior.

## Solution

Ship the new Temporal-based application as a separate product in this fork.
Provide read-only legacy inspection, an explicit import format for selected
completed or waiting runs, and a release report that separates deterministic,
local, live SDK, live GitHub and operating-system evidence. Do not make the old
SQLite engine a runtime dependency of new Workflows.

## User Stories

1. As an existing user, I want to inspect a legacy run before import, so that I can decide whether it is safe to continue.
2. As an existing user, I want a selected legacy run converted into a Temporal Workflow input, so that the new system can continue from verified facts.
3. As an operator, I want unknown legacy facts preserved as unknown, so that migration never invents completed work.
4. As an operator, I want a migration report identifying source and target identities, so that ownership is auditable.
5. As a maintainer, I want the new application installable without the old `spec-runner` source tree, so that the product has a real package boundary.
6. As a maintainer, I want a local development server and Worker recipe, so that a new Codex project can be started from a clean checkout.
7. As a release owner, I want release evidence to identify the build, SDK versions, scenario and operating system, so that evidence is reproducible.
8. As a release owner, I want unavailable live gates reported as `not_verified`, so that a green deterministic suite cannot close a live requirement.
9. As a user, I want the Server fork changes and application changes reviewable separately, so that upstream rebases remain manageable.

## Implementation Decisions

- Legacy import is read-only first. The imported input contains verified
  requirements, stage frontier, artifact references and unknown operations.
- A migration Workflow must establish one new Workflow ID before any external
  write. It never forks encrypted or opaque provider history.
- The application package has its own version, lock file and release artifact.
- The fork tracks upstream `temporalio/temporal` and keeps Codex changes in
  additive directories unless an existing Server extension seam requires a
  small integration change.
- Release qualification has separate evidence kinds: deterministic, local
  Temporal, live Codex SDK, live GitHub, Windows and project-level acceptance.
- CI always runs deterministic and local contract tests. Live tests are opt-in
  and must publish their actual status and run identity.
- No SPEC is closed merely because its implementation compiles or its mock
  Activity passes.

## Testing Decisions

- Migration tests use historical fixture databases and verify read-only access,
  unknown preservation and duplicate-import prevention.
- Packaging tests install the application outside the source checkout and run
  the Worker/CLI smoke path.
- CI checks the Server fork's normal build independently from the application.
- Release report validation rejects missing required evidence and rejects
  deterministic evidence labelled as live.
- A final acceptance matrix covers normal run, Worker restart, answer, pause,
  cancel, Codex Activity uncertainty, GitHub readback and legacy inspection.

## Out of Scope

- Closing or rewriting the historical issues in the old `skills` repository.
- Migrating every old run automatically.
- Modifying upstream Temporal protocol or persistence semantics.
- Treating a fork merge with upstream as application release evidence.

## Further Notes

This SPEC is the release gate. The project is complete only when the core
Workflow is thin, the external adapters are independently testable, and every
unavailable live capability is explicitly reported.
