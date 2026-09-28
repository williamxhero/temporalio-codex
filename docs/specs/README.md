# temporalio-codex SPEC index

This fork uses Temporal's native application model. Codex and GitHub behavior
runs in external Workers as Workflows and Activities. The Temporal Server fork
is kept for server distribution and explicitly supported server options; the
application behavior does not become Server core code.

## Delivery order

| SPEC | Name | Depends on | Completion signal |
| --- | --- | --- | --- |
| [TC-00](TC-00-foundation.md) | Thin architecture and repository skeleton | — | A local Temporal Server and Worker execute one deterministic run |
| [TC-01](TC-01-run-workflow.md) | Durable Run Workflow | TC-00 | Worker restart resumes the same Workflow without SQLite |
| [TC-02](TC-02-codex-worker.md) | Codex SDK Activity adapter | TC-01 | A real or explicitly unavailable Codex turn is observed through an Activity |
| [TC-03](TC-03-git-github-delivery.md) | Git and GitHub delivery Activities | TC-01 | Candidate, review, CI and merge are guarded by readback |
| [TC-04](TC-04-controls-recovery.md) | Signals, Updates and bounded recovery | TC-01, TC-02 | Answer, pause, cancel and unknown Activity outcomes are durable |
| [TC-05](TC-05-migration-release.md) | Migration and release qualification | TC-02, TC-03, TC-04 | Existing users have a documented migration path and reproducible release evidence |

## Architectural rule

The core Workflow has one responsibility: choose the next stage, schedule an
Activity or wait for a Temporal event, and return a typed result. GitHub, Git,
Codex, process management and evidence projection are adapters or Activities.
They do not add methods to the core Workflow interface.

Temporal history is the source of truth for Workflow progress. An external
database is not introduced for normal run state. Receipts are returned from
Activities and retained in Workflow state; external systems are queried again
when an Activity result is unknown.

The fork does not claim a generic Server plugin mechanism. Where the Server
offers an existing extension seam, such as an authorizer, interceptor, custom
archiver or custom persistence factory, a later SPEC may use that seam. Codex
and GitHub application behavior remains in the external Worker.

## Scope guard

The following are deliberately excluded from the first release:

- modifying Temporal history, matching or frontend semantics;
- a second SQLite recovery engine;
- a custom scheduler or detached process supervisor;
- automatic arbitrary historical SDK-thread takeover;
- claiming exactly-once behavior for external GitHub or Codex side effects;
- adding a new Server extension point before an existing one is proven insufficient.

The repository's GitHub Issues are currently disabled. These documents are the
canonical submitted SPEC set until the issue tracker is deliberately enabled.
