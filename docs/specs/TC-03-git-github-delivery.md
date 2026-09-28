# SPEC TC-03: Git and GitHub delivery Activities

## Problem Statement

Candidate workspaces, review, CI, pull requests, merges and issue closure are
external side effects. The previous controller expanded because it tried to
make all of these concerns part of one general-purpose Run implementation.

## Solution

Implement Git and GitHub as explicit Activities behind a delivery Workflow
extension. The core Run Workflow receives a compact delivery result. Each
external write uses a stable operation ID, records intent in the Activity
input/output, and performs readback when the write response is missing or
ambiguous.

## User Stories

1. As a developer, I want a candidate workspace prepared from a known base SHA, so that changes are isolated.
2. As a developer, I want acceptance checks bound to a candidate SHA, so that stale green results cannot authorize delivery.
3. As a reviewer, I want an independent review result bound to the candidate SHA, so that self-review cannot satisfy the gate.
4. As an operator, I want a pull request created or adopted by identity, so that a lost response does not create duplicates.
5. As an operator, I want CI checks paged and read back to completion, so that incomplete observation is not treated as green.
6. As an operator, I want merge queue and pending states represented as waiting, so that queued is not confused with merged.
7. As an operator, I want a merge response loss reconciled against the current PR and target branch, so that the next SPEC starts from actual mainline state.
8. As an operator, I want issue closure and workspace cleanup to be replayable, so that cleanup does not rerun implementation or merge.
9. As a maintainer, I want local delivery to work without GitHub credentials, so that the core path remains testable.

## Implementation Decisions

- A delivery extension is a separate Workflow or Activity composition; it is not
  required for the simplest Run Workflow.
- Git operations use an argument-array subprocess adapter or a proven Git
  library, with explicit repository and workspace scope. The choice is an
  implementation detail behind the Activity interface.
- GitHub operations use the official REST/GraphQL client or `gh` adapter, but
  all writes have stable operation IDs and identity readback.
- Candidate verification, review and CI receipts include candidate SHA and
  acceptance version. A changed candidate invalidates dependent receipts.
- Merge authorization remains explicit. A queued merge returns `waiting`; only
  a verified merged state returns success.
- Temporal retry policy is conservative for writes. Unknown write outcomes go
  to a readback Activity before another write.
- GitHub credentials are Worker configuration, never Workflow input or
  persisted result payload.

## Testing Decisions

- Delivery Workflow tests use in-memory fake Git and GitHub adapters.
- Negative tests cover stale SHA, duplicate operation, lost create response,
  external edit, incomplete CI pagination, merge queue and lost merge response.
- Local Git integration tests use temporary repositories and verify clean
  workspaces and branch ancestry.
- Opt-in live GitHub tests use run-marked temporary objects in the fork and
  remain distinct from deterministic evidence.

## Out of Scope

- Changing repository protection rules.
- Building a general GitHub issue tracker inside Temporal Server.
- Claiming provider exactly-once semantics.
- Automatic deletion of unknown external objects.

## Further Notes

The delivery extension is the correct home for receipts and readback policy.
The core Workflow should not learn GitHub PR fields, CI pagination or Git
worktree mechanics.
