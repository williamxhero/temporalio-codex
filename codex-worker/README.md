# Temporal Codex Worker

The delivery entrypoint now creates one `SpecExecutionWorkflow` for a single
SPEC. Ticket execution, Codex turns and delivery stages run inside that Workflow.
Every SPEC execution uses its GitHub Issue number in the Workflow ID, for
example `stock_advisor:#325`. The project name comes from the Codex project
directory, and ticket Issues remain part of their SPEC Workflow.
Repeated launches adopt the recorded execution, including historical IDs.
See [SPEC execution repair](docs/spec-workflow-repair.md) for compatibility and
verification rules.

The default conversation database is the absolute worker package path
`.tmp/codex-conversations.sqlite3`, independent of the shell working directory.
Set `--conversation-db` or `TEMPORALIO_CODEX_CONVERSATION_DB` explicitly for an
installed package or another deployment. Import a historical database and link
recorded descendants without resetting executions:

```text
uv run python scripts/repair-conversation-history.py --database .tmp/codex-conversations.sqlite3 --source ../.tmp/issue81-live-conversations.sqlite3
```

Install the development environment with `uv sync --extra dev` and run the
foundation tests with `uv run pytest`.

The package contains the external Temporal Worker workflow and Activity seam.
For a source checkout, start a local Server with `temporal server start-dev`,
then start the Worker with `uv run temporalio-codex-worker`. Submit a run from
another terminal with `uv run temporalio-codex-run --requirement "..."`.

Set `TEMPORALIO_CODEX_DEFAULT_MODEL` explicitly when the worker's provider
requires a replacement for the historical `gpt-5-codex` model. Unset values
and other explicit model names pass through unchanged.

`TEMPORALIO_CODEX_SANDBOX` explicitly selects the SDK sandbox for operations
that authorize writes. It defaults to `workspace-write`. Use `full-access`
only for a trusted deployment that authorizes the required test filesystem
and network access; this removes the SDK sandbox. Read-only operations always
remain read-only, and candidate capture still enforces authorized artifact
roots. Invalid values fail at worker startup. Independent review must still
produce matching approval evidence; environment configuration cannot waive it.

To use an installed artifact, build a wheel with `uv build --wheel`, install
that wheel into a clean environment, and run the same `temporalio-codex-worker`
and `temporalio-codex-run` entry points. The installed package does not import
the legacy Spec Runner source tree.

The client derives a stable Workflow ID from the requirement when
`--workflow-id` is omitted. Reusing a completed ID is rejected explicitly.

The acceptance suite exercises the whole requirement delivery flow with a
deterministic fake GitHub boundary:

```text
uv run pytest tests/acceptance -m "not live" -vv
```

The deterministic flow publishes each SPEC and its tickets through the fake
Issue gateway, reads back Parent and blocker relationships, then runs the
ticket scheduler before implementation and delivery.

The TC-08.3 harness is available as an explicit command. Deterministic mode is
credential-free and never writes to GitHub:

```text
uv run temporalio-codex-live --deterministic --manifest .tmp/tc083.json
```

The live mode requires an explicit opt-in, an authorized brief, a Runner config,
and an explicit control root. It polls the public Runner status and validates
durable SPEC, ticket, Codex, candidate, review, PR, checks, merge, origin,
Issue closure and final-summary evidence. Missing capabilities are reported as
`not_verified`; live success requires real live evidence:

```text
$env:TC083_RUN_LIVE = "1"
uv run temporalio-codex-live --live `
  --brief C:/path/to/authorized-brief.txt `
  --config C:/path/to/authorized-runner.json `
  --control-root C:/path/to/runner-control
```

The live harness does not synthesize a brief or Runner config. This prevents an
accidental run from selecting an unauthorized repository or artifact scope.
Bound polling with `--poll-timeout` and `--poll-interval` when needed.

To qualify an existing GitHub workflow, pass the umbrella Issue and existing
checkout explicitly. Live takeover performs read-only discovery first, binds
the apply and final evidence to the discovery digest, and reuses verified
Issue, ticket, candidate, review, PR, check, merge, closure and cleanup
readbacks. It is also opt-in and must use a unique run marker in the brief:

```text
$env:TC083_RUN_LIVE = "1"
uv run temporalio-codex-live --live `
  --repository williamxhero/skills `
  --takeover-issue 123 `
  --takeover-workspace C:/path/to/checkout `
  --takeover-target-ref refs/heads/main `
  --takeover-key takeover-123 `
  --brief C:/path/to/authorized-brief.txt `
  --config C:/path/to/authorized-runner.json `
  --control-root C:/path/to/runner-control `
  --required-check CI
```

The deterministic takeover matrix runs offline with
`uv run pytest tests/acceptance/test_takeover_matrix.py -q`. It covers fresh
intake, partial planning, adopted tickets, mixed completed and unfinished
SPECs, candidate and check frontiers, merge readback, dependency ordering,
snapshot drift, discovery blockers, process restart, and cleanup-only retry.

The authenticated live probe creates a marked issue and comment in
`williamxhero/skills`, reads both back, and closes the issue during cleanup.
It is opt-in so normal test runs remain offline:

```text
$env:TC07_RUN_LIVE = "1"
uv run pytest tests/acceptance/test_live_issue.py -m live -vv
```

The live probe writes its manifest under the system temporary directory, so
the evidence survives the pytest temporary directory lifecycle.

The Worker package is independent from the Go Server module. From the
repository root, run `go test ./...` for the Server check and then run the
Worker checks from this directory.

For an existing `RequirementDeliveryWorkflow`, inspect publication recovery
through the delivery entrypoint:

```text
uv run temporalio-codex-delivery diagnose --run-id <run-id> --details
uv run temporalio-codex-delivery extend-publication --run-id <run-id> --publication-timeout-seconds 300
uv run temporalio-codex-delivery retry-publication --run-id <run-id>
```

`extend-publication` changes only the pending SPEC publication activity's
start-to-close timeout and verifies the server readback. It checks the active
planning child's parent and exact execution identity, and cannot shorten the
timeout. Use it when a historical activity still carries an insufficient
timeout; changing worker source cannot update that scheduled command.
`retry-publication` is for publication awaiting readback. Check existing GitHub
operation identities first; retries adopt existing issues. Never start a new
top-level run to recover a partially published plan.

For a failed requirement delivery whose history replays with the current
workflow definitions, `recover-failed` verifies the source run and persisted
input identity, then uses Temporal `ResetWorkflowExecution` at the last
completed workflow task before the failed child was started. Supply the
existing Workflow ID, source run ID, and input identity from durable evidence:

```text
uv run temporalio-codex-delivery recover-failed \
  --run-id <workflow-id> \
  --source-run-id <failed-run-id> \
  --input-identity <persisted-input-identity>
```

The command rejects non-failed source executions, mismatched inputs, and active
runs that are not Temporal resets of the specified source run. A successful
reset receipt is not delivery completion; continue checking status and the
workflow's implementation, acceptance, review, and delivery evidence.
