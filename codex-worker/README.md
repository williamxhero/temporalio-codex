# Temporal Codex Worker

Install the development environment with `uv sync --extra dev` and run the
foundation tests with `uv run pytest`.

The package contains the external Temporal Worker workflow and Activity seam.
For a source checkout, start a local Server with `temporal server start-dev`,
then start the Worker with `uv run temporalio-codex-worker`. Submit a run from
another terminal with `uv run temporalio-codex-run --requirement "..."`.

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
