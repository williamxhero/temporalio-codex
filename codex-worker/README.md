# Temporal Codex Worker

Install the development environment with `uv sync --extra dev` and run the
foundation tests with `uv run pytest`.

The package contains the external Temporal Worker workflow and Activity seam.
Start a local Server with `temporal server start-dev`, then start the Worker
with `uv run temporalio-codex-worker`. Submit a run from another terminal with
`uv run temporalio-codex-run --requirement "..."`.

The client derives a stable Workflow ID from the requirement when
`--workflow-id` is omitted. Reusing a completed ID is rejected explicitly.

The Worker package is independent from the Go Server module. From the
repository root, run `go test ./...` for the Server check and then run the
Worker checks from this directory.
