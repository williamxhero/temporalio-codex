# SPEC execution repair

## Contract

- A single existing SPEC starts one SpecExecutionWorkflow directly. Its ID is
  `<Codex project directory>:#<SPEC Issue number>`, for example
  `stock_advisor:#325`. Tickets execute inside that SPEC Workflow.
- Multiple SPECs use a coordinator plus one SpecExecutionWorkflow per SPEC;
  each child ID uses its SPEC Issue number. A repeated launch adopts the
  existing execution; changing the input under an issue fails.
- Old workflow histories retain their original layout and identifiers. Recovery
  does not silently upgrade a historical execution or start another SDK turn.
- The conversation database default is absolute and independent of cwd.
  Historical databases are imported explicitly, including operation ledgers.
  Conflicting ledgers fail before mutation. Namespace and execution isolation
  remain mandatory.

## Implementation

WholeFlowInput.execution_layout selects legacy, spec or project behavior. Existing
workflow components are reused inside the SPEC execution so candidate capture,
review proof and delivery gates remain in force. Local progress and operator
controls are routed to the active component. Codex operation identities include
the ticket key; the frozen candidate is carried into the following ticket.

The new layout is input gated, avoiding replay changes to existing histories.
The SPEC Workflow ID patch preserves prior hashed child IDs when replaying
histories created before SPEC Issue numbers were used as Workflow IDs.
The coordinator supplies verified SPEC publication results to its children.
An implementation timeout remains an unknown external outcome requiring
readback. Business failure remains visible in the durable status and result;
Temporal COMPLETED alone is not a delivery success signal.

## Verification

Test the real Temporal workflow with activities replaced at the SDK boundary:
single SPEC has no children, multiple SPECs have exactly one child per SPEC,
every SPEC executes Codex activities, repeated launches do not duplicate work,
different tickets use distinct operation IDs, candidate SHA advances, pause and
cancel route correctly, rejected review prevents delivery, historical replay
still passes, and database imports preserve ledger and execution isolation.

## Operations and tradeoffs

Automatic recovery stays paused throughout repair. New layout histories must
replay successfully before deployment. Historical versions are audited separately;
histories that fail replay remain readable and must not be reset or resumed.
Do not reset historical workflows for visibility.
Larger workflow histories replace many small executions; bounded SPEC stages
and sequential tickets limit history growth and concurrent workspace writes.
At higher load, separate task queues and workers may share the configured SQLite
file on this local host; remote workers require a shared durable storage service.
Import is idempotent and transactional so interruption can be retried safely.

## Local deployment (2026-10-01)

The main Worker was restarted at 22:41 Asia/Singapore from this checkout's
editable Python package. Launcher PID 62320 owns Worker PID 55392, polling
the `codex-worker` queue at `127.0.0.1:7233`. Conversation API port 18001
continues to use `codex-worker/.tmp/codex-conversations.sqlite3`.

The request-loader smoke check preserved existing SPEC number 84 from the
stock_advisor request. An in-memory single-SPEC variant with number 325
resolved to `stock_advisor:#325` and the direct `spec` execution layout.
No workflow was launched by this check. The queue has workflow and activity
pollers, zero backlog, and the API health check passed. Historical Chat
readback returned one conversation and one turn; the UI returned HTTP 200.
Worker startup logs were empty of errors. Temporal listed no running
workflows before and after restart. Automatic recovery remains paused.

The preceding backend verification reported 305 passed and 2 skipped;
the final compatibility adjustment passed 18 focused tests. The UI assets
were unchanged. Existing workflow histories and IDs were not modified.
