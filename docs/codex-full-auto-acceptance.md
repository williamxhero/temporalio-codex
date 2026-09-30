# Issue 81 full automatic workflow acceptance evidence

## Ticket #89 recovery evidence

`tests/acceptance/test_automatic_recovery.py` exercises public Temporal seams with a real `WorkflowEnvironment` and `Worker`:

- A ready two-SPEC workflow reaches three Codex child workflows and nine streamed SDK turns without completion signals.
- The worker is stopped after a durable CI retry timer is recorded, the SQLite conversation store is closed and reopened, and a new Worker resumes the same parent execution.
- Parent and child histories are fetched and replayed with `Replayer`; replay does not call the SDK again. The reopened store retains all nine completed turns and rejects events from another run/namespace.
- Publication recovery uses a gateway that creates a remote issue and then raises (lost response). Automatic identity reconciliation adopts the existing issue without creating a duplicate. A readback identity mismatch becomes BLOCKED after the bounded policy.

The SDK in this acceptance is deterministic/instrumented, so it proves worker restart, Temporal replay, durable conversation persistence, stream event mapping, and bounded publication behavior. It is not evidence of an authenticated live Codex service; the separately recorded live probe is required for that claim.

## Discovered problems and delivered behavior

| Problem | Implemented behavior and evidence |
| --- | --- |
| Ready tickets waited for external completion | Automatic scheduler dispatches ticket Codex children and saves completion; missing automatic input returns BLOCKED. Legacy standalone manual mode remains an explicit input contract. |
| Planning waited for answers, confirmation and publication | Derives answers and stable operation identities from readable input; unreadable historical references terminate BLOCKED. |
| Publication recovery required manual signals | Stable identity reconciliation, bounded attempts/backoff, exact marker and paginated readback; lost create response is adopted without duplicates. |
| Unsupported child updates broke recovery | Parent controls use supported external signals and exact child execution checks. |
| Production delivery excluded SDK commits | Owned workspace prepared before implementation; Git HEAD, clean tree and base lineage captured after implementation. |
| Production review was permanently unverified | Native SDK output schema requests a verdict for the frozen SHA; capture validates approval, empty findings, and operation/thread/turn identity. |
| Candidate branch did not exist before PR | Candidate publication precedes PR creation, uses an absent-branch lease and remote SHA readback, and rejects a conflicting remote branch. |
| Completed delivery omitted final proof | Candidate/review records persist in DeliveryResult; parent verifies matching candidate, acceptance, review and publication receipts and compares them with captured Codex evidence. |
| Cleanup missed issues published during this run | Delivery receives the verified SPEC/ticket numbers from publication readback, together with explicitly configured closure targets. |
| SDK async turn and restart outcomes were unsafe | Await async turn handles; SQLite operation claim precedes launch, completed observations persist, uncertain launch is UNKNOWN without duplicate mutation, changed input is rejected. |
| Status omitted active child and retry details | Child Run ID guarded progress signals propagate phase/ticket/wait/retry/deadline/error; terminal states clear active progress. |
| Chat mixed scope or lost data on disconnect | Namespace/Workflow/Run snapshot and SSE scope, deterministic event identity, final output reconciliation, explicit resumed-thread history only, retained UI messages and automatic reconnect. |
| UI rebuild produced stale startup hashes | Build into a fresh versioned directory, verify manifest assets, restart with the same database, verify served HTML and asset responses. |
| Startup failure leaked HTTP/SQLite resources | Worker cleanup covers server startup and failed/cancelled Temporal connection; actual socket rebinding and closed-store tests. |
| Implementation and acceptance had 30-second ceilings | SPEC input now configures Codex stage timeout; delivery input configures acceptance timeout, both default to 1800 seconds and reject invalid bounds. |

## Authenticated SDK and browser evidence

On 2026-09-30, `scripts/probe_automatic_sdk.py --live` submitted a read-only automatic CodexRunWorkflow to the production local worker, using installed `openai-codex==0.155.1` and model `gpt-6.1-sol`.

- Namespace: `default`.
- Workflow: `issue-81-sdk-probe-31b0830686a1`.
- Run: `01a0f1a5-e705-786f-82d8-39ea25eed2fb`.
- SDK thread: `01a0f1a5-ed83-7160-985b-0a780bd4c7bd`.
- SDK turn: `01a0f1a5-ee8f-77e0-8625-8661aaff0d48`.
- Result: COMPLETED in approximately 20 seconds; the final response contained the documented offline acceptance command and `SDK_PROBE_OK`.
- Exact execution snapshot: one conversation and one turn; 91 persisted events: 1 input, 1 start, 50 assistant deltas, 36 tool events, 1 authoritative final, 2 completion observations. Completion observations do not create extra displayed turns.
- Manifest: project-local `.tmp/issue-81-live.json`; SQLite: `.tmp/issue81-live-conversations.sqlite3`.
- Real Chat browser check: desktop 1440 x 1000 and mobile 390 x 844, reply visible after reload, zero page errors, no horizontal overflow. Screenshots: `.tmp/issue81-sdk-chat-desktop.png` and `.tmp/issue81-sdk-chat-mobile.png`.
- Separate UI integration browser gate: all three cases passed on both desktop and mobile (default tab/status, service recovery, execution navigation). Svelte: zero errors and 59 existing unrelated warnings. Prettier and staged production build passed.

## Verification and limits

The complete offline Worker suite passed with 230 tests, 1 skipped and 1 live test deselected. The final cleanup-number binding was additionally verified with focused whole-flow/recovery tests. Core Ruff error checks, Python compilation, and full-branch `git diff --check` pass. Full Ruff reports 86 findings, chiefly nested context managers, broad gateway exception handling and import/style warnings, including existing repository debt; it is not reported as a clean gate. `make lint-code-fast` was attempted and cannot run because `make` is absent on this Windows host. No Go Server code was modified.

The full delivery tests use real local Git repositories/bare origins and production Git/candidate adapters, with instrumented SDK and GitHub boundaries. The authenticated live probe verifies actual SDK execution, persistence, HTTP and rendered Chat. A complete authenticated GitHub publication/PR/CI/merge delivery was not run, so this report does not claim live remote delivery or CI success. That requires a concrete authorized requirement, repository access, acceptance command and CI configuration.

The local Temporal/UI process remains available at `http://127.0.0.1:18000`, using the original `.tmp/temporal.db` and versioned assets. The conversation worker uses the isolated queue `issue-81-live-probe`; it does not take over historical executions on another queue.
