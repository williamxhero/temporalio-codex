# Workflow Chat UI

The tracked `temporal-ui-chat-tab.patch` applies to Temporal UI v2.54.1.
Chat is the first workflow tab and the default route. Snapshot and SSE requests
always include namespace, Workflow ID, and Run ID. Aggregation belongs to the
Conversation Server; the UI never requests conversations by a global thread ID.

The Chat component cancels snapshot/status requests and closes SSE when execution
scope changes. Disconnects retain recorded messages. It retries snapshots with
1, 2, 4, 8, then 15 second delays and subscribes again after recovery. A malformed
SSE snapshot also triggers reconciliation. Workflow status uses the existing
Temporal UI `getQuery` decoder for `get_status`; ordinary workflows without that
query retain the generic empty state.

## Build and restart

Temporal UI caches `index.html` at startup while serving assets from disk.
Rebuilding the currently served directory can therefore create missing hashes.
Build into a new versioned directory and restart with the same database:

```powershell
./codex-worker/scripts/start-temporal-chat-ui.ps1 `
  -UiSource ./.tmp/temporal-ui-v2.54.1 `
  -TemporalCli ./.tmp/temporal-cli-1.9.1/temporal.exe `
  -Database ./.tmp/temporal.db `
  -BuildRoot ./.tmp/temporal-ui-builds
```

The script checks startup asset references before stopping its recorded server,
refuses to stop a different process, preserves previous build directories, and
verifies served HTML and every startup JS/CSS asset after restart. `server.json`
records the PID, persistent database, ports, and current asset directory. Failed
builds leave the serving process intact. A failed restart leaves its logs and
staged assets available for diagnosis.

The server binds to `127.0.0.1`; configure the worker/client with
`--target-host 127.0.0.1:7233` to avoid a `localhost` IPv6 address connecting
to a different listener on Windows.

## Ticket 88 verification (2026-09-30)

- Red gate: the planning/default Chat browser test failed against the previous UI.
- Desktop browser gate: all three tests passed (default/planning scope, recovery,
  and route change to another Run ID).
- Mobile browser gate: all three tests passed at 320 x 800, including Run ID
  navigation. The synthetic navigation link is placed above fixed mobile chrome.
- Prettier passed for changed UI, build script, and browser tests.
- Svelte check: zero errors, 59 existing warnings in unrelated components.
- Production staged build passed. Served HTML exactly matched staged HTML;
  every referenced startup JS/CSS returned 200 with a non-HTML content type.
- Live default Chat redirect and reload passed at 1920 x 1080 and 320 x 800
  against the same persistent Temporal database, with zero page errors.
- `make lint-code-fast` could not run because `make` is absent on this host.

The live reload check uses the pre-existing failed execution and proves UI and
asset behavior. It does not claim a successful new SDK/delivery execution;
that acceptance is recorded with the worker's real execution evidence.

## SPEC 91 / Ticket 95 acceptance (2026-09-30)

The integrated reader was deployed locally from integration commit `6d3066853`.
The Temporal database remains `.tmp/temporal.db`; the conversation database
remains `.tmp/issue81-live-conversations.sqlite3`. Before restarting, Temporal
listed no running workflows. The previous worker PID and parent command lines
were checked against the `issue-81-live-probe` queue and conversation database.
Only the local processes were restarted; no new SDK workflow was started.

The versioned staging script completed successfully and recorded UI PID 54708
and assets `.tmp/temporal-ui-builds/20260930-210937-568`. Its served HTML equality
and startup JS/CSS checks passed. The refreshed Conversation Server returned
the historical execution at the public HTTP boundary with cursor 91, a context
activity summary `Read context: SKILL.md, README.md`, a clean `displayOutput`,
and `SDK_PROBE_OK` separately in `verificationMarkers`.

Live browser acceptance used the exact existing execution URL:

```text
http://127.0.0.1:18000/namespaces/default/workflows/issue-81-sdk-probe-31b0830686a1/01a0f1a5-e705-786f-82d8-39ea25eed2fb/chat
```

- Desktop 1920 x 1080 and mobile 320 x 800 passed against the real server.
- The default timeline shows the context summary and the final acceptance
  command. SKILL source, prompt, identifiers and the probe marker are collapsed.
- Expanding Context source exposes the retained SKILL source; expanding
  Verification details exposes the retained probe marker.
- Reload preserves the execution URL and restores the collapsed default view.
- Both viewports have no document horizontal overflow, page errors or failed
  JS/CSS requests. Screenshots were inspected for readable wrapping and overlap.
- Local evidence: `.tmp/ticket95-live-report.json`,
  `.tmp/ticket95-live-1920.png`, `.tmp/ticket95-live-320.png`.
- The implementation gates reported 12 passing UI integration tests, Svelte
  check with zero errors and 59 existing unrelated warnings, and focused
  Conversation Store / Server / SDK tests passing. These gates cover synthetic
  SSE recovery, deduplication, execution scope and command/error fixtures;
  this historical live probe only contains a context activity and final reply.
- The broader backend run reported 240 passed, one skipped, one deselected and
  one scheduler failure in `test_whole_flow[None]`; its isolated rerun passed.
  This intermittent result is retained as a limitation, not a clean suite gate.
- `make lint-code-fast` was attempted and could not run: `make` is not installed
  on this Windows host.

### Final readback after review fixes

Integration commit `79c969e8f` includes review fixes for compact failure causes,
command exit and test result summaries, bounded file summaries, streaming
replacement and plan snapshots. The final focused backend gate reported 54
passing tests. Those fixtures verify actual command/test/file/error events;
the historical live execution cannot provide evidence for activity kinds it
did not perform.

Temporal again listed no running workflows before the final worker restart.
The previous worker and uv parent identities were rechecked, then the worker
was restarted from the current root checkout with the same queue, database and
port. The final uv parent is PID 37248 and Conversation Server listener is PID
43924. No UI source changed during review, so the staged assets above remain
the served build.

The desktop/mobile live script was rerun against the final backend. Both passed
the exact execution URL, context summary, collapsed source/verification,
explicit expansion, reload, zero horizontal overflow and zero page/asset
errors checks. The local JSON report and screenshots above now contain this
final readback. No new live SDK execution or external deployment was needed.

## Browser verification

In the UI checkout:

```powershell
$env:PW_MODE = 'integration'
pnpm exec playwright test tests/integration/workflow-chat.spec.ts
```

Tests run desktop and mobile viewports, exercise the public snapshot/SSE and
Temporal query boundaries, and save recovered Chat screenshots. For live
acceptance use the same database and a real execution from the worker. Confirm
the default Chat route, exact namespace/Workflow/Run URL parameters, streamed
messages, and absence of page/module errors after reload.
