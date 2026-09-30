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
