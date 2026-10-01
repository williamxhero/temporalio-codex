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

The initial reader subscribes to the execution-scoped SSE endpoint directly;
the stream's first event supplies the snapshot, so the page does not wait for a
separate history request before subscribing. `view=thread` returns messages and
activity summaries in persisted event order, omitting prompt and raw activity
payloads. Expanding a prompt or activity fetches `details=1` for the same
namespace, Workflow ID, and Run ID. Conversation snapshots are cached by scope
and database revision, so the stream's 500 ms change checks avoid rebuilding an
unchanged history.

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

## Fast thread reader deployment (2026-10-01)

The reader now receives its initial conversation snapshot from the first SSE
event. The backend caches snapshots by execution scope and database revision;
the UI requests prompt and activity details only when expanded. Prompt, Codex
messages, and activity summaries are returned in persisted event order.

The versioned deployment script staged and served assets from
`.tmp/temporal-ui-builds/20261001-104950-852`, preserving
`.tmp/temporal.db`. No Workflow was running during the restart, and no new
Workflow was started. Both Worker queues (`codex-worker` and
`issue-81-live-probe`) now share `.tmp/issue81-live-conversations.sqlite3`,
which is the database read by the UI's Conversation Server on port 18001.

On the historical Issue 81 execution, the default thread response is about
1.2 KB and omits the full prompt; `details=1` returns the deferred content.
The response contains `user`, `assistant`, `activity`, then `assistant`
messages. Focused Conversation Store and Server tests passed (37); Ruff passed.
The production UI build and startup asset checks passed, and the deployed
desktop page rendered the existing execution with the collapsed Prompt and
Verification details and the inline activity timeline.

## Timeline cleanup deployment (2026-10-01)

The Chat timeline now groups adjacent activities with the same category into
one entry, joining their summaries with newlines. Adjacent Reasoning and Command
entries therefore read as one block. Command details and output remain hidden
until expanded. When Temporal reports a terminal execution while the captured
turn still says `running`, the UI labels the turn `Stopped` and suppresses its
animated generating cursor.

Command and Test remain separate timeline categories. Their summaries no longer
print the command line or test output. Opening `Detail` shows each captured
record as a `Command` block followed by its `Output` block, with multiple
records kept in chronological order. Context, Command, and Test now use one
compact row for the colored category, a short summary, and the `Detail` control.
Context summaries omit the redundant `Read context:` prefix; command summaries
remove the shell launcher and pass/fail suffix and are capped at 88 characters.
Adjacent Test records use a green check-circle for passed results and a red
cross-circle when any record fails. Other or in-progress results use a neutral
clock marker. Agent responses that contain adjacent code-review JSON documents
(`candidate_sha`, `findings`, and `verdict`) render as a compact findings table;
duplicate findings appear once and the original JSON remains under `Detail`.
Other JSON responses render as
`{...}` with their raw content behind `Detail`. The timeline's vertical gaps and
response line height are reduced for a denser thread view. Assistant replies are
labeled `Agent` in the timeline.

The versioned UI build was deployed to
`.tmp/temporal-ui-builds/20261001-124614-661`, preserving the existing
`.tmp/temporal.db` and conversation database. The historical Issue 81 snapshot
still returns the completed turn and its 1,185-byte conversation payload.
Conversation Store and Server tests passed (38), Ruff passed, desktop browser
tests passed (8), and mobile browser tests passed (2) on an earlier build. The
final Command/Test detail and compact summary checks passed on desktop and
mobile (4 tests). Svelte check reported zero errors and 59 existing warnings.
No new workflow was started.

## JSON and compact timeline deployment (2026-10-01)

The updated UI was rebuilt and served from
`.tmp/temporal-ui-builds/20261001-132933-504` on port 18000 with the existing
`.tmp/temporal.db`. The deployment script validated the staged HTML and all
startup assets after restart. No workflow or conversation data was changed.
The Agent title, compact spacing, JSON review summary, generic JSON detail
collapse, and result-colored Test icons were covered by the desktop and mobile
browser checks (20 passed on the deployed build). Expanded detail blocks occupy
a full line. Prettier, ESLint
(zero errors, two existing warnings), Stylelint, and Svelte check (zero errors,
59 existing warnings) passed. `make lint-code-fast` remains unavailable because
this Windows host has no `make` command.

## Command and Context summary refinement (2026-10-01)

Context entries use `Read <filename>` as their summary, with the full captured
context available under `Detail`. Command and Test summaries show the executable
basename, such as `pwsh.exe`, instead of the shell invocation or full command
line. The category, short summary, and `Detail` control share one row. Compact
timeline responses expose only the command basename; the full command and
output remain available in the expanded detail response. Older events whose
stored summary was already truncated can still show the executable name when
the full command is present in persisted detail; records without that detail
fall back to the short category label.

The UI was rebuilt and deployed to
`.tmp/temporal-ui-builds/20261001-135917-919` on port 18000 with the existing
Temporal database. The UI endpoint returned 200, the Conversation API health
check passed, and the historical Issue 81 thread still returned its completed
four-message timeline. Four focused desktop and mobile browser tests passed;
39 Conversation Store tests passed. The running Worker process was left intact.

The current Conversation API readback also returns `commandName: pwsh.exe` for
captured command and test records with full command details. The historical
Requirement Delivery thread returned 10 activity messages, 9 with executable
names; no Workflow was running during the final audit.

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
