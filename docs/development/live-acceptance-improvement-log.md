# Live Acceptance Improvement Log

This log records durable improvements discovered while supervising the
`stock_advisor` live `/implement-needs` acceptance run. It keeps conclusions
and evidence locations only; full command output stays in the run artifacts.

## 2026-09-29

- **Takeover identity repair is missing.** Discovery can read legacy GitHub
  Issues, but apply requires the publication marker
  `spec-runner-key:<key> operation:<id>`. Add a first-class, idempotent marker
  reconciliation operation with preflight, body-preservation checks, relation
  readback, and per-Issue receipts. Current workaround is the controlled local
  repair script and must not become the normal path.
- **Runner version drift is easy to miss.** The `skills-tc082` environment did
  not support `takeover discover`, while the global installed Runner did. The
  handoff should record executable path, package version, command capabilities,
  and reject an incompatible environment before starting discovery.
- **Control-root placement can create false dirty-worktree blockers.** A
  control root inside the candidate workspace was detected as an unauthorized
  change. Handoff should validate and select a control root outside the
  workspace before discovery, while retaining the original blocker evidence.
- **Umbrella scope does not prove all repository SPEC roots are covered.** The
  `#82` graph contains 31 SPEC frontiers, but other roots such as `#115`,
  `#119`, `#126`, `#133`, `#157`, and `#193` require an explicit independent
  root inventory and a final coverage check before claiming completion.
- **Large GitHub graphs need progress and bounded readback.** The 94-issue
  graph takes many paginated GitHub calls. Discovery should persist per-page
  progress, expose the last object read, and resume without restarting the
  whole graph after a process exit or transient API failure.
- **Live supervision needs a compact status projection.** The Codex thread can
  remain active while a Runner command has already failed. A small durable
  status file should expose the latest phase, process, blocker, evidence path,
  and next safe action without requiring the full thread transcript.

## Evidence

- Live control root: `D:\\WILL\\STOCK\\stock_advisor\\.codex\\live-acceptance-control`
- Clean discovery: `takeover-discovery-clean.json`, digest
  `b76becf26711d057504744dd70cd86e96a1074aaff589e2c6f453979650bc8d3`
- Initial apply blocker: `working_tree_dirty`
- Clean apply blocker: `takeover_issue_identity_missing`
- Supervised thread: `01a0ec2b-7164-7932-a3d4-589c6e806992`

## Deviations From The Intended Flow

- **The flow has not reached implementation.** The intended sequence was
  `implement-needs -> to-tickets (when needed) -> implement-spec -> tests ->
  review -> merge -> close -> push -> summary`. This run currently stops in
  takeover adoption, after discovery and before any worker, candidate, PR,
  review, merge, close, or push operation.
- **The first failure was a workspace-scope failure.** Control and artifact
  files were initially under the candidate workspace, so the Runner correctly
  classified its own files as unauthorized dirty changes. The control root was
  moved outside the workspace and the original evidence was retained.
- **The second failure was an external identity failure.** Clean discovery
  succeeded, but apply rejected all existing SPEC/ticket Issues because their
  bodies were created before Spec Runner markers existed. The existing Issue
  graph therefore cannot be adopted by the current Runner without an explicit
  identity-reconciliation step.
- **The live thread did not silently disappear.** The wrapper/Runner command
  returned a failed result at the adoption boundary, while the Codex thread
  remained active and continued inspecting the failure. A user message also
  interrupted one supervisor wait; that interrupted the observation call, not
  the worker's durable state. The monitoring UI must distinguish these cases:
  process exit, Runner failure, Codex turn still active, supervisor wait
  interrupted, and genuine thread termination.
- **The initial scope is incomplete for the user's “all SPECs” requirement.**
  The current handoff targets umbrella `#82` and found 31 child SPECs. Separate
  roots were observed but have not yet been adopted or proven unrelated. A
  final result cannot claim all SPECs are complete until those roots are
  explicitly classified and processed.
- **Historical completion evidence is not automatically reusable.** Closed
  Issues and prior PR comments exist, but candidate, check, review, merge,
  closure, cleanup, and target-branch evidence must be bound to the exact
  current Issue identity and revision. The discovery correctly marks these
  frontiers as incomplete when durable receipts are absent.
- **The run has already needed GitHub graph repair.** Stale Parent markers on
  Issues `#83`, `#270`, `#271`, `#275`, `#276`, `#278`, and `#279` conflicted
  with native sub-issue relations. Their original bodies and corrected
  readbacks are retained in the live control artifacts. The normal flow needs
  a non-destructive relation-reconciliation stage before adoption.
- **Evidence serialization can fail after a successful read.** The marker
  repair preflight completed its remote reads, but the PowerShell helper failed
  while writing the local audit JSON because a generic list was serialized in
  the wrong shape. No GitHub edit occurred. Durable helpers should use a
  schema-validated JSON writer, write to a temporary file, read it back, and
  publish a receipt only after the readback succeeds.
- **Preflight is correct but slow.** The 94-target marker preflight completed
  with zero conflicts and zero relation changes, but took about 250 seconds
  because it repeatedly reads body, sub-issues, and blocked-by relations. The
  repair path should cache one immutable graph snapshot and use bounded
  concurrency or a single batched API read, while preserving per-Issue
  readback before mutation.
- **Mutation throughput is also intentionally low.** The repair processes each
  Issue sequentially and performs a relation preflight plus a post-edit body
  readback. This is appropriate for correctness, but a production feature
  should persist a resumable cursor and use bounded parallelism for independent
  Issues, with a repository wide stop gate when any conflict appears.
- **Legacy Issue adoption required an out of band migration step.** The
  controlled repair added durable markers to 94 existing Issues only after
  full body and relation preflight; all 94 readbacks preserved the original
  business content. The intended flow assumes those markers already exist,
  so the system needs a documented migration command or a discovery format
  that can bind legacy Issue identity without rewriting every body.
- **Apply does not automatically mean execution has started.** After marked
  discovery and apply succeeded, the durable queue remained `planned`; a
  normal `drive` call did not advance it because production execution requires
  a worker launch entrypoint. The user-facing handoff should either launch the
  worker as part of apply or return an explicit `planned -> worker_required`
  status and next command, so a live acceptance cannot appear active while no
  implementation worker is running.
- **The first marked apply produced a second durable run without a worker.**
  The control database now shows the original run as `failed` and the marked
  run as `planned`; this is recoverable state, but the handoff did not make the
  transition to an active production worker visible. The acceptance harness
  must assert `worker_started` and a live worker identity immediately after
  apply, or report `planned` as a stopped handoff rather than an active run.
- **The continuation command was semantically incomplete.** `drive` accepted
  the existing launch key and returned successfully, but only replayed the
  durable execution intent; it did not launch the worker or advance the
  planned run. The supervision path must distinguish CLI success from actual
  stage progress and call the Runner's explicit `launch` handshake entry when
  production mode requires a worker.

## Append-only updates

Add new findings under the current date. Keep each entry to the observed
failure or gap, the proposed improvement, and a pointer to durable evidence.

### 2026-09-29 supervision update

- **The live supervisor remains active while the Runner is stopped.** At the
  latest readback, Codex thread `01a0ec2b-7164-7932-a3d4-589c6e806992` is
  active and its current turn is inspecting detached launch construction and
  the production `planned` path. The Runner still has no external thread or
  turn, so implementation has not started. This confirms that thread activity
  alone must not be treated as Runner progress; supervision must correlate
  thread status, Runner status, worker identity, and process state.
  Evidence: Codex wait snapshot revision `211`, latest turn
  `01a0ecc0-6159-7d80-8b9a-379ae6be1979`, and the existing launcher logs under
  `D:\\WILL\\STOCK\\stock_advisor\\.codex\\live-acceptance-control\\launcher-logs`.

- **A successful `drive` call still produces no execution progress.** During
  supervision, the live thread confirmed that the foreground `drive` command
  returned successfully while run `591b32e1-0ed2-4133-bef2-42032c96b1b5`
  remained `planned`; its `codex_planning` step was `pending`, its only worker
  was `pending`, and both external thread and turn identities were null. The
  handoff therefore needs a progress postcondition for `drive` and must report
  a non-progress success as a stopped handoff or blocker. Evidence: Runner
  status readback at 2026-09-29T10:45:07Z, run log
  `D:\\WILL\\STOCK\\stock_advisor\\.codex\\live-acceptance-control\\logs\\591b32e1-0ed2-4133-bef2-42032c96b1b5.jsonl`, and live thread revision `212`.

- **The durable runtime record outlives the Runner process.** At the 10:51
  supervision readback, the run still reported runtime PID `29300`, but no
  matching Runner process existed. The run remained `planned`, with a pending
  worker and null external identities, while the Codex thread continued a
  read-only source trace. Recovery must reconcile stale runtime ownership
  before treating the run as live, and should expose the distinction between a
  stale runtime row and an active process. Evidence: Runner status readback at
  `2026-09-29T17:59:56+08:00`, process enumeration from the same heartbeat,
  and Codex thread revision `213`.

- **The planned continuation branch drops a persisted ticket plan.** The live
  thread isolated the Runner defect: after takeover adoption, the queue finds
  the existing GH-84 ticket plan, assigns it to `ticketed`, but returns while
  the run is still `planned` because it never performs the required transition
  to `tickets_ready`. As a result no worker is created and no failure event is
  emitted, leaving a false successful continuation with no progress. The
  continuation path needs an explicit state transition and a postcondition
  requiring worker creation or a durable blocker. Evidence: Codex thread
  revision `214`, turn `01a0ecc0-6159-7d80-8b9a-379ae6be1979`, Runner status
  still `planned` with a pending worker and zero external identities.

- **Runtime ownership changed without a live process.** The same readback now
  reports runtime PID `42624` updated at `2026-09-29T18:50:11+08:00`, while
  process enumeration found no matching Runner process and the run log has not
  advanced beyond `run_started`. Recovery must validate PID liveness and
  event/log progress before accepting a refreshed runtime claim. Evidence:
  Runner status and process enumeration from the 10:56 heartbeat, plus
  `D:\\WILL\\STOCK\\stock_advisor\\.codex\\live-acceptance-control\\logs\\591b32e1-0ed2-4133-bef2-42032c96b1b5.jsonl`.

- **The first real implementation worker was admitted but rate limited.** The
  Runner finally advanced to `tickets_ready`, created implementation worker
  `codex_sdk:591b32e1-0ed2-4133-bef2-42032c96b1b5:codex_implementation:GH-100`,
  and persisted external thread
  `01a0ecd2-329b-7720-ae51-67048ad56ee6` plus turn
  `01a0ecd2-353b-7ec3-a30f-51f5c783f890`. The SDK then exhausted retries with
  `429 Too Many Requests`; the run entered `failed` and recovery chose
  `observe` with `reconcile_before_retry` because execution outcome was marked
  unknown. No candidate, verification, review, merge, close, or push evidence
  exists. Evidence: Runner events 72-82 and recovery episode
  `episode-d4524347e212b28464a35a546b60d617cad98b3fe470e538e95286b20fbfb8c2`.

- **Worker and operation state projections diverged after the rate limit.** The
  implementation worker is durably `failed`, while its implementation
  operation remains `running`. The recovery observation also labels the fault
  stage `codex_planning` although the run current step and operation are
  `codex_implementation`. Recovery and status reporting must close or reconcile
  the operation and preserve one authoritative stage before retrying. Evidence:
  Runner status at `2026-09-29T18:59:57+08:00`, operations and workers in the
  same readback, and the fault observation in event 79.

- **Reconciliation retried the same implementation thread and exhausted its
  capacity budget.** After the first failed turn was reconciled as idle/failed,
  the Runner started a second turn on the same GH-100 external thread
  (`01a0ecd2-329b-7720-ae51-67048ad56ee6`), turn
  `01a0ecdd-0496-7673-8cfd-426b60c4069d`. It received the same `429 Too Many
  Requests`; the recovery episode reached `capacity_attempts=2` with zero
  capacity retries remaining and the run entered `blocked`. This is a real
  external capacity blocker, and no implementation progress or candidate
  evidence exists. Evidence: Runner events 83-103 and recovery episode
  `episode-81556744e2c9ca6ca85bb9cd15efce65794e7201e11947f8753cda270f299089`.

- **Blocked state still briefly exposed a running worker after the second
  failure.** The durable status read during the heartbeat showed the GH-100
  worker and implementation operation as `running`, while the recovery
  execution owner had already been marked `failed` and the run was `blocked`.
  Recovery must make worker, operation, execution owner, and run state converge
  atomically or expose an explicit transitional state before allowing another
  retry. Evidence: status readback at `2026-09-29T19:11:45+08:00` and events
  95-103.

- **A later reconciliation attempt changed the durable run from `blocked` to
  `failed` without creating any delivery evidence.** At `2026-09-29T19:16:31+08:00`,
  the GH-100 implementation owner was still the same external thread and its
  latest turn `01a0ecde-d531-7e71-94af-a91b50fa19e2` failed with the same
  `429 Too Many Requests`. The Runner now reports run and implementation step
  `failed`, while the supervising Codex thread remains `active` with an
  `inProgress` turn. This leaves the user-facing thread looking alive even
  though the durable delivery run has stopped. Recovery should publish one
  terminal outcome consistently across run, step, worker, operation, and
  supervisor handoff, and must not treat an active supervisor turn as delivery
  progress. No candidate, verification, review, merge, close, or push receipt
  exists. Evidence: Runner status readback, recovery episode
  `episode-81556744e2c9ca6ca85bb9cd15efce65794e7201e11947f8753cda270f299089`,
  run log `D:\\WILL\\STOCK\\stock_advisor\\.codex\\live-acceptance-control\\logs\\591b32e1-0ed2-4133-bef2-42032c96b1b5.jsonl`,
  and Codex wait snapshot revision `218`.

- **The supervising thread completed while the delivery run remained failed.**
  At `2026-09-29T19:21:32+08:00`, Codex thread
  `01a0ec2b-7164-7932-a3d4-589c6e806992` changed to `idle` after its latest
  turn completed. Its final report says the run is waiting for SDK capacity,
  but no new `drive` execution was started. The durable Runner remained
  `failed` at `codex_implementation`, with the same failed GH-100 worker and
  zero verification receipts; process enumeration found no live Runner
  process. This is an acceptance interruption: the supervisor stopped after
  reporting the blocker, and there is no durable handoff or automatic resume
  that keeps the live acceptance active until completion. The workflow needs a
  terminal blocker protocol that either keeps supervision/retry scheduled or
  explicitly marks the acceptance as paused and actionable, rather than
  leaving an idle thread and a failed run. Evidence: Codex wait snapshot
  revision `219`, completed turn
  `01a0ecc0-6159-7d80-8b9a-379ae6be1979`, Runner status readback at
  `2026-09-29T19:21:32+08:00`, and process enumeration from the same check.

- **Heartbeat repair loop confirmed the delivery run is still failed and unsupervised.** At `2026-09-29T12:42:03Z`, run `591b32e1-0ed2-4133-bef2-42032c96b1b5` remained failed at `codex_implementation`; its implementation worker was failed, its implementation operation remained running, the recovery episode was still `observe` after three capacity attempts, and no candidate, verification, review, merge, close, or push receipt existed. The supervisor thread `01a0ec2b-7164-7932-a3d4-589c6e806992` was idle and no Runner process was alive. Root cause is now narrowed to recovery policy precedence plus non-authoritative failure closure: an accepted SDK turn that was read back as failed still takes the `active_execution`/unknown-outcome branch before the exhausted capacity budget, and queue error handling calls `fail_run` with `start:<run_id>` while the implementation operation has another identity. Impact: the heartbeat can report a durable failure but cannot resume it safely or prove stage convergence. Next step: land deterministic regression coverage for implementation-stage 429/readback-failed/exhausted-budget, atomically converge run/step/worker/operation state, run qualification and independent verification, then reconcile and resume the original run. Evidence: live Runner status and events, recovery episode readback, run log `D:\WILL\STOCK\stock_advisor\.codex\live-acceptance-control\logs\591b32e1-0ed2-4133-bef2-42032c96b1b5.jsonl`, Codex thread readback, and process enumeration from this heartbeat.
