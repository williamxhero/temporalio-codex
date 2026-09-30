# temporalio-codex Execution Log

## 2026-09-29

### 20:xx Asia/Singapore - continuation assessment

- Request: continue `codex://threads/01a0ec2b-7164-7932-a3d4-589c6e806992` with `/finish-needs`.
- Observed legacy run: `591b32e1-0ed2-4133-bef2-42032c96b1b5`.
- Observed legacy system: `spec-runner` takeover, not a Temporal workflow.
- Existing source scope: `williamxhero/stock_advisor`, Issue `#82`, ref `refs/heads/master`.
- Temporal check: `RequirementDeliveryWorkflow` accepts a complete `WholeFlowInput`; its public request has no legacy run ID, takeover snapshot, or adoption contract.
- Temporal check: existing adapter idempotency adopts matching operation identities for retries, but does not convert a legacy `spec-runner` run into a Temporal run.
- Decision: do not attach, rename, or resume the legacy run through Temporal. Treat it as immutable historical evidence.
- Recovery plan: build a new Temporal request from Issue `#82` and the thread as a historical source, obtain a new durable Temporal `run_id`, and record every subsequent query/control/recovery event here.
- Status: `not_started`; no Temporal launch performed yet.

### 20:xx Asia/Singapore - target repository compatibility check

- Blocker: the current worker configures GitHub adapters from `williamxhero/temporalio-codex`, and `RequirementDeliveryWorkflow` currently constructs ticket publication input with that same repository value.
- Target mismatch: the legacy thread targets `williamxhero/stock_advisor`.
- Risk: launching the new workflow before fixing this would publish or verify artifacts against the wrong repository.
- Resolution required: make the target repository an explicit, validated workflow/activity input, propagate it through planning, ticket publication, delivery, and summary adapters, then add a cross-repository acceptance test.
- Resolution applied: `WholeFlowInput.repository` now propagates to planning and ticket publication; entry request parsing binds the request repository; worker GitHub adapters accept `TEMPORALIO_CODEX_REPOSITORY`.
- Verification: targeted entry/planning/whole-flow tests passed, `16 passed`; Python source compilation passed.
- Remaining blocker: the old Issue #82 graph has not yet been converted into the complete `WholeFlowInput` plan required by the Temporal entry contract. No Temporal run was started.
- Status: `blocked_before_launch`, with repository routing fixed.

### 20:xx Asia/Singapore - runtime availability check

- Observed: no `temporal` executable was available on PATH and no identifiable Temporal server/worker process was running.
- Trigger: launch prerequisites are absent; starting a request would not produce a durable workflow execution.
- Action: did not launch or fabricate a run ID; retained the blocker and test evidence in this log.
- Resolution required: start Temporal server and register the worker on the selected task queue, then construct and validate the complete request.

### 2026-09-29 - supervised Temporal thread created

- New thread: `01a0edac-e92c-7110-89f7-706b11913864`.
- Supervisor: current coordination thread; all progress will be checked from durable Temporal status and recorded here.
- Execution contract: target `williamxhero/stock_advisor`, umbrella Issue `#82`, ref `refs/heads/master`, check `project-regression`.
- Hard boundary: the supervised thread must use `RequirementDeliveryWorkflow` and `temporalio-codex-delivery`; legacy `spec-runner` run remains historical evidence.
- Status: `created`, awaiting first durable execution update.

### 2026-09-29 - project placement corrected

- Correction: the first supervised thread `01a0edac-e92c-7110-89f7-706b11913864` was created under the wrong project and must not execute the task.
- Replacement thread: `01a0edb9-7614-7a03-add9-a7f34204b9d9`, created under the `stock_advisor` project.
- The old thread archive request failed because the Codex app could not determine the account for worktree cleanup; it has no authority to run the workflow and is superseded.
- Status: replacement thread active; supervision follows `01a0edb9-7614-7a03-add9-a7f34204b9d9`.

## 2026-09-30

### 07:27 Asia/Singapore - supervised thread interrupted before launch

- Thread: `01a0edb9-7614-7a03-add9-a7f34204b9d9` (`stock_advisor` project).
- Observation: Codex thread status is `notLoaded`; its first turn is `interrupted` with no reported error. Last completed work read the Temporal request/workflow source and Issue `#82`; last message was narrowing the request schema, acceptance graph, and runtime check.
- Temporal status: no launch receipt or durable `run_id` was found in the thread or execution log. A Temporal workflow is therefore not claimed to be running.
- Trigger: the app reports an interrupted turn; root cause is not provided by the thread status.
- Action: send a continuation to the same thread. The current supervising thread owns this append-only log; the execution thread should focus on the Temporal workflow and report durable receipts and blockers.
- Recovery result: continuation completed and launched a new Temporal workflow.

### 2026-09-30 - Temporal launch receipt

- Supervisor thread: `01a0edb9-7614-7a03-add9-a7f34204b9d9`.
- `run_id`: `requirement-delivery-9625431a194d7be1a31c`.
- `input_identity`: `c0e3012e8bbb5d62272438c0619eb693fff34c1fe65e782c41c67bdc5a010592`.
- Phase/status: `planning / active`.
- Scope: 31 SPECs and 62 canonical tickets.
- Repository/ref: `williamxhero/stock_advisor` / `refs/heads/master`.
- Runtime: Temporal server and worker healthy on `localhost:7233`, task queue `codex-worker`.
- Legacy boundary: old Spec Runner run was not resumed or adopted.

### 2026-09-30 - Web UI availability diagnosis

- User report: `http://localhost:8233/` returned `ERR_CONNECTION_REFUSED`.
- Evidence: Temporal server was listening on `127.0.0.1:7233`; `temporalio-codex-delivery status` still returned run `requirement-delivery-9625431a194d7be1a31c` as `planning / active`.
- Cause: server process command line was `temporal server start-dev --headless --db-filename ...`; headless mode intentionally does not expose the Web UI port `8233`.
- Recovery action: restart the same Temporal server with the same persistent database and without `--headless`; preserve the worker and workflow identity.

### 2026-09-30 - Web UI recovery

- First restart attempt: failed because Windows rejected binding `8233` (`WSAEACCES`); this port is inside an excluded TCP range `8198-8297`.
- Second attempt on `8234`: failed for the same excluded-range reason.
- Recovery: restarted Temporal server with the same database `.tmp/temporal.db` and UI port `18000`.
- Verified: Temporal gRPC `7233` is listening, UI `18000` is listening, HTTP request to `http://localhost:18000/` returned `200`.
- Workflow verification after restart: `requirement-delivery-9625431a194d7be1a31c` remains `planning / active`; completed SPECs remain empty; next action is `complete Grill and publish SPEC Issues`.
- Current UI URL: `http://localhost:18000/`.

### 2026-09-30 - planning child blocked on SPEC publication readback

- Durable child query: `requirement-delivery-9625431a194d7be1a31c:planning` reports phase `ready`, status `unknown`, `publication_requested=true`, `published_specs=[]`, reason `SPEC publication requires readback: TypeError`.
- Grill state: required question 1 was answered; `confirmed=true`; the workflow is not waiting for a user Grill answer.
- Parent query remains `planning / active` with `next_action=complete Grill and publish SPEC Issues`, which hides the child `unknown` state.
- Reproduction: `GhCliSpecIssueGateway.find_by_operation` on the same repository and operation identity raised `UnicodeDecodeError` in Python's Windows subprocess reader using GBK for Chinese GitHub JSON; the subsequent `json.loads(None)` raised `TypeError`.
- Read-only GitHub search for the Temporal publication operation marker returned no Issues. This is an incomplete check and should be repeated through the gateway after UTF-8 repair before any retry.
- Resolution: make GitHub subprocess output decode as UTF-8 explicitly, verify operation identity readback, then recover the `unknown` child through a durable Temporal control path; do not publish blindly or restart the workflow with a new identity.

### 2026-09-30 - workflow liveness design gap identified

- User observation: a child workflow in `unknown` can wait forever for manual resolution while the parent remains `active`.
- Assessment: confirmed design defect. External readback uncertainty needs a bounded wait and a durable terminal/blocked outcome; parent status must surface the child reason.
- Requested remediation: explicit timeout, queryable blocked/not_verified reason, parent propagation, and tests for success, recovery, timeout, and failure.
- Action: sent to supervised thread for implementation alongside the UTF-8 repair; current run identity must be preserved.

### 2026-09-30 - repair turn interrupted by model capacity

- Supervised thread: `01a0edb9-7614-7a03-add9-a7f34204b9d9`.
- Intended work: patch UTF-8 GitHub subprocess decoding, add bounded publication wait, propagate child blocker, and test.
- Result: turn failed before implementation with `Selected model is at capacity. Please try a different model.`
- Durable workflow remains unchanged: parent `planning / active`; child `ready / unknown`; run `requirement-delivery-9625431a194d7be1a31c`.
- Recovery: retry the same thread with another available model; preserve the existing run and operation identities.

### 2026-09-30 - model capacity is a retryable supervisor fault

- Policy decision: `Selected model is at capacity` must not terminate supervision or be treated as a Temporal workflow failure.
- Scope: this was a Codex thread turn failure, while the Temporal run remained durable and `planning / active`.
- Recovery policy: retry the same task with a locked same-or-stronger available model after route/readback validation; preserve thread identity, Temporal `run_id`, operation identities, and repository scope.
- Context handling: content compaction is a separate context-length safeguard and does not replace capacity fallback.
