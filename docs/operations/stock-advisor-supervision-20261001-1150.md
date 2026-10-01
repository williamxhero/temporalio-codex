# Supervision failure - 2026-10-01 11:50:37 UTC

Workflow requirement-delivery-9625431a194d7be1a31c latest execution 6d37e3f1-9613-4f62-a1f6-41de15a8d612 is Temporal COMPLETED/application failed: ticket scheduling did not complete. Input identity unchanged; no completed ticket/SPEC or final delivery evidence.

Initial a791c0f9 execution failed candidate preparation (SHA mismatch). Histories: .tmp/current-failure-a791-history.json and .tmp/current-scheduler-a791-history.json.

This run incorrectly changed shared candidate HEAD to 4666c3a and then original input base 691e0d9 to satisfy preparation. These changes were reversed: HEAD again bc0bf18591697221de48ceecb16168bf1f38c9c7, original untracked .tmp-lead-fix.patch restored. Backup: .tmp/candidate-drift-a791-20261001-1935. Preserve and explicitly bind candidate identity in future recovery; do not roll it back to base.

Official recover-failed Replay/Reset: a791 -> e3045859-c207-49af-910e-bc8353f16c09 at event 120; e304 -> 6d37 at event 123. No active execution reset or duplicate top-level Workflow. First recovery repeated SHA mismatch; second reached planning and heartbeat timed out during SDK initialize. Parent also had Workflow Task timeouts.

Latest histories: .tmp/current-recovery-6d37-history.json, .tmp/failure-6d37-scheduler.json, .tmp/failure-6d37-codex.json. Failed Codex child 01a0f748-2bda-753b-adf0-722835d4b428, planning timeout event 12. Worker log: codex-worker/.tmp/recovery-worker.err.log.

Read-only ledger in codex-worker/.tmp/codex-conversations.sqlite3 contains exact operation fingerprint but null thread_id, turn_id and observation_json. External launch outcome is unknown. Do not reset again without authoritative readback or verified no-side-effect evidence.

No code fix/regression completed. Worker runtime model not independently verified; recovery client environment was gpt-6.1-sol. Next: diagnose SDK initialization/heartbeat and candidate binding before recovery. Recovery receipts are not completion.
