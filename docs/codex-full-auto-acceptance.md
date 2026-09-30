# Issue 81 full automatic workflow acceptance evidence

## Ticket #89 recovery evidence

`tests/acceptance/test_automatic_recovery.py` exercises public Temporal seams with a real `WorkflowEnvironment` and `Worker`:

- A ready two-SPEC workflow reaches three Codex child workflows and nine streamed SDK turns without completion signals.
- The worker is stopped after a durable CI retry timer is recorded, the SQLite conversation store is closed and reopened, and a new Worker resumes the same parent execution.
- Parent and child histories are fetched and replayed with `Replayer`; replay does not call the SDK again. The reopened store retains all nine completed turns and rejects events from another run/namespace.
- Publication recovery uses a gateway that creates a remote issue and then raises (lost response). Automatic identity reconciliation adopts the existing issue without creating a duplicate. A readback identity mismatch becomes BLOCKED after the bounded policy.

The SDK in this acceptance is deterministic/instrumented, so it proves worker restart, Temporal replay, durable conversation persistence, stream event mapping, and bounded publication behavior. It is not evidence of an authenticated live Codex service; the separately recorded live probe is required for that claim.
