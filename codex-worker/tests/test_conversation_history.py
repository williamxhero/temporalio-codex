from dataclasses import replace

import pytest

from test_conversation_store import event
from temporalio_codex.conversation_store import ConversationStore


def test_import_preserves_history_and_completed_operation_claims(tmp_path):
    source = ConversationStore(tmp_path / "old.db")
    target = ConversationStore(tmp_path / "new.db")
    key = ("default", "codex", "run", "op")
    try:
        source.claim_operation(key, "fingerprint")
        source.record_operation(key, thread_id="thread", turn_id="turn", observation={"status": "completed"})
        source.append(event(workflow_id="codex", workflow_run_id="run", scope_workflow_id="parent",
                            scope_workflow_run_id="parent-run", operation_id="op", kind="assistant_final", text="recorded"))
        assert target.import_history(source.path) == {"events": 1, "operations": 1}
        assert target.import_history(source.path) == {"events": 0, "operations": 0}
        claimed, record = target.claim_operation(key, "fingerprint")
        assert claimed is False
        assert record["thread_id"] == "thread"
        assert record["observation_json"]
        assert target.snapshot("codex", "run")["conversations"]
        assert target.snapshot("parent", "parent-run")["conversations"]
        assert not target.snapshot("codex", "another-run")["conversations"]
    finally:
        source.close()
        target.close()


def test_conflicting_ledger_rolls_back_entire_import(tmp_path):
    source = ConversationStore(tmp_path / "old.db")
    target = ConversationStore(tmp_path / "new.db")
    try:
        source.claim_operation(("default", "codex", "run", "first"), "safe")
        key = ("default", "codex", "run", "conflict")
        source.claim_operation(key, "source")
        target.claim_operation(key, "target")
        with pytest.raises(ValueError, match="conflicting operation"):
            target.import_history(source.path)
        assert target.claim_operation(("default", "codex", "run", "first"), "safe")[0] is True
    finally:
        source.close()
        target.close()


def test_scheduler_scope_uses_actual_child_execution_without_cross_run_leak(tmp_path):
    store = ConversationStore(tmp_path / "events.db")
    try:
        base = event(workflow_id="codex", workflow_run_id="child-run", scope_workflow_id="parent",
                     scope_workflow_run_id="parent-run", operation_id="op", kind="assistant_final", text="answer")
        store.append(base)
        store.append(replace(base, namespace="another", text="hidden"))
        assert not store.snapshot("scheduler", "scheduler-run")["conversations"]
        store.link_execution("scheduler", "scheduler-run", "codex", "child-run")
        snapshot = store.snapshot("scheduler", "scheduler-run")
        assert len(snapshot["conversations"]) == 1
        assert snapshot["conversations"][0]["turns"][0]["output"] == "answer"
        assert not store.snapshot("scheduler", "another-run")["conversations"]
    finally:
        store.close()
