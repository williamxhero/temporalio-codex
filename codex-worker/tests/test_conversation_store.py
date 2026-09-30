from temporalio_codex.conversation_store import ConversationEvent, ConversationStore


def event(
    *,
    workflow_id: str,
    scope_workflow_id: str,
    operation_id: str,
    kind: str,
    text: str = "",
    thread_id: str | None = None,
    turn_id: str | None = None,
    workflow_run_id: str | None = None,
    scope_workflow_run_id: str | None = None,
) -> ConversationEvent:
    return ConversationEvent(
        workflow_id=workflow_id,
        scope_workflow_id=scope_workflow_id,
        operation_id=operation_id,
        stage="planning",
        role="planning",
        thread_id=thread_id,
        turn_id=turn_id,
        kind=kind,
        text=text,
        workflow_run_id=workflow_run_id,
        scope_workflow_run_id=scope_workflow_run_id,
    )


def test_snapshot_groups_input_and_streamed_output_for_parent_workflow(tmp_path) -> None:
    store = ConversationStore(tmp_path / "conversations.sqlite3")
    try:
        store.append(
            event(
                workflow_id="parent:codex:spec-a",
                scope_workflow_id="parent",
                operation_id="op-1",
                kind="user_input",
                text="Plan this requirement",
            )
        )
        store.append(
            event(
                workflow_id="parent:codex:spec-a",
                scope_workflow_id="parent",
                operation_id="op-1",
                kind="turn_started",
                thread_id="thread-1",
                turn_id="turn-1",
            )
        )
        store.append(
            event(
                workflow_id="parent:codex:spec-a",
                scope_workflow_id="parent",
                operation_id="op-1",
                kind="assistant_delta",
                text="Plan complete",
                thread_id="thread-1",
                turn_id="turn-1",
            )
        )
        store.append(
            event(
                workflow_id="parent:codex:spec-a",
                scope_workflow_id="parent",
                operation_id="op-1",
                kind="turn_completed",
                thread_id="thread-1",
                turn_id="turn-1",
            )
        )

        child_snapshot = store.snapshot("parent:codex:spec-a")
        parent_snapshot = store.snapshot("parent")

        assert child_snapshot == parent_snapshot
        conversation = parent_snapshot["conversations"][0]
        assert conversation["threadId"] == "thread-1"
        assert conversation["turns"] == [
            {
                "id": "op-1",
                "turnId": "turn-1",
                "input": "Plan this requirement",
                "output": "Plan complete",
                "working": [],
                "status": "completed",
                "updatedAt": conversation["turns"][0]["updatedAt"],
            }
        ]
    finally:
        store.close()


def test_snapshot_keeps_multiple_turns_in_one_codex_thread(tmp_path) -> None:
    store = ConversationStore(tmp_path / "conversations.sqlite3")
    try:
        for operation_id, turn_id, prompt, response in (
            ("op-1", "turn-1", "first prompt", "first response"),
            ("op-2", "turn-2", "second prompt", "second response"),
        ):
            store.append(
                event(
                    workflow_id="workflow",
                    scope_workflow_id="workflow",
                    operation_id=operation_id,
                    kind="user_input",
                    text=prompt,
                    thread_id="thread-1",
                    turn_id=turn_id,
                )
            )
            store.append(
                event(
                    workflow_id="workflow",
                    scope_workflow_id="workflow",
                    operation_id=operation_id,
                    kind="assistant_final",
                    text=response,
                    thread_id="thread-1",
                    turn_id=turn_id,
                )
            )

        snapshot = store.snapshot("workflow")

        assert len(snapshot["conversations"]) == 1
        assert [turn["input"] for turn in snapshot["conversations"][0]["turns"]] == [
            "first prompt",
            "second prompt",
        ]
        assert [turn["output"] for turn in snapshot["conversations"][0]["turns"]] == [
            "first response",
            "second response",
        ]
    finally:
        store.close()


def test_snapshot_filters_workflow_execution_and_parent_execution(tmp_path) -> None:
    store = ConversationStore(tmp_path / "conversations.sqlite3")
    try:
        for run_id, text in (("run-1", "first"), ("run-2", "second")):
            store.append(
                event(
                    workflow_id="workflow",
                    scope_workflow_id="parent",
                    workflow_run_id=run_id,
                    scope_workflow_run_id=run_id,
                    operation_id=f"op-{run_id}",
                    kind="user_input",
                    text=text,
                )
            )

        run_one = store.snapshot("workflow", "run-1")
        parent_run_two = store.snapshot("parent", "run-2")
        all_runs = store.snapshot("workflow")

        assert [
            turn["input"]
            for conversation in run_one["conversations"]
            for turn in conversation["turns"]
        ] == ["first"]
        assert [
            turn["input"]
            for conversation in parent_run_two["conversations"]
            for turn in conversation["turns"]
        ] == ["second"]
        assert [
            turn["input"]
            for conversation in all_runs["conversations"]
            for turn in conversation["turns"]
        ] == ["first", "second"]
    finally:
        store.close()


def test_snapshot_excludes_events_without_run_scope_for_execution_queries(tmp_path) -> None:
    store = ConversationStore(tmp_path / "conversations.sqlite3")
    try:
        store.append(
            event(
                workflow_id="workflow",
                scope_workflow_id="parent",
                operation_id="legacy-op",
                kind="user_input",
                text="legacy",
            )
        )
        store.append(
            event(
                workflow_id="workflow",
                scope_workflow_id="parent",
                workflow_run_id="run-1",
                scope_workflow_run_id="parent-run-1",
                operation_id="current-op",
                kind="user_input",
                text="current",
            )
        )

        snapshot = store.snapshot("workflow", "run-1")

        assert [
            turn["input"]
            for conversation in snapshot["conversations"]
            for turn in conversation["turns"]
        ] == ["current"]
    finally:
        store.close()
