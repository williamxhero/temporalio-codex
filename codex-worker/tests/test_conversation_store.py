from dataclasses import replace

import pytest
from temporalio_codex.conversation_store import ConversationEvent, ConversationStore


@pytest.mark.parametrize("command,name", [
    ('"C:\\' + 'very-long-directory\\' * 12 + 'pwsh.exe" -Command "git status"', "pwsh.exe"),
    ("'/usr/local/bin/python' script.py", "python"),
    ("git status --short", "git"),
])
def test_compact_command_name_survives_summary_truncation(tmp_path, command, name):
    store = ConversationStore(tmp_path / "command-name.db")
    try:
        store.append(replace(
            event(workflow_id="workflow", scope_workflow_id="workflow", operation_id="op", kind="tool_delta"),
            text="command output",
            detail={"command": command, "item_type": "commandExecution", "method": "item/completed", "exit_code": 0},
        ))
        turn = store.snapshot("workflow", include_timeline=True)["conversations"][0]["turns"][0]
        activity = turn["messages"][0]["activity"]
        assert activity["commandName"] == name
        assert "detail" not in activity
        assert activity["text"] == ""
        full = store.snapshot("workflow", include_timeline=True, include_details=True)["conversations"][0]["turns"][0]
        assert full["messages"][0]["activity"]["detail"]["command"] == command
    finally:
        store.close()


@pytest.mark.parametrize("final", [None, "**Result**\n\nFull answer\nwith details."])
def test_failed_turn_displays_cause_and_preserves_raw_error(tmp_path, final):
    store = ConversationStore(tmp_path / "failure.db")
    base = event(
        workflow_id="workflow",
        scope_workflow_id="workflow",
        operation_id="op",
        kind="error",
    )
    raw = "\nConnection lost\nTraceback (most recent call last):\n  private_path.py:42"
    try:
        if final:
            store.append(replace(base, kind="assistant_final", text=final))
        store.append(replace(base, text=raw))
        turn = store.snapshot("workflow")["conversations"][0]["turns"][0]
        assert turn["status"] == "failed"
        assert turn["output"] == (final or raw)
        assert turn["displayOutput"] == (final or "Connection lost")
        assert turn["activities"][0]["summary"] == "Connection lost"
        assert turn["activities"][0]["text"] == raw
    finally:
        store.close()


@pytest.mark.parametrize(
    ("code", "status", "result", "expected"),
    [
        (
            0,
            "completed",
            "===== 12 passed, 1 skipped in 0.3s =====",
            "pytest tests -vv | 12 passed, 1 skipped | Passed (exit 0)",
        ),
        (
            1,
            "failed",
            "===== 2 failed, 10 passed in 0.3s =====",
            "pytest tests -vv | 2 failed, 10 passed | Failed (exit 1)",
        ),
        (None, "inProgress", "", "pytest tests -vv | inProgress"),
        (True, "completed", "", "pytest tests -vv | completed"),
    ],
)
def test_test_activity_exposes_compact_result_with_full_evidence(
    tmp_path, code, status, result, expected
):
    store = ConversationStore(tmp_path / "results.db")
    text = "pytest tests -vv\n" + result
    try:
        store.append(
            replace(
                event(
                    workflow_id="workflow",
                    scope_workflow_id="workflow",
                    operation_id="op",
                    kind="tool_delta",
                    text=text,
                ),
                detail={
                    "method": "item/completed",
                    "item_type": "commandExecution",
                    "command": "pytest tests -vv",
                    "exit_code": code,
                    "status": status,
                    "aggregated_output": result,
                },
            )
        )
        activity = store.snapshot("workflow")["conversations"][0]["turns"][0][
            "activities"
        ][0]
        assert activity["summary"] == expected
        assert activity["text"] == text
        assert activity["detail"]["aggregated_output"] == result
    finally:
        store.close()


def test_file_activity_counts_all_changes_and_bounds_visible_paths(tmp_path):
    paths = [
        "src/" + "long-directory/" * 20 + name
        for name in ("first.py", "second.py", "third.py", "fourth.py")
    ]
    raw = "Files changed: " + ", ".join(paths)
    store = ConversationStore(tmp_path / "files.db")
    try:
        store.append(
            replace(
                event(
                    workflow_id="workflow",
                    scope_workflow_id="workflow",
                    operation_id="op",
                    kind="tool_delta",
                    text=raw,
                ),
                detail={
                    "method": "item/completed",
                    "item_type": "fileChange",
                    "paths": paths,
                },
            )
        )
        activity = store.snapshot("workflow")["conversations"][0]["turns"][0][
            "activities"
        ][0]
        assert (
            activity["summary"]
            == "Files changed (4): first.py, second.py, third.py (+1 more)"
        )
        assert activity["text"] == raw
        assert activity["detail"]["paths"] == paths
    finally:
        store.close()


@pytest.mark.parametrize("command", [None, [], {"invalid": True}])
def test_blank_tool_output_with_malformed_command_remains_readable(tmp_path, command):
    store = ConversationStore(tmp_path / "malformed.db")
    try:
        store.append(
            replace(
                event(
                    workflow_id="workflow",
                    scope_workflow_id="workflow",
                    operation_id="op",
                    kind="tool_delta",
                    text="\n",
                ),
                detail={
                    "command": command,
                    "item_type": "commandExecution",
                    "status": "failed",
                },
            )
        )
        activity = store.snapshot("workflow")["conversations"][0]["turns"][0][
            "activities"
        ][0]
        assert activity["summary"] == "Command | failed"
    finally:
        store.close()


@pytest.mark.parametrize(
    ("kind", "item_type", "text"),
    [
        ("reasoning_delta", "reasoning", "Public summary"),
        ("tool_delta", "fileChange", "Files changed: src/main.py"),
        ("tool_delta", "mcpToolCall", "search.query"),
        ("plan_delta", "plan", "New plan"),
    ],
)
def test_legacy_command_stream_survives_unrelated_completed_items(
    tmp_path, kind, item_type, text
):
    store = ConversationStore(tmp_path / "legacy.db")
    base = event(
        workflow_id="workflow",
        scope_workflow_id="workflow",
        operation_id="op",
        kind="tool_delta",
    )
    try:
        store.append(
            replace(
                base,
                text="valuable stdout",
                detail={"method": "item/commandExecution/outputDelta"},
            )
        )
        store.append(
            replace(
                base,
                kind=kind,
                text=text,
                detail={"method": "item/completed", "item_type": item_type},
            )
        )
        activities = store.snapshot("workflow")["conversations"][0]["turns"][0][
            "activities"
        ]
        assert len(activities) == 2
        assert activities[0]["text"] == "valuable stdout"
        assert activities[1]["text"] == text
    finally:
        store.close()


def test_plan_snapshot_shows_latest_status_and_deltas_still_stream(tmp_path):
    store = ConversationStore(tmp_path / "plan.db")
    base = event(
        workflow_id="workflow",
        scope_workflow_id="workflow",
        operation_id="op",
        kind="plan_delta",
    )
    try:
        store.append(
            replace(
                base,
                text="Implement: inProgress",
                detail={"method": "turn/plan/updated"},
            )
        )
        store.append(
            replace(
                base,
                kind="tool_delta",
                text="git status",
                detail={"method": "item/completed", "item_type": "commandExecution"},
            )
        )
        store.append(
            replace(
                base,
                text="Implement: completed",
                detail={"method": "turn/plan/updated"},
            )
        )
        store.append(
            replace(
                base,
                text="Next ",
                detail={"method": "item/plan/delta", "item_id": "plan-2"},
            )
        )
        store.append(
            replace(
                base,
                text="step",
                detail={"method": "item/plan/delta", "item_id": "plan-2"},
            )
        )
        plans = [
            a
            for a in store.snapshot("workflow")["conversations"][0]["turns"][0][
                "activities"
            ]
            if a["category"] == "plan"
        ]
        assert [a["text"] for a in plans] == ["Implement: completed", "Next step"]
        assert "inProgress" not in str(plans)
    finally:
        store.close()


@pytest.mark.parametrize("status", ["inProgress", "provider status " * 30])
def test_command_summary_stays_bounded_without_losing_raw_evidence(tmp_path, status):
    store = ConversationStore(tmp_path / "bounded.db")
    command = "python script.py " + "long-argument " * 100
    try:
        store.append(
            replace(
                event(
                    workflow_id="workflow",
                    scope_workflow_id="workflow",
                    operation_id="op",
                    kind="tool_delta",
                    text=command,
                ),
                detail={
                    "item_type": "commandExecution",
                    "command": command,
                    "status": status,
                },
            )
        )
        activity = store.snapshot("workflow")["conversations"][0]["turns"][0][
            "activities"
        ][0]
        assert len(activity["summary"]) <= 160
        assert activity["summary"].startswith("python script.py")
        assert activity["text"] == command
        assert activity["detail"]["status"] == status
    finally:
        store.close()


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


def test_snapshot_groups_input_and_streamed_output_for_parent_workflow(
    tmp_path,
) -> None:
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
                "displayOutput": "Plan complete",
                "verificationMarkers": [],
                "working": [],
                "activities": [],
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


def test_thread_snapshot_preserves_prompt_activity_and_response_order(tmp_path) -> None:
    store = ConversationStore(tmp_path / "timeline.db")
    try:
        base = {
            "workflow_id": "workflow",
            "scope_workflow_id": "workflow",
            "workflow_run_id": "run-1",
            "scope_workflow_run_id": "run-1",
            "operation_id": "op-1",
            "stage": "implementation",
            "role": "implementation",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
        }
        store.append(ConversationEvent(**base, kind="user_input", text="prompt"))
        store.append(
            ConversationEvent(
                **base,
                kind="assistant_delta",
                text="before ",
                event_id="assistant-1",
            )
        )
        store.append(
            ConversationEvent(
                **base,
                kind="tool_delta",
                text="read README.md",
                detail={"method": "item/completed", "item_type": "commandExecution"},
                event_id="tool-1",
            )
        )
        store.append(
            ConversationEvent(
                **base,
                kind="assistant_final",
                text="after",
                event_id="assistant-final",
            )
        )

        full_turn = store.snapshot(
            "workflow",
            "run-1",
            include_timeline=True,
            include_details=True,
        )["conversations"][0]["turns"][0]
        assert [
            (message["type"], message.get("text")) for message in full_turn["messages"]
        ] == [
            ("user", "prompt"),
            ("assistant", "before "),
            ("activity", None),
            ("assistant", "after"),
        ]
        assert full_turn["messages"][2]["activity"]["id"] == "tool-1"

        compact_turn = store.snapshot("workflow", "run-1", include_timeline=True)[
            "conversations"
        ][0]["turns"][0]
        assert compact_turn["messages"][0]["text"] == ""
        assert compact_turn["messages"][2]["activity"]["text"] == ""
        assert full_turn["messages"][0]["text"] == "prompt"
        assert full_turn["messages"][2]["activity"]["text"] == "read README.md"
    finally:
        store.close()


def test_consecutive_reasoning_is_grouped_on_separate_lines(tmp_path) -> None:
    store = ConversationStore(tmp_path / "reasoning-timeline.db")
    base = {
        "workflow_id": "workflow",
        "scope_workflow_id": "workflow",
        "workflow_run_id": "run-1",
        "scope_workflow_run_id": "run-1",
        "operation_id": "op-1",
        "stage": "implementation",
        "role": "implementation",
        "thread_id": "thread-1",
        "turn_id": "turn-1",
    }
    try:
        store.append(
            ConversationEvent(
                **base,
                kind="reasoning_delta",
                text="Inspect the current state",
                event_id="reasoning-1",
            )
        )
        store.append(
            ConversationEvent(
                **base,
                kind="reasoning_delta",
                text="Then verify the result",
                event_id="reasoning-2",
            )
        )

        turn = store.snapshot(
            "workflow", "run-1", include_timeline=True, include_details=True
        )["conversations"][0]["turns"][0]
        assert len(turn["activities"]) == 1
        assert turn["activities"][0]["summary"] == (
            "Inspect the current state\nThen verify the result"
        )
        assert turn["activities"][0]["text"] == (
            "Inspect the current state\nThen verify the result"
        )
        assert turn["working"][0]["text"] == (
            "Inspect the current state\nThen verify the result"
        )
        assert len(turn["messages"]) == 1
    finally:
        store.close()


def test_thread_snapshot_cache_reuses_stable_execution_and_invalidates_on_append(
    tmp_path,
):
    store = ConversationStore(tmp_path / "snapshot-cache.db")
    try:
        first_event = ConversationEvent(
            workflow_id="workflow",
            scope_workflow_id="workflow",
            operation_id="op-1",
            stage="implementation",
            role="implementation",
            thread_id="thread-1",
            turn_id="turn-1",
            kind="user_input",
            text="prompt",
            workflow_run_id="run-1",
            scope_workflow_run_id="run-1",
        )
        store.append(first_event)

        first = store.snapshot("workflow", "run-1", include_timeline=True)
        assert store.snapshot("workflow", "run-1", include_timeline=True) is first

        store.append(
            replace(
                first_event,
                kind="assistant_final",
                text="response",
                event_id="response",
            )
        )
        updated = store.snapshot("workflow", "run-1", include_timeline=True)

        assert updated is not first
        assert updated["cursor"] > first["cursor"]
        assert (
            updated["conversations"][0]["turns"][0]["messages"][-1]["text"]
            == "response"
        )
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


def test_snapshot_excludes_events_without_run_scope_for_execution_queries(
    tmp_path,
) -> None:
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


def test_namespace_run_identity_and_authoritative_final(tmp_path):
    store = ConversationStore(tmp_path / "events.db")
    base = event(
        workflow_id="child",
        scope_workflow_id="parent",
        operation_id="op",
        workflow_run_id="child-run",
        scope_workflow_run_id="parent-run",
        kind="assistant_delta",
        text="part",
    )
    try:
        first = store.append(replace(base, event_id="same"))
        assert store.append(replace(base, event_id="same")) == first
        store.append(replace(base, namespace="other", event_id="same", text="foreign"))
        store.append(
            replace(base, workflow_run_id="another", event_id="same", text="old")
        )
        store.append(replace(base, kind="assistant_final", text="complete answer"))
        assert (
            store.snapshot("child", "child-run")["conversations"][0]["turns"][0][
                "output"
            ]
            == "complete answer"
        )
        assert store.snapshot("parent", "wrong")["conversations"] == []
        assert (
            store.snapshot("child", "child-run", namespace="other")["conversations"][0][
                "turns"
            ][0]["output"]
            == "foreign"
        )
    finally:
        store.close()


def test_legacy_database_migrates_without_leaking_unscoped_rows(tmp_path):
    import sqlite3

    path = tmp_path / "legacy.db"
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE codex_conversation_events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id TEXT NOT NULL,
        scope_workflow_id TEXT NOT NULL, operation_id TEXT NOT NULL, stage TEXT NOT NULL,
        role TEXT NOT NULL, thread_id TEXT, turn_id TEXT, kind TEXT NOT NULL,
        text TEXT NOT NULL, detail_json TEXT NOT NULL, created_at REAL NOT NULL)"""
    )
    connection.execute(
        "INSERT INTO codex_conversation_events(workflow_id,scope_workflow_id,operation_id,stage,role,kind,text,detail_json,created_at) VALUES ('child','parent','old','planning','planning','user_input','legacy','{}',1)"
    )
    connection.commit()
    connection.close()
    store = ConversationStore(path)
    try:
        assert (
            store.snapshot("child")["conversations"][0]["turns"][0]["input"] == "legacy"
        )
        assert store.snapshot("child", "current")["conversations"] == []
        assert store.snapshot("child", namespace="other")["conversations"] == []
    finally:
        store.close()


def test_snapshot_exposes_readable_activity_summaries_and_preserves_details(tmp_path):
    store = ConversationStore(tmp_path / "activities.db")
    try:
        common = {
            "workflow_id": "workflow",
            "scope_workflow_id": "workflow",
            "operation_id": "op-1",
            "workflow_run_id": "run-1",
            "scope_workflow_run_id": "run-1",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
        }
        store.append(event(kind="plan_delta", text="inspect files", **common))
        store.append(event(kind="plan_delta", text="run tests", **common))
        store.append(
            ConversationEvent(
                **common,
                stage="implementation",
                role="implementation",
                kind="tool_delta",
                text="uv run pytest tests/acceptance",
                detail={"method": "command/exec/outputDelta", "exit_code": 0},
                event_id="command-1",
            )
        )
        store.append(
            ConversationEvent(
                **common,
                stage="implementation",
                role="implementation",
                kind="tool_delta",
                text="SDK_PROBE_OK",
                detail={"method": "command/exec/outputDelta", "probe": True},
                event_id="probe-1",
            )
        )

        turn = store.snapshot("workflow", "run-1")["conversations"][0]["turns"][0]

        assert turn["activities"] == [
            {
                "id": "event:1",
                "category": "plan",
            "summary": "inspect files\nrun tests",
            "text": "inspect files\nrun tests",
            },
            {
                "id": "command-1",
                "category": "test",
                "summary": "uv run pytest tests/acceptance | Passed (exit 0)",
                "text": "uv run pytest tests/acceptance",
                "detail": {"method": "command/exec/outputDelta", "exit_code": 0},
            },
            {
                "id": "probe-1",
                "category": "verification",
                "summary": "SDK_PROBE_OK",
                "text": "SDK_PROBE_OK",
                "detail": {"method": "command/exec/outputDelta", "probe": True},
            },
        ]
        assert turn["working"] == [
            {"kind": "plan", "text": "inspect files\nrun tests"},
            {"kind": "tool", "text": "uv run pytest tests/acceptance\nSDK_PROBE_OK"},
        ]
    finally:
        store.close()


def test_snapshot_classifies_malformed_activity_details_without_failing(tmp_path):
    store = ConversationStore(tmp_path / "malformed-activities.db")
    try:
        store.append(
            ConversationEvent(
                workflow_id="workflow",
                scope_workflow_id="workflow",
                operation_id="op-1",
                workflow_run_id="run-1",
                scope_workflow_run_id="run-1",
                thread_id="thread-1",
                turn_id="turn-1",
                stage="implementation",
                role="implementation",
                kind="tool_delta",
                text="open src/main.py",
                detail={"method": None, "path": "src/main.py"},
                event_id="file-1",
            )
        )
        store.append(
            ConversationEvent(
                workflow_id="workflow",
                scope_workflow_id="workflow",
                operation_id="op-1",
                workflow_run_id="run-1",
                scope_workflow_run_id="run-1",
                thread_id="thread-1",
                turn_id="turn-1",
                stage="implementation",
                role="implementation",
                kind="error",
                text="tool failed",
                detail="not-an-object",
                event_id="error-1",
            )
        )

        activities = store.snapshot("workflow", "run-1")["conversations"][0]["turns"][
            0
        ]["activities"]

        assert activities == [
            {
                "id": "file-1",
                "category": "file",
                "summary": "open src/main.py",
                "text": "open src/main.py",
                "detail": {"method": None, "path": "src/main.py"},
            },
            {
                "id": "error-1",
                "category": "error",
                "summary": "tool failed",
                "text": "tool failed",
            },
        ]
    finally:
        store.close()


def test_streamed_command_output_is_replaced_by_completed_item(tmp_path):
    store = ConversationStore(tmp_path / "streamed-command.db")
    try:
        base = event(
            workflow_id="workflow",
            scope_workflow_id="workflow",
            workflow_run_id="run-1",
            scope_workflow_run_id="run-1",
            operation_id="op-1",
            kind="tool_delta",
            text="part ",
        )
        store.append(
            replace(
                base,
                detail={
                    "method": "item/commandExecution/outputDelta",
                    "item_id": "item-1",
                },
                event_id="delta-1",
            )
        )
        store.append(
            replace(
                base,
                text="output",
                detail={
                    "method": "item/commandExecution/outputDelta",
                    "item_id": "item-1",
                },
                event_id="delta-2",
            )
        )
        completed = replace(
            base,
            text="uv run pytest tests\npart output",
            detail={
                "method": "item/completed",
                "item_id": "item-1",
                "item_type": "commandExecution",
                "command": "uv run pytest tests",
                "exit_code": 0,
            },
            event_id="complete-1",
        )
        store.append(completed)
        store.append(completed)

        activities = store.snapshot("workflow", "run-1")["conversations"][0]["turns"][
            0
        ]["activities"]
        assert activities == [
            {
                "id": "delta-1",
                "category": "test",
                "summary": "uv run pytest tests | Passed (exit 0)",
                "text": "uv run pytest tests\npart output",
                "detail": completed.detail,
            }
        ]
    finally:
        store.close()


def test_legacy_context_read_stream_is_one_activity_with_complete_evidence(tmp_path):
    store = ConversationStore(tmp_path / "context.db")
    base = event(
        workflow_id="workflow",
        scope_workflow_id="workflow",
        operation_id="op",
        kind="tool_delta",
    )
    command = (
        'pwsh -Command "Get-Content C:/skills/tdd/SKILL.md; Get-Content README.md"'
    )
    source = "# TDD\nInternal skill source\n# README\nProject instructions"
    try:
        for chunk in (
            "# TDD\n",
            "Internal skill source\n",
            "# README\n",
            "Project instructions",
        ):
            store.append(
                replace(
                    base,
                    text=chunk,
                    detail={"method": "item/commandExecution/outputDelta"},
                )
            )
        store.append(
            replace(
                base, text=command + "\n" + source, detail={"method": "item/completed"}
            )
        )
        turn = store.snapshot("workflow")["conversations"][0]["turns"][0]
        assert len(turn["activities"]) == 1
        activity = turn["activities"][0]
        assert activity["category"] == "context"
        assert activity["summary"] == "Read context: SKILL.md, README.md"
        assert activity["text"] == command + "\n" + source
        assert turn["working"][0]["text"].endswith(command + "\n" + source)
    finally:
        store.close()


@pytest.mark.parametrize(
    ("original", "display", "markers"),
    [
        ("SDK_PROBE_OK", "", ["SDK_PROBE_OK"]),
        (
            "**Result**\n\nAll tests passed.\n\nSDK_PROBE_OK",
            "**Result**\n\nAll tests passed.",
            ["SDK_PROBE_OK"],
        ),
        (
            "The SDK_PROBE_OK marker confirms the probe.",
            "The SDK_PROBE_OK marker confirms the probe.",
            [],
        ),
        ("```text\nSDK_PROBE_OK\n```", "```text\nSDK_PROBE_OK\n```", []),
    ],
)
def test_snapshot_separates_standalone_probe_metadata_from_complete_answer(
    tmp_path, original, display, markers
):
    store = ConversationStore(tmp_path / "probe.db")
    try:
        store.append(
            event(
                workflow_id="workflow",
                scope_workflow_id="workflow",
                operation_id="op",
                kind="assistant_final",
                text=original,
            )
        )
        turn = store.snapshot("workflow")["conversations"][0]["turns"][0]
        assert turn["output"] == original
        assert turn["displayOutput"] == display
        assert turn["verificationMarkers"] == markers
    finally:
        store.close()


def test_structured_context_reads_are_concise_and_file_edits_remain_edits(tmp_path):
    store = ConversationStore(tmp_path / "structured-context.db")
    base = event(
        workflow_id="workflow",
        scope_workflow_id="workflow",
        operation_id="op",
        kind="tool_delta",
    )
    try:
        store.append(
            replace(
                base,
                text="full config source\nsettings",
                detail={
                    "method": "item/completed",
                    "item_id": "read-1",
                    "item_type": "commandExecution",
                    "command": "cat policy.md config.yaml AGENTS.md",
                    "exit_code": 0,
                },
            )
        )
        store.append(
            replace(
                base,
                text="Files changed: README.md",
                detail={
                    "method": "item/completed",
                    "item_id": "edit-1",
                    "item_type": "fileChange",
                    "paths": ["README.md"],
                },
            )
        )
        activities = store.snapshot("workflow")["conversations"][0]["turns"][0][
            "activities"
        ]
        assert [
            (activity["category"], activity["summary"]) for activity in activities
        ] == [
            ("context", "Read context: policy.md, config.yaml, AGENTS.md"),
            ("file", "Files changed (1): README.md"),
        ]
        assert activities[0]["text"] == "full config source\nsettings"
    finally:
        store.close()


def test_context_summary_excludes_command_separator_strings(tmp_path):
    store = ConversationStore(tmp_path / "separator.db")
    command = 'pwsh -Command "Get-Content C:/skills/tdd/SKILL.md; Write-Output "`n---README---"; Get-Content README.md"'
    try:
        store.append(
            replace(
                event(
                    workflow_id="workflow",
                    scope_workflow_id="workflow",
                    operation_id="op",
                    kind="tool_delta",
                    text=command + "\nfull source",
                ),
                detail={"method": "item/completed"},
            )
        )
        activity = store.snapshot("workflow")["conversations"][0]["turns"][0][
            "activities"
        ][0]
        assert activity["summary"] == "Read context: SKILL.md, README.md"
        assert activity["text"] == command + "\nfull source"
    finally:
        store.close()
