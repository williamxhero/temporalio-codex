from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from temporalio_codex.codex_models import (
    CodexFailure,
    CodexOperation,
    CodexOutcome,
    CodexRole,
)
from temporalio_codex.conversation_store import ConversationStore
from temporalio_codex.openai_adapter import OpenAICodexAdapter


def operation() -> CodexOperation:
    return CodexOperation(
        operation_id="op-1",
        run_id="run-1",
        stage="planning",
        role=CodexRole.PLANNING,
        repository="D:/repo",
        allowed_scope=("src/",),
        approval_policy="deny_all",
        model="gpt-5-codex",
        effort="medium",
        prompt="make a plan",
    )


def adapter_for(result):
    turn = SimpleNamespace(id="turn-1", run=AsyncMock(return_value=result))
    thread = MagicMock(id="thread-1")
    thread.turn.return_value = turn
    codex = SimpleNamespace(
        thread_start=AsyncMock(return_value=thread),
        thread_resume=AsyncMock(return_value=thread),
        close=AsyncMock(),
    )
    return OpenAICodexAdapter(lambda: codex, "0.155.1"), codex, thread


class StreamingTurn:
    id = "turn-stream"

    def __init__(self, events):
        self.events = events

    async def stream(self):
        for event in self.events:
            yield event


async def test_production_adapter_maps_completed_turn_without_live_call() -> None:
    result = SimpleNamespace(
        id="turn-1",
        status=SimpleNamespace(value="completed"),
        final_response="plan complete",
        error=None,
    )
    adapter, codex, thread = adapter_for(result)

    observation = await adapter.execute(operation())

    assert observation.outcome is CodexOutcome.COMPLETED
    assert observation.thread_id == "thread-1"
    assert observation.turn_id == "turn-1"
    assert observation.summary == "plan complete"
    codex.thread_start.assert_awaited_once()
    thread.turn.assert_called_once()


async def test_stream_deltas_are_persisted_as_one_conversation_turn(tmp_path) -> None:
    completed = SimpleNamespace(
        id="turn-stream",
        status=SimpleNamespace(value="completed"),
        error=None,
    )
    events = [
        SimpleNamespace(
            method="item/plan/delta",
            payload=SimpleNamespace(
                thread_id="thread-stream", turn_id="turn-stream", delta="outline"
            ),
        ),
        SimpleNamespace(
            method="item/agentMessage/delta",
            payload=SimpleNamespace(
                thread_id="thread-stream", turn_id="turn-stream", delta="hello "
            ),
        ),
        SimpleNamespace(
            method="item/reasoning/summaryTextDelta",
            payload=SimpleNamespace(
                thread_id="thread-stream", turn_id="turn-stream", delta="thinking"
            ),
        ),
        SimpleNamespace(
            method="item/agentMessage/delta",
            payload=SimpleNamespace(
                thread_id="thread-stream", turn_id="turn-stream", delta="world"
            ),
        ),
        SimpleNamespace(
            method="item/completed",
            payload=SimpleNamespace(
                item=SimpleNamespace(type="agentMessage", text="hello world")
            ),
        ),
        SimpleNamespace(
            method="turn/completed",
            payload=SimpleNamespace(turn=completed),
        ),
    ]
    turn = StreamingTurn(events)
    thread = MagicMock(id="thread-stream")
    thread.turn.return_value = turn
    codex = SimpleNamespace(
        thread_start=AsyncMock(return_value=thread),
        close=AsyncMock(),
    )
    store = ConversationStore(tmp_path / "conversations.sqlite3")
    try:
        current_operation = operation()
        current_operation = current_operation.__class__(
            **{
                **current_operation.__dict__,
                "parent_workflow_id": "parent-workflow",
            }
        )
        adapter = OpenAICodexAdapter(lambda: codex, "0.155.1", store)

        observation = await adapter.execute(current_operation)
        snapshot = store.snapshot("parent-workflow")

        assert observation.summary == "hello world"
        turn_snapshot = snapshot["conversations"][0]["turns"][0]
        assert turn_snapshot["input"] == "make a plan"
        assert turn_snapshot["output"] == "hello world"
        assert turn_snapshot["working"] == [
            {"kind": "plan", "text": "outline"},
            {"kind": "reasoning", "text": "thinking"},
        ]
        assert turn_snapshot["status"] == "completed"
    finally:
        store.close()


async def test_streamed_sdk_actions_are_exposed_as_distinct_activities(
    tmp_path,
) -> None:
    completed = SimpleNamespace(
        id="turn-stream",
        status=SimpleNamespace(value="completed"),
        error=None,
    )
    events = [
        SimpleNamespace(
            method="item/commandExecution/outputDelta",
            id="delta-1",
            payload=SimpleNamespace(
                thread_id="thread-stream",
                turn_id="turn-stream",
                item_id="cmd-1",
                delta="1 passed",
            ),
        ),
        SimpleNamespace(
            method="item/completed",
            id="complete-cmd-1",
            payload=SimpleNamespace(
                thread_id="thread-stream",
                turn_id="turn-stream",
                item=SimpleNamespace(
                    id="cmd-1",
                    type="commandExecution",
                    command="uv run pytest tests",
                    aggregated_output="1 passed",
                    status=SimpleNamespace(value="completed"),
                    exit_code=0,
                ),
            ),
        ),
        SimpleNamespace(
            method="item/completed",
            id="complete-file-1",
            payload=SimpleNamespace(
                thread_id="thread-stream",
                turn_id="turn-stream",
                item=SimpleNamespace(
                    id="file-1",
                    type="fileChange",
                    changes=[
                        SimpleNamespace(
                            path="src/main.py",
                            diff="+print('done')",
                            kind=SimpleNamespace(type="add"),
                        )
                    ],
                    status=SimpleNamespace(value="completed"),
                ),
            ),
        ),
        SimpleNamespace(
            method="item/completed",
            id="complete-mcp-1",
            payload=SimpleNamespace(
                thread_id="thread-stream",
                turn_id="turn-stream",
                item=SimpleNamespace(
                    id="mcp-1",
                    type="mcpToolCall",
                    server="github",
                    tool="get_issue",
                    arguments={"number": 91},
                    result={"body": "full technical result"},
                    error={"message": "tool result warning"},
                    status=SimpleNamespace(value="completed"),
                ),
            ),
        ),
        SimpleNamespace(
            method="turn/completed", payload=SimpleNamespace(turn=completed)
        ),
    ]
    thread = MagicMock(id="thread-stream")
    thread.turn.return_value = StreamingTurn(events)
    codex = SimpleNamespace(
        thread_start=AsyncMock(return_value=thread), close=AsyncMock()
    )
    store = ConversationStore(tmp_path / "sdk-actions.db")
    try:
        adapter = OpenAICodexAdapter(lambda: codex, "0.155.1", store)
        await adapter.execute(operation())

        activities = store.snapshot("run-1")["conversations"][0]["turns"][0][
            "activities"
        ]
        assert [
            (activity["category"], activity["summary"]) for activity in activities
        ] == [
            ("test", "uv run pytest tests | 1 passed | Passed (exit 0)"),
            ("file", "Files changed (1): src/main.py"),
            ("tool", "github.get_issue"),
        ]
        assert activities[0]["text"] == "uv run pytest tests\n1 passed"
        assert activities[0]["detail"]["exit_code"] == 0
        assert activities[1]["detail"]["paths"] == ["src/main.py"]
        assert activities[1]["detail"]["changes"] == [
            {"path": "src/main.py", "diff": "+print('done')", "kind": "add"}
        ]
        assert activities[2]["detail"]["arguments"] == {"number": 91}
        assert activities[2]["detail"]["result"] == {"body": "full technical result"}
        assert activities[2]["detail"]["error"] == {"message": "tool result warning"}
    finally:
        store.close()


async def test_same_operation_id_in_different_workflow_runs_is_not_cached_or_deduplicated(
    tmp_path,
) -> None:
    result = SimpleNamespace(
        id="turn-1",
        status=SimpleNamespace(value="completed"),
        final_response="response",
        error=None,
    )
    thread = MagicMock(id="thread-1")
    thread.turn.side_effect = [
        SimpleNamespace(id="turn-1", run=AsyncMock(return_value=result)),
        SimpleNamespace(id="turn-2", run=AsyncMock(return_value=result)),
    ]
    codex = SimpleNamespace(
        thread_start=AsyncMock(return_value=thread),
        close=AsyncMock(),
    )
    store = ConversationStore(tmp_path / "conversations.sqlite3")
    try:
        adapter = OpenAICodexAdapter(lambda: codex, "0.155.1", store)
        first = operation().__class__(
            **{**operation().__dict__, "workflow_run_id": "workflow-run-1"}
        )
        second = operation().__class__(
            **{**operation().__dict__, "workflow_run_id": "workflow-run-2"}
        )

        await adapter.execute(first)
        await adapter.execute(second)

        assert thread.turn.call_count == 2
        assert [
            turn["input"]
            for conversation in store.snapshot("run-1", "workflow-run-1")[
                "conversations"
            ]
            for turn in conversation["turns"]
        ] == ["make a plan"]
        assert [
            turn["input"]
            for conversation in store.snapshot("run-1", "workflow-run-2")[
                "conversations"
            ]
            for turn in conversation["turns"]
        ] == ["make a plan"]
    finally:
        store.close()


async def test_existing_thread_history_is_persisted_for_workflow_chat(tmp_path) -> None:
    result = SimpleNamespace(
        id="turn-current",
        status=SimpleNamespace(value="completed"),
        final_response="current response",
        error=None,
    )
    historical_user = SimpleNamespace(
        root=SimpleNamespace(
            id="item-user",
            type="userMessage",
            content=[
                SimpleNamespace(root=SimpleNamespace(type="text", text="old prompt"))
            ],
        )
    )
    historical_agent = SimpleNamespace(
        root=SimpleNamespace(
            id="item-agent",
            type="agentMessage",
            text="old response",
        )
    )
    thread = MagicMock(id="thread-existing")
    thread.read = AsyncMock(
        return_value=SimpleNamespace(
            thread=SimpleNamespace(
                turns=[
                    SimpleNamespace(
                        id="turn-old",
                        items=[historical_user, historical_agent],
                    )
                ]
            )
        )
    )
    thread.turn.return_value = SimpleNamespace(
        id="turn-current",
        run=AsyncMock(return_value=result),
    )
    codex = SimpleNamespace(
        thread_resume=AsyncMock(return_value=thread),
        close=AsyncMock(),
    )
    store = ConversationStore(tmp_path / "conversations.sqlite3")
    try:
        current_operation = operation().__class__(
            **{
                **operation().__dict__,
                "thread_id": "thread-existing",
                "run_id": "workflow",
            }
        )
        adapter = OpenAICodexAdapter(lambda: codex, "0.155.1", store)

        await adapter.execute(current_operation)
        snapshot = store.snapshot("workflow")
        turns = snapshot["conversations"][0]["turns"]

        assert [turn["input"] for turn in turns] == ["old prompt", "make a plan"]
        assert [turn["output"] for turn in turns] == [
            "old response",
            "current response",
        ]
        thread.read.assert_awaited_once_with(include_turns=True)
    finally:
        store.close()


async def test_unknown_sdk_error_requires_readback_and_is_cached() -> None:
    turn = SimpleNamespace(
        id="turn-1", run=AsyncMock(side_effect=RuntimeError("disconnect"))
    )
    thread = MagicMock(id="thread-1")
    thread.turn.return_value = turn
    codex = SimpleNamespace(
        thread_start=AsyncMock(return_value=thread),
        close=AsyncMock(),
    )
    adapter = OpenAICodexAdapter(lambda: codex, "0.155.1")

    first = await adapter.execute(operation())
    second = await adapter.execute(operation())

    assert first.outcome is CodexOutcome.UNKNOWN
    assert first.failure is CodexFailure.UNKNOWN
    assert first.thread_id == "thread-1"
    assert first.turn_id == "turn-1"
    assert first.readback_required is True
    assert second == first
    assert codex.thread_start.await_count == 1


@pytest.mark.parametrize(
    ("error", "failure"),
    [
        (RuntimeError("secret-provider-detail"), CodexFailure.UNKNOWN),
        (TimeoutError(), CodexFailure.TIMEOUT),
        (ConnectionError("private provider detail"), CodexFailure.STREAM_DISCONNECTED),
    ],
)
async def test_sdk_failures_have_stable_bounded_classifications(error, failure) -> None:
    turn = SimpleNamespace(id="turn-1", run=AsyncMock(side_effect=error))
    thread = MagicMock(id="thread-1")
    thread.turn.return_value = turn
    codex = SimpleNamespace(
        thread_start=AsyncMock(return_value=thread), close=AsyncMock()
    )
    adapter = OpenAICodexAdapter(lambda: codex, "0.155.1")

    observation = await adapter.execute(operation())

    assert observation.failure is failure
    assert "secret-provider-detail" not in observation.summary
    assert "private provider detail" not in observation.summary
    assert len(observation.summary) <= 1000


async def test_rejected_call_is_failed_without_readback() -> None:
    rejected = type("InvalidRequestError", (Exception,), {})()
    codex = SimpleNamespace(
        thread_start=AsyncMock(side_effect=rejected), close=AsyncMock()
    )
    adapter = OpenAICodexAdapter(lambda: codex, "0.155.1")

    observation = await adapter.execute(operation())

    assert observation.outcome is CodexOutcome.FAILED
    assert observation.failure is CodexFailure.REJECTED
    assert observation.readback_required is False


async def test_pending_input_is_bounded_and_typed() -> None:
    result = SimpleNamespace(
        id="turn-1",
        status=SimpleNamespace(value="requires_input"),
        pending_question="  Which repository scope should be changed?  ",
    )
    adapter, _, _ = adapter_for(result)

    observation = await adapter.execute(operation())

    assert observation.outcome is CodexOutcome.PENDING_INPUT
    assert observation.pending_question == "Which repository scope should be changed?"
    assert observation.readback_required is False


async def test_unrecognized_turn_status_requires_readback() -> None:
    result = SimpleNamespace(
        id="turn-1",
        status=SimpleNamespace(value="provider_added_status"),
        final_response=None,
    )
    adapter, _, _ = adapter_for(result)

    observation = await adapter.execute(operation())

    assert observation.outcome is CodexOutcome.UNKNOWN
    assert observation.failure is CodexFailure.UNKNOWN
    assert observation.readback_required is True


async def test_historical_interrupt_is_explicitly_unsupported() -> None:
    adapter = OpenAICodexAdapter(lambda: None, "0.155.1")

    assert await adapter.interrupt_historical_turn(operation()) is False
    assert await adapter.interrupt_owned_turn("missing") is False


async def test_completed_operation_survives_adapter_and_store_restart(tmp_path):
    result = SimpleNamespace(
        id="turn-1",
        status=SimpleNamespace(value="completed"),
        final_response="durable",
        error=None,
    )
    adapter, codex, thread = adapter_for(result)
    path = tmp_path / "ledger.db"
    store = ConversationStore(path)
    adapter.conversation_store = store
    first = await adapter.execute(operation())
    store.close()
    store = ConversationStore(path)
    try:
        restarted = OpenAICodexAdapter(lambda: codex, "0.155.1", store)
        assert await restarted.execute(operation()) == first
        thread.turn.assert_called_once()
    finally:
        store.close()


async def test_cancelled_external_launch_never_launches_again_after_restart(tmp_path):
    import asyncio

    started = asyncio.Event()

    async def launch(**kwargs):
        started.set()
        await asyncio.Future()

    codex = SimpleNamespace(
        thread_start=AsyncMock(side_effect=launch), close=AsyncMock()
    )
    store = ConversationStore(tmp_path / "ledger.db")
    try:
        adapter = OpenAICodexAdapter(lambda: codex, "0.155.1", store)
        task = asyncio.create_task(adapter.execute(operation()))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        restarted = OpenAICodexAdapter(lambda: codex, "0.155.1", store)
        observation = await restarted.execute(operation())
        assert observation.outcome is CodexOutcome.UNKNOWN
        assert observation.readback_required
        codex.thread_start.assert_awaited_once()
    finally:
        store.close()


async def test_async_sdk_turn_captures_completed_items_and_excludes_private_reasoning(
    tmp_path,
):
    completed = SimpleNamespace(
        id="turn-async", status=SimpleNamespace(value="completed"), error=None
    )
    events = [
        SimpleNamespace(
            method="item/reasoning/textDelta", payload=SimpleNamespace(delta="PRIVATE")
        ),
        SimpleNamespace(
            method="turn/plan/updated",
            payload=SimpleNamespace(
                plan=[SimpleNamespace(step="Implement", status="inProgress")]
            ),
        ),
        SimpleNamespace(
            method="item/completed",
            payload=SimpleNamespace(
                item=SimpleNamespace(
                    type="reasoning", summary=["public summary"], content=["PRIVATE"]
                )
            ),
        ),
        SimpleNamespace(
            method="item/completed",
            payload=SimpleNamespace(
                item=SimpleNamespace(
                    type="commandExecution",
                    command="pytest",
                    aggregated_output="passed",
                )
            ),
        ),
        SimpleNamespace(
            method="item/agentMessage/delta", payload=SimpleNamespace(delta="partial")
        ),
        SimpleNamespace(
            method="item/completed",
            payload=SimpleNamespace(
                item=SimpleNamespace(type="agentMessage", text="full final answer")
            ),
        ),
        SimpleNamespace(
            method="turn/completed", payload=SimpleNamespace(turn=completed)
        ),
    ]
    thread = SimpleNamespace(
        id="thread-async", turn=AsyncMock(return_value=StreamingTurn(events))
    )
    codex = SimpleNamespace(
        thread_start=AsyncMock(return_value=thread), close=AsyncMock()
    )
    store = ConversationStore(tmp_path / "events.db")
    try:
        observation = await OpenAICodexAdapter(lambda: codex, "0.155.1", store).execute(
            operation()
        )
        assert observation.outcome is CodexOutcome.COMPLETED
        turn = store.snapshot("run-1")["conversations"][0]["turns"][0]
        assert turn["output"] == "full final answer"
        assert turn["working"] == [
            {"kind": "plan", "text": "Implement: inProgress"},
            {"kind": "reasoning", "text": "public summary"},
            {"kind": "tool", "text": "pytest\npassed"},
        ]
        assert "PRIVATE" not in str(turn)
        thread.turn.assert_awaited_once()
    finally:
        store.close()


async def test_durable_operation_rejects_changed_input_and_separates_namespaces(
    tmp_path,
):
    from dataclasses import replace

    result = SimpleNamespace(
        id="turn",
        status=SimpleNamespace(value="completed"),
        final_response="done",
        error=None,
    )
    adapter, codex, thread = adapter_for(result)
    store = ConversationStore(tmp_path / "events.db")
    adapter.conversation_store = store
    try:
        await adapter.execute(operation())
        restarted = OpenAICodexAdapter(lambda: codex, "0.155.1", store)
        with pytest.raises(ValueError, match="different input"):
            await restarted.execute(replace(operation(), prompt="changed"))
        await restarted.execute(replace(operation(), namespace="other"))
        assert thread.turn.call_count == 2
    finally:
        store.close()


async def test_resumed_history_never_imports_private_reasoning_content(tmp_path):
    from dataclasses import replace

    result = SimpleNamespace(
        id="turn",
        status=SimpleNamespace(value="completed"),
        final_response="done",
        error=None,
    )
    adapter, _codex, thread = adapter_for(result)
    thread.read = AsyncMock(
        return_value=SimpleNamespace(
            thread=SimpleNamespace(
                turns=[
                    SimpleNamespace(
                        id="history",
                        status="failed",
                        items=[
                            SimpleNamespace(
                                type="reasoning", summary=[], content=["PRIVATE"]
                            )
                        ],
                    )
                ]
            )
        )
    )
    store = ConversationStore(tmp_path / "events.db")
    adapter.conversation_store = store
    try:
        await adapter.execute(replace(operation(), thread_id="existing"))
        snapshot = store.snapshot("run-1")
        assert "PRIVATE" not in str(snapshot)
        assert snapshot["conversations"][0]["turns"][0]["status"] == "failed"
    finally:
        store.close()


async def test_sdk_error_exposes_public_cause_without_private_payload(tmp_path):
    from openai_codex.generated.v2_all import ErrorNotification

    completed = SimpleNamespace(
        id="turn-error",
        status="failed",
        error=SimpleNamespace(message="Connection lost"),
    )
    error_payload = ErrorNotification.model_validate(
        {
            "threadId": "thread-error",
            "turnId": "turn-error",
            "willRetry": False,
            "error": {
                "message": "Connection lost\nTraceback: raw stack",
                "additionalDetails": "Request timed out",
                "codexErrorInfo": "serverOverloaded",
                "content": "PRIVATE",
            },
        }
    )
    events = [
        SimpleNamespace(method="error", payload=error_payload),
        SimpleNamespace(
            method="turn/completed", payload=SimpleNamespace(turn=completed)
        ),
    ]
    thread = SimpleNamespace(
        id="thread-error", turn=AsyncMock(return_value=StreamingTurn(events))
    )
    codex = SimpleNamespace(
        thread_start=AsyncMock(return_value=thread), close=AsyncMock()
    )
    store = ConversationStore(tmp_path / "errors.db")
    try:
        await OpenAICodexAdapter(lambda: codex, "0.155.1", store).execute(operation())
        turn = store.snapshot("run-1")["conversations"][0]["turns"][0]
        sdk_error = turn["activities"][0]
        assert sdk_error["summary"] == "Connection lost"
        assert sdk_error["detail"] == {
            "method": "error",
            "message": "Connection lost\nTraceback: raw stack",
            "additional_details": "Request timed out",
            "error_type": "serverOverloaded",
            "will_retry": False,
        }
        assert "PRIVATE" not in str(turn)
        assert turn["displayOutput"] == "Connection lost"
    finally:
        store.close()
