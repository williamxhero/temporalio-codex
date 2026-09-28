import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from temporalio_codex.codex_models import (
    CodexFailure,
    CodexOperation,
    CodexRole,
    CodexOutcome,
)
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
        (asyncio.TimeoutError(), CodexFailure.TIMEOUT),
        (ConnectionError("private provider detail"), CodexFailure.STREAM_DISCONNECTED),
    ],
)
async def test_sdk_failures_have_stable_bounded_classifications(error, failure) -> None:
    turn = SimpleNamespace(id="turn-1", run=AsyncMock(side_effect=error))
    thread = MagicMock(id="thread-1")
    thread.turn.return_value = turn
    codex = SimpleNamespace(thread_start=AsyncMock(return_value=thread), close=AsyncMock())
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
