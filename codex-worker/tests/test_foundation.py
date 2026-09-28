import pytest
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.activities import foundation_stage
from temporalio_codex.client import execute_run
from temporalio_codex.models import (
    RunInput,
    RunStatus,
    StageDefinition,
    StageOutcome,
)
from temporalio_codex.workflows import CodexRunWorkflow


async def test_foundation_workflow_uses_public_input_and_result_seams() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="foundation-test",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            result = await environment.client.execute_workflow(
                CodexRunWorkflow.run,
                RunInput(requirement="build a durable Codex flow"),
                id="foundation-test-run",
                task_queue="foundation-test",
            )

    assert result.status is RunStatus.COMPLETED
    assert result.outcome is StageOutcome.COMPLETED
    assert result.workflow_id == "foundation-test-run"
    assert result.stage == "foundation"
    assert result.summary == "Accepted requirement: build a durable Codex flow"


async def test_client_rejects_duplicate_workflow_id() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="identity-test",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            await execute_run(
                environment.client,
                "a stable run",
                workflow_id="stable-run",
                task_queue="identity-test",
            )
            with pytest.raises(WorkflowAlreadyStartedError):
                await execute_run(
                    environment.client,
                    "a stable run",
                    workflow_id="stable-run",
                    task_queue="identity-test",
                )


async def test_status_query_returns_public_run_snapshot() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="query-test",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            handle = await environment.client.start_workflow(
                CodexRunWorkflow.run,
                RunInput(requirement="query the run status"),
                id="query-test-run",
                task_queue="query-test",
            )
            result = await handle.result()
            snapshot = await handle.query(CodexRunWorkflow.get_status)

    assert result.status is RunStatus.COMPLETED
    assert snapshot.workflow_id == "query-test-run"
    assert snapshot.status is RunStatus.COMPLETED
    assert snapshot.completed_stages == ("foundation",)


async def test_run_waits_for_answer_and_resumes_same_workflow() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="answer-test",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            handle = await environment.client.start_workflow(
                CodexRunWorkflow.run,
                RunInput(
                    requirement="answer before implementation",
                    stages=(StageDefinition(key="question", requires_input=True),),
                ),
                id="answer-test-run",
                task_queue="answer-test",
            )
            await environment.client.get_workflow_handle(
                "answer-test-run"
            ).query(CodexRunWorkflow.get_status)
            snapshot = await handle.query(CodexRunWorkflow.get_status)
            assert snapshot.status is RunStatus.WAITING_FOR_INPUT
            assert snapshot.pending_input == "Input required for stage: question"

            assert (
                await handle.execute_update(
                    CodexRunWorkflow.submit_answer,
                    "approved",
                    result_type=bool,
                )
                is True
            )
            assert (
                await handle.execute_update(
                    CodexRunWorkflow.submit_answer,
                    "duplicate",
                    result_type=bool,
                )
                is False
            )
            result = await handle.result()

    assert result.workflow_id == "answer-test-run"
    assert result.status is RunStatus.COMPLETED
    assert result.summary == "Accepted answer for question: approved"


async def test_cancel_stops_later_stages() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="cancel-test",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            handle = await environment.client.start_workflow(
                CodexRunWorkflow.run,
                RunInput(
                    requirement="cancel before the second stage",
                    stages=(
                        StageDefinition(key="first"),
                        StageDefinition(key="second"),
                    ),
                ),
                id="cancel-test-run",
                task_queue="cancel-test",
            )
            await handle.signal(CodexRunWorkflow.cancel)
            result = await handle.result()

    assert result.status is RunStatus.CANCELLED
    assert result.outcome is StageOutcome.CANCELLED
    assert result.stage == "first"
