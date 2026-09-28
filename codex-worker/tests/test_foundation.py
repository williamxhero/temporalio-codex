import asyncio

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from temporalio_codex.activities import foundation_stage
from temporalio_codex.client import execute_run
from temporalio_codex.models import (
    RunInput,
    RunStatus,
    StageDefinition,
    StageOutcome,
)
from temporalio_codex.workflows import CodexRunWorkflow


@activity.defn(name="foundation-stage")
async def transient_stage(input):
    raise ApplicationError("retry me", type="TransientFailure")


@activity.defn(name="foundation-stage")
async def unknown_stage(input):
    raise ApplicationError("outcome unknown", type="UnknownExternalOutcome")


@activity.defn(name="foundation-stage")
async def timeout_stage(input):
    await asyncio.sleep(1)


async def wait_for_status(handle, expected: RunStatus):
    for _ in range(100):
        snapshot = await handle.query(CodexRunWorkflow.get_status)
        if snapshot.status is expected:
            return snapshot
        await asyncio.sleep(0.05)
    raise AssertionError(f"run did not reach {expected}")


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
            snapshot = await wait_for_status(handle, RunStatus.WAITING_FOR_INPUT)
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
                        StageDefinition(key="first", requires_input=True),
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


async def test_retry_policy_classifies_failed_activity() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="retry-test",
            workflows=[CodexRunWorkflow],
            activities=[transient_stage],
        ):
            result = await environment.client.execute_workflow(
                CodexRunWorkflow.run,
                RunInput(requirement="retry a transient activity"),
                id="retry-test-run",
                task_queue="retry-test",
            )

    assert result.status is RunStatus.FAILED
    assert result.outcome is StageOutcome.FAILED
    assert result.stage == "foundation"


async def test_unknown_activity_outcome_waits_for_external_observation() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="unknown-test",
            workflows=[CodexRunWorkflow],
            activities=[unknown_stage],
        ):
            handle = await environment.client.start_workflow(
                CodexRunWorkflow.run,
                RunInput(requirement="reconcile unknown activity outcome"),
                id="unknown-test-run",
                task_queue="unknown-test",
            )
            snapshot = await wait_for_status(
                handle, RunStatus.WAITING_FOR_EXTERNAL_OBSERVATION
            )

            assert snapshot.status is RunStatus.WAITING_FOR_EXTERNAL_OBSERVATION
            assert (
                await handle.execute_update(
                    CodexRunWorkflow.resolve_external_observation,
                    True,
                    result_type=bool,
                )
                is True
            )
            result = await handle.result()

    assert result.status is RunStatus.COMPLETED
    assert result.outcome is StageOutcome.COMPLETED


async def test_activity_timeout_becomes_unknown_observation() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="timeout-test",
            workflows=[CodexRunWorkflow],
            activities=[timeout_stage],
        ):
            handle = await environment.client.start_workflow(
                CodexRunWorkflow.run,
                RunInput(
                    requirement="time out an activity",
                    stages=(
                        StageDefinition(
                            key="timeout",
                            start_to_close_timeout_seconds=0.01,
                        ),
                    ),
                ),
                id="timeout-test-run",
                task_queue="timeout-test",
            )
            snapshot = await wait_for_status(
                handle, RunStatus.WAITING_FOR_EXTERNAL_OBSERVATION
            )

            assert snapshot.status is RunStatus.WAITING_FOR_EXTERNAL_OBSERVATION
            assert (
                await handle.execute_update(
                    CodexRunWorkflow.resolve_external_observation,
                    False,
                    result_type=bool,
                )
                is True
            )
            result = await handle.result()

    assert result.status is RunStatus.FAILED
    assert result.outcome is StageOutcome.UNKNOWN


async def test_workflow_history_replays() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="replay-test",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            handle = await environment.client.start_workflow(
                CodexRunWorkflow.run,
                RunInput(requirement="replay a completed run"),
                id="replay-test-run",
                task_queue="replay-test",
            )
            await handle.result()
            history = await handle.fetch_history()

    await Replayer(workflows=[CodexRunWorkflow]).replay_workflow(history)


async def test_worker_restart_preserves_waiting_run() -> None:
    async with await WorkflowEnvironment.start_local() as environment:
        handle = await environment.client.start_workflow(
            CodexRunWorkflow.run,
            RunInput(
                requirement="restart while waiting",
                stages=(StageDefinition(key="question", requires_input=True),),
            ),
            id="restart-test-run",
            task_queue="restart-test",
        )

        async with Worker(
            environment.client,
            task_queue="restart-test",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            snapshot = await wait_for_status(handle, RunStatus.WAITING_FOR_INPUT)
            assert snapshot.current_stage == "question"

        async with Worker(
            environment.client,
            task_queue="restart-test",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            after_restart = await handle.query(CodexRunWorkflow.get_status)
            assert after_restart.workflow_id == "restart-test-run"
            assert after_restart.status is RunStatus.WAITING_FOR_INPUT
            assert after_restart.pending_input == "Input required for stage: question"
            assert (
                await handle.execute_update(
                    CodexRunWorkflow.submit_answer,
                    "continue",
                    result_type=bool,
                )
                is True
            )
            result = await handle.result()

    assert result.status is RunStatus.COMPLETED
    assert result.workflow_id == "restart-test-run"
