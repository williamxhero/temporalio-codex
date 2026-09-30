import asyncio
from dataclasses import replace

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.activities import (
    codex_stage,
    configure_codex_adapter,
    foundation_stage,
)
from temporalio_codex.codex_adapter import FakeCodexAdapter
from temporalio_codex.codex_models import (
    CodexObservation,
    CodexOutcome,
    CodexRole,
)
from temporalio_codex.models import (
    RunInput,
    RunStatus,
    StageDefinition,
    StageOutcome,
)
from temporalio_codex.qualification import run_live_qualification
from temporalio_codex.workflows import CodexRunWorkflow


def codex_stage_definition(role: CodexRole) -> StageDefinition:
    return StageDefinition(
        key=role.value,
        role=role,
        repository="D:/repo",
        allowed_scope=("src/",),
    )


async def execute_with_fake(run_id: str, stages, observations=None):
    configure_codex_adapter(FakeCodexAdapter(observations or {}))
    try:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue=run_id,
                workflows=[CodexRunWorkflow],
                activities=[foundation_stage, codex_stage],
            ),
        ):
            handle = await environment.client.start_workflow(
                CodexRunWorkflow.run,
                RunInput(
                    requirement="deliver a governed change",
                    stages=stages,
                    automatic=True,
                ),
                id=run_id,
                task_queue=run_id,
            )
            result = await handle.result()
            snapshot = await handle.query(CodexRunWorkflow.get_status)
        return result, snapshot
    finally:
        configure_codex_adapter(None)


async def test_workflow_runs_distinct_planning_implementation_and_review_roles() -> (
    None
):
    result, snapshot = await execute_with_fake(
        "role-separation-run",
        tuple(codex_stage_definition(role) for role in CodexRole),
    )

    assert result.status is RunStatus.COMPLETED
    assert snapshot.completed_stages == tuple(role.value for role in CodexRole)
    assert [stage.role for stage in snapshot.stage_results] == [
        role.value for role in CodexRole
    ]
    assert len({stage.operation_id for stage in snapshot.stage_results}) == 3
    assert len({stage.thread_id for stage in snapshot.stage_results}) == 3


def test_codex_prompt_requires_role_specific_project_skills() -> None:
    prompt = CodexRunWorkflow._codex_prompt(
        "deliver a governed change",
        codex_stage_definition(CodexRole.IMPLEMENTATION),
        None,
    )

    assert ".claude/skills/implement/SKILL.md" in prompt
    assert ".claude/skills/tdd/SKILL.md" in prompt
    assert "default values" in prompt
    assert "global or external skill copies" in prompt


async def test_failed_review_cannot_be_satisfied_by_implementation() -> None:
    run_id = "review-gate-run"
    review_operation_id = f"{run_id}:review:review"
    result, snapshot = await execute_with_fake(
        run_id,
        (
            codex_stage_definition(CodexRole.IMPLEMENTATION),
            codex_stage_definition(CodexRole.REVIEW),
        ),
        {
            review_operation_id: CodexObservation(
                operation_id=review_operation_id,
                role=CodexRole.REVIEW,
                outcome=CodexOutcome.FAILED,
                summary="review failed",
            )
        },
    )

    assert result.status is RunStatus.FAILED
    assert result.outcome is StageOutcome.FAILED
    assert snapshot.completed_stages == (CodexRole.IMPLEMENTATION.value,)
    assert snapshot.stage_results[-1].role == CodexRole.REVIEW.value


async def test_pending_codex_input_resumes_after_answer_update() -> None:
    run_id = "pending-codex-run"
    initial_operation_id = f"{run_id}:planning:planning"
    configure_codex_adapter(
        FakeCodexAdapter(
            {
                initial_operation_id: CodexObservation(
                    operation_id=initial_operation_id,
                    role=CodexRole.PLANNING,
                    outcome=CodexOutcome.PENDING_INPUT,
                    thread_id="thread-pending",
                    turn_id="turn-pending",
                    pending_question="Which scope should planning use?",
                )
            }
        )
    )
    try:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue=run_id,
                workflows=[CodexRunWorkflow],
                activities=[foundation_stage, codex_stage],
            ),
        ):
            handle = await environment.client.start_workflow(
                CodexRunWorkflow.run,
                RunInput(
                    requirement="resume a pending Codex stage",
                    automatic=False,
                    stages=(codex_stage_definition(CodexRole.PLANNING),),
                ),
                id=run_id,
                task_queue=run_id,
            )
            for _ in range(100):
                snapshot = await handle.query(CodexRunWorkflow.get_status)
                if snapshot.status is RunStatus.WAITING_FOR_INPUT:
                    break
                await asyncio.sleep(0.01)
            assert snapshot.pending_input == "Which scope should planning use?"
            assert (
                await handle.execute_update(
                    CodexRunWorkflow.submit_answer,
                    "src/",
                    result_type=bool,
                )
                is True
            )
            result = await handle.result()
        assert result.status is RunStatus.COMPLETED
        assert result.outcome is StageOutcome.COMPLETED
    finally:
        configure_codex_adapter(None)


async def test_unknown_codex_observation_waits_for_readback() -> None:
    run_id = "unknown-codex-run"
    operation_id = f"{run_id}:planning:planning"
    configure_codex_adapter(
        FakeCodexAdapter(
            {
                operation_id: CodexObservation(
                    operation_id=operation_id,
                    role=CodexRole.PLANNING,
                    outcome=CodexOutcome.UNKNOWN,
                    thread_id="thread-unknown",
                    turn_id="turn-unknown",
                    summary="readback required",
                    readback_required=True,
                )
            }
        )
    )
    try:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue=run_id,
                workflows=[CodexRunWorkflow],
                activities=[foundation_stage, codex_stage],
            ),
        ):
            handle = await environment.client.start_workflow(
                CodexRunWorkflow.run,
                RunInput(
                    requirement="read back an uncertain Codex turn",
                    automatic=False,
                    stages=(codex_stage_definition(CodexRole.PLANNING),),
                ),
                id=run_id,
                task_queue=run_id,
            )
            for _ in range(100):
                snapshot = await handle.query(CodexRunWorkflow.get_status)
                if snapshot.status is RunStatus.WAITING_FOR_EXTERNAL_OBSERVATION:
                    break
                await asyncio.sleep(0.01)
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
    finally:
        configure_codex_adapter(None)


async def test_live_qualification_accepts_fake_evidence_without_claiming_provider_access() -> (
    None
):
    result = await run_live_qualification(
        adapter=FakeCodexAdapter(
            {
                "live-qualification-1": CodexObservation(
                    operation_id="live-qualification-1",
                    role=CodexRole.PLANNING,
                    outcome=CodexOutcome.COMPLETED,
                    thread_id="thread-live-test",
                    turn_id="turn-live-test",
                    summary="test evidence",
                )
            }
        )
    )

    assert result.status == "verified"
    assert result.thread_id == "thread-live-test"
    assert result.turn_id == "turn-live-test"


@pytest.mark.parametrize("outcome", [CodexOutcome.UNKNOWN, CodexOutcome.PENDING_INPUT])
async def test_automatic_codex_unresolved_outcome_finishes_without_updates(
    outcome,
) -> None:
    run_id = f"automatic-{outcome.value}"
    operation_id = f"{run_id}:planning:planning"
    observation = CodexObservation(
        operation_id=operation_id,
        role=CodexRole.PLANNING,
        outcome=outcome,
        thread_id="thread-pending",
        pending_question="Missing authorization?",
        summary="outcome unresolved",
    )
    result, snapshot = await execute_with_fake(
        run_id,
        (codex_stage_definition(CodexRole.PLANNING),),
        {
            operation_id: observation,
            f"{operation_id}:answer-1": replace(
                observation, operation_id=f"{operation_id}:answer-1"
            ),
        },
    )
    assert result.status is RunStatus.FAILED
    assert snapshot.completed_stages == ()
    if outcome is CodexOutcome.PENDING_INPUT:
        assert snapshot.stage_results[-1].failure == "automatic_input_unresolved"
    else:
        assert result.outcome is StageOutcome.UNKNOWN


async def test_automatic_codex_input_resumes_with_context_without_updates() -> None:
    run_id = "automatic-context"
    operation_id = f"{run_id}:planning:planning"
    result, snapshot = await execute_with_fake(
        run_id,
        (replace(codex_stage_definition(CodexRole.PLANNING), requires_input=True),),
        {
            operation_id: CodexObservation(
                operation_id=operation_id,
                role=CodexRole.PLANNING,
                outcome=CodexOutcome.PENDING_INPUT,
                thread_id="thread-context",
                pending_question="Which scope?",
            )
        },
    )
    assert result.status is RunStatus.COMPLETED
    assert snapshot.stage_results[-1].operation_id == f"{operation_id}:answer-1"
