import asyncio
import hashlib

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.planning_activities import prepare_grill
from temporalio_codex.planning_models import (
    GrillAnswer,
    PlanningInput,
    PlanningPhase,
    PlanningStatus,
    SourceOrigin,
)
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow


async def wait_for_phase(handle, phase: PlanningPhase):
    for _ in range(100):
        snapshot = await handle.query(RequirementPlanningWorkflow.get_status)
        if snapshot.phase is phase:
            return snapshot
        await asyncio.sleep(0.01)
    raise AssertionError(f"workflow did not reach {phase}")


async def start_planning(environment, run_id: str, input: PlanningInput):
    return await environment.client.start_workflow(
        RequirementPlanningWorkflow.run,
        input,
        id=run_id,
        task_queue=run_id,
    )


async def test_text_intake_grill_confirmation_and_publication_gate() -> None:
    input = PlanningInput(origin=SourceOrigin.TEXT, source_text="ship a governed change")
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="planning-text",
            workflows=[RequirementPlanningWorkflow],
            activities=[prepare_grill],
        ):
            handle = await start_planning(environment, "planning-text", input)
            grilling = await wait_for_phase(handle, PlanningPhase.GRILLING)
            assert grilling.status is PlanningStatus.WAITING_FOR_INPUT
            assert grilling.source is not None
            assert grilling.source.source_reference is None
            assert grilling.source.source_identity == hashlib.sha256(
                b"text:ship a governed change"
            ).hexdigest()
            assert (
                await handle.execute_update(
                    RequirementPlanningWorkflow.request_spec_publication,
                    "publish-before-confirmation",
                    result_type=bool,
                )
                is False
            )
            assert await handle.execute_update(
                RequirementPlanningWorkflow.answer_grill,
                GrillAnswer(1, "deliver a tested change"),
                result_type=bool,
            )
            await wait_for_phase(handle, PlanningPhase.CONFIRMATION_REQUIRED)
            assert await handle.execute_update(
                RequirementPlanningWorkflow.confirm_planning,
                "confirm-1",
                result_type=bool,
            )
            assert (
                await handle.execute_update(
                    RequirementPlanningWorkflow.confirm_planning,
                    "confirm-1",
                    result_type=bool,
                )
                is True
            )
            assert (
                await handle.execute_update(
                    RequirementPlanningWorkflow.confirm_planning,
                    "confirm-2",
                    result_type=bool,
                )
                is False
            )
            await wait_for_phase(handle, PlanningPhase.READY)
            assert await handle.execute_update(
                RequirementPlanningWorkflow.request_spec_publication,
                "publish-1",
                result_type=bool,
            )
            result = await handle.result()

    assert result.status is PlanningStatus.COMPLETED
    assert result.phase is PlanningPhase.COMPLETED
    assert result.grill.decisions == ("deliver a tested change",)


async def test_historical_chat_uses_reference_and_assumption_is_recorded() -> None:
    input = PlanningInput(
        origin=SourceOrigin.HISTORICAL_CHAT,
        source_reference="artifact://chat-123",
    )
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="planning-chat",
            workflows=[RequirementPlanningWorkflow],
            activities=[prepare_grill],
        ):
            handle = await start_planning(environment, "planning-chat", input)
            snapshot = await wait_for_phase(handle, PlanningPhase.GRILLING)
            assert snapshot.source is not None
            assert snapshot.source.source_reference == "artifact://chat-123"
            assert await handle.execute_update(
                RequirementPlanningWorkflow.answer_grill,
                GrillAnswer(1, "treat the approved decision as the requirement", True),
                result_type=bool,
            )
            await wait_for_phase(handle, PlanningPhase.CONFIRMATION_REQUIRED)
            assert await handle.execute_update(
                RequirementPlanningWorkflow.confirm_planning,
                "confirm-chat",
                result_type=bool,
            )
            await wait_for_phase(handle, PlanningPhase.READY)
            assert await handle.execute_update(
                RequirementPlanningWorkflow.request_spec_publication,
                "publish-chat",
                result_type=bool,
            )
            result = await handle.result()

    assert result.grill.assumptions == (
        "treat the approved decision as the requirement",
    )


def test_sensitive_text_requires_reference() -> None:
    with pytest.raises(ValueError, match="sensitive"):
        PlanningInput(
            origin=SourceOrigin.TEXT,
            source_text="provider secret",
            sensitive=True,
        )


def test_historical_chat_requires_reference() -> None:
    with pytest.raises(ValueError, match="historical chat"):
        PlanningInput(origin=SourceOrigin.HISTORICAL_CHAT, source_text="chat")
