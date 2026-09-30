import asyncio
import hashlib

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio import activity
from temporalio.exceptions import ApplicationError

from temporalio_codex.planning_activities import prepare_grill, publish_spec_issues
from temporalio_codex.planning_models import (
    GrillAnswer,
    PlanningInput,
    PlanningPhase,
    PlanningStatus,
    SourceOrigin,
)
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.planning_activities import configure_spec_issue_gateway
from temporalio_codex.spec_issue_adapter import FakeSpecIssueGateway, SpecDraft


class FailOnceSpecIssueGateway(FakeSpecIssueGateway):
    def __init__(self) -> None:
        super().__init__()
        self.failed = True

    async def create_issue(self, input, draft, body):
        if self.failed:
            self.failed = False
            raise ConnectionError("simulated lost create response")
        return await super().create_issue(input, draft, body)


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


def configure_fake_issue_gateway() -> None:
    configure_spec_issue_gateway(FakeSpecIssueGateway())


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


async def test_confirmed_specs_publish_after_confirmation() -> None:
    configure_fake_issue_gateway()
    input = PlanningInput(
        origin=SourceOrigin.TEXT,
        source_text="publish two specs",
        specs=(
            SpecDraft(
                key="foundation",
                title="Foundation",
                scope="Foundation scope",
                acceptance_criteria=("Foundation works",),
                testing_decisions=("Test foundation",),
            ),
        ),
    )
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="planning-publish",
            workflows=[RequirementPlanningWorkflow],
            activities=[prepare_grill, publish_spec_issues],
        ):
            handle = await start_planning(environment, "planning-publish", input)
            await wait_for_phase(handle, PlanningPhase.GRILLING)
            assert await handle.execute_update(
                RequirementPlanningWorkflow.answer_grill,
                GrillAnswer(1, "approved"),
                result_type=bool,
            )
            await wait_for_phase(handle, PlanningPhase.CONFIRMATION_REQUIRED)
            assert await handle.execute_update(
                RequirementPlanningWorkflow.confirm_planning,
                "confirm-publish",
                result_type=bool,
            )
            await wait_for_phase(handle, PlanningPhase.READY)
            assert await handle.execute_update(
                RequirementPlanningWorkflow.request_spec_publication,
                "publish-specs",
                result_type=bool,
            )
            result = await handle.result()

    assert result.status is PlanningStatus.COMPLETED
    assert len(result.published_specs) == 1


async def test_unknown_spec_publication_times_out_as_blocked() -> None:
    configure_spec_issue_gateway(FakeSpecIssueGateway(fail_create=True))
    input = PlanningInput(
        origin=SourceOrigin.TEXT,
        source_text="bounded publication",
        specs=(
            SpecDraft(
                key="foundation",
                title="Foundation",
                scope="Foundation scope",
                acceptance_criteria=("Foundation works",),
                testing_decisions=("Test foundation",),
            ),
        ),
        grill_answers=(GrillAnswer(1, "approved"),),
        confirmation_operation_id="confirm-timeout",
        publication_operation_id="publish-timeout",
        publication_timeout_seconds=0.1,
    )
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="planning-timeout",
                workflows=[RequirementPlanningWorkflow],
                activities=[prepare_grill, publish_spec_issues],
            ):
                handle = await start_planning(environment, "planning-timeout", input)
                result = await handle.result()

        assert result.status is PlanningStatus.BLOCKED
        assert "timed out" in result.publication_reason
    finally:
        configure_spec_issue_gateway(None)


async def test_unknown_spec_publication_can_retry_by_operation_identity() -> None:
    gateway = FailOnceSpecIssueGateway()
    configure_spec_issue_gateway(gateway)
    input = PlanningInput(
        origin=SourceOrigin.TEXT,
        source_text="retry publication",
        specs=(
            SpecDraft(
                key="foundation",
                title="Foundation",
                scope="Foundation scope",
                acceptance_criteria=("Foundation works",),
                testing_decisions=("Test foundation",),
            ),
        ),
        grill_answers=(GrillAnswer(1, "approved"),),
        confirmation_operation_id="confirm-retry",
        publication_operation_id="publish-retry",
        publication_timeout_seconds=10,
    )
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="planning-retry",
                workflows=[RequirementPlanningWorkflow],
                activities=[prepare_grill, publish_spec_issues],
            ):
                handle = await start_planning(environment, "planning-retry", input)
                for _ in range(100):
                    snapshot = await handle.query(RequirementPlanningWorkflow.get_status)
                    if snapshot.status is PlanningStatus.UNKNOWN:
                        break
                    await asyncio.sleep(0.01)
                assert snapshot.status is PlanningStatus.UNKNOWN
                assert await handle.execute_update(
                    RequirementPlanningWorkflow.retry_spec_publication,
                    result_type=bool,
                )
                result = await handle.result()

        assert result.status is PlanningStatus.COMPLETED
        assert len(result.published_specs) == 1
        assert len(gateway.issues) == 1
    finally:
        configure_spec_issue_gateway(None)


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


@activity.defn(name="publish-spec-issues")
async def failed_publication(input):
    raise ApplicationError("publication worker failed", non_retryable=True)


async def test_activity_failure_enters_readback_instead_of_failing_workflow():
    input = PlanningInput(
        origin=SourceOrigin.TEXT, source_text="failed publication",
        specs=(SpecDraft("a", "A", "A", ("works",), ("test",)),),
        grill_answers=(GrillAnswer(1, "approved"),),
        confirmation_operation_id="confirm", publication_operation_id="publish",
        publication_timeout_seconds=0.1,
    )
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client, task_queue="publication-failure",
            workflows=[RequirementPlanningWorkflow],
            activities=[prepare_grill, failed_publication],
        ):
            handle = await start_planning(environment, "publication-failure", input)
            result = await handle.result()
    assert result.status is PlanningStatus.BLOCKED
    assert "readback timed out" in result.publication_reason
