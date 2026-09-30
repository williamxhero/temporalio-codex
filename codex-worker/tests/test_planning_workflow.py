import asyncio
import hashlib

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.planning_activities import (
    configure_spec_issue_gateway,
    prepare_grill,
    publish_spec_issues,
)
from temporalio_codex.planning_models import (
    GrillAnswer,
    PlanningInput,
    PlanningPhase,
    PlanningStatus,
    SourceOrigin,
)
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
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


async def test_text_intake_completes_without_manual_planning_signals() -> None:
    input = PlanningInput(
        origin=SourceOrigin.TEXT, source_text="ship a governed change"
    )
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="planning-text",
            workflows=[RequirementPlanningWorkflow],
            activities=[prepare_grill],
        ):
            handle = await start_planning(environment, "planning-text", input)
            result = await handle.result()

    assert result.status is PlanningStatus.COMPLETED
    assert result.phase is PlanningPhase.COMPLETED
    assert (
        result.source.source_identity
        == hashlib.sha256(b"text:ship a governed change").hexdigest()
    )
    assert result.grill.decisions == ("ship a governed change",)
    assert result.confirmation_operation_id.startswith("planning-confirm:")
    assert result.publication_operation_id.startswith("planning-publish:")


async def test_historical_chat_without_readable_context_blocks_without_manual_input() -> (
    None
):
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
            result = await handle.result()

    assert result.status is PlanningStatus.BLOCKED
    assert result.source.source_reference == "artifact://chat-123"
    assert result.grill.decisions == ()
    assert "unable to safely derive" in result.publication_reason


async def test_specs_publish_without_confirmation_signal() -> None:
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
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="planning-publish",
                workflows=[RequirementPlanningWorkflow],
                activities=[prepare_grill, publish_spec_issues],
            ):
                handle = await start_planning(environment, "planning-publish", input)
                result = await handle.result()
    finally:
        configure_spec_issue_gateway(None)

    assert result.status is PlanningStatus.COMPLETED
    assert len(result.published_specs) == 1
    assert result.confirmation_operation_id.startswith("planning-confirm:")
    assert result.publication_operation_id.startswith("planning-publish:")


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
                    snapshot = await handle.query(
                        RequirementPlanningWorkflow.get_status
                    )
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
        origin=SourceOrigin.TEXT,
        source_text="failed publication",
        specs=(SpecDraft("a", "A", "A", ("works",), ("test",)),),
        grill_answers=(GrillAnswer(1, "approved"),),
        confirmation_operation_id="confirm",
        publication_operation_id="publish",
        publication_timeout_seconds=0.1,
    )
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="publication-failure",
            workflows=[RequirementPlanningWorkflow],
            activities=[prepare_grill, failed_publication],
        ):
            handle = await start_planning(environment, "publication-failure", input)
            result = await handle.result()
    assert result.status is PlanningStatus.BLOCKED
    assert "readback timed out" in result.publication_reason


async def test_planning_completes_automatically_without_operator_signals() -> None:
    input = PlanningInput(
        origin=SourceOrigin.TEXT,
        source_text="automatically deliver this requirement",
    )
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="planning-automatic",
            workflows=[RequirementPlanningWorkflow],
            activities=[prepare_grill],
        ):
            handle = await start_planning(environment, "planning-automatic", input)
            result = await handle.result()

    assert result.status is PlanningStatus.COMPLETED
    assert result.phase is PlanningPhase.COMPLETED
    assert result.grill.decisions == (input.source_text,)
    assert result.confirmation_operation_id.startswith("planning-confirm:")
    assert result.publication_operation_id.startswith("planning-publish:")
