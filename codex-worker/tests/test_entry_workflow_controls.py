import asyncio
from dataclasses import replace

from acceptance.test_whole_flow import whole_flow_input
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.activities import (
    codex_stage,
    delivery_git_stage,
    delivery_github_stage,
    foundation_stage,
    heartbeat_stage,
)
from temporalio_codex.delivery_workflows import DeliveryWorkflow
from temporalio_codex.planning_activities import (
    configure_spec_issue_gateway,
    prepare_grill,
    publish_spec_issues,
    publish_ticket_issues,
)
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.spec_issue_adapter import FakeSpecIssueGateway
from temporalio_codex.summary_activities import publish_delivery_summary
from temporalio_codex.summary_workflows import DeliverySummaryWorkflow
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
from temporalio_codex.whole_flow_models import WholeFlowPhase, WholeFlowStatus
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow
from temporalio_codex.workflows import CodexRunWorkflow


async def test_public_controls_preserve_identity_and_cancel_before_children() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="entry-controls",
            workflows=[
                RequirementDeliveryWorkflow,
                RequirementPlanningWorkflow,
                TicketSchedulerWorkflow,
                CodexRunWorkflow,
                DeliveryWorkflow,
                DeliverySummaryWorkflow,
            ],
            activities=[
                foundation_stage,
                heartbeat_stage,
                codex_stage,
                delivery_git_stage,
                delivery_github_stage,
                prepare_grill,
                publish_spec_issues,
                publish_ticket_issues,
                publish_delivery_summary,
            ],
        ):
            handle = await environment.client.start_workflow(
                RequirementDeliveryWorkflow.run,
                whole_flow_input(),
                id="entry-controls-run",
                task_queue="entry-controls",
            )
            paused = await handle.execute_update(
                RequirementDeliveryWorkflow.pause,
                result_type=bool,
            )
            snapshot = await handle.query(RequirementDeliveryWorkflow.get_status)
            assert paused is True
            assert snapshot.status is WholeFlowStatus.BLOCKED
            assert snapshot.reason == "paused by operator"
            assert await handle.execute_update(
                RequirementDeliveryWorkflow.resume,
                result_type=bool,
            )
            assert await handle.execute_update(
                RequirementDeliveryWorkflow.answer,
                ("grill:1", "approved"),
                result_type=bool,
            )
            assert (
                await handle.execute_update(
                    RequirementDeliveryWorkflow.answer,
                    ("grill:1", "duplicate"),
                    result_type=bool,
                )
                is False
            )
            await handle.signal(RequirementDeliveryWorkflow.cancel)
            result = await asyncio.wait_for(handle.result(), timeout=5)
            final = await handle.query(RequirementDeliveryWorkflow.get_status)

    assert result.status is WholeFlowStatus.CANCELLED
    assert result.phase is WholeFlowPhase.CANCELLED
    assert final.status is WholeFlowStatus.CANCELLED
    assert final.completed_specs == ()


async def test_parent_status_exposes_bounded_planning_readback_blocker() -> None:
    configure_spec_issue_gateway(FakeSpecIssueGateway(fail_create=True))
    input = whole_flow_input()
    input = replace(
        input,
        planning=replace(input.planning, publication_timeout_seconds=0.1),
    )
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="entry-planning-blocked",
                workflows=[
                    RequirementDeliveryWorkflow,
                    RequirementPlanningWorkflow,
                    TicketSchedulerWorkflow,
                    CodexRunWorkflow,
                    DeliveryWorkflow,
                    DeliverySummaryWorkflow,
                ],
                activities=[
                    foundation_stage,
                    heartbeat_stage,
                    codex_stage,
                    delivery_git_stage,
                    delivery_github_stage,
                    prepare_grill,
                    publish_spec_issues,
                    publish_ticket_issues,
                    publish_delivery_summary,
                ],
            ):
                handle = await environment.client.start_workflow(
                    RequirementDeliveryWorkflow.run,
                    input,
                    id="entry-planning-blocked",
                    task_queue="entry-planning-blocked",
                )
                result = await asyncio.wait_for(handle.result(), timeout=5)
                snapshot = await handle.query(RequirementDeliveryWorkflow.get_status)

        assert result.status is WholeFlowStatus.BLOCKED
        assert snapshot.phase is WholeFlowPhase.BLOCKED
        assert snapshot.status is WholeFlowStatus.BLOCKED
        assert "timed out" in snapshot.reason
    finally:
        configure_spec_issue_gateway(None)

