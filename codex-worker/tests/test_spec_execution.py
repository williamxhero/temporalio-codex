import asyncio
from dataclasses import replace

import pytest
from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from acceptance.test_whole_flow import whole_flow_input
from test_entry_launch import capture_fake_candidate
from temporalio_codex.activities import (
    configure_delivery_adapters, delivery_git_stage, delivery_github_stage,
)
from temporalio_codex.codex_models import CodexObservation, CodexOperation, CodexOutcome
from temporalio_codex.delivery_adapter import FakeDeliveryAdapter
from temporalio_codex.planning_activities import (
    configure_spec_issue_gateway, configure_ticket_issue_gateway, prepare_grill,
    publish_spec_issues, publish_ticket_issues,
)
from temporalio_codex.spec_issue_adapter import FakeSpecIssueGateway
from temporalio_codex.summary_activities import configure_summary_gateway, publish_delivery_summary
from temporalio_codex.summary_adapter import FakeSummaryCommentGateway
from temporalio_codex.ticket_issue_adapter import FakeTicketIssueGateway
from temporalio_codex.ticket_scheduler import SpecPlan
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow


@pytest.mark.parametrize("multiple", [False, True])
@pytest.mark.parametrize("rejected", [False, True])
async def test_spec_execution_has_codex_activities_without_stage_children(multiple, rejected):
    from temporalio_codex.spec_workflows import SpecExecutionWorkflow

    plan = replace(whole_flow_input(), execution_layout="project" if multiple else "spec")
    if not multiple:
        plan = replace(
            plan,
            planning=replace(plan.planning, specs=(plan.planning.specs[0],)),
            scheduler=replace(plan.scheduler, specs=(SpecPlan("foundation"),),
                              tickets=plan.scheduler.tickets[:2]),
            codex=plan.codex[:1], deliveries=plan.deliveries[:1],
        )
    operations = []

    @activity.defn(name="codex-stage")
    async def observe_codex(operation: CodexOperation) -> CodexObservation:
        operations.append(operation)
        return CodexObservation(
            operation.operation_id, operation.role, CodexOutcome.COMPLETED, "thread", "turn",
            summary='{"candidate_sha":"candidate-sha","verdict":"rejected","findings":["review failed"]}'
            if rejected and operation.role.value == "review"
            else '{"candidate_sha":"candidate-sha","verdict":"approved","findings":[]}',
        )

    configure_delivery_adapters(FakeDeliveryAdapter(), FakeDeliveryAdapter())
    configure_spec_issue_gateway(FakeSpecIssueGateway())
    configure_ticket_issue_gateway(FakeTicketIssueGateway())
    configure_summary_gateway(FakeSummaryCommentGateway())
    try:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client, task_queue="spec-layout",
                workflows=[RequirementDeliveryWorkflow, SpecExecutionWorkflow],
                activities=[observe_codex, capture_fake_candidate, delivery_git_stage,
                            delivery_github_stage, prepare_grill, publish_spec_issues,
                            publish_ticket_issues, publish_delivery_summary],
            ):
                handle = await env.client.start_workflow(
                    RequirementDeliveryWorkflow.run if multiple else SpecExecutionWorkflow.run,
                    plan, id="project:temporalio-codex:spec:layout", task_queue="spec-layout",
                )
                result = await asyncio.wait_for(handle.result(), 30)
                history = await handle.fetch_history()
                children = [event for event in history.events
                            if event.event_type == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED]
                assert len(children) == (1 if multiple and rejected else 2 if multiple else 0)
                for event in children:
                    attributes = event.start_child_workflow_execution_initiated_event_attributes
                    assert attributes.workflow_type.name == "SpecExecutionWorkflow"
                    assert attributes.workflow_id.endswith((":#100", ":#101"))
                    child = env.client.get_workflow_handle(attributes.workflow_id)
                    child_history = await child.fetch_history()
                    assert not any(event.event_type == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
                                   for event in child_history.events)
                    await Replayer(workflows=[SpecExecutionWorkflow]).replay_workflow(child_history)
                assert result.status.value == ("failed" if rejected else "completed")
                assert operations
                assert len({op.operation_id for op in operations}) == len(operations)
                assert len({op.run_id for op in operations}) == (2 if multiple and not rejected else 1)
                await Replayer(workflows=[RequirementDeliveryWorkflow, SpecExecutionWorkflow]).replay_workflow(history)
    finally:
        configure_delivery_adapters(None, None)
        configure_spec_issue_gateway(None)
        configure_ticket_issue_gateway(None)
        configure_summary_gateway(None)
