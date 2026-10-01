from dataclasses import replace

import pytest
from acceptance.test_whole_flow import whole_flow_input
from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.activities import (
    codex_stage,
    configure_codex_adapter,
    configure_delivery_adapters,
    delivery_git_stage,
    delivery_github_stage,
    foundation_stage,
    heartbeat_stage,
)
from temporalio_codex.candidate_activities import (
    CandidateCaptureInput,
    CandidateCaptureResult,
)
from temporalio_codex.codex_adapter import FakeCodexAdapter
from temporalio_codex.codex_models import CodexRole
from temporalio_codex.delivery_adapter import FakeDeliveryAdapter
from temporalio_codex.delivery_models import ReviewEvidence
from temporalio_codex.delivery_workflows import DeliveryWorkflow
from temporalio_codex.entry import launch_requirement, stable_run_id
from temporalio_codex.entry_models import RequirementDeliveryRequest, RequirementSource
from temporalio_codex.planning_activities import (
    configure_spec_issue_gateway,
    configure_ticket_issue_gateway,
    prepare_grill,
    publish_spec_issues,
    publish_ticket_issues,
)
from temporalio_codex.planning_models import SourceOrigin
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.spec_issue_adapter import FakeSpecIssueGateway, SpecIssueRecord
from temporalio_codex.summary_activities import (
    configure_summary_gateway,
    publish_delivery_summary,
)
from temporalio_codex.summary_adapter import FakeSummaryCommentGateway
from temporalio_codex.summary_workflows import DeliverySummaryWorkflow
from temporalio_codex.ticket_issue_adapter import FakeTicketIssueGateway
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow
from temporalio_codex.workflows import CodexRunWorkflow
from temporalio_codex.spec_workflows import SpecExecutionWorkflow


@activity.defn(name="capture-codex-candidate")
async def capture_fake_candidate(input: CandidateCaptureInput) -> CandidateCaptureResult:
    return CandidateCaptureResult(
        candidate=input.candidate,
        review=ReviewEvidence(
            input.candidate.candidate_sha, "approved", input.operation_id,
            input.thread_id, input.turn_id,
        ) if input.review_json is not None else None,
    )


def delivery_request(*, single_spec: bool = False) -> RequirementDeliveryRequest:
    plan = whole_flow_input()
    if single_spec:
        spec = plan.scheduler.specs[0]
        plan = replace(
            plan,
            planning=replace(
                plan.planning,
                specs=(replace(plan.planning.specs[0], number=325),),
            ),
            scheduler=replace(
                plan.scheduler,
                specs=(spec,),
                tickets=tuple(ticket for ticket in plan.scheduler.tickets if ticket.spec_key == spec.key),
            ),
            codex=(replace(plan.codex[0], repository="D:/WILL/temporalio-codex-proj"),),
            deliveries=plan.deliveries[:1],
        )
    source = RequirementSource(SourceOrigin.TEXT, text=plan.planning.source_text)
    return RequirementDeliveryRequest(
        source=source,
        repository="williamxhero/temporalio-codex",
        artifact_roots=("codex-worker",),
        execution_plan=plan,
        launch_key="entry-launch-test",
        task_queue="entry-launch-test",
    )


class ApprovedCodexAdapter(FakeCodexAdapter):
    async def execute(self, operation):
        observation = await super().execute(operation)
        if operation.role is CodexRole.REVIEW:
            return replace(observation, summary='{"verdict":"approved","findings":[]}')
        return observation


@pytest.mark.parametrize("single_spec", [False, True])
async def test_public_launch_returns_durable_identity_and_adopts_retry(single_spec: bool) -> None:
    configure_codex_adapter(ApprovedCodexAdapter({}))
    configure_delivery_adapters(FakeDeliveryAdapter(), FakeDeliveryAdapter())
    spec_gateway = FakeSpecIssueGateway(
        issues={
            "existing-spec-325": SpecIssueRecord(
                number=325,
                issue_id=1_000_000_325,
                title="[SPEC foundation] Foundation acceptance",
                operation_id="existing-spec-325",
                parent_issue_number=43,
            ),
        } if single_spec else {},
    )
    configure_spec_issue_gateway(spec_gateway)
    configure_ticket_issue_gateway(FakeTicketIssueGateway())
    configure_summary_gateway(FakeSummaryCommentGateway())
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="entry-launch-test",
                workflows=[
                    RequirementDeliveryWorkflow,
                    SpecExecutionWorkflow,
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
                    capture_fake_candidate,
                ],
            ):
                request = delivery_request(single_spec=single_spec)
                first = await launch_requirement(environment.client, request)
                handle = environment.client.get_workflow_handle(first.run_id)
                result = await handle.result()
                history = await handle.fetch_history()
                retry = await launch_requirement(environment.client, request)

        assert first.adopted is False
        assert retry.adopted is True
        assert retry.run_id == first.run_id
        assert retry.input_identity == request.input_identity
        expected_specs = ("foundation",) if single_spec else ("foundation", "follow-up")
        assert retry.status.completed_specs == expected_specs
        assert result["status"] == "completed"
        if single_spec:
            assert first.run_id == "temporalio-codex-proj:#325"
            assert not any(
                event.event_type == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
                for event in history.events
            )
            assert tuple(spec_gateway.issues.values()) == (
                spec_gateway.issues["existing-spec-325"],
            )
        else:
            spec_children = [
                event.start_child_workflow_execution_initiated_event_attributes
                for event in history.events
                if event.event_type == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
                and event.start_child_workflow_execution_initiated_event_attributes.workflow_type.name
                == "SpecExecutionWorkflow"
            ]
            assert len(spec_children) == len(expected_specs)
            assert {child.workflow_id for child in spec_children} == {
                "foundation:#100", "follow-up:#101",
            }
    finally:
        configure_codex_adapter(None)
        configure_delivery_adapters(None, None)
        configure_spec_issue_gateway(None)
        configure_ticket_issue_gateway(None)
        configure_summary_gateway(None)


async def test_public_launch_rejects_input_drift_for_same_launch_key() -> None:
    configure_codex_adapter(ApprovedCodexAdapter({}))
    configure_delivery_adapters(FakeDeliveryAdapter(), FakeDeliveryAdapter())
    configure_spec_issue_gateway(FakeSpecIssueGateway())
    configure_ticket_issue_gateway(FakeTicketIssueGateway())
    configure_summary_gateway(FakeSummaryCommentGateway())
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="entry-launch-test",
                workflows=[
                    RequirementDeliveryWorkflow,
                    SpecExecutionWorkflow,
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
                    capture_fake_candidate,
                ],
            ):
                first = delivery_request()
                await launch_requirement(environment.client, first)
                changed_plan = whole_flow_input()
                changed_plan = changed_plan.__class__(
                    planning=changed_plan.planning.__class__(
                        origin=SourceOrigin.TEXT,
                        source_text="changed input",
                        umbrella_issue_number=changed_plan.planning.umbrella_issue_number,
                        specs=changed_plan.planning.specs,
                        grill_answers=changed_plan.planning.grill_answers,
                        confirmation_operation_id=changed_plan.planning.confirmation_operation_id,
                        publication_operation_id=changed_plan.planning.publication_operation_id,
                    ),
                    scheduler=changed_plan.scheduler,
                    codex=changed_plan.codex,
                    deliveries=changed_plan.deliveries,
                    summary=changed_plan.summary,
                )
                changed = first.__class__(
                    source=RequirementSource(SourceOrigin.TEXT, text="changed input"),
                    repository=first.repository,
                    artifact_roots=first.artifact_roots,
                    execution_plan=changed_plan,
                    launch_key=first.launch_key,
                    task_queue=first.task_queue,
                )
                assert stable_run_id(first) == stable_run_id(changed)
                try:
                    await launch_requirement(environment.client, changed)
                except ValueError as error:
                    assert "different normalized request" in str(error)
                else:
                    raise AssertionError("input drift was accepted")
    finally:
        configure_codex_adapter(None)
        configure_delivery_adapters(None, None)
        configure_spec_issue_gateway(None)
        configure_ticket_issue_gateway(None)
        configure_summary_gateway(None)
