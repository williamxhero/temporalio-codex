import asyncio
from pathlib import Path

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
from temporalio_codex.codex_adapter import FakeCodexAdapter
from temporalio_codex.delivery_adapter import FakeDeliveryAdapter
from temporalio_codex.delivery_models import DeliveryInput, DeliveryPhase
from temporalio_codex.planning_activities import (
    configure_ticket_issue_gateway,
    configure_spec_issue_gateway,
    publish_ticket_issues,
    prepare_grill,
    publish_spec_issues,
)
from temporalio_codex.planning_models import (
    GrillAnswer,
    PlanningInput,
    SourceOrigin,
)
from temporalio_codex.spec_issue_adapter import FakeSpecIssueGateway, SpecDraft
from temporalio_codex.ticket_issue_adapter import FakeTicketIssueGateway
from temporalio_codex.summary_activities import (
    configure_summary_gateway,
    publish_delivery_summary,
)
from temporalio_codex.summary_adapter import (
    FakeSummaryCommentGateway,
    SummaryPublicationInput,
)
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
from temporalio_codex.ticket_scheduler import SchedulerInput, SpecPlan, TicketPlan
from temporalio_codex.whole_flow_models import (
    SpecCodexPlan,
    SpecDeliveryPlan,
    WholeFlowInput,
    WholeFlowPhase,
    WholeFlowStatus,
    PlanningPayload,
    validate_whole_flow_input,
)
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow
from temporalio_codex.workflows import CodexRunWorkflow
from temporalio_codex.delivery_workflows import DeliveryWorkflow
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.summary_workflows import DeliverySummaryWorkflow


def spec_drafts() -> tuple[SpecDraft, ...]:
    return (
        SpecDraft(
            key="foundation",
            title="Foundation acceptance",
            scope="Prove the first delivery path",
            acceptance_criteria=("foundation is delivered",),
            testing_decisions=("run deterministic acceptance",),
            provenance=("grill:deliver a governed change",),
        ),
        SpecDraft(
            key="follow-up",
            title="Follow-up acceptance",
            scope="Prove sequential SPEC delivery",
            acceptance_criteria=("follow-up is delivered",),
            testing_decisions=("read back origin",),
            dependencies=("foundation",),
            provenance=("grill:deliver a governed change",),
        ),
    )


def delivery_input(spec_key: str) -> DeliveryInput:
    return DeliveryInput(
        repository=f"D:/acceptance/{spec_key}",
        workspace=f"D:/acceptance/{spec_key}/candidate",
        base_sha=f"base-{spec_key}",
        candidate_branch=f"codex/{spec_key}",
        pull_request_identity=f"acceptance:{spec_key}",
        title=f"Acceptance {spec_key}",
        body=f"acceptance:{spec_key}",
        issue_numbers=(101, 102),
    )


def whole_flow_input() -> WholeFlowInput:
    drafts = spec_drafts()
    return WholeFlowInput(
        planning=PlanningPayload(
            origin=SourceOrigin.TEXT,
            source_text="deliver a governed change",
            umbrella_issue_number=43,
            specs=drafts,
            grill_answers=(GrillAnswer(1, "deliver a governed change"),),
            confirmation_operation_id="acceptance:confirm",
            publication_operation_id="acceptance:publish",
        ),
        scheduler=SchedulerInput(
            specs=(
                SpecPlan("foundation"),
                SpecPlan("follow-up", ("foundation",)),
            ),
            tickets=(
                TicketPlan("foundation-a", "foundation"),
                TicketPlan("foundation-b", "foundation"),
                TicketPlan("follow-up-a", "follow-up", ("foundation-a", "foundation-b")),
            ),
            completion_operations=(),
        ),
        codex=tuple(
            SpecCodexPlan(
                spec_key=key,
                requirement=f"deliver {key}",
                repository=f"D:/acceptance/{key}",
                allowed_scope=("src/",),
            )
            for key in ("foundation", "follow-up")
        ),
        deliveries=tuple(
            SpecDeliveryPlan(key, delivery_input(key))
            for key in ("foundation", "follow-up")
        ),
        summary=SummaryPublicationInput(
            repository="owner/repo",
            umbrella_issue_number=43,
            operation_id="acceptance:summary",
            summary_text="Status: pass\nSPECs: foundation, follow-up\nRemote: verified",
        ),
    )


async def test_two_spec_whole_flow_runs_through_public_child_workflows() -> None:
    configure_codex_adapter(FakeCodexAdapter({}))
    configure_delivery_adapters(FakeDeliveryAdapter(), FakeDeliveryAdapter())
    configure_spec_issue_gateway(FakeSpecIssueGateway())
    configure_ticket_issue_gateway(FakeTicketIssueGateway())
    configure_summary_gateway(FakeSummaryCommentGateway())
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="whole-flow-acceptance",
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
                    id="whole-flow-acceptance",
                    task_queue="whole-flow-acceptance",
                )
                try:
                    result = await asyncio.wait_for(handle.result(), timeout=5)
                except asyncio.TimeoutError as error:
                    snapshot = await handle.query(RequirementDeliveryWorkflow.get_status)
                    raise AssertionError(f"whole flow stalled at {snapshot}") from error
                snapshot = await handle.query(RequirementDeliveryWorkflow.get_status)
        assert result.status == WholeFlowStatus.COMPLETED
        assert result.phase == WholeFlowPhase.COMPLETED
        assert snapshot.completed_specs == ("foundation", "follow-up")
        assert result.delivery_results
        assert all(
            run["ticket_publication"]["status"] == "verified"
            for run in result.scheduler["runs"]
        )
        assert all(
            len(run["ticket_publication"]["issues"]) > 0
            for run in result.scheduler["runs"]
        )
        assert all(
            any(receipt["phase"] == DeliveryPhase.PUSH for receipt in item["receipts"])
            for item in result.delivery_results
        )
        assert result.summary is not None
        assert result.summary["comment"] is not None
    finally:
        configure_codex_adapter(None)
        configure_delivery_adapters(None, None)
        configure_spec_issue_gateway(None)
        configure_ticket_issue_gateway(None)
        configure_summary_gateway(None)


async def test_historical_chat_is_accepted_without_persisting_inline_source() -> None:
    configure_spec_issue_gateway(FakeSpecIssueGateway())
    configure_ticket_issue_gateway(FakeTicketIssueGateway())
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="historical-chat-acceptance",
                workflows=[RequirementPlanningWorkflow],
                activities=[prepare_grill, publish_spec_issues],
            ):
                handle = await environment.client.start_workflow(
                    RequirementPlanningWorkflow.run,
                    PlanningPayload(
                        origin=SourceOrigin.HISTORICAL_CHAT,
                        source_reference="artifact://acceptance/chat-1",
                        grill_answers=(GrillAnswer(1, "the approved chat decision"),),
                        confirmation_operation_id="historical:confirm",
                        publication_operation_id="historical:publish",
                    ),
                    id="historical-chat-acceptance",
                    task_queue="historical-chat-acceptance",
                )
                result = await handle.result()
        assert result.source.source_reference == "artifact://acceptance/chat-1"
        assert result.source.source_identity
        assert result.source.source_reference != "the approved chat decision"
    finally:
        configure_spec_issue_gateway(None)
        configure_ticket_issue_gateway(None)


def test_whole_flow_rejects_future_cross_spec_blocker_before_starting_children() -> None:
    input = whole_flow_input()
    scheduler = SchedulerInput(
        specs=input.scheduler.specs,
        tickets=(
            TicketPlan("foundation-a", "foundation", ("follow-up-a",)),
            TicketPlan("foundation-b", "foundation"),
            TicketPlan("follow-up-a", "follow-up"),
        ),
        completion_operations=input.scheduler.completion_operations,
    )
    errors = validate_whole_flow_input(
        WholeFlowInput(
            planning=input.planning,
            scheduler=scheduler,
            codex=input.codex,
            deliveries=input.deliveries,
            summary=input.summary,
        )
    )

    assert any("later or unrelated" in error for error in errors)


def test_acceptance_directory_has_no_local_spec_mirror() -> None:
    assert not (Path(__file__).parents[3] / "docs" / "specs").exists()
