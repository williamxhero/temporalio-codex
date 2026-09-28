import asyncio

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.summary_activities import (
    configure_summary_gateway,
    publish_delivery_summary,
)
from temporalio_codex.summary_adapter import (
    FakeSummaryCommentGateway,
    SummaryPublicationInput,
    SummaryPublicationResult,
    SummaryPublicationStatus,
    SummaryCommentRecord,
)
from temporalio_codex.summary_workflows import DeliverySummaryWorkflow


def publication_input() -> SummaryPublicationInput:
    return SummaryPublicationInput(
        repository="owner/repo",
        umbrella_issue_number=8,
        operation_id="summary-workflow:summary",
        summary_text="Status: pass",
    )


async def test_summary_workflow_publishes_final_summary() -> None:
    configure_summary_gateway(FakeSummaryCommentGateway())
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="summary-complete",
                workflows=[DeliverySummaryWorkflow],
                activities=[publish_delivery_summary],
            ):
                handle = await environment.client.start_workflow(
                    DeliverySummaryWorkflow.run,
                    publication_input(),
                    id="summary-complete",
                    task_queue="summary-complete",
                )
                result = await handle.result()
        assert result.status is SummaryPublicationStatus.VERIFIED
        assert result.comment is not None
    finally:
        configure_summary_gateway(None)


async def test_summary_workflow_waits_for_external_readback() -> None:
    configure_summary_gateway(FakeSummaryCommentGateway(fail_find=True))
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="summary-unknown",
                workflows=[DeliverySummaryWorkflow],
                activities=[publish_delivery_summary],
            ):
                handle = await environment.client.start_workflow(
                    DeliverySummaryWorkflow.run,
                    publication_input(),
                    id="summary-unknown",
                    task_queue="summary-unknown",
                )
                for _ in range(100):
                    status = await handle.query(DeliverySummaryWorkflow.get_status)
                    if status is not None and status.status is SummaryPublicationStatus.UNKNOWN:
                        break
                    await asyncio.sleep(0.01)
                assert status is not None
                assert status.status is SummaryPublicationStatus.UNKNOWN
                resolved = SummaryPublicationResult(
                    status=SummaryPublicationStatus.VERIFIED,
                    reason="verified by external comment readback",
                )
                assert await handle.execute_update(
                    DeliverySummaryWorkflow.resolve_summary_publication,
                    resolved,
                    result_type=bool,
                ) is False
                resolved = SummaryPublicationResult(
                    status=SummaryPublicationStatus.VERIFIED,
                    comment=SummaryCommentRecord(
                        comment_id=1,
                        issue_number=8,
                        operation_id="summary-workflow:summary",
                        body="verified body",
                    ),
                    reason="verified by external comment readback",
                )
                assert await handle.execute_update(
                    DeliverySummaryWorkflow.resolve_summary_publication,
                    resolved,
                    result_type=bool,
                )
                result = await handle.result()
        assert result == resolved
    finally:
        configure_summary_gateway(None)
