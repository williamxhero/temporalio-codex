from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.execution_status import report_progress
    from temporalio_codex.summary_activities import publish_delivery_summary
    from temporalio_codex.summary_adapter import (
        SummaryPublicationInput,
        SummaryPublicationResult,
        SummaryPublicationStatus,
    )


@workflow.defn
class DeliverySummaryWorkflow:
    def __init__(self) -> None:
        self._status = SummaryPublicationStatus.UNKNOWN
        self._result: SummaryPublicationResult | None = None

    @workflow.run
    async def run(self, input: SummaryPublicationInput) -> SummaryPublicationResult:
        for attempt in range(3 if input.automatic else 1):
            await report_progress(phase="summary", next_action="publish and verify summary",
                                  retry_count=attempt, timeout_seconds=30,
                                  deadline=(workflow.now() + timedelta(seconds=30)).isoformat())
            result = await workflow.execute_activity(
                publish_delivery_summary,
                input,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            self._result = result
            self._status = result.status
            if result.status is not SummaryPublicationStatus.UNKNOWN:
                break
            if input.automatic and attempt < 2:
                await report_progress(
                    phase="summary", status="retrying", retry_count=attempt + 1,
                    pending_reason="summary publication outcome requires readback",
                    next_action="reconcile summary publication",
                    deadline=(workflow.now() + timedelta(seconds=2**attempt)).isoformat(),
                    last_error=result.reason,
                )
                await workflow.sleep(timedelta(seconds=2**attempt))
        self._result = result
        self._status = result.status
        if result.status is SummaryPublicationStatus.UNKNOWN and not input.automatic:
            await workflow.wait_condition(
                lambda: (
                    self._result is not None
                    and self._result.status is not SummaryPublicationStatus.UNKNOWN
                )
            )
            result = self._result
            assert result is not None
        return result

    @workflow.query(name="get_summary_publication_status")
    def get_status(self) -> SummaryPublicationResult | None:
        return self._result

    @workflow.update(name="resolve_summary_publication")
    async def resolve_summary_publication(
        self, result: SummaryPublicationResult
    ) -> bool:
        if (
            self._result is None
            or self._result.status is not SummaryPublicationStatus.UNKNOWN
        ):
            return False
        if result.status is SummaryPublicationStatus.UNKNOWN:
            return False
        if (
            result.status is SummaryPublicationStatus.VERIFIED
            and result.comment is None
        ):
            return False
        self._result = result
        self._status = result.status
        return True
