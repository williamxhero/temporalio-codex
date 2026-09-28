from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.summary_adapter import (
        SummaryPublicationInput,
        SummaryPublicationResult,
        SummaryPublicationStatus,
    )
    from temporalio_codex.summary_activities import publish_delivery_summary


@workflow.defn
class DeliverySummaryWorkflow:
    def __init__(self) -> None:
        self._status = SummaryPublicationStatus.UNKNOWN
        self._result: SummaryPublicationResult | None = None

    @workflow.run
    async def run(self, input: SummaryPublicationInput) -> SummaryPublicationResult:
        result = await workflow.execute_activity(
            publish_delivery_summary,
            input,
            start_to_close_timeout=timedelta(seconds=30),
        )
        self._result = result
        self._status = result.status
        if result.status is SummaryPublicationStatus.UNKNOWN:
            await workflow.wait_condition(
                lambda: self._result is not None
                and self._result.status is not SummaryPublicationStatus.UNKNOWN
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
        self._result = result
        self._status = result.status
        return True
