from temporalio import activity

from temporalio_codex.summary_adapter import (
    SummaryCommentGateway,
    SummaryPublicationInput,
    SummaryPublicationResult,
    SummaryPublicationStatus,
    publish_summary,
)


_summary_gateway: SummaryCommentGateway | None = None


def configure_summary_gateway(gateway: SummaryCommentGateway | None) -> None:
    global _summary_gateway
    _summary_gateway = gateway


@activity.defn(name="publish-delivery-summary")
async def publish_delivery_summary(
    input: SummaryPublicationInput,
) -> SummaryPublicationResult:
    if _summary_gateway is None:
        return SummaryPublicationResult(
            SummaryPublicationStatus.UNKNOWN,
            reason="GitHub summary adapter is unavailable",
        )
    return await publish_summary(input, _summary_gateway)
