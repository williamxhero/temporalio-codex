from temporalio import activity

from temporalio_codex.planning_models import (
    GrillPreparationInput,
    GrillQuestion,
    SourceOrigin,
)
from temporalio_codex.spec_issue_adapter import (
    SpecIssueGateway,
    SpecPublicationInput,
    SpecPublicationResult,
    SpecPublicationStatus,
    publish_specs,
)


_spec_issue_gateway: SpecIssueGateway | None = None


def configure_spec_issue_gateway(gateway: SpecIssueGateway | None) -> None:
    global _spec_issue_gateway
    _spec_issue_gateway = gateway


@activity.defn(name="prepare_grill")
async def prepare_grill(
    input: GrillPreparationInput,
) -> tuple[GrillQuestion, ...]:
    if input.origin is SourceOrigin.HISTORICAL_CHAT:
        prompt = "Which decisions in the historical chat are confirmed requirements?"
    else:
        prompt = "What user-visible outcome must this brief deliver?"
    return (GrillQuestion(number=1, prompt=prompt),)


@activity.defn(name="publish-spec-issues")
async def publish_spec_issues(input: SpecPublicationInput) -> SpecPublicationResult:
    if _spec_issue_gateway is None:
        return SpecPublicationResult(
            status=SpecPublicationStatus.UNKNOWN,
            reason="GitHub SPEC issue adapter is unavailable",
        )
    return await publish_specs(input, _spec_issue_gateway)
