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
from temporalio_codex.ticket_issue_adapter import (
    TicketIssueGateway,
    TicketPublicationInput,
    TicketPublicationResult,
    TicketPublicationStatus,
    publish_tickets,
)


_spec_issue_gateway: SpecIssueGateway | None = None
_ticket_issue_gateway: TicketIssueGateway | None = None


def configure_spec_issue_gateway(gateway: SpecIssueGateway | None) -> None:
    global _spec_issue_gateway
    _spec_issue_gateway = gateway


def configure_ticket_issue_gateway(gateway: TicketIssueGateway | None) -> None:
    global _ticket_issue_gateway
    _ticket_issue_gateway = gateway


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


@activity.defn(name="publish-ticket-issues")
async def publish_ticket_issues(input: TicketPublicationInput) -> TicketPublicationResult:
    if _ticket_issue_gateway is None:
        return TicketPublicationResult(
            status=TicketPublicationStatus.UNKNOWN,
            reason="GitHub ticket Issue adapter is unavailable",
        )
    return await publish_tickets(input, _ticket_issue_gateway)
