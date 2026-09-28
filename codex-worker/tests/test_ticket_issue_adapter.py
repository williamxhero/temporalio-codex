from temporalio_codex.ticket_issue_adapter import (
    FakeTicketIssueGateway,
    TicketPublicationInput,
    TicketPublicationStatus,
    publish_tickets,
)
from temporalio_codex.ticket_scheduler import TicketPlan


def ticket_input() -> TicketPublicationInput:
    return TicketPublicationInput(
        repository="owner/repo",
        operation_id="run:tickets:foundation",
        spec_issue_number=43,
        blocker_issue_numbers=(),
        tickets=(
            TicketPlan("foundation-a", "foundation", title="Foundation A", acceptance_criteria=("A works",)),
            TicketPlan("foundation-b", "foundation", blockers=("foundation-a",), title="Foundation B"),
        ),
    )


async def test_ticket_publication_records_parent_blockers_and_adopts_retry() -> None:
    gateway = FakeTicketIssueGateway()
    result = await publish_tickets(ticket_input(), gateway)

    assert result.status is TicketPublicationStatus.VERIFIED
    assert result.issues[0].parent_spec_issue_number == 43
    assert result.issues[1].blocker_issue_numbers == (result.issues[0].number,)
    calls_after_first = list(gateway.calls)

    adopted = await publish_tickets(ticket_input(), gateway)

    assert adopted.status is TicketPublicationStatus.VERIFIED
    assert [record.number for record in adopted.issues] == [record.number for record in result.issues]
    assert gateway.calls.count("create:run:tickets:foundation:foundation-a") == 1
    assert len(gateway.calls) > len(calls_after_first)


async def test_ticket_publication_blocks_cycles_without_creating_issues() -> None:
    gateway = FakeTicketIssueGateway()
    input = TicketPublicationInput(
        **{
            **ticket_input().__dict__,
            "tickets": (
                TicketPlan("a", "foundation", blockers=("b",)),
                TicketPlan("b", "foundation", blockers=("a",)),
            ),
        }
    )

    result = await publish_tickets(input, gateway)

    assert result.status is TicketPublicationStatus.BLOCKED
    assert not gateway.issues
