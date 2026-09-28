import asyncio

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.ticket_scheduler import (
    SchedulerInput,
    SchedulerStatus,
    SpecPlan,
    TicketPlan,
    validate_scheduler_graph,
)
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow


def scheduler_input() -> SchedulerInput:
    return SchedulerInput(
        specs=(SpecPlan("first"), SpecPlan("second", ("first",))),
        tickets=(
            TicketPlan("first-a", "first"),
            TicketPlan("first-b", "first"),
            TicketPlan("second-a", "second", ("first-a", "first-b")),
        ),
    )


async def wait_for_frontier(handle, frontier):
    for _ in range(100):
        snapshot = await handle.query(TicketSchedulerWorkflow.get_status)
        if snapshot.ready_frontier == frontier:
            return snapshot
        await asyncio.sleep(0.01)
    raise AssertionError(f"frontier did not become {frontier}")


async def test_parallel_frontier_advances_specs_in_order() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="ticket-scheduler",
            workflows=[TicketSchedulerWorkflow],
        ):
            handle = await environment.client.start_workflow(
                TicketSchedulerWorkflow.run,
                scheduler_input(),
                id="ticket-scheduler",
                task_queue="ticket-scheduler",
            )
            snapshot = await wait_for_frontier(handle, ("first-a", "first-b"))
            assert snapshot.active_spec == "first"
            assert await handle.execute_update(
                TicketSchedulerWorkflow.complete_ticket,
                args=["second-a", "too-early"],
                result_type=bool,
            ) is False
            assert await handle.execute_update(
                TicketSchedulerWorkflow.complete_ticket,
                args=["first-a", "complete-first-a"],
                result_type=bool,
            )
            assert (
                await handle.execute_update(
                    TicketSchedulerWorkflow.complete_ticket,
                    args=["first-a", "complete-first-a"],
                    result_type=bool,
                )
                is True
            )
            await wait_for_frontier(handle, ("first-b",))
            assert await handle.execute_update(
                TicketSchedulerWorkflow.complete_ticket,
                args=["first-b", "complete-first-b"],
                result_type=bool,
            )
            await wait_for_frontier(handle, ("second-a",))
            assert await handle.execute_update(
                TicketSchedulerWorkflow.complete_ticket,
                args=["second-a", "complete-second-a"],
                result_type=bool,
            )
            result = await handle.result()

    assert result.status is SchedulerStatus.COMPLETED
    assert result.completed_specs == ("first", "second")


async def test_scheduler_restart_preserves_frontier() -> None:
    async with await WorkflowEnvironment.start_local() as environment:
        handle = await environment.client.start_workflow(
            TicketSchedulerWorkflow.run,
            scheduler_input(),
            id="ticket-restart",
            task_queue="ticket-restart",
        )
        async with Worker(
            environment.client,
            task_queue="ticket-restart",
            workflows=[TicketSchedulerWorkflow],
        ):
            await wait_for_frontier(handle, ("first-a", "first-b"))
            assert await handle.execute_update(
                TicketSchedulerWorkflow.complete_ticket,
                args=["first-a", "restart-a"],
                result_type=bool,
            )
        async with Worker(
            environment.client,
            task_queue="ticket-restart",
            workflows=[TicketSchedulerWorkflow],
        ):
            snapshot = await wait_for_frontier(handle, ("first-b",))
            assert snapshot.completed_tickets == ("first-a",)
            assert await handle.execute_update(
                TicketSchedulerWorkflow.complete_ticket,
                args=["first-b", "restart-b"],
                result_type=bool,
            )
            await wait_for_frontier(handle, ("second-a",))
            assert await handle.execute_update(
                TicketSchedulerWorkflow.complete_ticket,
                args=["second-a", "restart-second"],
                result_type=bool,
            )
            result = await handle.result()
    assert result.status is SchedulerStatus.COMPLETED


def test_scheduler_graph_rejects_unresolved_and_cycle() -> None:
    assert "unresolved blockers" in " ".join(
        validate_scheduler_graph(
            SchedulerInput((SpecPlan("one"),), (TicketPlan("a", "one", ("missing",)),))
        )
    )
    assert "cycle" in " ".join(
        validate_scheduler_graph(
            SchedulerInput(
                (SpecPlan("one"),),
                (TicketPlan("a", "one", ("b",)), TicketPlan("b", "one", ("a",))),
            )
        )
    )
    assert "requires at least one ticket" in " ".join(
        validate_scheduler_graph(SchedulerInput((SpecPlan("empty"),), ()))
    )
