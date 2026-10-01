import asyncio

from temporalio import workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.activities import foundation_stage
from temporalio_codex.delivery_models import CandidateEvidence
from temporalio_codex.models import RunInput, StageDefinition
from temporalio_codex.ticket_scheduler import (
    SchedulerInput,
    SchedulerResult,
    SchedulerStatus,
    SpecPlan,
    TicketPlan,
    validate_scheduler_graph,
)
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
from temporalio_codex.workflows import CodexRunWorkflow


def scheduler_input() -> SchedulerInput:
    return SchedulerInput(
        specs=(SpecPlan("first"), SpecPlan("second", ("first",))),
        tickets=(
            TicketPlan("first-a", "first"),
            TicketPlan("first-b", "first"),
            TicketPlan("second-a", "second", ("first-a", "first-b")),
        ),
    )


def reverse_dependency_scheduler_input() -> SchedulerInput:
    return SchedulerInput(
        specs=(SpecPlan("second", ("first",)), SpecPlan("first")),
        tickets=(
            TicketPlan("second-a", "second"),
            TicketPlan("first-a", "first"),
        ),
    )


@workflow.defn
class SchedulerConfigurationParent:
    @workflow.run
    async def run(self) -> SchedulerResult:
        workflow_id = workflow.info().workflow_id
        run_id = workflow.info().run_id
        handle = await workflow.start_child_workflow(
            TicketSchedulerWorkflow.run,
            SchedulerInput(
                specs=(SpecPlan("first"),),
                tickets=(
                    TicketPlan("first-a", "first"),
                    TicketPlan("first-b", "first", ("first-a",)),
                ),
            ),
            id=f"{workflow_id}:tickets:first",
            task_queue=workflow.info().task_queue,
            result_type=SchedulerResult,
        )
        await handle.signal(
            "configure_codex_runs",
            tuple(
                (
                    ticket_key,
                    RunInput(
                        f"implement {ticket_key}",
                        (StageDefinition(key="foundation"),),
                        parent_workflow_id=workflow_id,
                        parent_workflow_run_id=run_id,
                        automatic=True,
                        candidate=CandidateEvidence("repo", "workspace", "base", "base"),
                    ),
                )
                for ticket_key in ("first-a", "first-b")
            ),
        )
        return await handle


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


async def test_scheduler_orders_specs_by_dependencies_not_input_order() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="ticket-scheduler-reverse",
            workflows=[TicketSchedulerWorkflow],
        ):
            handle = await environment.client.start_workflow(
                TicketSchedulerWorkflow.run,
                reverse_dependency_scheduler_input(),
                id="ticket-scheduler-reverse",
                task_queue="ticket-scheduler-reverse",
            )
            snapshot = await wait_for_frontier(handle, ("first-a",))
            assert snapshot.active_spec == "first"
            assert await handle.execute_update(
                TicketSchedulerWorkflow.complete_ticket,
                args=["first-a", "reverse-first"],
                result_type=bool,
            )
            await wait_for_frontier(handle, ("second-a",))
            assert await handle.execute_update(
                TicketSchedulerWorkflow.complete_ticket,
                args=["second-a", "reverse-second"],
                result_type=bool,
            )
            result = await handle.result()

    assert result.status is SchedulerStatus.COMPLETED


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


async def test_ready_ticket_runs_codex_and_completes_without_update() -> None:
    input = SchedulerInput(
        specs=(SpecPlan("first"),),
        tickets=(TicketPlan("first-a", "first"), TicketPlan("first-b", "first", ("first-a",))),
        codex_runs=(
            (
                "first-a",
                RunInput("implement first-a", (StageDefinition(key="foundation"),)),
            ),
            (
                "first-b",
                RunInput("implement first-b", (StageDefinition(key="foundation"),)),
            ),
        ),
    )
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="ticket-automatic-codex",
            workflows=[TicketSchedulerWorkflow, CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            handle = await environment.client.start_workflow(
                TicketSchedulerWorkflow.run,
                input,
                id="ticket-automatic-codex",
                task_queue="ticket-automatic-codex",
            )
            result = await handle.result()

    assert result.status is SchedulerStatus.COMPLETED
    assert result.completed_tickets == ("first-a", "first-b")
    assert len(result.codex_results) == 2


async def test_legacy_scheduler_can_be_configured_by_parent_and_run_dependent_tickets() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="ticket-parent-configure",
            workflows=[
                SchedulerConfigurationParent,
                TicketSchedulerWorkflow,
                CodexRunWorkflow,
            ],
            activities=[foundation_stage],
        ):
            result = await environment.client.execute_workflow(
                SchedulerConfigurationParent.run,
                id="ticket-parent-configure",
                task_queue="ticket-parent-configure",
            )

    assert result.status is SchedulerStatus.COMPLETED
    assert result.completed_tickets == ("first-a", "first-b")
    assert len(result.codex_results) == 2


async def test_automatic_scheduler_blocks_ready_ticket_without_codex_plan() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="ticket-missing-plan",
            workflows=[TicketSchedulerWorkflow],
        ):
            result = await environment.client.execute_workflow(
                TicketSchedulerWorkflow.run,
                SchedulerInput(
                    specs=(SpecPlan("first"),),
                    tickets=(TicketPlan("first-a", "first"),),
                    automatic=True,
                ),
                id="ticket-missing-plan",
                task_queue="ticket-missing-plan",
            )
    assert result.status is SchedulerStatus.BLOCKED
    assert "no automatic Codex plan" in result.reason


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
