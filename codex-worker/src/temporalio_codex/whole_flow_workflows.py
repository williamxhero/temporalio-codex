from dataclasses import asdict
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.delivery_workflows import DeliveryWorkflow
    from temporalio_codex.planning_activities import publish_ticket_issues
    from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
    from temporalio_codex.summary_workflows import DeliverySummaryWorkflow
    from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
    from temporalio_codex.whole_flow_models import (
        SpecCodexPlan,
        WholeFlowInput,
        WholeFlowPhase,
        WholeFlowResult,
        WholeFlowSnapshot,
        WholeFlowStatus,
        topological_spec_keys,
        validate_whole_flow_input,
    )
    from temporalio_codex.ticket_scheduler import SchedulerInput, SpecPlan, TicketPlan
    from temporalio_codex.ticket_issue_adapter import (
        TicketPublicationInput,
        TicketPublicationStatus,
    )
    from temporalio_codex.workflows import CodexRunWorkflow


@workflow.defn
class RequirementDeliveryWorkflow:
    def __init__(self) -> None:
        self._phase = WholeFlowPhase.PLANNING
        self._status = WholeFlowStatus.ACTIVE
        self._completed_specs: list[str] = []
        self._reason = ""

    @workflow.query(name="get_whole_flow_status")
    def get_status(self) -> WholeFlowSnapshot:
        return WholeFlowSnapshot(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            completed_specs=tuple(self._completed_specs),
            reason=self._reason,
        )

    @workflow.run
    async def run(self, input: WholeFlowInput) -> WholeFlowResult:
        validation_errors = validate_whole_flow_input(input)
        if validation_errors:
            return self._blocked("; ".join(validation_errors))

        self._phase = WholeFlowPhase.PLANNING
        planning = await workflow.execute_child_workflow(
            "RequirementPlanningWorkflow",
            input.planning.to_input(),
            id=f"{workflow.info().workflow_id}:planning",
            result_type=dict,
        )
        if planning.get("status") != "completed":
            return self._failed("planning did not complete", planning=planning)

        expected_specs = topological_spec_keys(input.scheduler)
        ticket_by_key = {ticket.key: ticket for ticket in input.scheduler.tickets}
        completion_by_ticket = dict(input.scheduler.completion_operations)
        codex_by_spec = {plan.spec_key: plan for plan in input.codex}
        delivery_by_spec = {plan.spec_key: plan for plan in input.deliveries}
        spec_issue_numbers = {
            record["operation_id"].rsplit(":", 1)[-1]: record["number"]
            for record in planning.get("published_specs", ())
        }
        ticket_issue_numbers: dict[str, int] = {}
        scheduler_runs: list[dict] = []
        codex_results: list[dict] = []
        delivery_results: list[dict] = []

        for spec_key in expected_specs:
            self._phase = WholeFlowPhase.TICKETS
            spec_tickets = tuple(
                TicketPlan(
                    ticket.key,
                    ticket.spec_key,
                    tuple(
                        blocker
                        for blocker in ticket.blockers
                        if ticket_by_key[blocker].spec_key == spec_key
                    ),
                    ticket.title,
                    ticket.acceptance_criteria,
                )
                for ticket in input.scheduler.tickets
                if ticket.spec_key == spec_key
            )
            scheduler_input = SchedulerInput(
                specs=(SpecPlan(spec_key),),
                tickets=spec_tickets,
                completion_operations=tuple(
                    (ticket.key, completion_by_ticket[ticket.key])
                    for ticket in spec_tickets
                    if ticket.key in completion_by_ticket
                ),
            )
            spec_issue_number = spec_issue_numbers.get(spec_key)
            if spec_issue_number is None:
                return self._failed(
                    f"SPEC {spec_key} has no published Issue readback",
                    planning,
                    {"status": "blocked", "runs": scheduler_runs},
                )
            ticket_publication = await workflow.execute_activity(
                publish_ticket_issues,
                TicketPublicationInput(
                    repository="williamxhero/temporalio-codex",
                    operation_id=f"{workflow.info().workflow_id}:tickets:{spec_key}",
                    spec_issue_number=spec_issue_number,
                    blocker_issue_numbers=tuple(
                        (blocker, ticket_issue_numbers[blocker])
                        for ticket in spec_tickets
                        for blocker in ticket.blockers
                        if blocker in ticket_issue_numbers
                    ),
                    tickets=spec_tickets,
                ),
                start_to_close_timeout=timedelta(seconds=30),
            )
            if ticket_publication.status is not TicketPublicationStatus.VERIFIED:
                return self._failed(
                    "ticket Issue publication did not complete",
                    planning,
                    {
                        "status": "blocked",
                        "runs": (*scheduler_runs, asdict(ticket_publication)),
                    },
                )
            ticket_issue_numbers.update(
                {record.key: record.number for record in ticket_publication.issues}
            )
            scheduler_run = await workflow.execute_child_workflow(
                "TicketSchedulerWorkflow",
                scheduler_input,
                id=f"{workflow.info().workflow_id}:tickets:{spec_key}",
                result_type=dict,
            )
            scheduler_runs.append(
                {**scheduler_run, "ticket_publication": asdict(ticket_publication)}
            )
            if (
                scheduler_run.get("status") != "completed"
                or tuple(scheduler_run.get("completed_specs", ())) != (spec_key,)
            ):
                scheduler = {"status": "failed", "runs": scheduler_runs}
                return self._failed("ticket scheduling did not complete", planning, scheduler)

            self._phase = WholeFlowPhase.CODEX
            plan = codex_by_spec[spec_key]
            result = await workflow.execute_child_workflow(
                "CodexRunWorkflow",
                plan.to_run_input(),
                id=f"{workflow.info().workflow_id}:codex:{plan.spec_key}",
                result_type=dict,
            )
            codex_results.append(result)
            if result.get("status") != "completed":
                scheduler = {"status": "completed", "runs": scheduler_runs}
                return self._failed("Codex implementation or review did not complete", planning, scheduler, tuple(codex_results))

            self._phase = WholeFlowPhase.DELIVERY
            plan = delivery_by_spec[spec_key]
            result = await workflow.execute_child_workflow(
                "DeliveryWorkflow",
                plan.delivery,
                id=f"{workflow.info().workflow_id}:delivery:{plan.spec_key}",
                result_type=dict,
            )
            delivery_results.append(result)
            if result.get("status") != "completed":
                scheduler = {"status": "completed", "runs": scheduler_runs}
                return self._failed("SPEC delivery did not complete", planning, scheduler, tuple(codex_results), tuple(delivery_results))
            self._completed_specs.append(spec_key)

        scheduler = {
            "status": "completed",
            "completed_specs": expected_specs,
            "completed_tickets": tuple(
                ticket.key for ticket in input.scheduler.tickets
            ),
            "runs": tuple(scheduler_runs),
        }

        self._phase = WholeFlowPhase.SUMMARY
        summary = await workflow.execute_child_workflow(
            "DeliverySummaryWorkflow",
            input.summary,
            id=f"{workflow.info().workflow_id}:summary",
            result_type=dict,
        )
        if summary.get("status") != "verified":
            return self._failed("final summary was not verified", planning, scheduler, tuple(codex_results), tuple(delivery_results), summary)
        self._phase = WholeFlowPhase.COMPLETED
        self._status = WholeFlowStatus.COMPLETED
        return WholeFlowResult(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            planning=planning,
            scheduler=scheduler,
            codex_results=tuple(codex_results),
            delivery_results=tuple(delivery_results),
            summary=summary,
        )

    def _blocked(self, reason: str) -> WholeFlowResult:
        self._phase = WholeFlowPhase.BLOCKED
        self._status = WholeFlowStatus.BLOCKED
        self._reason = reason
        return WholeFlowResult(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            reason=reason,
        )

    def _failed(
        self,
        reason: str,
        planning=None,
        scheduler=None,
        codex_results=(),
        delivery_results=(),
        summary=None,
    ) -> WholeFlowResult:
        self._phase = WholeFlowPhase.FAILED
        self._status = WholeFlowStatus.FAILED
        self._reason = reason
        return WholeFlowResult(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            planning=planning,
            scheduler=scheduler,
            codex_results=tuple(codex_results),
            delivery_results=tuple(delivery_results),
            summary=summary,
            reason=reason,
        )
