from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.delivery_workflows import DeliveryWorkflow
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
        self._phase = WholeFlowPhase.PLANNING
        planning = await workflow.execute_child_workflow(
            "RequirementPlanningWorkflow",
            input.planning.to_input(),
            id=f"{workflow.info().workflow_id}:planning",
            result_type=dict,
        )
        if planning.get("status") != "completed":
            return self._failed("planning did not complete", planning=planning)

        self._phase = WholeFlowPhase.TICKETS
        scheduler = await workflow.execute_child_workflow(
            "TicketSchedulerWorkflow",
            input.scheduler,
            id=f"{workflow.info().workflow_id}:tickets",
            result_type=dict,
        )
        if scheduler.get("status") != "completed":
            return self._failed("ticket scheduling did not complete", planning, scheduler)

        expected_specs = tuple(spec.key for spec in input.scheduler.specs)
        if tuple(scheduler.get("completed_specs", ())) != expected_specs:
            return self._failed("ticket scheduler completed an unexpected SPEC graph", planning, scheduler)

        self._phase = WholeFlowPhase.CODEX
        codex_results: list[dict] = []
        for plan in input.codex:
            result = await workflow.execute_child_workflow(
                "CodexRunWorkflow",
                plan.to_run_input(),
                id=f"{workflow.info().workflow_id}:codex:{plan.spec_key}",
                result_type=dict,
            )
            codex_results.append(result)
            if result.get("status") != "completed":
                return self._failed("Codex implementation or review did not complete", planning, scheduler, tuple(codex_results))

        self._phase = WholeFlowPhase.DELIVERY
        delivery_results: list[dict] = []
        for plan in input.deliveries:
            if plan.spec_key not in expected_specs:
                return self._failed(f"delivery references unknown SPEC {plan.spec_key}", planning, scheduler, tuple(codex_results))
            result = await workflow.execute_child_workflow(
                "DeliveryWorkflow",
                plan.delivery,
                id=f"{workflow.info().workflow_id}:delivery:{plan.spec_key}",
                result_type=dict,
            )
            delivery_results.append(result)
            if result.get("status") != "completed":
                return self._failed("SPEC delivery did not complete", planning, scheduler, tuple(codex_results), tuple(delivery_results))
            self._completed_specs.append(plan.spec_key)

        if tuple(self._completed_specs) != expected_specs:
            return self._failed("SPEC deliveries were not complete in dependency order", planning, scheduler, tuple(codex_results), tuple(delivery_results))

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
