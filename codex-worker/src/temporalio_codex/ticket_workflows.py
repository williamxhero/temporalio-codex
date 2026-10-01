from dataclasses import asdict, replace

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.execution_status import ExecutionProgress, report_progress
    from temporalio_codex.models import RunInput
    from temporalio_codex.delivery_models import CandidateEvidence
    from temporalio_codex.workflows import CodexRunWorkflow
    from temporalio_codex.ticket_scheduler import (
        SchedulerInput,
        SchedulerResult,
        SchedulerSnapshot,
        SchedulerStatus,
        TicketPlan,
        ready_frontier,
        validate_scheduler_graph,
    )


@workflow.defn
class TicketSchedulerWorkflow:
    def __init__(self) -> None:
        self._input: SchedulerInput | None = None
        self._status = SchedulerStatus.ACTIVE
        self._completed_tickets: set[str] = set()
        self._completion_operations: dict[str, str] = {}
        self._codex_results: list[dict] = []
        self._reason = ""
        self._active_child_id: str | None = None
        self._active_child_run_id: str | None = None
        self._active_ticket: str | None = None
        self._configuration_error = ""

    @workflow.signal(name="configure_codex_runs")
    def configure_codex_runs(
        self, codex_runs: tuple[tuple[str, RunInput], ...]
    ) -> None:
        if self._input is None or self._input.automatic:
            return
        active_spec = self._active_spec()
        frontier = (
            ready_frontier(self._input, active_spec, self._completed_tickets)
            if active_spec is not None
            else ()
        )
        configured_keys = tuple(key for key, _ in codex_runs)
        completed_operations = dict(self._input.completion_operations)
        expected_keys = {
            ticket.key
            for ticket in self._input.tickets
            if ticket.key not in self._completed_tickets
            and ticket.key not in completed_operations
        }
        parent = workflow.info().parent
        if (
            not frontier
            or set(configured_keys) != expected_keys
            or len(configured_keys) != len(set(configured_keys))
            or parent is None
            or any(
                not run_input.automatic
                or run_input.candidate is None
                or run_input.parent_workflow_id != parent.workflow_id
                or run_input.parent_workflow_run_id != parent.run_id
                for _, run_input in codex_runs
            )
        ):
            self._configuration_error = "Codex ticket configuration failed validation"
            self._status = SchedulerStatus.BLOCKED
            self._reason = self._configuration_error
            return
        self._input = replace(self._input, automatic=True, codex_runs=codex_runs)

    @workflow.signal(name="execution_progress")
    async def execution_progress(self, progress: ExecutionProgress) -> None:
        if (progress.workflow_id != self._active_child_id
                or progress.workflow_run_id != self._active_child_run_id):
            return
        info = workflow.info()
        if info.parent:
            await workflow.get_external_workflow_handle(
                info.parent.workflow_id, run_id=info.parent.run_id
            ).signal("execution_progress", replace(
                progress, workflow_id=info.workflow_id,
                workflow_run_id=info.run_id, active_ticket=self._active_ticket,
            ))

    @workflow.query(name="get_scheduler_status")
    def get_status(self) -> SchedulerSnapshot:
        active_spec = self._active_spec()
        return SchedulerSnapshot(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            active_spec=active_spec,
            ready_frontier=(
                ready_frontier(self._input, active_spec, self._completed_tickets)
                if self._input is not None and active_spec is not None
                else ()
            ),
            completed_specs=self._completed_specs(),
            completed_tickets=tuple(sorted(self._completed_tickets)),
            completion_operations=tuple(sorted(self._completion_operations.items())),
            reason=self._reason,
        )

    @workflow.run
    async def run(self, input: SchedulerInput) -> SchedulerResult:
        self._input = input
        errors = validate_scheduler_graph(input)
        if errors:
            self._status = SchedulerStatus.BLOCKED
            self._reason = "; ".join(errors)
            return self._result()
        completion_operations = dict(input.completion_operations)
        while len(self._completed_tickets) < len(input.tickets):
            checkpoint = workflow.get_signal_handler("component_checkpoint")
            if checkpoint is not None:
                await checkpoint()
            input = self._input
            assert input is not None
            if self._configuration_error:
                return self._result()
            active_spec = self._active_spec()
            if active_spec is None:
                self._status = SchedulerStatus.BLOCKED
                self._reason = "no ready SPEC or ticket frontier remains"
                return self._result()
            frontier = ready_frontier(input, active_spec, self._completed_tickets)
            if not frontier:
                self._status = SchedulerStatus.BLOCKED
                self._reason = f"SPEC {active_spec} has no ready ticket frontier"
                return self._result()
            automatic = [key for key in frontier if key in completion_operations]
            if automatic:
                for ticket_key in automatic:
                    self._complete(ticket_key, completion_operations[ticket_key])
                continue
            codex_runs = dict(input.codex_runs)
            automatic_codex = [key for key in frontier if key in codex_runs]
            if automatic_codex:
                for ticket_key in automatic_codex:
                    self._active_ticket = ticket_key
                    self._active_child_id = f"{workflow.info().workflow_id}:codex:{ticket_key}"
                    await report_progress(phase="codex", active_ticket=ticket_key,
                                          next_action="start ticket Codex execution")
                    if input.inline_codex:
                        run_input = replace(codex_runs[ticket_key], operation_prefix=f"ticket:{ticket_key}:")
                        if self._codex_results and self._codex_results[-1].get("candidate"):
                            run_input = replace(run_input, candidate=CandidateEvidence(
                                **self._codex_results[-1]["candidate"]
                            ))
                        result = asdict(await CodexRunWorkflow().run(run_input))
                    else:
                        handle = await workflow.start_child_workflow(
                            "CodexRunWorkflow",
                            codex_runs[ticket_key],
                            id=f"{workflow.info().workflow_id}:codex:{ticket_key}",
                            result_type=dict,
                        )
                        self._active_child_run_id = handle.first_execution_run_id
                        try:
                            result = await handle
                        finally:
                            self._active_child_id = None
                            self._active_child_run_id = None
                    self._active_ticket = None
                    self._codex_results.append(result)
                    if result.get("status") != "completed":
                        self._status = SchedulerStatus.BLOCKED
                        self._reason = (
                            f"Codex execution failed for ticket {ticket_key}: "
                            f"{result.get('summary', 'unknown failure')}"
                        )
                        return self._result()
                    self._complete(
                        ticket_key,
                        f"{workflow.info().workflow_id}:ticket:{ticket_key}" if input.inline_codex
                        else f"{workflow.info().workflow_id}:codex:{ticket_key}",
                    )
                continue
            if input.automatic:
                self._status = SchedulerStatus.BLOCKED
                self._reason = (
                    "ready ticket has no automatic Codex plan or completion operation"
                )
                return self._result()
            await report_progress(phase="tickets", status="durable_waiting",
                                  pending_reason="waiting for ticket completion",
                                  next_action="observe durable ticket completion")
            await workflow.wait_condition(
                lambda: bool(
                    set(self._completed_tickets).intersection(frontier)
                )
                or self._active_spec() != active_spec
                or self._configuration_error
                or bool(
                    set(dict(self._input.codex_runs)).intersection(frontier)
                )
            )
        self._status = SchedulerStatus.COMPLETED
        return self._result()

    def _complete(self, ticket_key: str, operation_id: str) -> None:
        self._completed_tickets.add(ticket_key)
        self._completion_operations[ticket_key] = operation_id

    @workflow.update(name="complete_ticket")
    async def complete_ticket(self, ticket_key: str, operation_id: str) -> bool:
        if self._input is None or not ticket_key.strip() or not operation_id.strip():
            return False
        if ticket_key in self._completed_tickets:
            return self._completion_operations.get(ticket_key) == operation_id
        active_spec = self._active_spec()
        if active_spec is None:
            return False
        frontier = ready_frontier(self._input, active_spec, self._completed_tickets)
        if ticket_key not in frontier:
            return False
        self._completed_tickets.add(ticket_key)
        self._completion_operations[ticket_key] = operation_id
        return True

    def _active_spec(self) -> str | None:
        if self._input is None:
            return None
        specs_with_tickets = {ticket.spec_key for ticket in self._input.tickets}
        completed_specs = set(self._completed_specs())
        for spec in self._input.specs:
            if spec.key in specs_with_tickets and spec.key not in completed_specs:
                if set(spec.dependencies).issubset(completed_specs):
                    return spec.key
                continue
        return None

    def _completed_specs(self) -> tuple[str, ...]:
        if self._input is None:
            return ()
        complete = {
            spec.key
            for spec in self._input.specs
            if any(ticket.spec_key == spec.key for ticket in self._input.tickets)
            and all(
                ticket.key in self._completed_tickets
                for ticket in self._input.tickets
                if ticket.spec_key == spec.key
            )
        }
        return tuple(spec.key for spec in self._input.specs if spec.key in complete)

    def _result(self) -> SchedulerResult:
        return SchedulerResult(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            completed_specs=self._completed_specs(),
            completed_tickets=tuple(sorted(self._completed_tickets)),
            reason=self._reason,
            codex_results=tuple(self._codex_results),
        )
