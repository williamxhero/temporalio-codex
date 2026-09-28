from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.ticket_scheduler import (
        SchedulerInput,
        SchedulerResult,
        SchedulerSnapshot,
        SchedulerStatus,
        TicketPlan,
        validate_scheduler_graph,
        ready_frontier,
    )


@workflow.defn
class TicketSchedulerWorkflow:
    def __init__(self) -> None:
        self._input: SchedulerInput | None = None
        self._status = SchedulerStatus.ACTIVE
        self._completed_tickets: set[str] = set()
        self._completion_operations: dict[str, str] = {}
        self._reason = ""

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
        while len(self._completed_tickets) < len(input.tickets):
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
            await workflow.wait_condition(
                lambda: bool(
                    set(self._completed_tickets).intersection(frontier)
                )
                or self._active_spec() != active_spec
            )
        self._status = SchedulerStatus.COMPLETED
        return self._result()

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
        )
