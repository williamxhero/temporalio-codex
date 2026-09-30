from dataclasses import dataclass, field
from enum import StrEnum

from temporalio_codex.models import RunInput


class SchedulerStatus(StrEnum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    COMPLETED = "completed"


@dataclass(frozen=True)
class SpecPlan:
    key: str
    dependencies: tuple[str, ...] = ()


@dataclass(frozen=True)
class TicketPlan:
    key: str
    spec_key: str
    blockers: tuple[str, ...] = ()
    title: str = ""
    acceptance_criteria: tuple[str, ...] = ()


@dataclass(frozen=True)
class SchedulerInput:
    specs: tuple[SpecPlan, ...]
    tickets: tuple[TicketPlan, ...]
    completion_operations: tuple[tuple[str, str], ...] = ()
    # Optional Codex inputs keyed by ticket. When present, ready tickets are
    # executed by the scheduler and completed from the child result.
    codex_runs: tuple[tuple[str, RunInput], ...] = ()
    parent_workflow_id: str | None = None
    parent_workflow_run_id: str | None = None
    automatic: bool = False


@dataclass(frozen=True)
class SchedulerSnapshot:
    workflow_id: str
    status: SchedulerStatus
    active_spec: str | None
    ready_frontier: tuple[str, ...]
    completed_specs: tuple[str, ...]
    completed_tickets: tuple[str, ...]
    completion_operations: tuple[tuple[str, str], ...] = ()
    reason: str = ""


@dataclass(frozen=True)
class SchedulerResult:
    workflow_id: str
    status: SchedulerStatus
    completed_specs: tuple[str, ...]
    completed_tickets: tuple[str, ...]
    reason: str = ""
    codex_results: tuple[dict, ...] = ()


def validate_scheduler_graph(input: SchedulerInput) -> tuple[str, ...]:
    errors: list[str] = []
    spec_keys = [spec.key for spec in input.specs]
    ticket_keys = [ticket.key for ticket in input.tickets]
    completion_keys = [key for key, _ in input.completion_operations]
    if not input.specs:
        errors.append("at least one SPEC is required")
    for spec in input.specs:
        if not any(ticket.spec_key == spec.key for ticket in input.tickets):
            errors.append(f"SPEC {spec.key} requires at least one ticket")
    if len(set(spec_keys)) != len(spec_keys):
        errors.append("SPEC keys must be unique")
    if len(set(ticket_keys)) != len(ticket_keys):
        errors.append("ticket keys must be unique")
    if len(set(completion_keys)) != len(completion_keys):
        errors.append("completion ticket keys must be unique")
    if set(completion_keys) - set(ticket_keys):
        errors.append("completion operations reference unknown tickets")
    if any(not key.strip() or not operation.strip() for key, operation in input.completion_operations):
        errors.append("completion operations require ticket and operation identities")
    known_specs = set(spec_keys)
    known_tickets = set(ticket_keys)
    for spec in input.specs:
        missing = sorted(set(spec.dependencies) - known_specs)
        if missing:
            errors.append(f"SPEC {spec.key} has unresolved dependencies: {', '.join(missing)}")
    for ticket in input.tickets:
        if ticket.spec_key not in known_specs:
            errors.append(f"ticket {ticket.key} references unknown SPEC {ticket.spec_key}")
        missing = sorted(set(ticket.blockers) - known_tickets)
        if missing:
            errors.append(f"ticket {ticket.key} has unresolved blockers: {', '.join(missing)}")
    errors.extend(
        _cycle_errors({spec.key: set(spec.dependencies) for spec in input.specs}, "SPEC")
    )
    errors.extend(
        _cycle_errors(
            {ticket.key: set(ticket.blockers) for ticket in input.tickets}, "ticket"
        )
    )
    return tuple(dict.fromkeys(errors))


def _cycle_errors(graph: dict[str, set[str]], label: str) -> list[str]:
    errors: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str) -> None:
        if key in visiting:
            errors.append(f"{label} dependency cycle includes {key}")
            return
        if key in visited:
            return
        visiting.add(key)
        for dependency in graph.get(key, ()):
            if dependency in graph:
                visit(dependency)
        visiting.remove(key)
        visited.add(key)

    for key in graph:
        visit(key)
    return errors


def ready_frontier(
    input: SchedulerInput,
    active_spec: str,
    completed_tickets: set[str],
) -> tuple[str, ...]:
    return tuple(
        sorted(
            ticket.key
            for ticket in input.tickets
            if ticket.spec_key == active_spec
            and ticket.key not in completed_tickets
            and set(ticket.blockers).issubset(completed_tickets)
        )
    )


@dataclass
class InMemorySchedulerState:
    completed_tickets: set[str] = field(default_factory=set)
    completion_operations: dict[str, str] = field(default_factory=dict)
