from dataclasses import dataclass
from enum import StrEnum

from temporalio_codex.codex_models import CodexRole
from temporalio_codex.models import RunInput, StageDefinition
from temporalio_codex.delivery_models import DeliveryInput
from temporalio_codex.planning_models import PlanningInput
from temporalio_codex.planning_models import GrillAnswer, SourceOrigin
from temporalio_codex.spec_issue_adapter import SpecDraft
from temporalio_codex.summary_adapter import SummaryPublicationInput
from temporalio_codex.ticket_scheduler import SchedulerInput, validate_scheduler_graph


class WholeFlowPhase(StrEnum):
    PLANNING = "planning"
    TICKETS = "tickets"
    CODEX = "codex"
    DELIVERY = "delivery"
    SUMMARY = "summary"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"


class WholeFlowStatus(StrEnum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    FAILED = "failed"
    NOT_VERIFIED = "not_verified"
    COMPLETED = "completed"


@dataclass(frozen=True)
class SpecCodexPlan:
    spec_key: str
    requirement: str
    repository: str
    allowed_scope: tuple[str, ...]
    approval_policy: str = "deny_all"
    model: str = "gpt-5-codex"
    effort: str = "low"

    def to_run_input(self) -> RunInput:
        stages = tuple(
            StageDefinition(
                key=role.value,
                role=role,
                repository=self.repository,
                allowed_scope=self.allowed_scope,
                approval_policy=self.approval_policy,
                model=self.model,
                effort=self.effort,
            )
            for role in (CodexRole.PLANNING, CodexRole.IMPLEMENTATION, CodexRole.REVIEW)
        )
        return RunInput(requirement=self.requirement, stages=stages)


@dataclass(frozen=True)
class PlanningPayload:
    origin: SourceOrigin
    source_text: str | None = None
    source_reference: str | None = None
    sensitive: bool = False
    umbrella_issue_number: int = 1
    specs: tuple[SpecDraft, ...] = ()
    grill_answers: tuple[GrillAnswer, ...] = ()
    confirmation_operation_id: str | None = None
    publication_operation_id: str | None = None

    def to_input(self) -> PlanningInput:
        return PlanningInput(
            origin=self.origin,
            source_text=self.source_text,
            source_reference=self.source_reference,
            sensitive=self.sensitive,
            umbrella_issue_number=self.umbrella_issue_number,
            specs=self.specs,
            grill_answers=self.grill_answers,
            confirmation_operation_id=self.confirmation_operation_id,
            publication_operation_id=self.publication_operation_id,
        )


@dataclass(frozen=True)
class SpecDeliveryPlan:
    spec_key: str
    delivery: DeliveryInput


@dataclass(frozen=True)
class WholeFlowInput:
    planning: PlanningPayload
    scheduler: SchedulerInput
    codex: tuple[SpecCodexPlan, ...]
    deliveries: tuple[SpecDeliveryPlan, ...]
    summary: SummaryPublicationInput


@dataclass(frozen=True)
class WholeFlowSnapshot:
    workflow_id: str
    phase: WholeFlowPhase
    status: WholeFlowStatus
    completed_specs: tuple[str, ...] = ()
    reason: str = ""


@dataclass(frozen=True)
class WholeFlowResult:
    workflow_id: str
    phase: WholeFlowPhase
    status: WholeFlowStatus
    planning: dict | None = None
    scheduler: dict | None = None
    codex_results: tuple[dict, ...] = ()
    delivery_results: tuple[dict, ...] = ()
    summary: dict | None = None
    reason: str = ""


def topological_spec_keys(input: SchedulerInput) -> tuple[str, ...]:
    dependencies = {spec.key: set(spec.dependencies) for spec in input.specs}
    remaining = set(dependencies)
    ordered: list[str] = []
    while remaining:
        ready = sorted(
            key
            for key in remaining
            if dependencies[key].isdisjoint(remaining)
        )
        if not ready:
            return ()
        ordered.extend(ready)
        remaining.difference_update(ready)
    return tuple(ordered)


def validate_whole_flow_input(input: WholeFlowInput) -> tuple[str, ...]:
    errors = list(validate_scheduler_graph(input.scheduler))
    if errors:
        return tuple(dict.fromkeys(errors))

    expected_specs = topological_spec_keys(input.scheduler)
    planning_keys = tuple(draft.key for draft in input.planning.specs)
    codex_keys = tuple(plan.spec_key for plan in input.codex)
    delivery_keys = tuple(plan.spec_key for plan in input.deliveries)
    expected_set = set(expected_specs)
    draft_by_key = {draft.key: draft for draft in input.planning.specs}
    scheduler_dependencies = {
        spec.key: set(spec.dependencies) for spec in input.scheduler.specs
    }

    if set(planning_keys) != expected_set or len(planning_keys) != len(expected_specs):
        errors.append("planning SPECs do not match the scheduler graph")
    elif any(
        set(draft_by_key[key].dependencies) != scheduler_dependencies[key]
        for key in expected_specs
    ):
        errors.append("planning dependencies do not match the scheduler graph")
    if set(codex_keys) != expected_set or len(codex_keys) != len(expected_specs):
        errors.append("Codex plans do not cover each SPEC exactly once")
    if set(delivery_keys) != expected_set or len(delivery_keys) != len(expected_specs):
        errors.append("delivery plans do not cover each SPEC exactly once")
    if codex_keys != expected_specs:
        errors.append("Codex plans must follow dependency order")
    if delivery_keys != expected_specs:
        errors.append("delivery plans must follow dependency order")
    spec_order = {key: index for index, key in enumerate(expected_specs)}
    dependency_closure: dict[str, set[str]] = {}
    for key in expected_specs:
        dependencies = set(scheduler_dependencies[key])
        for dependency in tuple(dependencies):
            dependencies.update(dependency_closure.get(dependency, set()))
        dependency_closure[key] = dependencies
    ticket_by_key = {ticket.key: ticket for ticket in input.scheduler.tickets}
    for ticket in input.scheduler.tickets:
        for blocker_key in ticket.blockers:
            blocker = ticket_by_key.get(blocker_key)
            if blocker is None or blocker.spec_key == ticket.spec_key:
                continue
            if (
                spec_order.get(blocker.spec_key, -1)
                >= spec_order.get(ticket.spec_key, 0)
                or blocker.spec_key not in dependency_closure[ticket.spec_key]
            ):
                errors.append(
                    f"ticket {ticket.key} has a blocker in a later or unrelated SPEC"
                )
    return tuple(dict.fromkeys(errors))
