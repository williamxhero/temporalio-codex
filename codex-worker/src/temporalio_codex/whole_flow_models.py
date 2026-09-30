import math
from dataclasses import dataclass
from enum import StrEnum

from temporalio_codex.codex_models import CodexRole
from temporalio_codex.delivery_models import DeliveryInput
from temporalio_codex.models import RunInput, StageDefinition
from temporalio_codex.planning_models import GrillAnswer, PlanningInput, SourceOrigin
from temporalio_codex.spec_issue_adapter import SpecDraft
from temporalio_codex.summary_adapter import SummaryPublicationInput
from temporalio_codex.ticket_scheduler import SchedulerInput, validate_scheduler_graph


class WholeFlowPhase(StrEnum):
    INTAKE = "intake"
    PLANNING = "planning"
    TICKETS = "tickets"
    CODEX = "codex"
    DELIVERY = "delivery"
    SUMMARY = "summary"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WholeFlowStatus(StrEnum):
    ACTIVE = "active"
    DURABLE_WAITING = "durable_waiting"
    RETRYING = "retrying"
    BLOCKED = "blocked"
    FAILED = "failed"
    NOT_VERIFIED = "not_verified"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class SpecCodexPlan:
    spec_key: str
    requirement: str
    repository: str
    allowed_scope: tuple[str, ...]
    approval_policy: str = "deny_all"
    model: str = "gpt-5-codex"
    effort: str = "low"
    start_to_close_timeout_seconds: float = 1800

    def to_run_input(
        self,
        *,
        parent_workflow_id: str | None = None,
        parent_workflow_run_id: str | None = None,
    ) -> RunInput:
        stages = tuple(
            StageDefinition(
                key=role.value,
                role=role,
                repository=self.repository,
                allowed_scope=self.allowed_scope,
                approval_policy=self.approval_policy,
                model=self.model,
                effort=self.effort,
                start_to_close_timeout_seconds=self.start_to_close_timeout_seconds,
            )
            for role in (CodexRole.PLANNING, CodexRole.IMPLEMENTATION, CodexRole.REVIEW)
        )
        return RunInput(
            requirement=self.requirement,
            stages=stages,
            parent_workflow_id=parent_workflow_id,
            parent_workflow_run_id=parent_workflow_run_id,
            automatic=True,
        )


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
    publication_timeout_seconds: float = 300.0
    publication_max_attempts: int = 3
    publication_retry_backoff_seconds: float = 1.0

    def to_input(
        self,
        *,
        repository: str = "williamxhero/temporalio-codex",
        parent_workflow_id: str | None = None,
        parent_workflow_run_id: str | None = None,
    ) -> PlanningInput:
        return PlanningInput(
            origin=self.origin,
            repository=repository,
            source_text=self.source_text,
            source_reference=self.source_reference,
            sensitive=self.sensitive,
            umbrella_issue_number=self.umbrella_issue_number,
            specs=self.specs,
            grill_answers=self.grill_answers,
            confirmation_operation_id=self.confirmation_operation_id,
            publication_operation_id=self.publication_operation_id,
            publication_timeout_seconds=self.publication_timeout_seconds,
            publication_max_attempts=self.publication_max_attempts,
            publication_retry_backoff_seconds=self.publication_retry_backoff_seconds,
            parent_workflow_id=parent_workflow_id,
            parent_workflow_run_id=parent_workflow_run_id,
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
    repository: str = "williamxhero/temporalio-codex"
    entry_contract_version: str | None = None
    entry_launch_key: str | None = None
    entry_input_identity: str | None = None


@dataclass(frozen=True)
class WholeFlowSnapshot:
    workflow_id: str
    phase: WholeFlowPhase
    status: WholeFlowStatus
    completed_specs: tuple[str, ...] = ()
    active_spec: str | None = None
    active_ticket: str | None = None
    next_action: str = ""
    evidence_refs: tuple[str, ...] = ()
    entry_contract_version: str | None = None
    entry_launch_key: str | None = None
    entry_input_identity: str | None = None
    reason: str = ""
    pending_reason: str = ""
    retry_count: int = 0
    deadline: str | None = None
    timeout_seconds: float | None = None
    last_error: str | None = None
    workflow_run_id: str = ""


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
            key for key in remaining if dependencies[key].isdisjoint(remaining)
        )
        if not ready:
            return ()
        ordered.extend(ready)
        remaining.difference_update(ready)
    return tuple(ordered)


def validate_whole_flow_input(input: WholeFlowInput) -> tuple[str, ...]:
    errors = list(validate_scheduler_graph(input.scheduler))
    if any(
        not math.isfinite(plan.start_to_close_timeout_seconds)
        or plan.start_to_close_timeout_seconds <= 0
        for plan in input.codex
    ):
        errors.append("invalid bounded Codex stage timeout")
    policy = input.planning
    if (not isinstance(policy.publication_max_attempts, int)
            or policy.publication_max_attempts < 1
            or policy.publication_max_attempts > 100
            or not math.isfinite(policy.publication_timeout_seconds)
            or policy.publication_timeout_seconds <= 0
            or not math.isfinite(policy.publication_retry_backoff_seconds)
            or policy.publication_retry_backoff_seconds < 0):
        errors.append("invalid bounded publication retry policy")
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


def validate_delivery_evidence(result: dict) -> tuple[str, ...]:
    """Return completion-gate errors for the delivery child result."""
    if result.get("status") != "completed":
        return ("delivery child did not report completed",)
    receipts = result.get("receipts") or ()
    if not receipts:
        return ("delivery completed without receipts",)
    candidate = result.get("candidate") or {}
    review = result.get("review_evidence") or {}
    sha = candidate.get("candidate_sha")
    if (
        not sha
        or review.get("candidate_sha") != sha
        or review.get("verdict") != "approved"
        or not all(review.get(key) for key in ("operation_id", "thread_id", "turn_id"))
    ):
        return ("delivery has no matching candidate and approved SDK review proof",)
    for phase in ("candidate", "acceptance", "review", "publish_candidate"):
        if not any(
            receipt.get("phase") == phase
            and receipt.get("outcome") == "completed"
            and receipt.get("candidate_sha") == sha
            for receipt in receipts
        ):
            return (f"delivery has no verified {phase} receipt for its candidate",)
    push_receipts = [receipt for receipt in receipts if receipt.get("phase") == "push"]
    if not push_receipts:
        return ("delivery completed without push evidence",)
    if not any(
        receipt.get("remote_contains_merge") is True for receipt in push_receipts
    ):
        return ("push evidence does not verify the expected remote merge",)
    return ()
