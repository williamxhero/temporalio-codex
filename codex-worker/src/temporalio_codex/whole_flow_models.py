from dataclasses import dataclass
from enum import StrEnum

from temporalio_codex.codex_models import CodexRole
from temporalio_codex.models import RunInput, StageDefinition
from temporalio_codex.delivery_models import DeliveryInput
from temporalio_codex.planning_models import PlanningInput
from temporalio_codex.planning_models import GrillAnswer, SourceOrigin
from temporalio_codex.spec_issue_adapter import SpecDraft
from temporalio_codex.summary_adapter import SummaryPublicationInput
from temporalio_codex.ticket_scheduler import SchedulerInput


class WholeFlowPhase(StrEnum):
    PLANNING = "planning"
    TICKETS = "tickets"
    CODEX = "codex"
    DELIVERY = "delivery"
    SUMMARY = "summary"
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
