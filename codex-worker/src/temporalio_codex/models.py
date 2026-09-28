from dataclasses import dataclass, field
from enum import StrEnum

from temporalio_codex.codex_models import CodexRole


class StageOutcome(StrEnum):
    COMPLETED = "completed"
    WAITING_FOR_INPUT = "waiting_for_input"
    FAILED = "failed"
    UNKNOWN = "unknown"
    CANCELLED = "cancelled"


class RunStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    WAITING_FOR_INPUT = "waiting_for_input"
    WAITING_FOR_EXTERNAL_OBSERVATION = "waiting_for_external_observation"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class StageDefinition:
    key: str
    requires_input: bool = False
    start_to_close_timeout_seconds: float = 30
    role: CodexRole | None = None
    repository: str = "."
    allowed_scope: tuple[str, ...] = ("read_only",)
    approval_policy: str = "deny_all"
    model: str = "gpt-5-codex"
    effort: str = "medium"
    thread_id: str | None = None


@dataclass(frozen=True)
class RunInput:
    requirement: str
    stages: tuple[StageDefinition, ...] = field(
        default_factory=lambda: (StageDefinition(key="foundation"),)
    )


@dataclass(frozen=True)
class StageInput:
    stage: str
    requirement: str
    answer: str | None = None


@dataclass(frozen=True)
class StageResult:
    stage: str
    outcome: StageOutcome
    summary: str
    evidence_refs: tuple[str, ...] = ()
    pending_input: str | None = None
    role: str | None = None
    operation_id: str | None = None
    thread_id: str | None = None
    turn_id: str | None = None
    failure: str | None = None


@dataclass(frozen=True)
class RunSnapshot:
    workflow_id: str
    status: RunStatus
    current_stage: str | None
    completed_stages: tuple[str, ...]
    pending_input: str | None
    stage_results: tuple[StageResult, ...]


@dataclass(frozen=True)
class RunResult:
    workflow_id: str
    status: RunStatus
    outcome: StageOutcome
    stage: str
    summary: str
