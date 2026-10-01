from dataclasses import dataclass, field
from enum import StrEnum

from temporalio_codex.codex_models import CodexRole
from temporalio_codex.delivery_models import CandidateEvidence, ReviewEvidence


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
    external_recheck_seconds: float = 0


@dataclass(frozen=True)
class RunInput:
    requirement: str
    stages: tuple[StageDefinition, ...] = field(
        default_factory=lambda: (StageDefinition(key="foundation"),)
    )
    parent_workflow_id: str | None = None
    parent_workflow_run_id: str | None = None
    automatic: bool = False
    automatic_input_max_attempts: int = 1
    candidate: CandidateEvidence | None = None
    operation_prefix: str = ""


@dataclass(frozen=True)
class StageInput:
    stage: str
    requirement: str
    answer: str | None = None


@dataclass(frozen=True)
class HeartbeatInput:
    operation_id: str
    stage: str
    progress: str

    def __post_init__(self) -> None:
        if not self.operation_id.strip() or not self.stage.strip():
            raise ValueError("heartbeat identity must not be empty")
        if len(self.progress) > 500:
            raise ValueError("heartbeat progress is too long")


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
    external_recheck_count: int = 0
    pending_reason: str = ""
    next_action: str = ""
    retry_count: int = 0
    deadline: str | None = None
    timeout_seconds: float | None = None
    last_error: str | None = None
    workflow_run_id: str = ""


@dataclass(frozen=True)
class RunResult:
    workflow_id: str
    status: RunStatus
    outcome: StageOutcome
    stage: str
    summary: str
    candidate: CandidateEvidence | None = None
    review_evidence: ReviewEvidence | None = None
