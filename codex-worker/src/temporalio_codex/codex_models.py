from dataclasses import dataclass
from enum import StrEnum


class CodexRole(StrEnum):
    PLANNING = "planning"
    IMPLEMENTATION = "implementation"
    REVIEW = "review"


class CodexOutcome(StrEnum):
    COMPLETED = "completed"
    PENDING_INPUT = "pending_input"
    FAILED = "failed"
    UNKNOWN = "unknown"


class CodexFailure(StrEnum):
    REJECTED = "rejected"
    TIMEOUT = "timeout"
    STREAM_DISCONNECTED = "stream_disconnected"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CodexOperation:
    operation_id: str
    run_id: str
    stage: str
    role: CodexRole
    repository: str
    allowed_scope: tuple[str, ...]
    approval_policy: str
    model: str
    effort: str
    prompt: str
    thread_id: str | None = None
    parent_workflow_id: str | None = None
    workflow_run_id: str | None = None
    parent_workflow_run_id: str | None = None
    namespace: str = "default"
    output_schema: dict | None = None


@dataclass(frozen=True)
class CodexCapabilities:
    sdk_version: str
    can_interrupt_owned_turn: bool
    can_resume_thread: bool
    can_interrupt_historical_turn: bool = False


@dataclass(frozen=True)
class CodexObservation:
    operation_id: str
    role: CodexRole
    outcome: CodexOutcome
    thread_id: str | None = None
    turn_id: str | None = None
    summary: str = ""
    failure: CodexFailure | None = None
    evidence_refs: tuple[str, ...] = ()
    pending_question: str | None = None
    readback_required: bool = False
    capabilities: CodexCapabilities | None = None


def validate_operation(operation: CodexOperation) -> None:
    if not operation.namespace.strip():
        raise ValueError("namespace must not be empty")
    if not operation.operation_id.strip():
        raise ValueError("operation_id must not be empty")
    if not operation.run_id.strip():
        raise ValueError("run_id must not be empty")
    if not operation.stage.strip():
        raise ValueError("stage must not be empty")
    if not operation.repository.strip():
        raise ValueError("repository must not be empty")
    if not operation.allowed_scope:
        raise ValueError("allowed_scope must not be empty")
    if not operation.prompt.strip():
        raise ValueError("prompt must not be empty")
