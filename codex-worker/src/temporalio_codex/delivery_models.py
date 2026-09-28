from dataclasses import dataclass, field
from enum import StrEnum


class DeliveryPhase(StrEnum):
    CANDIDATE = "candidate"
    ACCEPTANCE = "acceptance"
    REVIEW = "review"
    CI = "ci"
    PULL_REQUEST = "pull_request"
    MERGE = "merge"
    CLEANUP = "cleanup"


class DeliveryOutcome(StrEnum):
    COMPLETED = "completed"
    WAITING = "waiting"
    FAILED = "failed"
    UNKNOWN = "unknown"
    NOT_VERIFIED = "not_verified"


@dataclass(frozen=True)
class DeliveryOperation:
    operation_id: str
    run_id: str
    phase: DeliveryPhase
    repository: str
    workspace: str | None = None
    target_branch: str = "main"
    base_sha: str | None = None
    candidate_sha: str | None = None
    acceptance_version: str | None = None
    pull_request_identity: str | None = None
    pull_request_number: int | None = None
    issue_numbers: tuple[int, ...] = ()


@dataclass(frozen=True)
class DeliveryReceipt:
    operation_id: str
    phase: DeliveryPhase
    outcome: DeliveryOutcome
    summary: str
    candidate_sha: str | None = None
    acceptance_version: str | None = None
    pull_request_number: int | None = None
    merged_sha: str | None = None
    readback_required: bool = False
    evidence_refs: tuple[str, ...] = field(default_factory=tuple)


def validate_operation(operation: DeliveryOperation) -> None:
    if not operation.operation_id.strip():
        raise ValueError("operation_id must not be empty")
    if not operation.run_id.strip():
        raise ValueError("run_id must not be empty")
    if not operation.repository.strip():
        raise ValueError("repository must not be empty")
    if operation.phase is DeliveryPhase.CANDIDATE and not operation.workspace:
        raise ValueError("candidate operations require a workspace")
    if operation.phase in {
        DeliveryPhase.ACCEPTANCE,
        DeliveryPhase.REVIEW,
        DeliveryPhase.CI,
    } and not operation.candidate_sha:
        raise ValueError("verification operations require candidate_sha")
    if operation.phase is DeliveryPhase.MERGE and not operation.pull_request_number:
        raise ValueError("merge operations require pull_request_number")
