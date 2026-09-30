from dataclasses import dataclass, field
from enum import StrEnum


class DeliveryPhase(StrEnum):
    CANDIDATE = "candidate"
    PUBLISH_CANDIDATE = "publish_candidate"
    ACCEPTANCE = "acceptance"
    REVIEW = "review"
    CI = "ci"
    PULL_REQUEST = "pull_request"
    MERGE = "merge"
    PUSH = "push"
    CLEANUP = "cleanup"


class DeliveryOutcome(StrEnum):
    COMPLETED = "completed"
    WAITING = "waiting"
    FAILED = "failed"
    UNKNOWN = "unknown"
    NOT_VERIFIED = "not_verified"


class DeliveryStatus(StrEnum):
    ACTIVE = "active"
    WAITING_FOR_READBACK = "waiting_for_readback"
    CLEANUP_PENDING = "cleanup_pending"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class CandidateEvidence:
    repository: str
    workspace: str
    base_sha: str
    candidate_sha: str


@dataclass(frozen=True)
class ReviewEvidence:
    candidate_sha: str
    verdict: str
    operation_id: str
    thread_id: str
    turn_id: str


@dataclass(frozen=True)
class DeliveryOperation:
    operation_id: str
    run_id: str
    phase: DeliveryPhase
    repository: str
    workspace: str | None = None
    target_branch: str = "main"
    candidate_branch: str | None = None
    title: str | None = None
    body: str | None = None
    acceptance_command: tuple[str, ...] = ()
    base_sha: str | None = None
    candidate_sha: str | None = None
    merge_commit_sha: str | None = None
    acceptance_version: str | None = None
    pull_request_identity: str | None = None
    pull_request_number: int | None = None
    issue_numbers: tuple[int, ...] = ()
    review_evidence: ReviewEvidence | None = None


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
    remote_sha: str | None = None
    remote_contains_merge: bool | None = None
    external_id: str | None = None
    external_state: str | None = None
    readback_required: bool = False
    evidence_refs: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class DeliveryInput:
    repository: str
    workspace: str
    base_sha: str
    target_branch: str = "main"
    candidate_branch: str = ""
    acceptance_version: str = "v1"
    acceptance_command: tuple[str, ...] = ()
    pull_request_identity: str = ""
    title: str = ""
    body: str = ""
    issue_numbers: tuple[int, ...] = ()
    automatic: bool = False
    readback_max_attempts: int = 60
    readback_backoff_seconds: float = 5
    candidate: CandidateEvidence | None = None
    review_evidence: ReviewEvidence | None = None
    acceptance_timeout_seconds: float = 1800


@dataclass(frozen=True)
class DeliverySnapshot:
    workflow_id: str
    status: DeliveryStatus
    phase: DeliveryPhase | None
    receipts: tuple[DeliveryReceipt, ...]


@dataclass(frozen=True)
class DeliveryResult:
    workflow_id: str
    status: DeliveryStatus
    outcome: DeliveryOutcome
    summary: str
    receipts: tuple[DeliveryReceipt, ...] = ()
    candidate: CandidateEvidence | None = None
    review_evidence: ReviewEvidence | None = None


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
    }:
        if not operation.candidate_sha:
            raise ValueError("verification operations require candidate_sha")
    if operation.phase is DeliveryPhase.MERGE and not operation.pull_request_number:
        raise ValueError("merge operations require pull_request_number")
    if operation.phase is DeliveryPhase.PUSH and not operation.merge_commit_sha:
        raise ValueError("push operations require merge_commit_sha")
    if operation.phase is DeliveryPhase.PULL_REQUEST:
        if not operation.candidate_sha:
            raise ValueError("pull request operations require candidate_sha")
        if not operation.pull_request_identity:
            raise ValueError("pull request operations require stable identity")
        if not operation.candidate_branch:
            raise ValueError("pull request operations require candidate_branch")
    if (
        operation.phase in {DeliveryPhase.CI, DeliveryPhase.MERGE, DeliveryPhase.PUSH}
        and not operation.pull_request_number
    ):
        raise ValueError("CI and merge operations require pull_request_number")
