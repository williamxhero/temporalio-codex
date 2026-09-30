from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from enum import StrEnum

from temporalio_codex.planning_models import SourceOrigin, stable_source_identity
from temporalio_codex.whole_flow_models import WholeFlowInput

ENTRY_CONTRACT_VERSION = "requirement-delivery/v1"


class EntryStatus(StrEnum):
    ACTIVE = "active"
    DURABLE_WAITING = "durable_waiting"
    RETRYING = "retrying"
    WAITING_FOR_INPUT = "waiting_for_input"
    BLOCKED = "blocked"
    FAILED = "failed"
    NOT_VERIFIED = "not_verified"
    CANCELLED = "cancelled"
    COMPLETED = "completed"


class EntryPhase(StrEnum):
    INTAKE = "intake"
    PLANNING = "planning"
    TICKETS = "tickets"
    CODEX = "codex"
    DELIVERY = "delivery"
    SUMMARY = "summary"
    BLOCKED = "blocked"
    FAILED = "failed"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class RequirementSource:
    origin: SourceOrigin
    text: str | None = None
    reference: str | None = None

    def __post_init__(self) -> None:
        has_text = bool(self.text and self.text.strip())
        has_reference = bool(self.reference and self.reference.strip())
        if has_text == has_reference:
            raise ValueError("provide exactly one source text or reference")
        if has_text and len(self.text or "") > 100_000:
            raise ValueError("source text is too large")
        if has_reference and len(self.reference or "") > 4_000:
            raise ValueError("source reference is too large")
        if self.origin is SourceOrigin.HISTORICAL_CHAT and not has_reference:
            raise ValueError("historical chat sources require a reference")

    @property
    def identity(self) -> str:
        value = self.text or self.reference or ""
        return hashlib.sha256(
            f"{self.origin.value}:{value}".encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class RequirementDeliveryRequest:
    source: RequirementSource
    repository: str
    artifact_roots: tuple[str, ...]
    execution_plan: WholeFlowInput
    launch_key: str
    target_ref: str = "refs/heads/main"
    task_queue: str = "codex-worker"
    contract_version: str = ENTRY_CONTRACT_VERSION
    metadata: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.contract_version != ENTRY_CONTRACT_VERSION:
            raise ValueError(f"unsupported entry contract: {self.contract_version}")
        if not self.repository.strip():
            raise ValueError("repository is required")
        if not self.launch_key.strip():
            raise ValueError("launch key is required")
        if not self.target_ref.strip():
            raise ValueError("target ref is required")
        if not self.task_queue.strip():
            raise ValueError("task queue is required")
        if not self.artifact_roots:
            raise ValueError("at least one artifact root is required")
        for root in self.artifact_roots:
            normalized = root.replace("\\", "/").strip()
            if not normalized or normalized.startswith("/") or ".." in normalized.split("/"):
                raise ValueError("artifact roots must be non-empty repository-relative paths")
        planning = self.execution_plan.planning
        if planning.origin is not self.source.origin:
            raise ValueError("execution plan source origin does not match request source")
        planning_value = planning.source_text or planning.source_reference or ""
        if stable_source_identity(planning.origin, planning_value) != self.source.identity:
            raise ValueError("execution plan source identity does not match request source")

    @property
    def input_identity(self) -> str:
        payload = {
            "contract_version": self.contract_version,
            "source_identity": self.source.identity,
            "repository": self.repository,
            "artifact_roots": tuple(self.artifact_roots),
            "target_ref": self.target_ref,
            "task_queue": self.task_queue,
            "launch_key": self.launch_key,
            "metadata": tuple(self.metadata),
            "execution_plan": asdict(self.execution_plan),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class EntryStatusSnapshot:
    run_id: str
    launch_key: str
    input_identity: str
    contract_version: str
    phase: EntryPhase
    status: EntryStatus
    active_spec: str | None = None
    active_ticket: str | None = None
    completed_specs: tuple[str, ...] = ()
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
class EntryLaunchReceipt:
    run_id: str
    launch_key: str
    input_identity: str
    contract_version: str
    adopted: bool
    status: EntryStatusSnapshot
