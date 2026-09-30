from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import StrEnum

from temporalio_codex.spec_issue_adapter import (
    SpecDraft,
    SpecIssueRecord,
)


class SourceOrigin(StrEnum):
    TEXT = "text"
    HISTORICAL_CHAT = "historical_chat"


class PlanningPhase(StrEnum):
    INTAKE = "intake"
    GRILLING = "grilling"
    CONFIRMATION_REQUIRED = "confirmation_required"
    READY = "ready"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class PlanningStatus(StrEnum):
    ACTIVE = "active"
    WAITING_FOR_INPUT = "waiting_for_input"
    READY = "ready"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    PUBLISHING = "publishing"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PlanningInput:
    origin: SourceOrigin
    source_text: str | None = None
    source_reference: str | None = None
    sensitive: bool = False
    umbrella_issue_number: int = 1
    specs: tuple[SpecDraft, ...] = ()
    grill_answers: tuple[GrillAnswer, ...] = ()
    confirmation_operation_id: str | None = None
    publication_operation_id: str | None = None
    repository: str = "williamxhero/temporalio-codex"
    publication_timeout_seconds: float = 300.0
    publication_max_attempts: int = 3
    publication_retry_backoff_seconds: float = 1.0
    parent_workflow_id: str | None = None
    parent_workflow_run_id: str | None = None

    def __post_init__(self) -> None:
        has_text = bool(self.source_text and self.source_text.strip())
        has_reference = bool(self.source_reference and self.source_reference.strip())
        if has_text == has_reference:
            raise ValueError("provide exactly one source_text or source_reference")
        if has_text and len(self.source_text or "") > 100_000:
            raise ValueError("source_text is too large")
        if has_reference and len(self.source_reference or "") > 4_000:
            raise ValueError("source_reference is too large")
        if self.sensitive and has_text:
            raise ValueError("sensitive sources must use source_reference")
        if self.origin is SourceOrigin.HISTORICAL_CHAT and not has_reference:
            raise ValueError("historical chat sources must use source_reference")
        if self.publication_timeout_seconds <= 0:
            raise ValueError("publication_timeout_seconds must be positive")
        if self.publication_max_attempts <= 0:
            raise ValueError("publication_max_attempts must be positive")
        if self.publication_retry_backoff_seconds < 0:
            raise ValueError("publication_retry_backoff_seconds must not be negative")

    @property
    def source_identity(self) -> str:
        value = self.source_text or self.source_reference or ""
        return hashlib.sha256(
            f"{self.origin.value}:{value}".encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class SourceRecord:
    origin: SourceOrigin
    source_identity: str
    source_reference: str | None


@dataclass(frozen=True)
class GrillQuestion:
    number: int
    prompt: str
    required: bool = True


@dataclass(frozen=True)
class GrillAnswer:
    question_number: int
    answer: str
    accepted_as_assumption: bool = False


@dataclass(frozen=True)
class GrillRecord:
    questions: tuple[GrillQuestion, ...] = ()
    answers: tuple[GrillAnswer, ...] = ()
    assumptions: tuple[str, ...] = ()
    decisions: tuple[str, ...] = ()
    unresolved: tuple[str, ...] = ()


@dataclass(frozen=True)
class PlanningSnapshot:
    workflow_id: str
    phase: PlanningPhase
    status: PlanningStatus
    source: SourceRecord | None
    grill: GrillRecord
    confirmed: bool = False
    confirmation_operation_id: str | None = None
    publication_requested: bool = False
    publication_operation_id: str | None = None
    published_specs: tuple[SpecIssueRecord, ...] = ()
    publication_reason: str = ""


@dataclass(frozen=True)
class PlanningResult:
    workflow_id: str
    status: PlanningStatus
    phase: PlanningPhase
    source: SourceRecord
    grill: GrillRecord
    confirmation_operation_id: str
    publication_operation_id: str
    published_specs: tuple[SpecIssueRecord, ...] = ()
    publication_reason: str = ""


@dataclass(frozen=True)
class GrillPreparationInput:
    origin: SourceOrigin
    source_identity: str


def stable_source_identity(origin: SourceOrigin, value: str) -> str:
    return hashlib.sha256(f"{origin.value}:{value}".encode("utf-8")).hexdigest()
