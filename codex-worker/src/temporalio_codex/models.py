from dataclasses import dataclass
from enum import StrEnum


class StageOutcome(StrEnum):
    COMPLETED = "completed"


@dataclass(frozen=True)
class RunInput:
    requirement: str


@dataclass(frozen=True)
class StageInput:
    requirement: str


@dataclass(frozen=True)
class StageResult:
    stage: str
    outcome: StageOutcome
    summary: str


@dataclass(frozen=True)
class RunResult:
    workflow_id: str
    status: StageOutcome
    stage: str
    summary: str
