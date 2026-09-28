from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from temporalio_codex.legacy_inspector import inspect_legacy_database


class LegacyOperation(StrEnum):
    RESUME = "resume"
    MIGRATE = "migrate"
    TAKEOVER = "takeover"
    REPAIR = "repair"
    CONTINUE = "continue"


class LegacyDecision(StrEnum):
    REJECTED = "rejected"
    NOT_VERIFIED = "not_verified"


@dataclass(frozen=True)
class LegacyPolicyResult:
    decision: LegacyDecision
    operation: LegacyOperation
    source_run_id: str
    source_identity: str | None = None
    evidence_refs: tuple[str, ...] = ()
    reason: str = ""


def reject_legacy_operation(
    path: str | Path,
    source_run_id: str,
    operation: LegacyOperation,
) -> LegacyPolicyResult:
    """Inspect a legacy record and reject executable compatibility actions."""
    inspection = inspect_legacy_database(path)
    if inspection.status != "verified":
        return LegacyPolicyResult(
            decision=LegacyDecision.NOT_VERIFIED,
            operation=operation,
            source_run_id=source_run_id,
            source_identity=inspection.source_identity,
            reason=inspection.reason or "legacy evidence is not verified",
        )
    records = tuple(record for record in inspection.records if record.run_id == source_run_id)
    if len(records) != 1:
        return LegacyPolicyResult(
            decision=LegacyDecision.NOT_VERIFIED,
            operation=operation,
            source_run_id=source_run_id,
            source_identity=inspection.source_identity,
            reason=(
                "legacy run identity is missing or ambiguous; "
                "no replacement execution was created"
            ),
        )
    record = records[0]
    return LegacyPolicyResult(
        decision=LegacyDecision.REJECTED,
        operation=operation,
        source_run_id=source_run_id,
        source_identity=inspection.source_identity,
        evidence_refs=record.evidence,
        reason=(
            f"legacy run {source_run_id} is immutable evidence; "
            f"{operation.value} is unavailable"
        ),
    )
