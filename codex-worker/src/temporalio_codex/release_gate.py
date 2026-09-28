from dataclasses import dataclass

from temporalio_codex.release_evidence import (
    EvidenceStatus,
    ReleaseReport,
)


REQUIRED_ACCEPTANCE_CASES = (
    "normal_run",
    "worker_restart",
    "answer",
    "pause",
    "cancel",
    "codex_activity_uncertainty",
    "github_readback",
    "legacy_inspection",
)


@dataclass(frozen=True)
class AcceptanceCase:
    name: str
    status: EvidenceStatus
    evidence_refs: tuple[str, ...] = ()
    reason: str = ""


@dataclass(frozen=True)
class AcceptanceMatrix:
    cases: tuple[AcceptanceCase, ...]
    required_cases: tuple[str, ...] = REQUIRED_ACCEPTANCE_CASES

    def validation_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        by_name = {case.name: case for case in self.cases}
        for name in self.required_cases:
            case = by_name.get(name)
            if case is None:
                errors.append(f"missing acceptance case: {name}")
                continue
            if not case.evidence_refs:
                errors.append(f"acceptance case requires evidence_refs: {name}")
            if case.status is EvidenceStatus.NOT_VERIFIED and not case.reason.strip():
                errors.append(f"not_verified acceptance case requires reason: {name}")
        counts: dict[str, int] = {}
        for case in self.cases:
            counts[case.name] = counts.get(case.name, 0) + 1
        duplicate_names = {name for name, count in counts.items() if count > 1}
        errors.extend(
            f"duplicate acceptance case: {name}" for name in sorted(duplicate_names)
        )
        return tuple(errors)


@dataclass(frozen=True)
class QualificationResult:
    status: EvidenceStatus
    reasons: tuple[str, ...] = ()


def qualify_release(
    report: ReleaseReport,
    matrix: AcceptanceMatrix,
) -> QualificationResult:
    report_errors = report.validation_errors()
    matrix_errors = matrix.validation_errors()
    if report_errors or matrix_errors:
        return QualificationResult(
            status=EvidenceStatus.BLOCKED,
            reasons=(*report_errors, *matrix_errors),
        )

    report_status = report.qualification_status()
    statuses = {case.status for case in matrix.cases}
    if EvidenceStatus.FAIL in statuses or report_status is EvidenceStatus.FAIL:
        return QualificationResult(
            status=EvidenceStatus.FAIL,
            reasons=("a required qualification check failed",),
        )
    if EvidenceStatus.BLOCKED in statuses or report_status is EvidenceStatus.BLOCKED:
        return QualificationResult(
            status=EvidenceStatus.BLOCKED,
            reasons=("a required qualification check is blocked",),
        )
    if (
        EvidenceStatus.NOT_VERIFIED in statuses
        or report_status is EvidenceStatus.NOT_VERIFIED
    ):
        return QualificationResult(
            status=EvidenceStatus.NOT_VERIFIED,
            reasons=("a required qualification check is not verified",),
        )
    return QualificationResult(status=EvidenceStatus.PASS)
