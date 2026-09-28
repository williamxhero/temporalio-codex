from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace

from temporalio_codex.release_evidence import (
    EvidenceStatus,
    ReleaseReport,
)
from temporalio_codex.release_gate import (
    AcceptanceCase,
    AcceptanceMatrix,
    QualificationResult,
    qualify_release,
)

FINAL_RELEASE_SCHEMA_VERSION = "temporalio-codex-final-release/v1"
_SHA_LENGTH = 40


@dataclass(frozen=True)
class FinalReleaseReport:
    build_id: str
    qualification_run_id: str
    repository: str
    candidate_sha: str
    artifact_ref: str
    completed_specs: tuple[str, ...]
    completed_tickets: tuple[str, ...]
    test_refs: tuple[str, ...]
    review_refs: tuple[str, ...]
    pull_requests: tuple[str, ...]
    check_refs: tuple[str, ...]
    merge_commits: tuple[str, ...]
    push_refs: tuple[str, ...]
    origin_branch: str
    origin_sha: str
    cleanup_refs: tuple[str, ...]
    summary_refs: tuple[str, ...]
    retirement_policy: str
    retirement_refs: tuple[str, ...]
    limitations: tuple[str, ...]
    release_report: ReleaseReport
    acceptance_cases: tuple[AcceptanceCase, ...]
    schema_version: str = FINAL_RELEASE_SCHEMA_VERSION
    claimed_status: EvidenceStatus = EvidenceStatus.NOT_VERIFIED
    claimed_reasons: tuple[str, ...] = ()

    def validation_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        if self.schema_version != FINAL_RELEASE_SCHEMA_VERSION:
            errors.append("unsupported final release schema version")
        for field, value in (
            ("build_id", self.build_id),
            ("qualification_run_id", self.qualification_run_id),
            ("repository", self.repository),
            ("candidate_sha", self.candidate_sha),
            ("artifact_ref", self.artifact_ref),
            ("origin_branch", self.origin_branch),
            ("origin_sha", self.origin_sha),
            ("retirement_policy", self.retirement_policy),
        ):
            if not isinstance(value, str) or not value.strip():
                errors.append(f"final release {field} is required")
        if not isinstance(self.candidate_sha, str) or len(self.candidate_sha) != _SHA_LENGTH:
            errors.append("final release candidate_sha must be a 40-character commit SHA")
        if not isinstance(self.origin_sha, str) or len(self.origin_sha) != _SHA_LENGTH:
            errors.append("final release origin_sha must be a 40-character commit SHA")
        for field, values in (
            ("completed_specs", self.completed_specs),
            ("completed_tickets", self.completed_tickets),
            ("test_refs", self.test_refs),
            ("review_refs", self.review_refs),
            ("pull_requests", self.pull_requests),
            ("check_refs", self.check_refs),
            ("merge_commits", self.merge_commits),
            ("push_refs", self.push_refs),
            ("cleanup_refs", self.cleanup_refs),
            ("summary_refs", self.summary_refs),
            ("retirement_refs", self.retirement_refs),
            ("acceptance_cases", self.acceptance_cases),
        ):
            if not values:
                errors.append(f"final release {field} is required")
        if any(len(commit) != _SHA_LENGTH for commit in self.merge_commits):
            errors.append("final release merge_commits must contain full commit SHAs")
        if self.merge_commits and self.origin_sha != self.merge_commits[-1]:
            errors.append("final release origin_sha must match the final merge commit")
        required_policy = (
            "legacy records are immutable evidence",
            "resume, migration, takeover and continuation are unavailable",
        )
        for marker in required_policy:
            if marker not in self.retirement_policy.lower():
                errors.append(f"retirement policy is missing: {marker}")
        if self.release_report.build_id != self.build_id:
            errors.append("nested release report build_id differs")
        if self.release_report.qualification_run_id != self.qualification_run_id:
            errors.append("nested release report run_id differs")
        if self.release_report.candidate_sha != self.candidate_sha:
            errors.append("nested release report candidate_sha differs")
        if self.release_report.artifact_ref != self.artifact_ref:
            errors.append("nested release report artifact_ref differs")
        return tuple(errors)

    def qualify(self) -> QualificationResult:
        errors = self.validation_errors()
        if errors:
            return QualificationResult(EvidenceStatus.BLOCKED, errors)
        result = qualify_release(
            self.release_report,
            AcceptanceMatrix(cases=self.acceptance_cases),
            expected_candidate_sha=self.candidate_sha,
            expected_artifact_ref=self.artifact_ref,
            strict=True,
        )
        if result.status is EvidenceStatus.PASS and self.limitations:
            return QualificationResult(
                EvidenceStatus.NOT_VERIFIED,
                ("release has unresolved limitations", *self.limitations),
            )
        return result

    def with_qualification(self) -> FinalReleaseReport:
        result = self.qualify()
        return replace(self, claimed_status=result.status, claimed_reasons=result.reasons)

    def to_json(self) -> str:
        payload = asdict(self)
        payload["release_report"] = json.loads(self.release_report.to_json())
        payload["acceptance_cases"] = [asdict(case) for case in self.acceptance_cases]
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, value: str) -> FinalReleaseReport:
        payload = json.loads(value)
        release = ReleaseReport.from_json(json.dumps(payload["release_report"]))
        cases = tuple(
            AcceptanceCase(
                name=item["name"],
                status=EvidenceStatus(item["status"]),
                evidence_refs=tuple(item.get("evidence_refs", ())),
                reason=item.get("reason", ""),
            )
            for item in payload["acceptance_cases"]
        )
        return cls(
            build_id=payload["build_id"],
            qualification_run_id=payload["qualification_run_id"],
            repository=payload["repository"],
            candidate_sha=payload["candidate_sha"],
            artifact_ref=payload["artifact_ref"],
            completed_specs=tuple(payload["completed_specs"]),
            completed_tickets=tuple(payload["completed_tickets"]),
            test_refs=tuple(payload["test_refs"]),
            review_refs=tuple(payload["review_refs"]),
            pull_requests=tuple(payload["pull_requests"]),
            check_refs=tuple(payload["check_refs"]),
            merge_commits=tuple(payload["merge_commits"]),
            push_refs=tuple(payload["push_refs"]),
            origin_branch=payload["origin_branch"],
            origin_sha=payload["origin_sha"],
            cleanup_refs=tuple(payload["cleanup_refs"]),
            summary_refs=tuple(payload["summary_refs"]),
            retirement_policy=payload["retirement_policy"],
            retirement_refs=tuple(payload["retirement_refs"]),
            limitations=tuple(payload["limitations"]),
            release_report=release,
            acceptance_cases=cases,
            schema_version=payload.get("schema_version", FINAL_RELEASE_SCHEMA_VERSION),
            claimed_status=EvidenceStatus(payload.get("claimed_status", EvidenceStatus.NOT_VERIFIED)),
            claimed_reasons=tuple(payload.get("claimed_reasons", ())),
        )


def qualify_final_release(report: FinalReleaseReport) -> QualificationResult:
    """Recompute the final decision from receipts instead of trusting claims."""
    return report.qualify()
