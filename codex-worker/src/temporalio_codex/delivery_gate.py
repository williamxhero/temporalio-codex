from dataclasses import dataclass
from enum import StrEnum


class DeliveryGateStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    BLOCKED = "blocked"
    WAITING_FOR_READBACK = "waiting_for_readback"
    NOT_VERIFIED = "not_verified"


@dataclass(frozen=True)
class TicketDeliveryEvidence:
    ticket_key: str
    issue_number: int
    status: str
    commit_sha: str | None = None


@dataclass(frozen=True)
class SpecDeliveryEvidence:
    spec_key: str
    ticket_evidence: tuple[TicketDeliveryEvidence, ...]
    tests_passed: bool | None
    review_completed: bool
    unresolved_findings: tuple[str, ...]
    delivery_status: str
    delivery_run_id: str | None = None
    pull_request_number: int | None = None
    pull_request_url: str | None = None
    merge_commit_sha: str | None = None
    remote_branch: str = "main"
    remote_sha: str | None = None
    remote_contains_merge: bool | None = None
    live_unverified_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class DeliveryGateResult:
    status: DeliveryGateStatus
    reason: str = ""
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class DevelopmentSummary:
    status: DeliveryGateStatus
    completed_specs: tuple[str, ...]
    merge_commits: tuple[str, ...]
    pull_requests: tuple[str, ...]
    test_summary: str
    unverified_gates: tuple[str, ...]
    remote_state: str
    reason: str = ""

    def to_text(self) -> str:
        lines = [
            f"Status: {self.status.value}",
            f"Completed SPECs: {', '.join(self.completed_specs) or 'none'}",
            f"Merge commits: {', '.join(self.merge_commits) or 'none'}",
            f"Pull requests: {', '.join(self.pull_requests) or 'none'}",
            f"Tests: {self.test_summary}",
            f"Remote: {self.remote_state}",
            f"Unverified gates: {', '.join(self.unverified_gates) or 'none'}",
        ]
        if self.reason:
            lines.append(f"Reason: {self.reason}")
        return "\n".join(lines)


def qualify_spec_delivery(
    evidence: SpecDeliveryEvidence,
    required_ticket_keys: tuple[str, ...],
) -> DeliveryGateResult:
    by_key = {item.ticket_key: item for item in evidence.ticket_evidence}
    if len(by_key) != len(evidence.ticket_evidence):
        return DeliveryGateResult(
            DeliveryGateStatus.BLOCKED,
            "duplicate ticket evidence",
        )
    missing = [key for key in required_ticket_keys if key not in by_key]
    if missing:
        return DeliveryGateResult(
            DeliveryGateStatus.BLOCKED,
            f"missing ticket evidence: {', '.join(missing)}",
        )
    incomplete = [
        key
        for key in required_ticket_keys
        if by_key[key].status != "completed"
    ]
    if incomplete:
        return DeliveryGateResult(
            DeliveryGateStatus.BLOCKED,
            f"tickets are incomplete: {', '.join(incomplete)}",
        )
    if evidence.tests_passed is False:
        return DeliveryGateResult(DeliveryGateStatus.FAIL, "required tests failed")
    if evidence.tests_passed is None:
        return DeliveryGateResult(DeliveryGateStatus.BLOCKED, "test result is missing")
    if any(item.issue_number <= 0 for item in evidence.ticket_evidence):
        return DeliveryGateResult(
            DeliveryGateStatus.BLOCKED,
            "ticket evidence has an invalid issue number",
        )
    if not evidence.review_completed:
        return DeliveryGateResult(DeliveryGateStatus.BLOCKED, "independent review is incomplete")
    if evidence.unresolved_findings:
        return DeliveryGateResult(
            DeliveryGateStatus.BLOCKED,
            "review findings are unresolved: " + ", ".join(evidence.unresolved_findings),
        )
    if evidence.delivery_status in {"unknown", "waiting", "waiting_for_readback"}:
        return DeliveryGateResult(
            DeliveryGateStatus.WAITING_FOR_READBACK,
            "delivery requires external readback",
        )
    if evidence.delivery_status != "completed":
        return DeliveryGateResult(
            DeliveryGateStatus.FAIL,
            f"delivery status is {evidence.delivery_status}",
        )
    if not evidence.merge_commit_sha or not evidence.remote_sha:
        return DeliveryGateResult(
            DeliveryGateStatus.WAITING_FOR_READBACK,
            "merge or remote SHA evidence is missing",
        )
    if evidence.remote_contains_merge is not True:
        return DeliveryGateResult(
            DeliveryGateStatus.WAITING_FOR_READBACK,
            "origin remote readback does not contain the merge commit",
        )
    refs = tuple(
        ref
        for ref in (
            *(item.commit_sha or "" for item in evidence.ticket_evidence),
            evidence.delivery_run_id or "",
            str(evidence.pull_request_number or ""),
            evidence.pull_request_url or "",
            evidence.merge_commit_sha,
            f"{evidence.remote_branch}:{evidence.remote_sha}",
        )
        if ref
    )
    if evidence.live_unverified_reasons:
        return DeliveryGateResult(
            DeliveryGateStatus.NOT_VERIFIED,
            "live gates are unavailable",
            refs + evidence.live_unverified_reasons,
        )
    return DeliveryGateResult(DeliveryGateStatus.PASS, evidence_refs=refs)


def build_development_summary(
    results: tuple[DeliveryGateResult, ...],
    evidences: tuple[SpecDeliveryEvidence, ...],
) -> DevelopmentSummary:
    if not results or len(results) != len(evidences):
        return DevelopmentSummary(
            status=DeliveryGateStatus.BLOCKED,
            completed_specs=(),
            merge_commits=(),
            pull_requests=(),
            test_summary="none",
            unverified_gates=(),
            remote_state="not verified",
            reason=(
                "no SPEC delivery evidence"
                if not results
                else "delivery result and evidence counts differ"
            ),
        )
    statuses = {result.status for result in results}
    if DeliveryGateStatus.FAIL in statuses:
        status = DeliveryGateStatus.FAIL
    elif DeliveryGateStatus.BLOCKED in statuses:
        status = DeliveryGateStatus.BLOCKED
    elif DeliveryGateStatus.WAITING_FOR_READBACK in statuses:
        status = DeliveryGateStatus.WAITING_FOR_READBACK
    elif DeliveryGateStatus.NOT_VERIFIED in statuses:
        status = DeliveryGateStatus.NOT_VERIFIED
    else:
        status = DeliveryGateStatus.PASS
    completed = tuple(
        evidence.spec_key
        for evidence, result in zip(evidences, results)
        if result.status in (DeliveryGateStatus.PASS, DeliveryGateStatus.NOT_VERIFIED)
    )
    merge_commits = tuple(
        evidence.merge_commit_sha
        for evidence in evidences
        if evidence.merge_commit_sha
    )
    pull_requests = tuple(
        evidence.pull_request_url or f"#{evidence.pull_request_number}"
        for evidence in evidences
        if evidence.pull_request_url or evidence.pull_request_number
    )
    unverified = tuple(
        reason
        for evidence in evidences
        for reason in evidence.live_unverified_reasons
    )
    remote = evidences[-1].remote_sha or "not verified"
    return DevelopmentSummary(
        status=status,
        completed_specs=completed,
        merge_commits=merge_commits,
        pull_requests=pull_requests,
        test_summary="recorded for completed delivery gates",
        unverified_gates=unverified,
        remote_state=f"origin/{evidences[-1].remote_branch}={remote}",
        reason="; ".join(result.reason for result in results if result.reason),
    )
