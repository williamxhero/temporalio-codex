from __future__ import annotations

from temporalio_codex.final_release import FinalReleaseReport, qualify_final_release
from temporalio_codex.release_evidence import (
    EvidenceKind,
    EvidenceOrigin,
    EvidenceStatus,
    ReleaseEvidence,
    ReleaseReport,
)
from temporalio_codex.release_gate import REQUIRED_ACCEPTANCE_CASES, AcceptanceCase


def _report(*, limitations=()):
    candidate = "a" * 40
    artifact = "wheel:candidate"
    run_id = "release-run-1"
    evidence = []
    for kind in EvidenceKind:
        live = kind in {EvidenceKind.LIVE_CODEX_SDK, EvidenceKind.LIVE_GITHUB, EvidenceKind.WINDOWS}
        evidence.append(
            ReleaseEvidence(
                kind=kind,
                status=EvidenceStatus.PASS,
                origin=EvidenceOrigin.LIVE if live else EvidenceOrigin.LOCAL,
                build_id="build-1",
                package_version="0.1.0",
                scenario=kind.value,
                operating_system="Windows",
                command="qualification",
                run_id=run_id,
                sdk_versions=("0.155.1",) if kind is EvidenceKind.LIVE_CODEX_SDK else (),
                evidence_refs=(f"evidence:{kind.value}",),
                candidate_sha=candidate,
                artifact_ref=artifact,
            )
        )
    release = ReleaseReport(
        build_id="build-1",
        evidence=tuple(evidence),
        qualification_run_id=run_id,
        candidate_sha=candidate,
        artifact_ref=artifact,
    )
    cases = tuple(
        AcceptanceCase(name=name, status=EvidenceStatus.PASS, evidence_refs=(f"case:{name}",))
        for name in REQUIRED_ACCEPTANCE_CASES
    )
    return FinalReleaseReport(
        build_id="build-1",
        qualification_run_id=run_id,
        repository="owner/repo",
        candidate_sha=candidate,
        artifact_ref=artifact,
        completed_specs=("TC-08.4",),
        completed_tickets=("TC-08.4.1", "TC-08.4.2", "TC-08.4.3", "TC-08.4.4", "TC-08.4.5"),
        test_refs=("tests:worker",),
        review_refs=("review:pr-1",),
        pull_requests=("https://example.test/pr/1",),
        check_refs=("checks:pr-1",),
        merge_commits=("b" * 40,),
        push_refs=("push:main",),
        origin_branch="main",
        origin_sha="b" * 40,
        cleanup_refs=("cleanup:run-1",),
        summary_refs=("summary:run-1",),
        retirement_policy="Legacy records are immutable evidence; resume, migration, takeover and continuation are unavailable.",
        retirement_refs=("retirement:run-1",),
        limitations=tuple(limitations),
        release_report=release,
        acceptance_cases=cases,
    )


def test_final_release_requires_every_delivery_receipt_and_round_trips() -> None:
    report = _report()
    result = qualify_final_release(report)

    assert result.status is EvidenceStatus.PASS
    assert FinalReleaseReport.from_json(report.to_json()) == report
    assert report.with_qualification().claimed_status is EvidenceStatus.PASS


def test_final_release_preserves_limitations_as_not_verified() -> None:
    report = _report(limitations=("live GitHub credentials unavailable",))

    result = qualify_final_release(report)

    assert result.status is EvidenceStatus.NOT_VERIFIED
    assert "credentials unavailable" in " ".join(result.reasons)


def test_final_release_rejects_origin_divergence_and_fake_claimed_pass() -> None:
    report = _report()
    report = report.__class__(
        **{
            **report.__dict__,
            "origin_sha": "c" * 40,
            "claimed_status": EvidenceStatus.PASS,
        }
    )

    result = qualify_final_release(report)

    assert result.status is EvidenceStatus.BLOCKED
    assert "origin_sha" in " ".join(result.reasons)
