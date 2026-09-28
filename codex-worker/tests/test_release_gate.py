from temporalio_codex.release_evidence import (
    EvidenceKind,
    EvidenceOrigin,
    EvidenceStatus,
    ReleaseEvidence,
    ReleaseReport,
)
from temporalio_codex.release_gate import (
    REQUIRED_ACCEPTANCE_CASES,
    AcceptanceCase,
    AcceptanceMatrix,
    qualify_release,
)


def report(*statuses: EvidenceStatus) -> ReleaseReport:
    kinds = tuple(EvidenceKind)
    evidence = []
    for kind, status in zip(kinds, statuses):
        evidence.append(
            ReleaseEvidence(
                kind=kind,
                status=status,
                origin=(
                    EvidenceOrigin.LIVE
                    if kind in {
                        EvidenceKind.LIVE_CODEX_SDK,
                        EvidenceKind.LIVE_GITHUB,
                        EvidenceKind.WINDOWS,
                    }
                    else EvidenceOrigin.LOCAL
                ),
                build_id="build-1",
                package_version="0.1.0",
                scenario=kind.value,
                operating_system="Windows",
                command="qualification",
                run_id=(f"run-{kind.value}" if status is EvidenceStatus.PASS else None),
                sdk_versions=("0.155.1",)
                if kind is EvidenceKind.LIVE_CODEX_SDK and status is EvidenceStatus.PASS
                else (),
                reason=("live gate unavailable" if status is EvidenceStatus.NOT_VERIFIED else ""),
            )
        )
    return ReleaseReport(build_id="build-1", evidence=tuple(evidence))


def matrix(status=EvidenceStatus.PASS) -> AcceptanceMatrix:
    return AcceptanceMatrix(
        cases=tuple(
            AcceptanceCase(
                name=name,
                status=status,
                evidence_refs=(f"evidence:{name}",),
                reason="live gate unavailable" if status is EvidenceStatus.NOT_VERIFIED else "",
            )
            for name in REQUIRED_ACCEPTANCE_CASES
        )
    )


def test_complete_matrix_passes_only_with_complete_report() -> None:
    result = qualify_release(
        report(*([EvidenceStatus.PASS] * len(tuple(EvidenceKind)))),
        matrix(),
    )

    assert result.status is EvidenceStatus.PASS
    assert result.reasons == ()


def test_missing_matrix_case_is_blocked() -> None:
    incomplete = AcceptanceMatrix(cases=matrix().cases[:-1])

    result = qualify_release(
        report(*([EvidenceStatus.PASS] * len(tuple(EvidenceKind)))),
        incomplete,
    )

    assert result.status is EvidenceStatus.BLOCKED
    assert "legacy_inspection" in " ".join(result.reasons)


def test_unavailable_live_gate_is_not_verified() -> None:
    statuses = [EvidenceStatus.PASS] * len(tuple(EvidenceKind))
    statuses[tuple(EvidenceKind).index(EvidenceKind.LIVE_GITHUB)] = EvidenceStatus.NOT_VERIFIED

    result = qualify_release(report(*statuses), matrix())

    assert result.status is EvidenceStatus.NOT_VERIFIED


def test_failed_case_wins_over_not_verified_evidence() -> None:
    statuses = [EvidenceStatus.PASS] * len(tuple(EvidenceKind))
    statuses[tuple(EvidenceKind).index(EvidenceKind.LIVE_GITHUB)] = EvidenceStatus.NOT_VERIFIED
    failed_matrix = AcceptanceMatrix(
        cases=tuple(
            AcceptanceCase(
                name=case.name,
                status=EvidenceStatus.FAIL if case.name == "pause" else case.status,
                evidence_refs=case.evidence_refs,
                reason=case.reason,
            )
            for case in matrix().cases
        )
    )

    result = qualify_release(report(*statuses), failed_matrix)

    assert result.status is EvidenceStatus.FAIL


def test_strict_release_gate_rejects_missing_exact_candidate_binding() -> None:
    result = qualify_release(
        report(*([EvidenceStatus.PASS] * len(tuple(EvidenceKind)))),
        matrix(),
        expected_candidate_sha="a" * 40,
        expected_artifact_ref="wheel:candidate",
        strict=True,
    )

    assert result.status is EvidenceStatus.BLOCKED
    assert any("candidate_sha" in reason for reason in result.reasons)
