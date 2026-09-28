import pytest

from temporalio_codex.release_evidence import (
    EvidenceKind,
    EvidenceOrigin,
    EvidenceStatus,
    ReleaseEvidence,
    ReleaseReport,
)


def evidence(kind: EvidenceKind, *, status=EvidenceStatus.PASS, **overrides):
    values = {
        "kind": kind,
        "status": status,
        "origin": EvidenceOrigin.DETERMINISTIC,
        "build_id": "build-1",
        "package_version": "0.1.0",
        "scenario": kind.value,
        "operating_system": "Windows",
        "command": "uv run pytest",
    }
    values.update(overrides)
    return ReleaseEvidence(**values)


def complete_report(*items: ReleaseEvidence) -> ReleaseReport:
    return ReleaseReport(build_id="build-1", evidence=items)


def test_report_round_trip_preserves_evidence_and_status() -> None:
    items = tuple(
        evidence(kind)
        for kind in (EvidenceKind.DETERMINISTIC, EvidenceKind.LOCAL_TEMPORAL)
    )
    report = complete_report(*items)

    restored = ReleaseReport.from_json(report.to_json())

    assert restored == report
    assert restored.qualification_status(
        (items[0].kind, items[1].kind)
    ) is EvidenceStatus.PASS


def test_live_evidence_requires_live_identity_and_sdk_version() -> None:
    report = complete_report(evidence(EvidenceKind.LIVE_CODEX_SDK))

    with pytest.raises(ValueError, match="live origin"):
        report.validate((EvidenceKind.LIVE_CODEX_SDK,))

    live = evidence(
        EvidenceKind.LIVE_CODEX_SDK,
        origin=EvidenceOrigin.LIVE,
        run_id="qualification-run-1",
        sdk_versions=("0.155.1",),
    )
    assert complete_report(live).qualification_status(
        (EvidenceKind.LIVE_CODEX_SDK,)
    ) is EvidenceStatus.PASS


def test_not_verified_evidence_is_never_reported_as_pass() -> None:
    report = complete_report(
        evidence(EvidenceKind.DETERMINISTIC),
        evidence(
            EvidenceKind.LOCAL_TEMPORAL,
            status=EvidenceStatus.NOT_VERIFIED,
            reason="local server unavailable",
        ),
    )

    assert report.qualification_status(
        (EvidenceKind.DETERMINISTIC, EvidenceKind.LOCAL_TEMPORAL)
    ) is EvidenceStatus.NOT_VERIFIED


def test_validation_rejects_missing_kinds_and_fake_live_label() -> None:
    fake_live = evidence(
        EvidenceKind.LIVE_GITHUB,
        origin=EvidenceOrigin.DETERMINISTIC,
        run_id="fake-run",
    )

    with pytest.raises(ValueError, match="missing required evidence"):
        complete_report(evidence(EvidenceKind.DETERMINISTIC)).validate(
            (EvidenceKind.DETERMINISTIC, EvidenceKind.WINDOWS)
        )
    with pytest.raises(ValueError, match="live origin"):
        complete_report(fake_live).validate((EvidenceKind.LIVE_GITHUB,))


def test_validation_rejects_duplicate_evidence_and_missing_metadata() -> None:
    duplicate = complete_report(
        evidence(EvidenceKind.DETERMINISTIC),
        evidence(EvidenceKind.DETERMINISTIC),
    )
    missing_metadata = evidence(EvidenceKind.DETERMINISTIC, build_id=None)

    with pytest.raises(ValueError, match="duplicate evidence kind"):
        duplicate.validate((EvidenceKind.DETERMINISTIC,))
    with pytest.raises(ValueError, match="build_id"):
        complete_report(missing_metadata).validate((EvidenceKind.DETERMINISTIC,))


def test_strict_report_binds_every_evidence_record_to_candidate_and_artifact() -> None:
    candidate_sha = "a" * 40
    artifact = "wheel:temporalio-codex-worker-0.1.0"
    item = evidence(
        EvidenceKind.DETERMINISTIC,
        evidence_refs=("test:deterministic",),
        run_id="run-1",
        candidate_sha=candidate_sha,
        artifact_ref=artifact,
        source_revision=candidate_sha,
    )
    report = ReleaseReport(
        build_id="build-1",
        evidence=(item,),
        qualification_run_id="run-1",
        candidate_sha=candidate_sha,
        artifact_ref=artifact,
    )

    assert report.qualification_status(
        (EvidenceKind.DETERMINISTIC,),
        expected_candidate_sha=candidate_sha,
        expected_artifact_ref=artifact,
        strict=True,
    ) is EvidenceStatus.PASS


def test_strict_report_rejects_stale_candidate_and_fake_live_evidence() -> None:
    candidate_sha = "a" * 40
    stale = "b" * 40
    item = evidence(
        EvidenceKind.LIVE_GITHUB,
        origin=EvidenceOrigin.DETERMINISTIC,
        run_id="run-1",
        candidate_sha=stale,
        artifact_ref="artifact-1",
        evidence_refs=("fake:github",),
    )
    report = ReleaseReport(
        build_id="build-1",
        evidence=(item,),
        qualification_run_id="run-1",
        candidate_sha=candidate_sha,
        artifact_ref="artifact-1",
    )

    errors = report.validation_errors(
        (EvidenceKind.LIVE_GITHUB,),
        expected_candidate_sha=candidate_sha,
        expected_artifact_ref="artifact-1",
        strict=True,
    )

    assert any("live origin" in error for error in errors)
    assert any("candidate_sha differs" in error for error in errors)
