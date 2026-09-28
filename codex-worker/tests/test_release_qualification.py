from __future__ import annotations

from types import SimpleNamespace

import temporalio_codex
from temporalio_codex.release_evidence import EvidenceKind, EvidenceStatus
from temporalio_codex.release_qualification import qualify_installed_artifact


def test_installed_artifact_evidence_is_bound_to_exact_build_candidate_and_artifact(monkeypatch) -> None:
    monkeypatch.setattr(
        "temporalio_codex.release_qualification.importlib.metadata.distribution",
        lambda _: SimpleNamespace(
            version="0.1.0",
            entry_points=tuple(SimpleNamespace(name=name) for name in (
                "temporalio-codex-run",
                "temporalio-codex-worker",
                "temporalio-codex-qualify",
                "temporalio-codex-delivery",
                "temporalio-codex-live",
                "temporalio-codex-retirement",
                "temporalio-codex-release-qualify",
            )),
        ),
    )
    result = qualify_installed_artifact(
        build_id="build-1",
        qualification_run_id="qualify-1",
        candidate_sha="a" * 40,
        artifact_ref="wheel:candidate",
    )

    assert result.kind is EvidenceKind.INSTALLED_ARTIFACT
    assert result.status is EvidenceStatus.PASS
    assert result.origin.value == "local"
    assert result.run_id == "qualify-1"
    assert result.candidate_sha == "a" * 40
    assert result.artifact_ref == "wheel:candidate"
    assert result.evidence_refs


def test_installed_artifact_rejects_a_checkout_path(monkeypatch, tmp_path) -> None:
    distribution = SimpleNamespace(
        version="0.1.0",
        entry_points=tuple(
            SimpleNamespace(name=name)
            for name in (
                "temporalio-codex-run",
                "temporalio-codex-worker",
                "temporalio-codex-qualify",
                "temporalio-codex-delivery",
                "temporalio-codex-live",
                "temporalio-codex-retirement",
                "temporalio-codex-release-qualify",
            )
        ),
    )
    monkeypatch.setattr(
        "temporalio_codex.release_qualification.importlib.metadata.distribution",
        lambda _: distribution,
    )
    package_path = tmp_path / "checkout" / "codex-worker" / "src" / "temporalio_codex" / "__init__.py"
    monkeypatch.setattr(temporalio_codex, "__file__", str(package_path))

    result = qualify_installed_artifact(
        build_id="build-1",
        qualification_run_id="qualify-1",
        candidate_sha="a" * 40,
        artifact_ref="wheel:candidate",
        checkout_root=tmp_path / "checkout",
    )

    assert result.status is EvidenceStatus.FAIL
    assert "checkout" in result.reason
