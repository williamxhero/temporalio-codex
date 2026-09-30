from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
from dataclasses import asdict
from pathlib import Path

from temporalio_codex.release_evidence import (
    EvidenceKind,
    EvidenceOrigin,
    EvidenceStatus,
    ReleaseEvidence,
)

PUBLIC_ENTRY_POINTS = frozenset(
    {
        "temporalio-codex-run",
        "temporalio-codex-worker",
        "temporalio-codex-qualify",
        "temporalio-codex-delivery",
        "temporalio-codex-live",
        "temporalio-codex-retirement",
    }
)
PACKAGE_NAME = "temporalio-codex-worker"


def qualify_installed_artifact(
    *,
    build_id: str,
    qualification_run_id: str,
    candidate_sha: str,
    artifact_ref: str,
    checkout_root: str | Path | None = None,
) -> ReleaseEvidence:
    """Inspect the installed package without importing repository source."""
    errors: list[str] = []
    refs: list[str] = []
    if not build_id.strip():
        errors.append("build_id is required")
    if not qualification_run_id.strip():
        errors.append("qualification_run_id is required")
    if len(candidate_sha) != 40:
        errors.append("candidate_sha must be a 40-character commit SHA")
    if not artifact_ref.strip():
        errors.append("artifact_ref is required")
    try:
        distribution = importlib.metadata.distribution(PACKAGE_NAME)
        package_version = distribution.version
        entry_points = {entry.name for entry in distribution.entry_points}
        missing = sorted(PUBLIC_ENTRY_POINTS - entry_points)
        if missing:
            errors.append(f"missing installed entry points: {', '.join(missing)}")
        refs.append(f"distribution:{PACKAGE_NAME}=={package_version}")
    except importlib.metadata.PackageNotFoundError:
        package_version = "unknown"
        errors.append(f"installed distribution is missing: {PACKAGE_NAME}")

    try:
        import temporalio_codex

        package_path = Path(temporalio_codex.__file__ or "").resolve()
        refs.append(f"package:{package_path}")
        if checkout_root is not None:
            root = Path(checkout_root).resolve()
            if root == package_path or root in package_path.parents:
                errors.append("installed package resolves inside the repository checkout")
    except (ImportError, OSError, RuntimeError) as error:
        errors.append(f"installed package import failed: {type(error).__name__}")

    return ReleaseEvidence(
        kind=EvidenceKind.INSTALLED_ARTIFACT,
        status=EvidenceStatus.PASS if not errors else EvidenceStatus.FAIL,
        origin=EvidenceOrigin.LOCAL,
        build_id=build_id,
        package_version=package_version,
        scenario="installed-artifact-public-entry",
        operating_system=platform.platform(),
        command="temporalio-codex-release-qualify",
        run_id=qualification_run_id,
        evidence_refs=tuple(refs),
        candidate_sha=candidate_sha,
        artifact_ref=artifact_ref,
        reason="; ".join(errors),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Qualify the installed Temporal Codex artifact")
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--artifact-ref", required=True)
    parser.add_argument("--checkout-root", type=Path)
    args = parser.parse_args(argv)
    evidence = qualify_installed_artifact(
        build_id=args.build_id,
        qualification_run_id=args.run_id,
        candidate_sha=args.candidate_sha,
        artifact_ref=args.artifact_ref,
        checkout_root=args.checkout_root,
    )
    print(json.dumps(asdict(evidence), ensure_ascii=False, sort_keys=True))
    return 0 if evidence.status is EvidenceStatus.PASS else 2
