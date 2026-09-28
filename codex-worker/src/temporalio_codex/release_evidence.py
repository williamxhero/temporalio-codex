import json
from dataclasses import asdict, dataclass
from enum import StrEnum

RELEASE_EVIDENCE_SCHEMA_VERSION = "temporalio-codex-release-evidence/v2"
_SHA_LENGTH = 40


class EvidenceKind(StrEnum):
    DETERMINISTIC = "deterministic"
    LOCAL_TEMPORAL = "local_temporal"
    INSTALLED_ARTIFACT = "installed_artifact"
    LIVE_CODEX_SDK = "live_codex_sdk"
    LIVE_GITHUB = "live_github"
    WINDOWS = "windows"
    PROJECT_ACCEPTANCE = "project_acceptance"


class EvidenceOrigin(StrEnum):
    DETERMINISTIC = "deterministic"
    LOCAL = "local"
    LIVE = "live"


class EvidenceStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    BLOCKED = "blocked"
    NOT_VERIFIED = "not_verified"


LIVE_KINDS = frozenset(
    {
        EvidenceKind.LIVE_CODEX_SDK,
        EvidenceKind.LIVE_GITHUB,
        EvidenceKind.WINDOWS,
    }
)


@dataclass(frozen=True)
class ReleaseEvidence:
    kind: EvidenceKind
    status: EvidenceStatus
    origin: EvidenceOrigin
    build_id: str
    package_version: str
    scenario: str
    operating_system: str
    command: str
    run_id: str | None = None
    sdk_versions: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    reason: str = ""
    candidate_sha: str | None = None
    artifact_ref: str | None = None
    source_revision: str | None = None

    def validation_errors(
        self,
        *,
        expected_build_id: str | None = None,
        expected_run_id: str | None = None,
        expected_candidate_sha: str | None = None,
        expected_artifact_ref: str | None = None,
        strict: bool = False,
    ) -> tuple[str, ...]:
        errors = [
            field
            for field, value in (
                ("build_id", self.build_id),
                ("package_version", self.package_version),
                ("scenario", self.scenario),
                ("operating_system", self.operating_system),
                ("command", self.command),
            )
            if not isinstance(value, str) or not value.strip()
        ]
        if expected_build_id is not None and self.build_id != expected_build_id:
            errors.append("build_id differs from expected release build")
        if expected_run_id is not None and self.run_id != expected_run_id:
            errors.append("run_id differs from expected qualification run")
        if expected_candidate_sha is not None and self.candidate_sha != expected_candidate_sha:
            errors.append("candidate_sha differs from expected candidate")
        if expected_artifact_ref is not None and self.artifact_ref != expected_artifact_ref:
            errors.append("artifact_ref differs from expected artifact")
        if self.kind in LIVE_KINDS:
            if self.origin is not EvidenceOrigin.LIVE:
                errors.append("live evidence must have live origin")
            if self.status is not EvidenceStatus.NOT_VERIFIED and (
                not self.run_id or not self.run_id.strip()
            ):
                errors.append("live evidence requires run_id")
        elif self.origin is EvidenceOrigin.LIVE:
            errors.append("deterministic or local evidence cannot have live origin")
        if (
            self.kind is EvidenceKind.LIVE_CODEX_SDK
            and self.status is not EvidenceStatus.NOT_VERIFIED
            and not self.sdk_versions
        ):
            errors.append("live Codex SDK evidence requires sdk_versions")
        if self.status is EvidenceStatus.NOT_VERIFIED and not self.reason.strip():
            errors.append("not_verified evidence requires reason")
        if self.status in {EvidenceStatus.FAIL, EvidenceStatus.BLOCKED} and not self.reason.strip():
            errors.append(f"{self.status.value} evidence requires reason")
        if strict and not self.evidence_refs:
            errors.append("strict evidence requires evidence_refs")
        if self.candidate_sha is not None and len(self.candidate_sha) != _SHA_LENGTH:
            errors.append("candidate_sha must be a 40-character commit SHA")
        if self.source_revision is not None and len(self.source_revision) != _SHA_LENGTH:
            errors.append("source_revision must be a 40-character commit SHA")
        return tuple(errors)


@dataclass(frozen=True)
class ReleaseReport:
    build_id: str
    evidence: tuple[ReleaseEvidence, ...]
    schema_version: str = RELEASE_EVIDENCE_SCHEMA_VERSION
    qualification_run_id: str | None = None
    candidate_sha: str | None = None
    artifact_ref: str | None = None
    limitations: tuple[str, ...] = ()

    def validation_errors(
        self,
        required_kinds: tuple[EvidenceKind, ...] = tuple(EvidenceKind),
        *,
        expected_candidate_sha: str | None = None,
        expected_artifact_ref: str | None = None,
        strict: bool = False,
    ) -> tuple[str, ...]:
        errors: list[str] = []
        if self.schema_version != RELEASE_EVIDENCE_SCHEMA_VERSION:
            errors.append("unsupported release evidence schema version")
        if not self.build_id.strip():
            errors.append("report build_id is required")
        if expected_candidate_sha is not None and self.candidate_sha != expected_candidate_sha:
            errors.append("report candidate_sha differs from expected candidate")
        if expected_artifact_ref is not None and self.artifact_ref != expected_artifact_ref:
            errors.append("report artifact_ref differs from expected artifact")
        if strict and self.candidate_sha is None:
            errors.append("strict release report requires candidate_sha")
        if strict and self.artifact_ref is None:
            errors.append("strict release report requires artifact_ref")
        if strict and not self.qualification_run_id:
            errors.append("strict release report requires qualification_run_id")
        if self.candidate_sha is not None and len(self.candidate_sha) != _SHA_LENGTH:
            errors.append("report candidate_sha must be a 40-character commit SHA")
        present = {item.kind for item in self.evidence}
        counts: dict[EvidenceKind, int] = {}
        for item in self.evidence:
            counts[item.kind] = counts.get(item.kind, 0) + 1
        errors.extend(
            f"duplicate evidence kind: {kind.value}"
            for kind, count in counts.items()
            if count > 1
        )
        errors.extend(
            f"missing required evidence: {kind.value}"
            for kind in required_kinds
            if kind not in present
        )
        for index, item in enumerate(self.evidence):
            if item.build_id != self.build_id:
                errors.append(f"evidence[{index}] build_id differs from report")
            if self.qualification_run_id is not None and item.run_id != self.qualification_run_id:
                errors.append(f"evidence[{index}] run_id differs from report")
            if self.candidate_sha is not None and item.candidate_sha != self.candidate_sha:
                errors.append(f"evidence[{index}] candidate_sha differs from report")
            if self.artifact_ref is not None and item.artifact_ref != self.artifact_ref:
                errors.append(f"evidence[{index}] artifact_ref differs from report")
            if strict and not item.run_id:
                errors.append(f"evidence[{index}] strict evidence requires run_id")
            errors.extend(
                f"evidence[{index}] {error}"
                for error in item.validation_errors(
                    expected_candidate_sha=expected_candidate_sha,
                    expected_artifact_ref=expected_artifact_ref,
                    strict=strict,
                )
            )
        return tuple(errors)

    def validate(
        self,
        required_kinds: tuple[EvidenceKind, ...] = tuple(EvidenceKind),
        *,
        expected_candidate_sha: str | None = None,
        expected_artifact_ref: str | None = None,
        strict: bool = False,
    ) -> None:
        errors = self.validation_errors(
            required_kinds,
            expected_candidate_sha=expected_candidate_sha,
            expected_artifact_ref=expected_artifact_ref,
            strict=strict,
        )
        if errors:
            raise ValueError("; ".join(errors))

    def qualification_status(
        self,
        required_kinds: tuple[EvidenceKind, ...] = tuple(EvidenceKind),
        *,
        expected_candidate_sha: str | None = None,
        expected_artifact_ref: str | None = None,
        strict: bool = False,
    ) -> EvidenceStatus:
        self.validate(
            required_kinds,
            expected_candidate_sha=expected_candidate_sha,
            expected_artifact_ref=expected_artifact_ref,
            strict=strict,
        )
        statuses = {item.status for item in self.evidence}
        if EvidenceStatus.FAIL in statuses:
            return EvidenceStatus.FAIL
        if EvidenceStatus.BLOCKED in statuses:
            return EvidenceStatus.BLOCKED
        if EvidenceStatus.NOT_VERIFIED in statuses:
            return EvidenceStatus.NOT_VERIFIED
        return EvidenceStatus.PASS

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, value: str) -> "ReleaseReport":
        payload = json.loads(value)
        return cls(
            build_id=payload["build_id"],
            evidence=tuple(
                ReleaseEvidence(
                    kind=EvidenceKind(item["kind"]),
                    status=EvidenceStatus(item["status"]),
                    origin=EvidenceOrigin(item["origin"]),
                    build_id=item["build_id"],
                    package_version=item["package_version"],
                    scenario=item["scenario"],
                    operating_system=item["operating_system"],
                    command=item["command"],
                    run_id=item.get("run_id"),
                    sdk_versions=tuple(item.get("sdk_versions", ())),
                    evidence_refs=tuple(item.get("evidence_refs", ())),
                    reason=item.get("reason", ""),
                    candidate_sha=item.get("candidate_sha"),
                    artifact_ref=item.get("artifact_ref"),
                    source_revision=item.get("source_revision"),
                )
                for item in payload["evidence"]
            ),
            schema_version=payload.get("schema_version", RELEASE_EVIDENCE_SCHEMA_VERSION),
            qualification_run_id=payload.get("qualification_run_id"),
            candidate_sha=payload.get("candidate_sha"),
            artifact_ref=payload.get("artifact_ref"),
            limitations=tuple(payload.get("limitations", ())),
        )
