import json
from dataclasses import asdict, dataclass
from enum import StrEnum


class EvidenceKind(StrEnum):
    DETERMINISTIC = "deterministic"
    LOCAL_TEMPORAL = "local_temporal"
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

    def validation_errors(self) -> tuple[str, ...]:
        errors = [
            field
            for field, value in (
                ("build_id", self.build_id),
                ("package_version", self.package_version),
                ("scenario", self.scenario),
                ("operating_system", self.operating_system),
                ("command", self.command),
            )
            if not value or not value.strip()
        ]
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
        return tuple(errors)


@dataclass(frozen=True)
class ReleaseReport:
    build_id: str
    evidence: tuple[ReleaseEvidence, ...]

    def validation_errors(
        self,
        required_kinds: tuple[EvidenceKind, ...] = tuple(EvidenceKind),
    ) -> tuple[str, ...]:
        errors: list[str] = []
        if not self.build_id.strip():
            errors.append("report build_id is required")
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
            errors.extend(
                f"evidence[{index}] {error}" for error in item.validation_errors()
            )
        return tuple(errors)

    def validate(
        self,
        required_kinds: tuple[EvidenceKind, ...] = tuple(EvidenceKind),
    ) -> None:
        errors = self.validation_errors(required_kinds)
        if errors:
            raise ValueError("; ".join(errors))

    def qualification_status(
        self,
        required_kinds: tuple[EvidenceKind, ...] = tuple(EvidenceKind),
    ) -> EvidenceStatus:
        self.validate(required_kinds)
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
                )
                for item in payload["evidence"]
            ),
        )
