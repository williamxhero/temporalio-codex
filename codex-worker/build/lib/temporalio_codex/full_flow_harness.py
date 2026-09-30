from __future__ import annotations

import asyncio
import json
import os
import platform
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

CODEX_SDK_VERSION = "0.155.1"
CODEX_APPROVAL_MODE = "deny_all"


class HarnessStatus(StrEnum):
    VERIFIED = "verified"
    FAILED = "failed"
    BLOCKED = "blocked"
    NOT_VERIFIED = "not_verified"
    CLEANUP_PENDING = "cleanup_pending"


class EvidenceKind(StrEnum):
    DETERMINISTIC = "deterministic"
    INSTALLED = "installed"
    LOCAL = "local"
    LIVE = "live"


class EvidenceStatus(StrEnum):
    VERIFIED = "verified"
    FAILED = "failed"
    BLOCKED = "blocked"
    NOT_VERIFIED = "not_verified"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CapabilityObservation:
    name: str
    available: bool
    detail: str = ""


@dataclass(frozen=True)
class EvidenceRecord:
    reference: str
    kind: EvidenceKind
    status: EvidenceStatus
    operation_id: str
    phase: str
    details: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class ResourceRecord:
    resource_type: str
    identity: str
    marker: str
    operation_id: str
    state: str = "created"


@dataclass(frozen=True)
class HarnessManifest:
    schema_version: str
    run_id: str
    marker: str
    build_id: str
    repository: str
    scenario: str
    command: str
    operating_system: str
    status: HarnessStatus
    phase: str
    capabilities: tuple[CapabilityObservation, ...] = ()
    resources: tuple[ResourceRecord, ...] = ()
    evidence: tuple[EvidenceRecord, ...] = ()
    cleanup_plan: tuple[str, ...] = ()
    cleanup_attempted: bool = False
    summary_reference: str = ""
    failure_reason: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True, indent=2)

    @classmethod
    def from_json(cls, value: str) -> HarnessManifest:
        payload = json.loads(value)
        return cls(
            schema_version=payload["schema_version"],
            run_id=payload["run_id"],
            marker=payload["marker"],
            build_id=payload["build_id"],
            repository=payload["repository"],
            scenario=payload["scenario"],
            command=payload["command"],
            operating_system=payload["operating_system"],
            status=HarnessStatus(payload["status"]),
            phase=payload["phase"],
            capabilities=tuple(
                CapabilityObservation(**item) for item in payload.get("capabilities", ())
            ),
            resources=tuple(ResourceRecord(**item) for item in payload.get("resources", ())),
            evidence=tuple(
                EvidenceRecord(
                    reference=item["reference"],
                    kind=EvidenceKind(item["kind"]),
                    status=EvidenceStatus(item["status"]),
                    operation_id=item["operation_id"],
                    phase=item["phase"],
                    details=tuple(tuple(pair) for pair in item.get("details", ())),
                )
                for item in payload.get("evidence", ())
            ),
            cleanup_plan=tuple(payload.get("cleanup_plan", ())),
            cleanup_attempted=payload.get("cleanup_attempted", False),
            summary_reference=payload.get("summary_reference", ""),
            failure_reason=payload.get("failure_reason", ""),
        )


class ManifestStore:
    def __init__(self, path: Path):
        self.path = path

    def create(self, manifest: HarnessManifest) -> HarnessManifest:
        if self.path.exists():
            raise FileExistsError(f"manifest already exists: {self.path}")
        self._write(manifest)
        return manifest

    def update(self, manifest: HarnessManifest) -> HarnessManifest:
        self._write(manifest)
        return manifest

    def read(self) -> HarnessManifest:
        return HarnessManifest.from_json(self.path.read_text(encoding="utf-8"))

    def _write(self, manifest: HarnessManifest) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(f"{self.path.suffix}.tmp")
        temporary.write_text(manifest.to_json() + "\n", encoding="utf-8", newline="\n")
        temporary.replace(self.path)


def new_identity(prefix: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return f"{prefix}-{timestamp}-{secrets.token_hex(5)}"


def marker_for(run_id: str) -> str:
    return f"<!-- tc083:{run_id} -->"


def marker_matches(value: str, marker: str) -> bool:
    return bool(marker.strip()) and marker in value


def _append_evidence(
    manifest: HarnessManifest,
    *,
    reference: str,
    kind: EvidenceKind,
    status: EvidenceStatus,
    operation_id: str,
    phase: str,
    details: Iterable[tuple[str, str]] = (),
) -> HarnessManifest:
    evidence = EvidenceRecord(
        reference=reference,
        kind=kind,
        status=status,
        operation_id=operation_id,
        phase=phase,
        details=tuple(details),
    )
    return replace(manifest, evidence=(*manifest.evidence, evidence), phase=phase)


def _append_resource(
    manifest: HarnessManifest,
    *,
    resource_type: str,
    identity: str,
    operation_id: str,
    marker: str | None = None,
    state: str = "created",
) -> HarnessManifest:
    resource = ResourceRecord(resource_type, identity, marker or manifest.marker, operation_id, state)
    return replace(manifest, resources=(*manifest.resources, resource))


class ExternalWriteUnknown(RuntimeError):
    """The write may have happened; a readback is required before retry."""


class FullFlowBoundary(Protocol):
    async def write(self, phase: str, operation_id: str, resource_type: str) -> ResourceRecord: ...

    async def read(self, operation_id: str) -> ResourceRecord | None: ...

    async def cleanup(self, resource: ResourceRecord) -> None: ...


@dataclass
class DeterministicFullFlowBoundary:
    marker: str
    resources: dict[str, ResourceRecord] = field(default_factory=dict)
    write_calls: list[str] = field(default_factory=list)
    cleanup_calls: list[str] = field(default_factory=list)
    lost_writes: set[str] = field(default_factory=set)
    stale_reads: set[str] = field(default_factory=set)
    cleanup_failures: set[str] = field(default_factory=set)
    _next_id: int = 1

    async def write(self, phase: str, operation_id: str, resource_type: str) -> ResourceRecord:
        self.write_calls.append(operation_id)
        existing = self.resources.get(operation_id)
        if existing is None:
            existing = ResourceRecord(
                resource_type=resource_type,
                identity=f"{resource_type}-{self._next_id}",
                marker=self.marker,
                operation_id=operation_id,
            )
            self._next_id += 1
            self.resources[operation_id] = existing
        if operation_id in self.lost_writes:
            self.lost_writes.remove(operation_id)
            raise ExternalWriteUnknown(f"write response lost for {operation_id}")
        return existing

    async def read(self, operation_id: str) -> ResourceRecord | None:
        if operation_id in self.stale_reads:
            return None
        return self.resources.get(operation_id)

    async def cleanup(self, resource: ResourceRecord) -> None:
        if not marker_matches(resource.marker, self.marker):
            raise ValueError("cleanup resource is outside this run marker")
        if resource.identity in self.cleanup_failures:
            raise ExternalWriteUnknown(f"cleanup response lost for {resource.identity}")
        self.cleanup_calls.append(resource.identity)
        self.resources[resource.operation_id] = replace(resource, state="closed")


@dataclass(frozen=True)
class HarnessRunInput:
    repository: str
    scenario: str = "deterministic whole-flow"
    build_id: str = "working-tree"
    command: str = "temporalio-codex-live --deterministic"
    brief_path: Path | None = None
    config_path: Path | None = None
    control_root: Path | None = None
    takeover_issue: int | None = None
    takeover_workspace: Path | None = None
    takeover_target_ref: str | None = None
    takeover_key: str | None = None
    artifact_roots: tuple[Path, ...] = ()
    required_checks: tuple[str, ...] = ()
    source_thread_id: str | None = None
    poll_interval_seconds: float = 2.0
    poll_timeout_seconds: float = 900.0
    phases: tuple[str, ...] = (
        "intake",
        "grill",
        "specs",
        "tickets",
        "codex-planning",
        "codex-implementation",
        "candidate",
        "tests",
        "review",
        "pull-request",
        "checks",
        "merge",
        "push",
        "origin",
        "summary",
    )


@dataclass(frozen=True)
class HarnessRunResult:
    status: HarnessStatus
    manifest: HarnessManifest
    manifest_path: str
    reason: str = ""


async def run_deterministic_harness(
    input: HarnessRunInput,
    *,
    manifest_path: Path,
    boundary: DeterministicFullFlowBoundary | None = None,
) -> HarnessRunResult:
    run_id = new_identity("tc083")
    marker = marker_for(run_id)
    store = ManifestStore(manifest_path)
    manifest = HarnessManifest(
        schema_version="tc083-harness/v1",
        run_id=run_id,
        marker=marker,
        build_id=input.build_id,
        repository=input.repository,
        scenario=input.scenario,
        command=input.command,
        operating_system=platform.platform(),
        status=HarnessStatus.BLOCKED,
        phase="bootstrap",
        cleanup_plan=("close only marker-matched resources", "read back every cleanup operation"),
    )
    store.create(manifest)
    boundary = boundary or DeterministicFullFlowBoundary(marker)
    boundary.marker = marker
    try:
        for index, phase in enumerate(input.phases):
            operation_id = f"{run_id}:{phase}"
            resource_type = "issue" if phase in {"specs", "tickets"} else phase
            try:
                resource = await boundary.write(phase, operation_id, resource_type)
            except ExternalWriteUnknown:
                resource = await boundary.read(operation_id)
                if resource is None:
                    manifest = replace(
                        manifest,
                        status=HarnessStatus.NOT_VERIFIED,
                        phase=phase,
                        failure_reason=f"{phase} write and readback are unknown",
                    )
                    store.update(manifest)
                    return HarnessRunResult(manifest.status, manifest, str(manifest_path), manifest.failure_reason)
                manifest = _append_evidence(
                    manifest,
                    reference=resource.identity,
                    kind=EvidenceKind.DETERMINISTIC,
                    status=EvidenceStatus.VERIFIED,
                    operation_id=operation_id,
                    phase=phase,
                    details=(("adopted_after_lost_response", "true"),),
                )
            else:
                manifest = _append_evidence(
                    manifest,
                    reference=resource.identity,
                    kind=EvidenceKind.DETERMINISTIC,
                    status=EvidenceStatus.VERIFIED,
                    operation_id=operation_id,
                    phase=phase,
                )
            manifest = _append_resource(
                manifest,
                resource_type=resource.resource_type,
                identity=resource.identity,
                operation_id=operation_id,
                marker=resource.marker,
            )
            manifest = replace(manifest, phase=f"{phase}:{index + 1}/{len(input.phases)}")
            store.update(manifest)

        for resource in manifest.resources:
            try:
                await boundary.cleanup(resource)
            except ExternalWriteUnknown:
                manifest = replace(
                    manifest,
                    status=HarnessStatus.CLEANUP_PENDING,
                    cleanup_attempted=True,
                    phase="cleanup",
                    failure_reason=f"cleanup readback is unknown for {resource.identity}",
                )
                store.update(manifest)
                return HarnessRunResult(manifest.status, manifest, str(manifest_path), manifest.failure_reason)
            readback = await boundary.read(resource.operation_id)
            if readback is None or readback.state != "closed":
                manifest = replace(
                    manifest,
                    status=HarnessStatus.CLEANUP_PENDING,
                    cleanup_attempted=True,
                    phase="cleanup",
                    failure_reason=f"cleanup readback did not close {resource.identity}",
                )
                store.update(manifest)
                return HarnessRunResult(manifest.status, manifest, str(manifest_path), manifest.failure_reason)
            manifest = replace(
                manifest,
                resources=tuple(
                    replace(item, state="closed") if item.operation_id == resource.operation_id else item
                    for item in manifest.resources
                ),
                phase="cleanup",
            )
            store.update(manifest)
        manifest = replace(
            manifest,
            status=HarnessStatus.VERIFIED,
            cleanup_attempted=True,
            phase="complete",
        )
        store.update(manifest)
        return HarnessRunResult(manifest.status, manifest, str(manifest_path))
    except (OSError, ValueError, RuntimeError) as error:
        manifest = replace(
            manifest,
            status=HarnessStatus.FAILED,
            phase=manifest.phase,
            failure_reason=f"{type(error).__name__}: {error}",
        )
        store.update(manifest)
        return HarnessRunResult(manifest.status, manifest, str(manifest_path), manifest.failure_reason)


async def retry_cleanup(
    manifest_path: Path,
    *,
    boundary: DeterministicFullFlowBoundary,
) -> HarnessRunResult:
    """Retry only persisted cleanup operations after a cleanup-only failure."""
    store = ManifestStore(manifest_path)
    manifest = store.read()
    if manifest.status is not HarnessStatus.CLEANUP_PENDING:
        return HarnessRunResult(manifest.status, manifest, str(manifest_path), manifest.failure_reason)
    boundary.marker = manifest.marker
    for resource in manifest.resources:
        if resource.state == "closed":
            continue
        try:
            await boundary.cleanup(resource)
        except ExternalWriteUnknown:
            manifest = replace(
                manifest,
                cleanup_attempted=True,
                phase="cleanup",
                failure_reason=f"cleanup readback is unknown for {resource.identity}",
            )
            store.update(manifest)
            return HarnessRunResult(manifest.status, manifest, str(manifest_path), manifest.failure_reason)
        readback = await boundary.read(resource.operation_id)
        if readback is None or readback.state != "closed":
            manifest = replace(
                manifest,
                cleanup_attempted=True,
                phase="cleanup",
                failure_reason=f"cleanup readback did not close {resource.identity}",
            )
            store.update(manifest)
            return HarnessRunResult(manifest.status, manifest, str(manifest_path), manifest.failure_reason)
        manifest = replace(
            manifest,
            resources=tuple(
                replace(item, state="closed") if item.operation_id == resource.operation_id else item
                for item in manifest.resources
            ),
            phase="cleanup",
        )
        store.update(manifest)
    manifest = replace(manifest, status=HarnessStatus.VERIFIED, cleanup_attempted=True, phase="complete", failure_reason="")
    store.update(manifest)
    return HarnessRunResult(manifest.status, manifest, str(manifest_path))


def validate_live_manifest(manifest: HarnessManifest) -> tuple[str, ...]:
    errors: list[str] = []
    if manifest.status is HarnessStatus.VERIFIED:
        if not manifest.cleanup_attempted:
            errors.append("verified live run must have cleanup evidence")
        if not manifest.resources:
            errors.append("verified live run must have resources")
    for resource in manifest.resources:
        if not marker_matches(resource.marker, manifest.marker):
            errors.append(f"resource is outside marker scope: {resource.identity}")
    operations = [e.operation_id for e in manifest.evidence]
    if len(operations) != len(set(operations)):
        errors.append("duplicate evidence operation identity")
    if manifest.status is HarnessStatus.VERIFIED:
        if not manifest.evidence:
            errors.append("verified live run must have evidence")
        if any(e.kind is not EvidenceKind.LIVE for e in manifest.evidence):
            errors.append("installed, local or deterministic evidence cannot qualify verified live status")
        if any(e.status is not EvidenceStatus.VERIFIED for e in manifest.evidence):
            errors.append("verified live run contains non-verified evidence")
    return tuple(errors)


def _installed_skill_root() -> Path | None:
    configured = os.environ.get("IMPLEMENT_NEEDS_SKILL_ROOT")
    candidates = [
        Path(configured) if configured else None,
        Path.home() / ".codex" / "skills" / "implement-needs",
        Path.home() / ".agents" / "skills" / "implement-needs",
    ]
    for candidate in candidates:
        if candidate is not None and (candidate / "SKILL.md").is_file() and (candidate / "scripts" / "spec_runner_handoff.py").is_file():
            return candidate
    return None


def _authorized_config_repository(input: HarnessRunInput) -> tuple[bool, str]:
    if input.config_path is None:
        return False, "--config is required"
    try:
        document = json.loads(input.config_path.expanduser().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        return False, f"Runner config is unreadable: {error}"
    github = document.get("github") if isinstance(document, dict) else None
    repository = github.get("repository") if isinstance(github, dict) else None
    if repository != input.repository:
        return False, f"Runner config targets {repository!r}, expected {input.repository!r}"
    return True, "Runner config repository matches the authorized repository"


def _capabilities(input: HarnessRunInput) -> tuple[CapabilityObservation, ...]:
    gh = shutil.which("gh")
    runner = shutil.which("spec-runner")
    skill_root = _installed_skill_root()
    repository_matches, repository_detail = _authorized_config_repository(input)
    return (
        CapabilityObservation("gh", gh is not None, gh or "gh CLI unavailable"),
        CapabilityObservation("spec-runner", runner is not None, runner or "spec-runner unavailable"),
        CapabilityObservation("installed-implement-needs", skill_root is not None, "installed handoff" if skill_root else "installed Skill not found"),
        CapabilityObservation("authorized-brief", bool(input.brief_path and input.brief_path.expanduser().is_file()), "brief file" if input.brief_path and input.brief_path.expanduser().is_file() else "--brief is required"),
        CapabilityObservation("authorized-runner-config", bool(input.config_path and input.config_path.expanduser().is_file()), "Runner config" if input.config_path and input.config_path.expanduser().is_file() else "--config is required"),
        CapabilityObservation("runner-control-root", input.control_root is not None, "explicit control root" if input.control_root else "--control-root is required"),
        CapabilityObservation("authorized-repository", repository_matches, repository_detail),
    )


def _live_manifest(input: HarnessRunInput, run_id: str, marker: str) -> HarnessManifest:
    mode = "takeover " if input.takeover_issue is not None else ""
    return HarnessManifest(
        schema_version="tc083-harness/v1",
        run_id=run_id,
        marker=marker,
        build_id=input.build_id,
        repository=input.repository,
        scenario=f"live installed implement-needs {mode}{input.scenario}",
        command=input.command,
        operating_system=platform.platform(),
        status=HarnessStatus.NOT_VERIFIED,
        phase="bootstrap",
        capabilities=_capabilities(input),
        cleanup_plan=("close or remove only marker-matched resources", "read back cleanup"),
    )


def _invoke_runner(
    skill_root: Path,
    *,
    operation: str,
    control_root: Path,
    run_id: str | None = None,
    brief_path: Path | None = None,
    config_path: Path | None = None,
    launch_key: str | None = None,
) -> dict[str, Any]:
    script = skill_root / "scripts" / "spec_runner_handoff.py"
    command = [sys.executable, str(script), operation, "--control-root", str(control_root)]
    if operation == "launch":
        if brief_path is None or config_path is None or launch_key is None:
            raise ValueError("launch requires brief, config and launch key")
        command.extend(["--brief", str(brief_path), "--config", str(config_path), "--launch-key", launch_key])
    elif operation == "status":
        if run_id is None:
            raise ValueError("status requires run id")
        command.extend(["--run-id", run_id])
    completed = subprocess.run(
        command,
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="strict",
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("installed implement-needs returned invalid JSON") from error
    payload["process_exit_code"] = completed.returncode
    payload["operation"] = operation
    if completed.stderr:
        payload["stderr"] = completed.stderr
    return payload


def _run_handoff(
    skill_root: Path,
    *,
    input: HarnessRunInput,
    run_id: str,
    control_root: Path,
) -> dict[str, Any]:
    if input.brief_path is None or input.config_path is None:
        raise ValueError("live harness requires an authorized brief and Runner config")
    brief = input.brief_path.expanduser().resolve()
    marker = marker_for(run_id)
    brief_text = brief.read_text(encoding="utf-8")
    if marker not in brief_text:
        raise ValueError("authorized brief must contain the live run marker")
    return _invoke_runner(
        skill_root,
        operation="launch",
        control_root=control_root,
        brief_path=brief,
        config_path=input.config_path.expanduser().resolve(),
        launch_key=run_id,
    )


def _runner_prefix() -> list[str]:
    executable = shutil.which("spec-runner")
    return [executable] if executable else [sys.executable, "-m", "spec_runner.cli"]


def _run_takeover_handoff(
    skill_root: Path,
    *,
    input: HarnessRunInput,
    run_id: str,
    control_root: Path,
    discovery_path: Path,
) -> dict[str, Any]:
    if input.brief_path is None or input.config_path is None:
        raise ValueError("live takeover requires an authorized brief and Runner config")
    if input.takeover_issue is None or input.takeover_workspace is None or not input.takeover_target_ref or not input.takeover_key:
        raise ValueError("live takeover requires issue, workspace, target ref and takeover key")
    marker = marker_for(run_id)
    if marker not in input.brief_path.expanduser().read_text(encoding="utf-8"):
        raise ValueError("authorized brief must contain the live run marker")
    discovery_path.parent.mkdir(parents=True, exist_ok=True)
    roots = input.artifact_roots or (control_root,)
    discover = [
        *_runner_prefix(), "takeover", "discover", "--repository", input.repository,
        "--issue", str(input.takeover_issue), "--workspace", str(input.takeover_workspace.expanduser().resolve()),
        "--target-ref", input.takeover_target_ref, "--control-root", str(control_root),
        "--takeover-key", input.takeover_key, "--output", str(discovery_path),
    ]
    for root in roots:
        discover.extend(["--artifact-root", str(root.expanduser().resolve())])
    for check in input.required_checks:
        discover.extend(["--required-check", check])
    if input.source_thread_id:
        discover.extend(["--thread-id", input.source_thread_id])
    discovery_result = subprocess.run(
        discover, capture_output=True, check=False, encoding="utf-8", errors="strict",
    )
    if discovery_result.returncode:
        raise RuntimeError(discovery_result.stderr.strip() or "takeover discovery failed")
    discovery = _read_json_object(discovery_path)
    snapshot = discovery.get("snapshot")
    if not isinstance(snapshot, dict) or not snapshot.get("digest"):
        raise ValueError("takeover discovery did not return a digest-bound snapshot")
    apply = [
        *_runner_prefix(), "takeover", "apply", "--discovery", str(discovery_path),
        "--control-root", str(control_root), "--takeover-key", input.takeover_key,
        "--brief", str(input.brief_path.expanduser().resolve()),
        "--config", str(input.config_path.expanduser().resolve()), "--launch-key", run_id,
    ]
    applied = subprocess.run(
        apply, capture_output=True, check=False, encoding="utf-8", errors="strict",
    )
    try:
        payload = json.loads(applied.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("takeover apply returned invalid JSON") from error
    if not isinstance(payload, dict):
        raise TypeError("takeover apply returned a non-object")
    if applied.returncode:
        raise RuntimeError(applied.stderr.strip() or "takeover apply failed")
    observed_run_id = _durable_run_id(payload)
    if observed_run_id:
        payload["run_id"] = observed_run_id
    payload["discovery_snapshot_digest"] = snapshot["digest"]
    return payload


def _durable_run_id(payload: dict[str, Any]) -> str | None:
    direct = payload.get("run_id")
    if isinstance(direct, str) and direct:
        return direct
    for key in ("runner", "run"):
        value = payload.get(key)
        if not isinstance(value, dict):
            continue
        nested = value.get("run_id")
        if isinstance(nested, str) and nested:
            return nested
        nested_run = value.get("run")
        if isinstance(nested_run, dict) and isinstance(nested_run.get("run_id"), str) and nested_run["run_id"]:
            return nested_run["run_id"]
    return None


def _runner_state(payload: dict[str, Any]) -> str:
    run = payload.get("run")
    if isinstance(run, dict) and isinstance(run.get("state"), str):
        return run["state"]
    return str(payload.get("status") or payload.get("state") or "")


def _runner_artifact_directory(control_root: Path, config_path: Path, runner_run_id: str) -> Path:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    artifact_root = config.get("artifact_root")
    if not isinstance(artifact_root, str) or not artifact_root:
        raise ValueError("Runner config artifact_root must be a relative path")
    relative = Path(artifact_root)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Runner config artifact_root must stay below control root")
    root = (control_root / relative).resolve()
    control_root = control_root.resolve()
    if root != control_root and control_root not in root.parents:
        raise ValueError("Runner artifact root escapes control root")
    return root / runner_run_id


def _runner_github_receipt_path(control_root: Path, config_path: Path) -> Path:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    github = config.get("github") if isinstance(config, dict) else None
    receipt_root = github.get("receipt_root") if isinstance(github, dict) else None
    if not isinstance(receipt_root, str) or not receipt_root:
        raise ValueError("Runner config github.receipt_root is required")
    relative = Path(receipt_root)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Runner config github.receipt_root must stay below control root")
    root = (control_root / relative).resolve()
    control_root = control_root.resolve()
    if root != control_root and control_root not in root.parents:
        raise ValueError("Runner GitHub receipt root escapes control root")
    return root / ".spec-runner-github-receipts.json"


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"{path.name} must contain a JSON object")
    return payload


def _write_live_summary(directory: Path, manifest: HarnessManifest, spec_keys: tuple[str, ...]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "final-summary.json"
    summary = {
        "schema_version": "tc083-live-summary/v1",
        "run_id": manifest.run_id,
        "marker": manifest.marker,
        "runner_repository": manifest.repository,
        "spec_keys": list(spec_keys),
        "evidence_refs": [item.reference for item in manifest.evidence],
        "resource_refs": [item.identity for item in manifest.resources],
        "status": HarnessStatus.VERIFIED,
        "limitations": [],
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8", newline="\n")
    temporary.replace(path)
    readback = _read_json_object(path)
    if readback.get("marker") != manifest.marker or readback.get("run_id") != manifest.run_id:
        raise ValueError("final summary readback does not match the live run")
    return path


def _worker_has_identity(worker: object, role: str) -> bool:
    if not isinstance(worker, dict):
        return False
    identity = str(worker.get("worker_id") or "")
    role_marker = f":{role}"
    return (
        worker.get("backend_kind") == "codex_sdk"
        and (identity.endswith(role_marker) or f"{role_marker}:" in identity)
        and bool(worker.get("external_thread_id"))
        and bool(worker.get("external_turn_id"))
        and worker.get("state") not in {"failed", "cancelled"}
    )


def _worker_artifact_candidates(artifact_directory: Path, role: str) -> tuple[Path, ...]:
    patterns = {
        "codex_planning": "codex_planning-*.json",
        "codex_implementation": "implementation-*.json",
        "codex_review": "review-worker-*.json",
    }
    return tuple(sorted(artifact_directory.glob(patterns[role])))


def _validate_worker_artifacts(artifact_directory: Path, workers: list[object]) -> tuple[str, ...]:
    errors: list[str] = []
    for role in ("codex_planning", "codex_implementation", "codex_review"):
        candidates = _worker_artifact_candidates(artifact_directory, role)
        matching = []
        for path in candidates:
            try:
                payload = _read_json_object(path)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
                continue
            if payload.get("status") == "completed" and payload.get("error") is None:
                matching.append(payload)
        if not matching:
            errors.append(f"missing completed {role} artifact")
            continue
        if not any(isinstance(payload.get("thread_id"), str) and payload.get("thread_id") for payload in matching):
            errors.append(f"{role} artifact lacks thread identity")
        if not any(isinstance(payload.get("turn_id"), str) and payload.get("turn_id") for payload in matching):
            errors.append(f"{role} artifact lacks turn identity")
        if not any(payload.get("approval_mode") == CODEX_APPROVAL_MODE for payload in matching):
            errors.append(f"{role} artifact lacks the required approval mode")
        if not any(payload.get("sdk_version") == CODEX_SDK_VERSION for payload in matching):
            errors.append(f"{role} artifact lacks the required SDK version")
        if not any(
            any(
                isinstance(worker, dict)
                and worker.get("external_thread_id") == payload.get("thread_id")
                and worker.get("external_turn_id") == payload.get("turn_id")
                for worker in workers
            )
            and payload.get("approval_mode") == CODEX_APPROVAL_MODE
            and payload.get("sdk_version") == CODEX_SDK_VERSION
            for payload in matching
        ):
            errors.append(f"{role} artifact lacks matching worker identity")
    return tuple(errors)


def _validate_candidate_receipt(candidate: dict[str, Any], spec_key: str) -> tuple[str, ...]:
    errors: list[str] = []
    if candidate.get("schema_version") != "spec-runner-candidate-receipt/v1":
        errors.append(f"candidate {spec_key} has no trusted receipt schema")
    if not isinstance(candidate.get("acceptance_version"), str) or not candidate["acceptance_version"].strip():
        errors.append(f"candidate {spec_key} has no acceptance version")
    write_scope = candidate.get("write_scope")
    if not isinstance(write_scope, dict):
        errors.append(f"candidate {spec_key} has no write-scope evidence")
    else:
        for field in ("allowed_paths", "changed_paths"):
            values = write_scope.get(field)
            if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
                errors.append(f"candidate {spec_key} has invalid write-scope {field}")
    checks = candidate.get("checks")
    if not isinstance(checks, list) or not checks:
        errors.append(f"candidate {spec_key} has no acceptance checks")
    else:
        for check in checks:
            if (
                not isinstance(check, dict)
                or not isinstance(check.get("command"), list)
                or not check["command"]
                or any(not isinstance(value, str) or not value for value in check["command"])
                or not isinstance(check.get("acceptance"), list)
                or not check["acceptance"]
                or check.get("passed") is not True
            ):
                errors.append(f"candidate {spec_key} has an unverified acceptance check")
                break
    return tuple(errors)


def _github_issue_readback(repository: str, number: int) -> dict[str, Any]:
    completed = subprocess.run(
        ["gh", "issue", "view", str(number), "--repo", repository, "--json", "number,title,body,state,labels,url"],
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="strict",
    )
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or f"GitHub Issue readback failed for #{number}")
    payload = json.loads(completed.stdout)
    if not isinstance(payload, dict):
        raise TypeError(f"GitHub Issue readback for #{number} is not an object")
    return payload


def validate_runner_evidence(
    status: dict[str, Any],
    *,
    artifact_directory: Path,
    runner_run_id: str,
    repository: str,
    marker: str,
    issue_reader: Any | None = None,
    github_receipt_path: Path | None = None,
) -> tuple[tuple[str, ...], tuple[EvidenceRecord, ...], tuple[ResourceRecord, ...], tuple[str, ...]]:
    errors: list[str] = []
    evidence: list[EvidenceRecord] = []
    resources: list[ResourceRecord] = []
    run = status.get("run") if isinstance(status.get("run"), dict) else {}
    if run.get("run_id") != runner_run_id:
        errors.append("Runner status run_id does not match the launched run")
    if _runner_state(status) != "completed":
        errors.append(f"Runner terminal state is {_runner_state(status) or 'unknown'}")
    workers = status.get("workers")
    if not isinstance(workers, list):
        workers = []
        errors.append("Runner status has no worker readback")
    for role in ("codex_planning", "codex_implementation", "codex_review"):
        if not any(_worker_has_identity(worker, role) for worker in workers):
            errors.append(f"missing live {role} worker identity")
    errors.extend(_validate_worker_artifacts(artifact_directory, workers))

    try:
        spec_plan = _read_json_object(artifact_directory / "spec-plan.json")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        errors.append(f"SPEC plan readback failed: {error}")
        spec_plan = {}
    specs = spec_plan.get("specs")
    if spec_plan.get("outcome") != "planned" or not isinstance(specs, list) or not specs:
        errors.append("spec-plan.json is incomplete")
        specs = []
    spec_keys: list[str] = []
    for spec in specs:
        if not isinstance(spec, dict) or not isinstance(spec.get("key"), str) or not spec["key"]:
            errors.append("SPEC plan contains an invalid identity")
            continue
        spec_key = spec["key"]
        spec_keys.append(spec_key)
        try:
            ticket_plan = _read_json_object(artifact_directory / f"ticket-plan-{spec_key}.json")
            candidate = _read_json_object(artifact_directory / f"candidate-{spec_key}.json")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
            errors.append(f"SPEC {spec_key} planning or candidate readback failed: {error}")
            continue
        if ticket_plan.get("outcome") != "planned" or ticket_plan.get("spec_key") != spec_key:
            errors.append(f"ticket plan {spec_key} is incomplete")
        candidate_sha = candidate.get("candidate_sha")
        if candidate.get("outcome") != "verified" or not isinstance(candidate_sha, str) or len(candidate_sha) != 40:
            errors.append(f"candidate {spec_key} is not verified")
            continue
        errors.extend(_validate_candidate_receipt(candidate, spec_key))
        try:
            review = _read_json_object(artifact_directory / f"review-{spec_key}-{candidate_sha[:12]}.json")
            delivery = _read_json_object(artifact_directory / f"delivery-{spec_key}.json")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
            errors.append(f"SPEC {spec_key} delivery readback failed: {error}")
            continue
        if (
            review.get("approved") is not True
            or review.get("candidate_sha") != candidate_sha
            or not isinstance(review.get("review_digest"), str)
            or not review["review_digest"].strip()
        ):
            errors.append(f"review {spec_key} does not approve its candidate")
        checks = delivery.get("checks")
        pr = delivery.get("pr")
        merge = delivery.get("merge")
        if delivery.get("run_id") != runner_run_id or delivery.get("spec_key") != spec_key:
            errors.append(f"delivery {spec_key} has the wrong Runner identity")
        delivery_valid = delivery.get("state") in {"spec_completed", "github_completed"}
        if not delivery_valid:
            errors.append(f"delivery {spec_key} is not complete")
        checks_valid = isinstance(checks, dict) and checks.get("candidate_sha") == candidate_sha and checks.get("ready") is True
        if not checks_valid:
            errors.append(f"checks {spec_key} do not qualify the exact candidate")
        pr_valid = (
            isinstance(pr, dict)
            and pr.get("candidate_sha") == candidate_sha
            and isinstance(pr.get("number"), int)
            and not isinstance(pr.get("number"), bool)
            and isinstance(pr.get("url"), str)
            and bool(pr["url"].strip())
        )
        if not pr_valid:
            errors.append(f"PR {spec_key} is not tied to the candidate")
        merge_valid = (
            isinstance(merge, dict)
            and merge.get("merged") is True
            and isinstance(merge.get("sha"), str)
            and len(merge["sha"]) == 40
        )
        if not merge_valid:
            errors.append(f"merge {spec_key} has no confirmed merge SHA")
        origin_valid = merge_valid and isinstance(merge.get("base_sync"), dict) and merge["base_sync"].get("synced_sha") == merge["sha"]
        if merge_valid and not origin_valid:
            errors.append(f"origin readback {spec_key} does not match the merge SHA")
        closure = delivery.get("issue_closure")
        closure_valid = (
            isinstance(closure, dict)
            and closure.get("complete") is True
            and isinstance(closure.get("issues"), list)
            and bool(closure["issues"])
            and all(
                isinstance(item, dict)
                and isinstance(item.get("number"), int)
                and item.get("state") == "closed"
                for item in closure["issues"]
            )
        )
        if not closure_valid:
            errors.append(f"ticket Issue closure {spec_key} is incomplete")
        cleanup = delivery.get("cleanup")
        cleanup_valid = isinstance(cleanup, dict) and cleanup.get("outcome") == "cleaned"
        if not cleanup_valid:
            errors.append(f"cleanup receipt {spec_key} is incomplete")
        if delivery_valid and checks_valid and pr_valid and merge_valid and origin_valid and closure_valid and cleanup_valid:
            evidence.append(EvidenceRecord(
                reference=f"runner:{runner_run_id}:{spec_key}",
                kind=EvidenceKind.LIVE,
                status=EvidenceStatus.VERIFIED,
                operation_id=f"{runner_run_id}:delivery:{spec_key}",
                phase="delivery",
                details=(
                    ("candidate_sha", str(candidate_sha)),
                    ("merge_sha", str(merge.get("sha"))),
                    ("origin_sha", str(merge["base_sync"].get("synced_sha"))),
                    ("cleanup", str(cleanup.get("outcome"))),
                ),
            ))
            if isinstance(pr, dict) and isinstance(pr.get("number"), int):
                resources.append(ResourceRecord("pull-request", str(pr["number"]), marker, f"{runner_run_id}:pr:{spec_key}"))

    receipt_path = github_receipt_path or artifact_directory.parent.parent / "github-receipts" / ".spec-runner-github-receipts.json"
    try:
        receipts = _read_json_object(receipt_path)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        errors.append(f"GitHub Issue publication receipt readback failed: {error}")
        receipts = {}
    issue_numbers: list[int] = []
    for receipt in receipts.values():
        if not isinstance(receipt, dict) or receipt.get("repository") != repository:
            continue
        operation_id = str(receipt.get("operation_id") or "")
        issue_items = receipt.get("issues")
        if not isinstance(issue_items, list):
            errors.append("GitHub Issue publication receipt has no issue list")
            continue
        issue_markers = [
            str(issue.get("marker") or "")
            for issue in issue_items
            if isinstance(issue, dict)
        ]
        if runner_run_id not in operation_id and not any(runner_run_id in marker_value for marker_value in issue_markers):
            continue
        if receipt.get("complete") is not True:
            errors.append("GitHub Issue publication receipt is incomplete")
        relation_evidence = receipt.get("relation_evidence")
        if not isinstance(relation_evidence, dict) or relation_evidence.get("native") is not True:
            errors.append("GitHub Issue relationships lack native relation evidence")
        for issue in issue_items:
            if isinstance(issue, dict) and isinstance(issue.get("number"), int):
                issue_numbers.append(issue["number"])
                resources.append(ResourceRecord("issue", str(issue["number"]), marker, f"{runner_run_id}:issue:{issue['number']}"))
    if not issue_numbers:
        errors.append("no GitHub SPEC or ticket Issue identities were read back")
    elif issue_reader is not None:
        for number in sorted(set(issue_numbers)):
            try:
                issue = issue_reader(repository, number)
            except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as error:
                errors.append(f"GitHub Issue #{number} readback failed: {error}")
                continue
            body = str(issue.get("body") or "")
            if f"spec-runner-run:{runner_run_id}" not in body:
                errors.append(f"GitHub Issue #{number} is missing the Runner run marker")
            if not str(issue.get("title") or "").strip() or not str(issue.get("url") or "").strip():
                errors.append(f"GitHub Issue #{number} has incomplete identity readback")
            if not isinstance(issue.get("labels"), list) or not issue["labels"]:
                errors.append(f"GitHub Issue #{number} has no label readback")

    return tuple(errors), tuple(evidence), tuple(resources), tuple(spec_keys)


def validate_takeover_evidence(
    status: dict[str, Any],
    *,
    artifact_directory: Path,
    runner_run_id: str,
    repository: str,
    marker: str,
    discovery_snapshot_digest: str,
    issue_reader: Any | None = None,
) -> tuple[tuple[str, ...], tuple[EvidenceRecord, ...], tuple[ResourceRecord, ...], tuple[str, ...]]:
    """Validate a live run that adopted an existing Issue graph.

    Takeover intentionally allows planning or worker receipts to be absent for
    stages proven complete by the discovery snapshot. Its durable plan,
    ticket identities, current delivery receipts, and readbacks remain required.
    """
    errors: list[str] = []
    evidence: list[EvidenceRecord] = []
    resources: list[ResourceRecord] = []
    run = status.get("run") if isinstance(status.get("run"), dict) else {}
    if run.get("run_id") != runner_run_id:
        errors.append("takeover Runner status run_id does not match the launched run")
    if _runner_state(status) != "completed":
        errors.append(f"takeover Runner terminal state is {_runner_state(status) or 'unknown'}")
    try:
        spec_plan = _read_json_object(artifact_directory / "spec-plan.json")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        errors.append(f"takeover SPEC plan readback failed: {error}")
        spec_plan = {}
    if spec_plan.get("outcome") != "planned":
        errors.append("takeover SPEC plan is incomplete")
    if spec_plan.get("takeover_snapshot_digest") != discovery_snapshot_digest:
        errors.append("takeover SPEC plan is bound to a different discovery snapshot")
    specs = spec_plan.get("specs")
    if not isinstance(specs, list) or any(not isinstance(item, dict) or not item.get("key") for item in specs):
        errors.append("takeover SPEC plan has no complete keyed graph")
        specs = []
    spec_keys = tuple(str(item["key"]) for item in specs)
    for spec in specs:
        spec_key = str(spec["key"])
        try:
            ticket_plan = _read_json_object(artifact_directory / f"ticket-plan-{spec_key}.json")
            candidate = _read_json_object(artifact_directory / f"candidate-{spec_key}.json")
            delivery = _read_json_object(artifact_directory / f"delivery-{spec_key}.json")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
            errors.append(f"takeover {spec_key} artifact readback failed: {error}")
            continue
        if ticket_plan.get("outcome") != "planned" or ticket_plan.get("spec_key") != spec_key:
            errors.append(f"takeover ticket plan {spec_key} is incomplete")
        if ticket_plan.get("takeover_snapshot_digest") != discovery_snapshot_digest:
            errors.append(f"takeover ticket plan {spec_key} is bound to a different discovery snapshot")
        identities = ticket_plan.get("github_issues")
        if not isinstance(identities, list) or len(identities) != len(ticket_plan.get("tickets", ())) + 1:
            errors.append(f"takeover Issue identities {spec_key} are incomplete")
            identities = []
        for identity in identities:
            if not isinstance(identity, dict) or not isinstance(identity.get("number"), int) or not identity.get("marker"):
                errors.append(f"takeover Issue identity {spec_key} is invalid")
                continue
            resources.append(ResourceRecord("issue", str(identity["number"]), marker, f"{runner_run_id}:issue:{identity['number']}"))
            if issue_reader is not None:
                try:
                    issue = issue_reader(repository, int(identity["number"]))
                except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as error:
                    errors.append(f"takeover Issue #{identity['number']} readback failed: {error}")
                    continue
                if str(identity["marker"]) not in str(issue.get("body") or ""):
                    errors.append(f"takeover Issue #{identity['number']} marker readback does not match")
        candidate_sha = candidate.get("candidate_sha")
        if candidate.get("outcome") != "verified" or not isinstance(candidate_sha, str) or len(candidate_sha) != 40:
            errors.append(f"takeover candidate {spec_key} is not verified")
            continue
        errors.extend(_validate_candidate_receipt(candidate, spec_key))
        review_path = artifact_directory / f"review-{spec_key}-{candidate_sha[:12]}.json"
        try:
            review = _read_json_object(review_path)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
            errors.append(f"takeover review {spec_key} readback failed: {error}")
            continue
        if (
            review.get("approved") is not True
            or review.get("candidate_sha") != candidate_sha
            or not str(review.get("review_digest") or "")
        ):
            errors.append(f"takeover review {spec_key} does not approve its candidate")
        if delivery.get("run_id") != runner_run_id or delivery.get("spec_key") != spec_key:
            errors.append(f"takeover delivery {spec_key} has the wrong Runner identity")
        if delivery.get("discovery_snapshot_digest") != discovery_snapshot_digest:
            errors.append(f"takeover delivery {spec_key} is bound to a different discovery snapshot")
        if delivery.get("candidate") != candidate:
            errors.append(f"takeover delivery {spec_key} does not match its candidate receipt")
        delivery_review = delivery.get("review")
        if not isinstance(delivery_review, dict) or any(delivery_review.get(key) != review.get(key) for key in ("approved", "candidate_sha", "review_digest")):
            errors.append(f"takeover delivery {spec_key} does not match its review receipt")
        checks = delivery.get("checks")
        if not isinstance(checks, dict) or checks.get("candidate_sha") != candidate_sha or checks.get("ready") is not True:
            errors.append(f"takeover checks {spec_key} do not qualify the candidate")
        pr = delivery.get("pr")
        if not isinstance(pr, dict) or not isinstance(pr.get("number"), int) or pr.get("candidate_sha") != candidate_sha:
            errors.append(f"takeover PR {spec_key} is not tied to the candidate")
        else:
            resources.append(ResourceRecord("pull-request", str(pr["number"]), marker, f"{runner_run_id}:pr:{spec_key}"))
        merge = delivery.get("merge")
        if not isinstance(merge, dict) or merge.get("merged") is not True or not isinstance(merge.get("sha"), str) or len(merge["sha"]) != 40:
            errors.append(f"takeover merge {spec_key} has no confirmed merge SHA")
        elif not isinstance(merge.get("base_sync"), dict) or merge["base_sync"].get("synced_sha") != merge["sha"]:
            errors.append(f"takeover origin readback {spec_key} does not match the merge SHA")
        closure = delivery.get("issue_closure")
        if not isinstance(closure, dict) or closure.get("complete") is not True or not isinstance(closure.get("issues"), list) or any(item.get("state") != "closed" for item in closure["issues"] if isinstance(item, dict)):
            errors.append(f"takeover Issue closure {spec_key} is incomplete")
        cleanup = delivery.get("cleanup")
        if not isinstance(cleanup, dict) or cleanup.get("outcome") != "cleaned":
            errors.append(f"takeover cleanup {spec_key} is incomplete")
        if not any(error.startswith(f"takeover candidate {spec_key}") for error in errors):
            evidence.append(EvidenceRecord(
                reference=f"takeover:{runner_run_id}:{spec_key}", kind=EvidenceKind.LIVE,
                status=EvidenceStatus.VERIFIED, operation_id=f"{runner_run_id}:takeover:{spec_key}",
                phase="takeover-delivery", details=(("candidate_sha", candidate_sha),),
            ))
    return tuple(errors), tuple(evidence), tuple(resources), spec_keys


async def run_live_harness(
    input: HarnessRunInput,
    *,
    manifest_path: Path,
    opt_in: bool | None = None,
) -> HarnessRunResult:
    run_id = new_identity("tc083-live")
    marker = marker_for(run_id)
    store = ManifestStore(manifest_path)
    manifest = _live_manifest(input, run_id, marker)
    store.create(manifest)
    enabled = opt_in if opt_in is not None else os.environ.get("TC083_RUN_LIVE") == "1"
    if not enabled:
        reason = "set TC083_RUN_LIVE=1 to enable live full-flow qualification"
        manifest = replace(manifest, failure_reason=reason)
        store.update(manifest)
        return HarnessRunResult(HarnessStatus.NOT_VERIFIED, manifest, str(manifest_path), reason)
    skill_root = _installed_skill_root()
    missing = [item.name for item in manifest.capabilities if not item.available]
    if skill_root is None or missing:
        reason = "required live capabilities unavailable: " + ", ".join(missing or ["installed-implement-needs"])
        manifest = replace(manifest, failure_reason=reason)
        store.update(manifest)
        return HarnessRunResult(HarnessStatus.NOT_VERIFIED, manifest, str(manifest_path), reason)
    if input.control_root is None:
        reason = "live harness requires an explicit Runner control root"
        manifest = replace(manifest, failure_reason=reason)
        store.update(manifest)
        return HarnessRunResult(HarnessStatus.NOT_VERIFIED, manifest, str(manifest_path), reason)
    control_root = input.control_root.expanduser().resolve()
    try:
        discovery_path = manifest_path.parent / f"{run_id}-takeover-discovery.json"
        takeover_mode = input.takeover_issue is not None
        payload = await asyncio.to_thread(
            _run_takeover_handoff if takeover_mode else _run_handoff,
            skill_root,
            input=input,
            run_id=run_id,
            control_root=control_root,
            **({"discovery_path": discovery_path} if takeover_mode else {}),
        )
        observed_run_id = _durable_run_id(payload)
        if not observed_run_id:
            reason = "installed implement-needs did not return a durable run_id"
            manifest = replace(manifest, phase="handoff", failure_reason=reason)
            store.update(manifest)
            return HarnessRunResult(HarnessStatus.BLOCKED, manifest, str(manifest_path), reason)
        status = str(payload.get("status", ""))
        manifest = _append_evidence(
            manifest,
            reference=f"runner:{observed_run_id}",
            kind=EvidenceKind.INSTALLED,
            status=EvidenceStatus.UNKNOWN,
            operation_id=f"{run_id}:handoff",
            phase="handoff",
            details=(("runner_status", status),),
        )
        if takeover_mode:
            discovery_digest = payload.get("discovery_snapshot_digest")
            if not isinstance(discovery_digest, str) or not discovery_digest:
                reason = "takeover apply did not return the discovery snapshot digest"
                manifest = replace(manifest, status=HarnessStatus.BLOCKED, phase="takeover", failure_reason=reason)
                store.update(manifest)
                return HarnessRunResult(manifest.status, manifest, str(manifest_path), reason)
            manifest = _append_evidence(
                manifest,
                reference=f"takeover-snapshot:{discovery_digest}",
                kind=EvidenceKind.INSTALLED,
                status=EvidenceStatus.UNKNOWN,
                operation_id=f"{run_id}:takeover-discovery",
                phase="takeover",
                details=(("snapshot_path", str(discovery_path)), ("snapshot_digest", discovery_digest)),
            )
        deadline = time.monotonic() + input.poll_timeout_seconds
        runner_status = payload
        terminal_states = {"completed", "failed", "blocked", "needs_input", "cancelled", "paused"}
        while _runner_state(runner_status) not in terminal_states and time.monotonic() < deadline:
            await asyncio.sleep(input.poll_interval_seconds)
            runner_status = await asyncio.to_thread(
                _invoke_runner,
                skill_root,
                operation="status",
                control_root=control_root,
                run_id=str(observed_run_id),
            )
        if _runner_state(runner_status) not in terminal_states:
            reason = "live Runner status polling timed out"
            manifest = replace(manifest, status=HarnessStatus.BLOCKED, phase="polling", failure_reason=reason)
            store.update(manifest)
            return HarnessRunResult(manifest.status, manifest, str(manifest_path), reason)
        config_path = input.config_path.expanduser().resolve() if input.config_path else None
        if config_path is None:
            raise ValueError("live harness requires Runner config")
        artifact_directory = _runner_artifact_directory(control_root, config_path, str(observed_run_id))
        github_receipt_path = _runner_github_receipt_path(control_root, config_path)
        if takeover_mode:
            errors, live_evidence, live_resources, _spec_keys = validate_takeover_evidence(
                runner_status,
                artifact_directory=artifact_directory,
                runner_run_id=str(observed_run_id),
                repository=input.repository,
                marker=manifest.marker,
                discovery_snapshot_digest=str(payload["discovery_snapshot_digest"]),
                issue_reader=_github_issue_readback,
            )
        else:
            errors, live_evidence, live_resources, _spec_keys = validate_runner_evidence(
                runner_status,
                artifact_directory=artifact_directory,
                runner_run_id=str(observed_run_id),
                repository=input.repository,
                marker=manifest.marker,
                issue_reader=_github_issue_readback,
                github_receipt_path=github_receipt_path,
            )
        evidence = tuple(
            replace(item, kind=EvidenceKind.LIVE, status=EvidenceStatus.VERIFIED)
            if item.operation_id in {f"{run_id}:handoff", f"{run_id}:takeover-discovery"}
            else item
            for item in manifest.evidence
        )
        manifest = replace(
            manifest,
            evidence=(*evidence, *live_evidence),
            resources=(*manifest.resources, *live_resources),
            phase="verification",
        )
        errors = (*errors, *validate_live_manifest(manifest))
        if errors:
            result_status = HarnessStatus.NOT_VERIFIED if _runner_state(runner_status) != "completed" else HarnessStatus.BLOCKED
            manifest = replace(manifest, status=result_status, failure_reason="; ".join(errors))
            store.update(manifest)
            return HarnessRunResult(manifest.status, manifest, str(manifest_path), manifest.failure_reason)
        summary_path = _write_live_summary(manifest_path.parent / str(observed_run_id), manifest, _spec_keys)
        manifest = replace(manifest, status=HarnessStatus.VERIFIED, phase="complete", cleanup_attempted=True, summary_reference=str(summary_path))
        store.update(manifest)
        return HarnessRunResult(manifest.status, manifest, str(manifest_path))
    except (OSError, ValueError, RuntimeError) as error:
        manifest = replace(manifest, status=HarnessStatus.FAILED, phase="handoff", failure_reason=f"{type(error).__name__}: {error}")
        store.update(manifest)
        return HarnessRunResult(manifest.status, manifest, str(manifest_path), manifest.failure_reason)


def default_manifest_path(prefix: str = "tc083") -> Path:
    directory = Path(os.environ.get("TC083_MANIFEST_DIR", Path(tempfile.gettempdir()) / "temporalio-codex-tc083"))
    return directory / f"{prefix}-{new_identity('run')}.json"


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run the TC-08.3 full-flow harness")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--deterministic", action="store_true")
    mode.add_argument("--live", action="store_true")
    parser.add_argument("--repository", default="williamxhero/temporalio-codex")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--scenario", default="whole-flow")
    parser.add_argument("--brief", type=Path, help="authorized requirement brief for live mode")
    parser.add_argument("--config", type=Path, help="authorized Spec Runner config for live mode")
    parser.add_argument("--control-root", type=Path, help="explicit Spec Runner control root for live mode")
    parser.add_argument("--takeover-issue", type=int, help="umbrella GitHub Issue for live takeover mode")
    parser.add_argument("--takeover-workspace", type=Path, help="existing repository workspace for live takeover mode")
    parser.add_argument("--takeover-target-ref", help="target ref for live takeover mode")
    parser.add_argument("--takeover-key", help="stable takeover identity")
    parser.add_argument("--artifact-root", action="append", type=Path, default=[])
    parser.add_argument("--required-check", action="append", default=[])
    parser.add_argument("--source-thread-id")
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--poll-timeout", type=float, default=900.0)
    args = parser.parse_args(argv)
    manifest_path = args.manifest or default_manifest_path()
    run_input = HarnessRunInput(
        repository=args.repository,
        scenario=args.scenario,
        command="temporalio-codex-live " + ("--live" if args.live else "--deterministic"),
        brief_path=args.brief,
        config_path=args.config,
        control_root=args.control_root,
        takeover_issue=args.takeover_issue,
        takeover_workspace=args.takeover_workspace,
        takeover_target_ref=args.takeover_target_ref,
        takeover_key=args.takeover_key,
        artifact_roots=tuple(args.artifact_root),
        required_checks=tuple(args.required_check),
        source_thread_id=args.source_thread_id,
        poll_interval_seconds=args.poll_interval,
        poll_timeout_seconds=args.poll_timeout,
    )
    if args.live:
        result = asyncio.run(run_live_harness(run_input, manifest_path=manifest_path))
    else:
        result = asyncio.run(run_deterministic_harness(run_input, manifest_path=manifest_path))
    print(
        json.dumps(
            {
                "status": result.status,
                "manifest_path": result.manifest_path,
                "run_id": result.manifest.run_id,
                "marker": result.manifest.marker,
                "reason": result.reason,
                "evidence_refs": [item.reference for item in result.manifest.evidence],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0 if result.status is HarnessStatus.VERIFIED else 2


if __name__ == "__main__":
    raise SystemExit(main())
