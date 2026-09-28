import hashlib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from temporalio_codex.legacy_inspector import LegacyRunRecord, inspect_legacy_database


class LegacyImportStatus(StrEnum):
    VERIFIED = "verified"
    DUPLICATE = "duplicate"
    NOT_VERIFIED = "not_verified"


@dataclass(frozen=True)
class LegacyImportInput:
    source_identity: str
    source_run_id: str
    source_status: str
    requirements: tuple[str, ...]
    stage_frontier: tuple[str, ...]
    artifact_refs: tuple[str, ...]
    unknown_operations: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    target_workflow_id: str


@dataclass(frozen=True)
class LegacyImportResult:
    status: LegacyImportStatus
    source_identity: str | None = None
    source_run_id: str | None = None
    target_workflow_id: str | None = None
    import_input: LegacyImportInput | None = None
    reason: str = ""


def _target_workflow_id(source_identity: str, run_id: str) -> str:
    digest = hashlib.sha256(f"{source_identity}:{run_id}".encode("utf-8")).hexdigest()[:16]
    return f"legacy-import-{digest}"


def _selected_record(
    records: tuple[LegacyRunRecord, ...], source_run_id: str
) -> LegacyRunRecord | None:
    return next((record for record in records if record.run_id == source_run_id), None)


def prepare_legacy_import(
    path: str | Path,
    source_run_id: str,
    *,
    target_workflow_id: str | None = None,
    imported_source_identities: tuple[str, ...] = (),
    imported_workflow_ids: tuple[str, ...] = (),
) -> LegacyImportResult:
    """Convert one verified legacy record into a read-only Temporal input.

    The function never writes to the legacy database. A caller starting the
    resulting Workflow must use the returned stable ID and reject an existing
    ID rather than retrying an ambiguous external start.
    """
    inspection = inspect_legacy_database(path)
    source_identity = inspection.source_identity
    if inspection.status != "verified" or source_identity is None:
        return LegacyImportResult(
            status=LegacyImportStatus.NOT_VERIFIED,
            source_identity=source_identity,
            source_run_id=source_run_id,
            reason=inspection.reason or "legacy inspection is not verified",
        )

    record = _selected_record(inspection.records, source_run_id)
    if record is None:
        return LegacyImportResult(
            status=LegacyImportStatus.NOT_VERIFIED,
            source_identity=source_identity,
            source_run_id=source_run_id,
            reason="selected legacy run is absent",
        )

    resolved_workflow_id = target_workflow_id or _target_workflow_id(
        source_identity, source_run_id
    )
    if (
        source_identity in imported_source_identities
        or resolved_workflow_id in imported_workflow_ids
    ):
        return LegacyImportResult(
            status=LegacyImportStatus.DUPLICATE,
            source_identity=source_identity,
            source_run_id=source_run_id,
            target_workflow_id=resolved_workflow_id,
            reason="legacy source identity or target Workflow ID was already imported",
        )

    if record.status.lower() not in {"completed", "waiting", "waiting_for_input"}:
        return LegacyImportResult(
            status=LegacyImportStatus.NOT_VERIFIED,
            source_identity=source_identity,
            source_run_id=source_run_id,
            target_workflow_id=resolved_workflow_id,
            reason=f"legacy status is not importable: {record.status}",
        )
    if not record.requirements:
        return LegacyImportResult(
            status=LegacyImportStatus.NOT_VERIFIED,
            source_identity=source_identity,
            source_run_id=source_run_id,
            target_workflow_id=resolved_workflow_id,
            reason="legacy record has no verified requirements",
        )

    return LegacyImportResult(
        status=LegacyImportStatus.VERIFIED,
        source_identity=source_identity,
        source_run_id=source_run_id,
        target_workflow_id=resolved_workflow_id,
        import_input=LegacyImportInput(
            source_identity=source_identity,
            source_run_id=source_run_id,
            source_status=record.status,
            requirements=record.requirements,
            stage_frontier=record.stage_frontier,
            artifact_refs=record.artifact_refs,
            unknown_operations=record.unknown_operations,
            evidence_refs=record.evidence,
            target_workflow_id=resolved_workflow_id,
        ),
    )
