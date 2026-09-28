import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LegacyRunRecord:
    run_id: str
    status: str
    evidence: tuple[str, ...]
    requirements: tuple[str, ...] = ()
    stage_frontier: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = ()
    unknown_operations: tuple[str, ...] = ()


@dataclass(frozen=True)
class LegacyInspection:
    status: str
    records: tuple[LegacyRunRecord, ...] = ()
    reason: str = ""
    source_identity: str | None = None


def _json_values(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        if not value.strip():
            return ()
        value = json.loads(value)
    if isinstance(value, (list, tuple)):
        return tuple(item for item in value if isinstance(item, str))
    return ()


def inspect_legacy_database(path: str | Path) -> LegacyInspection:
    """Read the legacy run table without acquiring a writable SQLite handle."""
    database = Path(path).resolve()
    if not database.is_file():
        return LegacyInspection(status="not_verified", reason="legacy database is absent")

    source_identity: str | None = None
    try:
        source_identity = hashlib.sha256(database.read_bytes()).hexdigest()
        uri = f"file:{database.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            if "legacy_runs" not in tables:
                return LegacyInspection(
                    status="not_verified",
                    reason="legacy_runs table is absent",
                )
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(legacy_runs)")
            }
            optional_columns = [
                name
                for name in (
                    "requirements_json",
                    "stage_frontier_json",
                    "artifact_refs_json",
                    "unknown_operations_json",
                )
                if name in columns
            ]
            selected_columns = ["run_id", "status", "evidence_json", *optional_columns]
            optional_indexes = {
                name: selected_columns.index(name) for name in optional_columns
            }
            rows = connection.execute(
                "SELECT "
                + ", ".join(selected_columns)
                + " FROM legacy_runs ORDER BY run_id"
            )
            records = tuple(
                LegacyRunRecord(
                    run_id=row[0],
                    status=row[1],
                    evidence=_json_values(row[2]),
                    requirements=_json_values(
                        row[optional_indexes["requirements_json"]]
                        if "requirements_json" in optional_indexes
                        else None
                    ),
                    stage_frontier=_json_values(
                        row[optional_indexes["stage_frontier_json"]]
                        if "stage_frontier_json" in optional_indexes
                        else None
                    ),
                    artifact_refs=_json_values(
                        row[optional_indexes["artifact_refs_json"]]
                        if "artifact_refs_json" in optional_indexes
                        else None
                    ),
                    unknown_operations=_json_values(
                        row[optional_indexes["unknown_operations_json"]]
                        if "unknown_operations_json" in optional_indexes
                        else None
                    ),
                )
                for row in rows
            )
    except (OSError, sqlite3.Error, ValueError, TypeError) as error:
        return LegacyInspection(
            status="not_verified",
            reason=f"legacy read failed: {type(error).__name__}",
            source_identity=source_identity,
        )
    return LegacyInspection(
        status="verified",
        records=records,
        source_identity=source_identity,
    )
