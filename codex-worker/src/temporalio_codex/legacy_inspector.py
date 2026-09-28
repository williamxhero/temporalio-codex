import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LegacyRunRecord:
    run_id: str
    status: str
    evidence: tuple[str, ...]


@dataclass(frozen=True)
class LegacyInspection:
    status: str
    records: tuple[LegacyRunRecord, ...] = ()
    reason: str = ""


def inspect_legacy_database(path: str | Path) -> LegacyInspection:
    """Read the legacy run table without acquiring a writable SQLite handle."""
    database = Path(path).resolve()
    if not database.is_file():
        return LegacyInspection(status="not_verified", reason="legacy database is absent")

    uri = f"file:{database.as_posix()}?mode=ro"
    try:
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
            rows = connection.execute(
                "SELECT run_id, status, evidence_json FROM legacy_runs ORDER BY run_id"
            )
            records = tuple(
                LegacyRunRecord(
                    run_id=run_id,
                    status=status,
                    evidence=tuple(json.loads(evidence_json or "[]")),
                )
                for run_id, status, evidence_json in rows
            )
    except (OSError, sqlite3.Error, ValueError, TypeError) as error:
        return LegacyInspection(
            status="not_verified",
            reason=f"legacy read failed: {type(error).__name__}",
        )
    return LegacyInspection(status="verified", records=records)
