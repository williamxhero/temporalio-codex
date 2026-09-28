import hashlib
import json
import sqlite3

from temporalio_codex.legacy_import import (
    LegacyImportStatus,
    prepare_legacy_import,
)
from temporalio_codex.legacy_inspector import inspect_legacy_database


def test_legacy_inspection_is_read_only_and_preserves_identity_evidence(tmp_path) -> None:
    database = tmp_path / "legacy.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE legacy_runs (run_id TEXT PRIMARY KEY, status TEXT, evidence_json TEXT)"
        )
        connection.execute(
            "INSERT INTO legacy_runs VALUES (?, ?, ?)",
            ("legacy-1", "waiting", json.dumps(["issue:1", "thread:t1"])),
        )
        connection.commit()
    before = hashlib.sha256(database.read_bytes()).digest()

    inspection = inspect_legacy_database(database)

    after = hashlib.sha256(database.read_bytes()).digest()
    assert inspection.status == "verified"
    assert inspection.records[0].run_id == "legacy-1"
    assert inspection.records[0].evidence == ("issue:1", "thread:t1")
    assert before == after


def test_missing_legacy_database_reports_not_verified(tmp_path) -> None:
    inspection = inspect_legacy_database(tmp_path / "missing.sqlite")

    assert inspection.status == "not_verified"
    assert inspection.records == ()


def test_selected_import_preserves_frontier_unknowns_and_is_idempotent(tmp_path) -> None:
    database = tmp_path / "legacy.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE legacy_runs ("
            "run_id TEXT PRIMARY KEY, status TEXT, evidence_json TEXT, "
            "requirements_json TEXT, stage_frontier_json TEXT, "
            "artifact_refs_json TEXT, unknown_operations_json TEXT)"
        )
        connection.execute(
            "INSERT INTO legacy_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "legacy-complete",
                "completed",
                json.dumps(["issue:7"]),
                json.dumps(["ship the change"]),
                json.dumps(["review"]),
                json.dumps(["pr:12"]),
                json.dumps(["merge:unknown"]),
            ),
        )
        connection.commit()

    first = prepare_legacy_import(database, "legacy-complete")

    assert first.status is LegacyImportStatus.VERIFIED
    assert first.import_input is not None
    assert first.import_input.requirements == ("ship the change",)
    assert first.import_input.stage_frontier == ("review",)
    assert first.import_input.unknown_operations == ("merge:unknown",)
    assert first.target_workflow_id == first.import_input.target_workflow_id

    duplicate = prepare_legacy_import(
        database,
        "legacy-complete",
        imported_source_identities=(first.source_identity,),
    )
    assert duplicate.status is LegacyImportStatus.DUPLICATE
    assert duplicate.target_workflow_id == first.target_workflow_id


def test_selected_waiting_and_unknown_records_are_not_invented(tmp_path) -> None:
    database = tmp_path / "legacy.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE legacy_runs (run_id TEXT PRIMARY KEY, status TEXT, evidence_json TEXT, requirements_json TEXT)"
        )
        connection.executemany(
            "INSERT INTO legacy_runs VALUES (?, ?, ?, ?)",
            [
                ("waiting", "waiting", "[]", json.dumps(["answer needed"])),
                ("unknown", "unknown", "[]", json.dumps(["do not guess"])),
            ],
        )
        connection.commit()

    waiting = prepare_legacy_import(database, "waiting")
    unknown = prepare_legacy_import(database, "unknown")

    assert waiting.status is LegacyImportStatus.VERIFIED
    assert waiting.import_input is not None
    assert waiting.import_input.source_status == "waiting"
    assert unknown.status is LegacyImportStatus.NOT_VERIFIED
    assert "not importable" in unknown.reason
