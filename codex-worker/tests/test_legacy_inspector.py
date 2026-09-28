import hashlib
import json
import sqlite3

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
