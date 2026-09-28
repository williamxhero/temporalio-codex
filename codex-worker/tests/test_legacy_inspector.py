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
    before = database.read_bytes()

    inspection = inspect_legacy_database(database)

    after = database.read_bytes()
    assert inspection.status == "verified"
    assert inspection.records[0].run_id == "legacy-1"
    assert inspection.records[0].evidence == ("issue:1", "thread:t1")
    assert before == after


def test_missing_legacy_database_reports_not_verified(tmp_path) -> None:
    inspection = inspect_legacy_database(tmp_path / "missing.sqlite")

    assert inspection.status == "not_verified"
    assert inspection.records == ()


def test_historical_records_are_read_only_evidence(tmp_path) -> None:
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

    inspection = inspect_legacy_database(database)

    assert inspection.status == "verified"
    assert inspection.records[0].requirements == ("ship the change",)
    assert inspection.records[0].stage_frontier == ("review",)
    assert inspection.records[0].unknown_operations == ("merge:unknown",)
    assert database.read_bytes()


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

    inspection = inspect_legacy_database(database)

    assert inspection.status == "verified"
    assert {record.run_id for record in inspection.records} == {"waiting", "unknown"}
    assert all(record.requirements for record in inspection.records)
