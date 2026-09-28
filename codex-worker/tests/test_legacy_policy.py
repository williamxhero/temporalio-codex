from __future__ import annotations

import json
import sqlite3

import pytest

from temporalio_codex.legacy_policy import (
    LegacyDecision,
    LegacyOperation,
    reject_legacy_operation,
)


def _database(tmp_path, rows):
    database = tmp_path / "legacy.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE legacy_runs (run_id TEXT PRIMARY KEY, status TEXT, evidence_json TEXT, requirements_json TEXT)"
        )
        connection.executemany(
            "INSERT INTO legacy_runs VALUES (?, ?, ?, ?)",
            [(run_id, status, json.dumps(evidence), json.dumps(requirements)) for run_id, status, evidence, requirements in rows],
        )
        connection.commit()
    return database


@pytest.mark.parametrize("operation", tuple(LegacyOperation))
def test_all_legacy_execution_operations_are_rejected(tmp_path, operation) -> None:
    database = _database(tmp_path, [("legacy-1", "completed", ["issue:1"], ["ship it"])])

    result = reject_legacy_operation(database, "legacy-1", operation)

    assert result.decision is LegacyDecision.REJECTED
    assert result.operation is operation
    assert result.evidence_refs == ("issue:1",)
    assert operation.value in result.reason


def test_waiting_and_unknown_history_are_not_guessed_into_execution(tmp_path) -> None:
    database = _database(
        tmp_path,
        [
            ("waiting", "waiting", ["question:q1"], ["answer needed"]),
            ("unknown", "unknown", ["merge:unknown"], ["do not guess"]),
        ],
    )

    waiting = reject_legacy_operation(database, "waiting", LegacyOperation.CONTINUE)
    unknown = reject_legacy_operation(database, "unknown", LegacyOperation.TAKEOVER)

    assert waiting.decision is LegacyDecision.REJECTED
    assert unknown.decision is LegacyDecision.REJECTED
    assert waiting.evidence_refs == ("question:q1",)
    assert unknown.evidence_refs == ("merge:unknown",)


def test_missing_and_malformed_history_remain_not_verified(tmp_path) -> None:
    missing = reject_legacy_operation(
        tmp_path / "missing.sqlite", "legacy-1", LegacyOperation.RESUME
    )
    malformed = tmp_path / "malformed.sqlite"
    with sqlite3.connect(malformed) as connection:
        connection.execute(
            "CREATE TABLE legacy_runs (run_id TEXT PRIMARY KEY, status TEXT, evidence_json TEXT)"
        )
        connection.execute(
            "INSERT INTO legacy_runs VALUES (?, ?, ?)",
            ("legacy-1", "completed", "not-json"),
        )
        connection.commit()
    broken = reject_legacy_operation(malformed, "legacy-1", LegacyOperation.MIGRATE)

    assert missing.decision is LegacyDecision.NOT_VERIFIED
    assert broken.decision is LegacyDecision.NOT_VERIFIED
    assert "absent" in missing.reason
    assert "legacy read failed" in broken.reason


def test_unknown_or_duplicate_identity_is_not_replaced(tmp_path) -> None:
    database = _database(tmp_path, [("legacy-1", "completed", [], ["ship it"])])

    absent = reject_legacy_operation(database, "other", LegacyOperation.TAKEOVER)

    assert absent.decision is LegacyDecision.NOT_VERIFIED
    assert "missing or ambiguous" in absent.reason
