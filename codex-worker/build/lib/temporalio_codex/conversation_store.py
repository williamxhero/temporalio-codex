from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ConversationEvent:
    workflow_id: str
    scope_workflow_id: str
    operation_id: str
    stage: str
    role: str
    thread_id: str | None
    turn_id: str | None
    kind: str
    workflow_run_id: str | None = None
    scope_workflow_run_id: str | None = None
    text: str = ""
    detail: dict[str, Any] | None = None
    event_id: str | None = None
    namespace: str = "default"


class ConversationStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(
            self.path,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS codex_conversation_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL,
                scope_workflow_id TEXT NOT NULL,
                workflow_run_id TEXT,
                scope_workflow_run_id TEXT,
                operation_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                role TEXT NOT NULL,
                thread_id TEXT,
                turn_id TEXT,
                kind TEXT NOT NULL,
                text TEXT NOT NULL,
                detail_json TEXT NOT NULL,
                event_id TEXT,
                created_at REAL NOT NULL
            )
            """
        )
        columns = {
            row[1]
            for row in self._connection.execute(
                "PRAGMA table_info(codex_conversation_events)"
            ).fetchall()
        }
        if "event_id" not in columns:
            self._connection.execute(
                "ALTER TABLE codex_conversation_events ADD COLUMN event_id TEXT"
            )
        if "workflow_run_id" not in columns:
            self._connection.execute(
                "ALTER TABLE codex_conversation_events ADD COLUMN workflow_run_id TEXT"
            )
        if "scope_workflow_run_id" not in columns:
            self._connection.execute(
                "ALTER TABLE codex_conversation_events ADD COLUMN scope_workflow_run_id TEXT"
            )
        if "namespace" not in columns:
            self._connection.execute(
                "ALTER TABLE codex_conversation_events ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default'"
            )
        self._connection.execute("DROP INDEX IF EXISTS idx_codex_conversation_event_id")
        self._connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_codex_conversation_event_id
            ON codex_conversation_events(namespace, workflow_id, COALESCE(workflow_run_id, ''), event_id)
            WHERE event_id IS NOT NULL
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_codex_conversation_scope
            ON codex_conversation_events(scope_workflow_id, sequence)
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_codex_conversation_workflow
            ON codex_conversation_events(workflow_id, sequence)
            """
        )
        self._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_codex_execution_scope ON codex_conversation_events(namespace,scope_workflow_id,scope_workflow_run_id,sequence)"
        )
        self._connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_codex_execution ON codex_conversation_events(namespace,workflow_id,workflow_run_id,sequence)"
        )
        self._connection.commit()
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS codex_operation_ledger (
                namespace TEXT NOT NULL, workflow_id TEXT NOT NULL,
                workflow_run_id TEXT NOT NULL, operation_id TEXT NOT NULL,
                thread_id TEXT, turn_id TEXT, observation_json TEXT, fingerprint TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(namespace, workflow_id, workflow_run_id, operation_id)
            )
        """)
        ledger_columns = {
            row[1]
            for row in self._connection.execute(
                "PRAGMA table_info(codex_operation_ledger)"
            )
        }
        if "fingerprint" not in ledger_columns:
            self._connection.execute(
                "ALTER TABLE codex_operation_ledger ADD COLUMN fingerprint TEXT NOT NULL DEFAULT ''"
            )
        self._connection.commit()

    def claim_operation(
        self, key: tuple[str, str, str, str], fingerprint: str = ""
    ) -> tuple[bool, dict[str, Any]]:
        with self._lock:
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO codex_operation_ledger(namespace,workflow_id,workflow_run_id,operation_id,fingerprint) VALUES (?,?,?,?,?)",
                (*key, fingerprint),
            )
            self._connection.commit()
            row = self._connection.execute(
                "SELECT thread_id,turn_id,observation_json,fingerprint FROM codex_operation_ledger WHERE namespace=? AND workflow_id=? AND workflow_run_id=? AND operation_id=?",
                key,
            ).fetchone()
            return cursor.rowcount == 1, dict(row)

    def record_operation(
        self,
        key: tuple[str, str, str, str],
        *,
        thread_id: str | None,
        turn_id: str | None,
        observation: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE codex_operation_ledger SET thread_id=COALESCE(?,thread_id),turn_id=COALESCE(?,turn_id),observation_json=COALESCE(?,observation_json) WHERE namespace=? AND workflow_id=? AND workflow_run_id=? AND operation_id=?",
                (
                    thread_id,
                    turn_id,
                    json.dumps(observation) if observation is not None else None,
                    *key,
                ),
            )
            self._connection.commit()

    def append(self, event: ConversationEvent) -> int:
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO codex_conversation_events (
                    workflow_id, scope_workflow_id, workflow_run_id,
                    scope_workflow_run_id, operation_id, stage, role,
                    thread_id, turn_id, kind, text, detail_json, event_id, created_at, namespace
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.workflow_id,
                    event.scope_workflow_id,
                    event.workflow_run_id,
                    event.scope_workflow_run_id,
                    event.operation_id,
                    event.stage,
                    event.role,
                    event.thread_id,
                    event.turn_id,
                    event.kind,
                    event.text,
                    json.dumps(event.detail or {}, ensure_ascii=False, default=str),
                    event.event_id,
                    time.time(),
                    event.namespace,
                ),
            )
            self._connection.commit()
            if cursor.rowcount == 0:
                row = self._connection.execute(
                    "SELECT sequence FROM codex_conversation_events WHERE namespace=? AND workflow_id=? AND COALESCE(workflow_run_id,'')=? AND event_id=?",
                    (
                        event.namespace,
                        event.workflow_id,
                        event.workflow_run_id or "",
                        event.event_id,
                    ),
                ).fetchone()
                return int(row[0])
            assert cursor.lastrowid is not None
            return int(cursor.lastrowid)

    def snapshot(
        self,
        workflow_id: str,
        workflow_run_id: str | None = None,
        *,
        namespace: str = "default",
    ) -> dict[str, Any]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT sequence, namespace, workflow_id, operation_id, stage, role,
                       thread_id, turn_id, kind, text, detail_json, event_id,
                       workflow_run_id, scope_workflow_run_id, created_at
                FROM codex_conversation_events
                WHERE namespace = ? AND ((
                    workflow_id = ?
                    AND (
                        ? IS NULL
                        OR workflow_run_id = ?
                    )
                ) OR (
                    scope_workflow_id = ?
                    AND workflow_id != scope_workflow_id
                    AND (
                        ? IS NULL
                        OR scope_workflow_run_id = ?
                    )
                ))
                ORDER BY sequence ASC
                """,
                (
                    namespace,
                    workflow_id,
                    workflow_run_id,
                    workflow_run_id,
                    workflow_id,
                    workflow_run_id,
                    workflow_run_id,
                ),
            ).fetchall()

        return {
            "cursor": int(rows[-1]["sequence"]) if rows else 0,
            "conversations": _build_conversations(rows),
        }

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def check_health(self) -> None:
        with self._lock:
            self._connection.execute("SELECT 1").fetchone()


def _build_conversations(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    conversations: dict[str, dict[str, Any]] = {}
    operation_groups: dict[tuple, str] = {}
    turns: dict[tuple, dict[str, Any]] = {}

    for row in rows:
        operation_id = str(row["operation_id"])
        execution_key = (row["namespace"], row["workflow_id"], row["workflow_run_id"])
        operation_key = (*execution_key, operation_id)
        thread_id = row["thread_id"]
        previous_group_id = operation_groups.get(operation_key)
        group_id = thread_id or previous_group_id or operation_id
        if previous_group_id is not None and previous_group_id != group_id:
            existing = conversations.pop(previous_group_id, None)
            if existing is not None:
                target = conversations.get(group_id)
                if target is None:
                    existing["id"] = group_id
                    conversations[group_id] = existing
                else:
                    target["turns"].extend(
                        turn
                        for turn in existing["turns"]
                        if turn["id"] not in {item["id"] for item in target["turns"]}
                    )
                    target["updatedAt"] = max(
                        target["updatedAt"], existing["updatedAt"]
                    )
            operation_groups[operation_key] = group_id
        elif previous_group_id is None:
            operation_groups[operation_key] = group_id

        conversation = conversations.setdefault(
            group_id,
            {
                "id": group_id,
                "threadId": thread_id,
                "stage": row["stage"],
                "role": row["role"],
                "turns": [],
                "updatedAt": row["created_at"],
            },
        )
        if thread_id:
            conversation["threadId"] = thread_id
        conversation["updatedAt"] = row["created_at"]

        turn = turns.get(operation_key)
        if turn is None:
            turn = {
                "id": operation_id,
                "turnId": row["turn_id"],
                "input": "",
                "output": "",
                "working": [],
                "status": "queued",
                "updatedAt": row["created_at"],
            }
            turns[operation_key] = turn
            conversation["turns"].append(turn)
        if row["turn_id"]:
            turn["turnId"] = row["turn_id"]
        turn["updatedAt"] = row["created_at"]

        kind = row["kind"]
        text = row["text"] or ""
        if kind == "user_input":
            if turn["input"] and turn["input"] != text:
                turn["input"] += f"\n\n{text}"
            else:
                turn["input"] = text
            turn["status"] = "running"
        elif kind == "assistant_delta":
            turn["output"] += text
            turn["status"] = "running"
        elif kind == "assistant_final":
            if text:
                turn["output"] = text
            turn["status"] = "completed"
        elif kind in {"reasoning_delta", "plan_delta", "tool_delta"} and text:
            working_kind = {
                "reasoning_delta": "reasoning",
                "plan_delta": "plan",
                "tool_delta": "tool",
            }[kind]
            if not turn["working"] or turn["working"][-1]["kind"] != working_kind:
                turn["working"].append({"kind": working_kind, "text": text})
            else:
                turn["working"][-1]["text"] += text
        elif kind == "turn_started":
            turn["status"] = "running"
        elif kind == "turn_completed":
            turn["status"] = "completed"
        elif kind in {"turn_failed", "error"}:
            turn["status"] = "failed"
            if text and not turn["output"]:
                turn["output"] = text
        elif kind == "history_error":
            turn["status"] = "failed"

    return list(conversations.values())


def iso_timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat()
