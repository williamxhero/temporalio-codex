from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from collections import OrderedDict
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
        self._snapshots: OrderedDict[tuple, tuple[int, dict[str, Any]]] = OrderedDict()
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
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS codex_operation_ledger (
                namespace TEXT NOT NULL, workflow_id TEXT NOT NULL,
                workflow_run_id TEXT NOT NULL, operation_id TEXT NOT NULL,
                thread_id TEXT, turn_id TEXT, observation_json TEXT, fingerprint TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(namespace, workflow_id, workflow_run_id, operation_id)
            )
        """
        )
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
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS codex_execution_scopes (namespace TEXT NOT NULL,"
            "scope_workflow_id TEXT NOT NULL,scope_run_id TEXT NOT NULL,workflow_id TEXT NOT NULL,run_id TEXT NOT NULL,"
            "PRIMARY KEY(namespace,scope_workflow_id,scope_run_id,workflow_id,run_id))"
        )
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS codex_history_imports (source_path TEXT NOT NULL,source_sequence INTEGER NOT NULL,"
            "PRIMARY KEY(source_path,source_sequence))"
        )
        self._connection.commit()

    def link_execution(
        self, scope_workflow_id: str, scope_run_id: str,
        workflow_id: str, run_id: str, *, namespace: str = "default",
    ) -> None:
        if not all(value.strip() for value in (namespace, scope_workflow_id, scope_run_id, workflow_id, run_id)):
            raise ValueError("execution scope requires exact namespace and run identities")
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO codex_execution_scopes VALUES (?,?,?,?,?)",
                (namespace, scope_workflow_id, scope_run_id, workflow_id, run_id),
            )

    def import_history(self, source_path: str | Path) -> dict[str, int]:
        source_path = Path(source_path).resolve(strict=True)
        if source_path == self.path.resolve():
            raise ValueError("cannot import the active conversation database into itself")
        source = sqlite3.connect(source_path.as_uri() + "?mode=ro", uri=True)
        source.row_factory = sqlite3.Row
        try:
            events = [dict(row) for row in source.execute("SELECT * FROM codex_conversation_events ORDER BY sequence")]
            ledgers = [dict(row) for row in source.execute("SELECT * FROM codex_operation_ledger")]
        finally:
            source.close()
        imported = {"events": 0, "operations": 0}
        with self._lock, self._connection:
            for ledger in ledgers:
                key = tuple(ledger[column] for column in ("namespace", "workflow_id", "workflow_run_id", "operation_id"))
                current = self._connection.execute(
                    "SELECT * FROM codex_operation_ledger WHERE namespace=? AND workflow_id=? AND workflow_run_id=? AND operation_id=?", key,
                ).fetchone()
                if current is not None:
                    for column in ("fingerprint", "thread_id", "turn_id", "observation_json"):
                        if current[column] and ledger.get(column) and current[column] != ledger[column]:
                            raise ValueError(f"conflicting operation ledger: {key}")
                    self._connection.execute(
                        "UPDATE codex_operation_ledger SET thread_id=COALESCE(thread_id,?),turn_id=COALESCE(turn_id,?),"
                        "observation_json=COALESCE(observation_json,?),fingerprint=CASE WHEN fingerprint='' THEN ? ELSE fingerprint END "
                        "WHERE namespace=? AND workflow_id=? AND workflow_run_id=? AND operation_id=?",
                        (ledger["thread_id"], ledger["turn_id"], ledger["observation_json"], ledger.get("fingerprint", ""), *key),
                    )
                else:
                    self._connection.execute(
                        "INSERT INTO codex_operation_ledger VALUES (?,?,?,?,?,?,?,?)",
                        (*key, ledger["thread_id"], ledger["turn_id"], ledger["observation_json"], ledger.get("fingerprint", "")),
                    )
                    imported["operations"] += 1
            for event in events:
                origin = (str(source_path), event["sequence"])
                if self._connection.execute(
                    "SELECT 1 FROM codex_history_imports WHERE source_path=? AND source_sequence=?", origin,
                ).fetchone():
                    continue
                event.pop("sequence")
                event.setdefault("namespace", "default")
                for column in ("workflow_run_id", "scope_workflow_run_id", "event_id"):
                    event.setdefault(column, None)
                columns = tuple(event)
                cursor = self._connection.execute(
                    f"INSERT OR IGNORE INTO codex_conversation_events ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                    tuple(event.values()),
                )
                imported["events"] += cursor.rowcount
                self._connection.execute("INSERT INTO codex_history_imports VALUES (?,?)", origin)
        return imported

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
        include_timeline: bool = False,
        include_details: bool = False,
    ) -> dict[str, Any]:
        with self._lock:
            data_version = self._connection.execute("PRAGMA data_version").fetchone()[0]
            revision = (data_version, self._connection.total_changes)
            key = (
                namespace,
                workflow_id,
                workflow_run_id,
                include_timeline,
                include_details,
            )
            cached = self._snapshots.get(key)
            if cached is not None and cached[0] == revision:
                self._snapshots.move_to_end(key)
                return cached[1]
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
                ) OR EXISTS (
                    SELECT 1 FROM codex_execution_scopes s
                    WHERE s.namespace=codex_conversation_events.namespace
                      AND s.scope_workflow_id=? AND (? IS NULL OR s.scope_run_id=?)
                      AND s.workflow_id=codex_conversation_events.workflow_id
                      AND s.run_id=codex_conversation_events.workflow_run_id
                ))
                ORDER BY created_at ASC, sequence ASC
                """,
                (
                    namespace,
                    workflow_id,
                    workflow_run_id,
                    workflow_run_id,
                    workflow_id,
                    workflow_run_id,
                    workflow_run_id,
                    workflow_id,
                    workflow_run_id,
                    workflow_run_id,
                ),
            ).fetchall()

            snapshot = {
                "cursor": max((int(row["sequence"]) for row in rows), default=0),
                "conversations": _build_conversations(
                    rows,
                    include_timeline=include_timeline,
                    include_details=include_details,
                ),
            }
            self._snapshots[key] = (revision, snapshot)
            self._snapshots.move_to_end(key)
            while len(self._snapshots) > 16:
                self._snapshots.popitem(last=False)
            return snapshot

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def check_health(self) -> None:
        with self._lock:
            self._connection.execute("SELECT 1").fetchone()


def _build_conversations(
    rows: list[sqlite3.Row],
    *,
    include_timeline: bool = False,
    include_details: bool = False,
) -> list[dict[str, Any]]:
    conversations: dict[str, dict[str, Any]] = {}
    operation_groups: dict[tuple, str] = {}
    turns: dict[tuple, dict[str, Any]] = {}
    final_turns: set[tuple] = set()

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
                "activities": [],
                "status": "queued",
                "updatedAt": row["created_at"],
            }
            if include_timeline:
                turn["messages"] = []
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
            if include_timeline and text:
                _append_message(turn, {"type": "user", "text": text}, row)
        elif kind == "assistant_delta":
            turn["output"] += text
            turn["status"] = "running"
            if include_timeline and text:
                _append_message(turn, {"type": "assistant", "text": text}, row)
        elif kind == "assistant_final":
            if text:
                turn["output"] = text
                final_turns.add(operation_key)
                if include_timeline:
                    _append_final_message(turn, text, row)
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
                turn["working"][-1]["text"] += f"\n{text}"
            activity = _activity_for_event(
                kind,
                text,
                _detail_from_row(row),
                event_id=row["event_id"],
                sequence=row["sequence"],
            )
            stored_activity = _append_activity(
                turn["activities"],
                activity,
            )
            if include_timeline and not any(
                message.get("activity") is stored_activity
                for message in turn["messages"]
                if message["type"] == "activity"
            ):
                _append_message(
                    turn,
                    {"type": "activity", "activity": stored_activity},
                    row,
                )
        elif kind == "turn_started":
            turn["status"] = "running"
        elif kind == "turn_completed":
            turn["status"] = "completed"
        elif kind in {"turn_failed", "error"}:
            turn["status"] = "failed"
            if text and not turn["output"]:
                turn["output"] = text
            if text:
                activity = _activity_for_event(
                    kind,
                    text,
                    _detail_from_row(row),
                    event_id=row["event_id"],
                    sequence=row["sequence"],
                )
                stored_activity = _append_activity(
                    turn["activities"],
                    activity,
                )
                if include_timeline and not any(
                    message.get("activity") is stored_activity
                    for message in turn["messages"]
                    if message["type"] == "activity"
                ):
                    _append_message(
                        turn,
                        {"type": "activity", "activity": stored_activity},
                        row,
                    )
        elif kind == "history_error":
            turn["status"] = "failed"

    for operation_key, turn in turns.items():
        for activity in turn["activities"]:
            _normalize_activity(activity)
        turn["displayOutput"], turn["verificationMarkers"] = _display_output(
            turn["output"]
        )
        if turn["status"] == "failed" and operation_key not in final_turns:
            errors = [a for a in turn["activities"] if a["category"] == "error"]
            sdk_errors = [
                a for a in errors if a.get("detail", {}).get("method") == "error"
            ]
            errors = sdk_errors or errors
            turn["displayOutput"] = (
                errors[-1]["summary"] if errors else _error_cause(turn["output"])
            )
        if include_timeline and not include_details:
            _compact_timeline_turn(turn)
    return list(conversations.values())


def _append_message(
    turn: dict[str, Any], message: dict[str, Any], row: sqlite3.Row
) -> None:
    messages = turn["messages"]
    if (
        messages
        and message["type"] == "assistant"
        and messages[-1]["type"] == "assistant"
    ):
        messages[-1]["text"] += message["text"]
        return
    message.update(
        id=f"message:{row['sequence']}",
        sequence=row["sequence"],
        createdAt=row["created_at"],
    )
    messages.append(message)


def _append_final_message(turn: dict[str, Any], text: str, row: sqlite3.Row) -> None:
    messages = turn["messages"]
    if (
        messages
        and messages[-1]["type"] == "assistant"
        and messages[-1]["text"].strip() == text.strip()
    ):
        return
    if messages and messages[-1]["type"] == "assistant":
        messages[-1]["text"] = text
    else:
        _append_message(turn, {"type": "assistant", "text": text}, row)


def _compact_timeline_turn(turn: dict[str, Any]) -> None:
    """Keep the thread reader fast without changing the retained audit record."""
    for message in turn["messages"]:
        if message["type"] == "user":
            message["text"] = ""
        elif message["type"] == "assistant":
            message["text"], _ = _display_output(message["text"])
        if message["type"] == "activity":
            activity = message["activity"]
            detail = activity.get("detail")
            command = detail.get("command") if isinstance(detail, dict) else None
            command_name = _command_name(command) if isinstance(command, str) else ""
            message["activity"] = {
                "id": activity["id"],
                "category": activity["category"],
                "summary": activity["summary"],
                "text": "",
                "hasDetails": bool(activity["text"] or activity.get("detail")),
                **({"commandName": command_name} if command_name else {}),
            }
    turn.pop("input", None)
    turn.pop("output", None)
    turn.pop("working", None)
    turn.pop("activities", None)


_TEST_COMMAND = re.compile(
    r"(?:^|\s)(?:pytest|uv\s+run\s+pytest|python(?:\d+(?:\.\d+)*)?\s+-m\s+pytest)(?:\s|$)",
    re.IGNORECASE,
)
_FILE_TEXT = re.compile(
    r"(?:^|\s)(?:read|write|edit|open|create|delete)\s+[^\s]+\.[A-Za-z0-9]+",
    re.IGNORECASE,
)


def _detail_from_row(row: sqlite3.Row) -> dict[str, Any] | None:
    try:
        value = json.loads(row["detail_json"] or "{}")
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _activity_for_event(
    kind: str,
    text: str,
    detail: dict[str, Any] | None,
    *,
    event_id: str | None,
    sequence: int,
) -> dict[str, Any]:
    detail = detail or None
    raw_command = detail.get("command") if detail else None
    command = raw_command.strip() if isinstance(raw_command, str) else ""
    if kind in {"error", "history_error", "turn_failed"}:
        category = "error"
    elif kind == "plan_delta":
        category = "plan"
    elif kind == "reasoning_delta":
        category = "reasoning"
    elif detail and detail.get("probe") is True:
        category = "verification"
    elif kind == "tool_delta":
        method = str(detail.get("method", "")).lower() if detail else ""
        item_type = str(detail.get("item_type", "")).lower() if detail else ""
        if detail and (
            any(key in detail for key in ("path", "file", "file_path"))
            or item_type == "filechange"
        ):
            category = "file"
        elif "test" in item_type or _TEST_COMMAND.search(
            command or text.splitlines()[0]
        ):
            category = "test"
        elif (
            "command" in item_type
            or "command" in method
            or "process" in method
            or command
        ):
            category = "command"
        elif _FILE_TEXT.search(text):
            category = "file"
        elif (
            "verify" in text.splitlines()[0].lower()
            or "probe" in text.splitlines()[0].lower()
        ):
            category = "verification"
        else:
            category = "tool"
    else:
        category = kind.removesuffix("_delta") or "activity"

    activity: dict[str, Any] = {
        "id": event_id or f"event:{sequence}",
        "category": category,
        "summary": (
            _error_cause(text)
            if category == "error"
            else (
                _activity_summary(command or text.splitlines()[0])
                if kind == "tool_delta"
                else _activity_summary(text)
            )
        ),
        "text": text,
    }
    if detail:
        activity["detail"] = detail
    return activity


def _append_activity(
    activities: list[dict[str, Any]], activity: dict[str, Any]
) -> dict[str, Any]:
    detail = activity.get("detail")
    item_id = detail.get("item_id") if isinstance(detail, dict) else None
    method = detail.get("method", "") if isinstance(detail, dict) else ""
    if method == "turn/plan/updated":
        for existing in reversed(activities):
            existing_detail = existing.get("detail")
            if (
                isinstance(existing_detail, dict)
                and existing_detail.get("method") == method
            ):
                existing.update(
                    {key: value for key, value in activity.items() if key != "id"}
                )
                return existing
        activities.append(activity)
        return activity
    if not item_id and activities and isinstance(detail, dict):
        previous = activities[-1]
        previous_detail = previous.get("detail", {})
        if (
            isinstance(previous_detail, dict)
            and not previous_detail.get("item_id")
            and not any(
                key in previous_detail for key in ("exit_code", "probe", "command")
            )
            and not detail.get("probe")
        ):
            previous_method = previous_detail.get("method", "")
            if previous_method in _OUTPUT_DELTA_METHODS and method == previous_method:
                previous["text"] += activity["text"]
                return previous
            if (
                previous_method in _OUTPUT_DELTA_METHODS
                and method == "item/completed"
                and _is_command_completion(activity, detail)
            ):
                previous.update(
                    {key: value for key, value in activity.items() if key != "id"}
                )
                return previous
    if item_id:
        for existing in reversed(activities):
            existing_detail = existing.get("detail")
            if (
                isinstance(existing_detail, dict)
                and existing_detail.get("item_id") == item_id
            ):
                if detail.get("method") == "item/completed":
                    existing.update(
                        {key: value for key, value in activity.items() if key != "id"}
                    )
                else:
                    existing["text"] += activity["text"]
                    existing["summary"] = _activity_summary(
                        existing["text"].splitlines()[0]
                    )
                return existing
    if (
        activities
        and activity["category"] in {"plan", "reasoning"}
        and activities[-1]["category"] == activity["category"]
        and not item_id
        and not (
            isinstance(activities[-1].get("detail"), dict)
            and activities[-1]["detail"].get("method") == "turn/plan/updated"
        )
    ):
        activities[-1]["text"] += f"\n{activity['text']}"
        activities[-1]["summary"] += f"\n{activity['summary']}"
        if "detail" in activity:
            previous_detail = activities[-1].get("detail")
            if previous_detail is None:
                activities[-1]["detail"] = activity["detail"]
            elif previous_detail != activity["detail"]:
                details = (
                    previous_detail
                    if isinstance(previous_detail, list)
                    else [previous_detail]
                )
                details.append(activity["detail"])
                activities[-1]["detail"] = details
        return activities[-1]
    activities.append(activity)
    return activity


def _is_command_completion(activity: dict[str, Any], detail: dict[str, Any]) -> bool:
    item_type = detail.get("item_type")
    if item_type:
        return item_type == "commandExecution"
    if activity["category"] in {"command", "test"}:
        return True
    command = _command_text(detail, activity["text"])
    return bool(
        _READ_COMMAND.search(command)
        or re.match(
            r"^(?:pwsh|powershell|bash|sh|cmd|git|uv|python(?:\d+(?:\.\d+)*)?|npm|pnpm|yarn|go|make|rg|ls)(?:\s|$)",
            command,
            re.IGNORECASE,
        )
    )


_OUTPUT_DELTA_METHODS = {
    "item/commandExecution/outputDelta",
    "command/exec/outputDelta",
    "process/outputDelta",
}
_CONTEXT_PATH = re.compile(
    r"(?<![\w.-])(?:[A-Za-z]:)?(?:[^\s\"'`;|]*[\\/])?(?:SKILL\.md|AGENTS\.md|CLAUDE\.md|README(?:\.[\w-]+)?|[\w.-]*(?:policy|config)[\w.-]*\.(?:md|json|ya?ml|toml|ini))(?=$|[\s\"'`;|])",
    re.IGNORECASE,
)
_READ_COMMAND = re.compile(
    r"(?:^|[\s\"';])(?:Get-Content|cat|type|read|open|sed|head|tail)(?:\s|$)",
    re.IGNORECASE,
)


def _normalize_activity(activity: dict[str, Any]) -> None:
    detail = activity.get("detail", {})
    if not isinstance(detail, dict):
        detail = {}
    text = activity["text"]
    command = _command_text(detail, text)
    paths = _CONTEXT_PATH.findall(command) if _READ_COMMAND.search(command) else []
    path = detail.get("path") or detail.get("file") or detail.get("file_path")
    if (
        isinstance(path, str)
        and _READ_COMMAND.search(command)
        and _CONTEXT_PATH.fullmatch(path)
    ):
        paths.append(path)
    if paths:
        names = list(
            dict.fromkeys(path.replace("\\", "/").rsplit("/", 1)[-1] for path in paths)
        )
        activity["category"] = "context"
        activity["summary"] = _activity_summary(
            "Read context: "
            + ", ".join(names[:3])
            + (f" (+{len(names) - 3} more)" if len(names) > 3 else "")
        )
    elif (
        activity["category"] == "command"
        and detail.get("method") in _OUTPUT_DELTA_METHODS
        and not detail.get("command")
    ):
        activity["summary"] = "Command output"
    elif activity["category"] in {"command", "test"}:
        result = []
        if activity["category"] == "test":
            output = detail.get("aggregated_output", text)
            if isinstance(output, str):
                for line in reversed(output.splitlines()):
                    counts = re.findall(
                        r"\b\d+ (?:passed|failed|skipped|errors?|xfailed|xpassed|deselected)\b",
                        line,
                    )
                    if counts:
                        result.append(", ".join(counts))
                        break
        exit_code = detail.get("exit_code")
        if type(exit_code) is int:
            result.append(
                f"{'Passed' if exit_code == 0 else 'Failed'} (exit {exit_code})"
            )
        elif isinstance(detail.get("status"), str) and detail["status"].strip():
            result.append(_activity_summary(detail["status"].strip(), 40))
        suffix = _activity_summary(" | " + " | ".join(result), 120) if result else ""
        if suffix:
            suffix = " " + suffix
        activity["summary"] = (
            _activity_summary(command or "Command", 160 - len(suffix)) + suffix
        )
    elif activity["category"] == "file" and detail.get("item_type") == "fileChange":
        paths = detail.get("paths", [])
        if isinstance(paths, list):
            paths = [path for path in paths if isinstance(path, str) and path]
            names = [
                _activity_summary(
                    (
                        path
                        if len(path) <= 36
                        else path.replace("\\", "/").rsplit("/", 1)[-1]
                    ),
                    36,
                )
                for path in paths[:3]
            ]
            activity["summary"] = (
                f"Files changed ({len(paths)}): "
                + ", ".join(names)
                + (f" (+{len(paths) - 3} more)" if len(paths) > 3 else "")
            )


def _command_text(detail: dict[str, Any], text: str) -> str:
    command = detail.get("command")
    if isinstance(command, str) and command.strip():
        return command.strip()
    return next((line.strip() for line in text.splitlines() if line.strip()), "")


def _command_name(command: str) -> str:
    executable = re.match(r'''^(?:"([^"]+)"|'([^']+)'|(\S+))''', command.strip())
    if executable is None:
        return ""
    value = next(part for part in executable.groups() if part is not None)
    return value.replace("\\", "/").rsplit("/", 1)[-1]


def _display_output(output: str) -> tuple[str, list[str]]:
    lines = []
    markers = []
    fence = ""
    for line in output.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith(("```", "~~~")):
            delimiter = stripped[:3]
            if not fence:
                fence = delimiter
            elif fence == delimiter:
                fence = ""
        if not fence and stripped in {"SDK_PROBE_OK", "`SDK_PROBE_OK`"}:
            if "SDK_PROBE_OK" not in markers:
                markers.append("SDK_PROBE_OK")
            continue
        lines.append(line)
    return ("".join(lines).strip() if markers else output), markers


def _activity_summary(text: str, limit: int = 160) -> str:
    summary = " ".join(text.split())
    if len(summary) <= limit:
        return summary
    return summary[: limit - 3].rstrip() + "..."


def _error_cause(text: str) -> str:
    return _activity_summary(
        next(
            (line.strip() for line in text.splitlines() if line.strip()),
            "Codex turn failed",
        )
    )


def iso_timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat()
