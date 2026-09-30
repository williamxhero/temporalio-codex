"""Opt-in read-only SDK acceptance against an existing local worker."""

import argparse
import asyncio
import json
import sqlite3
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import urlopen
from uuid import uuid4

from temporalio.client import Client

from temporalio_codex.codex_models import CodexRole
from temporalio_codex.models import RunInput, RunStatus, StageDefinition
from temporalio_codex.workflows import CodexRunWorkflow


def fetch_snapshot(origin: str, scope: dict[str, str]) -> dict:
    url = f"{origin}/api/v1/codex/conversations?{urlencode(scope)}"
    with urlopen(url, timeout=10) as response:
        return json.load(response)


def count_events(database: Path, scope: dict[str, str]) -> dict[str, int]:
    with sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True) as connection:
        return dict(connection.execute(
            "SELECT kind, COUNT(*) FROM codex_conversation_events "
            "WHERE namespace=? AND workflow_id=? AND workflow_run_id=? GROUP BY kind",
            (scope["namespace"], scope["workflow_id"], scope["run_id"]),
        ))


async def probe(args: argparse.Namespace) -> dict:
    client = await Client.connect(args.target_host, namespace=args.namespace)
    workflow_id = f"issue-81-sdk-probe-{uuid4().hex[:12]}"
    handle = await client.start_workflow(
        CodexRunWorkflow.run,
        RunInput(
            requirement=(
                "Read-only acceptance probe. Read codex-worker/README.md and state "
                "the documented offline acceptance test command. End your final "
                "response with SDK_PROBE_OK. Do not implement anything, write files, "
                "publish issues, invoke external services or modify Git."
            ),
            automatic=True,
            stages=(StageDefinition(
                key="sdk-read-only-probe",
                role=CodexRole.PLANNING,
                repository=str(args.repository.resolve()),
                allowed_scope=("read_only",),
                model=args.model,
                effort="low",
                start_to_close_timeout_seconds=args.timeout,
            ),),
        ),
        id=workflow_id,
        task_queue=args.task_queue,
    )
    scope = {
        "namespace": args.namespace,
        "workflow_id": workflow_id,
        "run_id": handle.first_execution_run_id,
    }
    evidence = {
        "timestamp": datetime.now(UTC).isoformat(),
        "scope": scope,
        "model": args.model,
        "sdk_boundary": "real openai-codex SDK through production worker",
        "read_only": True,
        "full_delivery_verified": False,
        "chat_url": (
            f"{args.ui_origin}/namespaces/{quote(args.namespace, safe='')}/workflows/"
            f"{quote(workflow_id, safe='')}/{scope['run_id']}/chat"
        ),
    }
    try:
        result = await asyncio.wait_for(handle.result(), args.timeout + 30)
        evidence["result"] = asdict(result)
    except TimeoutError:
        evidence["error"] = "bounded probe timeout; requested cancellation"
        await handle.cancel()
    snapshot = await handle.query(CodexRunWorkflow.get_status)
    evidence["status"] = asdict(snapshot)
    conversations = await asyncio.to_thread(fetch_snapshot, args.conversation_origin, scope)
    evidence["conversation_snapshot"] = conversations
    evidence["conversation_count"] = len(conversations["conversations"])
    evidence["turn_count"] = sum(
        len(item["turns"]) for item in conversations["conversations"]
    )
    evidence["event_counts"] = count_events(args.conversation_db, scope)
    evidence["passed"] = (
        snapshot.status is RunStatus.COMPLETED
        and evidence["turn_count"] == 1
        and any(
            "SDK_PROBE_OK" in turn["output"]
            for item in conversations["conversations"] for turn in item["turns"]
        )
        and bool(snapshot.stage_results)
        and all(item.thread_id and item.turn_id for item in snapshot.stage_results)
    )
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", required=True)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--conversation-db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-host", default="localhost:7233")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--task-queue", default="codex-worker")
    parser.add_argument("--conversation-origin", default="http://127.0.0.1:18001")
    parser.add_argument("--ui-origin", default="http://localhost:18000")
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    evidence = asyncio.run(probe(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({
        "passed": evidence["passed"], "scope": evidence["scope"],
        "event_counts": evidence["event_counts"], "chat_url": evidence["chat_url"],
    }))
    if not evidence["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
