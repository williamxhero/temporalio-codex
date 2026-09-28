import argparse
import asyncio
import hashlib
import json
from dataclasses import asdict

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy

from temporalio_codex.models import RunInput, RunResult
from temporalio_codex.settings import DEFAULT_TARGET_HOST, DEFAULT_TASK_QUEUE
from temporalio_codex.workflows import CodexRunWorkflow


def stable_workflow_id(requirement: str) -> str:
    if not requirement.strip():
        raise ValueError("requirement must not be empty")
    digest = hashlib.sha256(requirement.encode("utf-8")).hexdigest()[:16]
    return f"codex-run-{digest}"


async def execute_run(
    client: Client,
    requirement: str,
    *,
    workflow_id: str | None = None,
    task_queue: str = DEFAULT_TASK_QUEUE,
) -> RunResult:
    resolved_workflow_id = workflow_id or stable_workflow_id(requirement)
    return await client.execute_workflow(
        CodexRunWorkflow.run,
        RunInput(requirement=requirement),
        id=resolved_workflow_id,
        task_queue=task_queue,
        result_type=RunResult,
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
    )


async def run_from_server(
    requirement: str,
    *,
    target_host: str = DEFAULT_TARGET_HOST,
    workflow_id: str | None = None,
    task_queue: str = DEFAULT_TASK_QUEUE,
) -> RunResult:
    client = await Client.connect(target_host)
    return await execute_run(
        client,
        requirement,
        workflow_id=workflow_id,
        task_queue=task_queue,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Start a Codex Temporal run")
    parser.add_argument("--requirement", required=True)
    parser.add_argument("--workflow-id")
    parser.add_argument("--task-queue", default=DEFAULT_TASK_QUEUE)
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST)
    args = parser.parse_args()
    result = asyncio.run(
        run_from_server(
            args.requirement,
            target_host=args.target_host,
            workflow_id=args.workflow_id,
            task_queue=args.task_queue,
        )
    )
    print(json.dumps(asdict(result), sort_keys=True))
