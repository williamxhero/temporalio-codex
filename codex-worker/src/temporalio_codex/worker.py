import argparse
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from temporalio_codex.activities import foundation_stage
from temporalio_codex.settings import DEFAULT_TARGET_HOST, DEFAULT_TASK_QUEUE
from temporalio_codex.workflows import CodexRunWorkflow


async def run_worker(
    *,
    target_host: str = DEFAULT_TARGET_HOST,
    task_queue: str = DEFAULT_TASK_QUEUE,
) -> None:
    client = await Client.connect(target_host)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[CodexRunWorkflow],
        activities=[foundation_stage],
    ):
        await asyncio.Event().wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Codex Temporal Worker")
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST)
    parser.add_argument("--task-queue", default=DEFAULT_TASK_QUEUE)
    args = parser.parse_args()
    asyncio.run(run_worker(target_host=args.target_host, task_queue=args.task_queue))
