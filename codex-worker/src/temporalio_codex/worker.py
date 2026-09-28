import argparse
import asyncio
from importlib.metadata import PackageNotFoundError, version

from temporalio.client import Client
from temporalio.worker import Worker

from temporalio_codex.activities import (
    codex_stage,
    configure_codex_adapter,
    configure_delivery_adapters,
    delivery_git_stage,
    delivery_github_stage,
    foundation_stage,
    heartbeat_stage,
)
from temporalio_codex.github_adapter import GhCliGateway, GitHubDeliveryAdapter
from temporalio_codex.git_adapter import LocalGitAdapter
from temporalio_codex.openai_adapter import OpenAICodexAdapter
from temporalio_codex.settings import DEFAULT_TARGET_HOST, DEFAULT_TASK_QUEUE
from temporalio_codex.workflows import CodexRunWorkflow
from temporalio_codex.delivery_workflows import DeliveryWorkflow
from temporalio_codex.planning_activities import prepare_grill
from temporalio_codex.planning_activities import configure_spec_issue_gateway, publish_spec_issues
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.spec_issue_adapter import GhCliSpecIssueGateway
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow


async def run_worker(
    *,
    target_host: str = DEFAULT_TARGET_HOST,
    task_queue: str = DEFAULT_TASK_QUEUE,
) -> None:
    configure_codex_adapter(_build_codex_adapter())
    configure_delivery_adapters(
        LocalGitAdapter(),
        GitHubDeliveryAdapter(GhCliGateway(_repository_name())),
    )
    configure_spec_issue_gateway(GhCliSpecIssueGateway(_repository_name()))
    client = await Client.connect(target_host)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[
            CodexRunWorkflow,
            DeliveryWorkflow,
            RequirementPlanningWorkflow,
            TicketSchedulerWorkflow,
        ],
        activities=[
            foundation_stage,
            heartbeat_stage,
            codex_stage,
            delivery_git_stage,
            delivery_github_stage,
            prepare_grill,
            publish_spec_issues,
        ],
    ):
        await asyncio.Event().wait()


def _build_codex_adapter() -> OpenAICodexAdapter | None:
    try:
        from openai_codex import AsyncCodex

        sdk_version = version("openai-codex")
    except (ImportError, PackageNotFoundError):
        return None
    return OpenAICodexAdapter(AsyncCodex, sdk_version)


def _repository_name() -> str:
    return "williamxhero/temporalio-codex"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Codex Temporal Worker")
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST)
    parser.add_argument("--task-queue", default=DEFAULT_TASK_QUEUE)
    args = parser.parse_args()
    asyncio.run(run_worker(target_host=args.target_host, task_queue=args.task_queue))
