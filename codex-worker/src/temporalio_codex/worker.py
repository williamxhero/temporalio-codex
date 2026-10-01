import argparse
import asyncio
import os
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

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
from temporalio_codex.candidate_activities import capture_codex_candidate
from temporalio_codex.conversation_server import ConversationServer
from temporalio_codex.conversation_store import ConversationStore
from temporalio_codex.delivery_workflows import DeliveryWorkflow
from temporalio_codex.git_adapter import LocalGitAdapter
from temporalio_codex.github_adapter import GhCliGateway, GitHubDeliveryAdapter
from temporalio_codex.openai_adapter import OpenAICodexAdapter
from temporalio_codex.planning_activities import (
    configure_spec_issue_gateway,
    configure_ticket_issue_gateway,
    prepare_grill,
    publish_spec_issues,
    publish_ticket_issues,
)
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.settings import DEFAULT_TARGET_HOST, DEFAULT_TASK_QUEUE
from temporalio_codex.spec_issue_adapter import GhCliSpecIssueGateway
from temporalio_codex.spec_workflows import SpecExecutionWorkflow
from temporalio_codex.summary_activities import (
    configure_summary_gateway,
    publish_delivery_summary,
)
from temporalio_codex.summary_adapter import GhCliSummaryCommentGateway
from temporalio_codex.summary_workflows import DeliverySummaryWorkflow
from temporalio_codex.ticket_issue_adapter import GhCliTicketIssueGateway
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow
from temporalio_codex.workflows import CodexRunWorkflow

DEFAULT_CONVERSATION_DB = (
    Path(__file__).resolve().parents[2] / ".tmp" / "codex-conversations.sqlite3"
)


async def run_worker(
    *,
    target_host: str = DEFAULT_TARGET_HOST,
    task_queue: str = DEFAULT_TASK_QUEUE,
    conversation_db: str | None = None,
    conversation_port: int = 18001,
) -> None:
    store = ConversationStore(
        conversation_db
        or os.environ.get(
            "TEMPORALIO_CODEX_CONVERSATION_DB",
            str(DEFAULT_CONVERSATION_DB),
        )
    )
    conversation_server = ConversationServer(store, port=conversation_port)
    try:
        await conversation_server.start()
        configure_codex_adapter(_build_codex_adapter(store))
        configure_delivery_adapters(
            LocalGitAdapter(),
            GitHubDeliveryAdapter(GhCliGateway(_repository_name())),
        )
        configure_spec_issue_gateway(GhCliSpecIssueGateway(_repository_name()))
        configure_ticket_issue_gateway(GhCliTicketIssueGateway(_repository_name()))
        configure_summary_gateway(GhCliSummaryCommentGateway(_repository_name()))
        client = await Client.connect(target_host)
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[
                CodexRunWorkflow,
                DeliveryWorkflow,
                RequirementPlanningWorkflow,
                TicketSchedulerWorkflow,
                DeliverySummaryWorkflow,
                RequirementDeliveryWorkflow,
                SpecExecutionWorkflow,
            ],
            activities=[
                capture_codex_candidate,
                foundation_stage,
                heartbeat_stage,
                codex_stage,
                delivery_git_stage,
                delivery_github_stage,
                prepare_grill,
                publish_spec_issues,
                publish_ticket_issues,
                publish_delivery_summary,
            ],
        ):
            await asyncio.Event().wait()
    finally:
        await conversation_server.close()
        store.close()


def _build_codex_adapter(
    conversation_store: ConversationStore | None = None,
) -> OpenAICodexAdapter | None:
    try:
        from openai_codex import AsyncCodex

        sdk_version = version("openai-codex")
    except (ImportError, PackageNotFoundError):
        return None
    OpenAICodexAdapter._sandbox(("src",))
    return OpenAICodexAdapter(AsyncCodex, sdk_version, conversation_store)


def _repository_name() -> str:
    return os.environ.get(
        "TEMPORALIO_CODEX_REPOSITORY", "williamxhero/temporalio-codex"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Codex Temporal Worker")
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST)
    parser.add_argument("--task-queue", default=DEFAULT_TASK_QUEUE)
    parser.add_argument(
        "--conversation-db",
        default=os.environ.get("TEMPORALIO_CODEX_CONVERSATION_DB"),
    )
    parser.add_argument(
        "--conversation-port",
        type=int,
        default=int(os.environ.get("TEMPORALIO_CODEX_CONVERSATION_PORT", "18001")),
    )
    args = parser.parse_args()
    asyncio.run(
        run_worker(
            target_host=args.target_host,
            task_queue=args.task_queue,
            conversation_db=args.conversation_db,
            conversation_port=args.conversation_port,
        )
    )
