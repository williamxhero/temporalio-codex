import asyncio
import json
import subprocess
import sys
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
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
from temporalio_codex.conversation_store import ConversationEvent, ConversationStore
from temporalio_codex.delivery_adapter import FakeDeliveryAdapter
from temporalio_codex.delivery_models import (
    DeliveryOutcome,
    DeliveryPhase,
    DeliveryReceipt,
)
from temporalio_codex.delivery_workflows import DeliveryWorkflow
from temporalio_codex.git_adapter import LocalGitAdapter
from temporalio_codex.openai_adapter import OpenAICodexAdapter
from temporalio_codex.planning_activities import (
    configure_spec_issue_gateway,
    configure_ticket_issue_gateway,
    prepare_grill,
    publish_spec_issues,
    publish_ticket_issues,
)
from temporalio_codex.planning_models import PlanningStatus
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.spec_issue_adapter import FakeSpecIssueGateway
from temporalio_codex.summary_activities import (
    configure_summary_gateway,
    publish_delivery_summary,
)
from temporalio_codex.summary_adapter import FakeSummaryCommentGateway
from temporalio_codex.summary_workflows import DeliverySummaryWorkflow
from temporalio_codex.ticket_issue_adapter import FakeTicketIssueGateway
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
from temporalio_codex.whole_flow_models import WholeFlowStatus
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow
from temporalio_codex.workflows import CodexRunWorkflow

from .test_whole_flow import whole_flow_input


def git(cwd, *args):
    return subprocess.run(
        ["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


class LostCreateResponseGateway(FakeSpecIssueGateway):
    def __init__(self, *, mismatch=False):
        super().__init__()
        self.create_count = 0
        self.mismatch = mismatch

    async def create_issue(self, input, draft, body):
        self.create_count += 1
        await super().create_issue(input, draft, body)
        raise ConnectionError("created remotely; response lost")

    async def read_issue(self, issue_number):
        record = await super().read_issue(issue_number)
        return (
            replace(record, operation_id="another-operation")
            if self.mismatch
            else record
        )


@pytest.mark.parametrize("mismatch", [False, True])
async def test_publication_adopts_created_issue_after_response_loss(mismatch):
    gateway = LostCreateResponseGateway(mismatch=mismatch)
    configure_spec_issue_gateway(gateway)
    planning = replace(
        whole_flow_input().planning,
        publication_max_attempts=3,
        publication_retry_backoff_seconds=0.01,
    )
    try:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="publication-response-loss",
                workflows=[RequirementPlanningWorkflow],
                activities=[prepare_grill, publish_spec_issues],
            ),
        ):
            result = await asyncio.wait_for(
                environment.client.execute_workflow(
                    RequirementPlanningWorkflow.run,
                    planning,
                    id=f"publication-response-loss-{mismatch}",
                    task_queue="publication-response-loss",
                ),
                timeout=15,
            )
        if mismatch:
            assert result.status == PlanningStatus.BLOCKED
            assert "mismatch" in result.publication_reason
            assert gateway.create_count == 1
            assert len(gateway.issues) == 1
        else:
            assert result.status == PlanningStatus.COMPLETED
            assert gateway.create_count == 2
            assert len(gateway.issues) == 2
            assert len(result.published_specs) == 2
            assert all(
                issue.parent_issue_number == 43 for issue in result.published_specs
            )
    finally:
        configure_spec_issue_gateway(None)


class InstrumentedAsyncSdk:
    def __init__(self):
        self.prompts = []
        self.close = AsyncMock()

    async def thread_start(self, **kwargs):
        ordinal = len(self.prompts) + 1
        sdk = self

        class Thread:
            id = f"thread-{ordinal}"

            async def turn(self, prompt, **kwargs):
                sdk.prompts.append(prompt)
                return Turn(prompt, Path(kwargs["cwd"]))

        class Turn:
            id = f"turn-{ordinal}"

            def __init__(self, prompt, workspace):
                self.prompt = prompt
                self.workspace = workspace

            async def stream(self):
                if self.prompt.startswith("Role: implementation"):
                    ticket = self.prompt.split("Ready ticket: ", 1)[1].splitlines()[0]
                    (self.workspace / f"{ticket}-implemented.txt").write_text(
                        "implemented\n", encoding="utf-8"
                    )
                    await asyncio.to_thread(git, self.workspace, "add", ".")
                    await asyncio.to_thread(
                        git, self.workspace, "commit", "-m", f"implement {ticket}"
                    )
                output = "governed change completed"
                if self.prompt.startswith("Role: review"):
                    sha = await asyncio.to_thread(
                        git, self.workspace, "rev-parse", "HEAD"
                    )
                    output = json.dumps(
                        {"candidate_sha": sha, "verdict": "approved", "findings": []}
                    )
                for method, payload in (
                    ("item/agentMessage/delta", SimpleNamespace(delta="partial ")),
                    (
                        "item/completed",
                        SimpleNamespace(
                            item=SimpleNamespace(type="agentMessage", text=output)
                        ),
                    ),
                    (
                        "turn/completed",
                        SimpleNamespace(
                            turn=SimpleNamespace(
                                id=self.id,
                                status=SimpleNamespace(value="completed"),
                                error=None,
                            )
                        ),
                    ),
                ):
                    payload.thread_id = Thread.id
                    payload.turn_id = self.id
                    yield SimpleNamespace(method=method, payload=payload)

        return Thread()


async def test_whole_flow_restarts_worker_reopens_store_and_replays_all_children(
    tmp_path,
):
    workflows = [
        RequirementDeliveryWorkflow,
        RequirementPlanningWorkflow,
        TicketSchedulerWorkflow,
        CodexRunWorkflow,
        DeliveryWorkflow,
        DeliverySummaryWorkflow,
    ]
    activities = [
        foundation_stage,
        heartbeat_stage,
        codex_stage,
        delivery_git_stage,
        delivery_github_stage,
        prepare_grill,
        publish_spec_issues,
        publish_ticket_issues,
        publish_delivery_summary,
    ]
    sdk = InstrumentedAsyncSdk()
    store_path = tmp_path / "restart.sqlite3"
    store = ConversationStore(store_path)
    github = FakeDeliveryAdapter()
    execute = github.execute
    waiting = asyncio.Event()
    ci_calls = 0

    async def delayed_ci(operation):
        nonlocal ci_calls
        if operation.phase == DeliveryPhase.CI:
            ci_calls += 1
            if ci_calls == 1:
                waiting.set()
                return DeliveryReceipt(
                    operation.operation_id,
                    operation.phase,
                    DeliveryOutcome.WAITING,
                    "external CI still running",
                )
        return await execute(operation)

    github.execute = delayed_ci
    configure_codex_adapter(OpenAICodexAdapter(lambda: sdk, "0.155.1", store))
    configure_delivery_adapters(LocalGitAdapter(), github)
    configure_spec_issue_gateway(FakeSpecIssueGateway())
    configure_ticket_issue_gateway(FakeTicketIssueGateway())
    configure_summary_gateway(FakeSummaryCommentGateway())
    input = whole_flow_input()
    plans = []
    for plan in input.deliveries:
        repository = tmp_path / plan.spec_key
        repository.mkdir()
        git(repository, "init", "-b", "main")
        git(repository, "config", "user.name", "Acceptance")
        git(repository, "config", "user.email", "acceptance@example.invalid")
        (repository / "README").write_text("base\n", encoding="utf-8")
        git(repository, "add", "README")
        git(repository, "commit", "-m", "base")
        remote = tmp_path / f"{plan.spec_key}.git"
        git(tmp_path, "init", "--bare", str(remote))
        git(repository, "remote", "add", "origin", str(remote))
        git(repository, "push", "origin", "main")
        plans.append(
            replace(
                plan,
                delivery=replace(
                    plan.delivery,
                    repository=str(repository),
                    workspace=str(tmp_path / f"{plan.spec_key}-candidate"),
                    base_sha=git(repository, "rev-parse", "HEAD"),
                    acceptance_command=(
                        sys.executable,
                        "-c",
                        "from pathlib import Path; assert list(Path('.').glob('*-implemented.txt'))",
                    ),
                ),
            )
        )
    input = replace(input, deliveries=tuple(plans))
    input = replace(
        input,
        deliveries=tuple(
            replace(plan, delivery=replace(plan.delivery, readback_backoff_seconds=1))
            for plan in input.deliveries
        ),
    )
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            queue = "automatic-worker-restart"
            worker_options = {
                "task_queue": queue,
                "workflows": workflows,
                "activities": (*activities, capture_codex_candidate),
                "graceful_shutdown_timeout": timedelta(seconds=5),
                "max_cached_workflows": 0,
            }
            async with Worker(environment.client, **worker_options):
                handle = await environment.client.start_workflow(
                    RequirementDeliveryWorkflow.run,
                    input,
                    id=queue,
                    task_queue=queue,
                )
                await asyncio.wait_for(waiting.wait(), timeout=15)
                delivery_handle = environment.client.get_workflow_handle(
                    f"{queue}:delivery:foundation"
                )
                for _ in range(200):
                    history = await delivery_handle.fetch_history()
                    if any(
                        event.HasField("timer_started_event_attributes")
                        for event in history.events
                    ):
                        break
                    await asyncio.sleep(0.01)
                else:
                    raise AssertionError(
                        "CI retry timer was not persisted before shutdown"
                    )
                first_snapshot = store.snapshot(queue, handle.first_execution_run_id)
                assert (
                    sum(len(c["turns"]) for c in first_snapshot["conversations"]) == 6
                )
            store.close()
            store = ConversationStore(store_path)
            assert (
                store.snapshot(queue, handle.first_execution_run_id) == first_snapshot
            )
            configure_codex_adapter(OpenAICodexAdapter(lambda: sdk, "0.155.1", store))
            async with Worker(environment.client, **worker_options):
                try:
                    result = await asyncio.wait_for(handle.result(), timeout=20)
                except TimeoutError as error:
                    state = await handle.query(
                        RequirementDeliveryWorkflow.get_status,
                        rpc_timeout=timedelta(seconds=3),
                    )
                    raise AssertionError(
                        f"restart stalled: {state}; CI calls={ci_calls}"
                    ) from error
            assert result.status == WholeFlowStatus.COMPLETED
            assert len(sdk.prompts) == 9
            histories = []
            pending = [(queue, handle.first_execution_run_id)]
            while pending:
                workflow_id, run_id = pending.pop()
                history = await environment.client.get_workflow_handle(
                    workflow_id,
                    run_id=run_id,
                ).fetch_history()
                histories.append(history)
                for event in history.events:
                    if event.HasField(
                        "child_workflow_execution_started_event_attributes"
                    ):
                        child = event.child_workflow_execution_started_event_attributes.workflow_execution
                        pending.append((child.workflow_id, child.run_id))
            assert len(histories) == 10
            for history in histories:
                await Replayer(workflows=workflows).replay_workflow(history)
            assert len(sdk.prompts) == 9
            parent = store.snapshot(queue, handle.first_execution_run_id)
            turns = [turn for c in parent["conversations"] for turn in c["turns"]]
            assert len(turns) == 9
            assert all(turn["status"] == "completed" for turn in turns)
            assert all(turn["input"] for turn in turns)
            assert all(
                "candidate_sha" in turn["output"]
                or "governed change completed" in turn["output"]
                for turn in turns
            )
            codex_histories = [
                h
                for h in histories
                if h.events[
                    0
                ].workflow_execution_started_event_attributes.workflow_type.name
                == "CodexRunWorkflow"
            ]
            assert len(codex_histories) == 3
            for history in codex_histories:
                child = store.snapshot(history.workflow_id, history.run_id)
                assert sum(len(c["turns"]) for c in child["conversations"]) == 3
            assert not store.snapshot(queue, "another-run")["conversations"]
            assert not store.snapshot(
                queue, handle.first_execution_run_id, namespace="another-namespace"
            )["conversations"]
            store.append(
                ConversationEvent(
                    workflow_id=queue,
                    scope_workflow_id=queue,
                    workflow_run_id="another-run",
                    scope_workflow_run_id="another-run",
                    namespace="another-namespace",
                    operation_id="foreign",
                    stage="planning",
                    role="planning",
                    thread_id="foreign-thread",
                    turn_id="foreign-turn",
                    kind="user_input",
                    text="foreign execution",
                )
            )
            assert store.snapshot(queue, handle.first_execution_run_id) == parent
    finally:
        store.close()
        configure_codex_adapter(None)
        configure_delivery_adapters(None, None)
        configure_spec_issue_gateway(None)
        configure_ticket_issue_gateway(None)
        configure_summary_gateway(None)
