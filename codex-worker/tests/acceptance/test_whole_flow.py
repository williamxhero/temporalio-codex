import asyncio
import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.testing import WorkflowEnvironment
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
from temporalio_codex.delivery_adapter import FakeDeliveryAdapter
from temporalio_codex.delivery_models import (
    DeliveryInput,
    DeliveryOutcome,
    DeliveryPhase,
    DeliveryReceipt,
)
from temporalio_codex.delivery_workflows import DeliveryWorkflow
from temporalio_codex.execution_status import ExecutionProgress
from temporalio_codex.git_adapter import LocalGitAdapter
from temporalio_codex.openai_adapter import OpenAICodexAdapter
from temporalio_codex.planning_activities import (
    configure_spec_issue_gateway,
    configure_ticket_issue_gateway,
    prepare_grill,
    publish_spec_issues,
    publish_ticket_issues,
)
from temporalio_codex.planning_models import (
    GrillAnswer,
    SourceOrigin,
)
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.spec_issue_adapter import FakeSpecIssueGateway, SpecDraft
from temporalio_codex.summary_activities import (
    configure_summary_gateway,
    publish_delivery_summary,
)
from temporalio_codex.summary_adapter import (
    FakeSummaryCommentGateway,
    SummaryPublicationInput,
)
from temporalio_codex.summary_workflows import DeliverySummaryWorkflow
from temporalio_codex.ticket_issue_adapter import FakeTicketIssueGateway
from temporalio_codex.ticket_scheduler import SchedulerInput, SpecPlan, TicketPlan
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
from temporalio_codex.whole_flow_models import (
    PlanningPayload,
    SpecCodexPlan,
    SpecDeliveryPlan,
    WholeFlowInput,
    WholeFlowPhase,
    WholeFlowStatus,
    validate_whole_flow_input,
)
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow
from temporalio_codex.workflows import CodexRunWorkflow


def spec_drafts() -> tuple[SpecDraft, ...]:
    return (
        SpecDraft(
            key="foundation",
            title="Foundation acceptance",
            scope="Prove the first delivery path",
            acceptance_criteria=("foundation is delivered",),
            testing_decisions=("run deterministic acceptance",),
            provenance=("grill:deliver a governed change",),
        ),
        SpecDraft(
            key="follow-up",
            title="Follow-up acceptance",
            scope="Prove sequential SPEC delivery",
            acceptance_criteria=("follow-up is delivered",),
            testing_decisions=("read back origin",),
            dependencies=("foundation",),
            provenance=("grill:deliver a governed change",),
        ),
    )


def delivery_input(spec_key: str) -> DeliveryInput:
    return DeliveryInput(
        repository=f"D:/acceptance/{spec_key}",
        workspace=f"D:/acceptance/{spec_key}/candidate",
        base_sha=f"base-{spec_key}",
        candidate_branch=f"codex/{spec_key}",
        pull_request_identity=f"acceptance:{spec_key}",
        title=f"Acceptance {spec_key}",
        body=f"acceptance:{spec_key}",
        issue_numbers=(101, 102),
    )


def whole_flow_input() -> WholeFlowInput:
    drafts = spec_drafts()
    return WholeFlowInput(
        planning=PlanningPayload(
            origin=SourceOrigin.TEXT,
            source_text="deliver a governed change",
            umbrella_issue_number=43,
            specs=drafts,
        ),
        scheduler=SchedulerInput(
            specs=(
                SpecPlan("foundation"),
                SpecPlan("follow-up", ("foundation",)),
            ),
            tickets=(
                TicketPlan("foundation-a", "foundation"),
                TicketPlan("foundation-b", "foundation"),
                TicketPlan(
                    "follow-up-a", "follow-up", ("foundation-a", "foundation-b")
                ),
            ),
            completion_operations=(),
        ),
        codex=tuple(
            SpecCodexPlan(
                spec_key=key,
                requirement=f"deliver {key}",
                repository=f"D:/acceptance/{key}",
                allowed_scope=("src/",),
                start_to_close_timeout_seconds=30,
            )
            for key in ("foundation", "follow-up")
        ),
        deliveries=tuple(
            SpecDeliveryPlan(
                key,
                replace(
                    delivery_input(key),
                    readback_max_attempts=3,
                    readback_backoff_seconds=0.01,
                ),
            )
            for key in ("foundation", "follow-up")
        ),
        summary=SummaryPublicationInput(
            repository="owner/repo",
            umbrella_issue_number=43,
            operation_id="acceptance:summary",
            summary_text="Status: pass\nSPECs: foundation, follow-up\nRemote: verified",
        ),
    )


@pytest.mark.parametrize(
    "failure_phase",
    [
        None,
        DeliveryPhase.CLEANUP,
        DeliveryPhase.CI,
        "summary",
        "ci-transient",
        "review-missing",
        "review-mismatch",
        "review-dirty",
        "review-rejected",
    ],
)
async def test_two_spec_whole_flow_runs_through_public_child_workflows(
    failure_phase,
    tmp_path,
) -> None:
    handle = None
    observed_active = []
    def git(cwd, *args):
        return subprocess.run(
            ["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True
        ).stdout.strip()

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
    sdk_result = SimpleNamespace(
        id="turn-acceptance",
        status=SimpleNamespace(value="completed"),
        final_response="governed change completed",
        error=None,
    )
    thread = MagicMock(id="thread-acceptance")

    def sdk_turn(prompt, **kwargs):
        workspace = Path(kwargs["cwd"])

        async def run():
            for _ in range(100):
                state = await handle.query(RequirementDeliveryWorkflow.get_status)
                if state.active_ticket:
                    break
                await asyncio.sleep(0.01)
            assert state.phase == WholeFlowPhase.CODEX
            assert state.status == WholeFlowStatus.ACTIVE
            assert state.active_spec in ("foundation", "follow-up")
            assert state.active_ticket.startswith(state.active_spec)
            assert state.deadline and state.timeout_seconds == 30
            assert "Codex" in state.next_action
            await handle.signal("execution_progress", ExecutionProgress(
                "whole-flow-acceptance:tickets:foundation", "stale-run",
                "planning", "blocked", pending_reason="stale signal",
            ))
            unchanged = await handle.query(RequirementDeliveryWorkflow.get_status)
            assert unchanged.phase == WholeFlowPhase.CODEX
            assert unchanged.status == WholeFlowStatus.ACTIVE
            assert unchanged.active_ticket == state.active_ticket
            assert unchanged.pending_reason != "stale signal"
            observed_active.append(state)
            if prompt.startswith("Role: implementation"):
                ticket = prompt.split("Ready ticket: ", 1)[1].splitlines()[0]
                (workspace / f"{ticket}-implemented.txt").write_text(
                    "implemented\n", encoding="utf-8"
                )
                git(workspace, "add", ".")
                git(workspace, "commit", "-m", f"implement {ticket}")
            if prompt.startswith("Role: review"):
                assert kwargs["output_schema"]["required"] == [
                    "candidate_sha", "verdict", "findings"
                ]
                sha = git(workspace, "rev-parse", "HEAD")
                if failure_phase == "review-dirty":
                    (workspace / "dirty").write_text(
                        "changed during review", encoding="utf-8"
                    )
                response = json.dumps(
                    {
                        "candidate_sha": "wrong"
                        if failure_phase == "review-mismatch"
                        else sha,
                        "verdict": "rejected" if failure_phase == "review-rejected" else "approved",
                        "findings": ["finding"] if failure_phase == "review-rejected" else [],
                    }
                )
                if failure_phase == "review-missing":
                    response = "SDK completed, but no review verdict"
                return SimpleNamespace(
                    id="turn-acceptance",
                    status=SimpleNamespace(value="completed"),
                    final_response=response,
                    error=None,
                )
            return sdk_result

        return SimpleNamespace(id="turn-acceptance", run=run)

    thread.turn.side_effect = sdk_turn
    sdk = SimpleNamespace(
        thread_start=AsyncMock(return_value=thread),
        thread_resume=AsyncMock(return_value=thread),
        close=AsyncMock(),
    )
    configure_codex_adapter(OpenAICodexAdapter(lambda: sdk, "0.155.1"))
    receipts = {}
    if isinstance(failure_phase, DeliveryPhase):
        operation_id = (
            f"whole-flow-acceptance:delivery:foundation:{failure_phase.value}"
        )
        receipts[operation_id] = DeliveryReceipt(
            operation_id=operation_id,
            phase=failure_phase,
            outcome=DeliveryOutcome.WAITING
            if failure_phase is DeliveryPhase.CI
            else DeliveryOutcome.UNKNOWN,
            summary="injected unresolved external result",
        )
    github_adapter = FakeDeliveryAdapter(receipts)
    fake_execute = github_adapter.execute

    async def assert_published_before_pr(operation):
        if operation.phase is DeliveryPhase.CLEANUP:
            expected = (
                {100, 500, 501}
                if ":foundation:" in operation.operation_id else {101, 502}
            )
            assert expected.issubset(operation.issue_numbers)
        if operation.phase is DeliveryPhase.PULL_REQUEST:
            remote = git(
                Path(operation.repository),
                "ls-remote",
                "origin",
                f"refs/heads/{operation.candidate_branch}",
            )
            assert remote.split()[0] == operation.candidate_sha
        return await fake_execute(operation)

    github_adapter.execute = assert_published_before_pr
    if failure_phase == "ci-transient":
        execute = github_adapter.execute
        ci_calls = 0

        async def transient_ci(operation):
            nonlocal ci_calls
            if operation.phase is DeliveryPhase.CI:
                ci_calls += 1
                if ci_calls < 3:
                    return DeliveryReceipt(
                        operation_id=operation.operation_id,
                        phase=operation.phase,
                        outcome=DeliveryOutcome.WAITING,
                        summary="CI still running",
                    )
            return await execute(operation)

        github_adapter.execute = transient_ci
    configure_delivery_adapters(LocalGitAdapter(), github_adapter)
    configure_spec_issue_gateway(FakeSpecIssueGateway())
    configure_ticket_issue_gateway(FakeTicketIssueGateway())
    configure_summary_gateway(
        FakeSummaryCommentGateway(fail_find=failure_phase == "summary")
    )
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="whole-flow-acceptance",
                workflows=[
                    RequirementDeliveryWorkflow,
                    RequirementPlanningWorkflow,
                    TicketSchedulerWorkflow,
                    CodexRunWorkflow,
                    DeliveryWorkflow,
                    DeliverySummaryWorkflow,
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
                handle = await environment.client.start_workflow(
                    RequirementDeliveryWorkflow.run,
                    input,
                    id="whole-flow-acceptance",
                    task_queue="whole-flow-acceptance",
                )
                try:
                    result = await asyncio.wait_for(handle.result(), timeout=30)
                except TimeoutError as error:
                    snapshot = await handle.query(
                        RequirementDeliveryWorkflow.get_status
                    )
                    raise AssertionError(f"whole flow stalled at {snapshot}") from error
                snapshot = await handle.query(RequirementDeliveryWorkflow.get_status)
        if failure_phase not in (None, "ci-transient"):
            assert result.status == WholeFlowStatus.FAILED
            assert result.reason
            assert snapshot.status == WholeFlowStatus.FAILED
            assert snapshot.last_error == result.reason
            assert snapshot.next_action == ""
            assert snapshot.deadline is None
            return
        assert result.status == WholeFlowStatus.COMPLETED, result
        assert result.phase == WholeFlowPhase.COMPLETED
        assert observed_active
        assert snapshot.next_action == ""
        assert snapshot.active_ticket is None
        assert snapshot.pending_reason == ""
        assert snapshot.deadline is None
        assert snapshot.completed_specs == ("foundation", "follow-up")
        assert result.delivery_results
        for codex_result in result.codex_results:
            assert (
                codex_result["candidate"]["candidate_sha"]
                != codex_result["candidate"]["base_sha"]
            )
            assert (
                codex_result["review_evidence"]["candidate_sha"]
                == codex_result["candidate"]["candidate_sha"]
            )
        final_foundation = result.codex_results[1]["candidate"]
        files = git(
            Path(final_foundation["workspace"]),
            "ls-tree",
            "--name-only",
            final_foundation["candidate_sha"],
        )
        assert "foundation-a-implemented.txt" in files
        assert "foundation-b-implemented.txt" in files
        assert all(
            run["ticket_publication"]["status"] == "verified"
            for run in result.scheduler["runs"]
        )
        assert all(
            len(run["ticket_publication"]["issues"]) > 0
            for run in result.scheduler["runs"]
        )
        assert all(
            any(receipt["phase"] == DeliveryPhase.PUSH for receipt in item["receipts"])
            for item in result.delivery_results
        )
        assert result.summary is not None
        assert result.summary["comment"] is not None
        assert sdk.thread_start.await_count == 9
        assert thread.turn.call_count == 9
        prompts = [call.args[0] for call in thread.turn.call_args_list]
        for ticket_key in ("foundation-a", "foundation-b", "follow-up-a"):
            ticket_prompts = [
                prompt
                for prompt in prompts
                if f"Ready ticket: {ticket_key}\n" in prompt
            ]
            assert len(ticket_prompts) == 3
        assert prompts[0].startswith("Role: planning")
        assert ".claude/skills/implement-spec/SKILL.md" in prompts[0]
        assert ".claude/skills/to-tickets/SKILL.md" in prompts[0]
        assert ".claude/skills/implement/SKILL.md" in prompts[1]
        assert ".claude/skills/tdd/SKILL.md" in prompts[1]
        assert ".claude/skills/review/SKILL.md" in prompts[2]
        assert all(
            "without requesting human confirmation" in prompt for prompt in prompts
        )
    finally:
        configure_codex_adapter(None)
        configure_delivery_adapters(None, None)
        configure_spec_issue_gateway(None)
        configure_ticket_issue_gateway(None)
        configure_summary_gateway(None)


async def test_historical_chat_is_accepted_without_persisting_inline_source() -> None:
    configure_spec_issue_gateway(FakeSpecIssueGateway())
    configure_ticket_issue_gateway(FakeTicketIssueGateway())
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="historical-chat-acceptance",
                workflows=[RequirementPlanningWorkflow],
                activities=[prepare_grill, publish_spec_issues],
            ):
                handle = await environment.client.start_workflow(
                    RequirementPlanningWorkflow.run,
                    PlanningPayload(
                        origin=SourceOrigin.HISTORICAL_CHAT,
                        source_reference="artifact://acceptance/chat-1",
                        grill_answers=(GrillAnswer(1, "the approved chat decision"),),
                        confirmation_operation_id="historical:confirm",
                        publication_operation_id="historical:publish",
                    ),
                    id="historical-chat-acceptance",
                    task_queue="historical-chat-acceptance",
                )
                result = await handle.result()
        assert result.source.source_reference == "artifact://acceptance/chat-1"
        assert result.source.source_identity
        assert result.source.source_reference != "the approved chat decision"
    finally:
        configure_spec_issue_gateway(None)
        configure_ticket_issue_gateway(None)


async def test_parent_reports_publication_retry_and_terminal_blocked_status() -> None:
    configure_spec_issue_gateway(FakeSpecIssueGateway(fail_create=True))
    input = whole_flow_input()
    input = replace(input, planning=replace(
        input.planning, publication_max_attempts=2,
        publication_retry_backoff_seconds=60,
    ))
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client, task_queue="status-retry",
                workflows=[RequirementDeliveryWorkflow, RequirementPlanningWorkflow],
                activities=[prepare_grill, publish_spec_issues],
            ):
                handle = await environment.client.start_workflow(
                    RequirementDeliveryWorkflow.run, input,
                    id="status-retry", task_queue="status-retry",
                )
                for _ in range(100):
                    snapshot = await handle.query(RequirementDeliveryWorkflow.get_status)
                    if snapshot.status == WholeFlowStatus.RETRYING:
                        break
                    await asyncio.sleep(0.01)
                assert snapshot.status == WholeFlowStatus.RETRYING
                assert snapshot.phase == WholeFlowPhase.PLANNING
                assert snapshot.retry_count == 1
                assert snapshot.deadline and snapshot.last_error
                assert snapshot.pending_reason
                assert "retry" in snapshot.next_action
                result = await handle.result()
                terminal = await handle.query(RequirementDeliveryWorkflow.get_status)
                assert result.status == WholeFlowStatus.BLOCKED
                assert terminal.status == WholeFlowStatus.BLOCKED
                assert terminal.last_error == result.reason
                assert terminal.pending_reason == result.reason
                assert terminal.next_action == ""
                assert terminal.deadline is None
                assert terminal.retry_count == 1
    finally:
        configure_spec_issue_gateway(None)


def test_whole_flow_rejects_future_cross_spec_blocker_before_starting_children() -> (
    None
):
    input = whole_flow_input()
    scheduler = SchedulerInput(
        specs=input.scheduler.specs,
        tickets=(
            TicketPlan("foundation-a", "foundation", ("follow-up-a",)),
            TicketPlan("foundation-b", "foundation"),
            TicketPlan("follow-up-a", "follow-up"),
        ),
        completion_operations=input.scheduler.completion_operations,
    )
    errors = validate_whole_flow_input(
        WholeFlowInput(
            planning=input.planning,
            scheduler=scheduler,
            codex=input.codex,
            deliveries=input.deliveries,
            summary=input.summary,
        )
    )

    assert any("later or unrelated" in error for error in errors)


def test_acceptance_directory_has_no_local_spec_mirror() -> None:
    assert not (Path(__file__).parents[3] / "docs" / "specs").exists()
