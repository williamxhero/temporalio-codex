import asyncio

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker import Replayer

from temporalio_codex.activities import (
    configure_delivery_adapters,
    delivery_git_stage,
    delivery_github_stage,
)
from temporalio_codex.delivery_adapter import FakeDeliveryAdapter
from temporalio_codex.delivery_models import (
    DeliveryInput,
    DeliveryOutcome,
    DeliveryPhase,
    DeliveryReceipt,
    DeliveryStatus,
)
from temporalio_codex.delivery_workflows import DeliveryWorkflow


def delivery_input() -> DeliveryInput:
    return DeliveryInput(
        repository="D:/repo",
        workspace="D:/workspace",
        base_sha="base-1",
        candidate_branch="codex/run-1",
        pull_request_identity="operation:run-1",
        title="delivery",
        body="operation:run-1",
        issue_numbers=(23, 24),
    )


async def run_delivery(run_id: str, git=None, github=None):
    configure_delivery_adapters(git or FakeDeliveryAdapter(), github or FakeDeliveryAdapter())
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue=run_id,
                workflows=[DeliveryWorkflow],
                activities=[delivery_git_stage, delivery_github_stage],
            ):
                handle = await environment.client.start_workflow(
                    DeliveryWorkflow.run,
                    delivery_input(),
                    id=run_id,
                    task_queue=run_id,
                )
                result = await handle.result()
                snapshot = await handle.query(DeliveryWorkflow.get_status)
        return result, snapshot
    finally:
        configure_delivery_adapters(None, None)


async def test_delivery_workflow_runs_all_phases_with_fake_adapters() -> None:
    result, snapshot = await run_delivery("delivery-complete")

    assert result.status is DeliveryStatus.COMPLETED
    assert result.outcome is DeliveryOutcome.COMPLETED
    assert snapshot.status is DeliveryStatus.COMPLETED
    assert [receipt.phase for receipt in snapshot.receipts] == [
        DeliveryPhase.CANDIDATE,
        DeliveryPhase.ACCEPTANCE,
        DeliveryPhase.REVIEW,
        DeliveryPhase.PULL_REQUEST,
        DeliveryPhase.CI,
        DeliveryPhase.MERGE,
        DeliveryPhase.CLEANUP,
    ]


async def test_unknown_delivery_write_waits_for_matching_readback() -> None:
    unknown = DeliveryReceipt(
        operation_id="delivery-unknown:pull-request",
        phase=DeliveryPhase.PULL_REQUEST,
        outcome=DeliveryOutcome.UNKNOWN,
        summary="create response is unknown",
        candidate_sha="candidate-delivery-unknown:candidate",
        readback_required=True,
    )
    github = FakeDeliveryAdapter({unknown.operation_id: unknown})
    configure_delivery_adapters(FakeDeliveryAdapter(), github)
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="delivery-unknown",
                workflows=[DeliveryWorkflow],
                activities=[delivery_git_stage, delivery_github_stage],
            ):
                handle = await environment.client.start_workflow(
                    DeliveryWorkflow.run,
                    delivery_input(),
                    id="delivery-unknown",
                    task_queue="delivery-unknown",
                )
                for _ in range(100):
                    snapshot = await handle.query(DeliveryWorkflow.get_status)
                    if snapshot.status is DeliveryStatus.WAITING_FOR_READBACK:
                        break
                    await asyncio.sleep(0.01)
                assert snapshot.status is DeliveryStatus.WAITING_FOR_READBACK
                invalid = DeliveryReceipt(
                    operation_id="wrong",
                    phase=DeliveryPhase.PULL_REQUEST,
                    outcome=DeliveryOutcome.COMPLETED,
                    summary="wrong receipt",
                )
                assert (
                    await handle.execute_update(
                        DeliveryWorkflow.resolve_delivery_readback,
                        invalid,
                        result_type=bool,
                    )
                    is True
                )
                result = await handle.result()
        assert result.status is DeliveryStatus.FAILED
    finally:
        configure_delivery_adapters(None, None)


async def test_delivery_workflow_replays_completed_history() -> None:
    configure_delivery_adapters(FakeDeliveryAdapter(), FakeDeliveryAdapter())
    try:
        async with await WorkflowEnvironment.start_time_skipping() as environment:
            async with Worker(
                environment.client,
                task_queue="delivery-replay",
                workflows=[DeliveryWorkflow],
                activities=[delivery_git_stage, delivery_github_stage],
            ):
                handle = await environment.client.start_workflow(
                    DeliveryWorkflow.run,
                    delivery_input(),
                    id="delivery-replay",
                    task_queue="delivery-replay",
                )
                result = await handle.result()
                history = await handle.fetch_history()
        assert result.status is DeliveryStatus.COMPLETED
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    finally:
        configure_delivery_adapters(None, None)
