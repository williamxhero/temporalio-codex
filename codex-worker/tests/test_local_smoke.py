from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.activities import foundation_stage
from temporalio_codex.client import execute_run
from temporalio_codex.models import RunStatus, StageOutcome
from temporalio_codex.workflows import CodexRunWorkflow


async def test_local_temporal_server_smoke() -> None:
    async with await WorkflowEnvironment.start_local() as environment:
        async with Worker(
            environment.client,
            task_queue="local-smoke",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            result = await execute_run(
                environment.client,
                "run against a local Temporal Server",
                workflow_id="local-smoke-run",
                task_queue="local-smoke",
            )

    assert result.status is RunStatus.COMPLETED
    assert result.outcome is StageOutcome.COMPLETED
