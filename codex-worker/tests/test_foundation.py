from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporalio_codex.activities import foundation_stage
from temporalio_codex.models import RunInput, StageOutcome
from temporalio_codex.workflows import CodexRunWorkflow


async def test_foundation_workflow_uses_public_input_and_result_seams() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="foundation-test",
            workflows=[CodexRunWorkflow],
            activities=[foundation_stage],
        ):
            result = await environment.client.execute_workflow(
                CodexRunWorkflow.run,
                RunInput(requirement="build a durable Codex flow"),
                id="foundation-test-run",
                task_queue="foundation-test",
            )

    assert result.status is StageOutcome.COMPLETED
    assert result.stage == "foundation"
    assert result.summary == "Accepted requirement: build a durable Codex flow"
