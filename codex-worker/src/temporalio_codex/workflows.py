from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.activities import foundation_stage
    from temporalio_codex.models import RunInput, RunResult, StageInput


@workflow.defn
class CodexRunWorkflow:
    @workflow.run
    async def run(self, input: RunInput) -> RunResult:
        stage_result = await workflow.execute_activity(
            foundation_stage,
            StageInput(requirement=input.requirement),
            start_to_close_timeout=timedelta(seconds=30),
        )
        return RunResult(
            workflow_id=workflow.info().workflow_id,
            status=stage_result.outcome,
            stage=stage_result.stage,
            summary=stage_result.summary,
        )
