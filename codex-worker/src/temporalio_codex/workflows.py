from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.activities import foundation_stage
    from temporalio_codex.models import (
        RunInput,
        RunResult,
        RunSnapshot,
        RunStatus,
        StageInput,
        StageOutcome,
        StageResult,
    )


@workflow.defn
class CodexRunWorkflow:
    def __init__(self) -> None:
        self._status = RunStatus.ACTIVE
        self._current_stage: str | None = None
        self._completed_stages: list[str] = []
        self._pending_input: str | None = None
        self._answer: str | None = None
        self._stage_results: list[StageResult] = []

    @workflow.query(name="get_status")
    def get_status(self) -> RunSnapshot:
        return RunSnapshot(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            current_stage=self._current_stage,
            completed_stages=tuple(self._completed_stages),
            pending_input=self._pending_input,
            stage_results=tuple(self._stage_results),
        )

    @workflow.run
    async def run(self, input: RunInput) -> RunResult:
        if not input.stages:
            self._status = RunStatus.FAILED
            return RunResult(
                workflow_id=workflow.info().workflow_id,
                status=self._status,
                outcome=StageOutcome.FAILED,
                stage="",
                summary="At least one stage is required",
            )

        stage_result: StageResult | None = None
        for stage in input.stages:
            if self._status is RunStatus.CANCELLED:
                return self._cancelled_result()

            self._current_stage = stage.key
            answer: str | None = None
            if stage.requires_input:
                self._status = RunStatus.WAITING_FOR_INPUT
                self._pending_input = f"Input required for stage: {stage.key}"
                await workflow.wait_condition(
                    lambda: self._answer is not None
                    or self._status is RunStatus.CANCELLED
                )
                if self._status is RunStatus.CANCELLED:
                    return self._cancelled_result()
                answer = self._answer
                self._answer = None
                self._pending_input = None
                self._status = RunStatus.ACTIVE

            stage_result = await workflow.execute_activity(
                foundation_stage,
                StageInput(
                    stage=stage.key,
                    requirement=input.requirement,
                    answer=answer,
                ),
                start_to_close_timeout=timedelta(seconds=30),
            )
            self._stage_results.append(stage_result)
            if self._status is RunStatus.CANCELLED:
                return self._cancelled_result()
            if stage_result.outcome is not StageOutcome.COMPLETED:
                self._status = RunStatus.FAILED
                return RunResult(
                    workflow_id=workflow.info().workflow_id,
                    status=self._status,
                    outcome=stage_result.outcome,
                    stage=stage_result.stage,
                    summary=stage_result.summary,
                )
            self._completed_stages.append(stage.key)

        assert stage_result is not None
        self._status = RunStatus.COMPLETED
        self._current_stage = None
        return RunResult(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            outcome=stage_result.outcome,
            stage=stage_result.stage,
            summary=stage_result.summary,
        )

    def _cancelled_result(self) -> RunResult:
        return RunResult(
            workflow_id=workflow.info().workflow_id,
            status=RunStatus.CANCELLED,
            outcome=StageOutcome.CANCELLED,
            stage=self._current_stage or "",
            summary="Run cancelled before all stages completed",
        )

    @workflow.signal(name="cancel")
    async def cancel(self) -> None:
        if self._status not in (RunStatus.COMPLETED, RunStatus.FAILED):
            self._status = RunStatus.CANCELLED

    @workflow.update(name="submit_answer")
    async def submit_answer(self, answer: str) -> bool:
        if self._status is not RunStatus.WAITING_FOR_INPUT or not answer.strip():
            return False
        self._answer = answer
        self._status = RunStatus.ACTIVE
        return True
