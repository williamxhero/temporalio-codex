from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.activities import codex_stage, foundation_stage
    from temporalio_codex.codex_models import (
        CodexObservation,
        CodexOperation,
        CodexOutcome,
    )
    from temporalio_codex.models import (
        RunInput,
        RunResult,
        RunSnapshot,
        RunStatus,
        StageDefinition,
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
        self._external_resolution: bool | None = None
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

            try:
                if stage.role is None:
                    stage_result = await workflow.execute_activity(
                        foundation_stage,
                        StageInput(
                            stage=stage.key,
                            requirement=input.requirement,
                            answer=answer,
                        ),
                        start_to_close_timeout=timedelta(
                            seconds=stage.start_to_close_timeout_seconds
                        ),
                        retry_policy=RetryPolicy(
                            initial_interval=timedelta(milliseconds=10),
                            maximum_interval=timedelta(milliseconds=50),
                            maximum_attempts=3,
                            non_retryable_error_types=["UnknownExternalOutcome"],
                        ),
                    )
                else:
                    stage_result = await self._run_codex_stage(
                        input,
                        stage,
                        answer,
                    )
            except ActivityError as error:
                if isinstance(error.cause, TimeoutError) or (
                    isinstance(error.cause, ApplicationError)
                    and error.cause.type == "UnknownExternalOutcome"
                ):
                    stage_result = StageResult(
                        stage=stage.key,
                        outcome=StageOutcome.UNKNOWN,
                        summary="Activity timed out; external outcome requires readback",
                    )
                else:
                    stage_result = StageResult(
                        stage=stage.key,
                        outcome=StageOutcome.FAILED,
                        summary=f"Activity failed: {error}",
                    )
            self._stage_results.append(stage_result)
            if self._status is RunStatus.CANCELLED:
                return self._cancelled_result()
            if stage_result.outcome is not StageOutcome.COMPLETED:
                if stage_result.outcome is StageOutcome.UNKNOWN:
                    self._status = RunStatus.WAITING_FOR_EXTERNAL_OBSERVATION
                    await workflow.wait_condition(
                        lambda: self._external_resolution is not None
                        or self._status is RunStatus.CANCELLED
                    )
                    if self._status is RunStatus.CANCELLED:
                        return self._cancelled_result()
                    if self._external_resolution:
                        stage_result = replace(
                            stage_result,
                            outcome=StageOutcome.COMPLETED,
                            summary="External outcome confirmed by readback",
                        )
                        self._stage_results[-1] = stage_result
                        self._external_resolution = None
                        self._status = RunStatus.ACTIVE
                    else:
                        self._status = RunStatus.FAILED
                        return RunResult(
                            workflow_id=workflow.info().workflow_id,
                            status=self._status,
                            outcome=stage_result.outcome,
                            stage=stage_result.stage,
                            summary=stage_result.summary,
                        )
                else:
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

    async def _run_codex_stage(
        self,
        input: RunInput,
        stage: StageDefinition,
        answer: str | None,
    ) -> StageResult:
        assert stage.role is not None
        operation_id = f"{workflow.info().workflow_id}:{stage.key}:{stage.role.value}"
        thread_id = stage.thread_id
        prompt = self._codex_prompt(input.requirement, stage, answer)
        answer_number = 0
        while True:
            observation = await workflow.execute_activity(
                codex_stage,
                CodexOperation(
                    operation_id=operation_id,
                    run_id=workflow.info().workflow_id,
                    stage=stage.key,
                    role=stage.role,
                    repository=stage.repository,
                    allowed_scope=stage.allowed_scope,
                    approval_policy=stage.approval_policy,
                    model=stage.model,
                    effort=stage.effort,
                    prompt=prompt,
                    thread_id=thread_id,
                ),
                start_to_close_timeout=timedelta(
                    seconds=stage.start_to_close_timeout_seconds
                ),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=10),
                    maximum_interval=timedelta(milliseconds=50),
                    maximum_attempts=3,
                    non_retryable_error_types=["UnknownExternalOutcome"],
                ),
            )
            if observation.outcome is not CodexOutcome.PENDING_INPUT:
                return self._stage_result_from_codex(stage, observation)

            self._status = RunStatus.WAITING_FOR_INPUT
            self._pending_input = observation.pending_question
            self._answer = None
            await workflow.wait_condition(
                lambda: self._answer is not None
                or self._status is RunStatus.CANCELLED
            )
            if self._status is RunStatus.CANCELLED:
                return self._stage_result_from_codex(stage, observation)
            answer = self._answer
            self._answer = None
            self._pending_input = None
            self._status = RunStatus.ACTIVE
            answer_number += 1
            operation_id = (
                f"{workflow.info().workflow_id}:{stage.key}:{stage.role.value}"
                f":answer-{answer_number}"
            )
            thread_id = observation.thread_id
            prompt = self._codex_prompt(input.requirement, stage, answer)

    @staticmethod
    def _codex_prompt(
        requirement: str,
        stage: StageDefinition,
        answer: str | None,
    ) -> str:
        prompt = f"Role: {stage.role.value}\nRequirement: {requirement}"
        if answer is not None:
            prompt += f"\nBusiness answer: {answer}"
        return prompt

    @staticmethod
    def _stage_result_from_codex(
        stage: StageDefinition,
        observation: CodexObservation,
    ) -> StageResult:
        outcome = {
            CodexOutcome.COMPLETED: StageOutcome.COMPLETED,
            CodexOutcome.PENDING_INPUT: StageOutcome.WAITING_FOR_INPUT,
            CodexOutcome.FAILED: StageOutcome.FAILED,
            CodexOutcome.UNKNOWN: StageOutcome.UNKNOWN,
        }[observation.outcome]
        return StageResult(
            stage=stage.key,
            outcome=outcome,
            summary=observation.summary,
            evidence_refs=observation.evidence_refs,
            pending_input=observation.pending_question,
            role=observation.role.value,
            operation_id=observation.operation_id,
            thread_id=observation.thread_id,
            turn_id=observation.turn_id,
            failure=observation.failure.value if observation.failure else None,
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

    @workflow.update(name="resolve_external_observation")
    async def resolve_external_observation(self, completed: bool) -> bool:
        if self._status is not RunStatus.WAITING_FOR_EXTERNAL_OBSERVATION:
            return False
        self._external_resolution = completed
        return True
