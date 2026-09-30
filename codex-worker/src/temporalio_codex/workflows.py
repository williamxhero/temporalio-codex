from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.activities import codex_stage, foundation_stage
    from temporalio_codex.candidate_activities import (
        CandidateCaptureInput,
        capture_codex_candidate,
    )
    from temporalio_codex.codex_models import (
        CodexObservation,
        CodexOperation,
        CodexOutcome,
    )
    from temporalio_codex.execution_status import ExecutionProgress, report_progress
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


_PROJECT_SKILLS_BY_ROLE: dict[str, tuple[str, ...]] = {
    "planning": (
        ".claude/skills/implement-spec/SKILL.md",
        ".claude/skills/to-tickets/SKILL.md",
    ),
    "implementation": (
        ".claude/skills/implement/SKILL.md",
        ".claude/skills/tdd/SKILL.md",
    ),
    "review": (".claude/skills/review/SKILL.md",),
}


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
        self._paused_from: RunStatus | None = None
        self._external_recheck_count = 0
        self._progress: ExecutionProgress | None = None
        self._candidate = None
        self._review_evidence = None

    @workflow.query(name="get_status")
    def get_status(self) -> RunSnapshot:
        progress = self._progress if self._status is RunStatus.ACTIVE else None
        return RunSnapshot(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            current_stage=self._current_stage,
            completed_stages=tuple(self._completed_stages),
            pending_input=self._pending_input,
            stage_results=tuple(self._stage_results),
            external_recheck_count=self._external_recheck_count,
            pending_reason=self._pending_input or "",
            next_action=progress.next_action if progress else "",
            retry_count=progress.retry_count if progress else 0,
            deadline=progress.deadline if progress else None,
            timeout_seconds=progress.timeout_seconds if progress else None,
            last_error=(self._stage_results[-1].summary
                        if self._status is RunStatus.FAILED and self._stage_results else None),
            workflow_run_id=workflow.info().run_id,
        )

    @workflow.run
    async def run(self, input: RunInput) -> RunResult:
        self._candidate = input.candidate
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
            await self._wait_if_paused()
            if self._status is RunStatus.CANCELLED:
                return self._cancelled_result()

            self._current_stage = stage.key
            answer: str | None = None
            if stage.requires_input and input.automatic:
                answer = input.requirement.strip()
                if not answer:
                    self._status = RunStatus.FAILED
                    return RunResult(
                        workflow_id=workflow.info().workflow_id,
                        status=self._status,
                        outcome=StageOutcome.FAILED,
                        stage=stage.key,
                        summary="Required stage input cannot be derived from an empty requirement",
                    )
            elif stage.requires_input:
                self._status = RunStatus.WAITING_FOR_INPUT
                self._pending_input = f"Input required for stage: {stage.key}"
                await workflow.wait_condition(
                    lambda: (
                        self._answer is not None or self._status is RunStatus.CANCELLED
                    )
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
                        heartbeat_timeout=timedelta(seconds=30),
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
                    if (
                        self._candidate
                        and stage_result.outcome is StageOutcome.COMPLETED
                        and stage.role.value in ("implementation", "review")
                    ):
                        captured = await workflow.execute_activity(
                            capture_codex_candidate,
                            CandidateCaptureInput(
                                candidate=self._candidate,
                                expected_sha=self._candidate.candidate_sha
                                if stage.role.value == "review"
                                else None,
                                review_json=stage_result.summary
                                if stage.role.value == "review"
                                else None,
                                operation_id=stage_result.operation_id or "",
                                thread_id=stage_result.thread_id or "",
                                turn_id=stage_result.turn_id or "",
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(maximum_attempts=1),
                        )
                        self._candidate = captured.candidate
                        self._review_evidence = captured.review
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
                if input.automatic:
                    self._status = RunStatus.FAILED
                    return RunResult(
                        workflow_id=workflow.info().workflow_id,
                        status=self._status,
                        outcome=stage_result.outcome,
                        stage=stage_result.stage,
                        summary=stage_result.summary,
                    )
                elif stage_result.outcome is StageOutcome.UNKNOWN:
                    self._status = RunStatus.WAITING_FOR_EXTERNAL_OBSERVATION
                    await workflow.wait_condition(
                        lambda: (
                            self._external_resolution is not None
                            or self._status is RunStatus.CANCELLED
                        )
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
                        if stage.external_recheck_seconds <= 0:
                            self._status = RunStatus.FAILED
                            return RunResult(
                                workflow_id=workflow.info().workflow_id,
                                status=self._status,
                                outcome=stage_result.outcome,
                                stage=stage_result.stage,
                                summary=stage_result.summary,
                            )
                        await workflow.sleep(
                            timedelta(seconds=stage.external_recheck_seconds)
                        )
                        self._external_recheck_count += 1
                        self._external_resolution = None
                        self._status = RunStatus.WAITING_FOR_EXTERNAL_OBSERVATION
                        await workflow.wait_condition(
                            lambda: (
                                self._external_resolution is not None
                                or self._status is RunStatus.CANCELLED
                            )
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
            candidate=self._candidate,
            review_evidence=self._review_evidence,
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
        if self._candidate and stage.role.value == "review":
            prompt += (
                f"\nIndependently review frozen candidate SHA {self._candidate.candidate_sha}. "
                "Do not modify or commit files during review. Final response must be exactly "
                "a JSON object with candidate_sha, verdict (approved or rejected), and findings "
                "(an array). Approval requires no findings. SDK completion alone is not approval."
            )
        answer_number = 0
        while True:
            self._progress = await report_progress(
                phase="codex", next_action=f"execute Codex {stage.key} turn",
                timeout_seconds=stage.start_to_close_timeout_seconds,
                deadline=(workflow.now() + timedelta(
                    seconds=stage.start_to_close_timeout_seconds
                )).isoformat(),
                retry_count=answer_number,
            )
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
                    parent_workflow_id=input.parent_workflow_id,
                    workflow_run_id=workflow.info().run_id,
                    parent_workflow_run_id=input.parent_workflow_run_id,
                    namespace=workflow.info().namespace,
                    output_schema={
                        "type": "object",
                        "properties": {
                            "candidate_sha": {"type": "string"},
                            "verdict": {"type": "string", "enum": ["approved", "rejected"]},
                            "findings": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["candidate_sha", "verdict", "findings"],
                        "additionalProperties": False,
                    } if self._candidate and stage.role.value == "review" else None,
                ),
                start_to_close_timeout=timedelta(
                    seconds=stage.start_to_close_timeout_seconds
                ),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=10),
                    maximum_interval=timedelta(milliseconds=50),
                    # A timeout cannot prove that the SDK turn was not started.
                    maximum_attempts=1 if input.automatic else 3,
                    non_retryable_error_types=["UnknownExternalOutcome"],
                ),
            )
            if observation.outcome is not CodexOutcome.PENDING_INPUT:
                return self._stage_result_from_codex(stage, observation)

            if input.automatic:
                if (
                    answer_number >= input.automatic_input_max_attempts
                    or not input.requirement.strip()
                    or not observation.thread_id
                ):
                    return replace(
                        self._stage_result_from_codex(stage, observation),
                        outcome=StageOutcome.FAILED,
                        summary="Automatic input exhausted or cannot safely resume the pending turn: "
                        + (observation.pending_question or observation.summary),
                        failure="automatic_input_unresolved",
                    )
                answer = (
                    f"Question: {observation.pending_question or observation.summary}\n"
                    f"Authorized requirement: {input.requirement}\n"
                    f"Allowed scope: {', '.join(stage.allowed_scope)}\n"
                    "Derive the answer only from this context and project defaults. "
                    "Do not invent authorization or credentials. If the answer cannot "
                    "be safely derived, return a failed result with a reason."
                )
            else:
                self._status = RunStatus.WAITING_FOR_INPUT
                self._pending_input = observation.pending_question
                self._answer = None
                await workflow.wait_condition(
                    lambda: (
                        self._answer is not None or self._status is RunStatus.CANCELLED
                    )
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
        skill_paths = _PROJECT_SKILLS_BY_ROLE.get(stage.role.value, ())
        skill_instructions = "\n".join(f"- {path}" for path in skill_paths)
        prompt = (
            f"Role: {stage.role.value}\n"
            f"Requirement: {requirement}\n"
            "Execution policy: read and follow the required project-local skills "
            "below from the repository root before acting. Use their default "
            "values and continue automatically without requesting human "
            "confirmation. Do not use global or external skill copies.\n"
            f"Required project-local skills:\n{skill_instructions}"
        )
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

    async def _wait_if_paused(self) -> None:
        if self._status is not RunStatus.PAUSED:
            return
        await workflow.wait_condition(
            lambda: (
                self._status is not RunStatus.PAUSED
                or self._status is RunStatus.CANCELLED
            )
        )

    @workflow.update(name="pause")
    async def pause(self) -> bool:
        if self._status in (
            RunStatus.COMPLETED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.PAUSED,
        ):
            return False
        self._paused_from = self._status
        self._status = RunStatus.PAUSED
        return True

    @workflow.update(name="resume")
    async def resume(self) -> bool:
        if self._status is not RunStatus.PAUSED:
            return False
        self._status = self._paused_from or RunStatus.ACTIVE
        self._paused_from = None
        return True

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
