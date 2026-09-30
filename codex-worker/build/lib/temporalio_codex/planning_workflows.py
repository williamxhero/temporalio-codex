from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.execution_status import report_progress
    from temporalio_codex.planning_activities import prepare_grill, publish_spec_issues
    from temporalio_codex.planning_models import (
        GrillAnswer,
        GrillPreparationInput,
        GrillRecord,
        PlanningInput,
        PlanningPhase,
        PlanningResult,
        PlanningSnapshot,
        PlanningStatus,
        SourceRecord,
    )
    from temporalio_codex.spec_issue_adapter import (
        SpecPublicationInput,
        SpecPublicationResult,
        SpecPublicationStatus,
    )


@workflow.defn
class RequirementPlanningWorkflow:
    def __init__(self) -> None:
        self._phase = PlanningPhase.INTAKE
        self._status = PlanningStatus.ACTIVE
        self._source: SourceRecord | None = None
        self._grill = GrillRecord()
        self._confirmed = False
        self._confirmation_operation_id: str | None = None
        self._publication_requested = False
        self._publication_operation_id: str | None = None
        self._published_specs = ()
        self._publication_reason = ""
        self._publication_resolution: SpecPublicationResult | None = None
        self._publication_retry_requested = False
        self._cancelled = False
        self._paused = False

    @workflow.query(name="get_planning_status")
    def get_status(self) -> PlanningSnapshot:
        return PlanningSnapshot(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            source=self._source,
            grill=self._grill,
            confirmed=self._confirmed,
            confirmation_operation_id=self._confirmation_operation_id,
            publication_requested=self._publication_requested,
            publication_operation_id=self._publication_operation_id,
            published_specs=self._published_specs,
            publication_reason=self._publication_reason,
        )

    @workflow.run
    async def run(self, input: PlanningInput) -> PlanningResult:
        self._source = SourceRecord(
            origin=input.origin,
            source_identity=input.source_identity,
            source_reference=input.source_reference,
        )
        self._phase = PlanningPhase.GRILLING
        self._status = PlanningStatus.WAITING_FOR_INPUT
        await report_progress(
            phase="planning", next_action="derive requirement planning questions",
            timeout_seconds=30,
            deadline=(workflow.now() + timedelta(seconds=30)).isoformat(),
        )
        questions = await workflow.execute_activity(
            prepare_grill,
            GrillPreparationInput(input.origin, input.source_identity),
            start_to_close_timeout=timedelta(seconds=30),
        )
        self._grill = GrillRecord(questions=questions)
        for answer in input.grill_answers:
            if not self._record_grill_answer(answer):
                self._status = PlanningStatus.BLOCKED
                self._publication_reason = "invalid initial Grill answer"
                return self._planning_result()
        if not self._required_questions_answered():
            answers = self._derive_grill_answers(input)
            if answers is None or any(not self._record_grill_answer(answer) for answer in answers):
                self._status = PlanningStatus.BLOCKED
                self._publication_reason = (
                    "unable to safely derive required Grill answers from requirement context"
                )
                return self._planning_result()
        await workflow.wait_condition(
            lambda: (self._required_questions_answered() and not self._paused)
            or self._cancelled
        )
        if self._cancelled:
            return self._cancelled_result()

        self._phase = PlanningPhase.CONFIRMATION_REQUIRED
        self._status = PlanningStatus.ACTIVE
        if input.confirmation_operation_id:
            self._confirmed = True
            self._confirmation_operation_id = input.confirmation_operation_id
        else:
            self._confirmed = True
            self._confirmation_operation_id = (
                f"planning-confirm:{self._source.source_identity}"
            )
        await workflow.wait_condition(
            lambda: (self._confirmed and not self._paused) or self._cancelled
        )
        if self._cancelled:
            return self._cancelled_result()

        self._phase = PlanningPhase.READY
        self._status = PlanningStatus.READY
        if input.publication_operation_id:
            self._publication_requested = True
            self._publication_operation_id = input.publication_operation_id
        else:
            self._publication_requested = True
            self._publication_operation_id = (
                f"planning-publish:{self._source.source_identity}"
            )
        await workflow.wait_condition(
            lambda: (self._publication_requested and not self._paused) or self._cancelled
        )
        if self._cancelled:
            return self._cancelled_result()
        if not input.specs:
            self._phase = PlanningPhase.COMPLETED
            self._status = PlanningStatus.COMPLETED
            return self._planning_result()
        self._phase = PlanningPhase.READY
        self._status = PlanningStatus.PUBLISHING
        await report_progress(phase="planning", next_action="publish and verify SPEC Issues",
                              timeout_seconds=max(30.0, input.publication_timeout_seconds),
                              deadline=(workflow.now() + timedelta(
                                  seconds=max(30.0, input.publication_timeout_seconds))).isoformat())
        publication = await self._publish_specs(input)
        attempt = 1
        while publication.status is SpecPublicationStatus.UNKNOWN:
            self._status = PlanningStatus.UNKNOWN
            self._publication_reason = publication.reason
            if attempt >= input.publication_max_attempts:
                self._status = PlanningStatus.BLOCKED
                self._publication_reason = (
                    f"SPEC publication readback timed out or remained unknown after {attempt} "
                    f"attempts: {publication.reason}"
                )
                await report_progress(
                    phase="planning", status="blocked",
                    retry_count=attempt - 1,
                    pending_reason=self._publication_reason,
                    last_error=self._publication_reason,
                )
                return self._planning_result()
            if self._cancelled:
                return self._cancelled_result()
            backoff = input.publication_retry_backoff_seconds * (2 ** (attempt - 1))
            await report_progress(
                phase="planning", status="retrying", retry_count=attempt,
                pending_reason="SPEC publication outcome requires readback",
                next_action="reconcile and retry SPEC publication",
                deadline=(workflow.now() + timedelta(seconds=backoff)).isoformat(),
                last_error=publication.reason,
            )
            if backoff:
                await workflow.sleep(backoff)
            if self._cancelled:
                return self._cancelled_result()
            # A recovery update may arrive during the backoff. It is treated as
            # an optional acceleration; normal execution always retries itself.
            self._publication_retry_requested = False
            self._publication_resolution = None
            self._status = PlanningStatus.PUBLISHING
            attempt += 1
            await report_progress(phase="planning", next_action="reconcile SPEC publication",
                                  retry_count=attempt - 1,
                                  timeout_seconds=max(30.0, input.publication_timeout_seconds),
                                  deadline=(workflow.now() + timedelta(
                                      seconds=max(30.0, input.publication_timeout_seconds))).isoformat())
            publication = await self._publish_specs(input)
        if publication.status is not SpecPublicationStatus.VERIFIED:
            self._status = (
                PlanningStatus.BLOCKED
                if publication.status is SpecPublicationStatus.BLOCKED
                else PlanningStatus.UNKNOWN
            )
            return self._planning_result()
        self._phase = PlanningPhase.COMPLETED
        self._status = PlanningStatus.COMPLETED
        return self._planning_result()

    def _derive_grill_answers(self, input: PlanningInput) -> tuple[GrillAnswer, ...] | None:
        """Derive deterministic answers for every required question without an operator."""
        answered = {answer.question_number for answer in self._grill.answers}
        required = tuple(
            question
            for question in self._grill.questions
            if question.required and question.number not in answered
        )
        if not required:
            return ()
        context = (input.source_text or "").strip()
        if not context:
            return None
        return tuple(GrillAnswer(question.number, context) for question in required)

    async def _publish_specs(self, input: PlanningInput) -> SpecPublicationResult:
        try:
            publication = await workflow.execute_activity(
                publish_spec_issues,
                SpecPublicationInput(
                    repository=input.repository,
                    umbrella_issue_number=input.umbrella_issue_number,
                    source_identity=self._source.source_identity if self._source else "",
                    operation_id=self._publication_operation_id or "",
                    drafts=input.specs,
                ),
                start_to_close_timeout=timedelta(
                    seconds=max(30.0, input.publication_timeout_seconds)
                ),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        except ActivityError as error:
            publication = SpecPublicationResult(
                SpecPublicationStatus.UNKNOWN,
                self._published_specs,
                "SPEC publication activity failed; recheck operation identities before retry: "
                f"{type(error.cause).__name__}",
            )
        self._published_specs = publication.issues
        self._publication_reason = publication.reason
        return publication

    def _planning_result(self) -> PlanningResult:
        assert self._source is not None
        return PlanningResult(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            phase=self._phase,
            source=self._source,
            grill=self._grill,
            confirmation_operation_id=self._confirmation_operation_id or "",
            publication_operation_id=self._publication_operation_id or "",
            published_specs=self._published_specs,
            publication_reason=self._publication_reason,
        )

    def _required_questions_answered(self) -> bool:
        answered = {answer.question_number for answer in self._grill.answers}
        return all(
            not question.required or question.number in answered
            for question in self._grill.questions
        )

    @workflow.update(name="answer_grill")
    async def answer_grill(
        self,
        answer: GrillAnswer,
    ) -> bool:
        if self._phase is not PlanningPhase.GRILLING:
            return False
        return self._record_grill_answer(answer)

    @workflow.signal(name="answer_grill_signal")
    async def answer_grill_signal(self, answer: GrillAnswer) -> None:
        self._record_grill_answer(answer)

    @workflow.signal(name="pause_planning")
    async def pause_planning(self) -> None:
        if self._status not in (PlanningStatus.COMPLETED, PlanningStatus.CANCELLED):
            self._paused = True

    @workflow.signal(name="resume_planning")
    async def resume_planning(self) -> None:
        self._paused = False

    def _record_grill_answer(self, answer: GrillAnswer) -> bool:
        if not answer.answer.strip():
            return False
        if answer.question_number not in {
            question.number for question in self._grill.questions
        }:
            return False
        if answer.question_number in {
            item.question_number for item in self._grill.answers
        }:
            return False
        answers = (*self._grill.answers, answer)
        assumptions = self._grill.assumptions
        decisions = self._grill.decisions
        if answer.accepted_as_assumption:
            assumptions = (*assumptions, answer.answer)
        else:
            decisions = (*decisions, answer.answer)
        self._grill = GrillRecord(
            questions=self._grill.questions,
            answers=answers,
            assumptions=assumptions,
            decisions=decisions,
            unresolved=self._grill.unresolved,
        )
        return True

    @workflow.update(name="confirm_planning")
    async def confirm_planning(self, operation_id: str) -> bool:
        if not operation_id.strip():
            return False
        if self._confirmed:
            return operation_id == self._confirmation_operation_id
        if self._phase is not PlanningPhase.CONFIRMATION_REQUIRED:
            return False
        self._confirmed = True
        self._confirmation_operation_id = operation_id
        return True

    @workflow.update(name="request_spec_publication")
    async def request_spec_publication(self, operation_id: str) -> bool:
        if not operation_id.strip() or not self._confirmed:
            return False
        if self._publication_requested:
            return operation_id == self._publication_operation_id
        self._publication_requested = True
        self._publication_operation_id = operation_id
        return True

    @workflow.update(name="resolve_spec_publication")
    async def resolve_spec_publication(self, result: SpecPublicationResult) -> bool:
        if (
            self._status is not PlanningStatus.UNKNOWN
            or result.status is SpecPublicationStatus.UNKNOWN
        ):
            return False
        self._publication_resolution = result
        return True

    @workflow.update(name="retry_spec_publication")
    async def retry_spec_publication(self) -> bool:
        if self._status is not PlanningStatus.UNKNOWN:
            return False
        self._publication_retry_requested = True
        return True

    @workflow.signal(name="retry_spec_publication_signal")
    async def retry_spec_publication_signal(self) -> None:
        if self._status is PlanningStatus.UNKNOWN:
            self._publication_retry_requested = True

    @workflow.signal(name="resolve_spec_publication_signal")
    async def resolve_spec_publication_signal(
        self, result: SpecPublicationResult
    ) -> None:
        if (
            self._status is PlanningStatus.UNKNOWN
            and result.status is not SpecPublicationStatus.UNKNOWN
        ):
            self._publication_resolution = result

    @workflow.signal(name="cancel_planning")
    async def cancel_planning(self) -> None:
        if self._status not in (PlanningStatus.COMPLETED, PlanningStatus.CANCELLED):
            self._cancelled = True
            self._phase = PlanningPhase.CANCELLED
            self._status = PlanningStatus.CANCELLED

    def _cancelled_result(self) -> PlanningResult:
        assert self._source is not None
        return PlanningResult(
            workflow_id=workflow.info().workflow_id,
            status=PlanningStatus.CANCELLED,
            phase=PlanningPhase.CANCELLED,
            source=self._source,
            grill=self._grill,
            confirmation_operation_id=self._confirmation_operation_id or "",
            publication_operation_id=self._publication_operation_id or "",
            published_specs=self._published_specs,
            publication_reason=self._publication_reason,
        )
