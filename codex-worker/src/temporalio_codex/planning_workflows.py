from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.planning_activities import prepare_grill
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
        self._cancelled = False

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
        questions = await workflow.execute_activity(
            prepare_grill,
            GrillPreparationInput(input.origin, input.source_identity),
            start_to_close_timeout=timedelta(seconds=30),
        )
        self._grill = GrillRecord(questions=questions)
        await workflow.wait_condition(
            lambda: self._required_questions_answered() or self._cancelled
        )
        if self._cancelled:
            return self._cancelled_result()

        self._phase = PlanningPhase.CONFIRMATION_REQUIRED
        self._status = PlanningStatus.WAITING_FOR_INPUT
        await workflow.wait_condition(lambda: self._confirmed or self._cancelled)
        if self._cancelled:
            return self._cancelled_result()

        self._phase = PlanningPhase.READY
        self._status = PlanningStatus.READY
        await workflow.wait_condition(
            lambda: self._publication_requested or self._cancelled
        )
        if self._cancelled:
            return self._cancelled_result()
        self._phase = PlanningPhase.COMPLETED
        self._status = PlanningStatus.COMPLETED
        assert self._source is not None
        assert self._confirmation_operation_id is not None
        assert self._publication_operation_id is not None
        return PlanningResult(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            phase=self._phase,
            source=self._source,
            grill=self._grill,
            confirmation_operation_id=self._confirmation_operation_id,
            publication_operation_id=self._publication_operation_id,
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
        )
