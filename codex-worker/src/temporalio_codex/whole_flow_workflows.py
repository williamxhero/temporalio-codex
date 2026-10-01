import asyncio
from dataclasses import asdict, replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.activities import delivery_git_stage
    from temporalio_codex.delivery_models import (
        CandidateEvidence,
        DeliveryInput,
        DeliveryOperation,
        DeliveryOutcome,
        DeliveryPhase,
        ReviewEvidence,
    )
    from temporalio_codex.execution_status import ExecutionProgress
    from temporalio_codex.models import RunInput
    from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
    from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
    from temporalio_codex.workflows import CodexRunWorkflow
    from temporalio_codex.delivery_workflows import DeliveryWorkflow
    from temporalio_codex.summary_workflows import DeliverySummaryWorkflow
    from temporalio_codex.workflow_identity import project_issue_id, project_spec_id
    from temporalio_codex.planning_activities import publish_ticket_issues
    from temporalio_codex.planning_models import GrillAnswer
    from temporalio_codex.spec_issue_adapter import (
        SpecPublicationResult,
        SpecPublicationStatus,
    )
    from temporalio_codex.ticket_issue_adapter import (
        TicketPublicationInput,
        TicketPublicationStatus,
    )
    from temporalio_codex.ticket_scheduler import SchedulerInput, SpecPlan, TicketPlan
    from temporalio_codex.whole_flow_models import (
        WholeFlowInput,
        WholeFlowPhase,
        WholeFlowResult,
        WholeFlowSnapshot,
        WholeFlowStatus,
        topological_spec_keys,
        validate_delivery_evidence,
        validate_whole_flow_input,
    )


def _codex_runs_for_tickets(
    input: WholeFlowInput,
    spec_key: str,
    tickets: tuple[TicketPlan, ...],
    delivery_plan: DeliveryInput,
    candidate: CandidateEvidence,
    parent_workflow_id: str,
    parent_workflow_run_id: str,
    *,
    review_only: bool = False,
) -> tuple[tuple[str, RunInput], ...]:
    codex_plan = next(plan for plan in input.codex if plan.spec_key == spec_key)
    completion_operations = dict(input.scheduler.completion_operations)
    runs = []
    for ticket in tickets:
        if ticket.key in completion_operations:
            continue
        run_input = codex_plan.to_run_input(
            parent_workflow_id=parent_workflow_id,
            parent_workflow_run_id=parent_workflow_run_id,
        )
        stages = tuple(
            replace(stage, repository=delivery_plan.workspace)
            for stage in run_input.stages
        )
        if review_only:
            stages = tuple(
                stage for stage in stages
                if stage.role is not None and stage.role.value == "review"
            )
            if not stages:
                continue
        requirement = (
            f"{codex_plan.requirement}\n"
            f"Ready ticket: {ticket.key}\n"
            f"Title: {ticket.title}\n"
            "Acceptance criteria:\n"
            + "\n".join(f"- {criterion}" for criterion in ticket.acceptance_criteria)
        )
        runs.append(
            (
                ticket.key,
                replace(
                    run_input,
                    stages=stages,
                    candidate=candidate,
                    requirement=requirement,
                ),
            )
        )
    return tuple(runs)


@workflow.defn
class RequirementDeliveryWorkflow:
    def __init__(self) -> None:
        self._phase = WholeFlowPhase.INTAKE
        self._status = WholeFlowStatus.ACTIVE
        self._completed_specs: list[str] = []
        self._active_spec: str | None = None
        self._active_ticket: str | None = None
        self._next_action = "validate and start planning"
        self._evidence_refs: list[str] = []
        self._reason = ""
        self._entry_contract_version: str | None = None
        self._entry_launch_key: str | None = None
        self._entry_input_identity: str | None = None
        self._paused = False
        self._cancelled = False
        self._active_child_id: str | None = None
        self._answered_question_ids: list[str] = []
        self._active_child_run_id: str | None = None
        self._progress: ExecutionProgress | None = None
        self._retry_count = 0
        self._input: WholeFlowInput | None = None
        self._recovery_mode = False
        self._recovery_review_only = False
        self._ticket_recovery_operations: set[str] = set()
        self._recovery_candidate: CandidateEvidence | None = None
        self._component = None
        self._component_task = None

    async def _component_progress(self, progress: ExecutionProgress) -> None:
        if isinstance(self._component, TicketSchedulerWorkflow):
            progress = replace(progress, active_ticket=self._component._active_ticket)
        if not self._paused and not self._cancelled:
            self._progress = progress
            self._retry_count = progress.retry_count
            self._phase = WholeFlowPhase(progress.phase)
            self._status = WholeFlowStatus(progress.status)
            self._active_ticket = progress.active_ticket
            self._next_action = progress.next_action
        info = workflow.info()
        if info.parent is not None:
            await workflow.get_external_workflow_handle(
                info.parent.workflow_id, run_id=info.parent.run_id
            ).signal("execution_progress", progress)

    async def _component_checkpoint(self) -> None:
        if not await self._wait_if_paused():
            raise asyncio.CancelledError()

    def _conversation_scope(self) -> tuple[str, str]:
        info = workflow.info()
        if self._input.execution_layout == "spec" and info.parent is not None:
            return info.parent.workflow_id, info.parent.run_id
        return info.workflow_id, info.run_id

    async def _signal_component(self, name: str, *args) -> None:
        if self._component is not None:
            await getattr(self._component, name)(*args)
        else:
            await workflow.get_external_workflow_handle(
                self._active_child_id, run_id=self._active_child_run_id
            ).signal(name, *args)

    @workflow.signal(name="execution_progress")
    def execution_progress(self, progress: ExecutionProgress) -> None:
        if (
            progress.workflow_id != self._active_child_id
            or progress.workflow_run_id != self._active_child_run_id
            or self._paused or self._cancelled
        ):
            return
        try:
            phase = WholeFlowPhase(progress.phase)
            status = WholeFlowStatus(progress.status)
        except ValueError:
            return
        self._progress = progress
        self._retry_count = progress.retry_count
        self._phase = phase
        self._status = status
        self._active_ticket = progress.active_ticket
        self._next_action = progress.next_action

    async def _execute_child(self, name: str, input, **kwargs):
        self._progress = None
        self._retry_count = 0
        self._status = WholeFlowStatus.DURABLE_WAITING
        if self._input.execution_layout != "legacy" and name != "SpecExecutionWorkflow":
            component_types = {
                "RequirementPlanningWorkflow": RequirementPlanningWorkflow,
                "TicketSchedulerWorkflow": TicketSchedulerWorkflow,
                "CodexRunWorkflow": CodexRunWorkflow,
                "DeliveryWorkflow": DeliveryWorkflow,
                "DeliverySummaryWorkflow": DeliverySummaryWorkflow,
            }
            self._component = component_types[name]()
            workflow.set_signal_handler("component_progress", self._component_progress)
            workflow.set_signal_handler("component_checkpoint", self._component_checkpoint)
            self._component_task = asyncio.create_task(self._component.run(input))
            try:
                return asdict(await self._component_task)
            except asyncio.CancelledError:
                if not self._cancelled:
                    raise
                return {"status": "cancelled"}
            finally:
                self._component_task = None
                self._component = None
                self._progress = None
                self._active_ticket = None
                workflow.set_signal_handler("component_progress", None)
                workflow.set_signal_handler("component_checkpoint", None)
                if not self._paused and not self._cancelled:
                    self._status = WholeFlowStatus.ACTIVE
        handle = await workflow.start_child_workflow(name, input, **kwargs)
        self._active_child_run_id = handle.first_execution_run_id
        try:
            return await handle
        finally:
            self._active_child_run_id = None
            self._progress = None
            self._active_ticket = None
            if not self._paused and not self._cancelled:
                self._status = WholeFlowStatus.ACTIVE

    async def _prepare_candidate(
        self,
        repository: str,
        workspace: str,
        base_sha: str,
        spec_key: str,
        candidate_sha: str | None = None,
    ) -> tuple[CandidateEvidence | None, str | None]:
        try:
            prepared = await workflow.execute_activity(
                delivery_git_stage,
                DeliveryOperation(
                    operation_id=f"{workflow.info().workflow_id}:prepare:{spec_key}",
                    run_id=workflow.info().workflow_id,
                    phase=DeliveryPhase.CANDIDATE,
                    repository=repository,
                    workspace=workspace,
                    base_sha=base_sha,
                    candidate_sha=candidate_sha,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        except ActivityError as error:
            return None, f"candidate workspace preparation failed: {error}"
        if prepared.outcome is not DeliveryOutcome.COMPLETED or not prepared.candidate_sha:
            return None, "candidate workspace preparation not verified"
        return (
            CandidateEvidence(repository, workspace, prepared.candidate_sha, prepared.candidate_sha),
            None,
        )

    async def _configure_ticket_child(self, spec_key: str) -> str | None:
        input = self._input
        if input is None or self._active_child_id is None or self._active_child_run_id is None:
            return "ticket execution recovery has no active parent or child identity"
        delivery_plan = next(
            (plan.delivery for plan in input.deliveries if plan.spec_key == spec_key),
            None,
        )
        codex_plan = next(
            (plan for plan in input.codex if plan.spec_key == spec_key), None
        )
        if delivery_plan is None or codex_plan is None:
            return f"ticket execution recovery has no plan for {spec_key}"
        ticket_by_key = {ticket.key: ticket for ticket in input.scheduler.tickets}
        spec_tickets = tuple(
            TicketPlan(
                ticket.key,
                ticket.spec_key,
                tuple(
                    blocker
                    for blocker in ticket.blockers
                    if ticket_by_key[blocker].spec_key == spec_key
                ),
                ticket.title,
                ticket.acceptance_criteria,
            )
            for ticket in input.scheduler.tickets
            if ticket.spec_key == spec_key
        )
        candidate, error = await self._prepare_candidate(
            delivery_plan.repository,
            delivery_plan.workspace,
            delivery_plan.base_sha,
            spec_key,
            self._recovery_candidate.candidate_sha
            if self._recovery_candidate is not None
            and self._recovery_candidate.workspace == delivery_plan.workspace
            else None,
        )
        if error:
            return error
        if candidate is None:
            return "candidate workspace preparation returned no candidate"
        runs = _codex_runs_for_tickets(
            input,
            spec_key,
            spec_tickets,
            delivery_plan,
            candidate,
            workflow.info().workflow_id,
            workflow.info().run_id,
            review_only=self._recovery_review_only,
        )
        child = workflow.get_external_workflow_handle(
            self._active_child_id, run_id=self._active_child_run_id
        )
        await child.signal("configure_codex_runs", runs)
        self._next_action = f"execute ready tickets for {spec_key}"
        self._status = WholeFlowStatus.DURABLE_WAITING
        return None

    @workflow.signal(name="recover_ticket_execution")
    async def recover_ticket_execution(
        self,
        operation_id: str,
        candidate: dict[str, str] | None = None,
        review_only: bool = False,
    ) -> None:
        if (
            not operation_id.strip()
            or self._phase is not WholeFlowPhase.TICKETS
            or not self._active_spec
            or not self._active_child_id
            or not self._active_child_run_id
            or self._cancelled
            or operation_id in self._ticket_recovery_operations
        ):
            return
        self._ticket_recovery_operations.add(operation_id)
        self._recovery_mode = True
        if isinstance(candidate, dict):
            try:
                candidate = CandidateEvidence(**candidate)
            except (TypeError, ValueError):
                self._reason = "recovery candidate payload is invalid"
                self._status = WholeFlowStatus.BLOCKED
                return
        if candidate is not None:
            delivery = next(
                plan.delivery for plan in self._input.deliveries
                if plan.spec_key == self._active_spec
            )
            if candidate.repository != delivery.repository or candidate.workspace != delivery.workspace:
                self._reason = "recovery candidate belongs to a different delivery workspace"
                self._status = WholeFlowStatus.BLOCKED
                return
        self._recovery_candidate = candidate
        self._recovery_review_only = review_only and candidate is not None
        error = await self._configure_ticket_child(self._active_spec)
        if error:
            self._reason = error
            self._status = WholeFlowStatus.BLOCKED
            self._next_action = "resolve ticket execution recovery failure"
            await workflow.get_external_workflow_handle(
                self._active_child_id, run_id=self._active_child_run_id
            ).signal("configure_codex_runs", ())

    @workflow.query(name="get_whole_flow_status")
    def get_status(self) -> WholeFlowSnapshot:
        return WholeFlowSnapshot(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            completed_specs=tuple(self._completed_specs),
            active_spec=self._active_spec,
            active_ticket=self._active_ticket,
            next_action=self._next_action,
            evidence_refs=tuple(self._evidence_refs),
            entry_contract_version=self._entry_contract_version,
            entry_launch_key=self._entry_launch_key,
            entry_input_identity=self._entry_input_identity,
            reason=self._reason,
            pending_reason=(self._progress.pending_reason if self._progress else
                            (f"waiting for child execution {self._active_child_id}"
                             if self._status is WholeFlowStatus.DURABLE_WAITING
                             else self._reason)),
            retry_count=self._progress.retry_count if self._progress else self._retry_count,
            deadline=self._progress.deadline if self._progress else None,
            timeout_seconds=self._progress.timeout_seconds if self._progress else None,
            last_error=self._progress.last_error if self._progress else (self._reason or None),
            workflow_run_id=workflow.info().run_id,
        )

    @workflow.update(name="pause")
    async def pause(self) -> bool:
        if self._status in (
            WholeFlowStatus.COMPLETED,
            WholeFlowStatus.FAILED,
            WholeFlowStatus.CANCELLED,
        ):
            return False
        if self._paused:
            return True
        self._paused = True
        self._status = WholeFlowStatus.BLOCKED
        self._reason = "paused by operator"
        if self._active_child_id and self._phase is WholeFlowPhase.PLANNING:
            await self._signal_component("pause_planning")
        return True

    @workflow.update(name="resume")
    async def resume(self) -> bool:
        if not self._paused:
            return False
        self._paused = False
        self._status = WholeFlowStatus.ACTIVE
        self._reason = ""
        if self._active_child_id and self._phase is WholeFlowPhase.PLANNING:
            await self._signal_component("resume_planning")
        return True

    @workflow.update(name="answer")
    async def answer(self, answer: tuple[str, str]) -> bool:
        question_id, value = answer
        if (
            not question_id.strip()
            or not value.strip()
            or self._status
            in (
                WholeFlowStatus.COMPLETED,
                WholeFlowStatus.FAILED,
                WholeFlowStatus.CANCELLED,
            )
        ):
            return False
        if (
            not question_id.startswith("grill:")
            or not self._active_child_id
            or question_id in self._answered_question_ids
        ):
            return False
        try:
            question_number = int(question_id.removeprefix("grill:"))
        except ValueError:
            return False
        if self._phase is not WholeFlowPhase.PLANNING:
            return False
        await self._signal_component(
            "answer_grill_signal",
            GrillAnswer(question_number=question_number, answer=value),
        )
        self._answered_question_ids.append(question_id)
        self._next_action = f"answer acknowledged: {question_id}"
        self._reason = ""
        return True

    @workflow.signal(name="planning_blocked")
    async def planning_blocked(self, reason: str) -> None:
        # Legacy unscoped notifications cannot identify a child execution.
        return

    @workflow.signal(name="planning_resumed")
    async def planning_resumed(self) -> None:
        return

    @workflow.update(name="retry_spec_publication")
    async def retry_spec_publication(self) -> bool:
        if (
            self._phase not in (WholeFlowPhase.PLANNING, WholeFlowPhase.BLOCKED)
            or self._active_child_id != f"{workflow.info().workflow_id}:planning"
            or self._paused
        ):
            return False
        await self._signal_component(
            "retry_spec_publication_signal"
        )
        self._phase = WholeFlowPhase.PLANNING
        self._status = WholeFlowStatus.ACTIVE
        self._reason = ""
        self._next_action = "complete Grill and publish SPEC Issues"
        return True

    @workflow.update(name="resolve_spec_publication")
    async def resolve_spec_publication(self, result: SpecPublicationResult) -> bool:
        if (
            self._phase not in (WholeFlowPhase.PLANNING, WholeFlowPhase.BLOCKED)
            or self._active_child_id != f"{workflow.info().workflow_id}:planning"
            or self._paused
            or result.status is SpecPublicationStatus.UNKNOWN
        ):
            return False
        await self._signal_component(
            "resolve_spec_publication_signal", result
        )
        self._phase = WholeFlowPhase.PLANNING
        self._status = WholeFlowStatus.ACTIVE
        self._reason = ""
        return True

    @workflow.signal(name="cancel")
    async def cancel(self) -> None:
        if self._status not in (
            WholeFlowStatus.COMPLETED,
            WholeFlowStatus.FAILED,
            WholeFlowStatus.CANCELLED,
        ):
            self._cancelled = True
            self._status = WholeFlowStatus.CANCELLED
            self._phase = WholeFlowPhase.CANCELLED
            self._reason = "cancelled by operator"
            if self._component_task is not None:
                self._component_task.cancel()
            elif self._active_child_id:
                await workflow.get_external_workflow_handle(
                    self._active_child_id
                ).cancel(reason="parent run cancelled")

    async def _wait_if_paused(self) -> bool:
        await workflow.wait_condition(lambda: not self._paused or self._cancelled)
        return not self._cancelled

    @workflow.run
    async def run(self, input: WholeFlowInput) -> WholeFlowResult:
        self._input = input
        self._entry_contract_version = input.entry_contract_version
        self._entry_launch_key = input.entry_launch_key
        self._entry_input_identity = input.entry_input_identity
        if not await self._wait_if_paused():
            return self._cancelled_result()
        validation_errors = validate_whole_flow_input(input)
        if validation_errors:
            return self._blocked("; ".join(validation_errors))

        self._phase = WholeFlowPhase.PLANNING
        self._next_action = "complete Grill and publish SPEC Issues"
        self._active_child_id = f"{workflow.info().workflow_id}:planning"
        try:
            planning = input.published_planning or await self._execute_child(
                "RequirementPlanningWorkflow",
                input.planning.to_input(
                    repository=input.repository,
                    parent_workflow_id=workflow.info().workflow_id,
                    parent_workflow_run_id=workflow.info().run_id,
                ),
                id=self._active_child_id,
                result_type=dict,
            )
        except Exception as error:  # noqa: BLE001 - convert child failures to an explicit flow result
            if self._cancelled:
                return self._cancelled_result()
            return self._failed(f"planning child failed: {error}")
        finally:
            self._active_child_id = None
        if self._cancelled:
            return self._cancelled_result()
        if planning.get("status") != "completed":
            reason = planning.get("publication_reason") or "planning did not complete"
            self._phase = WholeFlowPhase.BLOCKED
            self._status = (
                WholeFlowStatus.NOT_VERIFIED
                if planning.get("status") == "unknown"
                else WholeFlowStatus.BLOCKED
            )
            self._reason = reason
            self._next_action = ""
            return WholeFlowResult(
                workflow_id=workflow.info().workflow_id,
                phase=self._phase,
                status=self._status,
                planning=planning,
                reason=reason,
            )
        self._evidence_refs.extend(
            f"spec-issue:{record['number']}"
            for record in planning.get("published_specs", ())
        )

        expected_specs = topological_spec_keys(input.scheduler)
        ticket_by_key = {ticket.key: ticket for ticket in input.scheduler.tickets}
        completion_by_ticket = dict(input.scheduler.completion_operations)
        codex_by_spec = {plan.spec_key: plan for plan in input.codex}
        delivery_by_spec = {plan.spec_key: plan for plan in input.deliveries}
        spec_issue_numbers = {
            record["operation_id"].rsplit(":", 1)[-1]: record["number"]
            for record in planning.get("published_specs", ())
        }
        ticket_issue_numbers: dict[str, int] = {}
        scheduler_runs: list[dict] = []
        codex_results: list[dict] = []
        delivery_results: list[dict] = []

        if input.execution_layout == "project":
            for spec_key in expected_specs:
                if not await self._wait_if_paused():
                    return self._cancelled_result(planning, scheduler_runs, codex_results, delivery_results)
                self._active_spec = spec_key
                spec_tickets = tuple(replace(
                    ticket, blockers=tuple(blocker for blocker in ticket.blockers
                                           if ticket_by_key[blocker].spec_key == spec_key)
                ) for ticket in input.scheduler.tickets if ticket.spec_key == spec_key)
                child_input = replace(
                    input, execution_layout="spec", published_planning=planning, defer_summary=True,
                    planning=replace(input.planning, specs=tuple(
                        replace(draft, dependencies=()) for draft in input.planning.specs if draft.key == spec_key
                    )),
                    scheduler=SchedulerInput(
                        specs=(SpecPlan(spec_key),), tickets=spec_tickets,
                        completion_operations=tuple((key, value) for key, value in input.scheduler.completion_operations
                                                    if key in {ticket.key for ticket in spec_tickets}),
                    ),
                    codex=(codex_by_spec[spec_key],), deliveries=(delivery_by_spec[spec_key],),
                )
                draft = next(item for item in input.planning.specs if item.key == spec_key)
                codex_plan = codex_by_spec[spec_key]
                if workflow.patched("spec-workflow-id-uses-published-issue"):
                    self._active_child_id = project_issue_id(
                        codex_plan.repository, draft.number or spec_issue_numbers[spec_key]
                    )
                else:
                    self._active_child_id = project_spec_id(
                        input.repository, spec_key, workflow.info().workflow_id
                    )
                try:
                    result = await self._execute_child(
                        "SpecExecutionWorkflow", child_input, id=self._active_child_id, result_type=dict,
                    )
                except Exception as error:
                    return self._failed(f"SPEC {spec_key} execution failed: {error}", planning)
                finally:
                    self._active_child_id = None
                codex_results.extend(result.get("codex_results", ()))
                delivery_results.extend(result.get("delivery_results", ()))
                if result.get("status") != "completed":
                    return self._failed(
                        f"SPEC {spec_key}: {result.get('reason') or result.get('status')}",
                        planning, {"runs": scheduler_runs}, tuple(codex_results), tuple(delivery_results),
                    )
                scheduler_runs.extend(result.get("scheduler", {}).get("runs", ()))
                self._completed_specs.append(spec_key)
                self._evidence_refs.append(f"spec-workflow:{result['workflow_id']}")

        for spec_key in (() if input.execution_layout == "project" else expected_specs):
            if not await self._wait_if_paused():
                return self._cancelled_result(
                    planning, scheduler_runs, codex_results, delivery_results
                )
            self._active_spec = spec_key
            delivery_plan = delivery_by_spec[spec_key].delivery
            prepare_before_tickets = workflow.patched(
                "candidate-preparation-before-tickets"
            )
            self._next_action = f"publish tickets for {spec_key}"
            self._phase = WholeFlowPhase.TICKETS
            spec_tickets = tuple(
                TicketPlan(
                    ticket.key,
                    ticket.spec_key,
                    tuple(
                        blocker
                        for blocker in ticket.blockers
                        if ticket_by_key[blocker].spec_key == spec_key
                    ),
                    ticket.title,
                    ticket.acceptance_criteria,
                )
                for ticket in input.scheduler.tickets
                if ticket.spec_key == spec_key
            )
            spec_issue_number = spec_issue_numbers.get(spec_key)
            if spec_issue_number is None:
                return self._failed(
                    f"SPEC {spec_key} has no published Issue readback",
                    planning,
                    {"status": "blocked", "runs": scheduler_runs},
                )
            candidate = None
            if prepare_before_tickets:
                candidate, preparation_error = await self._prepare_candidate(
                    delivery_plan.repository,
                    delivery_plan.workspace,
                    delivery_plan.base_sha,
                    spec_key,
                )
                if preparation_error:
                    return self._failed(
                        preparation_error, planning
                    )
                if candidate is None:
                    return self._failed(
                        "candidate workspace preparation returned no candidate", planning
                    )
            ticket_input = TicketPublicationInput(
                repository=input.repository,
                operation_id=f"{workflow.info().workflow_id}:tickets:{spec_key}",
                spec_issue_number=spec_issue_number,
                blocker_issue_numbers=tuple(
                    (blocker, ticket_issue_numbers[blocker])
                    for ticket in spec_tickets
                    for blocker in ticket.blockers
                    if blocker in ticket_issue_numbers
                ),
                tickets=spec_tickets,
            )
            if not prepare_before_tickets:
                scheduler_input = SchedulerInput(
                    specs=(SpecPlan(spec_key),),
                    tickets=spec_tickets,
                )
            else:
                scheduler_input = SchedulerInput(
                    specs=(SpecPlan(spec_key),),
                    tickets=spec_tickets,
                    automatic=True,
                    inline_codex=input.execution_layout == "spec",
                    completion_operations=tuple(
                        (ticket.key, completion_by_ticket[ticket.key])
                        for ticket in spec_tickets
                        if ticket.key in completion_by_ticket
                    ),
                    codex_runs=_codex_runs_for_tickets(
                        input,
                        spec_key,
                        spec_tickets,
                        delivery_plan,
                        candidate,
                        *self._conversation_scope(),
                    ),
                )
            ticket_publication = None
            max_attempts = input.planning.publication_max_attempts
            for attempt in range(1, max_attempts + 1):
                self._status = WholeFlowStatus.ACTIVE
                self._retry_count = attempt - 1
                self._progress = ExecutionProgress(
                    workflow.info().workflow_id, workflow.info().run_id,
                    "tickets", next_action=f"publish tickets for {spec_key}",
                    retry_count=attempt - 1, timeout_seconds=30,
                    deadline=(workflow.now() + timedelta(seconds=30)).isoformat(),
                )
                try:
                    ticket_publication = await workflow.execute_activity(
                        publish_ticket_issues,
                        ticket_input,
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=RetryPolicy(maximum_attempts=1),
                    )
                except ActivityError as error:
                    return self._failed(f"ticket publication activity failed: {error}", planning)
                if ticket_publication.status is not TicketPublicationStatus.UNKNOWN:
                    break
                if attempt < max_attempts:
                    backoff = input.planning.publication_retry_backoff_seconds * (2 ** (attempt - 1))
                    self._status = WholeFlowStatus.RETRYING
                    self._retry_count = attempt
                    self._progress = ExecutionProgress(
                        workflow.info().workflow_id, workflow.info().run_id,
                        "tickets", "retrying", retry_count=attempt,
                        pending_reason="ticket publication requires readback",
                        next_action="reconcile ticket publication",
                        deadline=(workflow.now() + timedelta(seconds=backoff)).isoformat(),
                        last_error=ticket_publication.reason,
                    )
                    await workflow.sleep(
                        backoff
                    )
            assert ticket_publication is not None
            if ticket_publication.status is not TicketPublicationStatus.VERIFIED:
                return self._failed(
                    "ticket Issue publication did not complete",
                    planning,
                    {
                        "status": "blocked",
                        "runs": (*scheduler_runs, asdict(ticket_publication)),
                    },
                )
            ticket_issue_numbers.update(
                {record.key: record.number for record in ticket_publication.issues}
            )
            self._evidence_refs.extend(
                f"ticket-issue:{record.number}" for record in ticket_publication.issues
            )
            self._active_child_id = f"{workflow.info().workflow_id}:tickets:{spec_key}"
            try:
                if not prepare_before_tickets and self._recovery_mode:
                    self._status = WholeFlowStatus.DURABLE_WAITING
                    self._progress = None
                    handle = await workflow.start_child_workflow(
                        "TicketSchedulerWorkflow",
                        scheduler_input,
                        id=self._active_child_id,
                        result_type=dict,
                    )
                    self._active_child_run_id = handle.first_execution_run_id
                    try:
                        error = await self._configure_ticket_child(spec_key)
                        if error:
                            return self._failed(error, planning)
                        scheduler_run = await handle
                    finally:
                        self._active_child_run_id = None
                        self._progress = None
                        self._active_ticket = None
                        if not self._paused and not self._cancelled:
                            self._status = WholeFlowStatus.ACTIVE
                else:
                    scheduler_run = await self._execute_child(
                        "TicketSchedulerWorkflow",
                        scheduler_input,
                        id=self._active_child_id,
                        result_type=dict,
                    )
            except Exception as error:
                if not prepare_before_tickets:
                    raise
                if self._cancelled:
                    return self._cancelled_result(
                        planning, scheduler_runs, codex_results, delivery_results
                    )
                return self._failed(
                    f"ticket scheduling child failed: {error}", planning
                )
            finally:
                self._active_child_id = None
            scheduler_runs.append(
                {**scheduler_run, "ticket_publication": asdict(ticket_publication)}
            )
            if scheduler_run.get("status") != "completed" or tuple(
                scheduler_run.get("completed_specs", ())
            ) != (spec_key,):
                scheduler = {"status": "failed", "runs": scheduler_runs}
                return self._failed(
                    scheduler_run.get("reason") or "ticket scheduling did not complete", planning, scheduler,
                    tuple(codex_results) + tuple(scheduler_run.get("codex_results", ())),
                )

            automatic_results = tuple(scheduler_run.get("codex_results", ()))
            if automatic_results:
                self._phase = WholeFlowPhase.CODEX
                self._next_action = f"Codex execution completed for {spec_key}"
                codex_results.extend(automatic_results)
                if not prepare_before_tickets:
                    final_candidate = automatic_results[-1].get("candidate")
                    if not final_candidate:
                        return self._failed(
                            "ticket Codex execution has no frozen candidate",
                            planning,
                            {"runs": scheduler_runs},
                            tuple(codex_results),
                        )
                    candidate = CandidateEvidence(**final_candidate)
            elif not prepare_before_tickets:
                candidate, preparation_error = await self._prepare_candidate(
                    delivery_plan.repository,
                    delivery_plan.workspace,
                    delivery_plan.base_sha,
                    spec_key,
                )
                if preparation_error:
                    return self._failed(preparation_error, planning)
                if candidate is None:
                    return self._failed(
                        "candidate workspace preparation returned no candidate", planning
                    )
            else:
                self._phase = WholeFlowPhase.CODEX
                self._next_action = (
                    f"run planning, implementation and review for {spec_key}"
                )
                plan = codex_by_spec[spec_key]
                self._active_child_id = (
                    f"{workflow.info().workflow_id}:codex:{plan.spec_key}"
                )
                try:
                    result = await self._execute_child(
                        "CodexRunWorkflow",
                        replace(
                            plan.to_run_input(
                                parent_workflow_id=workflow.info().workflow_id,
                                parent_workflow_run_id=workflow.info().run_id,
                            ),
                            stages=tuple(
                                replace(stage, repository=delivery_plan.workspace)
                                for stage in plan.to_run_input().stages
                            ),
                            candidate=candidate,
                        ),
                        id=self._active_child_id,
                        result_type=dict,
                    )
                except Exception as error:  # noqa: BLE001 - convert child failures to an explicit flow result
                    if self._cancelled:
                        return self._cancelled_result(
                            planning, scheduler_runs, codex_results, delivery_results
                        )
                    return self._failed(f"Codex child failed: {error}", planning)
                finally:
                    self._active_child_id = None
                codex_results.append(result)
                if result.get("status") != "completed":
                    scheduler = {"status": "completed", "runs": scheduler_runs}
                    return self._failed(
                        "Codex implementation or review did not complete",
                        planning,
                        scheduler,
                        tuple(codex_results),
                    )

            self._phase = WholeFlowPhase.DELIVERY
            self._next_action = f"deliver and read back {spec_key}"
            plan = delivery_by_spec[spec_key]
            final_result = codex_results[-1] if codex_results else {}
            final_candidate = final_result.get("candidate")
            final_review = final_result.get("review_evidence")
            if not final_candidate or not final_review:
                return self._failed(
                    "Codex execution has no frozen candidate and approved review evidence",
                    planning,
                    {"runs": scheduler_runs},
                    tuple(codex_results),
                )
            self._active_child_id = (
                f"{workflow.info().workflow_id}:delivery:{plan.spec_key}"
            )
            try:
                result = await self._execute_child(
                    "DeliveryWorkflow",
                    replace(
                        plan.delivery,
                        automatic=True,
                        candidate=CandidateEvidence(**final_candidate),
                        review_evidence=ReviewEvidence(**final_review),
                        issue_numbers=tuple(dict.fromkeys((
                            spec_issue_number,
                            *(ticket_issue_numbers[ticket.key] for ticket in spec_tickets),
                            *plan.delivery.issue_numbers,
                        ))),
                    ),
                    id=self._active_child_id,
                    result_type=dict,
                )
            except Exception as error:  # noqa: BLE001 - convert child failures to an explicit flow result
                if self._cancelled:
                    return self._cancelled_result(
                        planning, scheduler_runs, codex_results, delivery_results
                    )
                return self._failed(
                    f"delivery child failed: {error}",
                    planning,
                    {"runs": scheduler_runs},
                    tuple(codex_results),
                    tuple(delivery_results),
                )
            finally:
                self._active_child_id = None
            delivery_results.append(result)
            if (
                result.get("status") == "completed"
                and (result.get("candidate") != final_candidate
                     or result.get("review_evidence") != final_review)
            ):
                return self._failed(
                    "delivery proof differs from the frozen Codex candidate and review",
                    planning, {"runs": scheduler_runs},
                    tuple(codex_results), tuple(delivery_results),
                )
            evidence_errors = validate_delivery_evidence(result)
            if evidence_errors:
                scheduler = {"status": "completed", "runs": scheduler_runs}
                return self._failed(
                    "; ".join(evidence_errors),
                    planning,
                    scheduler,
                    tuple(codex_results),
                    tuple(delivery_results),
                )
            self._evidence_refs.extend(
                receipt.get("external_id") or receipt.get("operation_id", "")
                for receipt in result.get("receipts", ())
            )
            self._completed_specs.append(spec_key)
            self._active_spec = None
            self._active_ticket = None

        scheduler = {
            "status": "completed",
            "completed_specs": expected_specs,
            "completed_tickets": tuple(
                ticket.key for ticket in input.scheduler.tickets
            ),
            "runs": tuple(scheduler_runs),
        }

        self._phase = WholeFlowPhase.SUMMARY
        self._next_action = "publish and verify final summary"
        self._active_child_id = f"{workflow.info().workflow_id}:summary"
        try:
            summary = {"status": "verified"} if input.defer_summary else await self._execute_child(
                "DeliverySummaryWorkflow",
                replace(input.summary, automatic=True),
                id=self._active_child_id,
                result_type=dict,
            )
        except Exception as error:  # noqa: BLE001 - convert child failures to an explicit flow result
            if self._cancelled:
                return self._cancelled_result(
                    planning, scheduler, codex_results, delivery_results
                )
            return self._failed(
                f"summary child failed: {error}",
                planning,
                scheduler,
                tuple(codex_results),
                tuple(delivery_results),
            )
        finally:
            self._active_child_id = None
        if summary.get("status") != "verified":
            return self._failed(
                "final summary was not verified",
                planning,
                scheduler,
                tuple(codex_results),
                tuple(delivery_results),
                summary,
            )
        self._phase = WholeFlowPhase.COMPLETED
        if summary.get("comment"):
            self._evidence_refs.append(
                f"summary-comment:{summary['comment'].get('comment_id', '')}"
            )
        self._status = WholeFlowStatus.COMPLETED
        self._next_action = ""
        self._active_spec = None
        self._active_ticket = None
        self._progress = None
        self._reason = ""
        return WholeFlowResult(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            planning=planning,
            scheduler=scheduler,
            codex_results=tuple(codex_results),
            delivery_results=tuple(delivery_results),
            summary=summary,
        )

    def _cancelled_result(
        self,
        planning=None,
        scheduler=None,
        codex_results=(),
        delivery_results=(),
    ) -> WholeFlowResult:
        self._phase = WholeFlowPhase.CANCELLED
        self._status = WholeFlowStatus.CANCELLED
        return WholeFlowResult(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            planning=planning,
            scheduler={"runs": scheduler} if isinstance(scheduler, list) else scheduler,
            codex_results=tuple(codex_results),
            delivery_results=tuple(delivery_results),
            reason=self._reason or "cancelled by operator",
        )

    def _blocked(self, reason: str) -> WholeFlowResult:
        self._phase = WholeFlowPhase.BLOCKED
        self._status = WholeFlowStatus.BLOCKED
        self._reason = reason
        self._next_action = ""
        self._progress = None
        return WholeFlowResult(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            reason=reason,
        )

    def _failed(
        self,
        reason: str,
        planning=None,
        scheduler=None,
        codex_results=(),
        delivery_results=(),
        summary=None,
    ) -> WholeFlowResult:
        if self._cancelled:
            return self._cancelled_result(planning, scheduler, codex_results, delivery_results)
        self._phase = WholeFlowPhase.FAILED
        self._status = WholeFlowStatus.FAILED
        self._reason = reason
        self._next_action = ""
        self._progress = None
        return WholeFlowResult(
            workflow_id=workflow.info().workflow_id,
            phase=self._phase,
            status=self._status,
            planning=planning,
            scheduler=scheduler,
            codex_results=tuple(codex_results),
            delivery_results=tuple(delivery_results),
            summary=summary,
            reason=reason,
        )
