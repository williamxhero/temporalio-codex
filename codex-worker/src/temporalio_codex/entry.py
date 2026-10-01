from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import sqlite3
from dataclasses import asdict, replace
from datetime import timedelta
from pathlib import Path

from google.protobuf.duration_pb2 import Duration
from google.protobuf.field_mask_pb2 import FieldMask
from temporalio.api.activity.v1 import ActivityOptions
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType, ResetReapplyType, WorkflowExecutionStatus
from temporalio.api.workflowservice.v1 import (
    ResetWorkflowExecutionRequest,
    UpdateActivityOptionsRequest,
)
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Replayer

from temporalio_codex.candidate_activities import (
    CandidateCaptureInput,
    CandidateCaptureResult,
    capture_codex_candidate,
)
from temporalio_codex.codex_models import CodexObservation, CodexOperation, CodexOutcome
from temporalio_codex.delivery_models import DeliveryInput
from temporalio_codex.delivery_workflows import DeliveryWorkflow
from temporalio_codex.entry_models import (
    EntryLaunchReceipt,
    EntryPhase,
    EntryStatus,
    EntryStatusSnapshot,
    RequirementDeliveryRequest,
    RequirementSource,
)
from temporalio_codex.models import RunInput
from temporalio_codex.planning_models import (
    GrillAnswer,
    SourceOrigin,
)
from temporalio_codex.planning_workflows import RequirementPlanningWorkflow
from temporalio_codex.settings import DEFAULT_TARGET_HOST
from temporalio_codex.spec_issue_adapter import (
    SpecDraft,
    SpecIssueRecord,
    SpecPublicationResult,
    SpecPublicationStatus,
)
from temporalio_codex.spec_workflows import SpecExecutionWorkflow
from temporalio_codex.summary_adapter import SummaryPublicationInput
from temporalio_codex.summary_workflows import DeliverySummaryWorkflow
from temporalio_codex.ticket_scheduler import (
    SchedulerInput,
    SchedulerResult,
    SchedulerStatus,
    SpecPlan,
    TicketPlan,
)
from temporalio_codex.ticket_workflows import TicketSchedulerWorkflow
from temporalio_codex.whole_flow_models import (
    PlanningPayload,
    SpecCodexPlan,
    SpecDeliveryPlan,
    WholeFlowInput,
    WholeFlowPhase,
    WholeFlowResult,
    WholeFlowStatus,
)
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow
from temporalio_codex.workflow_identity import (
    project_issue_id,
    project_request_id,
    project_spec_id,
)
from temporalio_codex.workflows import CodexRunWorkflow


def stable_run_id(request: RequirementDeliveryRequest) -> str:
    plan = request.execution_plan
    drafts = {draft.key: draft for draft in plan.planning.specs}
    if len(plan.scheduler.specs) == 1:
        spec_key = plan.scheduler.specs[0].key
        draft = drafts.get(spec_key)
        codex = next((item for item in plan.codex if item.spec_key == spec_key), None)
        if draft is not None and draft.number is not None and codex is not None:
            return project_issue_id(codex.repository, draft.number)
    project_path = plan.codex[0].repository if plan.codex else request.repository
    return project_request_id(project_path, plan.planning.umbrella_issue_number)


def previous_stable_run_id(request: RequirementDeliveryRequest) -> str:
    identity = json.dumps(
        (request.contract_version, request.repository, request.launch_key),
        separators=(",", ":"),
    )
    keys = tuple(spec.key for spec in request.execution_plan.scheduler.specs)
    identifier = project_spec_id(
        request.repository, keys[0] if len(keys) == 1 else "batch", identity
    )
    return (
        identifier if len(keys) == 1 else identifier.rsplit(":spec:", 1)[0] + ":batch"
    )


def legacy_run_id(request: RequirementDeliveryRequest) -> str:
    identity = json.dumps(
        (request.contract_version, request.repository, request.launch_key),
        separators=(",", ":"),
    )
    return f"requirement-delivery-{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:20]}"


def _execution_plan(request: RequirementDeliveryRequest):
    plan = request.execution_plan
    drafts = {draft.key: draft for draft in plan.planning.specs}
    single_spec = len(plan.scheduler.specs) == 1
    draft = drafts.get(plan.scheduler.specs[0].key) if single_spec else None
    has_spec_issue_id = draft is not None and draft.number is not None
    return replace(
        plan,
        entry_contract_version=request.contract_version,
        entry_launch_key=request.launch_key,
        entry_input_identity=request.input_identity,
        execution_layout="spec" if has_spec_issue_id else "project",
    )


async def _existing_run_id(
    client: Client, request: RequirementDeliveryRequest
) -> str | None:
    legacy_id = legacy_run_id(request)
    try:
        await client.get_workflow_handle(legacy_id).describe()
        return legacy_id
    except RPCError as error:
        if error.status != RPCStatusCode.NOT_FOUND:
            raise
    for run_id in (stable_run_id(request), previous_stable_run_id(request)):
        try:
            await client.get_workflow_handle(run_id).describe()
            return run_id
        except RPCError as error:
            if error.status != RPCStatusCode.NOT_FOUND:
                raise
    return None


async def launch_requirement(
    client: Client,
    request: RequirementDeliveryRequest,
) -> EntryLaunchReceipt:
    existing_id = await _existing_run_id(client, request)
    run_id = existing_id or stable_run_id(request)
    if existing_id:
        handle = client.get_workflow_handle(existing_id)
        adopted = True
    else:
        try:
            handle = await client.start_workflow(
                SpecExecutionWorkflow.run
                if _execution_plan(request).execution_layout == "spec"
                else RequirementDeliveryWorkflow.run,
                _execution_plan(request),
                id=run_id,
                task_queue=request.task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                memo={
                    "project": request.repository,
                    "specs": [
                        spec.key for spec in request.execution_plan.scheduler.specs
                    ],
                    "launch_key": request.launch_key,
                },
            )
            adopted = False
        except WorkflowAlreadyStartedError:
            handle = client.get_workflow_handle(run_id)
            adopted = True
    snapshot = None
    for _ in range(50):
        snapshot = await read_status(client, request, run_id=handle.id)
        if snapshot.entry_input_identity is not None:
            break
        await asyncio.sleep(0.01)
    assert snapshot is not None
    observed_identity = snapshot.entry_input_identity
    if observed_identity != request.input_identity:
        raise ValueError(
            "launch key is already bound to a different normalized request"
        )
    return EntryLaunchReceipt(
        run_id=handle.id,
        launch_key=request.launch_key,
        input_identity=request.input_identity,
        contract_version=request.contract_version,
        adopted=adopted,
        status=snapshot,
    )


async def read_status(
    client: Client,
    request: RequirementDeliveryRequest,
    *,
    run_id: str | None = None,
) -> EntryStatusSnapshot:
    resolved_run_id = (
        run_id or await _existing_run_id(client, request) or stable_run_id(request)
    )
    handle = client.get_workflow_handle(resolved_run_id)
    whole = await handle.query(RequirementDeliveryWorkflow.get_status)
    return EntryStatusSnapshot(
        run_id=resolved_run_id,
        launch_key=request.launch_key,
        input_identity=request.input_identity,
        contract_version=request.contract_version,
        phase=EntryPhase(whole.phase.value),
        status=EntryStatus(whole.status.value),
        active_spec=whole.active_spec,
        active_ticket=whole.active_ticket,
        completed_specs=whole.completed_specs,
        next_action=whole.next_action,
        evidence_refs=whole.evidence_refs,
        entry_contract_version=whole.entry_contract_version,
        entry_launch_key=whole.entry_launch_key,
        entry_input_identity=whole.entry_input_identity,
        reason=whole.reason,
        pending_reason=whole.pending_reason,
        retry_count=whole.retry_count,
        deadline=whole.deadline,
        timeout_seconds=whole.timeout_seconds,
        last_error=whole.last_error,
        workflow_run_id=whole.workflow_run_id,
    )


async def pause_requirement(client: Client, run_id: str) -> bool:
    return await client.get_workflow_handle(run_id).execute_update(
        RequirementDeliveryWorkflow.pause, result_type=bool
    )


async def resume_requirement(client: Client, run_id: str) -> bool:
    return await client.get_workflow_handle(run_id).execute_update(
        RequirementDeliveryWorkflow.resume, result_type=bool
    )


async def answer_requirement(
    client: Client,
    run_id: str,
    question_id: str,
    value: str,
) -> bool:
    return await client.get_workflow_handle(run_id).execute_update(
        RequirementDeliveryWorkflow.answer,
        (question_id, value),
        result_type=bool,
    )


async def cancel_requirement(client: Client, run_id: str) -> None:
    await client.get_workflow_handle(run_id).signal(RequirementDeliveryWorkflow.cancel)


async def retry_spec_publication(client: Client, run_id: str) -> bool:
    return await client.get_workflow_handle(run_id).execute_update(
        RequirementDeliveryWorkflow.retry_spec_publication,
        result_type=bool,
    )


async def extend_spec_publication(client: Client, run_id: str, seconds: float) -> dict:
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("publication timeout must be finite and positive")
    child_id = f"{run_id}:planning"
    handle = client.get_workflow_handle(child_id)
    description = (await handle.describe()).raw_description
    info = description.workflow_execution_info
    if (
        info.type.name != "RequirementPlanningWorkflow"
        or info.parent_execution.workflow_id != run_id
        or info.status != 1
    ):
        raise ValueError("no active planning child belongs to this delivery run")
    pending = [
        item
        for item in description.pending_activities
        if item.activity_type.name == "publish-spec-issues"
    ]
    if len(pending) != 1:
        raise ValueError("expected exactly one pending SPEC publication activity")
    activity = pending[0]
    previous = (
        activity.activity_options.start_to_close_timeout.ToTimedelta().total_seconds()
    )
    if seconds < previous:
        raise ValueError("publication recovery must not shorten the active timeout")
    duration = Duration()
    duration.FromTimedelta(timedelta(seconds=seconds))
    await client.workflow_service.update_activity_options(
        UpdateActivityOptionsRequest(
            namespace=client.namespace,
            execution=info.execution,
            identity=client.identity,
            id=activity.activity_id,
            activity_options=ActivityOptions(start_to_close_timeout=duration),
            update_mask=FieldMask(paths=["start_to_close_timeout"]),
        )
    )
    readback = (await handle.describe()).raw_description
    current = next(
        (
            item
            for item in readback.pending_activities
            if item.activity_id == activity.activity_id
        ),
        None,
    )
    observed = (
        current.activity_options.start_to_close_timeout.ToTimedelta().total_seconds()
        if current is not None
        else None
    )
    if observed != seconds:
        raise RuntimeError(
            "publication timeout change requires readback; activity may have completed"
        )
    return {
        "run_id": run_id,
        "child_id": child_id,
        "child_run_id": info.execution.run_id,
        "activity_id": activity.activity_id,
        "attempt": current.attempt,
        "start_to_close_timeout_seconds": observed,
    }


async def resolve_spec_publication(
    client: Client,
    run_id: str,
    result: SpecPublicationResult,
) -> bool:
    return await client.get_workflow_handle(run_id).execute_update(
        RequirementDeliveryWorkflow.resolve_spec_publication,
        result,
        result_type=bool,
    )


async def diagnose_requirement(client: Client, run_id: str) -> EntryStatusSnapshot:
    handle = client.get_workflow_handle(run_id)
    whole = await handle.query(RequirementDeliveryWorkflow.get_status)
    return EntryStatusSnapshot(
        run_id=run_id,
        launch_key=whole.entry_launch_key or "",
        input_identity=whole.entry_input_identity or "",
        contract_version=whole.entry_contract_version or "",
        phase=EntryPhase(whole.phase.value),
        status=EntryStatus(whole.status.value),
        active_spec=whole.active_spec,
        active_ticket=whole.active_ticket,
        completed_specs=whole.completed_specs,
        next_action=whole.next_action,
        evidence_refs=whole.evidence_refs,
        entry_contract_version=whole.entry_contract_version,
        entry_launch_key=whole.entry_launch_key,
        entry_input_identity=whole.entry_input_identity,
        reason=whole.reason,
        pending_reason=whole.pending_reason,
        retry_count=whole.retry_count,
        deadline=whole.deadline,
        timeout_seconds=whole.timeout_seconds,
        last_error=whole.last_error,
        workflow_run_id=whole.workflow_run_id,
    )


def _validate_completed_ticket_failure(result, history, payload_converter) -> None:
    child = next(
        (
            event.child_workflow_execution_completed_event_attributes
            for event in reversed(history.events)
            if event.event_type
            == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
        ),
        None,
    )
    scheduler = (
        payload_converter.from_payloads(child.result.payloads, [SchedulerResult])[0]
        if child is not None
        else None
    )
    runs = (result.scheduler or {}).get("runs", ())
    if (
        result.phase is not WholeFlowPhase.FAILED
        or result.status is not WholeFlowStatus.FAILED
        or scheduler is None
        or scheduler.status is not SchedulerStatus.BLOCKED
        or not runs
        or runs[-1].get("workflow_id") != scheduler.workflow_id
        or runs[-1].get("status") != scheduler.status.value
        or result.reason != "ticket scheduling did not complete"
        or runs[-1].get("reason") != scheduler.reason
    ):
        raise ValueError(
            "completed execution is not the verified ticket scheduling failure"
        )


def _ticket_child_reset_point(
    history, payload_converter, *, allow_frozen_candidate=False
) -> int:
    terminal_child = next(
        (
            event
            for event in reversed(history.events)
            if event.event_type
            in {
                EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
                EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
            }
        ),
        None,
    )
    if terminal_child is None:
        raise ValueError("delivery history has no terminal ticket child")
    attributes_name = terminal_child.WhichOneof("attributes")
    attributes = getattr(terminal_child, attributes_name)
    if (
        terminal_child.event_type
        == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
    ):
        result = payload_converter.from_payloads(
            attributes.result.payloads, [SchedulerResult]
        )[0]
        if result.status is not SchedulerStatus.BLOCKED or (
            result.completed_tickets and not allow_frozen_candidate
        ):
            raise ValueError(
                "completed ticket child is not an unfinished blocked scheduler"
            )
        if result.codex_results and not allow_frozen_candidate:
            raise ValueError(
                "ticket child has external Codex results; authoritative readback is required before reset"
            )
    initiated_event_id = attributes.initiated_event_id
    start_event = next(
        (
            event.start_child_workflow_execution_initiated_event_attributes
            for event in history.events
            if event.event_id == initiated_event_id
            and event.event_type
            == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
        ),
        None,
    )
    if (
        start_event is None
        or start_event.workflow_type.name != "TicketSchedulerWorkflow"
    ):
        raise ValueError("terminated child is not a TicketSchedulerWorkflow")
    reset_point = initiated_event_id - 1
    point_event = next(
        (event for event in history.events if event.event_id == reset_point), None
    )
    if (
        point_event is None
        or point_event.event_type != EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    ):
        raise ValueError("ticket child does not follow a completed workflow task")
    return reset_point


async def _read_interrupted_operation(operation: CodexOperation, conversation_db: str):
    from openai_codex import AsyncCodex

    fingerprint = hashlib.sha256(
        json.dumps(asdict(operation), sort_keys=True).encode()
    ).hexdigest()
    with sqlite3.connect(
        Path(conversation_db).resolve().as_uri() + "?mode=ro", uri=True
    ) as connection:
        row = connection.execute(
            "SELECT thread_id,turn_id,fingerprint FROM codex_operation_ledger "
            "WHERE namespace=? AND workflow_id=? AND workflow_run_id=? AND operation_id=?",
            (
                operation.namespace,
                operation.run_id,
                operation.workflow_run_id,
                operation.operation_id,
            ),
        ).fetchone()
    if row is None or row[2] != fingerprint or not all(row[:2]):
        raise ValueError(
            "interrupted operation ledger does not match the durable activity input"
        )
    async with AsyncCodex() as codex:
        thread = await codex.thread_resume(row[0], include_turns=True)
        readback = await thread.read(include_turns=True)
    _validate_interrupted_readback(
        readback.thread, row[0], row[1], operation.repository
    )
    return row[0], row[1]


def _validate_interrupted_readback(thread, thread_id, turn_id, repository):
    turns = [turn for turn in thread.turns if turn.id == turn_id]
    cwd = getattr(thread.cwd, "root", thread.cwd)
    if (
        thread.id != thread_id
        or Path(str(cwd)).resolve() != Path(repository).resolve()
        or len(turns) != 1
        or getattr(turns[0].status, "value", turns[0].status) != "interrupted"
        or any(
            getattr(turn.status, "value", turn.status) == "inProgress"
            for turn in thread.turns
        )
    ):
        raise ValueError(
            "external readback does not prove the exact implementation turn was interrupted"
        )


def _validate_known_model_failure(thread, thread_id, turn_id, repository, model):
    turns = [turn for turn in thread.turns if turn.id == turn_id]
    cwd = getattr(thread.cwd, "root", thread.cwd)
    if (
        thread.id != thread_id
        or Path(str(cwd)).resolve() != Path(repository).resolve()
        or len(turns) != 1
        or getattr(turns[0].status, "value", turns[0].status) != "failed"
        or any(
            getattr(turn.status, "value", turn.status) == "inProgress"
            for turn in thread.turns
        )
    ):
        raise ValueError("external readback does not prove the exact model failure")
    error = getattr(turns[0], "error", None)
    message = str(getattr(error, "message", "") or "")
    if "model_not_found" not in message or model not in message:
        raise ValueError("external readback is not the verified model failure")


async def _freeze_implementation_for_recovery(
    client: Client,
    history,
    *,
    interrupted_conversation_db: str | None = None,
):
    scheduler_event = next(
        event.child_workflow_execution_completed_event_attributes
        for event in reversed(history.events)
        if event.event_type == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
        and event.child_workflow_execution_completed_event_attributes.workflow_type.name
        == "TicketSchedulerWorkflow"
    )
    scheduler_history = await client.get_workflow_handle(
        scheduler_event.workflow_execution.workflow_id,
        run_id=scheduler_event.workflow_execution.run_id,
    ).fetch_history()
    children = [
        event.child_workflow_execution_completed_event_attributes
        for event in scheduler_history.events
        if event.event_type == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
    ]
    child = None
    child_history = None
    for candidate_child in reversed(children):
        if candidate_child.workflow_type.name != "CodexRunWorkflow":
            continue
        candidate_history = await client.get_workflow_handle(
            candidate_child.workflow_execution.workflow_id,
            run_id=candidate_child.workflow_execution.run_id,
        ).fetch_history()
        converter = client.data_converter.payload_converter
        scheduled_codex = {
            event.event_id: event.activity_task_scheduled_event_attributes
            for event in candidate_history.events
            if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
            and event.activity_task_scheduled_event_attributes.activity_type.name
            == "codex-stage"
        }
        has_unresolved_codex = any(
            event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
            and event.activity_task_completed_event_attributes.scheduled_event_id
            in scheduled_codex
            and (
                (
                    observation := converter.from_payloads(
                        event.activity_task_completed_event_attributes.result.payloads,
                        [CodexObservation],
                    )[0]
                ).outcome
                is CodexOutcome.UNKNOWN
                or observation.readback_required
            )
            for event in candidate_history.events
        )
        if (
            any(
                event.event_type
                in {
                    EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
                    EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
                }
                for event in candidate_history.events
            )
            or has_unresolved_codex
        ):
            child = candidate_child
            child_history = candidate_history
            break
        # A completed Codex child can still be the recoverable failure when its
        # review durably rejected the captured candidate. Preserve that
        # candidate for a recovery-only review instead of treating the parent
        # scheduler failure as an external result that cannot be reset.
        if child is None:
            converter = client.data_converter.payload_converter
            scheduled = {
                event.event_id: event.activity_task_scheduled_event_attributes
                for event in candidate_history.events
                if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
            }
            capture = None
            rejected_sha = None
            for event in candidate_history.events:
                if event.event_type != EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
                    continue
                done = event.activity_task_completed_event_attributes
                activity = scheduled.get(done.scheduled_event_id)
                if activity is None:
                    continue
                if activity.activity_type.name == "capture-codex-candidate":
                    try:
                        capture = converter.from_payloads(
                            done.result.payloads, [CandidateCaptureResult]
                        )[0]
                    except (TypeError, ValueError):
                        pass
                elif activity.activity_type.name == "codex-stage":
                    try:
                        observation = converter.from_payloads(
                            done.result.payloads, [CodexObservation]
                        )[0]
                        if observation.role.value == "review":
                            review = json.loads(observation.summary)
                            if (
                                observation.outcome is CodexOutcome.COMPLETED
                                and review.get("verdict") == "rejected"
                                and isinstance(review.get("findings"), list)
                                and review["findings"]
                            ):
                                rejected_sha = review.get("candidate_sha")
                    except (TypeError, ValueError, AttributeError):
                        pass
            started_input = converter.from_payloads(
                candidate_history.events[
                    0
                ].workflow_execution_started_event_attributes.input.payloads,
                [RunInput],
            )[0]
            frozen = (
                capture.candidate if capture is not None else started_input.candidate
            )
            if (
                frozen is not None
                and rejected_sha
                and frozen.candidate_sha == rejected_sha
            ):
                child = candidate_child
                child_history = candidate_history
                break
    if child is None or child_history is None:
        raise ValueError("candidate recovery requires a failed Codex child")
    converter = client.data_converter.payload_converter
    input = converter.from_payloads(
        child_history.events[
            0
        ].workflow_execution_started_event_attributes.input.payloads,
        [RunInput],
    )[0]
    scheduled = {
        event.event_id: event.activity_task_scheduled_event_attributes
        for event in child_history.events
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    }
    failures = [
        event.activity_task_failed_event_attributes
        for event in child_history.events
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED
    ]
    timeouts = [
        event.activity_task_timed_out_event_attributes
        for event in child_history.events
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
    ]
    implementation = None
    implementation_capture = None
    review_rejected = False
    review_candidate_sha = None
    pre_frozen_candidate = input.candidate
    pre_frozen_recovery = False
    pre_frozen_review_failure = False
    known_model_failure_recovery = False
    unresolved_external = False
    for event in child_history.events:
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            done = event.activity_task_completed_event_attributes
            activity_type = scheduled[done.scheduled_event_id].activity_type.name
            if activity_type == "codex-stage":
                observed = converter.from_payloads(
                    done.result.payloads, [CodexObservation]
                )[0]
                if (
                    observed.outcome is not CodexOutcome.COMPLETED
                    or observed.readback_required
                ):
                    unresolved_external = True
                    continue
                if observed.role.value == "implementation":
                    implementation = observed
                elif observed.role.value == "review":
                    try:
                        review = json.loads(observed.summary)
                    except (TypeError, ValueError):
                        review = None
                    review_rejected = (
                        isinstance(review, dict)
                        and review.get("verdict") == "rejected"
                        and bool(review.get("candidate_sha"))
                        and isinstance(review.get("findings"), list)
                        and bool(review.get("findings"))
                    )
                    if review_rejected:
                        review_candidate_sha = review["candidate_sha"]
            elif activity_type == "capture-codex-candidate":
                try:
                    implementation_capture = converter.from_payloads(
                        done.result.payloads, [CandidateCaptureResult]
                    )[0]
                except (TypeError, ValueError):
                    pass
    capture_failure = (
        len(failures) == 1
        and scheduled[failures[0].scheduled_event_id].activity_type.name
        == "capture-codex-candidate"
    )
    dirty_capture_failure = (
        capture_failure
        and failures[0].failure.message == "candidate workspace is dirty"
    )
    scope_capture_failure = (
        capture_failure
        and failures[0].failure.message
        == "candidate changes exceed the authorized scope"
        and implementation is not None
        and implementation_capture is None
    )
    rejected_review_capture_failure = (
        capture_failure
        and failures[0].failure.message
        in {
            "Activity task failed",
            "review approval evidence is missing or does not match candidate SHA",
        }
        and review_rejected
        and implementation_capture is not None
        and review_candidate_sha == implementation_capture.candidate.candidate_sha
    )
    completed_rejected_review = (
        not capture_failure
        and review_rejected
        and implementation_capture is not None
        and review_candidate_sha == implementation_capture.candidate.candidate_sha
    )
    pre_frozen_review_failure = (
        pre_frozen_candidate is not None
        and (capture_failure or (not failures and not timeouts))
        and review_rejected
        and implementation_capture is None
        and review_candidate_sha == pre_frozen_candidate.candidate_sha
    )
    interrupted_operation = None
    interrupted_ids = None
    if pre_frozen_candidate and not failures and len(timeouts) == 1:
        timed_out = scheduled[timeouts[0].scheduled_event_id]
        operation = converter.from_payloads(timed_out.input.payloads, [CodexOperation])[
            0
        ]
        pre_frozen_recovery = operation.role.value == "planning"
    if (
        pre_frozen_candidate
        and interrupted_conversation_db
        and not failures
        and not timeouts
    ):
        scheduled_codex = {
            event.event_id: event.activity_task_scheduled_event_attributes
            for event in child_history.events
            if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
            and event.activity_task_scheduled_event_attributes.activity_type.name
            == "codex-stage"
        }
        for event in child_history.events:
            if event.event_type != EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
                continue
            scheduled_event = scheduled_codex.get(
                event.activity_task_completed_event_attributes.scheduled_event_id
            )
            if scheduled_event is None:
                continue
            observed = converter.from_payloads(
                event.activity_task_completed_event_attributes.result.payloads,
                [CodexObservation],
            )[0]
            if (
                observed.outcome is not CodexOutcome.UNKNOWN
                and not observed.readback_required
            ):
                continue
            operation = converter.from_payloads(
                scheduled_event.input.payloads, [CodexOperation]
            )[0]
            if operation.model != "gpt-5-codex":
                continue
            fingerprint = hashlib.sha256(
                json.dumps(asdict(operation), sort_keys=True).encode()
            ).hexdigest()
            with sqlite3.connect(
                Path(interrupted_conversation_db).resolve().as_uri() + "?mode=ro",
                uri=True,
            ) as connection:
                row = connection.execute(
                    "SELECT thread_id,turn_id,fingerprint FROM codex_operation_ledger "
                    "WHERE namespace=? AND workflow_id=? AND workflow_run_id=? AND operation_id=?",
                    (
                        operation.namespace,
                        operation.run_id,
                        operation.workflow_run_id,
                        operation.operation_id,
                    ),
                ).fetchone()
            if row is None or row[2] != fingerprint or not all(row[:2]):
                continue
            from openai_codex import AsyncCodex

            async with AsyncCodex() as codex:
                thread = await codex.thread_resume(row[0], include_turns=True)
                readback = await thread.read(include_turns=True)
            _validate_known_model_failure(
                readback.thread, row[0], row[1], operation.repository, operation.model
            )
            known_model_failure_recovery = True
            break
    if interrupted_conversation_db and len(timeouts) == 1 and not failures:
        timed_out = scheduled[timeouts[0].scheduled_event_id]
        if timed_out.activity_type.name == "codex-stage":
            operation = converter.from_payloads(
                timed_out.input.payloads, [CodexOperation]
            )[0]
            if (
                operation.role.value == "implementation"
                and operation.run_id == child.workflow_execution.workflow_id
                and operation.workflow_run_id == child.workflow_execution.run_id
                and operation.namespace == client.namespace
                and input.candidate is not None
                and operation.repository == input.candidate.workspace
            ):
                interrupted_ids = await _read_interrupted_operation(
                    operation, interrupted_conversation_db
                )
                interrupted_operation = operation
    if unresolved_external and not known_model_failure_recovery:
        raise ValueError(
            "candidate recovery refuses unresolved external Codex outcomes"
        )
    if not (
        dirty_capture_failure
        or scope_capture_failure
        or rejected_review_capture_failure
        or completed_rejected_review
        or interrupted_operation
        or pre_frozen_recovery
        or pre_frozen_review_failure
        or known_model_failure_recovery
    ):
        raise ValueError(
            "candidate recovery requires a verified frozen candidate or capture failure"
        )
    if input.candidate is None:
        raise ValueError(
            "candidate recovery has no completed implementation and candidate identity"
        )
    if pre_frozen_recovery and implementation is None and interrupted_operation is None:
        result = await capture_codex_candidate(
            CandidateCaptureInput(
                candidate=pre_frozen_candidate,
                operation_id=f"{child.workflow_execution.workflow_id}:pre-frozen-candidate",
            )
        )
        return result.candidate, (
            result.candidate.candidate_sha != result.candidate.base_sha
        )
    if pre_frozen_review_failure:
        result = await capture_codex_candidate(
            CandidateCaptureInput(
                candidate=pre_frozen_candidate,
                operation_id=f"{child.workflow_execution.workflow_id}:rejected-review-recovery",
            )
        )
        return result.candidate, False
    if completed_rejected_review:
        return implementation_capture.candidate, False
    if known_model_failure_recovery:
        return pre_frozen_candidate, False
    stage = next(
        stage for stage in input.stages if stage.role.value == "implementation"
    )
    result = await capture_codex_candidate(
        CandidateCaptureInput(
            candidate=input.candidate,
            operation_id=(interrupted_operation or implementation).operation_id,
            thread_id=interrupted_ids[0]
            if interrupted_ids
            else implementation.thread_id or "",
            turn_id=interrupted_ids[1]
            if interrupted_ids
            else implementation.turn_id or "",
            allowed_scope=stage.allowed_scope,
        )
    )
    return replace(input.candidate, candidate_sha=result.candidate.candidate_sha), (
        dirty_capture_failure
        or scope_capture_failure
        or interrupted_operation is not None
    )


async def recover_failed_requirement(
    client: Client,
    *,
    workflow_id: str,
    source_run_id: str,
    expected_input_identity: str,
    freeze_candidate: bool = False,
    interrupted_conversation_db: str | None = None,
) -> dict:
    if len(expected_input_identity) != 64:
        raise ValueError("expected input identity must be a SHA-256 hex digest")
    try:
        int(expected_input_identity, 16)
    except ValueError as error:
        raise ValueError(
            "expected input identity must be a SHA-256 hex digest"
        ) from error
    handle = client.get_workflow_handle(workflow_id)
    description = (await handle.describe()).raw_description
    info = description.workflow_execution_info
    run_id = info.execution.run_id
    reset_point = None
    recovered_candidate = None
    recovery_review_only = False
    is_completed_failure = (
        info.status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED
    )
    is_failed_execution = (
        info.status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED
    )

    if is_failed_execution or is_completed_failure:
        if run_id != source_run_id:
            raise ValueError(
                "failed execution differs from the explicitly authorized source run"
            )
        history = await handle.fetch_history()
        started = history.events[0].workflow_execution_started_event_attributes
        payload = client.data_converter.payload_converter.from_payloads(
            started.input.payloads, [WholeFlowInput]
        )[0]
        if payload.entry_input_identity != expected_input_identity:
            raise ValueError(
                "failed execution input identity differs from recovery evidence"
            )
        reset_point = _ticket_child_reset_point(
            history,
            client.data_converter.payload_converter,
            allow_frozen_candidate=freeze_candidate,
        )
        replay_history = history
        if is_completed_failure:
            completed = next(
                (
                    event.workflow_execution_completed_event_attributes
                    for event in reversed(history.events)
                    if event.event_type
                    == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
                ),
                None,
            )
            if completed is None:
                raise ValueError("completed execution has no durable result")
            result = client.data_converter.payload_converter.from_payloads(
                completed.result.payloads, [WholeFlowResult]
            )[0]
            _validate_completed_ticket_failure(
                result,
                history,
                client.data_converter.payload_converter,
            )
            replay_history = type(history)(
                workflow_id=history.workflow_id,
                events=[
                    event for event in history.events if event.event_id <= reset_point
                ],
            )
        workflows = [
            RequirementDeliveryWorkflow,
            RequirementPlanningWorkflow,
            TicketSchedulerWorkflow,
            CodexRunWorkflow,
            DeliveryWorkflow,
            DeliverySummaryWorkflow,
        ]
        await Replayer(workflows=workflows).replay_workflow(replay_history)
        if freeze_candidate:
            (
                recovered_candidate,
                recovery_review_only,
            ) = await _freeze_implementation_for_recovery(
                client,
                history,
                interrupted_conversation_db=interrupted_conversation_db,
            )
        reset = await client.workflow_service.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=client.namespace,
                workflow_execution=WorkflowExecution(
                    workflow_id=workflow_id,
                    run_id=source_run_id,
                ),
                workflow_task_finish_event_id=reset_point,
                reason=(
                    "finish-needs recovery after ticket scheduler failure; "
                    "preserve published SPEC and ticket identities"
                ),
                request_id=f"finish-needs-recovery:{source_run_id}",
                identity=client.identity,
                reset_reapply_type=ResetReapplyType.RESET_REAPPLY_TYPE_NONE,
            )
        )
        run_id = reset.run_id
        handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    elif info.status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING:
        history = await handle.fetch_history()
        started = history.events[0].workflow_execution_started_event_attributes
        if (
            started.original_execution_run_id != source_run_id
            or started.first_execution_run_id != source_run_id
        ):
            raise ValueError(
                "active execution is not a Temporal reset of the explicitly authorized source run"
            )
        payload = client.data_converter.payload_converter.from_payloads(
            started.input.payloads, [WholeFlowInput]
        )[0]
        if payload.entry_input_identity != expected_input_identity:
            raise ValueError(
                "active execution input identity differs from recovery evidence"
            )
    else:
        raise ValueError(
            f"delivery execution is {info.status}; expected the source failure or its active reset run"
        )

    snapshot = None
    child_id = None
    for _ in range(100):
        snapshot = await diagnose_requirement(client, workflow_id)
        if snapshot.entry_input_identity != expected_input_identity:
            raise ValueError("recovered execution input identity changed")
        if snapshot.phase is EntryPhase.TICKETS and snapshot.active_spec:
            child_id = f"{workflow_id}:tickets:{snapshot.active_spec}"
            child_description = (
                await client.get_workflow_handle(child_id).describe()
            ).raw_description
            child_info = child_description.workflow_execution_info
            if (
                child_info.status
                == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
                and child_info.type.name == "TicketSchedulerWorkflow"
                and child_info.parent_execution.workflow_id == workflow_id
                and child_info.parent_execution.run_id == run_id
            ):
                break
        await asyncio.sleep(0.2)
    else:
        return {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "status": snapshot.status.value if snapshot else "not_verified",
            "phase": snapshot.phase.value if snapshot else None,
            "active_spec": snapshot.active_spec if snapshot else None,
            "recovery_signal_sent": False,
        }

    operation_id = f"{workflow_id}:ticket-recovery:{source_run_id}"
    if recovered_candidate is None:
        await handle.signal("recover_ticket_execution", operation_id)
    else:
        await handle.signal(
            "recover_ticket_execution",
            args=[operation_id, recovered_candidate, recovery_review_only],
        )
    return {
        "workflow_id": workflow_id,
        "source_run_id": source_run_id,
        "run_id": run_id,
        "input_identity": expected_input_identity,
        "reset_point_event_id": reset_point if run_id != source_run_id else None,
        "phase": snapshot.phase.value,
        "status": snapshot.status.value,
        "active_spec": snapshot.active_spec,
        "active_ticket": snapshot.active_ticket,
        "next_action": snapshot.next_action,
        "recovery_signal_sent": True,
        "recovery_operation_id": operation_id,
        "frozen_candidate": asdict(recovered_candidate)
        if recovered_candidate
        else None,
        "recovery_review_only": recovery_review_only,
    }


async def diagnose_details(client: Client, run_id: str) -> dict:
    snapshot = await diagnose_requirement(client, run_id)
    result = asdict(snapshot)
    parent = (await client.get_workflow_handle(run_id).describe()).raw_description
    result["execution_status"] = parent.workflow_execution_info.status
    result["pending_children"] = []
    for child in parent.pending_children:
        child_handle = client.get_workflow_handle(
            child.workflow_id, run_id=child.run_id
        )
        description = (await child_handle.describe()).raw_description
        planning = (
            await child_handle.query("get_planning_status")
            if description.workflow_execution_info.type.name
            == "RequirementPlanningWorkflow"
            else None
        )
        scheduler = (
            await child_handle.query("get_scheduler_status")
            if description.workflow_execution_info.type.name
            == "TicketSchedulerWorkflow"
            else None
        )
        result["pending_children"].append(
            {
                "workflow_id": child.workflow_id,
                "run_id": child.run_id,
                "planning": planning,
                "scheduler": scheduler,
                "pending_activities": [
                    {
                        "activity_id": item.activity_id,
                        "type": item.activity_type.name,
                        "attempt": item.attempt,
                        "state": item.state,
                        "maximum_attempts": item.maximum_attempts,
                        "last_started_time": str(item.last_started_time.ToDatetime()),
                        "next_attempt_schedule_time": str(
                            item.next_attempt_schedule_time.ToDatetime()
                        ),
                        "start_to_close_timeout_seconds": item.activity_options.start_to_close_timeout.ToTimedelta().total_seconds(),
                    }
                    for item in description.pending_activities
                ],
            }
        )
    return result


async def _run_command(args: argparse.Namespace) -> object:
    client = await Client.connect(args.target_host)
    if args.command == "launch":
        request = load_request(args.request_file)
        return await launch_requirement(client, request)
    if args.command == "recover-failed":
        return await recover_failed_requirement(
            client,
            workflow_id=args.run_id,
            source_run_id=args.source_run_id,
            expected_input_identity=args.input_identity,
            freeze_candidate=args.freeze_candidate,
            interrupted_conversation_db=args.interrupted_conversation_db,
        )
    if args.command == "pause":
        return await pause_requirement(client, args.run_id)
    if args.command == "resume":
        return await resume_requirement(client, args.run_id)
    if args.command == "cancel":
        await cancel_requirement(client, args.run_id)
        return True
    if args.command == "retry-publication":
        return await retry_spec_publication(client, args.run_id)
    if args.command == "extend-publication":
        return await extend_spec_publication(
            client, args.run_id, args.publication_timeout_seconds
        )
    if args.command == "resolve-publication":
        return await resolve_spec_publication(
            client, args.run_id, load_publication_result(args.publication_file)
        )
    if args.command == "answer":
        return await answer_requirement(
            client, args.run_id, args.question_id, args.value
        )
    if args.details:
        return await diagnose_details(client, args.run_id)
    return await diagnose_requirement(client, args.run_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Control a requirement delivery run")
    parser.add_argument(
        "command",
        choices=(
            "launch",
            "recover-failed",
            "status",
            "diagnose",
            "pause",
            "resume",
            "answer",
            "cancel",
            "retry-publication",
            "extend-publication",
            "resolve-publication",
        ),
    )
    parser.add_argument("--run-id")
    parser.add_argument("--details", action="store_true")
    parser.add_argument("--request-file")
    parser.add_argument("--source-run-id")
    parser.add_argument("--input-identity")
    parser.add_argument("--freeze-candidate", action="store_true")
    parser.add_argument("--interrupted-conversation-db")
    parser.add_argument("--question-id")
    parser.add_argument("--value")
    parser.add_argument("--publication-file")
    parser.add_argument("--publication-timeout-seconds", type=float)
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST)
    args = parser.parse_args()
    if args.command == "launch" and not args.request_file:
        parser.error("launch requires --request-file")
    if args.command == "recover-failed" and not args.run_id:
        parser.error("recover-failed requires --run-id")
    if args.command == "recover-failed" and not args.source_run_id:
        parser.error("recover-failed requires --source-run-id")
    if args.command == "recover-failed" and not args.input_identity:
        parser.error("recover-failed requires --input-identity")
    if args.interrupted_conversation_db and (
        args.command != "recover-failed" or not args.freeze_candidate
    ):
        parser.error(
            "--interrupted-conversation-db requires recover-failed --freeze-candidate"
        )
    if args.command not in {"launch", "recover-failed"} and not args.run_id:
        parser.error(f"{args.command} requires --run-id")
    if args.command == "answer" and (not args.question_id or not args.value):
        parser.error("answer requires --question-id and --value")
    if args.command == "resolve-publication" and not args.publication_file:
        parser.error("resolve-publication requires --publication-file")
    if (
        args.command == "extend-publication"
        and args.publication_timeout_seconds is None
    ):
        parser.error("extend-publication requires --publication-timeout-seconds")
    result = asyncio.run(_run_command(args))
    print(
        json.dumps(
            asdict(result) if hasattr(result, "__dataclass_fields__") else result,
            default=str,
            sort_keys=True,
        )
    )


def load_publication_result(path: str) -> SpecPublicationResult:
    with open(path, encoding="utf-8") as stream:
        payload = json.load(stream)
    status = SpecPublicationStatus(payload["status"])
    issues = tuple(
        SpecIssueRecord(
            number=item["number"],
            issue_id=item["issue_id"],
            title=item["title"],
            operation_id=item["operation_id"],
            parent_issue_number=item["parent_issue_number"],
            state=item.get("state", "open"),
        )
        for item in payload.get("issues", ())
    )
    return SpecPublicationResult(
        status=status,
        issues=issues,
        reason=payload.get("reason", ""),
    )


def load_request(path: str) -> RequirementDeliveryRequest:
    with open(path, encoding="utf-8") as stream:
        payload = json.load(stream)
    source_payload = payload["source"]
    source = RequirementSource(
        origin=SourceOrigin(source_payload["origin"]),
        text=source_payload.get("text"),
        reference=source_payload.get("reference"),
    )
    plan_payload = payload["execution_plan"]
    planning_payload = plan_payload["planning"]
    planning = PlanningPayload(
        origin=SourceOrigin(planning_payload["origin"]),
        source_text=planning_payload.get("source_text"),
        source_reference=planning_payload.get("source_reference"),
        sensitive=planning_payload.get("sensitive", False),
        umbrella_issue_number=planning_payload.get("umbrella_issue_number", 1),
        specs=tuple(
            SpecDraft(
                key=item["key"],
                title=item["title"],
                scope=item["scope"],
                acceptance_criteria=tuple(item["acceptance_criteria"]),
                testing_decisions=tuple(item["testing_decisions"]),
                dependencies=tuple(item.get("dependencies", ())),
                provenance=tuple(item.get("provenance", ())),
                number=item.get("number"),
            )
            for item in planning_payload.get("specs", ())
        ),
        grill_answers=tuple(
            GrillAnswer(
                question_number=item["question_number"],
                answer=item["answer"],
                accepted_as_assumption=item.get("accepted_as_assumption", False),
            )
            for item in planning_payload.get("grill_answers", ())
        ),
        confirmation_operation_id=planning_payload.get("confirmation_operation_id"),
        publication_operation_id=planning_payload.get("publication_operation_id"),
        publication_timeout_seconds=planning_payload.get(
            "publication_timeout_seconds", 300.0
        ),
        publication_max_attempts=planning_payload.get("publication_max_attempts", 3),
        publication_retry_backoff_seconds=planning_payload.get(
            "publication_retry_backoff_seconds", 1.0
        ),
    )
    scheduler_payload = plan_payload["scheduler"]
    scheduler = SchedulerInput(
        specs=tuple(
            SpecPlan(item["key"], tuple(item.get("dependencies", ())))
            for item in scheduler_payload["specs"]
        ),
        tickets=tuple(
            TicketPlan(
                item["key"],
                item["spec_key"],
                tuple(item.get("blockers", ())),
                item.get("title", ""),
                tuple(item.get("acceptance_criteria", ())),
            )
            for item in scheduler_payload["tickets"]
        ),
        completion_operations=tuple(
            (item[0], item[1])
            for item in scheduler_payload.get("completion_operations", ())
        ),
    )
    codex = tuple(SpecCodexPlan(**item) for item in plan_payload["codex"])
    deliveries = tuple(
        SpecDeliveryPlan(
            item["spec_key"],
            DeliveryInput(**item["delivery"]),
        )
        for item in plan_payload["deliveries"]
    )
    summary = SummaryPublicationInput(**plan_payload["summary"])
    execution_plan = WholeFlowInput(
        repository=payload["repository"],
        planning=planning,
        scheduler=scheduler,
        codex=codex,
        deliveries=deliveries,
        summary=summary,
    )
    return RequirementDeliveryRequest(
        source=source,
        repository=payload["repository"],
        artifact_roots=tuple(payload["artifact_roots"]),
        execution_plan=execution_plan,
        launch_key=payload["launch_key"],
        target_ref=payload.get("target_ref", "refs/heads/main"),
        task_queue=payload.get("task_queue", "codex-worker"),
        contract_version=payload.get("contract_version", "requirement-delivery/v1"),
        metadata=tuple(tuple(item) for item in payload.get("metadata", ())),
    )
