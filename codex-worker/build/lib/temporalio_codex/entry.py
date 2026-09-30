from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
from dataclasses import asdict, replace
from datetime import timedelta

from google.protobuf.duration_pb2 import Duration
from google.protobuf.field_mask_pb2 import FieldMask
from temporalio.api.activity.v1 import ActivityOptions
from temporalio.api.workflowservice.v1 import UpdateActivityOptionsRequest
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from temporalio_codex.delivery_models import DeliveryInput
from temporalio_codex.entry_models import (
    EntryLaunchReceipt,
    EntryPhase,
    EntryStatus,
    EntryStatusSnapshot,
    RequirementDeliveryRequest,
    RequirementSource,
)
from temporalio_codex.planning_models import (
    GrillAnswer,
    SourceOrigin,
)
from temporalio_codex.settings import DEFAULT_TARGET_HOST
from temporalio_codex.spec_issue_adapter import (
    SpecDraft,
    SpecIssueRecord,
    SpecPublicationResult,
    SpecPublicationStatus,
)
from temporalio_codex.summary_adapter import SummaryPublicationInput
from temporalio_codex.ticket_scheduler import SchedulerInput, SpecPlan, TicketPlan
from temporalio_codex.whole_flow_models import (
    PlanningPayload,
    SpecCodexPlan,
    SpecDeliveryPlan,
    WholeFlowInput,
)
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow


def stable_run_id(request: RequirementDeliveryRequest) -> str:
    identity = json.dumps(
        (request.contract_version, request.repository, request.launch_key),
        separators=(",", ":"),
    )
    return f"requirement-delivery-{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:20]}"


def _execution_plan(request: RequirementDeliveryRequest):
    return replace(
        request.execution_plan,
        entry_contract_version=request.contract_version,
        entry_launch_key=request.launch_key,
        entry_input_identity=request.input_identity,
    )


async def launch_requirement(
    client: Client,
    request: RequirementDeliveryRequest,
) -> EntryLaunchReceipt:
    run_id = stable_run_id(request)
    try:
        handle = await client.start_workflow(
            RequirementDeliveryWorkflow.run,
            _execution_plan(request),
            id=run_id,
            task_queue=request.task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
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
    resolved_run_id = run_id or stable_run_id(request)
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
    pending = [item for item in description.pending_activities
               if item.activity_type.name == "publish-spec-issues"]
    if len(pending) != 1:
        raise ValueError("expected exactly one pending SPEC publication activity")
    activity = pending[0]
    previous = activity.activity_options.start_to_close_timeout.ToTimedelta().total_seconds()
    if seconds < previous:
        raise ValueError("publication recovery must not shorten the active timeout")
    duration = Duration()
    duration.FromTimedelta(timedelta(seconds=seconds))
    await client.workflow_service.update_activity_options(UpdateActivityOptionsRequest(
        namespace=client.namespace,
        execution=info.execution,
        identity=client.identity,
        id=activity.activity_id,
        activity_options=ActivityOptions(start_to_close_timeout=duration),
        update_mask=FieldMask(paths=["start_to_close_timeout"]),
    ))
    readback = (await handle.describe()).raw_description
    current = next((item for item in readback.pending_activities
                    if item.activity_id == activity.activity_id), None)
    observed = (current.activity_options.start_to_close_timeout.ToTimedelta().total_seconds()
                if current is not None else None)
    if observed != seconds:
        raise RuntimeError("publication timeout change requires readback; activity may have completed")
    return {"run_id": run_id, "child_id": child_id, "child_run_id": info.execution.run_id,
            "activity_id": activity.activity_id, "attempt": current.attempt,
            "start_to_close_timeout_seconds": observed}


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
        planning = (await child_handle.query("get_planning_status")
                    if description.workflow_execution_info.type.name == "RequirementPlanningWorkflow"
                    else None)
        scheduler = (await child_handle.query("get_scheduler_status")
                     if description.workflow_execution_info.type.name == "TicketSchedulerWorkflow"
                     else None)
        result["pending_children"].append({
            "workflow_id": child.workflow_id, "run_id": child.run_id,
            "planning": planning,
            "scheduler": scheduler,
            "pending_activities": [{
                "activity_id": item.activity_id, "type": item.activity_type.name,
                "attempt": item.attempt, "state": item.state,
                "maximum_attempts": item.maximum_attempts,
                "last_started_time": str(item.last_started_time.ToDatetime()),
                "next_attempt_schedule_time": str(item.next_attempt_schedule_time.ToDatetime()),
                "start_to_close_timeout_seconds": item.activity_options.start_to_close_timeout.ToTimedelta().total_seconds(),
            } for item in description.pending_activities],
        })
    return result


async def _run_command(args: argparse.Namespace) -> object:
    client = await Client.connect(args.target_host)
    if args.command == "launch":
        request = load_request(args.request_file)
        return await launch_requirement(client, request)
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
        return await extend_spec_publication(client, args.run_id, args.publication_timeout_seconds)
    if args.command == "resolve-publication":
        return await resolve_spec_publication(
            client, args.run_id, load_publication_result(args.publication_file)
        )
    if args.command == "answer":
        return await answer_requirement(client, args.run_id, args.question_id, args.value)
    if args.details:
        return await diagnose_details(client, args.run_id)
    return await diagnose_requirement(client, args.run_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Control a requirement delivery run")
    parser.add_argument(
        "command",
        choices=(
            "launch",
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
    parser.add_argument("--question-id")
    parser.add_argument("--value")
    parser.add_argument("--publication-file")
    parser.add_argument("--publication-timeout-seconds", type=float)
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST)
    args = parser.parse_args()
    if args.command == "launch" and not args.request_file:
        parser.error("launch requires --request-file")
    if args.command != "launch" and not args.run_id:
        parser.error(f"{args.command} requires --run-id")
    if args.command == "answer" and (not args.question_id or not args.value):
        parser.error("answer requires --question-id and --value")
    if args.command == "resolve-publication" and not args.publication_file:
        parser.error("resolve-publication requires --publication-file")
    if args.command == "extend-publication" and args.publication_timeout_seconds is None:
        parser.error("extend-publication requires --publication-timeout-seconds")
    result = asyncio.run(_run_command(args))
    print(json.dumps(asdict(result) if hasattr(result, "__dataclass_fields__") else result, default=str, sort_keys=True))


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
        publication_timeout_seconds=planning_payload.get("publication_timeout_seconds", 300.0),
        publication_max_attempts=planning_payload.get("publication_max_attempts", 3),
        publication_retry_backoff_seconds=planning_payload.get("publication_retry_backoff_seconds", 1.0),
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
            (item[0], item[1]) for item in scheduler_payload.get("completion_operations", ())
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
