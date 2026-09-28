from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from dataclasses import asdict, replace

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

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
from temporalio_codex.spec_issue_adapter import SpecDraft
from temporalio_codex.ticket_scheduler import SchedulerInput, SpecPlan, TicketPlan
from temporalio_codex.whole_flow_models import (
    PlanningPayload,
    SpecCodexPlan,
    SpecDeliveryPlan,
    WholeFlowInput,
)
from temporalio_codex.delivery_models import DeliveryInput
from temporalio_codex.summary_adapter import SummaryPublicationInput
from temporalio_codex.settings import DEFAULT_TARGET_HOST
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
    )


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
    if args.command == "answer":
        return await answer_requirement(client, args.run_id, args.question_id, args.value)
    return await diagnose_requirement(client, args.run_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Control a requirement delivery run")
    parser.add_argument(
        "command",
        choices=("launch", "status", "diagnose", "pause", "resume", "answer", "cancel"),
    )
    parser.add_argument("--run-id")
    parser.add_argument("--request-file")
    parser.add_argument("--question-id")
    parser.add_argument("--value")
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST)
    args = parser.parse_args()
    if args.command == "launch" and not args.request_file:
        parser.error("launch requires --request-file")
    if args.command != "launch" and not args.run_id:
        parser.error(f"{args.command} requires --run-id")
    if args.command == "answer" and (not args.question_id or not args.value):
        parser.error("answer requires --question-id and --value")
    result = asyncio.run(_run_command(args))
    print(json.dumps(asdict(result) if hasattr(result, "__dataclass_fields__") else result, default=str, sort_keys=True))


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
