import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.api.enums.v1 import EventType, WorkflowExecutionStatus
from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import WorkflowHistory
from temporalio.converter import DefaultPayloadConverter
from temporalio_codex import entry
from temporalio_codex.candidate_activities import CandidateCaptureResult
from temporalio_codex.codex_models import (
    CodexObservation,
    CodexOperation,
    CodexOutcome,
    CodexRole,
)
from temporalio_codex.delivery_models import CandidateEvidence
from temporalio_codex.entry_models import EntryPhase, EntryStatus
from temporalio_codex.models import RunInput, StageDefinition
from temporalio_codex.ticket_scheduler import SchedulerResult, SchedulerStatus
from temporalio_codex.whole_flow_models import (
    WholeFlowPhase,
    WholeFlowResult,
    WholeFlowStatus,
)


@pytest.mark.parametrize(
    "reason",
    [
        "ticket scheduling did not complete",
        "Codex execution failed for ticket first-a: Independent review rejected candidate",
    ],
)
def test_completed_failure_is_bound_to_durable_scheduler_result(reason):
    scheduler = SchedulerResult(
        "delivery:tickets:first", SchedulerStatus.BLOCKED, (), (), reason
    )
    result = WholeFlowResult(
        "delivery",
        WholeFlowPhase.FAILED,
        WholeFlowStatus.FAILED,
        scheduler={"status": "failed", "runs": [scheduler.__dict__]},
        reason=reason,
    )
    entry._validate_completed_ticket_failure(
        result,
        ticket_history(scheduler=scheduler),
        DefaultPayloadConverter(),
    )


@pytest.mark.parametrize(
    "change", ["phase", "status", "reason", "scheduler", "child_status"]
)
def test_completed_failure_refuses_unbound_failure(change):
    reason = "Codex execution failed for ticket first-a: Independent review rejected candidate"
    scheduler = SchedulerResult(
        "delivery:tickets:first", SchedulerStatus.BLOCKED, (), (), reason
    )
    values = {
        "workflow_id": "delivery",
        "phase": WholeFlowPhase.FAILED,
        "status": WholeFlowStatus.FAILED,
        "scheduler": {"status": "failed", "runs": [scheduler.__dict__]},
        "reason": reason,
    }
    if change == "child_status":
        scheduler = SchedulerResult(
            "delivery:tickets:first", SchedulerStatus.COMPLETED, (), (), reason
        )
    else:
        values[change] = {
            "phase": WholeFlowPhase.DELIVERY,
            "status": WholeFlowStatus.COMPLETED,
            "reason": "unrelated delivery failure",
            "scheduler": None,
        }[change]
    with pytest.raises(ValueError, match="verified ticket scheduling failure"):
        entry._validate_completed_ticket_failure(
            WholeFlowResult(**values),
            ticket_history(scheduler=scheduler),
            DefaultPayloadConverter(),
        )


def ticket_history(*, workflow_type="TicketSchedulerWorkflow", scheduler=None):
    converter = DefaultPayloadConverter()
    events = [
        HistoryEvent(
            event_id=1,
            event_type=EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        ),
        HistoryEvent(
            event_id=45,
            event_type=EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
        ),
        HistoryEvent(
            event_id=46,
            event_type=EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
            start_child_workflow_execution_initiated_event_attributes={
                "workflow_type": {"name": workflow_type},
            },
        ),
    ]
    if scheduler is None:
        events.append(
            HistoryEvent(
                event_id=51,
                event_type=EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
                child_workflow_execution_terminated_event_attributes={
                    "initiated_event_id": 46,
                },
            )
        )
    else:
        events.append(
            HistoryEvent(
                event_id=51,
                event_type=EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
                child_workflow_execution_completed_event_attributes={
                    "initiated_event_id": 46,
                    "result": {"payloads": converter.to_payloads([scheduler])},
                },
            )
        )
    return WorkflowHistory("delivery", events)


def test_reset_point_requires_ticket_child_and_completed_workflow_task():
    converter = DefaultPayloadConverter()
    assert entry._ticket_child_reset_point(ticket_history(), converter) == 45
    with pytest.raises(ValueError, match="TicketSchedulerWorkflow"):
        entry._ticket_child_reset_point(
            ticket_history(workflow_type="DeliveryWorkflow"), converter
        )
    history = ticket_history()
    history.events[1].event_type = EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED
    with pytest.raises(ValueError, match="completed workflow task"):
        entry._ticket_child_reset_point(history, converter)


def test_reset_allows_completed_prefix_when_candidate_is_frozen():
    result = SchedulerResult(
        "delivery:tickets:first",
        SchedulerStatus.BLOCKED,
        (),
        ("first-a",),
    )
    assert (
        entry._ticket_child_reset_point(
            ticket_history(scheduler=result),
            DefaultPayloadConverter(),
            allow_frozen_candidate=True,
        )
        == 45
    )


@pytest.mark.parametrize(
    "status,completed_tickets,codex_results",
    [
        (SchedulerStatus.COMPLETED, (), ()),
        (SchedulerStatus.BLOCKED, ("first-a",), ()),
        (SchedulerStatus.BLOCKED, (), ({"outcome": "unknown"},)),
    ],
)
def test_reset_refuses_completed_work_or_external_results(
    status, completed_tickets, codex_results
):
    result = SchedulerResult(
        "delivery:tickets:first",
        status,
        (),
        completed_tickets,
        codex_results=codex_results,
    )
    with pytest.raises(ValueError):
        entry._ticket_child_reset_point(
            ticket_history(scheduler=result), DefaultPayloadConverter()
        )


def recovery_client(*, source_run="source", identity="a" * 64):
    history = ticket_history()
    converter = SimpleNamespace(
        from_payloads=lambda *_: [
            SimpleNamespace(
                entry_input_identity=identity,
            )
        ]
    )
    handle = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(
                raw_description=SimpleNamespace(
                    workflow_execution_info=SimpleNamespace(
                        execution=SimpleNamespace(run_id=source_run),
                        status=WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED,
                    ),
                )
            )
        ),
        fetch_history=AsyncMock(return_value=history),
        signal=AsyncMock(),
    )
    child = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(
                raw_description=SimpleNamespace(
                    workflow_execution_info=SimpleNamespace(
                        status=WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
                        type=SimpleNamespace(name="TicketSchedulerWorkflow"),
                        parent_execution=SimpleNamespace(
                            workflow_id="delivery", run_id="reset"
                        ),
                    ),
                )
            )
        ),
    )
    reset = AsyncMock(return_value=SimpleNamespace(run_id="reset"))
    get_handle = MagicMock(
        side_effect=lambda workflow_id, **_: (
            child if workflow_id == "delivery:tickets:first" else handle
        )
    )
    client = SimpleNamespace(
        namespace="default",
        identity="test",
        get_workflow_handle=get_handle,
        data_converter=SimpleNamespace(payload_converter=converter),
        workflow_service=SimpleNamespace(reset_workflow_execution=reset),
    )
    return client, handle, history


@pytest.mark.parametrize(
    "source_run,identity", [("other", "a" * 64), ("source", "b" * 64)]
)
async def test_recovery_rejects_source_or_input_drift(source_run, identity):
    client, handle, _ = recovery_client(source_run=source_run, identity=identity)
    with pytest.raises(ValueError):
        await entry.recover_failed_requirement(
            client,
            workflow_id="delivery",
            source_run_id="source",
            expected_input_identity="a" * 64,
        )
    client.workflow_service.reset_workflow_execution.assert_not_awaited()
    handle.signal.assert_not_awaited()


async def test_replay_failure_prevents_reset(monkeypatch):
    client, handle, _ = recovery_client()
    replay = AsyncMock(side_effect=RuntimeError("nondeterminism"))
    monkeypatch.setattr(
        entry, "Replayer", lambda **_: SimpleNamespace(replay_workflow=replay)
    )
    with pytest.raises(RuntimeError, match="nondeterminism"):
        await entry.recover_failed_requirement(
            client,
            workflow_id="delivery",
            source_run_id="source",
            expected_input_identity="a" * 64,
        )
    client.workflow_service.reset_workflow_execution.assert_not_awaited()
    handle.signal.assert_not_awaited()


async def test_reset_uses_source_identity_then_signals_verified_child(monkeypatch):
    client, handle, history = recovery_client()
    replay = AsyncMock()
    monkeypatch.setattr(
        entry, "Replayer", lambda **_: SimpleNamespace(replay_workflow=replay)
    )
    snapshot = SimpleNamespace(
        entry_input_identity="a" * 64,
        phase=EntryPhase.TICKETS,
        status=EntryStatus.DURABLE_WAITING,
        active_spec="first",
        active_ticket=None,
        next_action="execute tickets",
    )
    diagnose = AsyncMock(return_value=snapshot)
    monkeypatch.setattr(entry, "diagnose_requirement", diagnose)

    receipt = await entry.recover_failed_requirement(
        client,
        workflow_id="delivery",
        source_run_id="source",
        expected_input_identity="a" * 64,
    )

    replay.assert_awaited_once_with(history)
    request = client.workflow_service.reset_workflow_execution.call_args.args[0]
    assert request.workflow_execution.workflow_id == "delivery"
    assert request.workflow_execution.run_id == "source"
    assert request.workflow_task_finish_event_id == 45
    diagnose.assert_awaited_once_with(client, "delivery")
    handle.signal.assert_awaited_once_with(
        "recover_ticket_execution", "delivery:ticket-recovery:source"
    )
    assert receipt["run_id"] == "reset"
    assert receipt["recovery_signal_sent"] is True


def rejected_review_history(
    *,
    review_sha="b" * 40,
    findings=None,
    failure="review approval evidence is missing or does not match candidate SHA",
    outcome=CodexOutcome.COMPLETED,
):
    converter = DefaultPayloadConverter()
    candidate = CandidateEvidence("repo", "work", "a" * 40, "a" * 40)
    frozen = CandidateEvidence("repo", "work", "a" * 40, "b" * 40)
    events = [
        HistoryEvent(
            event_id=1,
            event_type=EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            workflow_execution_started_event_attributes={
                "input": {
                    "payloads": converter.to_payloads(
                        [
                            RunInput(
                                "requirement",
                                (
                                    StageDefinition(
                                        "implementation",
                                        role=CodexRole.IMPLEMENTATION,
                                        allowed_scope=("src",),
                                    ),
                                ),
                                candidate=candidate,
                            ),
                        ]
                    )
                }
            },
        )
    ]
    observations = (
        (
            "codex-stage",
            CodexObservation("implement-op", CodexRole.IMPLEMENTATION, outcome),
        ),
        ("capture-codex-candidate", CandidateCaptureResult(frozen)),
        (
            "codex-stage",
            CodexObservation(
                "review-op",
                CodexRole.REVIEW,
                CodexOutcome.COMPLETED,
                summary=json.dumps(
                    {
                        "candidate_sha": review_sha,
                        "verdict": "rejected",
                        "findings": ["test environment failed"]
                        if findings is None
                        else findings,
                    }
                ),
            ),
        ),
    )
    for index, (activity_type, result) in enumerate(observations):
        scheduled_id = 2 + index * 2
        events.extend(
            [
                HistoryEvent(
                    event_id=scheduled_id,
                    event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
                    activity_task_scheduled_event_attributes={
                        "activity_type": {"name": activity_type}
                    },
                ),
                HistoryEvent(
                    event_id=scheduled_id + 1,
                    event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
                    activity_task_completed_event_attributes={
                        "scheduled_event_id": scheduled_id,
                        "result": {"payloads": converter.to_payloads([result])},
                    },
                ),
            ]
        )
    events.extend(
        [
            HistoryEvent(
                event_id=8,
                event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
                activity_task_scheduled_event_attributes={
                    "activity_type": {"name": "capture-codex-candidate"}
                },
            ),
            HistoryEvent(
                event_id=9,
                event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
                activity_task_failed_event_attributes={
                    "scheduled_event_id": 8,
                    "failure": {"message": failure},
                },
            ),
        ]
    )
    child_history = WorkflowHistory("codex", events)

    def completed_child(workflow_type, workflow_id, run_id):
        return HistoryEvent(
            event_id=10,
            event_type=EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
            child_workflow_execution_completed_event_attributes={
                "workflow_type": {"name": workflow_type},
                "workflow_execution": {"workflow_id": workflow_id, "run_id": run_id},
            },
        )

    parent = WorkflowHistory(
        "delivery",
        [completed_child("TicketSchedulerWorkflow", "scheduler", "scheduler-run")],
    )
    scheduler = WorkflowHistory(
        "scheduler", [completed_child("CodexRunWorkflow", "codex", "codex-run")]
    )
    histories = {"scheduler": scheduler, "codex": child_history}
    client = SimpleNamespace(
        data_converter=SimpleNamespace(payload_converter=converter),
        get_workflow_handle=MagicMock(
            side_effect=lambda workflow_id, **_: SimpleNamespace(
                fetch_history=AsyncMock(return_value=histories[workflow_id]),
            )
        ),
    )
    return client, parent, frozen


async def test_rejected_review_recovery_preserves_candidate_without_approving(
    monkeypatch,
):
    client, history, frozen = rejected_review_history()
    capture = AsyncMock(return_value=CandidateCaptureResult(frozen))
    monkeypatch.setattr(entry, "capture_codex_candidate", capture)
    recovered = await entry._freeze_implementation_for_recovery(client, history)
    assert recovered[0].candidate_sha == frozen.candidate_sha
    assert recovered[1] is False
    assert capture.call_args.args[0].review_json is None
    client.get_workflow_handle.assert_any_call("scheduler", run_id="scheduler-run")
    client.get_workflow_handle.assert_any_call("codex", run_id="codex-run")


async def test_pre_frozen_review_recovery_captures_repaired_candidate(monkeypatch):
    client, history, frozen = rejected_review_history()
    child = await client.get_workflow_handle("codex").fetch_history()
    child.events[:] = [
        event for event in child.events if event.event_id not in {2, 3, 4, 5}
    ]
    payloads = child.events[
        0
    ].workflow_execution_started_event_attributes.input.payloads
    del payloads[:]
    payloads.extend(
        DefaultPayloadConverter().to_payloads(
            [
                RunInput(
                    "requirement",
                    (StageDefinition("review", role=CodexRole.REVIEW),),
                    candidate=frozen,
                ),
            ]
        )
    )
    repaired = CandidateEvidence("repo", "work", "a" * 40, "c" * 40)
    capture = AsyncMock(return_value=CandidateCaptureResult(repaired))
    monkeypatch.setattr(entry, "capture_codex_candidate", capture)
    recovered = await entry._freeze_implementation_for_recovery(client, history)
    assert recovered == (repaired, False)
    assert capture.call_args.args[0].allowed_scope == ()
    assert capture.call_args.args[0].review_json is None


async def test_completed_pre_frozen_review_recovery_rechecks_workspace(monkeypatch):
    client, history, frozen = rejected_review_history()
    child = await client.get_workflow_handle("codex").fetch_history()
    child.events[:] = [event for event in child.events if event.event_id in {1, 6, 7}]
    converter = DefaultPayloadConverter()
    payloads = child.events[
        0
    ].workflow_execution_started_event_attributes.input.payloads
    del payloads[:]
    payloads.extend(
        converter.to_payloads(
            [
                RunInput(
                    "requirement",
                    (StageDefinition("review", role=CodexRole.REVIEW),),
                    candidate=frozen,
                ),
            ]
        )
    )
    repaired = CandidateEvidence("repo", "work", "a" * 40, "c" * 40)
    capture = AsyncMock(return_value=CandidateCaptureResult(repaired))
    monkeypatch.setattr(entry, "capture_codex_candidate", capture)
    assert await entry._freeze_implementation_for_recovery(client, history) == (
        repaired,
        False,
    )
    assert capture.call_args.args[0].review_json is None


@pytest.mark.parametrize(
    "candidate_sha,review_only", [("a" * 40, False), ("b" * 40, True)]
)
async def test_planning_timeout_recovery_does_not_skip_unimplemented_base(
    monkeypatch, candidate_sha, review_only
):
    client, history, _ = rejected_review_history()
    child = await client.get_workflow_handle("codex").fetch_history()
    converter = DefaultPayloadConverter()
    operation = CodexOperation(
        "planning-op",
        "codex",
        "planning",
        CodexRole.PLANNING,
        "work",
        ("src",),
        "deny_all",
        "gpt-5-codex",
        "low",
        "plan",
    )
    child.events[:] = [
        child.events[0],
        HistoryEvent(
            event_id=2,
            event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
            activity_task_scheduled_event_attributes={
                "activity_type": {"name": "codex-stage"},
                "input": {"payloads": converter.to_payloads([operation])},
            },
        ),
        HistoryEvent(
            event_id=3,
            event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
            activity_task_timed_out_event_attributes={"scheduled_event_id": 2},
        ),
    ]
    captured = CandidateEvidence("repo", "work", "a" * 40, candidate_sha)
    monkeypatch.setattr(
        entry,
        "capture_codex_candidate",
        AsyncMock(return_value=CandidateCaptureResult(captured)),
    )
    recovered = await entry._freeze_implementation_for_recovery(client, history)
    assert recovered == (captured, review_only)


@pytest.mark.parametrize(
    "capture_error", [None, "candidate changes exceed the authorized scope"]
)
async def test_scope_capture_recovery_rechecks_scope_before_reset(
    monkeypatch, capture_error
):
    client, history, frozen = rejected_review_history(
        failure="candidate changes exceed the authorized scope"
    )
    child = await client.get_workflow_handle("codex").fetch_history()
    child.events[:] = [
        event for event in child.events if event.event_id not in {4, 5, 6, 7}
    ]
    capture = AsyncMock(
        return_value=CandidateCaptureResult(frozen),
        side_effect=RuntimeError(capture_error) if capture_error else None,
    )
    monkeypatch.setattr(entry, "capture_codex_candidate", capture)
    if capture_error:
        with pytest.raises(RuntimeError, match="authorized scope"):
            await entry._freeze_implementation_for_recovery(client, history)
    else:
        recovered = await entry._freeze_implementation_for_recovery(client, history)
        assert recovered == (frozen, True)
    assert capture.call_args.args[0].allowed_scope == ("src",)
    assert capture.call_args.args[0].review_json is None


@pytest.mark.parametrize(
    "parameters",
    [
        {"review_sha": "c" * 40},
        {"findings": "malformed findings"},
        {"findings": []},
        {"failure": "unrelated capture failure"},
        {"outcome": CodexOutcome.UNKNOWN},
    ],
)
async def test_rejected_review_recovery_refuses_unbound_or_unknown_evidence(
    monkeypatch, parameters
):
    client, history, frozen = rejected_review_history(**parameters)
    capture = AsyncMock(return_value=CandidateCaptureResult(frozen))
    monkeypatch.setattr(entry, "capture_codex_candidate", capture)
    with pytest.raises(ValueError):
        await entry._freeze_implementation_for_recovery(client, history)
    capture.assert_not_awaited()


@pytest.mark.parametrize(
    "status,cwd,turn_id",
    [
        ("inProgress", "work", "exact"),
        ("completed", "work", "exact"),
        ("interrupted", "other", "exact"),
        ("interrupted", "work", "other"),
    ],
)
def test_interrupted_readback_rejects_active_completed_or_drifted_turn(
    status, cwd, turn_id
):
    thread = SimpleNamespace(
        id="thread", cwd=cwd, turns=[SimpleNamespace(id=turn_id, status=status)]
    )
    with pytest.raises(ValueError):
        entry._validate_interrupted_readback(thread, "thread", "exact", "work")


def test_interrupted_readback_accepts_only_exact_stopped_turn():
    thread = SimpleNamespace(
        id="thread",
        cwd="work",
        turns=[SimpleNamespace(id="exact", status="interrupted")],
    )
    entry._validate_interrupted_readback(thread, "thread", "exact", "work")


def test_interrupted_readback_accepts_sdk_path_buffer():
    class AbsolutePathBuf:
        root = "work"

    thread = SimpleNamespace(
        id="thread",
        cwd=AbsolutePathBuf(),
        turns=[SimpleNamespace(id="exact", status="interrupted")],
    )
    entry._validate_interrupted_readback(thread, "thread", "exact", "work")
