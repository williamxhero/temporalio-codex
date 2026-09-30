from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from temporalio.api.activity.v1 import ActivityOptions
from temporalio.api.common.v1 import ActivityType, WorkflowExecution
from temporalio.api.workflow.v1 import PendingActivityInfo, WorkflowExecutionInfo
from temporalio.api.workflowservice.v1 import DescribeWorkflowExecutionResponse

from temporalio_codex.entry import extend_spec_publication
from temporalio_codex.execution_status import ExecutionProgress
from temporalio_codex.whole_flow_models import WholeFlowPhase, WholeFlowStatus
from temporalio_codex.whole_flow_workflows import RequirementDeliveryWorkflow


def recovery_client(activity_type="publish-spec-issues", parent_id="delivery"):
    pending = PendingActivityInfo(
        activity_id="3", activity_type=ActivityType(name=activity_type), attempt=9,
        activity_options=ActivityOptions(start_to_close_timeout={"seconds": 30}),
    )
    raw = DescribeWorkflowExecutionResponse(
        workflow_execution_info=WorkflowExecutionInfo(
            execution=WorkflowExecution(workflow_id="delivery:planning", run_id="child-run"),
            parent_execution=WorkflowExecution(workflow_id=parent_id),
            type={"name": "RequirementPlanningWorkflow"}, status=1,
        ),
        pending_activities=[pending],
    )
    handle = SimpleNamespace(describe=AsyncMock(return_value=SimpleNamespace(raw_description=raw)))
    async def update(request):
        raw.pending_activities[0].activity_options.CopyFrom(request.activity_options)
    service = SimpleNamespace(update_activity_options=AsyncMock(side_effect=update))
    return SimpleNamespace(namespace="default", identity="test", workflow_service=service,
                           get_workflow_handle=lambda _: handle)


async def test_extend_only_existing_publication_and_read_back():
    client = recovery_client()
    receipt = await extend_spec_publication(client, "delivery", 300)
    request = client.workflow_service.update_activity_options.call_args.args[0]
    assert request.execution.run_id == "child-run"
    assert request.id == "3"
    assert list(request.update_mask.paths) == ["start_to_close_timeout"]
    assert receipt["start_to_close_timeout_seconds"] == 300
    assert receipt["run_id"] == "delivery"


@pytest.mark.parametrize("activity_type,parent_id", [("other", "delivery"), ("publish-spec-issues", "other")])
async def test_extend_refuses_unrelated_activity_or_parent(activity_type, parent_id):
    client = recovery_client(activity_type, parent_id)
    with pytest.raises(ValueError):
        await extend_spec_publication(client, "delivery", 300)
    client.workflow_service.update_activity_options.assert_not_called()


async def test_parent_retry_accepts_live_blocked_planning_child(monkeypatch):
    import temporalio_codex.whole_flow_workflows as module
    external = SimpleNamespace(signal=AsyncMock())
    monkeypatch.setattr(module.workflow, "info", lambda: SimpleNamespace(workflow_id="delivery"))
    monkeypatch.setattr(module.workflow, "get_external_workflow_handle", lambda _: external)
    parent = RequirementDeliveryWorkflow()
    parent._active_child_id = "delivery:planning"
    parent._active_child_run_id = "child-run"
    parent.execution_progress(ExecutionProgress(
        "delivery:planning", "child-run", "planning", "blocked",
        pending_reason="readback unknown",
    ))
    assert await parent.retry_spec_publication()
    external.signal.assert_awaited_once_with("retry_spec_publication_signal")
    assert parent._phase is WholeFlowPhase.PLANNING
    assert parent._status is WholeFlowStatus.ACTIVE
    assert parent._reason == ""
    parent._active_child_id = "delivery:codex:a"
    assert not await parent.retry_spec_publication()
