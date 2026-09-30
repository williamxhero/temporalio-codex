from dataclasses import dataclass

from temporalio import workflow


@dataclass(frozen=True)
class ExecutionProgress:
    workflow_id: str
    workflow_run_id: str
    phase: str
    status: str = "active"
    active_ticket: str | None = None
    pending_reason: str = ""
    next_action: str = ""
    retry_count: int = 0
    deadline: str | None = None
    timeout_seconds: float | None = None
    last_error: str | None = None


async def report_progress(**fields) -> ExecutionProgress:
    info = workflow.info()
    progress = ExecutionProgress(info.workflow_id, info.run_id, **fields)
    if info.parent is not None:
        await workflow.get_external_workflow_handle(
            info.parent.workflow_id, run_id=info.parent.run_id
        ).signal(
            "execution_progress",
            progress,
        )
    return progress
