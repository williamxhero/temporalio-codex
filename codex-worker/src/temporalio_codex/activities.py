import asyncio
from contextlib import suppress

from temporalio import activity

from temporalio_codex.codex_adapter import CodexAdapter
from temporalio_codex.codex_models import (
    CodexCapabilities,
    CodexFailure,
    CodexObservation,
    CodexOperation,
    CodexOutcome,
)
from temporalio_codex.models import (
    HeartbeatInput,
    StageInput,
    StageOutcome,
    StageResult,
)
from temporalio_codex.delivery_adapter import DeliveryAdapter
from temporalio_codex.delivery_models import (
    DeliveryOperation,
    DeliveryOutcome,
    DeliveryReceipt,
)


_codex_adapter: CodexAdapter | None = None
_delivery_git_adapter: DeliveryAdapter | None = None
_delivery_github_adapter: DeliveryAdapter | None = None


def configure_codex_adapter(adapter: CodexAdapter | None) -> None:
    global _codex_adapter
    _codex_adapter = adapter


def configure_delivery_adapters(
    git_adapter: DeliveryAdapter | None,
    github_adapter: DeliveryAdapter | None,
) -> None:
    global _delivery_git_adapter, _delivery_github_adapter
    _delivery_git_adapter = git_adapter
    _delivery_github_adapter = github_adapter


@activity.defn(name="foundation-stage")
async def foundation_stage(input: StageInput) -> StageResult:
    return StageResult(
        stage=input.stage,
        outcome=StageOutcome.COMPLETED,
        summary=(
            f"Accepted requirement: {input.requirement}"
            if input.answer is None
            else f"Accepted answer for {input.stage}: {input.answer}"
        ),
    )


@activity.defn(name="heartbeat-stage")
async def heartbeat_stage(input: HeartbeatInput) -> StageResult:
    activity.heartbeat(
        {
            "operation_id": input.operation_id,
            "stage": input.stage,
            "progress": input.progress,
        }
    )
    return StageResult(
        stage=input.stage,
        outcome=StageOutcome.COMPLETED,
        summary="heartbeat recorded",
    )


@activity.defn(name="codex-stage")
async def codex_stage(operation: CodexOperation) -> CodexObservation:
    if _codex_adapter is None:
        return CodexObservation(
            operation_id=operation.operation_id,
            role=operation.role,
            outcome=CodexOutcome.UNKNOWN,
            thread_id=operation.thread_id,
            summary="Codex SDK is unavailable; live qualification is not verified",
            failure=CodexFailure.UNKNOWN,
            readback_required=True,
            capabilities=CodexCapabilities(
                sdk_version="unavailable",
                can_interrupt_owned_turn=False,
                can_resume_thread=operation.thread_id is not None,
            ),
        )

    async def heartbeat_while_running() -> None:
        while True:
            activity.heartbeat(
                {
                    "operation_id": operation.operation_id,
                    "stage": operation.stage,
                    "progress": "codex operation running",
                }
            )
            await asyncio.sleep(10)

    heartbeat_task = asyncio.create_task(heartbeat_while_running())
    try:
        return await _codex_adapter.execute(operation)
    finally:
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task


@activity.defn(name="delivery-git-stage")
async def delivery_git_stage(operation: DeliveryOperation) -> DeliveryReceipt:
    if _delivery_git_adapter is None:
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.NOT_VERIFIED,
            summary="Git adapter is unavailable",
            candidate_sha=operation.candidate_sha,
            readback_required=True,
        )
    return await _delivery_git_adapter.execute(operation)


@activity.defn(name="delivery-github-stage")
async def delivery_github_stage(operation: DeliveryOperation) -> DeliveryReceipt:
    if _delivery_github_adapter is None:
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.NOT_VERIFIED,
            summary="GitHub adapter is unavailable",
            candidate_sha=operation.candidate_sha,
            readback_required=True,
        )
    return await _delivery_github_adapter.execute(operation)
