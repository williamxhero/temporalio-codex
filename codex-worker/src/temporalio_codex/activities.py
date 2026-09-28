from temporalio import activity

from temporalio_codex.codex_adapter import CodexAdapter
from temporalio_codex.codex_models import (
    CodexCapabilities,
    CodexFailure,
    CodexObservation,
    CodexOperation,
    CodexOutcome,
)
from temporalio_codex.models import StageInput, StageOutcome, StageResult


_codex_adapter: CodexAdapter | None = None


def configure_codex_adapter(adapter: CodexAdapter | None) -> None:
    global _codex_adapter
    _codex_adapter = adapter


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
    return await _codex_adapter.execute(operation)
