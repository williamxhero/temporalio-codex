from temporalio import activity

from temporalio_codex.models import StageInput, StageOutcome, StageResult


@activity.defn(name="foundation-stage")
async def foundation_stage(input: StageInput) -> StageResult:
    return StageResult(
        stage=input.stage,
        outcome=StageOutcome.COMPLETED,
        summary=f"Accepted requirement: {input.requirement}",
    )
