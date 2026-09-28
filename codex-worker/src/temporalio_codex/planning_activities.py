from temporalio import activity

from temporalio_codex.planning_models import (
    GrillPreparationInput,
    GrillQuestion,
    SourceOrigin,
)


@activity.defn(name="prepare_grill")
async def prepare_grill(
    input: GrillPreparationInput,
) -> tuple[GrillQuestion, ...]:
    if input.origin is SourceOrigin.HISTORICAL_CHAT:
        prompt = "Which decisions in the historical chat are confirmed requirements?"
    else:
        prompt = "What user-visible outcome must this brief deliver?"
    return (GrillQuestion(number=1, prompt=prompt),)
