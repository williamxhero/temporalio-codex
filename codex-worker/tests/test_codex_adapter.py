import pytest

from temporalio_codex.codex_adapter import FakeCodexAdapter
from temporalio_codex.codex_models import (
    CodexObservation,
    CodexOperation,
    CodexRole,
    CodexOutcome,
)


def make_operation(**overrides) -> CodexOperation:
    values = dict(
        operation_id="op-1",
        run_id="run-1",
        stage="planning",
        role=CodexRole.PLANNING,
        repository="D:/repo",
        allowed_scope=("src/",),
        approval_policy="auto_review",
        model="gpt-5-codex",
        effort="medium",
        prompt="turn this requirement into a plan",
    )
    values.update(overrides)
    return CodexOperation(**values)


async def test_fake_adapter_returns_typed_observation() -> None:
    observation = await FakeCodexAdapter({}).execute(make_operation())

    assert observation.operation_id == "op-1"
    assert observation.role is CodexRole.PLANNING
    assert observation.outcome is CodexOutcome.COMPLETED
    assert observation.capabilities.sdk_version == "fake"


async def test_fake_adapter_preserves_explicit_pending_observation() -> None:
    expected = FakeCodexAdapter(
        {
            "op-1": CodexObservation(
                operation_id="op-1",
                role=CodexRole.IMPLEMENTATION,
                outcome=CodexOutcome.PENDING_INPUT,
                pending_question="Which scope should be changed?",
            )
        }
    )

    actual = await expected.execute(make_operation(role=CodexRole.IMPLEMENTATION))

    assert actual.outcome is CodexOutcome.PENDING_INPUT
    assert actual.pending_question == "Which scope should be changed?"


@pytest.mark.parametrize(
    "field,value",
    [("operation_id", ""), ("repository", ""), ("allowed_scope", ())],
)
async def test_fake_adapter_rejects_invalid_operation(field, value) -> None:
    with pytest.raises(ValueError):
        await FakeCodexAdapter({}).execute(make_operation(**{field: value}))
