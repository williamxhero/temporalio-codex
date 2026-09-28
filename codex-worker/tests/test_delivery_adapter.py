import pytest

from temporalio_codex.delivery_adapter import FakeDeliveryAdapter
from temporalio_codex.delivery_models import (
    DeliveryOperation,
    DeliveryOutcome,
    DeliveryPhase,
    DeliveryReceipt,
)


def operation(**overrides) -> DeliveryOperation:
    values = dict(
        operation_id="op-1",
        run_id="run-1",
        phase=DeliveryPhase.CANDIDATE,
        repository="D:/repo",
        workspace="D:/workspace",
        base_sha="base-1",
    )
    values.update(overrides)
    return DeliveryOperation(**values)


async def test_fake_delivery_adapter_returns_typed_candidate_receipt() -> None:
    receipt = await FakeDeliveryAdapter().execute(operation())

    assert receipt.outcome is DeliveryOutcome.COMPLETED
    assert receipt.phase is DeliveryPhase.CANDIDATE
    assert receipt.candidate_sha == "candidate-op-1"


async def test_fake_delivery_adapter_caches_unknown_write_outcome() -> None:
    expected = DeliveryReceipt(
        operation_id="op-1",
        phase=DeliveryPhase.PULL_REQUEST,
        outcome=DeliveryOutcome.UNKNOWN,
        summary="create response was lost",
        candidate_sha="sha-1",
        readback_required=True,
    )
    adapter = FakeDeliveryAdapter({"op-1": expected})
    first = await adapter.execute(
        operation(phase=DeliveryPhase.PULL_REQUEST, candidate_sha="sha-1")
    )
    second = await adapter.execute(
        operation(phase=DeliveryPhase.PULL_REQUEST, candidate_sha="sha-1")
    )

    assert first == expected
    assert second == expected
    assert adapter.calls == ["op-1"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"operation_id": ""},
        {"workspace": None},
        {"phase": DeliveryPhase.ACCEPTANCE},
        {"phase": DeliveryPhase.MERGE},
    ],
)
async def test_delivery_operation_validation_rejects_missing_scope(overrides) -> None:
    with pytest.raises(ValueError):
        await FakeDeliveryAdapter().execute(operation(**overrides))
