from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

from temporalio_codex.delivery_models import (
    DeliveryOperation,
    DeliveryOutcome,
    DeliveryPhase,
    DeliveryReceipt,
    validate_operation,
)


class DeliveryAdapter(Protocol):
    async def execute(self, operation: DeliveryOperation) -> DeliveryReceipt: ...


class GitAdapter(Protocol):
    async def execute(self, operation: DeliveryOperation) -> DeliveryReceipt: ...


class GitHubAdapter(Protocol):
    async def execute(self, operation: DeliveryOperation) -> DeliveryReceipt: ...


@dataclass
class FakeDeliveryAdapter:
    receipts: Mapping[str, DeliveryReceipt] = field(default_factory=dict)
    calls: list[str] = field(default_factory=list)
    _cache: dict[str, DeliveryReceipt] = field(default_factory=dict)

    async def execute(self, operation: DeliveryOperation) -> DeliveryReceipt:
        validate_operation(operation)
        cached = self._cache.get(operation.operation_id)
        if cached is not None:
            return cached

        self.calls.append(operation.operation_id)
        receipt = self.receipts.get(operation.operation_id)
        if receipt is None:
            receipt = self._default_receipt(operation)
        self._cache[operation.operation_id] = receipt
        return receipt

    @staticmethod
    def _default_receipt(operation: DeliveryOperation) -> DeliveryReceipt:
        candidate_sha = operation.candidate_sha
        if operation.phase is DeliveryPhase.CANDIDATE:
            candidate_sha = candidate_sha or f"candidate-{operation.operation_id}"
        pull_request_number = operation.pull_request_number
        if operation.phase is DeliveryPhase.PULL_REQUEST:
            pull_request_number = pull_request_number or 1
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.COMPLETED,
            summary=f"fake {operation.phase.value} completed",
            candidate_sha=candidate_sha,
            acceptance_version=operation.acceptance_version,
            pull_request_number=pull_request_number,
            merged_sha=(candidate_sha if operation.phase is DeliveryPhase.MERGE else None),
        )
