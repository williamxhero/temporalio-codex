from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from temporalio_codex.codex_models import (
    CodexCapabilities,
    CodexObservation,
    CodexOperation,
    CodexOutcome,
    validate_operation,
)


class CodexAdapter(Protocol):
    async def execute(self, operation: CodexOperation) -> CodexObservation: ...


@dataclass
class FakeCodexAdapter:
    observations: Mapping[str, CodexObservation]

    async def execute(self, operation: CodexOperation) -> CodexObservation:
        validate_operation(operation)
        observation = self.observations.get(operation.operation_id)
        if observation is not None:
            return observation
        return CodexObservation(
            operation_id=operation.operation_id,
            role=operation.role,
            outcome=CodexOutcome.COMPLETED,
            thread_id=f"fake-thread-{operation.operation_id}",
            turn_id=f"fake-turn-{operation.operation_id}",
            summary="fake Codex operation completed",
            capabilities=CodexCapabilities(
                sdk_version="fake",
                can_interrupt_owned_turn=False,
                can_resume_thread=False,
            ),
        )
