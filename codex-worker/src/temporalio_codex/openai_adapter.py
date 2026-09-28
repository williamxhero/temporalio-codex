from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from temporalio_codex.codex_models import (
    CodexCapabilities,
    CodexObservation,
    CodexOperation,
    CodexOutcome,
    validate_operation,
)

try:
    from openai_codex import ApprovalMode, AsyncCodex, Sandbox
    from openai_codex.api import ReasoningEffort
except ImportError:  # pragma: no cover - exercised by optional dependency gate
    ApprovalMode = Any
    AsyncCodex = Any
    ReasoningEffort = Any
    Sandbox = Any


CodexFactory = Callable[[], AsyncCodex]


@dataclass
class OpenAICodexAdapter:
    factory: CodexFactory
    sdk_version: str
    _observations: dict[str, CodexObservation] = field(default_factory=dict)
    _owned_turns: dict[str, tuple[Any, Any]] = field(default_factory=dict)

    async def execute(self, operation: CodexOperation) -> CodexObservation:
        validate_operation(operation)
        existing = self._observations.get(operation.operation_id)
        if existing is not None:
            return existing

        codex = None
        capabilities = CodexCapabilities(
            sdk_version=self.sdk_version,
            can_interrupt_owned_turn=True,
            can_resume_thread=operation.thread_id is not None,
        )
        try:
            codex = self.factory()
            thread = await self._get_thread(codex, operation)
            turn_handle = thread.turn(
                operation.prompt,
                approval_mode=self._approval_mode(operation.approval_policy),
                cwd=operation.repository,
                effort=ReasoningEffort(operation.effort),
                model=operation.model,
                sandbox=self._sandbox(operation.allowed_scope),
            )
            self._owned_turns[operation.operation_id] = (codex, turn_handle)
            result = await turn_handle.run()
            observation = self._observation_from_result(
                operation,
                result,
                capabilities,
                thread_id=getattr(thread, "id", None) or operation.thread_id,
            )
        except Exception as error:
            observation = CodexObservation(
                operation_id=operation.operation_id,
                role=operation.role,
                outcome=CodexOutcome.UNKNOWN,
                thread_id=operation.thread_id,
                summary=f"Codex outcome is unknown: {type(error).__name__}",
                readback_required=True,
                capabilities=capabilities,
            )
        finally:
            self._owned_turns.pop(operation.operation_id, None)
            if codex is not None:
                await self._close(codex)

        self._observations[operation.operation_id] = observation
        return observation

    async def interrupt_owned_turn(self, operation_id: str) -> bool:
        owned = self._owned_turns.get(operation_id)
        if owned is None:
            return False
        _, turn_handle = owned
        await turn_handle.interrupt()
        return True

    async def interrupt_historical_turn(self, operation: CodexOperation) -> bool:
        validate_operation(operation)
        return False

    async def _get_thread(self, codex: AsyncCodex, operation: CodexOperation):
        if operation.thread_id is not None:
            return await codex.thread_resume(
                operation.thread_id,
                cwd=operation.repository,
                model=operation.model,
                sandbox=self._sandbox(operation.allowed_scope),
            )
        return await codex.thread_start(
            approval_mode=self._approval_mode(operation.approval_policy),
            cwd=operation.repository,
            model=operation.model,
            sandbox=self._sandbox(operation.allowed_scope),
        )

    @staticmethod
    def _approval_mode(policy: str):
        if policy == "deny_all":
            return ApprovalMode.deny_all
        if policy == "auto_review":
            return ApprovalMode.auto_review
        raise ValueError(f"unsupported approval policy: {policy}")

    @staticmethod
    def _sandbox(scope: tuple[str, ...]):
        if scope == ("read_only",):
            return Sandbox.read_only
        return Sandbox.workspace_write

    @staticmethod
    def _observation_from_result(operation, result, capabilities, thread_id):
        status = getattr(result.status, "value", str(result.status))
        if status == "completed":
            return CodexObservation(
                operation_id=operation.operation_id,
                role=operation.role,
                outcome=CodexOutcome.COMPLETED,
                thread_id=thread_id,
                turn_id=result.id,
                summary=result.final_response or "",
                capabilities=capabilities,
            )
        return CodexObservation(
            operation_id=operation.operation_id,
            role=operation.role,
            outcome=CodexOutcome.FAILED,
            thread_id=thread_id,
            turn_id=result.id,
            summary=f"Codex turn ended with status: {status}",
            capabilities=capabilities,
        )

    @staticmethod
    async def _close(codex: AsyncCodex) -> None:
        close = getattr(codex, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result
