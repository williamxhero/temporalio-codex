import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from temporalio_codex.codex_models import (
    CodexCapabilities,
    CodexFailure,
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
        thread_id = operation.thread_id
        turn_id = None
        capabilities = CodexCapabilities(
            sdk_version=self.sdk_version,
            can_interrupt_owned_turn=True,
            can_resume_thread=operation.thread_id is not None,
        )
        try:
            codex = self.factory()
            thread = await self._get_thread(codex, operation)
            thread_id = getattr(thread, "id", None) or thread_id
            turn_handle = thread.turn(
                operation.prompt,
                approval_mode=self._approval_mode(operation.approval_policy),
                cwd=operation.repository,
                effort=ReasoningEffort(operation.effort),
                model=operation.model,
                sandbox=self._sandbox(operation.allowed_scope),
            )
            turn_id = getattr(turn_handle, "id", None)
            self._owned_turns[operation.operation_id] = (codex, turn_handle)
            result = await turn_handle.run()
            observation = self._observation_from_result(
                operation,
                result,
                capabilities,
                thread_id=thread_id,
            )
        except Exception as error:
            failure = self._classify_exception(error)
            observation = CodexObservation(
                operation_id=operation.operation_id,
                role=operation.role,
                outcome=(
                    CodexOutcome.FAILED
                    if failure is CodexFailure.REJECTED
                    else CodexOutcome.UNKNOWN
                ),
                thread_id=thread_id,
                turn_id=turn_id,
                summary=self._failure_summary(failure),
                failure=failure,
                readback_required=failure
                in (
                    CodexFailure.TIMEOUT,
                    CodexFailure.STREAM_DISCONNECTED,
                    CodexFailure.UNKNOWN,
                ),
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
        turn_id = getattr(result, "id", None)
        if status == "completed":
            return CodexObservation(
                operation_id=operation.operation_id,
                role=operation.role,
                outcome=CodexOutcome.COMPLETED,
                thread_id=thread_id,
                turn_id=turn_id,
                summary=result.final_response or "",
                capabilities=capabilities,
            )
        if status in {
            "pending_input",
            "requires_input",
            "input_required",
            "waiting_for_input",
        }:
            question = getattr(result, "pending_question", None)
            return CodexObservation(
                operation_id=operation.operation_id,
                role=operation.role,
                outcome=CodexOutcome.PENDING_INPUT,
                thread_id=thread_id,
                turn_id=turn_id,
                summary="Codex is waiting for business input",
                pending_question=OpenAICodexAdapter._bounded_question(question),
                capabilities=capabilities,
            )
        if status == "failed":
            return CodexObservation(
                operation_id=operation.operation_id,
                role=operation.role,
                outcome=CodexOutcome.FAILED,
                thread_id=thread_id,
                turn_id=turn_id,
                summary="Codex turn failed",
                capabilities=capabilities,
            )
        return CodexObservation(
            operation_id=operation.operation_id,
            role=operation.role,
            outcome=CodexOutcome.FAILED,
            thread_id=thread_id,
            turn_id=turn_id,
            summary="Codex returned an unrecognized turn status",
            failure=CodexFailure.UNKNOWN,
            readback_required=True,
            capabilities=capabilities,
        )

    @staticmethod
    def _classify_exception(error: Exception) -> CodexFailure:
        error_name = type(error).__name__
        if error_name in {
            "InvalidParamsError",
            "InvalidRequestError",
            "MethodNotFoundError",
            "PermissionDeniedError",
            "RejectedError",
        }:
            return CodexFailure.REJECTED
        if isinstance(error, (asyncio.TimeoutError, TimeoutError)) or error_name in {
            "TimeoutError",
            "DeadlineExceededError",
        }:
            return CodexFailure.TIMEOUT
        if isinstance(error, (ConnectionError, EOFError)) or error_name in {
            "TransportClosedError",
            "StreamDisconnectedError",
        }:
            return CodexFailure.STREAM_DISCONNECTED
        return CodexFailure.UNKNOWN

    @staticmethod
    def _failure_summary(failure: CodexFailure) -> str:
        return {
            CodexFailure.REJECTED: "Codex request was rejected",
            CodexFailure.TIMEOUT: "Codex request timed out; readback is required",
            CodexFailure.STREAM_DISCONNECTED: (
                "Codex stream disconnected; readback is required"
            ),
            CodexFailure.UNKNOWN: "Codex outcome is unknown; readback is required",
        }[failure]

    @staticmethod
    def _bounded_question(question: Any) -> str:
        if not isinstance(question, str) or not question.strip():
            return "Codex requires additional business input"
        return question.strip()[:1000]

    @staticmethod
    async def _close(codex: AsyncCodex) -> None:
        close = getattr(codex, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result
