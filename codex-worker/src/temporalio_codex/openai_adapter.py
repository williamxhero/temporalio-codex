import asyncio
import hashlib
import inspect
import json
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

from temporalio_codex.codex_models import (
    CodexCapabilities,
    CodexFailure,
    CodexObservation,
    CodexOperation,
    CodexOutcome,
    validate_operation,
)
from temporalio_codex.conversation_store import ConversationEvent, ConversationStore

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
    conversation_store: ConversationStore | None = None
    _observations: dict[tuple[str, str, str, str], CodexObservation] = field(
        default_factory=dict
    )
    _owned_turns: dict[str, tuple[Any, Any]] = field(default_factory=dict)
    _fingerprints: dict[tuple[str, str, str, str], str] = field(default_factory=dict)

    async def execute(self, operation: CodexOperation) -> CodexObservation:
        validate_operation(operation)
        cache_key = self._operation_cache_key(operation)
        fingerprint = hashlib.sha256(
            json.dumps(asdict(operation), sort_keys=True).encode()
        ).hexdigest()
        previous_fingerprint = self._fingerprints.get(cache_key)
        if previous_fingerprint is not None and previous_fingerprint != fingerprint:
            raise ValueError("operation identity reused with different input")
        self._fingerprints[cache_key] = fingerprint
        existing = self._observations.get(cache_key)
        if existing is not None:
            return existing
        if self.conversation_store is not None:
            claimed, saved = self.conversation_store.claim_operation(
                cache_key, fingerprint
            )
            if not claimed:
                if saved["fingerprint"] != fingerprint:
                    raise ValueError("operation identity reused with different input")
                if saved["observation_json"]:
                    return _restore_observation(json.loads(saved["observation_json"]))
                return CodexObservation(
                    operation_id=operation.operation_id,
                    role=operation.role,
                    outcome=CodexOutcome.UNKNOWN,
                    thread_id=saved["thread_id"],
                    turn_id=saved["turn_id"],
                    failure=CodexFailure.UNKNOWN,
                    summary="Codex launch outcome is unknown; readback is required",
                    readback_required=True,
                )

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
            if self.conversation_store is not None:
                self.conversation_store.record_operation(
                    cache_key, thread_id=thread_id, turn_id=None
                )
            await self._record_thread_history(operation, thread, thread_id)
            self._emit(
                operation,
                "user_input",
                operation.prompt,
                event_id=f"operation:{operation.operation_id}:input",
            )
            turn_handle = thread.turn(
                operation.prompt,
                approval_mode=self._approval_mode(operation.approval_policy),
                cwd=operation.repository,
                effort=ReasoningEffort(operation.effort),
                model=self._model_for_operation(operation.model),
                sandbox=self._sandbox(operation.allowed_scope),
                output_schema=operation.output_schema,
            )
            if inspect.isawaitable(turn_handle):
                turn_handle = await turn_handle
            turn_id = getattr(turn_handle, "id", None)
            if self.conversation_store is not None:
                self.conversation_store.record_operation(
                    cache_key, thread_id=thread_id, turn_id=turn_id
                )
            self._owned_turns[operation.operation_id] = (codex, turn_handle)
            self._emit(
                operation,
                "turn_started",
                thread_id=thread_id,
                turn_id=turn_id,
                event_id=f"operation:{operation.operation_id}:turn-started:{turn_id}",
            )
            result = await self._run_turn(operation, turn_handle)
            observation = self._observation_from_result(
                operation,
                result,
                capabilities,
                thread_id=thread_id,
            )
            turn_id = observation.turn_id or turn_id
            self._emit(
                operation,
                (
                    "assistant_final"
                    if observation.outcome is CodexOutcome.COMPLETED
                    else "error"
                ),
                observation.summary,
                thread_id=thread_id,
                turn_id=turn_id,
                event_id=f"operation:{operation.operation_id}:assistant-final:{turn_id}",
            )
            self._emit(
                operation,
                (
                    "turn_completed"
                    if observation.outcome is CodexOutcome.COMPLETED
                    else "turn_failed"
                ),
                thread_id=thread_id,
                turn_id=turn_id,
                event_id=f"operation:{operation.operation_id}:turn-completed:{turn_id}",
            )
            if self.conversation_store is not None:
                self.conversation_store.record_operation(
                    cache_key,
                    thread_id=observation.thread_id,
                    turn_id=observation.turn_id,
                    observation=asdict(observation),
                )
        except Exception as error:  # noqa: BLE001 - classify SDK failures uniformly
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
            self._emit(
                operation,
                "error",
                observation.summary,
                thread_id=thread_id,
                turn_id=turn_id,
                event_id=f"operation:{operation.operation_id}:error:{turn_id}",
            )
            if turn_id:
                self._emit(
                    operation,
                    "turn_failed",
                    thread_id=thread_id,
                    turn_id=turn_id,
                    event_id=f"operation:{operation.operation_id}:turn-failed:{turn_id}",
                )
        finally:
            self._owned_turns.pop(operation.operation_id, None)
            if codex is not None:
                await self._close(codex)

        self._observations[cache_key] = observation
        if self.conversation_store is not None:
            self.conversation_store.record_operation(
                cache_key,
                thread_id=observation.thread_id,
                turn_id=observation.turn_id,
                observation=asdict(observation),
            )
        return observation

    async def _run_turn(self, operation: CodexOperation, turn_handle: Any) -> Any:
        stream = getattr(turn_handle, "stream", None)
        if stream is None:
            return await turn_handle.run()

        completed_turn = None
        items: list[Any] = []
        async for event in stream():
            self._record_notification(operation, event)
            payload = getattr(event, "payload", None)
            method = getattr(event, "method", "")
            if method == "turn/completed":
                completed_turn = getattr(payload, "turn", None)
            elif method == "item/completed":
                item = getattr(payload, "item", None)
                if item is not None:
                    items.append(item)

        if completed_turn is None:
            raise RuntimeError("turn completed event not received")
        error = getattr(completed_turn, "error", None)
        status = getattr(completed_turn.status, "value", str(completed_turn.status))
        if status == "failed":
            raise RuntimeError(getattr(error, "message", None) or "Codex turn failed")
        return _StreamTurnResult(
            id=completed_turn.id,
            status=completed_turn.status,
            error=error,
            final_response=_final_response_from_items(items),
        )

    def _record_notification(self, operation: CodexOperation, event: Any) -> None:
        method = getattr(event, "method", "")
        payload = getattr(event, "payload", None)
        thread_id = getattr(payload, "thread_id", None) or operation.thread_id
        turn_id = getattr(payload, "turn_id", None) or getattr(
            getattr(payload, "turn", None), "id", None
        )
        if self.conversation_store is not None and (thread_id or turn_id):
            self.conversation_store.record_operation(
                self._operation_cache_key(operation),
                thread_id=thread_id,
                turn_id=turn_id,
            )
        if method == "turn/plan/updated":
            text = "\n".join(
                f"{getattr(step, 'step', '')}: {getattr(step, 'status', '')}"
                for step in (getattr(payload, "plan", ()) or ())
            )
            self._emit(
                operation,
                "plan_delta",
                text,
                thread_id=thread_id,
                turn_id=turn_id,
                detail={"method": method},
            )
            return
        if method == "item/completed":
            item = getattr(payload, "item", None)
            kind, text = _history_item(item)
            if kind in {"reasoning_delta", "plan_delta", "tool_delta"} and text:
                self._emit(
                    operation,
                    kind,
                    text,
                    thread_id=thread_id,
                    turn_id=turn_id,
                    detail={
                        "method": method,
                        "item_id": getattr(getattr(item, "root", item), "id", None),
                        **_history_item_detail(item),
                    },
                    event_id=getattr(item, "id", None) or getattr(event, "id", None),
                )
            return
        if method == "error":
            error = getattr(payload, "error", None)
            detail = {"method": method}
            for name in ("message", "additional_details"):
                value = getattr(error, name, None)
                if isinstance(value, str):
                    detail[name] = value
            retry = getattr(payload, "will_retry", None)
            if isinstance(retry, bool):
                detail["will_retry"] = retry
            info = getattr(error, "codex_error_info", None)
            info = getattr(info, "root", info)
            error_type = getattr(info, "value", getattr(info, "type", None))
            if isinstance(error_type, str):
                detail["error_type"] = error_type
            self._emit(
                operation,
                "error",
                detail.get("message") or "Codex SDK reported an error",
                thread_id=thread_id,
                turn_id=turn_id,
                detail=detail,
            )
            return
        if method == "item/agentMessage/delta":
            kind = "assistant_delta"
        elif method == "item/plan/delta":
            kind = "plan_delta"
        elif method == "item/reasoning/summaryTextDelta":
            kind = "reasoning_delta"
        elif method in {
            "item/commandExecution/outputDelta",
            "command/exec/outputDelta",
            "process/outputDelta",
        }:
            kind = "tool_delta"
        elif method == "turn/completed":
            kind = "turn_completed"
        else:
            return
        text = getattr(payload, "delta", "") or ""
        self._emit(
            operation,
            kind,
            str(text),
            thread_id=thread_id,
            turn_id=turn_id,
            detail={"method": method, "item_id": getattr(payload, "item_id", None)},
            event_id=getattr(event, "id", None),
        )

    def _emit(
        self,
        operation: CodexOperation,
        kind: str,
        text: str = "",
        *,
        thread_id: str | None = None,
        turn_id: str | None = None,
        detail: dict[str, Any] | None = None,
        event_id: str | None = None,
        event_operation_id: str | None = None,
    ) -> None:
        if self.conversation_store is None:
            return
        self.conversation_store.append(
            ConversationEvent(
                workflow_id=operation.run_id,
                scope_workflow_id=operation.parent_workflow_id or operation.run_id,
                operation_id=event_operation_id or operation.operation_id,
                stage=operation.stage,
                role=operation.role.value,
                thread_id=thread_id or operation.thread_id,
                turn_id=turn_id,
                kind=kind,
                text=text,
                detail=detail,
                event_id=self._execution_event_id(operation, event_id),
                workflow_run_id=operation.workflow_run_id,
                scope_workflow_run_id=(
                    operation.parent_workflow_run_id or operation.workflow_run_id
                ),
                namespace=operation.namespace,
            )
        )

    @staticmethod
    def _operation_cache_key(
        operation: CodexOperation,
    ) -> tuple[str, str, str, str]:
        return (
            operation.namespace,
            operation.run_id,
            operation.workflow_run_id or "",
            operation.operation_id,
        )

    @staticmethod
    def _execution_event_id(
        operation: CodexOperation,
        event_id: str | None,
    ) -> str | None:
        if event_id is None or operation.workflow_run_id is None:
            return event_id
        return f"execution:{operation.run_id}:{operation.workflow_run_id}:{event_id}"

    async def _record_thread_history(
        self,
        operation: CodexOperation,
        thread: Any,
        thread_id: str | None,
    ) -> None:
        if operation.thread_id is None or thread_id is None:
            return
        read = getattr(thread, "read", None)
        if not callable(read):
            return
        try:
            response = read(include_turns=True)
            if inspect.isawaitable(response):
                response = await asyncio.wait_for(response, timeout=10)
            persisted_thread = getattr(response, "thread", response)
            turns = getattr(persisted_thread, "turns", ()) or ()
            for turn_index, turn in enumerate(turns):
                turn_id = str(getattr(turn, "id", None) or f"history-{turn_index}")
                history_operation_id = f"{operation.operation_id}:history:{turn_id}"
                items = getattr(turn, "items", ()) or ()
                for item_index, item in enumerate(items):
                    item_id = str(
                        getattr(getattr(item, "root", item), "id", None)
                        or f"item-{item_index}"
                    )
                    kind, text = _history_item(item)
                    if kind is None or not text:
                        continue
                    self._emit(
                        operation,
                        kind,
                        text,
                        thread_id=thread_id,
                        turn_id=turn_id,
                        detail={
                            "source": "thread_read",
                            "item_id": item_id,
                            **_history_item_detail(item),
                        },
                        event_operation_id=history_operation_id,
                        event_id=(
                            f"history:{operation.run_id}:{operation.workflow_run_id}:"
                            f"{thread_id}:{turn_id}:{item_id}"
                        ),
                    )
                status = getattr(turn, "status", "completed")
                status = getattr(status, "value", status)
                self._emit(
                    operation,
                    (
                        "turn_completed"
                        if status == "completed"
                        else (
                            "turn_failed"
                            if status in {"failed", "interrupted"}
                            else "turn_started"
                        )
                    ),
                    thread_id=thread_id,
                    turn_id=turn_id,
                    detail={"source": "thread_read"},
                    event_operation_id=history_operation_id,
                    event_id=(
                        f"history:{operation.run_id}:{operation.workflow_run_id}:"
                        f"{thread_id}:{turn_id}:completed"
                    ),
                )
        except Exception:  # noqa: BLE001 - history readback must not fail the turn
            self._emit(
                operation,
                "history_error",
                "Codex thread history readback failed",
                thread_id=thread_id,
                detail={"source": "thread_read"},
                event_id=(
                    f"history:{operation.run_id}:{operation.workflow_run_id}:"
                    f"{thread_id}:error"
                ),
            )

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
                model=self._model_for_operation(operation.model),
                sandbox=self._sandbox(operation.allowed_scope),
            )
        return await codex.thread_start(
            approval_mode=self._approval_mode(operation.approval_policy),
            cwd=operation.repository,
            model=self._model_for_operation(operation.model),
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
    def _model_for_operation(model: str) -> str:
        if model != "gpt-5-codex":
            return model
        return (
            os.environ.get("TEMPORALIO_CODEX_DEFAULT_MODEL", model).strip()
            or model
        )

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
            outcome=CodexOutcome.UNKNOWN,
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


@dataclass(frozen=True)
class _StreamTurnResult:
    id: str
    status: Any
    error: Any
    final_response: str | None


def _final_response_from_items(items: list[Any]) -> str | None:
    for item in reversed(items):
        candidate = getattr(item, "root", item)
        if getattr(candidate, "type", None) != "agentMessage":
            continue
        text = getattr(candidate, "text", None)
        if isinstance(text, str) and text:
            return text
    return None


def _history_item(item: Any) -> tuple[str | None, str]:
    value = getattr(item, "root", item)
    item_type = getattr(value, "type", None)
    if item_type == "userMessage":
        return "user_input", _history_user_text(getattr(value, "content", ()))
    if item_type == "agentMessage":
        return "assistant_delta", str(getattr(value, "text", "") or "")
    if item_type == "plan":
        return "plan_delta", str(getattr(value, "text", "") or "")
    if item_type == "reasoning":
        parts = getattr(value, "summary", None) or ()
        return "reasoning_delta", "\n".join(str(part) for part in parts if part)
    if item_type == "commandExecution":
        command = str(getattr(value, "command", "") or "")
        output = str(getattr(value, "aggregated_output", "") or "")
        return "tool_delta", "\n".join(part for part in (command, output) if part)
    if item_type == "fileChange":
        paths = [
            str(getattr(change, "path", "") or "")
            for change in (getattr(value, "changes", ()) or ())
        ]
        return "tool_delta", "Files changed: " + ", ".join(
            path for path in paths if path
        )
    if item_type == "mcpToolCall":
        server = str(getattr(value, "server", "") or "")
        tool = str(getattr(value, "tool", "") or "")
        return "tool_delta", ".".join(part for part in (server, tool) if part)
    return None, ""


def _history_item_detail(item: Any) -> dict[str, Any]:
    value = getattr(item, "root", item)
    item_type = getattr(value, "type", None)
    detail: dict[str, Any] = {}
    if item_type:
        detail["item_type"] = str(item_type)
    for field_name in (
        "command",
        "aggregated_output",
        "path",
        "file",
        "file_path",
        "exit_code",
        "status",
        "server",
        "tool",
    ):
        field_value = getattr(value, field_name, None)
        scalar = getattr(field_value, "value", field_value)
        if scalar is not None and isinstance(scalar, (str, int, float, bool)):
            detail[field_name] = scalar
    if item_type == "fileChange":
        detail["paths"] = [
            str(getattr(change, "path", "") or "")
            for change in (getattr(value, "changes", ()) or ())
        ]
        detail["changes"] = []
        for change in getattr(value, "changes", ()) or ():
            change_detail = {}
            for field_name in ("path", "diff", "kind"):
                field_value = getattr(change, field_name, None)
                if field_value is None:
                    continue
                model_dump = getattr(field_value, "model_dump", None)
                change_detail[field_name] = (
                    model_dump(mode="json", by_alias=True)
                    if callable(model_dump)
                    else getattr(
                        field_value, "value", getattr(field_value, "type", field_value)
                    )
                )
            detail["changes"].append(change_detail)
    if item_type == "mcpToolCall":
        for field_name in ("arguments", "result", "error"):
            field_value = getattr(value, field_name, None)
            if field_value is not None:
                model_dump = getattr(field_value, "model_dump", None)
                detail[field_name] = (
                    model_dump(mode="json", by_alias=True)
                    if callable(model_dump)
                    else field_value
                )
    return detail


def _history_user_text(content: Any) -> str:
    parts: list[str] = []
    for item in content or ():
        value = getattr(item, "root", item)
        text = getattr(value, "text", None)
        if text:
            parts.append(str(text))
            continue
        item_type = getattr(value, "type", None)
        if item_type:
            parts.append(f"[{item_type}]")
    return "\n".join(parts)


def _restore_observation(value: dict[str, Any]) -> CodexObservation:
    from temporalio_codex.codex_models import CodexRole

    value["role"] = CodexRole(value["role"])
    value["outcome"] = CodexOutcome(value["outcome"])
    if value.get("failure"):
        value["failure"] = CodexFailure(value["failure"])
    if value.get("capabilities"):
        value["capabilities"] = CodexCapabilities(**value["capabilities"])
    value["evidence_refs"] = tuple(value.get("evidence_refs", ()))
    return CodexObservation(**value)
