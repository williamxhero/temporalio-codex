import math
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporalio_codex.activities import delivery_git_stage, delivery_github_stage
    from temporalio_codex.delivery_models import (
        DeliveryInput,
        DeliveryOperation,
        DeliveryOutcome,
        DeliveryPhase,
        DeliveryReceipt,
        DeliveryResult,
        DeliverySnapshot,
        DeliveryStatus,
    )
    from temporalio_codex.execution_status import report_progress


@workflow.defn
class DeliveryWorkflow:
    def __init__(self) -> None:
        self._status = DeliveryStatus.ACTIVE
        self._phase: DeliveryPhase | None = None
        self._receipts: list[DeliveryReceipt] = []
        self._readback: DeliveryReceipt | None = None
        self._cleanup_retry_requested = False
        self._automatic = False
        self._readback_max_attempts = 60
        self._readback_backoff_seconds = 5.0
        self._acceptance_timeout_seconds = 1800.0

    @workflow.query(name="get_delivery_status")
    def get_status(self) -> DeliverySnapshot:
        return DeliverySnapshot(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            phase=self._phase,
            receipts=tuple(self._receipts),
        )

    @workflow.run
    async def run(self, input: DeliveryInput) -> DeliveryResult:
        self._automatic = input.automatic
        self._readback_max_attempts = input.readback_max_attempts
        self._readback_backoff_seconds = input.readback_backoff_seconds
        self._acceptance_timeout_seconds = input.acceptance_timeout_seconds
        if (self._readback_max_attempts < 1 or self._readback_backoff_seconds < 0
                or not math.isfinite(self._acceptance_timeout_seconds)
                or self._acceptance_timeout_seconds <= 0):
            self._status = DeliveryStatus.FAILED
            return DeliveryResult(
                workflow_id=workflow.info().workflow_id,
                status=self._status,
                outcome=DeliveryOutcome.FAILED,
                summary="Invalid bounded delivery readback policy",
            )
        if input.automatic and (not input.candidate or not input.review_evidence):
            return DeliveryResult(
                workflow_id=workflow.info().workflow_id,
                status=DeliveryStatus.FAILED,
                outcome=DeliveryOutcome.NOT_VERIFIED,
                summary="Automatic delivery requires frozen implementation and review evidence",
            )
        if input.candidate and (
            input.candidate.repository != input.repository
            or input.candidate.workspace != input.workspace
        ):
            return DeliveryResult(
                workflow_id=workflow.info().workflow_id,
                status=DeliveryStatus.FAILED,
                outcome=DeliveryOutcome.FAILED,
                summary="Implementation workspace identity does not match delivery",
            )
        candidate = await self._git(
            DeliveryOperation(
                operation_id=f"{workflow.info().workflow_id}:candidate",
                run_id=workflow.info().workflow_id,
                phase=DeliveryPhase.CANDIDATE,
                repository=input.repository,
                workspace=input.workspace,
                base_sha=input.base_sha,
                candidate_sha=input.candidate.candidate_sha
                if input.candidate
                else None,
                target_branch=input.target_branch,
            )
        )
        if not await self._accept(candidate):
            return self._failed()
        candidate_sha = candidate.candidate_sha
        assert candidate_sha is not None

        for phase in (DeliveryPhase.ACCEPTANCE, DeliveryPhase.REVIEW):
            receipt = await self._git(
                DeliveryOperation(
                    operation_id=f"{workflow.info().workflow_id}:{phase.value}",
                    run_id=workflow.info().workflow_id,
                    phase=phase,
                    repository=input.repository,
                    workspace=input.workspace,
                    candidate_sha=candidate_sha,
                    acceptance_version=input.acceptance_version,
                    acceptance_command=input.acceptance_command,
                    review_evidence=input.review_evidence,
                )
            )
            if not await self._accept(receipt):
                return self._failed()

        publication = await self._git(
            DeliveryOperation(
                operation_id=f"{workflow.info().workflow_id}:publish-candidate",
                run_id=workflow.info().workflow_id,
                phase=DeliveryPhase.PUBLISH_CANDIDATE,
                repository=input.repository,
                workspace=input.workspace,
                candidate_sha=candidate_sha,
                candidate_branch=input.candidate_branch,
            )
        )
        if not await self._accept(publication):
            return self._failed()

        pull_request = await self._github(
            DeliveryOperation(
                operation_id=f"{workflow.info().workflow_id}:pull-request",
                run_id=workflow.info().workflow_id,
                phase=DeliveryPhase.PULL_REQUEST,
                repository=input.repository,
                target_branch=input.target_branch,
                candidate_sha=candidate_sha,
                candidate_branch=input.candidate_branch,
                pull_request_identity=input.pull_request_identity,
                title=input.title,
                body=input.body,
            )
        )
        if not await self._accept(pull_request):
            return self._failed()
        assert pull_request.pull_request_number is not None

        ci = await self._github(
            DeliveryOperation(
                operation_id=f"{workflow.info().workflow_id}:ci",
                run_id=workflow.info().workflow_id,
                phase=DeliveryPhase.CI,
                repository=input.repository,
                target_branch=input.target_branch,
                candidate_sha=candidate_sha,
                pull_request_number=pull_request.pull_request_number,
            )
        )
        if not await self._accept(ci):
            return self._failed()

        merge = await self._github(
            DeliveryOperation(
                operation_id=f"{workflow.info().workflow_id}:merge",
                run_id=workflow.info().workflow_id,
                phase=DeliveryPhase.MERGE,
                repository=input.repository,
                target_branch=input.target_branch,
                candidate_sha=candidate_sha,
                pull_request_number=pull_request.pull_request_number,
            )
        )
        if not await self._accept(merge):
            return self._failed()
        if merge.merged_sha is None:
            self._status = DeliveryStatus.WAITING_FOR_READBACK
            return self._failed()

        push = await self._git(
            DeliveryOperation(
                operation_id=f"{workflow.info().workflow_id}:push",
                run_id=workflow.info().workflow_id,
                phase=DeliveryPhase.PUSH,
                repository=input.repository,
                workspace=input.workspace,
                target_branch=input.target_branch,
                candidate_sha=candidate_sha,
                merge_commit_sha=merge.merged_sha,
                pull_request_number=pull_request.pull_request_number,
            )
        )
        if not await self._accept(push):
            return self._failed()

        cleanup_operation = DeliveryOperation(
            operation_id=f"{workflow.info().workflow_id}:cleanup",
            run_id=workflow.info().workflow_id,
            phase=DeliveryPhase.CLEANUP,
            repository=input.repository,
            candidate_sha=candidate_sha,
            pull_request_number=pull_request.pull_request_number,
            issue_numbers=input.issue_numbers,
        )
        if not await self._finish_cleanup(cleanup_operation):
            return self._failed()
        self._status = DeliveryStatus.COMPLETED
        self._phase = None
        return DeliveryResult(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            outcome=DeliveryOutcome.COMPLETED,
            summary="delivery completed and cleanup verified",
            receipts=tuple(self._receipts),
            candidate=input.candidate,
            review_evidence=input.review_evidence,
        )

    async def _git(self, operation: DeliveryOperation) -> DeliveryReceipt:
        self._phase = operation.phase
        timeout_seconds = (
            self._acceptance_timeout_seconds
            if operation.phase is DeliveryPhase.ACCEPTANCE else 30
        )
        await report_progress(phase="delivery", next_action=f"execute {operation.phase.value}",
                              timeout_seconds=timeout_seconds,
                              deadline=(workflow.now() + timedelta(seconds=timeout_seconds)).isoformat())
        return await workflow.execute_activity(
            delivery_git_stage,
            operation,
            start_to_close_timeout=timedelta(seconds=timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

    async def _github(self, operation: DeliveryOperation) -> DeliveryReceipt:
        self._phase = operation.phase
        for attempt in range(self._readback_max_attempts):
            await report_progress(phase="delivery", next_action=f"verify {operation.phase.value}",
                                  retry_count=attempt, timeout_seconds=30,
                                  deadline=(workflow.now() + timedelta(seconds=30)).isoformat())
            receipt = await workflow.execute_activity(
                delivery_github_stage,
                operation,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            if not self._automatic or receipt.outcome is not DeliveryOutcome.WAITING:
                return receipt
            if attempt < self._readback_max_attempts - 1:
                await report_progress(
                    phase="delivery", status="retrying", retry_count=attempt + 1,
                    pending_reason=receipt.summary,
                    next_action=f"read back {operation.phase.value}",
                    deadline=(workflow.now() + timedelta(
                        seconds=self._readback_backoff_seconds)).isoformat(),
                    last_error=receipt.summary,
                )
                await workflow.sleep(timedelta(seconds=self._readback_backoff_seconds))
        return receipt

    async def _finish_cleanup(self, operation: DeliveryOperation) -> bool:
        while True:
            cleanup = await self._github(operation)
            self._receipts.append(cleanup)
            if cleanup.outcome is DeliveryOutcome.COMPLETED:
                return True
            if self._automatic:
                self._status = DeliveryStatus.FAILED
                return False
            self._status = DeliveryStatus.CLEANUP_PENDING
            await workflow.wait_condition(
                lambda: (
                    self._cleanup_retry_requested
                    or self._readback is not None
                    or self._status is DeliveryStatus.FAILED
                )
            )
            if self._status is DeliveryStatus.FAILED:
                return False
            readback = self._readback
            self._readback = None
            if readback is not None:
                if not self._matches_readback(cleanup, readback):
                    self._status = DeliveryStatus.FAILED
                    return False
                self._receipts[-1] = readback
                return True
            self._cleanup_retry_requested = False
            self._status = DeliveryStatus.ACTIVE

    @staticmethod
    def _matches_readback(
        receipt: DeliveryReceipt,
        readback: DeliveryReceipt,
    ) -> bool:
        return (
            readback.operation_id == receipt.operation_id
            and readback.phase is receipt.phase
            and readback.outcome is DeliveryOutcome.COMPLETED
            and (
                receipt.candidate_sha is None
                or readback.candidate_sha == receipt.candidate_sha
            )
            and (
                receipt.pull_request_number is None
                or readback.pull_request_number == receipt.pull_request_number
            )
            and (
                receipt.phase is not DeliveryPhase.PUSH
                or readback.remote_contains_merge is True
            )
        )

    async def _accept(self, receipt: DeliveryReceipt) -> bool:
        self._receipts.append(receipt)
        if receipt.outcome in (DeliveryOutcome.COMPLETED,):
            return True
        if self._automatic:
            self._status = DeliveryStatus.FAILED
            return False
        if receipt.outcome is DeliveryOutcome.WAITING:
            self._status = DeliveryStatus.WAITING_FOR_READBACK
        elif (
            receipt.outcome
            in (
                DeliveryOutcome.UNKNOWN,
                DeliveryOutcome.NOT_VERIFIED,
            )
            or receipt.readback_required
        ):
            self._status = DeliveryStatus.WAITING_FOR_READBACK
        else:
            self._status = DeliveryStatus.FAILED
            return False
        await workflow.wait_condition(
            lambda: self._readback is not None or self._status is DeliveryStatus.FAILED
        )
        readback = self._readback
        self._readback = None
        if readback is None:
            return False
        if (
            readback.operation_id != receipt.operation_id
            or readback.phase is not receipt.phase
            or readback.outcome is not DeliveryOutcome.COMPLETED
            or (
                receipt.candidate_sha is not None
                and readback.candidate_sha != receipt.candidate_sha
            )
            or (
                receipt.pull_request_number is not None
                and readback.pull_request_number != receipt.pull_request_number
            )
            or (
                receipt.phase is DeliveryPhase.PUSH
                and readback.remote_contains_merge is not True
            )
        ):
            self._status = DeliveryStatus.FAILED
            return False
        self._receipts[-1] = readback
        self._status = DeliveryStatus.ACTIVE
        return True

    def _failed(self) -> DeliveryResult:
        self._status = DeliveryStatus.FAILED
        receipt = self._receipts[-1] if self._receipts else None
        return DeliveryResult(
            workflow_id=workflow.info().workflow_id,
            status=self._status,
            outcome=receipt.outcome if receipt else DeliveryOutcome.FAILED,
            summary=receipt.summary if receipt else "delivery failed",
            receipts=tuple(self._receipts),
        )

    @workflow.update(name="resolve_delivery_readback")
    async def resolve_delivery_readback(self, receipt: DeliveryReceipt) -> bool:
        if self._status not in (
            DeliveryStatus.WAITING_FOR_READBACK,
            DeliveryStatus.CLEANUP_PENDING,
        ):
            return False
        self._readback = receipt
        return True

    @workflow.update(name="retry_cleanup")
    async def retry_cleanup(self) -> bool:
        if self._status is not DeliveryStatus.CLEANUP_PENDING:
            return False
        self._cleanup_retry_requested = True
        return True
