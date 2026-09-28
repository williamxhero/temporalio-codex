import asyncio
import subprocess
from dataclasses import dataclass
from pathlib import Path

from temporalio_codex.delivery_models import (
    DeliveryOperation,
    DeliveryOutcome,
    DeliveryPhase,
    DeliveryReceipt,
    validate_operation,
)


class GitCommandError(RuntimeError):
    pass


@dataclass
class LocalGitAdapter:
    async def execute(self, operation: DeliveryOperation) -> DeliveryReceipt:
        validate_operation(operation)
        if operation.phase is DeliveryPhase.CANDIDATE:
            return await self._prepare_candidate(operation)
        if operation.phase in {
            DeliveryPhase.ACCEPTANCE,
            DeliveryPhase.REVIEW,
            DeliveryPhase.CI,
        }:
            return await self._verify_candidate(operation)
        raise ValueError(f"unsupported local Git phase: {operation.phase.value}")

    async def _prepare_candidate(
        self, operation: DeliveryOperation
    ) -> DeliveryReceipt:
        repository = Path(operation.repository).resolve()
        workspace = Path(operation.workspace).resolve()
        base_sha = await self._git(repository, "rev-parse", "--verify", operation.base_sha or "HEAD^{commit}")

        if workspace.exists():
            current_sha = await self._git(workspace, "rev-parse", "--verify", "HEAD^{commit}")
            if current_sha != base_sha:
                raise GitCommandError("candidate workspace is bound to a different SHA")
        else:
            workspace.parent.mkdir(parents=True, exist_ok=True)
            await self._git(
                repository,
                "worktree",
                "add",
                "--detach",
                str(workspace),
                base_sha,
            )

        await self._assert_clean(workspace)
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.COMPLETED,
            summary="candidate workspace prepared",
            candidate_sha=base_sha,
            acceptance_version=operation.acceptance_version,
            evidence_refs=(f"git:{workspace}:HEAD={base_sha}",),
        )

    async def _verify_candidate(
        self, operation: DeliveryOperation
    ) -> DeliveryReceipt:
        if not operation.workspace:
            raise ValueError("local Git verification requires a workspace")
        workspace = Path(operation.workspace).resolve()
        current_sha = await self._git(workspace, "rev-parse", "--verify", "HEAD^{commit}")
        if current_sha != operation.candidate_sha:
            raise GitCommandError("candidate SHA changed after preparation")
        await self._assert_clean(workspace)
        if operation.phase is DeliveryPhase.ACCEPTANCE:
            if not operation.acceptance_command:
                return DeliveryReceipt(
                    operation_id=operation.operation_id,
                    phase=operation.phase,
                    outcome=DeliveryOutcome.NOT_VERIFIED,
                    summary="acceptance command is not configured",
                    candidate_sha=current_sha,
                    acceptance_version=operation.acceptance_version,
                )
            result = await asyncio.to_thread(
                self._run_command,
                workspace,
                operation.acceptance_command,
            )
            outcome = (
                DeliveryOutcome.COMPLETED
                if result.returncode == 0
                else DeliveryOutcome.FAILED
            )
            summary = (
                "acceptance command passed"
                if result.returncode == 0
                else "acceptance command failed"
            )
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=outcome,
                summary=summary,
                candidate_sha=current_sha,
                acceptance_version=operation.acceptance_version,
                evidence_refs=(f"git:{workspace}:HEAD={current_sha}",),
            )
        if operation.phase is DeliveryPhase.REVIEW:
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=DeliveryOutcome.NOT_VERIFIED,
                summary="independent review adapter is not configured",
                candidate_sha=current_sha,
                acceptance_version=operation.acceptance_version,
            )
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.COMPLETED,
            summary=f"{operation.phase.value} candidate verified",
            candidate_sha=current_sha,
            acceptance_version=operation.acceptance_version,
            evidence_refs=(f"git:{workspace}:HEAD={current_sha}",),
        )

    async def _assert_clean(self, workspace: Path) -> None:
        status = await self._git(workspace, "status", "--porcelain")
        if status:
            raise GitCommandError("candidate workspace is dirty")

    @staticmethod
    async def _git(cwd: Path, *args: str) -> str:
        return await asyncio.to_thread(LocalGitAdapter._run_git, cwd, args)

    @staticmethod
    def _run_git(cwd: Path, args: tuple[str, ...]) -> str:
        result = subprocess.run(
            ["git", "-C", str(cwd), *args],
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode != 0:
            detail = result.stderr.strip().splitlines()
            raise GitCommandError(detail[-1][:500] if detail else "git command failed")
        return result.stdout.strip()

    @staticmethod
    def _run_command(cwd: Path, args: tuple[str, ...]) -> subprocess.CompletedProcess:
        return subprocess.run(
            list(args),
            cwd=cwd,
            capture_output=True,
            check=False,
            text=True,
            timeout=1800,
        )
