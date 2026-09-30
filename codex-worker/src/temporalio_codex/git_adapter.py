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
        if operation.phase is DeliveryPhase.PUSH:
            return await self._verify_remote(operation)
        if operation.phase is DeliveryPhase.PUBLISH_CANDIDATE:
            return await self._publish_candidate(operation)
        raise ValueError(f"unsupported local Git phase: {operation.phase.value}")

    async def _prepare_candidate(self, operation: DeliveryOperation) -> DeliveryReceipt:
        repository = Path(operation.repository).resolve()
        workspace = Path(operation.workspace).resolve()
        base_sha = await self._git(
            repository, "rev-parse", "--verify", operation.base_sha or "HEAD^{commit}"
        )
        candidate_sha = operation.candidate_sha or base_sha

        if workspace.exists():
            current_sha = await self._git(
                workspace, "rev-parse", "--verify", "HEAD^{commit}"
            )
            if current_sha != candidate_sha:
                raise GitCommandError("candidate workspace is bound to a different SHA")
        else:
            workspace.parent.mkdir(parents=True, exist_ok=True)
            await self._git(
                repository,
                "worktree",
                "add",
                "--detach",
                str(workspace),
                candidate_sha,
            )

        repository_common = await self._git(
            repository, "rev-parse", "--path-format=absolute", "--git-common-dir"
        )
        workspace_common = await self._git(
            workspace, "rev-parse", "--path-format=absolute", "--git-common-dir"
        )
        if Path(repository_common).resolve() != Path(workspace_common).resolve():
            raise GitCommandError(
                "candidate workspace belongs to a different repository"
            )
        await self._git(
            workspace, "merge-base", "--is-ancestor", base_sha, candidate_sha
        )
        await self._assert_clean(workspace)
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.COMPLETED,
            summary="candidate workspace prepared",
            candidate_sha=candidate_sha,
            acceptance_version=operation.acceptance_version,
            evidence_refs=(f"git:{workspace}:HEAD={candidate_sha}",),
        )

    async def _verify_candidate(self, operation: DeliveryOperation) -> DeliveryReceipt:
        if not operation.workspace:
            raise ValueError("local Git verification requires a workspace")
        workspace = Path(operation.workspace).resolve()
        current_sha = await self._git(
            workspace, "rev-parse", "--verify", "HEAD^{commit}"
        )
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
            proof = operation.review_evidence
            verified = bool(
                proof
                and proof.verdict == "approved"
                and proof.candidate_sha == current_sha
                and proof.operation_id
                and proof.thread_id
                and proof.turn_id
            )
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=DeliveryOutcome.COMPLETED
                if verified
                else DeliveryOutcome.NOT_VERIFIED,
                summary="SDK review approved the candidate"
                if verified
                else "candidate has no matching approved SDK review evidence",
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

    async def _publish_candidate(self, operation: DeliveryOperation) -> DeliveryReceipt:
        if (
            not operation.workspace
            or not operation.candidate_branch
            or not operation.candidate_sha
        ):
            raise ValueError("candidate publication requires workspace, branch and SHA")
        workspace = Path(operation.workspace).resolve()
        await self._verify_candidate(operation)
        await self._git(
            workspace, "check-ref-format", f"refs/heads/{operation.candidate_branch}"
        )
        remote_ref = f"refs/heads/{operation.candidate_branch}"
        remote = await self._git(workspace, "ls-remote", "origin", remote_ref)
        existing_sha = remote.split()[0] if remote.split() else ""
        if existing_sha and existing_sha != operation.candidate_sha:
            raise GitCommandError("candidate origin branch changed unexpectedly")
        if not existing_sha:
            try:
                await self._git(
                    workspace,
                    "push",
                    f"--force-with-lease={remote_ref}:",
                    "origin",
                    f"{operation.candidate_sha}:{remote_ref}",
                )
            except GitCommandError:
                pass
        remote = await self._git(workspace, "ls-remote", "origin", remote_ref)
        remote_sha = remote.split()[0] if remote.split() else ""
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.COMPLETED
            if remote_sha == operation.candidate_sha
            else DeliveryOutcome.UNKNOWN,
            summary="candidate origin branch readback verified"
            if remote_sha == operation.candidate_sha
            else "candidate origin publication not verified",
            candidate_sha=operation.candidate_sha,
            remote_sha=remote_sha or None,
            readback_required=remote_sha != operation.candidate_sha,
        )

    async def _verify_remote(self, operation: DeliveryOperation) -> DeliveryReceipt:
        repository = Path(operation.repository).resolve()
        remote_ref = f"refs/heads/{operation.target_branch}"
        push_summary = "origin push completed"
        try:
            await self._git(
                repository,
                "push",
                "origin",
                f"{operation.merge_commit_sha}:{remote_ref}",
            )
        except GitCommandError:
            push_summary = "origin push response was lost; readback attempted"
        try:
            remote = await self._git(repository, "ls-remote", "origin", remote_ref)
        except GitCommandError:
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=DeliveryOutcome.UNKNOWN,
                summary="origin push and readback are unknown",
                candidate_sha=operation.candidate_sha,
                merged_sha=operation.merge_commit_sha,
                pull_request_number=operation.pull_request_number,
                readback_required=True,
            )
        remote_sha = remote.split()[0] if remote.split() else ""
        contains_merge = remote_sha == operation.merge_commit_sha
        outcome = (
            DeliveryOutcome.COMPLETED if contains_merge else DeliveryOutcome.FAILED
        )
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=outcome,
            summary=(
                f"{push_summary}; origin remote contains verified merge"
                if contains_merge
                else "origin remote does not contain expected merge"
            ),
            candidate_sha=operation.candidate_sha,
            merged_sha=operation.merge_commit_sha,
            remote_sha=remote_sha or None,
            remote_contains_merge=contains_merge,
            pull_request_number=operation.pull_request_number,
            evidence_refs=(
                f"origin:{operation.target_branch}={remote_sha}" if remote_sha else "",
            ),
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
