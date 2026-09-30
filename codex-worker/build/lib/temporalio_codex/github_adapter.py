import asyncio
import json
import subprocess
from dataclasses import dataclass, field
from typing import Protocol

from temporalio_codex.delivery_models import (
    DeliveryOperation,
    DeliveryOutcome,
    DeliveryPhase,
    DeliveryReceipt,
    validate_operation,
)


@dataclass(frozen=True)
class PullRequestRecord:
    number: int
    identity: str
    head_sha: str
    base_branch: str
    state: str = "open"
    merged: bool = False
    merge_commit_sha: str | None = None


@dataclass(frozen=True)
class CheckRecord:
    name: str
    sha: str
    status: str
    conclusion: str | None = None


class GitHubGateway(Protocol):
    async def find_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord | None: ...
    async def create_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord: ...
    async def read_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord: ...
    async def list_checks(self, operation: DeliveryOperation) -> tuple[CheckRecord, ...]: ...
    async def merge_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord: ...
    async def close_issue(self, issue_number: int) -> None: ...


@dataclass
class GitHubDeliveryAdapter:
    gateway: GitHubGateway
    _receipts: dict[str, DeliveryReceipt] = field(default_factory=dict)

    async def execute(self, operation: DeliveryOperation) -> DeliveryReceipt:
        validate_operation(operation)
        cached = self._receipts.get(operation.operation_id)
        if cached is not None:
            return cached
        if operation.phase is DeliveryPhase.PULL_REQUEST:
            receipt = await self._pull_request(operation)
        elif operation.phase is DeliveryPhase.CI:
            receipt = await self._ci(operation)
        elif operation.phase is DeliveryPhase.MERGE:
            receipt = await self._merge(operation)
        elif operation.phase is DeliveryPhase.CLEANUP:
            receipt = await self._cleanup(operation)
        else:
            raise ValueError(f"unsupported GitHub phase: {operation.phase.value}")
        if receipt.outcome is DeliveryOutcome.COMPLETED:
            self._receipts[operation.operation_id] = receipt
        return receipt

    async def _pull_request(self, operation: DeliveryOperation) -> DeliveryReceipt:
        try:
            existing = await self.gateway.find_pull_request(operation)
        except Exception:
            return self._unknown(
                operation,
                "pull request identity readback is unknown",
            )
        if existing is None:
            try:
                existing = await self.gateway.create_pull_request(operation)
            except Exception:
                try:
                    existing = await self.gateway.find_pull_request(operation)
                except Exception:
                    return self._unknown(
                        operation,
                        "pull request create and identity readback are unknown",
                    )
                if existing is None:
                    return self._unknown(operation, "pull request create outcome is unknown")
        if existing.head_sha != operation.candidate_sha:
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=DeliveryOutcome.FAILED,
                summary="pull request head SHA does not match candidate",
                candidate_sha=existing.head_sha,
                pull_request_number=existing.number,
                external_id=str(existing.number),
                external_state=existing.state,
            )
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.COMPLETED,
            summary="pull request created or adopted",
            candidate_sha=existing.head_sha,
            pull_request_number=existing.number,
            external_id=str(existing.number),
            external_state=existing.state,
        )

    async def _ci(self, operation: DeliveryOperation) -> DeliveryReceipt:
        checks = await self.gateway.list_checks(operation)
        if not checks or any(check.sha != operation.candidate_sha for check in checks):
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=DeliveryOutcome.FAILED,
                summary="CI checks are missing or bound to a different candidate",
                candidate_sha=operation.candidate_sha,
                pull_request_number=operation.pull_request_number,
            )
        if any(check.status != "completed" for check in checks):
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=DeliveryOutcome.WAITING,
                summary="CI checks are not terminal",
                candidate_sha=operation.candidate_sha,
                pull_request_number=operation.pull_request_number,
            )
        if any(check.conclusion != "success" for check in checks):
            outcome = DeliveryOutcome.FAILED
            summary = "CI checks did not pass"
        else:
            outcome = DeliveryOutcome.COMPLETED
            summary = "CI checks passed for candidate"
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=outcome,
            summary=summary,
            candidate_sha=operation.candidate_sha,
            pull_request_number=operation.pull_request_number,
        )

    async def _merge(self, operation: DeliveryOperation) -> DeliveryReceipt:
        try:
            pr = await self.gateway.merge_pull_request(operation)
        except Exception:
            try:
                pr = await self.gateway.read_pull_request(operation)
            except Exception:
                return self._unknown(operation, "merge response is unknown")
        if pr.head_sha != operation.candidate_sha:
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=DeliveryOutcome.FAILED,
                summary="pull request head SHA changed before merge",
                candidate_sha=pr.head_sha,
                pull_request_number=pr.number,
                external_id=str(pr.number),
                external_state=pr.state,
            )
        if pr.merged and pr.merge_commit_sha:
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=DeliveryOutcome.COMPLETED,
                summary="pull request merge verified",
                candidate_sha=operation.candidate_sha,
                pull_request_number=pr.number,
                merged_sha=pr.merge_commit_sha,
                external_id=str(pr.number),
                external_state="merged",
            )
        if pr.state == "queued":
            return DeliveryReceipt(
                operation_id=operation.operation_id,
                phase=operation.phase,
                outcome=DeliveryOutcome.WAITING,
                summary="pull request merge is queued",
                candidate_sha=operation.candidate_sha,
                pull_request_number=pr.number,
                external_id=str(pr.number),
                external_state=pr.state,
            )
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.FAILED,
            summary="pull request is not merged",
            candidate_sha=operation.candidate_sha,
            pull_request_number=pr.number,
            external_id=str(pr.number),
            external_state=pr.state,
        )

    async def _cleanup(self, operation: DeliveryOperation) -> DeliveryReceipt:
        try:
            for issue_number in operation.issue_numbers:
                await self.gateway.close_issue(issue_number)
        except Exception:
            return self._unknown(operation, "cleanup write outcome is unknown")
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.COMPLETED,
            summary="delivery cleanup completed",
            candidate_sha=operation.candidate_sha,
            pull_request_number=operation.pull_request_number,
        )

    @staticmethod
    def _unknown(operation: DeliveryOperation, summary: str) -> DeliveryReceipt:
        return DeliveryReceipt(
            operation_id=operation.operation_id,
            phase=operation.phase,
            outcome=DeliveryOutcome.UNKNOWN,
            summary=summary,
            candidate_sha=operation.candidate_sha,
            pull_request_number=operation.pull_request_number,
            readback_required=True,
        )


@dataclass
class FakeGitHubGateway:
    pull_requests: dict[str, PullRequestRecord] = field(default_factory=dict)
    checks: dict[int, tuple[CheckRecord, ...]] = field(default_factory=dict)
    merge_results: dict[int, PullRequestRecord] = field(default_factory=dict)
    fail_create_once: set[str] = field(default_factory=set)
    closed_issues: list[int] = field(default_factory=list)
    _next_number: int = 1

    async def find_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord | None:
        return self.pull_requests.get(operation.pull_request_identity or "")

    async def create_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord:
        identity = operation.pull_request_identity or ""
        record = PullRequestRecord(
            number=self._next_number,
            identity=identity,
            head_sha=operation.candidate_sha or "",
            base_branch=operation.target_branch,
        )
        self._next_number += 1
        self.pull_requests[identity] = record
        if identity in self.fail_create_once:
            self.fail_create_once.remove(identity)
            raise ConnectionError("simulated lost create response")
        return record

    async def read_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord:
        record = await self.find_pull_request(operation)
        if record is None:
            raise LookupError("pull request not found")
        return record

    async def list_checks(self, operation: DeliveryOperation) -> tuple[CheckRecord, ...]:
        return self.checks.get(operation.pull_request_number or 0, ())

    async def merge_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord:
        result = self.merge_results.get(operation.pull_request_number or 0)
        if result is None:
            return await self.read_pull_request(operation)
        return result

    async def close_issue(self, issue_number: int) -> None:
        self.closed_issues.append(issue_number)


@dataclass
class GhCliGateway:
    repository: str

    async def find_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord | None:
        records = await self._api(
            "pulls",
            fields=("state=all", f"head={operation.candidate_branch}"),
        )
        for record in records:
            if operation.pull_request_identity in (record.get("body") or ""):
                return self._pull_request(record)
        return None

    async def create_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord:
        record = await self._api(
            "pulls",
            method="POST",
            fields=(
                f"title={operation.title or operation.pull_request_identity}",
                f"head={operation.candidate_branch}",
                f"base={operation.target_branch}",
                f"body={operation.body or operation.pull_request_identity}",
            ),
        )
        return self._pull_request(record)

    async def read_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord:
        record = await self._api("pulls", str(operation.pull_request_number))
        return self._pull_request(record)

    async def list_checks(self, operation: DeliveryOperation) -> tuple[CheckRecord, ...]:
        payload = await self._api(
            "commits",
            operation.candidate_sha or "",
            "check-runs",
            fields=("per_page=100",),
            paginate=True,
        )
        pages = payload if isinstance(payload, list) else [payload]
        return tuple(
            CheckRecord(
                name=item.get("name", "unknown"),
                sha=item.get("head_sha", ""),
                status=item.get("status", "unknown"),
                conclusion=item.get("conclusion"),
            )
            for page in pages
            for item in page.get("check_runs", [])
        )

    async def merge_pull_request(self, operation: DeliveryOperation) -> PullRequestRecord:
        await self._api(
            "pulls",
            str(operation.pull_request_number),
            "merge",
            method="PUT",
            fields=("merge_method=merge",),
        )
        return await self.read_pull_request(operation)

    async def close_issue(self, issue_number: int) -> None:
        await self._api("issues", str(issue_number), method="PATCH", fields=("state=closed",))

    async def _api(
        self,
        *path: str,
        method: str = "GET",
        fields: tuple[str, ...] = (),
        paginate: bool = False,
    ):
        args = ["gh", "api", f"repos/{self.repository}/" + "/".join(path), "--method", method]
        if paginate:
            args.extend(["--paginate", "--slurp"])
        for field in fields:
            args.extend(["-f", field])
        return await asyncio.to_thread(self._run, args)

    @staticmethod
    def _run(args: list[str]):
        result = subprocess.run(
            args, capture_output=True, check=False, text=True, encoding="utf-8"
        )
        if result.returncode:
            raise RuntimeError("GitHub API request failed")
        return json.loads(result.stdout)

    @staticmethod
    def _pull_request(record: dict) -> PullRequestRecord:
        return PullRequestRecord(
            number=record["number"],
            identity=record.get("body", ""),
            head_sha=record.get("head", {}).get("sha", ""),
            base_branch=record.get("base", {}).get("ref", ""),
            state="merged" if record.get("merged_at") else record.get("state", "unknown"),
            merged=bool(record.get("merged_at")),
            merge_commit_sha=record.get("merge_commit_sha"),
        )
