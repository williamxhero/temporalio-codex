import asyncio
import json
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol


class SummaryPublicationStatus(StrEnum):
    VERIFIED = "verified"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SummaryPublicationInput:
    repository: str
    umbrella_issue_number: int
    operation_id: str
    summary_text: str
    automatic: bool = False


@dataclass(frozen=True)
class SummaryCommentRecord:
    comment_id: int
    issue_number: int
    operation_id: str
    body: str


@dataclass(frozen=True)
class SummaryPublicationResult:
    status: SummaryPublicationStatus
    comment: SummaryCommentRecord | None = None
    reason: str = ""
    evidence_refs: tuple[str, ...] = ()


class SummaryCommentGateway(Protocol):
    async def find_summary_comments(
        self, input: SummaryPublicationInput
    ) -> tuple[SummaryCommentRecord, ...]: ...

    async def create_summary_comment(
        self, input: SummaryPublicationInput, body: str
    ) -> SummaryCommentRecord: ...

    async def read_summary_comment(
        self, input: SummaryPublicationInput, comment_id: int
    ) -> SummaryCommentRecord: ...


def summary_comment_body(input: SummaryPublicationInput) -> str:
    marker = f"<!-- temporalio-codex:summary:{input.operation_id} -->"
    return f"{marker}\n\n{input.summary_text}"


def _validate_input(input: SummaryPublicationInput) -> str | None:
    if not input.repository.strip():
        return "repository must not be empty"
    if input.umbrella_issue_number <= 0:
        return "umbrella_issue_number must be positive"
    if not input.operation_id.strip():
        return "operation_id must not be empty"
    if not input.summary_text.strip():
        return "summary_text must not be empty"
    return None


async def publish_summary(
    input: SummaryPublicationInput,
    gateway: SummaryCommentGateway,
) -> SummaryPublicationResult:
    validation_error = _validate_input(input)
    if validation_error:
        return SummaryPublicationResult(
            SummaryPublicationStatus.BLOCKED,
            reason=validation_error,
        )

    body = summary_comment_body(input)
    try:
        candidates = await gateway.find_summary_comments(input)
    except Exception:
        return SummaryPublicationResult(
            SummaryPublicationStatus.UNKNOWN,
            reason="summary identity readback is unknown",
        )
    if len(candidates) > 1:
        return SummaryPublicationResult(
            SummaryPublicationStatus.UNKNOWN,
            reason="summary identity is ambiguous",
        )

    comment = candidates[0] if candidates else None
    if comment is None:
        try:
            comment = await gateway.create_summary_comment(input, body)
        except Exception:
            try:
                candidates = await gateway.find_summary_comments(input)
            except Exception:
                return SummaryPublicationResult(
                    SummaryPublicationStatus.UNKNOWN,
                    reason="summary create and identity readback are unknown",
                )
            if len(candidates) != 1:
                return SummaryPublicationResult(
                    SummaryPublicationStatus.UNKNOWN,
                    reason="summary create outcome is unknown",
                )
            comment = candidates[0]

    try:
        readback = await gateway.read_summary_comment(input, comment.comment_id)
    except Exception:
        return SummaryPublicationResult(
            SummaryPublicationStatus.UNKNOWN,
            reason="summary comment readback is unknown",
        )
    if (
        readback.issue_number != input.umbrella_issue_number
        or readback.operation_id != input.operation_id
        or readback.body != body
    ):
        return SummaryPublicationResult(
            SummaryPublicationStatus.UNKNOWN,
            reason="summary comment readback does not match operation or body",
        )
    return SummaryPublicationResult(
        SummaryPublicationStatus.VERIFIED,
        comment=readback,
        evidence_refs=(
            f"issue:{readback.issue_number}",
            f"comment:{readback.comment_id}",
            f"operation:{readback.operation_id}",
        ),
    )


@dataclass
class FakeSummaryCommentGateway:
    comments: dict[str, SummaryCommentRecord] = field(default_factory=dict)
    ambiguous_operations: set[str] = field(default_factory=set)
    fail_create_once: set[str] = field(default_factory=set)
    fail_find: bool = False
    fail_read: bool = False
    calls: list[str] = field(default_factory=list)
    next_comment_id: int = 1

    async def find_summary_comments(
        self, input: SummaryPublicationInput
    ) -> tuple[SummaryCommentRecord, ...]:
        self.calls.append(f"find:{input.operation_id}")
        if self.fail_find:
            raise ConnectionError("simulated summary lookup failure")
        comment = self.comments.get(input.operation_id)
        matches = (comment,) if comment is not None else ()
        if input.operation_id in self.ambiguous_operations:
            matches += (
                SummaryCommentRecord(
                    comment_id=self.next_comment_id + 1,
                    issue_number=input.umbrella_issue_number,
                    operation_id=input.operation_id,
                    body=summary_comment_body(input),
                ),
                SummaryCommentRecord(
                    comment_id=self.next_comment_id + 2,
                    issue_number=input.umbrella_issue_number,
                    operation_id=input.operation_id,
                    body=summary_comment_body(input),
                ),
            )
        return matches

    async def create_summary_comment(
        self, input: SummaryPublicationInput, body: str
    ) -> SummaryCommentRecord:
        self.calls.append(f"create:{input.operation_id}")
        comment = SummaryCommentRecord(
            comment_id=self.next_comment_id,
            issue_number=input.umbrella_issue_number,
            operation_id=input.operation_id,
            body=body,
        )
        self.next_comment_id += 1
        self.comments[input.operation_id] = comment
        if input.operation_id in self.fail_create_once:
            self.fail_create_once.remove(input.operation_id)
            raise ConnectionError("simulated lost summary create response")
        return comment

    async def read_summary_comment(
        self, input: SummaryPublicationInput, comment_id: int
    ) -> SummaryCommentRecord:
        self.calls.append(f"read:{comment_id}")
        if self.fail_read:
            raise ConnectionError("simulated summary readback failure")
        for comment in self.comments.values():
            if comment.comment_id == comment_id:
                return comment
        raise LookupError("summary comment not found")


@dataclass
class GhCliSummaryCommentGateway:
    repository: str

    async def find_summary_comments(
        self, input: SummaryPublicationInput
    ) -> tuple[SummaryCommentRecord, ...]:
        records = await self._api(
            "issues",
            str(input.umbrella_issue_number),
            "comments",
            fields=("per_page=100",),
            paginate=True,
        )
        pages = records if isinstance(records, list) else [records]
        marker = f"<!-- temporalio-codex:summary:{input.operation_id} -->"
        return tuple(
            self._record(item, input, input.operation_id)
            for page in pages
            for item in page
            if marker in (item.get("body") or "")
        )

    async def create_summary_comment(
        self, input: SummaryPublicationInput, body: str
    ) -> SummaryCommentRecord:
        body_path = self._write_body_file(body)
        try:
            await asyncio.to_thread(
                self._run_command,
                [
                    "gh",
                    "issue",
                    "comment",
                    str(input.umbrella_issue_number),
                    "--repo",
                    self.repository,
                    "--body-file",
                    body_path,
                ],
            )
        finally:
            os.unlink(body_path)
        comments = await self.find_summary_comments(input)
        if len(comments) != 1:
            raise RuntimeError("created summary comment was not uniquely readable")
        return comments[0]

    async def read_summary_comment(
        self, input: SummaryPublicationInput, comment_id: int
    ) -> SummaryCommentRecord:
        record = await self._api(
            "issues",
            "comments",
            str(comment_id),
        )
        return self._record(record, input, input.operation_id)

    async def _api(
        self,
        *path: str,
        method: str = "GET",
        fields: tuple[str, ...] = (),
        paginate: bool = False,
    ):
        args = [
            "gh",
            "api",
            f"repos/{self.repository}/" + "/".join(path),
            "--method",
            method,
        ]
        if paginate:
            args.extend(["--paginate", "--slurp"])
        for api_field in fields:
            args.extend(["-f", api_field])
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
    def _run_command(args: list[str]) -> None:
        result = subprocess.run(
            args, capture_output=True, check=False, text=True, encoding="utf-8"
        )
        if result.returncode:
            raise RuntimeError("GitHub comment request failed")

    @staticmethod
    def _write_body_file(body: str) -> str:
        descriptor, path = tempfile.mkstemp(
            prefix="temporalio-codex-summary-", suffix=".txt"
        )
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            stream.write(body)
        return path

    @staticmethod
    def _record(
        record: dict,
        input: SummaryPublicationInput,
        operation_id: str,
    ) -> SummaryCommentRecord:
        return SummaryCommentRecord(
            comment_id=record["id"],
            issue_number=(
                int(record["issue_url"].rstrip("/").rsplit("/", 1)[-1])
                if record.get("issue_url")
                else input.umbrella_issue_number
            ),
            operation_id=operation_id,
            body=record.get("body") or "",
        )
