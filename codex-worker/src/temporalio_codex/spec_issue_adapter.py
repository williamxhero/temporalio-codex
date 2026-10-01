import asyncio
import json
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol


class SpecPublicationStatus(StrEnum):
    VERIFIED = "verified"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SpecDraft:
    key: str
    title: str
    scope: str
    acceptance_criteria: tuple[str, ...]
    testing_decisions: tuple[str, ...]
    dependencies: tuple[str, ...] = ()
    provenance: tuple[str, ...] = ()
    number: int | None = None


@dataclass(frozen=True)
class SpecIssueRecord:
    number: int
    issue_id: int
    title: str
    operation_id: str
    parent_issue_number: int
    state: str = "open"


@dataclass(frozen=True)
class SpecPublicationInput:
    repository: str
    umbrella_issue_number: int
    source_identity: str
    operation_id: str
    drafts: tuple[SpecDraft, ...]


@dataclass(frozen=True)
class SpecPublicationResult:
    status: SpecPublicationStatus
    issues: tuple[SpecIssueRecord, ...] = ()
    reason: str = ""


class SpecIssueGateway(Protocol):
    async def find_by_operation(self, operation_id: str) -> SpecIssueRecord | None: ...
    async def create_issue(
        self, input: SpecPublicationInput, draft: SpecDraft, body: str
    ) -> SpecIssueRecord: ...
    async def add_parent(self, parent_issue_number: int, issue_id: int) -> None: ...
    async def read_issue(self, issue_number: int) -> SpecIssueRecord: ...


def validate_spec_graph(drafts: tuple[SpecDraft, ...]) -> tuple[str, ...]:
    errors: list[str] = []
    if not drafts:
        return ("at least one SPEC draft is required",)
    keys = [draft.key for draft in drafts]
    if any(not key.strip() for key in keys):
        errors.append("SPEC keys must not be empty")
    if len(set(keys)) != len(keys):
        errors.append("SPEC keys must be unique")
    issue_numbers = [draft.number for draft in drafts if draft.number is not None]
    if any(number < 1 for number in issue_numbers):
        errors.append("SPEC issue numbers must be positive")
    if len(set(issue_numbers)) != len(issue_numbers):
        errors.append("SPEC issue numbers must be unique")
    known = set(keys)
    for draft in drafts:
        missing = sorted(set(draft.dependencies) - known)
        if missing:
            errors.append(f"SPEC {draft.key} has unresolved dependencies: {', '.join(missing)}")
    if errors:
        return tuple(errors)

    graph = {draft.key: set(draft.dependencies) for draft in drafts}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str) -> None:
        if key in visiting:
            errors.append(f"SPEC dependency cycle includes {key}")
            return
        if key in visited:
            return
        visiting.add(key)
        for dependency in graph[key]:
            visit(dependency)
        visiting.remove(key)
        visited.add(key)

    for key in graph:
        visit(key)
    return tuple(dict.fromkeys(errors))


def topological_specs(drafts: tuple[SpecDraft, ...]) -> tuple[SpecDraft, ...]:
    errors = validate_spec_graph(drafts)
    if errors:
        raise ValueError("; ".join(errors))
    by_key = {draft.key: draft for draft in drafts}
    remaining = set(by_key)
    ordered: list[SpecDraft] = []
    while remaining:
        ready = [
            by_key[key]
            for key in sorted(remaining)
            if not (set(by_key[key].dependencies) & remaining)
        ]
        ordered.extend(ready)
        remaining.difference_update(draft.key for draft in ready)
    return tuple(ordered)


def spec_issue_body(input: SpecPublicationInput, draft: SpecDraft) -> str:
    dependencies = ", ".join(draft.dependencies) or "None"
    provenance = ", ".join(draft.provenance) or input.source_identity
    acceptance = "\n".join(f"- [ ] {item}" for item in draft.acceptance_criteria)
    testing = "\n".join(f"- {item}" for item in draft.testing_decisions)
    return (
        "# SPEC " + draft.key + ": " + draft.title + "\n\n"
        "## Parent\n\n"
        f"Umbrella issue: #{input.umbrella_issue_number}\n\n"
        "## Scope\n\n"
        f"{draft.scope}\n\n"
        "## Acceptance criteria\n\n"
        f"{acceptance}\n\n"
        "## Testing decisions\n\n"
        f"{testing}\n\n"
        "## Provenance\n\n"
        f"Source identity: {input.source_identity}\n"
        f"Planning provenance: {provenance}\n\n"
        "## Dependencies\n\n"
        f"{dependencies}\n"
    )


async def publish_specs(
    input: SpecPublicationInput,
    gateway: SpecIssueGateway,
) -> SpecPublicationResult:
    errors = validate_spec_graph(input.drafts)
    if errors:
        return SpecPublicationResult(SpecPublicationStatus.BLOCKED, reason="; ".join(errors))
    issues: list[SpecIssueRecord] = []
    try:
        for draft in topological_specs(input.drafts):
            operation_id = f"{input.operation_id}:{draft.key}"
            existing = (
                await gateway.read_issue(draft.number)
                if draft.number is not None
                else await gateway.find_by_operation(operation_id)
            )
            if existing is None:
                existing = await gateway.create_issue(
                    input,
                    draft,
                    spec_issue_body(input, draft),
                )
            current = await gateway.read_issue(existing.number)
            if current.parent_issue_number != input.umbrella_issue_number:
                await gateway.add_parent(input.umbrella_issue_number, existing.issue_id)
            readback = await gateway.read_issue(existing.number)
            if (
                (draft.number is None and readback.operation_id != operation_id)
                or readback.number != (draft.number or existing.number)
                or readback.parent_issue_number != input.umbrella_issue_number
            ):
                return SpecPublicationResult(
                    SpecPublicationStatus.UNKNOWN,
                    tuple(issues),
                    f"SPEC {draft.key} readback identity or parent mismatch",
                )
            issues.append(
                SpecIssueRecord(
                    number=readback.number,
                    issue_id=readback.issue_id,
                    title=readback.title,
                    operation_id=operation_id,
                    parent_issue_number=readback.parent_issue_number,
                    state=readback.state,
                )
            )
    except Exception as error:
        return SpecPublicationResult(
            SpecPublicationStatus.UNKNOWN,
            tuple(issues),
            f"SPEC publication requires readback: {type(error).__name__}",
        )
    return SpecPublicationResult(SpecPublicationStatus.VERIFIED, tuple(issues))


@dataclass
class FakeSpecIssueGateway:
    issues: dict[str, SpecIssueRecord] = field(default_factory=dict)
    parent_links: dict[int, int] = field(default_factory=dict)
    calls: list[str] = field(default_factory=list)
    next_number: int = 100
    fail_create: bool = False

    async def find_by_operation(self, operation_id: str) -> SpecIssueRecord | None:
        return self.issues.get(operation_id)

    async def create_issue(
        self, input: SpecPublicationInput, draft: SpecDraft, body: str
    ) -> SpecIssueRecord:
        if self.fail_create:
            raise ConnectionError("issue create response lost")
        operation_id = f"{input.operation_id}:{draft.key}"
        record = SpecIssueRecord(
            number=self.next_number,
            issue_id=self.next_number + 1_000_000_000,
            title=f"[SPEC {draft.key}] {draft.title}",
            operation_id=operation_id,
            parent_issue_number=0,
        )
        self.next_number += 1
        self.issues[operation_id] = record
        return record

    async def add_parent(self, parent_issue_number: int, issue_id: int) -> None:
        self.parent_links[issue_id] = parent_issue_number

    async def read_issue(self, issue_number: int) -> SpecIssueRecord:
        for issue in self.issues.values():
            if issue.number == issue_number:
                parent = self.parent_links.get(issue.issue_id)
                return SpecIssueRecord(
                    **{**issue.__dict__, "parent_issue_number": parent or issue.parent_issue_number}
                )
        raise LookupError("SPEC issue not found")


@dataclass
class GhCliSpecIssueGateway:
    repository: str

    async def find_by_operation(self, operation_id: str) -> SpecIssueRecord | None:
        pages = await self._api("issues?state=all&per_page=100", paginate=True)
        marker = f"Operation identity: {operation_id}"
        for records in pages:
            for record in records:
                if marker in (record.get("body") or "").splitlines():
                    return self._record(record, operation_id)
        return None

    async def create_issue(
        self, input: SpecPublicationInput, draft: SpecDraft, body: str
    ) -> SpecIssueRecord:
        operation_id = f"{input.operation_id}:{draft.key}"
        body_with_operation = body + f"\nOperation identity: {operation_id}\n"
        body_path = self._write_body_file(body_with_operation)
        try:
            url = await asyncio.to_thread(
                self._run_text,
                [
                    "gh",
                    "issue",
                    "create",
                    "--repo",
                    self.repository,
                    "--title",
                    f"[SPEC {draft.key}] {draft.title}",
                    "--body-file",
                    body_path,
                ],
            )
        finally:
            os.unlink(body_path)
        return await self.read_issue(int(url.rsplit("/", 1)[-1]))

    async def add_parent(self, parent_issue_number: int, issue_id: int) -> None:
        await self._api(
            f"issues/{parent_issue_number}/sub_issues",
            method="POST",
            fields=(),
            typed_fields=(f"sub_issue_id={issue_id}",),
        )

    async def read_issue(self, issue_number: int) -> SpecIssueRecord:
        record = await self._api(f"issues/{issue_number}")
        body = record.get("body") or ""
        marker = "Operation identity: "
        operation_id = body.split(marker, 1)[1].splitlines()[0].strip() if marker in body else ""
        parent = record.get("parent_issue_url") or ""
        parent_number = int(parent.rstrip("/").split("/")[-1]) if parent else 0
        return SpecIssueRecord(
            number=record["number"],
            issue_id=record["id"],
            title=record["title"],
            operation_id=operation_id,
            parent_issue_number=parent_number,
            state=record.get("state", "unknown"),
        )

    def _record(self, record: dict, operation_id: str) -> SpecIssueRecord:
        return SpecIssueRecord(
            number=record["number"],
            issue_id=record["id"],
            title=record["title"],
            operation_id=operation_id,
            parent_issue_number=0,
            state=record.get("state", "unknown"),
        )

    async def _api(
        self,
        path: str,
        *,
        method: str = "GET",
        fields: tuple[str, ...] = (),
        typed_fields: tuple[str, ...] = (),
        paginate: bool = False,
    ):
        args = ["gh", "api", f"repos/{self.repository}/{path}", "--method", method]
        if paginate:
            args.extend(["--paginate", "--slurp"])
        for field in fields:
            args.extend(["-f", field])
        for field in typed_fields:
            args.extend(["-F", field])
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
    def _run_text(args: list[str]) -> str:
        result = subprocess.run(
            args, capture_output=True, check=False, text=True, encoding="utf-8"
        )
        if result.returncode:
            raise RuntimeError("GitHub SPEC issue creation failed")
        return result.stdout.strip()

    @staticmethod
    def _write_body_file(body: str) -> str:
        descriptor, path = tempfile.mkstemp(prefix="temporalio-codex-spec-", suffix=".txt")
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            stream.write(body)
        return path
