from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from temporalio_codex.ticket_scheduler import TicketPlan


class TicketPublicationStatus(StrEnum):
    VERIFIED = "verified"
    BLOCKED = "blocked"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class TicketPublicationInput:
    repository: str
    operation_id: str
    spec_issue_number: int
    blocker_issue_numbers: tuple[tuple[str, int], ...]
    tickets: tuple[TicketPlan, ...]


@dataclass(frozen=True)
class TicketIssueRecord:
    number: int
    issue_id: int
    key: str
    operation_id: str
    parent_spec_issue_number: int
    blocker_issue_numbers: tuple[int, ...]
    body: str


@dataclass(frozen=True)
class TicketPublicationResult:
    status: TicketPublicationStatus
    issues: tuple[TicketIssueRecord, ...] = ()
    reason: str = ""


class TicketIssueGateway(Protocol):
    async def find_by_operation(self, operation_id: str) -> TicketIssueRecord | None: ...

    async def create_issue(
        self, input: TicketPublicationInput, ticket: TicketPlan, body: str
    ) -> TicketIssueRecord: ...

    async def add_parent(self, parent_issue_number: int, issue_id: int) -> None: ...

    async def read_issue(self, issue_number: int) -> TicketIssueRecord: ...


def ticket_issue_body(
    input: TicketPublicationInput,
    ticket: TicketPlan,
    blocker_issue_numbers: tuple[int, ...],
    operation_id: str,
) -> str:
    criteria = ticket.acceptance_criteria or (f"Complete work for ticket {ticket.key}.",)
    blockers = ", ".join(f"#{number}" for number in blocker_issue_numbers) or "None"
    return (
        f"<!-- temporalio-codex:ticket:{operation_id} -->\n\n"
        f"## Ticket {ticket.key}: {ticket.title or ticket.key}\n\n"
        f"Parent SPEC: #{input.spec_issue_number}\n\n"
        f"Blocked by: {blockers}\n\n"
        "## Acceptance criteria\n\n"
        + "\n".join(f"- [ ] {criterion}" for criterion in criteria)
        + "\n"
    )


async def publish_tickets(
    input: TicketPublicationInput, gateway: TicketIssueGateway
) -> TicketPublicationResult:
    if not input.repository.strip() or not input.operation_id.strip():
        return TicketPublicationResult(
            TicketPublicationStatus.BLOCKED,
            reason="repository and operation identity are required",
        )
    if input.spec_issue_number <= 0 or not input.tickets:
        return TicketPublicationResult(
            TicketPublicationStatus.BLOCKED,
            reason="a SPEC Issue and at least one ticket are required",
        )

    plans = {ticket.key: ticket for ticket in input.tickets}
    if len(plans) != len(input.tickets):
        return TicketPublicationResult(
            TicketPublicationStatus.BLOCKED, reason="ticket keys must be unique"
        )
    published_numbers = dict(input.blocker_issue_numbers)
    remaining = set(plans)
    ordered: list[TicketPlan] = []
    while remaining:
        ready = sorted(
            key
            for key in remaining
            if set(plans[key].blockers).isdisjoint(remaining)
        )
        if not ready:
            return TicketPublicationResult(
                TicketPublicationStatus.BLOCKED, reason="ticket dependency cycle"
            )
        ordered.extend(plans[key] for key in ready)
        remaining.difference_update(ready)

    records: list[TicketIssueRecord] = []
    try:
        for ticket in ordered:
            missing = [key for key in ticket.blockers if key not in published_numbers]
            if missing:
                return TicketPublicationResult(
                    TicketPublicationStatus.BLOCKED,
                    tuple(records),
                    f"blocker Issues are not published: {', '.join(missing)}",
                )
            operation_id = f"{input.operation_id}:{ticket.key}"
            blocker_numbers = tuple(sorted(published_numbers[key] for key in ticket.blockers))
            body = ticket_issue_body(input, ticket, blocker_numbers, operation_id)
            existing = await gateway.find_by_operation(operation_id)
            if existing is None:
                existing = await gateway.create_issue(input, ticket, body)
            current = await gateway.read_issue(existing.number)
            if current.parent_spec_issue_number != input.spec_issue_number:
                await gateway.add_parent(input.spec_issue_number, existing.issue_id)
            readback = await gateway.read_issue(existing.number)
            if (
                readback.operation_id != operation_id
                or readback.key != ticket.key
                or readback.parent_spec_issue_number != input.spec_issue_number
                or readback.blocker_issue_numbers != blocker_numbers
                or readback.body != body
            ):
                return TicketPublicationResult(
                    TicketPublicationStatus.UNKNOWN,
                    tuple(records),
                    f"ticket {ticket.key} identity or relationship readback mismatch",
                )
            published_numbers[ticket.key] = readback.number
            records.append(readback)
    except Exception as error:
        return TicketPublicationResult(
            TicketPublicationStatus.UNKNOWN,
            tuple(records),
            f"ticket publication requires readback: {type(error).__name__}",
        )
    return TicketPublicationResult(TicketPublicationStatus.VERIFIED, tuple(records))


@dataclass
class FakeTicketIssueGateway:
    issues: dict[str, TicketIssueRecord] = field(default_factory=dict)
    parent_links: dict[int, int] = field(default_factory=dict)
    calls: list[str] = field(default_factory=list)
    next_number: int = 500

    async def find_by_operation(self, operation_id: str) -> TicketIssueRecord | None:
        self.calls.append(f"find:{operation_id}")
        return self.issues.get(operation_id)

    async def create_issue(
        self, input: TicketPublicationInput, ticket: TicketPlan, body: str
    ) -> TicketIssueRecord:
        operation_id = f"{input.operation_id}:{ticket.key}"
        blocker_numbers = tuple(
            int(value.removeprefix("#").strip())
            for value in body.split("Blocked by: ", 1)[1].split("\n", 1)[0].split(",")
            if value.strip() and value.strip() != "None"
        )
        record = TicketIssueRecord(
            number=self.next_number,
            issue_id=self.next_number + 1_000_000_000,
            key=ticket.key,
            operation_id=operation_id,
            parent_spec_issue_number=0,
            blocker_issue_numbers=blocker_numbers,
            body=body,
        )
        self.next_number += 1
        self.issues[operation_id] = record
        self.calls.append(f"create:{operation_id}")
        return record

    async def add_parent(self, parent_issue_number: int, issue_id: int) -> None:
        self.parent_links[issue_id] = parent_issue_number

    async def read_issue(self, issue_number: int) -> TicketIssueRecord:
        for issue in self.issues.values():
            if issue.number == issue_number:
                return TicketIssueRecord(
                    **{
                        **issue.__dict__,
                        "parent_spec_issue_number": self.parent_links.get(
                            issue.issue_id, issue.parent_spec_issue_number
                        ),
                    }
                )
        raise LookupError("ticket Issue not found")


@dataclass
class GhCliTicketIssueGateway:
    repository: str

    async def find_by_operation(self, operation_id: str) -> TicketIssueRecord | None:
        pages = await self._api(
            "issues", fields=("state=all", "per_page=100"), paginate=True
        )
        for page in pages:
            for item in page:
                if f"<!-- temporalio-codex:ticket:{operation_id} -->" in (
                    item.get("body") or ""
                ):
                    return self._record(item)
        return None

    async def create_issue(
        self, input: TicketPublicationInput, ticket: TicketPlan, body: str
    ) -> TicketIssueRecord:
        path = self._write_body_file(body)
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
                    f"[TICKET {ticket.key}] {ticket.title or ticket.key}",
                    "--body-file",
                    path,
                ],
            )
        finally:
            os.unlink(path)
        return await self.read_issue(int(url.rsplit("/", 1)[-1]))

    async def add_parent(self, parent_issue_number: int, issue_id: int) -> None:
        await self._api(
            "issues",
            str(parent_issue_number),
            "sub_issues",
            method="POST",
            typed_fields=(f"sub_issue_id={issue_id}",),
        )

    async def read_issue(self, issue_number: int) -> TicketIssueRecord:
        return self._record(await self._api("issues", str(issue_number)))

    def _record(self, item: dict) -> TicketIssueRecord:
        body = item.get("body") or ""
        marker = "<!-- temporalio-codex:ticket:"
        operation_id = (
            body.split(marker, 1)[1].split(" -->", 1)[0] if marker in body else ""
        )
        key = body.split("## Ticket ", 1)[1].split(":", 1)[0] if "## Ticket " in body else ""
        parent = item.get("parent_issue_url") or ""
        parent_number = int(parent.rstrip("/").split("/")[-1]) if parent else 0
        blocker_line = next(
            (line for line in body.splitlines() if line.startswith("Blocked by: ")),
            "Blocked by: None",
        )
        blockers = tuple(
            int(value.strip().lstrip("#"))
            for value in blocker_line.removeprefix("Blocked by: ").split(",")
            if value.strip() and value.strip() != "None"
        )
        return TicketIssueRecord(
            item["number"], item["id"], key, operation_id, parent_number, blockers, body
        )

    async def _api(
        self,
        *path: str,
        method: str = "GET",
        fields: tuple[str, ...] = (),
        typed_fields: tuple[str, ...] = (),
        paginate: bool = False,
    ):
        args = ["gh", "api", f"repos/{self.repository}/" + "/".join(path), "--method", method]
        if paginate:
            args.extend(["--paginate", "--slurp"])
        args.extend(option for field in fields for option in ("-f", field))
        args.extend(option for field in typed_fields for option in ("-F", field))
        return await asyncio.to_thread(self._run_json, args)

    @staticmethod
    def _run_json(args: list[str]):
        result = subprocess.run(
            args, capture_output=True, check=False, text=True, encoding="utf-8"
        )
        if result.returncode:
            raise RuntimeError("GitHub ticket API request failed")
        return json.loads(result.stdout)

    @staticmethod
    def _run_text(args: list[str]) -> str:
        result = subprocess.run(
            args, capture_output=True, check=False, text=True, encoding="utf-8"
        )
        if result.returncode:
            raise RuntimeError("GitHub ticket creation failed")
        return result.stdout.strip()

    @staticmethod
    def _write_body_file(body: str) -> str:
        descriptor, path = tempfile.mkstemp(prefix="temporalio-codex-ticket-", suffix=".txt")
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            stream.write(body)
        return path
