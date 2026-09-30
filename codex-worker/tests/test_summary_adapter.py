from unittest.mock import AsyncMock

import pytest

from temporalio_codex.summary_adapter import (
    FakeSummaryCommentGateway,
    GhCliSummaryCommentGateway,
    SummaryPublicationInput,
    SummaryPublicationStatus,
    publish_summary,
)


def publication_input(**overrides) -> SummaryPublicationInput:
    values = dict(
        repository="owner/repo",
        umbrella_issue_number=8,
        operation_id="run-1:summary",
        summary_text="Status: pass\nRemote: origin/main=abc",
    )
    values.update(overrides)
    return SummaryPublicationInput(**values)


async def test_summary_readback_uses_comment_resource_and_actual_issue_scope() -> None:
    gateway = GhCliSummaryCommentGateway("owner/repo")
    gateway._api = AsyncMock(return_value={
        "id": 42, "body": "summary", "issue_url": "https://api.github.com/repos/owner/repo/issues/9"
    })

    record = await gateway.read_summary_comment(publication_input(), 42)

    gateway._api.assert_awaited_once_with("issues", "comments", "42")
    assert record.issue_number == 9


async def test_summary_publication_creates_and_verifies_comment() -> None:
    gateway = FakeSummaryCommentGateway()

    result = await publish_summary(publication_input(), gateway)
    repeated = await publish_summary(publication_input(), gateway)

    assert result.status is SummaryPublicationStatus.VERIFIED
    assert result.comment is not None
    assert repeated == result
    assert gateway.calls.count("create:run-1:summary") == 1


async def test_summary_publication_adopts_comment_after_lost_create_response() -> None:
    gateway = FakeSummaryCommentGateway(fail_create_once={"run-1:summary"})

    result = await publish_summary(publication_input(), gateway)

    assert result.status is SummaryPublicationStatus.VERIFIED
    assert result.comment is not None
    assert gateway.calls.count("create:run-1:summary") == 1


async def test_summary_publication_waits_on_ambiguous_or_unknown_github_state() -> None:
    ambiguous = FakeSummaryCommentGateway(ambiguous_operations={"run-1:summary"})
    lookup_failed = FakeSummaryCommentGateway(fail_find=True)

    ambiguous_result = await publish_summary(publication_input(), ambiguous)
    unknown_result = await publish_summary(publication_input(), lookup_failed)

    assert ambiguous_result.status is SummaryPublicationStatus.UNKNOWN
    assert "ambiguous" in ambiguous_result.reason
    assert unknown_result.status is SummaryPublicationStatus.UNKNOWN


@pytest.mark.parametrize(
    "overrides",
    [
        {"umbrella_issue_number": 0},
        {"operation_id": ""},
        {"summary_text": ""},
    ],
)
async def test_summary_publication_rejects_invalid_scope(overrides) -> None:
    result = await publish_summary(publication_input(**overrides), FakeSummaryCommentGateway())

    assert result.status is SummaryPublicationStatus.BLOCKED
