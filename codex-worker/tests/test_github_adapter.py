from temporalio_codex.delivery_models import (
    DeliveryOperation,
    DeliveryOutcome,
    DeliveryPhase,
)
from temporalio_codex.github_adapter import (
    CheckRecord,
    FakeGitHubGateway,
    GitHubDeliveryAdapter,
    PullRequestRecord,
)


class UnavailableGateway(FakeGitHubGateway):
    async def find_pull_request(self, operation):
        raise ConnectionError("readback unavailable")

    async def create_pull_request(self, operation):
        raise ConnectionError("create response lost")


def operation(phase=DeliveryPhase.PULL_REQUEST, **overrides):
    values = dict(
        operation_id="pr-1",
        run_id="run-1",
        phase=phase,
        repository="owner/repo",
        candidate_sha="sha-1",
        candidate_branch="codex/run-1",
        pull_request_identity="codex-operation:run-1",
        title="delivery",
        body="codex-operation:run-1",
    )
    if phase is DeliveryPhase.CI:
        values["pull_request_number"] = 1
    values.update(overrides)
    return DeliveryOperation(**values)


async def test_pull_request_adopts_after_lost_create_without_duplicate() -> None:
    gateway = FakeGitHubGateway(fail_create_once={"codex-operation:run-1"})
    receipt = await GitHubDeliveryAdapter(gateway).execute(operation())

    assert receipt.outcome is DeliveryOutcome.COMPLETED
    assert receipt.pull_request_number == 1
    assert len(gateway.pull_requests) == 1


async def test_pull_request_unknown_when_create_and_readback_both_fail() -> None:
    receipt = await GitHubDeliveryAdapter(UnavailableGateway()).execute(operation())

    assert receipt.outcome is DeliveryOutcome.UNKNOWN
    assert receipt.readback_required is True


async def test_ci_waits_for_terminal_checks_and_rejects_stale_sha() -> None:
    gateway = FakeGitHubGateway(
        checks={1: (CheckRecord("build", "sha-1", "in_progress"),)}
    )
    adapter = GitHubDeliveryAdapter(gateway)
    waiting = await adapter.execute(
        operation(DeliveryPhase.CI, operation_id="ci-wait")
    )
    gateway.checks[1] = (CheckRecord("build", "other-sha", "completed", "success"),)
    stale = await adapter.execute(
        operation(DeliveryPhase.CI, operation_id="ci-stale")
    )

    assert waiting.outcome is DeliveryOutcome.WAITING
    assert stale.outcome is DeliveryOutcome.FAILED


async def test_queued_merge_waits_and_cleanup_is_replayable() -> None:
    gateway = FakeGitHubGateway(
        merge_results={
            1: PullRequestRecord(
                number=1,
                identity="codex-operation:run-1",
                head_sha="sha-1",
                base_branch="main",
                state="queued",
            )
        }
    )
    adapter = GitHubDeliveryAdapter(gateway)
    merge = await adapter.execute(
        operation(DeliveryPhase.MERGE, operation_id="merge-1", pull_request_number=1)
    )
    cleanup_operation = operation(
        DeliveryPhase.CLEANUP,
        operation_id="cleanup-1",
        issue_numbers=(23, 24),
        pull_request_number=1,
    )
    first = await adapter.execute(cleanup_operation)
    second = await adapter.execute(cleanup_operation)

    assert merge.outcome is DeliveryOutcome.WAITING
    assert first.outcome is DeliveryOutcome.COMPLETED
    assert gateway.closed_issues == [23, 24]
    assert second == first


async def test_merge_rejects_external_head_edit() -> None:
    gateway = FakeGitHubGateway(
        merge_results={
            1: PullRequestRecord(
                number=1,
                identity="codex-operation:run-1",
                head_sha="other-sha",
                base_branch="main",
                merged=True,
                merge_commit_sha="merge-sha",
            )
        }
    )

    receipt = await GitHubDeliveryAdapter(gateway).execute(
        operation(DeliveryPhase.MERGE, operation_id="merge-external", pull_request_number=1)
    )

    assert receipt.outcome is DeliveryOutcome.FAILED
    assert "head SHA" in receipt.summary
