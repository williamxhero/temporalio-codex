from __future__ import annotations

from temporalio_codex.takeover_harness import (
    TakeoverHarness,
    TakeoverStatus,
    run_takeover_matrix,
)


def test_takeover_matrix_completes_every_supported_frontier() -> None:
    results = run_takeover_matrix()

    assert results
    assert all(result.status is TakeoverStatus.VERIFIED for result in results)
    assert all(result.state.source_revision == "source-v1" for result in results)


def test_existing_grill_and_ticket_graph_skip_planning_stages() -> None:
    result = TakeoverHarness.for_scenario("tickets-ready").run()

    assert result.status is TakeoverStatus.VERIFIED
    assert "grill" not in result.state.actions
    assert "to-spec" not in result.state.actions
    assert not any(action.startswith("to-tickets") for action in result.state.actions)


def test_completed_spec_is_adopted_while_next_dependency_ready_spec_continues() -> None:
    harness = TakeoverHarness.for_scenario("mixed-completed")
    result = harness.run()

    assert result.status is TakeoverStatus.VERIFIED
    assert "S1" in result.state.skipped
    assert all(action.endswith(":S2") for action in result.state.actions if ":" in action)
    assert result.state.implementation_calls == 1


def test_process_exit_resumes_from_durable_frontier_without_repeating_work() -> None:
    harness = TakeoverHarness.for_scenario("tickets-ready")
    first = harness.run(max_actions=2)
    saved = harness.serialized_state()
    resumed = TakeoverHarness.from_serialized(saved)
    second = resumed.run()

    assert first.status is TakeoverStatus.BLOCKED
    assert second.status is TakeoverStatus.VERIFIED
    assert second.state.implementation_calls == 2
    assert second.state.actions[:2] == first.state.actions


def test_merge_timeout_reconciles_without_second_merge_request() -> None:
    harness = TakeoverHarness.for_scenario("merge-timeout")
    first = harness.run()
    second = harness.run(expected_snapshot_digest=first.snapshot_digest)

    assert first.status is TakeoverStatus.BLOCKED
    assert "unknown" in first.reason
    assert second.status is TakeoverStatus.VERIFIED
    assert second.state.merge_calls == 1


def test_cleanup_retry_does_not_repeat_implementation_or_merge() -> None:
    harness = TakeoverHarness.for_scenario("cleanup-retry")
    first = harness.run()
    second = harness.run(expected_snapshot_digest=first.snapshot_digest)

    assert first.status is TakeoverStatus.CLEANUP_PENDING
    assert second.status is TakeoverStatus.VERIFIED
    assert second.state.implementation_calls == 0
    assert second.state.merge_calls == 0
    assert second.state.cleanup_calls == 2


def test_source_change_and_discovery_blockers_fail_closed() -> None:
    changed = TakeoverHarness.for_scenario("tickets-ready")
    original_digest = changed.snapshot_digest()
    changed.mutate_source()
    result = changed.run(expected_snapshot_digest=original_digest)
    assert result.status is TakeoverStatus.BLOCKED
    assert "snapshot changed" in result.reason

    for scenario in ("relation-conflict", "duplicate-issue", "branch-drift", "dirty-worktree", "remote-divergence", "scope-violation"):
        blocked = TakeoverHarness.for_scenario(scenario).run()
        assert blocked.status is TakeoverStatus.BLOCKED
        assert scenario in blocked.reason
