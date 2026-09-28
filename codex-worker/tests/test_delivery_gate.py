from temporalio_codex.delivery_gate import (
    DeliveryGateStatus,
    SpecDeliveryEvidence,
    TicketDeliveryEvidence,
    build_development_summary,
    qualify_spec_delivery,
)


def complete_evidence(**overrides) -> SpecDeliveryEvidence:
    values = dict(
        spec_key="spec-1",
        ticket_evidence=(TicketDeliveryEvidence("ticket-1", 101, "completed", "sha-1"),),
        tests_passed=True,
        review_completed=True,
        unresolved_findings=(),
        delivery_status="completed",
        delivery_run_id="delivery-run-1",
        pull_request_number=42,
        pull_request_url="https://github.com/owner/repo/pull/42",
        merge_commit_sha="merge-1",
        remote_sha="merge-1",
        remote_contains_merge=True,
    )
    values.update(overrides)
    return SpecDeliveryEvidence(**values)


def test_delivery_gate_requires_all_evidence_and_remote_readback() -> None:
    result = qualify_spec_delivery(complete_evidence(), ("ticket-1",))

    assert result.status is DeliveryGateStatus.PASS
    assert "merge-1" in result.evidence_refs

    missing = qualify_spec_delivery(complete_evidence(), ("ticket-1", "ticket-2"))
    assert missing.status is DeliveryGateStatus.BLOCKED


def test_delivery_gate_distinguishes_failure_unknown_and_remote_divergence() -> None:
    assert qualify_spec_delivery(
        complete_evidence(tests_passed=False), ("ticket-1",)
    ).status is DeliveryGateStatus.FAIL
    assert qualify_spec_delivery(
        complete_evidence(delivery_status="unknown"), ("ticket-1",)
    ).status is DeliveryGateStatus.WAITING_FOR_READBACK
    assert qualify_spec_delivery(
        complete_evidence(remote_contains_merge=False), ("ticket-1",)
    ).status is DeliveryGateStatus.WAITING_FOR_READBACK


def test_unverified_live_gate_surfaces_in_summary_without_becoming_pass() -> None:
    evidence = complete_evidence(
        live_unverified_reasons=("live GitHub checks not verified",)
    )
    result = qualify_spec_delivery(evidence, ("ticket-1",))
    summary = build_development_summary((result,), (evidence,))

    assert result.status is DeliveryGateStatus.NOT_VERIFIED
    assert summary.status is DeliveryGateStatus.NOT_VERIFIED
    assert "live GitHub checks not verified" in summary.to_text()


def test_summary_with_no_evidence_is_blocked() -> None:
    summary = build_development_summary((), ())

    assert summary.status is DeliveryGateStatus.BLOCKED
    assert "no SPEC delivery evidence" in summary.reason


def test_gate_rejects_duplicate_ticket_evidence_and_summary_mismatch() -> None:
    evidence = complete_evidence(
        ticket_evidence=(
            TicketDeliveryEvidence("ticket-1", 101, "completed", "sha-1"),
            TicketDeliveryEvidence("ticket-1", 101, "completed", "sha-1"),
        )
    )

    result = qualify_spec_delivery(evidence, ("ticket-1",))
    summary = build_development_summary((result,), ())

    assert result.status is DeliveryGateStatus.BLOCKED
    assert summary.status is DeliveryGateStatus.BLOCKED
    assert "counts differ" in summary.reason
