from copy import deepcopy
from dataclasses import replace

import pytest
from acceptance.test_whole_flow import whole_flow_input

from temporalio_codex.whole_flow_models import (
    SpecCodexPlan,
    validate_delivery_evidence,
    validate_whole_flow_input,
)


def completed_delivery():
    return {
        "status": "completed",
        "candidate": {"candidate_sha": "implemented-sha"},
        "review_evidence": {
            "candidate_sha": "implemented-sha", "verdict": "approved",
            "operation_id": "review-op", "thread_id": "review-thread", "turn_id": "review-turn",
        },
        "receipts": [
            {"phase": phase, "outcome": "completed", "candidate_sha": "implemented-sha"}
            for phase in ("candidate", "acceptance", "review", "publish_candidate")
        ] + [{"phase": "push", "remote_contains_merge": True}],
    }


def test_final_gate_accepts_candidate_with_matching_review_and_receipts():
    assert validate_delivery_evidence(completed_delivery()) == ()


@pytest.mark.parametrize("missing", ["candidate", "review_evidence", "review", "acceptance", "publish_candidate"])
def test_final_gate_rejects_missing_candidate_proof(missing):
    result = deepcopy(completed_delivery())
    if missing in ("candidate", "review_evidence"):
        result.pop(missing)
    else:
        result["receipts"] = [receipt for receipt in result["receipts"] if receipt["phase"] != missing]
    assert validate_delivery_evidence(result)


def test_final_gate_rejects_review_of_another_candidate():
    result = completed_delivery()
    result["review_evidence"]["candidate_sha"] = "other-sha"
    assert "matching candidate" in validate_delivery_evidence(result)[0]


@pytest.mark.parametrize("seconds", [30, 1800, 2400])
def test_spec_codex_plan_preserves_configured_stage_timeout(seconds):
    plan = SpecCodexPlan(
        "one", "implement", ".", ("src/",), start_to_close_timeout_seconds=seconds,
    )
    assert all(stage.start_to_close_timeout_seconds == seconds for stage in plan.to_run_input().stages)


@pytest.mark.parametrize("seconds", [0, -1, float("inf")])
def test_whole_flow_rejects_unbounded_or_invalid_stage_timeout(seconds):
    input = whole_flow_input()
    invalid = replace(input, codex=tuple(
        replace(plan, start_to_close_timeout_seconds=seconds) for plan in input.codex
    ))
    assert "invalid bounded Codex stage timeout" in validate_whole_flow_input(invalid)
