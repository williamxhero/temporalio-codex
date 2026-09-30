from copy import deepcopy

import pytest

from temporalio_codex.whole_flow_models import validate_delivery_evidence


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
