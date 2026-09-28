import os

import pytest

from .live_issue_harness import LiveHarnessUnavailable, run_issue_round_trip


@pytest.mark.live
def test_live_issue_round_trip_against_skills_repository() -> None:
    if os.environ.get("TC07_RUN_LIVE") != "1":
        pytest.skip("set TC07_RUN_LIVE=1 to run the authenticated GitHub probe")
    try:
        result = run_issue_round_trip(
            repository=os.environ.get("TC07_LIVE_REPOSITORY", "williamxhero/skills"),
        )
    except LiveHarnessUnavailable as error:
        pytest.skip(str(error))
    assert result.status == "cleaned"
    assert result.issue_number is not None
    assert result.comment_id is not None
