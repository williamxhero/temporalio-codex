import subprocess
from pathlib import Path

import pytest

from temporalio_codex.candidate_activities import CandidateCaptureInput, capture_codex_candidate
from temporalio_codex.delivery_models import CandidateEvidence
from temporalio_codex.git_adapter import GitCommandError


def git(path, *args):
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True,
        capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def candidate_repo(tmp_path):
    git(tmp_path, "init")
    git(tmp_path, "config", "user.email", "candidate-test@example.test")
    git(tmp_path, "config", "user.name", "Candidate Test")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "app.py").write_text("value = 1\n", encoding="utf-8", newline="\n")
    git(tmp_path, "add", "--", "src/app.py")
    git(tmp_path, "commit", "-m", "Initial candidate")
    sha = git(tmp_path, "rev-parse", "HEAD")
    return CandidateEvidence(str(tmp_path), str(tmp_path), sha, sha)


async def test_implementation_freezes_only_authorized_paths_and_is_idempotent(candidate_repo):
    workspace = Path(candidate_repo.workspace)
    (workspace / "src" / "app.py").write_text("value = 2\n", encoding="utf-8", newline="\n")
    input = CandidateCaptureInput(
        candidate_repo, operation_id="implementation-op", allowed_scope=("src",)
    )
    frozen = await capture_codex_candidate(input)
    assert frozen.candidate.candidate_sha != candidate_repo.candidate_sha
    assert git(workspace, "status", "--porcelain") == ""
    repeated = await capture_codex_candidate(input)
    assert repeated.candidate == frozen.candidate
    assert git(workspace, "rev-list", "--count", "HEAD") == "2"


async def test_freezing_refuses_out_of_scope_changes_before_staging(candidate_repo):
    workspace = Path(candidate_repo.workspace)
    (workspace / "secret.txt").write_text("local only\n", encoding="utf-8")
    with pytest.raises(GitCommandError, match="authorized scope"):
        await capture_codex_candidate(CandidateCaptureInput(
            candidate_repo, operation_id="implementation-op", allowed_scope=("src",)
        ))
    assert git(workspace, "diff", "--cached", "--name-only") == ""
    assert git(workspace, "rev-parse", "HEAD") == candidate_repo.candidate_sha


async def test_review_does_not_commit_dirty_workspace(candidate_repo):
    workspace = Path(candidate_repo.workspace)
    (workspace / "src" / "app.py").write_text("value = 3\n", encoding="utf-8")
    with pytest.raises(GitCommandError, match="dirty"):
        await capture_codex_candidate(CandidateCaptureInput(
            candidate_repo, expected_sha=candidate_repo.candidate_sha,
            review_json="{}", allowed_scope=("src",),
        ))
    assert git(workspace, "rev-parse", "HEAD") == candidate_repo.candidate_sha
