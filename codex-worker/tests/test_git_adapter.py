import shutil
import subprocess

import pytest

from temporalio_codex.delivery_models import DeliveryOperation, DeliveryPhase
from temporalio_codex.git_adapter import GitCommandError, LocalGitAdapter


pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git is required")


def run_git(cwd, *args):
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        check=True,
        text=True,
    )


def make_repository(tmp_path):
    repository = tmp_path / "repository"
    repository.mkdir()
    run_git(repository, "init", "-b", "main")
    run_git(repository, "config", "user.email", "test@example.invalid")
    run_git(repository, "config", "user.name", "Test User")
    (repository / "README").write_text("initial\n", encoding="utf-8")
    run_git(repository, "add", "README")
    run_git(repository, "commit", "-m", "initial")
    return repository


def candidate_operation(repository, workspace, **overrides):
    values = dict(
        operation_id="candidate-1",
        run_id="run-1",
        phase=DeliveryPhase.CANDIDATE,
        repository=str(repository),
        workspace=str(workspace),
        base_sha="HEAD",
    )
    values.update(overrides)
    return DeliveryOperation(**values)


async def test_local_git_adapter_prepares_and_reuses_clean_candidate(tmp_path) -> None:
    repository = make_repository(tmp_path)
    workspace = tmp_path / "candidate"
    adapter = LocalGitAdapter()

    first = await adapter.execute(candidate_operation(repository, workspace))
    second = await adapter.execute(candidate_operation(repository, workspace))

    assert first.candidate_sha
    assert second.candidate_sha == first.candidate_sha
    assert "HEAD=" in first.evidence_refs[0]
    assert run_git(workspace, "status", "--porcelain").stdout == ""


async def test_local_git_adapter_rejects_stale_and_dirty_candidate(tmp_path) -> None:
    repository = make_repository(tmp_path)
    workspace = tmp_path / "candidate"
    adapter = LocalGitAdapter()
    receipt = await adapter.execute(candidate_operation(repository, workspace))

    (workspace / "uncommitted").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(GitCommandError, match="dirty"):
        await adapter.execute(
            DeliveryOperation(
                operation_id="acceptance-1",
                run_id="run-1",
                phase=DeliveryPhase.ACCEPTANCE,
                repository=str(repository),
                workspace=str(workspace),
                candidate_sha=receipt.candidate_sha,
                acceptance_version="v1",
            )
        )

    (workspace / "uncommitted").unlink()
    (repository / "README").write_text("changed\n", encoding="utf-8")
    run_git(repository, "add", "README")
    run_git(repository, "commit", "-m", "second")
    with pytest.raises(GitCommandError, match="different SHA|changed"):
        await adapter.execute(candidate_operation(repository, workspace, base_sha="HEAD"))
