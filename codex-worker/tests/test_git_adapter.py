import shutil
import subprocess
import sys

import pytest

from temporalio_codex.delivery_models import (
    DeliveryOperation,
    DeliveryOutcome,
    DeliveryPhase,
    ReviewEvidence,
)
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


def make_bare_remote(tmp_path, repository):
    remote = tmp_path / "remote.git"
    run_git(tmp_path, "init", "--bare", str(remote))
    run_git(repository, "remote", "add", "origin", str(remote))
    run_git(repository, "push", "-u", "origin", "main")
    return remote


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
        await adapter.execute(
            candidate_operation(repository, workspace, base_sha="HEAD")
        )


async def test_local_git_adapter_requires_explicit_acceptance_command(tmp_path) -> None:
    repository = make_repository(tmp_path)
    workspace = tmp_path / "candidate"
    adapter = LocalGitAdapter()
    candidate = await adapter.execute(candidate_operation(repository, workspace))

    missing = await adapter.execute(
        candidate_operation(
            repository,
            workspace,
            operation_id="acceptance-missing",
            phase=DeliveryPhase.ACCEPTANCE,
            candidate_sha=candidate.candidate_sha,
        )
    )
    passed = await adapter.execute(
        candidate_operation(
            repository,
            workspace,
            operation_id="acceptance-pass",
            phase=DeliveryPhase.ACCEPTANCE,
            candidate_sha=candidate.candidate_sha,
            acceptance_command=(sys.executable, "-c", "pass"),
        )
    )

    assert missing.outcome is DeliveryOutcome.NOT_VERIFIED
    assert passed.outcome is DeliveryOutcome.COMPLETED


async def test_local_git_adapter_pushes_and_reads_back_origin_merge(tmp_path) -> None:
    repository = make_repository(tmp_path)
    make_bare_remote(tmp_path, repository)
    workspace = tmp_path / "candidate"
    adapter = LocalGitAdapter()
    candidate = await adapter.execute(candidate_operation(repository, workspace))

    push = await adapter.execute(
        DeliveryOperation(
            operation_id="push-1",
            run_id="run-1",
            phase=DeliveryPhase.PUSH,
            repository=str(repository),
            workspace=str(workspace),
            target_branch="main",
            candidate_sha=candidate.candidate_sha,
            merge_commit_sha=candidate.candidate_sha,
            pull_request_number=1,
        )
    )

    assert push.outcome is DeliveryOutcome.COMPLETED
    assert push.remote_contains_merge is True
    assert push.remote_sha == candidate.candidate_sha


async def test_local_git_adopts_committed_implementation_in_owned_workspace(tmp_path):
    repository = make_repository(tmp_path)
    workspace = tmp_path / "candidate"
    adapter = LocalGitAdapter()
    base = await adapter.execute(candidate_operation(repository, workspace))
    (workspace / "implemented.txt").write_text(
        "real implementation\n", encoding="utf-8"
    )
    run_git(workspace, "add", "implemented.txt")
    run_git(workspace, "commit", "-m", "implement change")
    implemented_sha = run_git(workspace, "rev-parse", "HEAD").stdout.strip()
    adopted = await adapter.execute(
        candidate_operation(
            repository,
            workspace,
            base_sha=base.candidate_sha,
            candidate_sha=implemented_sha,
        )
    )
    assert adopted.candidate_sha == implemented_sha
    assert adopted.candidate_sha != base.candidate_sha


@pytest.mark.parametrize(
    "proof_kind", ["approved", "missing", "mismatched", "rejected"]
)
async def test_local_git_requires_review_evidence_bound_to_candidate(
    tmp_path, proof_kind
):
    repository = make_repository(tmp_path)
    workspace = tmp_path / "candidate"
    adapter = LocalGitAdapter()
    candidate = await adapter.execute(candidate_operation(repository, workspace))
    proof = (
        None
        if proof_kind == "missing"
        else ReviewEvidence(
            candidate_sha="wrong-sha"
            if proof_kind == "mismatched"
            else candidate.candidate_sha,
            verdict="rejected" if proof_kind == "rejected" else "approved",
            operation_id="review-operation",
            thread_id="review-thread",
            turn_id="review-turn",
        )
    )
    result = await adapter.execute(
        candidate_operation(
            repository,
            workspace,
            phase=DeliveryPhase.REVIEW,
            candidate_sha=candidate.candidate_sha,
            review_evidence=proof,
        )
    )
    assert result.outcome is (
        DeliveryOutcome.COMPLETED
        if proof_kind == "approved"
        else DeliveryOutcome.NOT_VERIFIED
    )


async def test_candidate_publication_adopts_remote_after_response_loss(
    tmp_path, monkeypatch
):
    repository = make_repository(tmp_path)
    make_bare_remote(tmp_path, repository)
    workspace = tmp_path / "candidate"
    adapter = LocalGitAdapter()
    candidate = await adapter.execute(candidate_operation(repository, workspace))
    real_run_git = LocalGitAdapter._run_git

    def lose_push_response(cwd, args):
        result = real_run_git(cwd, args)
        if args[0] == "push":
            raise GitCommandError("response lost after server accepted push")
        return result

    monkeypatch.setattr(LocalGitAdapter, "_run_git", staticmethod(lose_push_response))
    operation = candidate_operation(
        repository,
        workspace,
        phase=DeliveryPhase.PUBLISH_CANDIDATE,
        candidate_sha=candidate.candidate_sha,
        candidate_branch="codex/candidate",
    )
    first = await adapter.execute(operation)
    second = await adapter.execute(operation)
    assert first.outcome is DeliveryOutcome.COMPLETED
    assert second.remote_sha == candidate.candidate_sha
    assert (
        run_git(
            repository, "ls-remote", "origin", "refs/heads/codex/candidate"
        ).stdout.split()[0]
        == candidate.candidate_sha
    )
