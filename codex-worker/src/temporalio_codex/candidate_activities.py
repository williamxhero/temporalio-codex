import json
from dataclasses import dataclass
from pathlib import Path

from temporalio import activity

from temporalio_codex.delivery_models import CandidateEvidence, ReviewEvidence
from temporalio_codex.git_adapter import GitCommandError, LocalGitAdapter


@dataclass(frozen=True)
class CandidateCaptureInput:
    candidate: CandidateEvidence
    expected_sha: str | None = None
    review_json: str | None = None
    operation_id: str = ""
    thread_id: str = ""
    turn_id: str = ""


@dataclass(frozen=True)
class CandidateCaptureResult:
    candidate: CandidateEvidence
    review: ReviewEvidence | None = None


@activity.defn(name="capture-codex-candidate")
async def capture_codex_candidate(
    input: CandidateCaptureInput,
) -> CandidateCaptureResult:
    adapter = LocalGitAdapter()
    workspace = Path(input.candidate.workspace).resolve()
    repository = Path(input.candidate.repository).resolve()
    repo_common = await adapter._git(
        repository, "rev-parse", "--path-format=absolute", "--git-common-dir"
    )
    work_common = await adapter._git(
        workspace, "rev-parse", "--path-format=absolute", "--git-common-dir"
    )
    if Path(repo_common).resolve() != Path(work_common).resolve():
        raise GitCommandError("implemented candidate belongs to a different repository")
    sha = await adapter._git(workspace, "rev-parse", "HEAD^{commit}")
    await adapter._git(
        workspace, "merge-base", "--is-ancestor", input.candidate.base_sha, sha
    )
    await adapter._assert_clean(workspace)
    if input.expected_sha and sha != input.expected_sha:
        raise GitCommandError("candidate SHA changed during independent review")
    candidate = CandidateEvidence(
        str(repository), str(workspace), input.candidate.base_sha, sha
    )
    review = None
    if input.review_json is not None:
        try:
            proof = json.loads(input.review_json)
        except (ValueError, TypeError) as error:
            raise GitCommandError(
                "review did not return structured JSON evidence"
            ) from error
        if (
            not isinstance(proof, dict)
            or proof.get("verdict") != "approved"
            or proof.get("candidate_sha") != sha
            or proof.get("findings") != []
            or not all((input.operation_id, input.thread_id, input.turn_id))
        ):
            raise GitCommandError(
                "review approval evidence is missing or does not match candidate SHA"
            )
        review = ReviewEvidence(
            sha, "approved", input.operation_id, input.thread_id, input.turn_id
        )
    return CandidateCaptureResult(candidate, review)
