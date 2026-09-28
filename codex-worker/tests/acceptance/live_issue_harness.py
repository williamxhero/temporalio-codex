from __future__ import annotations

import json
import os
import platform
import secrets
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class LiveIssueManifest:
    marker: str
    repository: str
    run_id: str
    build_id: str
    scenario: str
    command: str
    operating_system: str
    evidence_kind: str = "live_github"
    expected_resources: tuple[str, ...] = ("issue", "comment")
    cleanup_plan: tuple[str, ...] = ("close marker-matched issue", "verify CLOSED readback")
    evidence_refs: tuple[str, ...] = ()
    issue_number: int | None = None
    comment_id: int | None = None
    issue_url: str | None = None
    status: str = "active"
    reason: str = ""


class LiveHarnessUnavailable(RuntimeError):
    pass


def run_issue_round_trip(
    *, repository: str = "williamxhero/skills", manifest_dir: str | Path | None = None
) -> LiveIssueManifest:
    _require_gh()
    marker = _marker()
    directory = Path(
        manifest_dir
        or os.environ.get(
            "TC07_LIVE_MANIFEST_DIR",
            Path(tempfile.gettempdir()) / "temporalio-codex-tc07-live",
        )
    )
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = directory / f"{marker}.json"
    manifest = LiveIssueManifest(
        marker=marker,
        repository=repository,
        run_id=marker,
        build_id=os.environ.get("TC07_BUILD_ID", "working-tree"),
        scenario="authenticated GitHub Issue and comment round trip",
        command="uv run pytest tests/acceptance/test_live_issue.py -m live -vv",
        operating_system=platform.platform(),
    )
    _write_manifest(manifest_path, manifest)

    title = f"[TC-07 LIVE] {marker} acceptance probe"
    body = (
        "## TC-07 live acceptance probe\n\n"
        f"Run marker: `{marker}`\n"
        "This temporary Issue verifies UTF-8 body-file publication and readback.\n"
    )
    try:
        created = _gh_issue_create(repository, title, body)
        manifest = LiveIssueManifest(
            **{
                **asdict(manifest),
                "issue_number": created,
                "status": "issue_created",
            }
        )
        _write_manifest(manifest_path, manifest)
        issue = _gh_issue_view(repository, created)
        if marker not in issue["body"]:
            raise RuntimeError("Issue readback does not contain the run marker")
        manifest = LiveIssueManifest(
            **{
                **asdict(manifest),
                "issue_url": issue["url"],
                "evidence_refs": (issue["url"],),
            }
        )
        _write_manifest(manifest_path, manifest)
        comment_body = f"<!-- tc07-live:{marker} -->\nReadback verified for `{marker}`."
        comment_id = _gh_comment(repository, created, comment_body, marker)
        comment = _gh_comment_view(repository, created, comment_id)
        if marker not in comment["body"]:
            raise RuntimeError("comment readback does not contain the run marker")
        manifest = LiveIssueManifest(
            **{
                **asdict(manifest),
                "comment_id": comment_id,
                "evidence_refs": (*manifest.evidence_refs, f"comment:{comment_id}"),
                "status": "verified",
            }
        )
        _write_manifest(manifest_path, manifest)
        _gh_close(repository, created)
        closed = _gh_issue_view(repository, created)
        if closed["state"] != "CLOSED":
            raise RuntimeError("Issue cleanup readback did not show CLOSED")
        manifest = LiveIssueManifest(**{**asdict(manifest), "status": "cleaned"})
        _write_manifest(manifest_path, manifest)
        return manifest
    except Exception as error:
        manifest = LiveIssueManifest(
            **{**asdict(manifest), "status": "failed", "reason": str(error)[:500]}
        )
        _write_manifest(manifest_path, manifest)
        if manifest.issue_number:
            try:
                _gh_close(repository, manifest.issue_number)
            except Exception:
                pass
        raise


def _require_gh() -> None:
    try:
        result = subprocess.run(
            ["gh", "auth", "status"],
            capture_output=True,
            check=False,
            text=True,
        )
    except OSError as error:
        raise LiveHarnessUnavailable("gh CLI is unavailable") from error
    if result.returncode:
        raise LiveHarnessUnavailable("GitHub authentication is unavailable")


def _gh_issue_create(repository: str, title: str, body: str) -> int:
    body_file = _body_file(body)
    try:
        result = _run(["gh", "issue", "create", "--repo", repository, "--title", title, "--body-file", str(body_file)])
    finally:
        body_file.unlink(missing_ok=True)
    return int(result.rsplit("/", 1)[-1])


def _gh_issue_view(repository: str, number: int) -> dict:
    return json.loads(_run(["gh", "issue", "view", str(number), "--repo", repository, "--json", "number,body,state,url"]))


def _gh_comment(repository: str, number: int, body: str, marker: str) -> int:
    body_file = _body_file(body)
    try:
        _run(["gh", "issue", "comment", str(number), "--repo", repository, "--body-file", str(body_file)])
    finally:
        body_file.unlink(missing_ok=True)
    comments = json.loads(
        _run(["gh", "api", f"repos/{repository}/issues/{number}/comments", "--paginate"])
    )
    marker = f"<!-- tc07-live:{marker} -->"
    matches = [
        comment
        for comment in comments
        if marker in (comment.get("body") or "")
    ]
    if len(matches) != 1:
        raise RuntimeError("live comment was not uniquely readable")
    return int(matches[0]["id"])


def _gh_comment_view(repository: str, number: int, comment_id: int) -> dict:
    return json.loads(_run(["gh", "api", f"repos/{repository}/issues/comments/{comment_id}"]))


def _gh_close(repository: str, number: int) -> None:
    _run(["gh", "issue", "close", str(number), "--repo", repository])


def _run(command: list[str]) -> str:
    result = subprocess.run(command, capture_output=True, check=False, text=True)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "GitHub command failed")
    return result.stdout.strip()


def _body_file(body: str) -> Path:
    descriptor, path = tempfile.mkstemp(prefix="tc07-live-", suffix=".txt")
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
        stream.write(body)
    return Path(path)


def _write_manifest(path: Path, manifest: LiveIssueManifest) -> None:
    path.write_text(json.dumps(asdict(manifest), ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")


def _marker() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"TC07-{timestamp}-{secrets.token_hex(4)}"
