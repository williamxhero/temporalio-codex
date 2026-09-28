from pathlib import Path

import pytest

from temporalio_codex.artifact_contract import ArtifactPathError, resolve_artifact_path


def test_artifact_resolution_accepts_scope_relative_and_repository_relative_forms(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    scope = repository / "fixture-app" / "run-1"
    artifact = scope / "nested" / "task.py"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("pass\n", encoding="utf-8")

    scoped = resolve_artifact_path(scope, "nested/task.py")
    workspace_relative = resolve_artifact_path(
        scope,
        "fixture-app/run-1/nested/task.py",
        repository_root=repository,
    )

    assert Path(scoped.resolved_path) == artifact.resolve()
    assert Path(workspace_relative.resolved_path) == artifact.resolve()


@pytest.mark.parametrize(
    "requested_path",
    ["../outside.py", "fixture-app/run-1/../other/task.py"],
)
def test_artifact_resolution_rejects_traversal_and_double_prefix(
    tmp_path: Path, requested_path: str
) -> None:
    repository = tmp_path / "repository"
    scope = repository / "fixture-app" / "run-1"
    scope.mkdir(parents=True)

    with pytest.raises(ArtifactPathError):
        resolve_artifact_path(
            scope,
            requested_path,
            repository_root=repository,
        )


def test_artifact_resolution_rejects_empty_path(tmp_path: Path) -> None:
    with pytest.raises(ArtifactPathError, match="empty"):
        resolve_artifact_path(tmp_path, " ")
