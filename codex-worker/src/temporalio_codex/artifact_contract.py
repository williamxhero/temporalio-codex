from dataclasses import dataclass
from pathlib import Path


class ArtifactPathError(ValueError):
    """Raised when an implementation artifact escapes its declared scope."""


@dataclass(frozen=True)
class ArtifactResolution:
    requested_path: str
    resolved_path: str
    write_scope: str


def resolve_artifact_path(
    write_scope: str | Path,
    requested_path: str,
    *,
    repository_root: str | Path | None = None,
) -> ArtifactResolution:
    if not requested_path.strip():
        raise ArtifactPathError("artifact path must not be empty")

    scope = Path(write_scope).resolve()
    raw = Path(requested_path)
    if ".." in raw.parts:
        raise ArtifactPathError(
            f"artifact path contains traversal: {requested_path}"
        )
    candidates = []
    if raw.is_absolute():
        candidates.append(raw.resolve())
    elif repository_root is not None:
        root = Path(repository_root).resolve()
        try:
            scope_relative = scope.relative_to(root)
        except ValueError:
            scope_relative = None
        if scope_relative is not None and raw.parts[: len(scope_relative.parts)] == scope_relative.parts:
            candidates.append((root / raw).resolve())
        else:
            candidates.append((scope / raw).resolve())
    else:
        candidates.append((scope / raw).resolve())

    for candidate in candidates:
        if _within(candidate, scope):
            return ArtifactResolution(
                requested_path=requested_path,
                resolved_path=str(candidate),
                write_scope=str(scope),
            )

    raise ArtifactPathError(
        f"artifact path is outside write scope: {requested_path}"
    )


def _within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True
