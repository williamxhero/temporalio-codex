from pathlib import Path


def test_temporal_server_source_does_not_depend_on_worker_package() -> None:
    repository_root = Path(__file__).parents[2]
    go_sources = (
        path.read_text(encoding="utf-8")
        for path in repository_root.rglob("*.go")
        if ".git" not in path.parts
    )

    assert all("temporalio_codex" not in source for source in go_sources)
    assert (repository_root / "go.mod").is_file()
