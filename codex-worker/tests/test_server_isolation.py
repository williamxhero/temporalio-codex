import shutil
import subprocess
from pathlib import Path

import pytest


def test_temporal_server_source_does_not_depend_on_worker_package() -> None:
    repository_root = Path(__file__).parents[2]
    assert (repository_root / "go.mod").is_file()

    go = shutil.which("go")
    if go is None:
        pytest.skip("Go toolchain is required for the Server dependency check")

    result = subprocess.run(
        [go, "list", "-deps", "./..."],
        cwd=repository_root,
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "temporalio_codex" not in result.stdout
