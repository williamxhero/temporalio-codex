import os
import shutil
import subprocess
import sys
from pathlib import Path


def _run(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        capture_output=True,
        check=False,
        text=True,
    )


def test_wheel_installs_outside_checkout_and_exposes_cli_entrypoints(tmp_path) -> None:
    worker_root = Path(__file__).parents[1]
    uv = shutil.which("uv")
    assert uv is not None, "uv is required for the package installation gate"

    wheel_dir = tmp_path / "wheel"
    build = _run(
        [uv, "build", "--wheel", "--out-dir", str(wheel_dir)],
        cwd=worker_root,
    )
    assert build.returncode == 0, build.stderr
    wheels = tuple(wheel_dir.glob("*.whl"))
    assert len(wheels) == 1

    install_dir = tmp_path / "installed"
    install_dir.mkdir()
    install = _run(
        [
            uv,
            "pip",
            "install",
            "--target",
            str(install_dir),
            "--no-deps",
            str(wheels[0]),
        ],
        cwd=tmp_path,
    )
    assert install.returncode == 0, install.stderr

    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(install_dir)
    probe = (
        "import importlib.metadata as m; "
        "import temporalio_codex; "
        "assert temporalio_codex.__file__.startswith(r'"
        + str(install_dir)
        + "'); "
        "entry_points = {entry.name for entry in m.distribution('temporalio-codex-worker').entry_points}; "
        "assert {'temporalio-codex-run', 'temporalio-codex-worker', 'temporalio-codex-qualify'} <= entry_points"
    )
    imported = _run([sys.executable, "-c", probe], cwd=tmp_path, env=environment)
    assert imported.returncode == 0, imported.stderr

    for module, function in (
        ("temporalio_codex.client", "main"),
        ("temporalio_codex.worker", "main"),
        ("temporalio_codex.qualification", "main"),
    ):
        command = (
            "import sys; "
            f"from {module} import {function}; "
            "sys.argv = ['installed-entrypoint', '--help']; "
            f"{function}()"
        )
        help_result = _run(
            [sys.executable, "-c", command],
            cwd=tmp_path,
            env=environment,
        )
        assert help_result.returncode == 0, help_result.stderr
        assert "usage:" in help_result.stdout.lower()
