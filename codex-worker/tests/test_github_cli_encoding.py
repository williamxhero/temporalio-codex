from types import SimpleNamespace

import pytest

from temporalio_codex.github_adapter import GhCliGateway
from temporalio_codex.spec_issue_adapter import GhCliSpecIssueGateway
from temporalio_codex.summary_adapter import GhCliSummaryCommentGateway
from temporalio_codex.ticket_issue_adapter import GhCliTicketIssueGateway


@pytest.mark.parametrize(
    "runner",
    [
        GhCliSpecIssueGateway._run,
        GhCliTicketIssueGateway._run_json,
        GhCliSummaryCommentGateway._run,
        GhCliGateway._run,
    ],
)
def test_github_cli_json_uses_utf8_output(monkeypatch, runner) -> None:
    calls = []

    def fake_run(args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(returncode=0, stdout='[{"title":"中文"}]')

    monkeypatch.setattr("subprocess.run", fake_run)

    assert runner(["gh", "api", "fixture"]) == [{"title": "中文"}]
    assert calls == [{
        "capture_output": True,
        "check": False,
        "text": True,
        "encoding": "utf-8",
    }]
