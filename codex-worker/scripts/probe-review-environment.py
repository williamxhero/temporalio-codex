"""Read-only business probe with real SDK test and GitHub access."""

import argparse
import asyncio
import json
from pathlib import Path

from openai_codex import ApprovalMode, AsyncCodex, Sandbox


async def main(args):
    prompt = (
        "Verify the independent review environment for williamxhero/stock_advisor. "
        "Do not edit, commit, publish, deploy or delegate. "
        "Run powershell -NoProfile -ExecutionPolicy Bypass -File scripts/test.ps1 -ProjectRegression "
        "from the current directory, then gh issue view 82 and 84 --repo williamxhero/stock_advisor "
        "--json number,title,body. Require real successful command receipts. "
        "Return JSON with project_regression_passed (boolean), github_acceptance_read (boolean), "
        "passed_tests (integer), and failures (array of strings)."
    )
    async with AsyncCodex() as codex:
        thread = await codex.thread_start(
            cwd=str(args.workspace.resolve()), model="gpt-6.1-sol",
            approval_mode=ApprovalMode.deny_all, sandbox=Sandbox.full_access,
        )
        handle = await thread.turn(
            prompt, cwd=str(args.workspace.resolve()), model="gpt-6.1-sol",
            approval_mode=ApprovalMode.deny_all, sandbox=Sandbox.full_access,
        )
        turn = await handle.run()
        receipt = {"thread_id": thread.id, "status": str(turn.status),
                   "response": turn.final_response}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
        print(json.dumps(receipt))
        proof = json.loads(turn.final_response)
        if not proof.get("project_regression_passed") or not proof.get("github_acceptance_read"):
            raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    asyncio.run(main(parser.parse_args()))
