"""Read-only real SDK probe; GitHub publication and delivery are test boundaries."""

import argparse
import asyncio
import json
import subprocess
import shutil
from dataclasses import asdict, replace
from pathlib import Path
from uuid import uuid4

from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.worker import Replayer, Worker

from temporalio_codex.activities import codex_stage, configure_codex_adapter, configure_delivery_adapters, delivery_git_stage, delivery_github_stage
from temporalio_codex.candidate_activities import CandidateCaptureInput, CandidateCaptureResult
from temporalio_codex.conversation_store import ConversationStore
from temporalio_codex.delivery_adapter import FakeDeliveryAdapter
from temporalio_codex.delivery_models import DeliveryInput, DeliveryPhase, ReviewEvidence
from temporalio_codex.planning_activities import configure_spec_issue_gateway, configure_ticket_issue_gateway, prepare_grill, publish_spec_issues, publish_ticket_issues
from temporalio_codex.planning_models import SourceOrigin
from temporalio_codex.spec_issue_adapter import FakeSpecIssueGateway, SpecDraft
from temporalio_codex.spec_workflows import SpecExecutionWorkflow
from temporalio_codex.summary_activities import configure_summary_gateway, publish_delivery_summary
from temporalio_codex.summary_adapter import FakeSummaryCommentGateway, SummaryPublicationInput
from temporalio_codex.ticket_issue_adapter import FakeTicketIssueGateway
from temporalio_codex.ticket_scheduler import SchedulerInput, SpecPlan, TicketPlan
from temporalio_codex.whole_flow_models import PlanningPayload, SpecCodexPlan, SpecDeliveryPlan, WholeFlowInput
from temporalio_codex.worker import _build_codex_adapter
from temporalio_codex.workflow_identity import project_spec_id


@activity.defn(name="capture-codex-candidate")
async def capture_read_only(input: CandidateCaptureInput) -> CandidateCaptureResult:
    review = None
    if input.review_json is not None:
        proof = json.loads(input.review_json)
        if proof != {"candidate_sha": input.candidate.candidate_sha, "verdict": "approved", "findings": []}:
            raise ValueError("SDK review did not approve the read-only probe candidate")
        review = ReviewEvidence(input.candidate.candidate_sha, "approved", input.operation_id, input.thread_id, input.turn_id)
    return CandidateCaptureResult(input.candidate, review)


class ReadOnlyGitAdapter(FakeDeliveryAdapter):
    def __init__(self, sha):
        super().__init__()
        self.sha = sha

    async def execute(self, operation):
        receipt = await super().execute(operation)
        if operation.phase is DeliveryPhase.CANDIDATE:
            return replace(receipt, candidate_sha=self.sha)
        return receipt


async def probe(args):
    source = args.repository.resolve()
    root = source / ".tmp" / ("spec-probe-" + uuid4().hex[:12])
    root.mkdir(parents=True)
    shutil.copy2(source / "codex-worker" / "README.md", root / "README.md")
    (root / "AGENTS.md").write_text(
        "Only inspect README.md. Do not access paths outside this directory, "
        "invoke other agents, modify files, or run Git mutations.\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "--quiet"], cwd=root, check=True)
    subprocess.run(["git", "-c", "user.name=Probe", "-c", "user.email=probe@invalid", "add", "README.md"], cwd=root, check=True)
    subprocess.run(["git", "-c", "user.name=Probe", "-c", "user.email=probe@invalid", "commit", "--quiet", "-m", "baseline"], cwd=root, check=True)
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    key = "ReadOnlySdkProbeSpec"
    queue = "spec-sdk-probe-" + uuid4().hex[:12]
    workflow_id = project_spec_id(root.name, key, queue)
    store = ConversationStore(args.database)
    adapter = _build_codex_adapter(store)
    if adapter is None:
        store.close()
        raise RuntimeError("real openai-codex SDK is unavailable")
    configure_codex_adapter(adapter)
    configure_delivery_adapters(ReadOnlyGitAdapter(sha), FakeDeliveryAdapter())
    configure_spec_issue_gateway(FakeSpecIssueGateway())
    configure_ticket_issue_gateway(FakeTicketIssueGateway())
    configure_summary_gateway(FakeSummaryCommentGateway())
    requirement = (
        "Read-only acceptance probe. Read README.md and report the offline acceptance test command. "
        "End the planning and implementation responses with SPEC_SDK_PROBE_OK. "
        "Do not modify or commit files, publish issues, invoke external services, or delegate work. "
        "The implementation role is a read-only verification for this probe. "
        "The review role must inspect this probe and return the requested JSON approval for the frozen candidate."
    )
    input = WholeFlowInput(
        repository="local/" + root.name,
        execution_layout="spec",
        planning=PlanningPayload(SourceOrigin.TEXT, source_text=requirement, specs=(
            SpecDraft(key, "Read-only SDK probe", "README verification", ("Real SDK turn recorded",), ("No external publication",)),
        )),
        scheduler=SchedulerInput((SpecPlan(key),), (TicketPlan("read-only", key),)),
        codex=(SpecCodexPlan(key, requirement, str(root), ("read_only",), model=args.model, start_to_close_timeout_seconds=args.timeout),),
        deliveries=(SpecDeliveryPlan(key, DeliveryInput(
            repository=str(root), workspace=str(root), base_sha=sha,
            candidate_branch="codex/read-only-probe", pull_request_identity=queue,
            title="Read-only probe", body="Test boundary", readback_backoff_seconds=0,
        )),),
        summary=SummaryPublicationInput("local/probe", 1, queue, "Read-only probe; delivery boundary simulated"),
    )
    client = await Client.connect(args.target_host)
    try:
        async with Worker(client, task_queue=queue, workflows=[SpecExecutionWorkflow], activities=[
            codex_stage, capture_read_only, delivery_git_stage, delivery_github_stage,
            prepare_grill, publish_spec_issues, publish_ticket_issues, publish_delivery_summary,
        ]):
            handle = await client.start_workflow(SpecExecutionWorkflow.run, input, id=workflow_id, task_queue=queue)
            try:
                result = await asyncio.wait_for(handle.result(), args.timeout * 3 + 30)
            except TimeoutError:
                await handle.signal("cancel")
                raise
            history = await handle.fetch_history()
            await Replayer(workflows=[SpecExecutionWorkflow]).replay_workflow(history)
            children = sum(event.event_type == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED for event in history.events)
            snapshot = store.snapshot(workflow_id, handle.first_execution_run_id)
            turns = [turn for conversation in snapshot["conversations"] for turn in conversation["turns"]]
            passed = result.status.value == "completed" and children == 0 and len(turns) == 3 and all(turn["status"] == "completed" for turn in turns)
            evidence = {"passed": passed, "workflow_id": workflow_id, "run_id": handle.first_execution_run_id,
                        "children": children, "real_sdk_turns": len(turns), "delivery_boundary": "simulated",
                        "result": asdict(result), "snapshot": snapshot,
                        "chat_url": f"http://127.0.0.1:18000/namespaces/default/workflows/{workflow_id}/{handle.first_execution_run_id}/chat"}
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
            print(json.dumps({key: value for key, value in evidence.items() if key not in {"result", "snapshot"}}))
            if not passed:
                raise SystemExit(1)
    finally:
        store.close()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", required=True)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", default="gpt-6.1-sol")
    parser.add_argument("--target-host", default="127.0.0.1:7233")
    parser.add_argument("--timeout", type=float, default=180)
    asyncio.run(probe(parser.parse_args()))
